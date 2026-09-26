//! Public refund ciphertext in synthetic block envelopes. Consensus is trusted to RPC.
use axum::{extract::State, routing::post, Json, Router};
use enhance_pir_server::{receiver::extract_block, zakura::ZakuraClient};
use serde_json::{json, Value};
use std::sync::Arc;
use zakura_chain::{
    block::{Block, Height},
    serialization::{ZcashDeserialize, ZcashSerialize},
    transaction::Transaction,
    transparent::Input,
};

fn block() -> Block {
    let tx = Transaction::zcash_deserialize(
        hex::decode(include_str!("fixtures/receiver-refund.hex").trim())
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    let mut coinbase = tx.clone();
    match &mut coinbase {
        Transaction::V6 { inputs, .. } => {
            *inputs = vec![Input::Coinbase {
                height: Height(3496114),
                data: vec![0],
                sequence: u32::MAX,
            }]
        }
        _ => panic!("expected V6"),
    }
    // Give the coinbase the same recoverable ciphertext to exercise its exclusion.
    let mut raw = vec![0u8; 140];
    raw[..4].copy_from_slice(&4u32.to_le_bytes());
    raw.extend_from_slice(&[0xfd, 0x40, 0x05]);
    raw.extend_from_slice(&[0; 1344]);
    raw.push(2);
    coinbase.zcash_serialize(&mut raw).unwrap();
    tx.zcash_serialize(&mut raw).unwrap();
    Block::zcash_deserialize(raw.as_slice()).unwrap()
}
#[test]
fn coinbase_exclusion_preserves_position_and_compact_context() {
    let b = block();
    let indexed = extract_block(&b, 3496114, 610504).unwrap();
    assert_eq!(indexed.coinbase_actions, 1);
    assert_eq!(indexed.start_position, 610502);
    assert_eq!(indexed.payments.len(), 1);
    let p = &indexed.payments[0].1;
    assert_eq!((p.tx_index, p.action_index, p.position), (1, 0, 610503));
    assert_eq!(p.txid, b.transactions[1].hash().0);
    let action = b.transactions[1].ironwood_actions().next().unwrap();
    assert_eq!(
        p.ciphertext_prefix,
        <[u8; 580]>::from(action.enc_ciphertext)[..52]
    );
    assert!(extract_block(&b, 3496115, 610504).is_err());
    assert!(extract_block(&b, 3496114, 1).is_err());
}
#[derive(Clone)]
struct Rpc {
    block: Arc<Block>,
    wrong_hash: bool,
}
async fn rpc(State(s): State<Rpc>, Json(r): Json<Value>) -> Json<Value> {
    let hash = s.block.hash().to_string();
    let result = match r["method"].as_str().unwrap() {
        "getblockhash" => {
            if s.wrong_hash {
                json!("00".repeat(32))
            } else {
                json!(hash)
            }
        }
        "getblock" => {
            if r["params"][1] == 1 {
                // Tree size is fetched by hash. Raw blocks arrive concurrently by height.
                assert_eq!(
                    r["params"][0],
                    if s.wrong_hash { "00".repeat(32) } else { hash }
                );
                json!({"trees":{"ironwood":{"size":610504}}})
            } else {
                let mut raw = Vec::new();
                s.block.zcash_serialize(&mut raw).unwrap();
                json!(hex::encode(raw))
            }
        }
        _ => panic!("unexpected RPC"),
    };
    Json(json!({"result":result,"error":null}))
}
#[tokio::test]
async fn rpc_checks_raw_block_against_canonical_anchor() {
    for wrong_hash in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new().route("/", post(rpc)).with_state(Rpc {
            block: Arc::new(block()),
            wrong_hash,
        });
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let result = ZakuraClient::unauthenticated(url)
            .unwrap()
            .receiver_batch(
                &receiver_directory::store::Checkpoint {
                    height: 3496113,
                    hash: [0; 32],
                    position: 610502,
                },
                1,
                8,
            )
            .await;
        assert_eq!(result.is_err(), wrong_hash);
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_resumes_and_replaces_an_orphaned_publication() {
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, Ordering};
    #[derive(Clone)]
    struct Chain {
        blocks: Arc<[Block; 2]>,
        replaced: Arc<AtomicBool>,
    }
    async fn handler(State(s): State<Chain>, Json(r): Json<Value>) -> Json<Value> {
        let b = &s.blocks[usize::from(s.replaced.load(Ordering::SeqCst))];
        let height = enhance_pir::ACTIVATION_HEIGHT;
        let result = match r["method"].as_str().unwrap() {
            "getblockcount" => json!(height),
            "getblockhash" if r["params"][0] == 0 => {
                json!(zakura_chain::parameters::Network::Mainnet
                    .genesis_hash()
                    .to_string())
            }
            "getblockhash" if r["params"][0] == height - 1 => json!("00".repeat(32)),
            "getblockhash" => json!(b.hash().to_string()),
            "getblock" if r["params"][1] == 1 => json!({"trees":{"ironwood":{"size":2}}}),
            "getblock" => {
                let mut raw = Vec::new();
                b.zcash_serialize(&mut raw).unwrap();
                json!(hex::encode(raw))
            }
            _ => panic!("unexpected method"),
        };
        Json(json!({"result":result,"error":null}))
    }
    let mut b = block();
    let coinbase = Arc::make_mut(&mut b.transactions[0]);
    match coinbase {
        Transaction::V6 { inputs, .. } => match &mut inputs[0] {
            Input::Coinbase { height, .. } => {
                *height = Height(enhance_pir::ACTIVATION_HEIGHT as u32)
            }
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
    let mut replacement = b.clone();
    Arc::make_mut(&mut replacement.header).previous_block_hash = zakura_chain::block::Hash([0; 32]);
    // Change only the header nonce; both versions have the same recoverable payment.
    Arc::make_mut(&mut replacement.header).nonce[0] ^= 1;
    let changed = Arc::new(AtomicBool::new(false));
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", socket.local_addr().unwrap());
    let router = Router::new().route("/", post(handler)).with_state(Chain {
        blocks: Arc::new([b, replacement]),
        replaced: changed.clone(),
    });
    let task = tokio::spawn(async move { axum::serve(socket, router).await.unwrap() });
    let dir = tempfile::tempdir().unwrap();
    let run = || {
        let output = Command::new(env!("CARGO_BIN_EXE_receiver-directory"))
            .args([
                "--data-dir",
                dir.path().to_str().unwrap(),
                "--rpc-url",
                &url,
                "--no-auth",
                "--confirmations",
                "0",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    let first = run();
    assert_eq!(first["records"], 1);
    assert_eq!(first, run());
    changed.store(true, Ordering::SeqCst);
    let next = run();
    assert_eq!(next["records"], 1);
    assert_ne!(first["revision"], next["revision"]);
    assert_eq!(next, run());
    task.abort();
}

#[tokio::test]
async fn concurrent_batches_reject_gaps_forks_and_wrong_positions() {
    use receiver_directory::store::Checkpoint;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Clone)]
    struct BatchRpc {
        blocks: Arc<Vec<Block>>,
        fault: u8,
        calls: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }
    async fn handler(State(s): State<BatchRpc>, Json(r): Json<Value>) -> Json<Value> {
        let call = s.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let terminal = s.blocks.last().unwrap().hash().to_string();
        let result = match r["method"].as_str().unwrap() {
            "getblockhash" => {
                if s.fault == 4 || (s.fault == 7 && call == 7) {
                    json!("00".repeat(32))
                } else {
                    json!(terminal)
                }
            }
            "getblock" if r["params"][1] == 1 => {
                assert_eq!(
                    r["params"][0],
                    if s.fault == 4 {
                        "00".repeat(32)
                    } else {
                        terminal
                    }
                );
                json!({"trees":{"ironwood":{"size":if s.fault == 3 {209} else {208}}}})
            }
            "getblock" => {
                assert_eq!(r["params"][1], 0);
                let height: usize = r["params"][0].as_str().unwrap().parse().unwrap();
                let i = height - 3496114;
                let active = s.active.fetch_add(1, Ordering::SeqCst) + 1;
                s.peak.fetch_max(active, Ordering::SeqCst);
                // Force responses out of order while also observing the concurrency bound.
                tokio::time::sleep(std::time::Duration::from_millis(if i.is_multiple_of(2) {
                    30
                } else {
                    5
                }))
                .await;
                s.active.fetch_sub(1, Ordering::SeqCst);
                if s.fault == 6 && i == 1 {
                    return Json(
                        json!({"result":null,"error":{"code":-8,"message":"missing block"}}),
                    );
                }
                let mut raw = Vec::new();
                s.blocks[i].zcash_serialize(&mut raw).unwrap();
                if s.fault == 5 && i == 1 {
                    raw.push(1);
                }
                json!(hex::encode(raw))
            }
            _ => panic!("unexpected method"),
        };
        Json(json!({"result":result,"error":null}))
    }
    for fault in 0..=7 {
        let mut blocks = Vec::new();
        let mut parent = [0; 32];
        for i in 0..4 {
            let mut b = block();
            let tx = Arc::make_mut(&mut b.transactions[0]);
            if let Transaction::V6 { inputs, .. } = tx {
                if let Input::Coinbase { height, .. } = &mut inputs[0] {
                    *height = Height(3496114 + i + u32::from(fault == 2 && i == 1));
                }
            }
            Arc::make_mut(&mut b.header).previous_block_hash =
                zakura_chain::block::Hash(if fault == 1 && i == 1 {
                    [99; 32]
                } else {
                    parent
                });
            parent = b.hash().0;
            blocks.push(b);
        }
        let state = BatchRpc {
            blocks: Arc::new(blocks),
            fault,
            calls: Arc::new(AtomicUsize::new(0)),
            active: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new()
            .route("/", post(handler))
            .with_state(state.clone());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = ZakuraClient::unauthenticated(url).unwrap();
        let previous = Checkpoint {
            height: 3496113,
            hash: [0; 32],
            position: 200,
        };
        for (count, concurrency) in [(0, 2), (65, 2), (4, 0), (4, 17)] {
            assert!(client
                .receiver_batch(&previous, count, concurrency)
                .await
                .is_err());
        }
        assert_eq!(state.calls.load(Ordering::SeqCst), 0);
        let result = client.receiver_batch(&previous, 4, 2).await;
        assert_eq!(result.is_err(), fault != 0, "fault={fault}");
        assert_eq!(state.peak.load(Ordering::SeqCst), 2);
        if let Ok(indexed) = result {
            assert_eq!(indexed.len(), 4);
            // Four raw-block requests plus three terminal checks, rather than 20 requests.
            assert_eq!(state.calls.load(Ordering::SeqCst), 7);
            for (i, b) in indexed.iter().enumerate() {
                let expected =
                    extract_block(&state.blocks[i], 3496114 + i as u32, 202 + i as u64 * 2)
                        .unwrap();
                assert_eq!(b.payments, expected.payments);
                assert_eq!(b.start_position, 200 + i as u64 * 2);
                assert_eq!(b.coinbase_actions, 1);
            }
        }
        task.abort();
    }
}
