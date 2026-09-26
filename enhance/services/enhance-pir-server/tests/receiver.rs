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
            // Tree size and raw bytes must both be fetched by hash, never mixed by height.
            assert_eq!(
                r["params"][0],
                if s.wrong_hash { "00".repeat(32) } else { hash }
            );
            if r["params"][1] == 2 {
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
            .receiver_block(3496114)
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
            "getblock" if r["params"][1] == 2 => json!({"trees":{"ironwood":{"size":2}}}),
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
