//! Public refund ciphertext in synthetic block envelopes. Consensus is trusted to RPC.
use axum::{extract::State, routing::post, Json, Router};
use receiver_indexer::{blocks::extract_block, zakura::ZakuraClient};

/// Ironwood's mainnet activation height, as the tests count heights.
fn activation() -> u64 {
    u64::from(receiver_indexer::blocks::ironwood_activation())
}
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
    let mut b = Block::zcash_deserialize(raw.as_slice()).unwrap();
    seal(&mut b);
    b
}

/// Sets `b`'s header merkle root to its transactions' root.
fn seal(b: &mut Block) {
    let root = b.transactions.iter().collect();
    Arc::make_mut(&mut b.header).merkle_root = root;
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_resumes_and_replaces_an_orphaned_publication() {
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    #[derive(Clone)]
    struct Chain {
        blocks: Arc<[Block; 2]>,
        replaced: Arc<AtomicBool>,
        count: Arc<AtomicUsize>,
        hold: Arc<AtomicBool>,
    }
    async fn handler(State(s): State<Chain>, Json(r): Json<Value>) -> Json<Value> {
        if s.hold.load(Ordering::SeqCst) && r["method"] == "getblock" {
            return Json(
                json!({"result":null,"error":{"code":-8,"message":"temporarily unavailable"}}),
            );
        }
        let first = &s.blocks[usize::from(s.replaced.load(Ordering::SeqCst))];
        let height = activation();
        let count = s.count.load(Ordering::SeqCst);
        let mut second = first.clone();
        second.transactions.truncate(1);
        seal(&mut second);
        Arc::make_mut(&mut second.header).previous_block_hash = first.hash();
        match Arc::make_mut(&mut second.transactions[0]) {
            Transaction::V6 { inputs, .. } => match &mut inputs[0] {
                Input::Coinbase { height: h, .. } => *h = Height(height as u32 + 1),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
        let requested = r["params"][0]
            .as_u64()
            .or_else(|| r["params"][0].as_str().and_then(|s| s.parse().ok()));
        let b = if requested == Some(height + 1) {
            &second
        } else {
            first
        };
        let result = match r["method"].as_str().unwrap() {
            "getblockcount" => json!(height + count as u64 - 1),
            "getblockhash" if r["params"][0] == 0 => {
                json!(zakura_chain::parameters::Network::Mainnet
                    .genesis_hash()
                    .to_string())
            }
            "getblockhash" if r["params"][0] == height - 1 => json!("00".repeat(32)),
            "getblockhash" => {
                assert!(
                    requested.unwrap() < height + count as u64,
                    "must walk below the shorter node tip before fetching hashes"
                );
                json!(b.hash().to_string())
            }
            "getblock" if r["params"][1] == 1 => {
                json!({"trees":{"ironwood":{"size":if r["params"][0] == second.hash().to_string() {3} else {2}}}})
            }
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
            Input::Coinbase { height, .. } => *height = Height(activation() as u32),
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
    let mut replacement = b.clone();
    Arc::make_mut(&mut replacement.header).previous_block_hash = zakura_chain::block::Hash([0; 32]);
    // Change only the header nonce; both versions have the same recoverable payment.
    Arc::make_mut(&mut replacement.header).nonce[0] ^= 1;
    let changed = Arc::new(AtomicBool::new(false));
    let count = Arc::new(AtomicUsize::new(1));
    let hold = Arc::new(AtomicBool::new(false));
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", socket.local_addr().unwrap());
    let router = Router::new().route("/", post(handler)).with_state(Chain {
        blocks: Arc::new([b, replacement]),
        replaced: changed.clone(),
        count: count.clone(),
        hold: hold.clone(),
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
                "--depth",
                "0",
                "--witnesses",
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
    let revision = first["revision"].as_str().unwrap();
    assert!(dir
        .path()
        .join(format!("publications/{revision}.witness"))
        .exists());
    // Republishing a revision requires identical bytes in every existing file.
    assert_eq!(first, run());
    changed.store(true, Ordering::SeqCst);
    let next = run();
    assert_eq!(next["records"], 1);
    assert_ne!(first["revision"], next["revision"]);
    assert_eq!(next, run());
    count.store(2, Ordering::SeqCst);
    let extended = run();
    assert_eq!(extended["records"], 1);
    assert_eq!(extended["actions"], 3);
    assert_eq!(extended["end_height"], activation() + 1);
    // A node behind the index proves nothing about the blocks above its tip, so the
    // index waits for it instead of rewinding.
    count.store(1, Ordering::SeqCst);
    let behind = Command::new(env!("CARGO_BIN_EXE_receiver-directory"))
        .args([
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--rpc-url",
            &url,
            "--no-auth",
            "--depth",
            "0",
            "--witnesses",
        ])
        .output()
        .unwrap();
    assert!(!behind.status.success());
    assert!(String::from_utf8_lossy(&behind.stderr).contains("node is behind the index"));
    count.store(2, Ordering::SeqCst);
    assert_eq!(run(), extended, "the index keeps its coverage");
    // The daemon must revoke a forked publication even when ingestion cannot rebuild
    // the new chain yet.
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let start = || {
        Child(
            Command::new(env!("CARGO_BIN_EXE_receiver-directory"))
                .args([
                    "--data-dir",
                    dir.path().to_str().unwrap(),
                    "--rpc-url",
                    &url,
                    "--no-auth",
                    "--depth",
                    "0",
                    "--witnesses",
                    "--serve",
                    "--poll-seconds",
                    "1",
                    "--bind",
                    &addr.to_string(),
                ])
                .stdout(std::process::Stdio::null())
                .stderr(std::fs::File::create(dir.path().join("daemon.log")).unwrap())
                .spawn()
                .unwrap(),
        )
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap();
    let endpoint = format!("http://{addr}/v1/receiver/init");
    async fn wait_height(client: &reqwest::Client, endpoint: &str, height: u64) {
        tokio::time::timeout(std::time::Duration::from_secs(90), async {
            loop {
                if let Ok(response) = client.get(endpoint).send().await {
                    if response.status().is_success()
                        && response.json::<Value>().await.unwrap()["directory"]["end_height"]
                            == height
                    {
                        break;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("daemon did not publish the canonical height");
    }
    let files = || {
        std::fs::read_dir(dir.path().join("publications"))
            .unwrap()
            .count()
    };
    let written = files();
    let daemon = start();
    wait_height(&client, &endpoint, activation() + 1).await;
    assert_eq!(files(), written, "serving publishes from memory");
    // The monitor's probe looks up a pinned payment over encrypted PIR: the public refund,
    // at the height and position this synthetic chain gives it. The index has no NEAR
    // feed, so after the lookup it reports the recent set as stale.
    let revision = extended["revision"].as_str().unwrap();
    let rows = std::fs::read(dir.path().join(format!("publications/{revision}.rows"))).unwrap();
    let payment = rows
        .chunks(receiver_directory::snapshot::ROW_BYTES)
        .flat_map(|row| {
            row[..receiver_directory::snapshot::SLOTS * receiver_directory::RECORD_BYTES]
                .chunks(receiver_directory::RECORD_BYTES)
        })
        .find_map(|slot| receiver_directory::Record::decode(slot).unwrap())
        .unwrap()
        .payment;
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../crates/receiver-directory/tests/fixtures/zero-ovk-action.json"
    ))
    .unwrap();
    fixture["height"] = payment.height.into();
    fixture["position"] = payment.position.into();
    let probe = |fixture: &Value, pin: Option<&str>| {
        let path = dir.path().join("fixture.json");
        let bytes = serde_json::to_vec(fixture).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        let sha256 = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&bytes));
        let output = Command::new(env!("CARGO_BIN_EXE_receiver-probe"))
            .args([
                "--origin",
                &format!("http://{addr}"),
                "--health-url",
                &format!("http://{addr}/v1/receiver/health"),
                "--fixture",
                path.to_str().unwrap(),
                "--fixture-sha256",
                pin.unwrap_or(&sha256),
                "--rpc-url",
                &url,
                "--no-auth",
            ])
            .output()
            .unwrap();
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    let result = probe(&fixture, None);
    assert_eq!(result["phase"], "live_encrypted_probe", "{result}");
    assert_eq!(
        (result["queries"].clone(), result["correct"].clone()),
        (json!(1), json!(1))
    );
    assert_eq!(result["category"], "stale_feed");
    assert_eq!(result["passed"], false);
    assert_eq!(
        probe(&fixture, Some(&"0".repeat(64)))["category"],
        "oracle_invalid"
    );
    let mut moved = fixture.clone();
    moved["position"] = (payment.position + 1).into();
    let result = probe(&moved, None);
    assert_eq!(result["category"], "answer_mismatch", "{result}");
    assert_eq!(result["correct"], 0);
    hold.store(true, Ordering::SeqCst);
    changed.store(false, Ordering::SeqCst);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if client.get(&endpoint).send().await.unwrap().status()
                == reqwest::StatusCode::SERVICE_UNAVAILABLE
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("guard kept serving an orphaned publication during ingestion failure");
    hold.store(false, Ordering::SeqCst);
    wait_height(&client, &endpoint, activation() + 1).await;
    drop(daemon);
    let _restarted = start();
    wait_height(&client, &endpoint, activation() + 1).await;
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
    for fault in 0..=8 {
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
            // A transaction changed under its header no longer matches its merkle root.
            if fault == 8 && i == 1 {
                if let Transaction::V6 { expiry_height, .. } = Arc::make_mut(&mut b.transactions[1])
                {
                    expiry_height.0 += 1;
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
        let client = ZakuraClient::unauthenticated(vec![url]).unwrap();
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

#[test]
fn cli_rejects_unservable_geometry_before_contacting_the_node() {
    for rows in ["1", "4096", "8193", "131072"] {
        let dir = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_receiver-directory"))
            .args([
                "--data-dir",
                dir.path().to_str().unwrap(),
                "--rpc-url",
                "http://127.0.0.1:1",
                "--no-auth",
                "--min-rows",
                rows,
            ])
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("Unsupported"));
        assert!(!dir.path().join("publications").exists());
    }
}

/// Serving binds loopback or a private address, never a public one.
#[test]
fn cli_refuses_a_public_bind_before_contacting_the_node() {
    let dir = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_receiver-directory"))
        .args([
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--rpc-url",
            "http://127.0.0.1:1",
            "--no-auth",
            "--serve",
            "--bind",
            "0.0.0.0:18380",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("loopback or private bind"));
}
