//! Public refund ciphertext in synthetic block envelopes. Consensus is trusted to RPC.
use axum::{extract::State, routing::post, Json, Router};
use serde_json::{json, Value};
use std::sync::Arc;
use zakura_server_chain::testing::receiver::{ironwood_root, RefundBlock};

/// Ironwood's mainnet activation height, as the tests count heights.
fn activation() -> u64 {
    u64::from(receiver_indexer::blocks::ironwood_activation())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_resumes_and_replaces_an_orphaned_publication() {
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    #[derive(Clone)]
    struct Chain {
        blocks: Arc<[RefundBlock; 2]>,
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
        let second = first
            .clone()
            .coinbase_only()
            .at_height(height as u32 + 1)
            .after(first.hash());
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
                json!(zakura_server_chain::hash::display(
                    &zakura_server_chain::Network::Mainnet.genesis_hash()
                ))
            }
            "getblockhash" if r["params"][0] == height - 1 => json!("00".repeat(32)),
            "getblockhash" => {
                assert!(
                    requested.unwrap() < height + count as u64,
                    "must walk below the shorter node tip before fetching hashes"
                );
                json!(b.display_hash())
            }
            "getblock" if r["params"][1] == 1 => {
                json!({"trees":{"ironwood":{"size":if r["params"][0] == second.display_hash() {3} else {2}}}})
            }
            "getblockheader" => json!({"time": 1_800_000_000}),
            "getblock" => json!(b.raw_hex()),
            // The root comes from the node's own tree over every Action up to the block.
            "z_gettreestate" => {
                let extended = r["params"][0] == second.display_hash();
                let root = hex::encode(if extended {
                    ironwood_root(&[first, &second])
                } else {
                    ironwood_root(&[first])
                });
                json!({"hash": r["params"][0], "height": height + u64::from(extended),
                    "ironwood": {"commitments": {"finalRoot": root}}})
            }
            _ => panic!("unexpected method"),
        };
        Json(json!({"result":result,"error":null}))
    }
    let b = RefundBlock::new(activation() as u32);
    // Change only the header nonce; both versions have the same recoverable payment.
    let replacement = b.clone().after([0; 32]).renonced();
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
    // at the height and position this synthetic chain gives it, and checks its path in
    // the served witness file under the node's root. The index has no NEAR
    // feed, so after the lookup it reports the recent set as stale.
    let revision = extended["revision"].as_str().unwrap();
    let rows = std::fs::read(dir.path().join(format!("publications/{revision}.rows"))).unwrap();
    let payment = rows
        .chunks(receiver_directory::snapshot::ROW_BYTES)
        .flat_map(|row| {
            row[..receiver_directory::snapshot::SLOTS * receiver_directory::RECORD_BYTES]
                .chunks(receiver_directory::RECORD_BYTES)
        })
        .find_map(|slot| zakura_receiver_crypto::Record::decode(slot).unwrap())
        .unwrap()
        .payment;
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../crates/receiver-store/tests/fixtures/zero-ovk-action.json"
    ))
    .unwrap();
    fixture["height"] = payment.height.into();
    fixture["block_hash"] = zakura_server_chain::hash::display(&payment.block_hash).into();
    fixture["tx_index"] = payment.tx_index.into();
    fixture["position"] = payment.position.into();
    // The deploy gate's form of the probe: `receiver-directory probe`.
    let probe_nodes = |fixture: &Value, nodes: &[&str]| {
        let path = dir.path().join("fixture.json");
        std::fs::write(&path, serde_json::to_vec(fixture).unwrap()).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_receiver-directory"))
            .args([
                "probe",
                "--origin",
                &format!("http://{addr}"),
                "--health-url",
                &format!("http://{addr}/v1/receiver/health"),
                "--fixture",
                path.to_str().unwrap(),
                "--no-auth",
                "--witnesses",
            ])
            .args(nodes.iter().flat_map(|node| ["--rpc-url", node]))
            .output()
            .unwrap();
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    let probe = |fixture: &Value| probe_nodes(fixture, &[&url]);
    let result = probe(&fixture);
    assert_eq!(result["phase"], "live_encrypted_probe", "{result}");
    assert_eq!(
        (result["queries"].clone(), result["correct"].clone()),
        (json!(1), json!(1))
    );
    assert_eq!(result["category"], "stale_feed");
    assert_eq!(result["passed"], false);
    // The fixture carries its receiver's independent decoding; a pin that recovery
    // does not reproduce is refused before any lookup.
    let mut repinned = fixture.clone();
    // The receiver at external index zero of the spending key of all-three bytes.
    let other: serde_json::Value = serde_json::from_str(include_str!(
        "../../../crates/receiver-store/tests/fixtures/receivers.json"
    ))
    .unwrap();
    repinned["receiver"] = other["seeds"]["3"].clone();
    let result = probe(&repinned);
    assert_eq!(result["category"], "oracle_invalid", "{result}");
    assert!(result["phase"].is_null(), "{result}");
    let mut moved = fixture.clone();
    moved["position"] = (payment.position + 1).into();
    let result = probe(&moved);
    assert_eq!(result["category"], "answer_mismatch", "{result}");
    assert_eq!(result["correct"], 0);
    // An unreachable node falls back to the next; a node behind the publication can
    // check nothing, which is unavailability, never a pass.
    let result = probe_nodes(&fixture, &["http://127.0.0.1:1", &url]);
    assert_eq!(result["correct"], 1, "{result}");
    count.store(1, Ordering::SeqCst);
    let result = probe(&fixture);
    assert_eq!(result["category"], "oracle_unavailable", "{result}");
    assert_eq!(result["passed"], false);
    count.store(2, Ordering::SeqCst);
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

/// Witnesses need commitments from position zero, and wallets refuse served coverage
/// that starts later, so both refuse a later start before contacting the node.
#[test]
fn cli_refuses_a_later_start_for_witnesses_or_serving_before_contacting_the_node() {
    for flag in ["--witnesses", "--serve"] {
        let dir = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_receiver-directory"))
            .args([
                "--data-dir",
                dir.path().to_str().unwrap(),
                "--rpc-url",
                "http://127.0.0.1:1",
                "--no-auth",
                flag,
                "--start-height",
                &(activation() + 1).to_string(),
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "{flag}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("need history from Ironwood activation"),
            "{flag}"
        );
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
