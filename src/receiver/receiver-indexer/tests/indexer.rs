//! Public refund ciphertext in synthetic block envelopes. Consensus is trusted to RPC.
#[path = "common/bound.rs"]
mod bound;
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
    let layout = dir
        .path()
        .join(format!("publications/{revision}.inventory.json"));
    let layout: receiver_indexer::application::Layout =
        serde_json::from_slice(&std::fs::read(layout).unwrap()).unwrap();
    let segment = &layout.shards[0].segments[0];
    let (payments, companion) = (
        hex::encode(segment.payments),
        hex::encode(segment.witnesses),
    );
    for file in [
        format!("{payments}.rows"),
        format!("{payments}.json"),
        format!("{companion}.witness"),
        format!("{companion}.witness.json"),
    ] {
        assert!(dir.path().join("publications").join(file).exists());
    }
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
    // Each daemon binds a port of its own choosing, read back from its log.
    let log = dir.path().join("daemon.log");
    let start = || async {
        let child = Child(
            Command::new(env!("CARGO_BIN_EXE_receiver-directory"))
                .args([
                    "--data-dir",
                    dir.path().to_str().unwrap(),
                    "--rpc-url",
                    &url,
                    "--no-auth",
                    "--depth",
                    "0",
                    "--serve",
                    "--poll-seconds",
                    "1",
                    "--bind",
                    "127.0.0.1:0",
                ])
                .env("RUST_LOG", "info")
                .stdout(std::process::Stdio::null())
                .stderr(std::fs::File::create(&log).unwrap())
                .spawn()
                .unwrap(),
        );
        (child, bound::address(&log, 0, "receiver PIR bound").await)
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap();
    async fn wait_height(client: &reqwest::Client, endpoint: &str, height: u64) {
        tokio::time::timeout(std::time::Duration::from_secs(90), async {
            loop {
                if let Ok(response) = client.get(endpoint).send().await {
                    if response.status().is_success()
                        && response.json::<Value>().await.unwrap()["end_height"] == height
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
    let (daemon, addr) = start().await;
    let endpoint = format!("http://{addr}/v1/receiver/inventory");
    wait_height(&client, &endpoint, activation() + 1).await;
    assert_eq!(files(), written, "serving publishes from memory");
    // The deploy gate's form of the probe, `receiver-directory probe`, with its built-in
    // mainnet fixture. That payment is above this synthetic chain, so the probe reaches the
    // served manifest and refuses to check it. The lookup itself is the probe's unit test.
    let output = Command::new(env!("CARGO_BIN_EXE_receiver-directory"))
        .args([
            "probe",
            "--origin",
            &format!("http://{addr}"),
            "--health-url",
            &format!("http://{addr}/v1/receiver/health"),
            "--rpc-url",
            &url,
            "--no-auth",
        ])
        .output()
        .unwrap();
    let result = serde_json::from_slice::<Value>(&output.stdout).unwrap();
    assert!(!output.status.success());
    assert_eq!(result["category"], "oracle_invalid", "{result}");
    assert_eq!(result["detail"], json!({"fixture": "after coverage"}));
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
    let (_restarted, addr) = start().await;
    let endpoint = format!("http://{addr}/v1/receiver/inventory");
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

/// Witness tables need every commitment of each Ironwood shard, and wallets refuse
/// served coverage that starts later, so one-shot and serving runs both refuse a later
/// start before contacting the node.
#[test]
fn cli_refuses_a_later_start_before_contacting_the_node() {
    for flag in ["--depth=2", "--serve"] {
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
