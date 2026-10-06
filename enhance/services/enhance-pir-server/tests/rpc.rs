//! Actual canonical-mode CLI ingestion against a local trusted-RPC test double.
//! The public transaction is real; the block envelope and the mixed
//! transactions built from its actions are synthetic, not consensus-valid.
mod support;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use enhance_pir::{client::EnhancePirClient, protocol::Manifest, ACTIVATION_HEIGHT};
use enhance_pir_server::worker::Worker;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    path::Path,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use zakura_chain::serialization::ZcashDeserialize;

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[derive(Clone)]
struct Rpc {
    blocks: Arc<Vec<(String, String)>>,
    transactions: Arc<HashMap<String, String>>,
    selected: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    tree_size: Arc<AtomicUsize>,
}
async fn rpc(
    State(state): State<Rpc>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    // Public, test-only credential. No developer or service credentials are used.
    if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some("Basic dGVzdDp0ZXN0") {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state.calls.fetch_add(1, Ordering::SeqCst);
    Ok(Json(match request {
        // Spent outputs are requested in JSON-RPC 2.0 batches, answered in
        // reverse order so the client must place them by id.
        Value::Array(requests) => Value::Array(
            requests
                .iter()
                .rev()
                .map(|request| {
                    assert_eq!(request["jsonrpc"], "2.0");
                    answer(&state, request)
                })
                .collect(),
        ),
        request => answer(&state, &request),
    }))
}
fn answer(state: &Rpc, request: &Value) -> Value {
    let (raw, hash) = &state.blocks[state.selected.load(Ordering::SeqCst)];
    let result = match request["method"].as_str().unwrap() {
        "getblockcount" => json!(ACTIVATION_HEIGHT),
        "getblockhash" => {
            assert_eq!(request["params"], json!([ACTIVATION_HEIGHT]));
            json!(hash)
        }
        "getblock" => {
            assert_eq!(request["params"][0], ACTIVATION_HEIGHT.to_string());
            match request["params"][1].as_u64().unwrap() {
                0 => json!(raw),
                2 => json!({"trees":{"ironwood":{"size":state.tree_size.load(Ordering::SeqCst)}}}),
                _ => panic!("unexpected verbosity"),
            }
        }
        "getrawtransaction" => match state
            .transactions
            .get(request["params"][0].as_str().unwrap())
        {
            Some(raw) => json!(raw),
            None => {
                return json!({"result":null,"error":{"code":-5,"message":"No such transaction"},"id":request["id"]})
            }
        },
        _ => panic!("unexpected RPC method"),
    };
    json!({"result":result,"error":null,"id":request["id"]})
}
/// Positions 0..1: the frozen pure-Ironwood records. 2..3: 420,000 spent from
/// a same-block output into 400,000 Ironwood. 4..5: 99,000 same-block plus
/// 300,000 fetched from an earlier transaction, 100,000 transparent change
/// and 279,000 into Ironwood. Expected fees are hand-computed: 20,000 each.
async fn assert_answers(client: &mut EnhancePirClient, expected: &[Vec<u8>]) {
    for (i, record) in expected.iter().enumerate() {
        assert_eq!(
            client
                .query_position_with_timing(i as u64)
                .await
                .unwrap()
                .0
                .as_ref(),
            record
        );
    }
    for position in 2..6u64 {
        let answer = client.query_position_with_timing(position).await.unwrap().0;
        let record =
            enhance_pir::EnhanceRecord::from_bytes(answer.as_ref().try_into().unwrap()).unwrap();
        support::assert_fixture_action(&record, position as usize % 2);
        assert_eq!(record.metadata().fee_zatoshis(), Some(20_000), "{position}");
        let flags = record.as_bytes()[enhance_pir::types::RECORD_FLAGS_OFFSET];
        assert_eq!(flags & enhance_pir::types::FLAG_HAS_TRANSPARENT_INPUTS, 1);
        assert_eq!(
            flags & enhance_pir::types::FLAG_HAS_TRANSPARENT_OUTPUTS != 0,
            position >= 4,
            "{position}"
        );
    }
}
async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", socket.local_addr().unwrap());
    (
        origin,
        tokio::spawn(async move {
            axum::serve(socket, router).await.unwrap();
        }),
    )
}
fn start(root: &Path, address: &str, rpc: &str) -> Process {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("coordinator.log"))
        .unwrap();
    Process(
        Command::new(env!("CARGO_BIN_EXE_enhance-pir-server"))
            .args([
                "coordinator",
                "--listen",
                address,
                "--poll-seconds",
                "1",
                "--zakura-rpc-url",
                rpc,
            ])
            .arg("--data-dir")
            .arg(root.join("data"))
            .arg("--worker-config")
            .arg(root.join("workers.json"))
            .arg("--zakura-cookie")
            .arg(root.join("cookie"))
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    )
}
async fn published(process: &mut Process, root: &Path, origin: &str, hash: &str) -> Manifest {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    loop {
        assert!(
            process.0.try_wait().unwrap().is_none(),
            "coordinator stopped: {}",
            std::fs::read_to_string(root.join("coordinator.log")).unwrap()
        );
        if let Ok(response) = client.get(format!("{origin}/v1/enhance/init")).send().await {
            if let Ok(manifest) = response.json::<Manifest>().await {
                if manifest.anchor_block_hash == hash {
                    return manifest;
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "publication timeout: {}",
            std::fs::read_to_string(root.join("coordinator.log")).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canonical_cli_ingests_rpc_rewinds_reorg_and_restarts_without_duplication() {
    let tx = hex::decode(include_str!("fixtures/ironwood-fee-expiry.hex").trim()).unwrap();
    let earlier = support::funding(1, &[300_000]);
    let same_block = support::funding(2, &[420_000, 99_000]);
    let mixed = [
        support::Mixed::new(vec![support::spend(&same_block, 0)], &[], -400_000).build(),
        support::Mixed::new(
            vec![support::spend(&same_block, 1), support::spend(&earlier, 0)],
            &[100_000],
            -279_000,
        )
        .build(),
    ];
    let mut tail = Vec::new();
    for transaction in [&same_block, &mixed[0], &mixed[1]] {
        tail.extend(
            zakura_chain::serialization::ZcashSerialize::zcash_serialize_to_vec(transaction)
                .unwrap(),
        );
    }
    let mut blocks = Vec::new();
    for nonce in [0, 1, 2] {
        // Serialized Zcash header: version, roots, time/bits, nonce, Equihash
        // solution. The parser accepts this envelope; consensus is trusted to RPC.
        let mut raw = vec![0u8; 140];
        raw[..4].copy_from_slice(&4u32.to_le_bytes());
        raw[108] = nonce;
        raw.extend_from_slice(&[0xfd, 0x40, 0x05]);
        raw.extend_from_slice(&[0; 1344]);
        raw.push(4); // the public transaction, then the synthetic ones
        raw.extend_from_slice(&tx);
        raw.extend_from_slice(&tail);
        let parsed = zakura_chain::block::Block::zcash_deserialize(raw.as_slice()).unwrap();
        blocks.push((hex::encode(raw), parsed.hash().to_string()));
    }
    let expected = [
        include_str!("fixtures/ironwood-canonical-record-0.hex"),
        include_str!("fixtures/ironwood-canonical-record-1.hex"),
    ]
    // Historical oracle includes epk (32) and compact ciphertext (52).
    .map(|s| hex::decode(s.trim()).unwrap()[84..].to_vec());
    let state = Rpc {
        blocks: Arc::new(blocks),
        // Only the earlier transaction is outside the served block.
        transactions: Arc::new(HashMap::from([(
            earlier.hash().to_string(),
            support::raw(&earlier),
        )])),
        selected: Arc::new(AtomicUsize::new(0)),
        calls: Arc::new(AtomicUsize::new(0)),
        tree_size: Arc::new(AtomicUsize::new(6)),
    };
    let root = tempfile::tempdir().unwrap();
    let (rpc_url, rpc_task) = serve(
        Router::new()
            .route("/", post(rpc))
            .with_state(state.clone()),
    )
    .await;
    let mut workers = Vec::new();
    let mut replicas = Vec::new();
    let hold_prepare = Arc::new(AtomicBool::new(false));
    let held_requests = Arc::new(AtomicUsize::new(0));
    let hold_commit = Arc::new(AtomicBool::new(false));
    let held_commits = Arc::new(AtomicUsize::new(0));
    for i in 0..2 {
        let hold = hold_prepare.clone();
        let held = held_requests.clone();
        let commit_hold = hold_commit.clone();
        let commit_held = held_commits.clone();
        let (url, task) = serve(
            Worker::open(&root.path().join(format!("worker-{i}")))
                .unwrap()
                .router()
                .layer(axum::middleware::from_fn(
                    move |request: axum::extract::Request, next: axum::middleware::Next| {
                        let hold = hold.clone();
                        let held = held.clone();
                        let commit_hold = commit_hold.clone();
                        let commit_held = commit_held.clone();
                        async move {
                            if request.uri().path() == "/internal/prepare"
                                && hold.load(Ordering::SeqCst)
                            {
                                held.fetch_add(1, Ordering::SeqCst);
                                while hold.load(Ordering::SeqCst) {
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                }
                            }
                            if request.uri().path() == "/internal/commit"
                                && commit_hold.load(Ordering::SeqCst)
                            {
                                commit_held.fetch_add(1, Ordering::SeqCst);
                                while commit_hold.load(Ordering::SeqCst) {
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                }
                            }
                            next.run(request).await
                        }
                    },
                )),
        )
        .await;
        workers.push(task);
        replicas.push(json!({"name":format!("r{i}"),"url":url}));
    }
    std::fs::write(
        root.path().join("workers.json"),
        serde_json::to_vec(&json!({"groups":[{"name":"g0","replicas":replicas}]})).unwrap(),
    )
    .unwrap();
    std::fs::write(root.path().join("cookie"), "test:test").unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap().to_string();
    drop(socket);
    let origin = format!("http://{address}");
    let mut process = start(root.path(), &address, &rpc_url);
    let first = published(&mut process, root.path(), &origin, &state.blocks[0].1).await;
    assert_eq!(first.coverage.records, 6);
    assert_eq!(
        std::fs::read_to_string(root.path().join("data/source-mode")).unwrap(),
        "canonical-archive"
    );
    let mut retained = EnhancePirClient::connect(&origin).await.unwrap();
    assert_answers(&mut retained, &expected).await;
    // A reorg response whose declared tree has a missing prefix cannot be
    // committed. Observe the actual ingestion failure, not just a short delay.
    state.tree_size.store(7, Ordering::SeqCst);
    state.selected.store(1, Ordering::SeqCst);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(process.0.try_wait().unwrap().is_none());
        let log = std::fs::read_to_string(root.path().join("coordinator.log")).unwrap();
        if log.contains("journal continuity mismatch") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "missing ingestion rejection: {log}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        published(&mut process, root.path(), &origin, &state.blocks[0].1).await,
        first,
        "invalid RPC metadata must not replace the published generation"
    );
    assert_eq!(
        retained
            .query_position_with_timing(0)
            .await
            .unwrap()
            .0
            .as_ref(),
        expected[0],
        "journal rewind must not invalidate the retained serving snapshot"
    );
    hold_prepare.store(true, Ordering::SeqCst);
    state.tree_size.store(6, Ordering::SeqCst);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while held_requests.load(Ordering::SeqCst) == 0 {
        assert!(process.0.try_wait().unwrap().is_none());
        assert!(
            tokio::time::Instant::now() < deadline,
            "prepare barrier not reached"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let controller_path = root.path().join("data/control/controller.json");
    let interrupted: Value =
        serde_json::from_slice(&std::fs::read(&controller_path).unwrap()).unwrap();
    assert_eq!(interrupted["operation"]["phase"], "PREPARING");
    assert_eq!(
        published(&mut process, root.path(), &origin, &state.blocks[0].1).await,
        first,
        "an in-flight candidate must not replace the published generation"
    );
    drop(process); // Kill the real process with a durably reserved, uncommitted candidate.
    hold_prepare.store(false, Ordering::SeqCst);
    let mut process = start(root.path(), &address, &rpc_url);
    let second = published(&mut process, root.path(), &origin, &state.blocks[1].1).await;
    let recovered: Value =
        serde_json::from_slice(&std::fs::read(&controller_path).unwrap()).unwrap();
    assert!(recovered["epoch"].as_u64().unwrap() > interrupted["epoch"].as_u64().unwrap());
    assert!(
        recovered["next_attempt"].as_u64().unwrap() > interrupted["next_attempt"].as_u64().unwrap()
    );
    assert!(second.generation > first.generation);
    assert_eq!(
        second.coverage.records, 6,
        "reorg must rewind before replay, not append duplicates"
    );
    assert_eq!(
        retained
            .query_position_with_timing(1)
            .await
            .unwrap()
            .0
            .as_ref(),
        expected[1]
    );
    // Finish the preceding publication's acknowledgements before arming the
    // barrier, so only the new reorg decision can trigger this crash boundary.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let settled: Value =
            serde_json::from_slice(&std::fs::read(&controller_path).unwrap()).unwrap();
        if settled["pending_commits"].as_array().unwrap().is_empty() {
            break;
        }
        assert!(process.0.try_wait().unwrap().is_none());
        assert!(
            tokio::time::Instant::now() < deadline,
            "preceding commit outbox did not drain"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    hold_commit.store(true, Ordering::SeqCst);
    state.selected.store(2, Ordering::SeqCst);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while held_commits.load(Ordering::SeqCst) == 0 {
        assert!(process.0.try_wait().unwrap().is_none());
        assert!(
            tokio::time::Instant::now() < deadline,
            "commit barrier not reached"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let committed: Value =
        serde_json::from_slice(&std::fs::read(&controller_path).unwrap()).unwrap();
    assert_eq!(committed["pending_commits"].as_array().unwrap().len(), 2);
    let third = published(&mut process, root.path(), &origin, &state.blocks[2].1).await;
    assert!(third.generation > second.generation);
    assert_eq!(
        serde_json::to_value(&third).unwrap(),
        committed["published"][0]
    );
    drop(process); // Durable publication, but neither worker has acknowledged it.
    hold_commit.store(false, Ordering::SeqCst);
    let before_restart = state.calls.load(Ordering::SeqCst);
    let mut process = start(root.path(), &address, &rpc_url);
    let restarted = published(&mut process, root.path(), &origin, &state.blocks[2].1).await;
    assert_eq!(restarted, third);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let recovered: Value =
            serde_json::from_slice(&std::fs::read(&controller_path).unwrap()).unwrap();
        if recovered["pending_commits"].as_array().unwrap().is_empty() {
            assert!(recovered["epoch"].as_u64().unwrap() > committed["epoch"].as_u64().unwrap());
            assert_eq!(
                recovered["next_attempt"], committed["next_attempt"],
                "committed recovery must not start another candidate"
            );
            break;
        }
        assert!(process.0.try_wait().unwrap().is_none());
        assert!(
            tokio::time::Instant::now() < deadline,
            "commit outbox did not drain"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while state.calls.load(Ordering::SeqCst) <= before_restart + 2 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        published(&mut process, root.path(), &origin, &state.blocks[2].1).await,
        third
    );
    let mut current = EnhancePirClient::connect(&origin).await.unwrap();
    assert_answers(&mut current, &expected).await;
    assert_answers(&mut retained, &expected).await;
    assert_eq!(
        retained
            .query_position_with_timing(1)
            .await
            .unwrap()
            .0
            .as_ref(),
        expected[1]
    );
    drop(process);
    rpc_task.abort();
    for worker in workers {
        worker.abort();
    }
}
