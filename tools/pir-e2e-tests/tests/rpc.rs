//! Actual canonical-mode CLI ingestion against a local trusted-RPC test double.
//! The public transaction is real; the block envelope and the mixed
//! transactions built from its actions are synthetic, not consensus-valid.
#[path = "support/serving.rs"]
mod serving;
mod support;
// The offline journal repair shares the RPC double; producer fee derivation is
// tested with the producer in the shared server chain adapters.
#[path = "rpc/rebuild.rs"]
mod rebuild;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use enhance_query_server::{client::EnhancePirClient, protocol::Manifest, ACTIVATION_HEIGHT};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    path::Path,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

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
        "getblock" => match request["params"][1].as_u64().unwrap() {
            0 => {
                assert_eq!(request["params"][0], ACTIVATION_HEIGHT.to_string());
                json!(raw)
            }
            2 => {
                assert_eq!(request["params"][0], *hash);
                json!({"trees":{"ironwood":{"size":state.tree_size.load(Ordering::SeqCst)}}})
            }
            _ => panic!("unexpected verbosity"),
        },
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
            enhance_query_server::EnhanceRecord::from_bytes(answer.as_ref().try_into().unwrap())
                .unwrap();
        support::assert_fixture_action(&record, position as usize % 2);
        assert_eq!(record.metadata().fee_zatoshis(), Some(20_000), "{position}");
        let flags = record.as_bytes()[enhance_query_server::RECORD_FLAGS_OFFSET];
        assert_eq!(flags & enhance_query_server::FLAG_HAS_TRANSPARENT_INPUTS, 1);
        assert_eq!(
            flags & enhance_query_server::FLAG_HAS_TRANSPARENT_OUTPUTS != 0,
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
fn start(root: &Path, address: &str, rpc: &str, roles: &serving::Roles) -> Process {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("coordinator.log"))
        .unwrap();
    let config = root.join("publisher.json");
    std::fs::write(
        &config,
        roles
            .publisher_config(root, address, rpc)
            .unwrap()
            .to_string(),
    )
    .unwrap();
    Process(
        Command::new(env!("CARGO_BIN_EXE_pir-publisher-test"))
            .arg("--config")
            .arg(config)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    )
}
async fn published(
    process: &mut Process,
    root: &Path,
    origin: &str,
    hash: &str,
    roles: &serving::Roles,
) -> Manifest {
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
        roles.synchronize().await;
        if let Ok(response) = client.get(format!("{origin}/v1/enhance/init")).send().await {
            if let Ok(manifest) = response.json::<Manifest>().await {
                if manifest.anchor_block_hash == hash && roles.accepts(&manifest) {
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
    let mut blocks = Vec::new();
    for nonce in [0, 1, 2] {
        // The public transaction, then the synthetic ones, in an envelope the parser
        // accepts; consensus is trusted to RPC.
        let transactions = [
            tx.clone(),
            same_block.bytes(),
            mixed[0].bytes(),
            mixed[1].bytes(),
        ];
        blocks.push(zakura_server_chain::testing::block_from_raw(
            &transactions,
            nonce,
        ));
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
            earlier.txid_display(),
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
    std::fs::write(root.path().join("cookie"), "test:test").unwrap();
    let roles = serving::Roles::for_process(&root.path().join("roles"))
        .await
        .unwrap();
    let address = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string();
    let mut process = start(root.path(), &address, &rpc_url, &roles);
    let first = published(
        &mut process,
        root.path(),
        &roles.origin,
        &state.blocks[0].1,
        &roles,
    )
    .await;
    let mut client = EnhancePirClient::connect(&roles.origin).await.unwrap();
    assert_answers(&mut client, &expected).await;
    let size = || {
        serde_json::from_slice::<Value>(
            &std::fs::read(root.path().join("data/enhance/manifest.json")).unwrap(),
        )
        .unwrap()["tree_size"]
            .as_u64()
            .unwrap()
    };
    assert_eq!(size(), 6);
    drop(process);
    let mut process = start(root.path(), &address, &rpc_url, &roles);
    let restarted = published(
        &mut process,
        root.path(),
        &roles.origin,
        &state.blocks[0].1,
        &roles,
    )
    .await;
    assert!(restarted.generation > first.generation);
    assert_eq!(size(), 6, "restart must not duplicate records");
    state.selected.store(1, Ordering::SeqCst);
    state.tree_size.store(7, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(10), async {
        while size() != 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    roles.synchronize().await;
    assert!(
        !reqwest::get(format!("{}/v1/enhance/init", roles.origin))
            .await
            .unwrap()
            .status()
            .is_success(),
        "incomplete source cannot reactivate publication"
    );
    state.tree_size.store(6, Ordering::SeqCst);
    let next = published(
        &mut process,
        root.path(),
        &roles.origin,
        &state.blocks[1].1,
        &roles,
    )
    .await;
    assert!(next.generation > restarted.generation);
    assert_eq!(size(), 6);
    assert_answers(&mut client, &expected).await;
    rpc_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collection_commits_survive_withdrawal_restart_and_reorg_without_consumers() {
    use enhance_publisher::{
        application::{collect, CanonicalSource},
        chain_source::{Prevouts, ZakuraClient},
        ingest::EnhanceJournal,
        publisher::EnhancePublisher,
    };
    let tx = hex::decode(include_str!("fixtures/ironwood-fee-expiry.hex").trim()).unwrap();
    let state = Rpc {
        blocks: Arc::new(vec![
            zakura_server_chain::testing::block_from_raw(std::slice::from_ref(&tx), 0),
            zakura_server_chain::testing::block_from_raw(&[tx], 1),
        ]),
        transactions: Arc::new(HashMap::new()),
        selected: Arc::new(AtomicUsize::new(0)),
        calls: Arc::new(AtomicUsize::new(0)),
        tree_size: Arc::new(AtomicUsize::new(2)),
    };
    let (url, node) = serve(
        Router::new()
            .route("/", post(rpc))
            .with_state(state.clone()),
    )
    .await;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("cookie"), "test:test").unwrap();
    let rpc = ZakuraClient::from_cookie_file(&url, root.path().join("cookie")).unwrap();
    let source = CanonicalSource {
        rpc: rpc.clone(),
        prevouts: Prevouts::new(100),
    };
    let publisher_root = root.path().join("publication");
    let publisher = EnhancePublisher::open(&publisher_root, rpc.clone()).unwrap();
    let mut journal = EnhanceJournal::open(root.path()).unwrap();
    let work = collect(&mut journal, &publisher, &source)
        .await
        .unwrap()
        .unwrap();
    publisher.revoke_after(0).unwrap();
    assert!(publisher
        .publish(work, pir_publisher::publisher::PublisherEngine)
        .await
        .is_err());
    assert_eq!(
        journal.tree_size(),
        2,
        "publication refusal preserves acquisition"
    );
    let expected =
        hex::decode(include_str!("fixtures/ironwood-canonical-record-0.hex").trim()).unwrap();
    assert_eq!(
        journal.records().read_records(0, 1).unwrap(),
        expected[84..]
    );
    drop(journal);
    drop(publisher);
    let publisher = EnhancePublisher::open(&publisher_root, rpc).unwrap();
    let mut journal = EnhanceJournal::open(root.path()).unwrap();
    let work = collect(&mut journal, &publisher, &source)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(journal.tree_size(), 2);
    publisher
        .publish(work, pir_publisher::publisher::PublisherEngine)
        .await
        .unwrap();
    state.selected.store(1, Ordering::SeqCst);
    state.tree_size.store(3, Ordering::SeqCst);
    assert!(collect(&mut journal, &publisher, &source).await.is_err());
    assert_eq!(journal.tree_size(), 0);
    state.tree_size.store(2, Ordering::SeqCst);
    let work = collect(&mut journal, &publisher, &source)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(journal.tree_size(), 2);
    publisher
        .publish(work, pir_publisher::publisher::PublisherEngine)
        .await
        .unwrap();
    assert_eq!(journal.highest_committed().unwrap().1, state.blocks[1].1);
    node.abort();
}
