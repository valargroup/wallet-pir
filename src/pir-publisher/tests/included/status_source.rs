//! Status source regressions. Compiled into status-publisher only with
//! `fixtures`, so the cases can see the private checkpoint cache. The host
//! enables that feature; a plain `cargo test -p status-publisher` does not.
use super::*;
use axum::{extract::State, routing::post, Json, Router};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, RwLock,
};

#[derive(Clone)]
struct Mock {
    block_hex: String,
    hash: String,
    mempool_id: Arc<RwLock<String>>,
    tip_reads: Arc<AtomicUsize>,
    move_tip: bool,
    bad_block_hash: bool,
}

async fn rpc(State(mock): State<Mock>, Json(input): Json<Value>) -> Json<Value> {
    let result = match input["method"].as_str().unwrap() {
        "getblockcount" => {
            let n = mock.tip_reads.fetch_add(1, Ordering::SeqCst);
            json!(if mock.move_tip && n > 0 { 2 } else { 1 })
        }
        "getblockhash" => {
            if mock.bad_block_hash && input["params"][0].as_u64() == Some(1) {
                json!("00".repeat(32))
            } else {
                json!(mock.hash)
            }
        }
        "getblock" => json!(mock.block_hex),
        "getrawmempool" => json!([mock.mempool_id.read().unwrap().clone()]),
        method => panic!("unexpected RPC {method}"),
    };
    Json(json!({"result":result,"error":null,"id":"enhance-pir"}))
}

pub(crate) async fn fixture(
    move_tip: bool,
    bad_block_hash: bool,
) -> (ZakuraClient, tokio::task::JoinHandle<()>) {
    let (client, job, _) = controlled_fixture(move_tip, bad_block_hash).await;
    (client, job)
}

pub(crate) async fn controlled_fixture(
    move_tip: bool,
    bad_block_hash: bool,
) -> (
    ZakuraClient,
    tokio::task::JoinHandle<()>,
    Arc<RwLock<String>>,
) {
    let (block_hex, hash, mempool_id) = zakura_server_chain::testing::status::regtest_genesis();
    let mempool_id = Arc::new(RwLock::new(mempool_id));
    let control = mempool_id.clone();
    let mock = Mock {
        block_hex,
        hash,
        mempool_id,
        tip_reads: Arc::new(AtomicUsize::new(0)),
        move_tip,
        bad_block_hash,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let job = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/", post(rpc)).with_state(mock),
        )
        .await
        .unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let cookie = dir.path().join("cookie");
    std::fs::write(&cookie, "user:password").unwrap();
    let client = ZakuraClient::from_cookie_file(format!("http://{addr}"), cookie).unwrap();
    (client, job, control)
}

#[test]
fn compact_wire_preserves_stsobs01_checkpoint_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = vec![Block {
        height: 10,
        hash: [3; 32],
        parent: [2; 32],
        txids: vec![[4; 32]],
    }];
    let forks = vec![ForkRecord {
        txid: [5; 32],
        height: 10,
        block: [6; 32],
    }];
    {
        let source = RollingWindow::open(dir.path(), [1; 32], 1).unwrap();
        source.persist([9; 32], &blocks, &forks).unwrap();
    }
    let bytes = std::fs::read(dir.path().join("source.bin")).unwrap();
    assert_eq!(&bytes[..8], b"STSOBS01");
    let mut legacy = b"STSOBS01".to_vec();
    legacy.extend_from_slice(&1u32.to_le_bytes());
    legacy.extend_from_slice(&[1; 32]);
    legacy.extend_from_slice(&[9; 32]);
    legacy.extend_from_slice(&1u32.to_le_bytes());
    legacy.extend_from_slice(&10u32.to_le_bytes());
    legacy.extend_from_slice(&[3; 32]);
    legacy.extend_from_slice(&[2; 32]);
    legacy.extend_from_slice(&1u32.to_le_bytes());
    legacy.extend_from_slice(&[4; 32]);
    legacy.extend_from_slice(&1u32.to_le_bytes());
    legacy.extend_from_slice(&[5; 32]);
    legacy.extend_from_slice(&10u32.to_le_bytes());
    legacy.extend_from_slice(&[6; 32]);
    assert_eq!(bytes, legacy);
    let recovered = RollingWindow::open(dir.path(), [1; 32], 1).unwrap();
    assert!(recovered.blocks == blocks);
    assert_eq!(recovered.forks, forks);
    recovered.persist([9; 32], &blocks, &forks).unwrap();
    assert_eq!(std::fs::read(dir.path().join("source.bin")).unwrap(), bytes);
}
#[tokio::test]
async fn canonical_observation_indexes_every_transaction_type() {
    let (client, job) = fixture(false, false).await;
    let expected = zakura_server_chain::hash::parse_display(
        &zakura_server_chain::testing::status::regtest_genesis().2,
    )
    .unwrap();
    assert_eq!(client.status_mempool().await.unwrap(), vec![expected]);
    let observed = observe(&client, [1; 32], 1).await.unwrap();
    assert_eq!(observed.snapshot.height, 1);
    assert_eq!(observed.snapshot.start, 1);
    assert_eq!(observed.blocks, 1);
    assert_eq!(observed.mempool, 1);
    assert!(observed.snapshot.entries > 0);
    job.abort();
}

#[tokio::test]
async fn checkpoint_recovers_only_as_a_cache_and_reobserves_canonical_source() {
    let dir = tempfile::tempdir().unwrap();
    let (client, job) = fixture(false, false).await;
    let first_digest;
    {
        let mut window = RollingWindow::open(dir.path(), [1; 32], 1).unwrap();
        first_digest = window.observe(&client).await.unwrap().snapshot.digest;
        assert!(dir.path().join("source.bin").exists());
    }
    let mut recovered = RollingWindow::open(dir.path(), [1; 32], 1).unwrap();
    assert_eq!(recovered.blocks.len(), 1);
    assert!(recovered.network.is_some());
    // No observation timestamp or served generation is recovered.
    assert_eq!(
        recovered.observe(&client).await.unwrap().snapshot.digest,
        first_digest
    );
    job.abort();
}

#[tokio::test]
async fn live_rpc_candidate_revalidates_before_durable_publication() {
    use crate::authority::Authority;
    use status_query_server::Controller;
    use status_shared::Generation;

    let dir = tempfile::tempdir().unwrap();
    let (client, job) = fixture(false, false).await;
    let mut source = RollingWindow::open(dir.path().join("source"), [1; 32], 1).unwrap();
    let observed = source.observe(&client).await.unwrap();
    let mut authority = Authority::open(
        dir.path().join("authority"),
        observed.snapshot.network,
        [1; 32],
    )
    .unwrap();
    // Recovery epochs belong to the shared publication authority.
    let (number, epoch) = (authority.next_generation().unwrap(), 1);
    let (candidate, _) = Generation::prepare(
        &observed.snapshot,
        number,
        epoch,
        observed.observed_ms,
        None,
    )
    .unwrap();
    let verified = source.observe(&client).await.unwrap();
    assert_eq!(verified.snapshot.digest, candidate.manifest().rows_digest);
    assert_eq!(verified.snapshot.anchor, candidate.manifest().anchor_hash);
    let candidate = candidate.reobserved(verified.observed_ms);
    authority.commit(candidate.manifest()).unwrap();
    let controller = Controller::new(candidate);
    assert_eq!(controller.current().manifest().generation, 1);
    assert_eq!(controller.current().manifest().recovery_epoch, 1);
    drop(authority);
    let restarted = Authority::open(
        dir.path().join("authority"),
        verified.snapshot.network,
        [1; 32],
    )
    .unwrap();
    assert_eq!(restarted.next_generation().unwrap(), 2);
    job.abort();
}

#[test]
fn damaged_or_incompatible_checkpoint_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("source.bin"), b"incomplete").unwrap();
    assert!(matches!(
        RollingWindow::open(dir.path(), [1; 32], 1),
        Err(SourceError::Checkpoint)
    ));
}

#[tokio::test]
async fn changing_tip_discards_candidate() {
    let (client, job) = fixture(true, false).await;
    let mut window = RollingWindow::new([1; 32], 1).unwrap();
    window.blocks.push(Block {
        height: 1,
        hash: [9; 32],
        parent: [0; 32],
        txids: vec![[8; 32]],
    });
    assert!(matches!(
        window.observe(&client).await,
        Err(SourceError::TipChanged)
    ));
    assert_eq!(window.blocks[0].hash, [9; 32]);
    assert!(window.forks.is_empty());
    job.abort();
}

#[tokio::test]
async fn disconnected_block_becomes_fork_observation() {
    let (client, job) = fixture(false, false).await;
    let mut window = RollingWindow::new([1; 32], 1).unwrap();
    window.blocks.push(Block {
        height: 1,
        hash: [9; 32],
        parent: [0; 32],
        txids: vec![[8; 32]],
    });
    window.forks.push(ForkRecord {
        txid: [8; 32],
        height: 1,
        block: [7; 32],
    });
    let observed = window.observe(&client).await.unwrap();
    assert_eq!(observed.retained_forks, 1);
    assert_eq!(window.forks[0].txid, [8; 32]);
    assert_eq!(window.forks[0].block, [9; 32]);
    assert_eq!(observed.snapshot.entries, 2);
    job.abort();
}

#[tokio::test]
async fn noncanonical_raw_block_is_rejected() {
    let (client, job) = fixture(false, true).await;
    assert!(matches!(
        observe(&client, [1; 32], 1).await,
        Err(SourceError::Rpc(_))
    ));
    job.abort();
}
