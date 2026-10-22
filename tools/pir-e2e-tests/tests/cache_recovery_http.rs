//! Rows lost between worker processes are downloaded and verified again.
use enhance_shared::protocol::digest;
use pir_e2e_tests::enhance_fixture::{record, serve, System};
use pir_query_server::enhance::worker_sync::{Config, ReadinessRequest, Worker};
use sha2::{Digest, Sha256};
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_repairs_corrupt_cached_rows_before_readiness() {
    let system = System::start((0..67).map(record).collect()).await;
    let root = system.root.path().join("recovering-worker");
    let config = Config {
        source: system.source.clone(),
        shards: [0].into(),
    };
    let worker = Worker::open(
        &root,
        config.clone(),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    let mut sync = worker.synchronizer().unwrap();
    sync.poll_once().await.unwrap();
    let object = std::fs::read_dir(root.join("objects"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let original = std::fs::read(&object).unwrap();
    drop(sync);
    drop(worker);
    // Promoted objects are read-only; corruption comes from outside the cache.
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&object, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::write(&object, [0u8]).unwrap();
    let worker = Worker::open(&root, config, Default::default(), Default::default()).unwrap();
    let mut sync = worker.synchronizer().unwrap();
    let listening = serve(worker.router()).await;
    let http = pir_query_server::enhance::internal_auth::client_builder()
        .build()
        .unwrap();
    let request = ReadinessRequest {
        manifest_sha256: digest(&system.manifest),
        holder: None,
    };
    let url = format!("{}/internal/readiness", listening.url);
    assert!(!http
        .post(&url)
        .json(&request)
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    sync.poll_once().await.unwrap();
    assert!(http
        .post(&url)
        .json(&request)
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    let repaired = std::fs::read(&object).unwrap();
    assert_eq!(repaired, original);
    assert_eq!(
        object.file_name().unwrap().to_str().unwrap(),
        hex::encode(Sha256::digest(&repaired))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfers_interrupted_under_digest_part_names_do_not_exhaust_the_cache() {
    let system = System::start((0..67).map(record).collect()).await;
    let measured = system.root.path().join("measured-worker");
    let worker = Worker::open(
        &measured,
        Config {
            source: system.source.clone(),
            shards: [0].into(),
        },
        Default::default(),
        Default::default(),
    )
    .unwrap();
    worker.synchronizer().unwrap().poll_once().await.unwrap();
    let required: u64 = std::fs::read_dir(measured.join("objects"))
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum();
    drop(worker);

    // A full-budget file left by an interrupted transfer of the earlier layout.
    let root = system.root.path().join("restarted-worker");
    std::fs::create_dir_all(root.join("objects")).unwrap();
    let interrupted = root
        .join("objects")
        .join(format!("{}.part", "ab".repeat(32)));
    std::fs::write(&interrupted, vec![0u8; required as usize]).unwrap();
    let mut source = system.source.clone();
    source.cache_limit_bytes = required;
    let worker = Worker::open(
        &root,
        Config {
            source,
            shards: [0].into(),
        },
        Default::default(),
        Default::default(),
    )
    .unwrap();
    worker.synchronizer().unwrap().poll_once().await.unwrap();
    assert!(!interrupted.exists());
    for entry in std::fs::read_dir(root.join("objects")).unwrap() {
        let path = entry.unwrap().path();
        assert_eq!(
            path.file_name().unwrap().to_str().unwrap(),
            hex::encode(Sha256::digest(std::fs::read(&path).unwrap()))
        );
    }
}
