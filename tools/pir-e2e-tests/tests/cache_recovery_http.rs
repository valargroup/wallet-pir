//! Rows lost between worker processes are downloaded and verified again.
use enhance_query_server::{
    protocol::digest,
    worker_sync::{Config, ReadinessRequest, Worker},
};
use pir_e2e_tests::enhance_fixture::{record, serve, System};
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_repairs_corrupt_cached_rows_before_readiness() {
    let system = System::start((0..67).map(record).collect()).await;
    let certificate = reqwest::Certificate::from_pem(
        &std::fs::read(system.source.trusted_ca.as_ref().unwrap()).unwrap(),
    )
    .unwrap();
    let publisher = reqwest::Client::builder()
        .add_root_certificate(certificate)
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap();
    let url = format!(
        "{}{}authority",
        system.source.publisher_origin,
        enhance_query_server::publication::PREFIX
    );
    let expected = enhance_query_server::publication::manifest_checksum(&system.manifest);
    let response = publisher
        .post(&url)
        .json(&enhance_query_server::publication::AuthorityRequest {
            manifest_checksum: expected.clone(),
        })
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let response: enhance_query_server::publication::AuthorityResponse =
        response.json().await.unwrap();
    assert_eq!(response.authorized_manifest_checksum, Some(expected));
    // The existing wallet manifest identity cannot authorize this internal API.
    let response = publisher
        .post(&url)
        .json(&enhance_query_server::publication::AuthorityRequest {
            manifest_checksum: pir_artifact::Checksum {
                algorithm: pir_artifact::Algorithm::Sha256,
                digest: digest(&system.manifest),
            },
        })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
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
    let object = std::fs::read_dir(root.join(pir_artifact::namespace("objects")))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let original = std::fs::read(&object).unwrap();
    drop(sync);
    drop(worker);
    std::fs::write(&object, [0u8]).unwrap();
    let worker = Worker::open(&root, config, Default::default(), Default::default()).unwrap();
    let mut sync = worker.synchronizer().unwrap();
    let listening = serve(worker.router()).await;
    let http = enhance_query_server::internal_auth::client_builder()
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
        pir_artifact::digest(&repaired)
    );
}
