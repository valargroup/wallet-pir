//! Full native Status publication and wallet recovery through real TLS consumers.
use super::*;
use pir_query_server::status::serve_role_on;
use status_query_server::distributed::{Role, RoleConfig};
use std::sync::atomic::AtomicBool;
#[path = "../../../pir-query-server/tests/support/status_tls.rs"]
mod tls;
#[allow(dead_code)]
mod transport {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../pir-query-server/tests/support/status_http.rs"
    ));
}
use zakura_pir_client::status as wallet;
struct Abort(tokio::task::JoinHandle<()>);
impl Drop for Abort {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn wait_manifest(
    http: &reqwest::Client,
    origin: &str,
    previous_rows: Option<Hash>,
) -> Manifest {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    loop {
        if let Ok(response) = http.get(format!("{origin}/v1/status/init")).send().await {
            if response.status().is_success() {
                let manifest: Manifest = response.json().await.unwrap();
                if previous_rows != Some(manifest.rows_digest) {
                    return manifest;
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "HTTPS consumers did not activate the expected material"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_status_lifecycle_collects_publishes_and_advances_over_real_roles() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("warn")
        .with_test_writer()
        .try_init();
    let root = tempfile::tempdir().unwrap();
    let (rpc, source_task, mempool) = source::tests::controlled_fixture(false, false).await;
    let _source = Abort(source_task);
    *mempool.write().unwrap() = "77".repeat(32);
    let observed = source::observe(&rpc, [1; 32], 1).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let publisher = Publisher::new(Config {
        state_dir: root.path().join("publisher"),
        salt_hex: hex::encode([1; 32]),
        window_blocks: 1,
        private_listen: address,
    })
    .unwrap();
    let held = Arc::new(tokio::sync::Mutex::new(Some(listener)));
    let http = pir_pipeline::service::Http::new(move |_, routes| {
        let held = held.clone();
        async move { axum::serve(held.lock().await.take().unwrap(), routes).await }
    });
    let source_rpc = rpc.clone();
    let _publisher = Abort(tokio::spawn(async move {
        publisher
            .run_managed(source_rpc, http, pir_pipeline::service::Shutdown::default())
            .await
            .unwrap()
    }));
    // Start consumers after the source has a complete publication. Transport
    // outage and empty/restarted consumer behavior have separate focused tests.
    let ready = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if ready
            .get(format!("http://{address}/provider/status/v1/current"))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "publisher not ready"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let tls = tls::Tls::start(&format!("http://{address}")).await;
    let worker_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let worker_origin = format!("http://{}", worker_listener.local_addr().unwrap());
    let router_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let router_origin = format!("http://{}", router_listener.local_addr().unwrap());
    let config = |role, state, listen| RoleConfig {
        role,
        network_hex: hex::encode(observed.snapshot.network),
        state_dir: root.path().join(state),
        listen,
        query_listen: None,
        publisher: tls.origin.clone(),
        publisher_ca: Some(tls.certificate.clone()),
        worker_origin: worker_origin.clone(),
        cuda: false,
    };
    let worker = config(
        Role::Worker,
        "worker",
        worker_listener.local_addr().unwrap(),
    );
    let router = config(
        Role::Router,
        "router",
        router_listener.local_addr().unwrap(),
    );
    let _worker = Abort(tokio::spawn(async move {
        serve_role_on(worker, worker_listener, None).await.unwrap()
    }));
    let _router = Abort(tokio::spawn(async move {
        serve_role_on(router, router_listener, None).await.unwrap()
    }));
    let poll = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let first = wait_manifest(&poll, &router_origin, None).await;
    let transport = transport::Loopback {
        origin: router_origin.clone(),
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap(),
        corrupt_answer: AtomicBool::new(false),
    };
    let anchor = wallet::AcceptedAnchor {
        network: observed.snapshot.network,
        height: observed.snapshot.height,
        hash: observed.snapshot.anchor,
    };
    let local = wallet::LocalCoverageContext {
        earliest_possible_inclusion: Some(observed.snapshot.start),
        required_through: Some(observed.snapshot.height),
    };
    let client = wallet::transport::PendingClient::fetch(
        &transport,
        "https://status-wallet.invalid",
        now_ms,
    )
    .await
    .unwrap()
    .accept(&transport, &anchor)
    .await
    .unwrap();
    // Expected answers come from the node's complete mempool, independently of
    // the publisher's inventory and the consumer's prepared material.
    let txid = rpc.status_mempool().await.unwrap()[0];
    assert_eq!(
        client.observe(&transport, &txid, local).await.unwrap(),
        wallet::Observation::Mempool
    );
    assert_eq!(
        client
            .observe(&transport, &[0xfe; 32], local)
            .await
            .unwrap(),
        wallet::Observation::NotFound
    );
    *mempool.write().unwrap() = "88".repeat(32);
    let second = wait_manifest(&poll, &router_origin, Some(first.rows_digest)).await;
    assert!(second.generation > first.generation);
    assert!(second.observed_ms > first.observed_ms);
    let client = wallet::transport::PendingClient::fetch(
        &transport,
        "https://status-wallet.invalid",
        now_ms,
    )
    .await
    .unwrap()
    .accept(&transport, &anchor)
    .await
    .unwrap();
    assert_eq!(
        client
            .observe(&transport, &[0x88; 32], local)
            .await
            .unwrap(),
        wallet::Observation::Mempool
    );
    assert_eq!(
        client.observe(&transport, &txid, local).await.unwrap(),
        wallet::Observation::NotFound
    );
    assert!(root
        .path()
        .join("worker/publisher-high-water.json")
        .is_file());
    assert!(root
        .path()
        .join("router/publisher-high-water.json")
        .is_file());
}
