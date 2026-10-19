//! Status wallet lifecycle. Compiled into status-publisher with `fixtures`
//! and native packing, where it can drive the private publication task.
use super::*;
use pir_query_server::status::Service;
#[allow(dead_code)]
mod transport {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../pir-query-server/tests/support/status_http.rs"
    ));
}
use transport::Loopback;
use zakura_pir_client::status as wallet;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_status_lifecycle_collects_publishes_and_advances_over_real_roles() {
    // Exercise full production geometry with a bounded four-CPU preparation
    // lane, as the Transparent unit runner does. A child confines the existing
    // role constructor's environment setting; concurrent tests never see it.
    // The outer case budget includes child startup, work and teardown.
    const CHILD: &str = "WALLET_PIR_STATUS_LIFECYCLE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = tokio::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "publisher::lifecycle_wallet_tests::shared_status_lifecycle_collects_publishes_and_advances_over_real_roles"])
                .env(CHILD, "1").env("STATUS_PREPARATION_THREADS", "4")
                .kill_on_drop(true).output().await.unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let (rpc, source_task) = source::tests::fixture(false, false).await;
    let (tx, rx) = watch::channel(None);
    let mut input = ObservationTask {
        source: source::RollingWindow::open(root.path().join("source"), [1; 32], 1).unwrap(),
        rpc: rpc.clone(),
        tx,
    };
    pir_pipeline::Task::run(&mut input)
        .await
        .map_err(|_| "source collection")
        .unwrap();
    let observed = rx.borrow().clone().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let publisher_origin = format!("http://{}", listener.local_addr().unwrap());
    let worker_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let worker_origin = format!("http://{}", worker_listener.local_addr().unwrap());
    let router_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let router_origin = format!("http://{}", router_listener.local_addr().unwrap());
    let publisher = Publisher::new(Config {
        state_dir: root.path().join("publisher"),
        salt_hex: hex::encode([1; 32]),
        window_blocks: 1,
        worker_origin: worker_origin.clone(),
        router_origin: router_origin.clone(),
        private_listen: listener.local_addr().unwrap(),
    })
    .unwrap();
    let app = publisher.private_routes();
    let publisher_task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let worker = Service::open(
        Role::Worker,
        observed.snapshot.network,
        root.path().join("worker"),
        publisher_origin.clone(),
        worker_origin.clone(),
        false,
    )
    .unwrap();
    let router = Service::open(
        Role::Router,
        observed.snapshot.network,
        root.path().join("router"),
        worker_origin.clone(),
        worker_origin,
        false,
    )
    .unwrap();
    let worker_routes = worker.routes();
    let router_routes = router.routes();
    let worker_task =
        tokio::spawn(async move { axum::serve(worker_listener, worker_routes).await.unwrap() });
    let router_task =
        tokio::spawn(async move { axum::serve(router_listener, router_routes).await.unwrap() });
    // Reuse RAII listener cleanup from the existing independent-client test.
    let _services = [
        transport::Listening {
            origin: publisher_origin.clone(),
            task: publisher_task,
        },
        transport::Listening {
            origin: String::new(),
            task: worker_task,
        },
        transport::Listening {
            origin: String::new(),
            task: router_task,
        },
        transport::Listening {
            origin: String::new(),
            task: source_task,
        },
    ];
    let mut authority = authority::Authority::open(
        root.path().join("authority"),
        observed.snapshot.network,
        [1; 32],
    )
    .unwrap();
    let bindings = publisher
        .fence_roles(authority.next_identity().unwrap().1)
        .await
        .unwrap();
    let first = publisher
        .publish(&observed, &mut authority, &bindings, &rpc)
        .await
        .unwrap();
    assert!(root.path().join("source/source.bin").exists());
    // Wallets reach the router role; the publisher has no wallet routes.
    let transport = Loopback {
        origin: router_origin,
        http: reqwest::Client::new(),
        corrupt_answer: AtomicBool::new(false),
    };
    let anchor = wallet::AcceptedAnchor {
        network: first.network,
        height: first.anchor_height,
        hash: first.anchor_hash,
    };
    let coverage = wallet::LocalCoverageContext {
        earliest_possible_inclusion: Some(1),
        required_through: Some(1),
    };
    let txid = zakura_server_chain::hash::parse_display(
        &zakura_server_chain::testing::status::regtest_genesis().2,
    )
    .unwrap();
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
        client.observe(&transport, &txid, coverage).await.unwrap(),
        wallet::Observation::Mined(1)
    );
    pir_pipeline::Task::run(&mut input)
        .await
        .map_err(|_| "next source collection")
        .unwrap();
    let next_observed = rx.borrow().clone().unwrap();
    let next = publisher
        .publish(&next_observed, &mut authority, &bindings, &rpc)
        .await
        .unwrap();
    assert_eq!(next.generation, first.generation + 1);
    assert_eq!(next.rows_digest, first.rows_digest);
    assert_eq!(next.observed_ms, next_observed.observed_ms);
    let current = wallet::transport::PendingClient::fetch(
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
        current.observe(&transport, &txid, coverage).await.unwrap(),
        wallet::Observation::Mined(1)
    );
    // Keep the original real HTTP wallet regression's malformed and
    // independent synthetic observation cases unchanged in its own target.
}
