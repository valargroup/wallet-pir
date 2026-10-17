//! Independent wallet client over real Status coordinator/router/worker listeners.
#![cfg(feature = "native-reinspiring")]
use enhance_pir_server::status::{fixture, now_ms, Controller, Generation};
use ipir_sp::server::MatvecBackend;
use std::sync::atomic::{AtomicBool, Ordering};
use zakura_pir_status as wallet;

#[path = "support/status_http.rs"]
mod status_http;
use status_http::{serve, Loopback};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_status_wallet_decrypts_http_and_rejects_invalid_envelopes() {
    let snapshot = fixture::snapshot(32, false).unwrap();
    let (generation, _) =
        Generation::prepare(&snapshot, 1, 1, now_ms(), None, MatvecBackend::Cpu).unwrap();
    // Preparation may outlast the observation window on a busy host. This
    // immutable synthetic snapshot is still verified immediately before publish.
    let controller = Controller::new(generation.reobserved(now_ms()));
    let worker = serve(controller.worker_routes()).await;
    let router = serve(controller.router_routes(worker.origin.clone())).await;
    let coordinator = serve(controller.coordinator_routes(router.origin.clone())).await;
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap();
    let transport = Loopback {
        origin: coordinator.origin.clone(),
        http: http.clone(),
        corrupt_answer: AtomicBool::new(false),
    };
    let anchor = wallet::AcceptedAnchor {
        network: snapshot.network,
        height: snapshot.height,
        hash: snapshot.anchor,
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
    let coverage = wallet::LocalCoverageContext {
        earliest_possible_inclusion: Some(snapshot.start),
        required_through: Some(snapshot.height),
    };
    for (txid, expected) in fixture::cases(32, false) {
        let observation = client.observe(&transport, &txid, coverage).await.unwrap();
        let expected = match expected {
            enhance_pir::status::Observation::Mined(height) => wallet::Observation::Mined(height),
            enhance_pir::status::Observation::Mempool => wallet::Observation::Mempool,
            enhance_pir::status::Observation::Forked => wallet::Observation::Forked,
            enhance_pir::status::Observation::NotFound => wallet::Observation::NotFound,
        };
        assert_eq!(observation, expected);
    }
    transport.corrupt_answer.store(true, Ordering::SeqCst);
    assert_eq!(
        client
            .observe(&transport, &fixture::txid(0), coverage)
            .await,
        Err(wallet::Error::Malformed)
    );
    let malformed = http
        .post(format!("{}/v1/status/query", coordinator.origin))
        .body(vec![0; 8])
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), reqwest::StatusCode::BAD_REQUEST);
    let mut stale = enhance_pir::status::QUERY_MAGIC.to_vec();
    stale.resize(enhance_pir::status::HEADER_BYTES, 0);
    let stale = http
        .post(format!("{}/v1/status/query", coordinator.origin))
        .body(stale)
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), reqwest::StatusCode::CONFLICT);
}
