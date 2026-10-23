//! Independent wallet client over real Status router and worker listeners.
use super::Controller;
use super::Service;
use status_query_server::distributed::Role;
use status_shared::fixture;
use status_shared::{
    now_ms,
    protocol::{Observation, HEADER_BYTES, QUERY_MAGIC},
    Generation,
};
use std::path::Path as FsPath;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use zakura_pir_client::status as wallet;

use super::status_http::{serve, Loopback};

/// Install a verified fixture generation to exercise wallet request handling.
async fn activated(role: Role, dir: &FsPath, worker_origin: String, g: Generation) -> Service {
    let s = Service::open(role, g.manifest().network, dir.to_path_buf(), worker_origin).unwrap();
    s.state.lock().await.controller = Some(Controller::new(g));
    s
}

/// Accept the served anchor and check every fixture case through the
/// independent wallet client.
async fn observe_every_case(
    transport: &Loopback,
    snapshot: &status_shared::index::Snapshot,
) -> (
    wallet::transport::StatusPirClient<fn() -> u64>,
    wallet::LocalCoverageContext,
) {
    let anchor = wallet::AcceptedAnchor {
        network: snapshot.network,
        height: snapshot.height,
        hash: snapshot.anchor,
    };
    let client = wallet::transport::PendingClient::fetch(
        transport,
        "https://status-wallet.invalid",
        now_ms as fn() -> u64,
    )
    .await
    .unwrap()
    .accept(transport, &anchor)
    .await
    .unwrap();
    let coverage = wallet::LocalCoverageContext {
        earliest_possible_inclusion: Some(snapshot.start),
        required_through: Some(snapshot.height),
    };
    for (txid, expected) in fixture::cases(32, false) {
        let observation = client.observe(transport, &txid, coverage).await.unwrap();
        let expected = match expected {
            Observation::Mined(height) => wallet::Observation::Mined(height),
            Observation::Mempool => wallet::Observation::Mempool,
            Observation::Forked => wallet::Observation::Forked,
            Observation::NotFound => wallet::Observation::NotFound,
        };
        assert_eq!(observation, expected);
    }
    (client, coverage)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_status_wallet_decrypts_http_and_rejects_invalid_envelopes() {
    let snapshot = fixture::snapshot(32, false).unwrap();
    let (generation, _) = Generation::prepare(&snapshot, 1, 1, now_ms(), None).unwrap();
    // Preparation may outlast the observation window on a busy host. This
    // immutable synthetic snapshot is still verified immediately before publish.
    let generation = generation.reobserved(now_ms());
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let worker = activated(
        Role::Worker,
        dirs.0.path(),
        "http://127.0.0.1:1".into(),
        generation.clone(),
    )
    .await;
    let worker = serve(worker.control_routes()).await;
    let router = activated(
        Role::Router,
        dirs.1.path(),
        worker.origin.clone(),
        generation,
    )
    .await;
    let router = serve(router.query_routes()).await;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let transport = Loopback {
        origin: router.origin.clone(),
        http: http.clone(),
        corrupt_answer: AtomicBool::new(false),
    };
    let (client, coverage) = observe_every_case(&transport, &snapshot).await;
    transport.corrupt_answer.store(true, Ordering::SeqCst);
    assert_eq!(
        client
            .observe(&transport, &fixture::txid(0), coverage)
            .await,
        Err(wallet::Error::Malformed)
    );
    let malformed = http
        .post(format!("{}/v1/status/query", router.origin))
        .body(vec![0; 8])
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), reqwest::StatusCode::BAD_REQUEST);
    let mut stale = QUERY_MAGIC.to_vec();
    stale.resize(HEADER_BYTES, 0);
    let stale = http
        .post(format!("{}/v1/status/query", router.origin))
        .body(stale)
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), reqwest::StatusCode::CONFLICT);
}

/// One combined role evaluates and packs with direct calls: the same
/// independent wallet decrypts every case, and the worker's private
/// evaluation and handover routes stay unmounted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_status_wallet_decrypts_a_combined_role() {
    let snapshot = fixture::snapshot(32, false).unwrap();
    let (generation, _) = Generation::prepare(&snapshot, 1, 1, now_ms(), None).unwrap();
    let generation = generation.reobserved(now_ms());
    let id = hex::encode(generation.manifest().id());
    let dir = tempfile::tempdir().unwrap();
    let combined = activated(Role::Combined, dir.path(), String::new(), generation).await;
    let routes = serve(combined.routes()).await;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let transport = Loopback {
        origin: routes.origin.clone(),
        http: http.clone(),
        corrupt_answer: AtomicBool::new(false),
    };
    let (client, coverage) = observe_every_case(&transport, &snapshot).await;
    let mut header = QUERY_MAGIC.to_vec();
    header.extend(hex::decode(&id).unwrap());
    header.resize(HEADER_BYTES, 0);
    for path in [
        "/evaluate".to_string(),
        format!("/v1/provider-pin/{id}"),
        format!("/v1/provider-commit/{id}"),
    ] {
        let response = http
            .post(format!("{}{path}", routes.origin))
            .body(header.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND, "{path}");
    }
    // Revoked authority refuses new queries; nothing is answered from it.
    combined
        .state
        .lock()
        .await
        .controller
        .as_ref()
        .unwrap()
        .revoke();
    assert!(client
        .observe(&transport, &fixture::txid(0), coverage)
        .await
        .is_err());
    let init = http
        .get(format!("{}/status/v1/init", routes.origin))
        .send()
        .await
        .unwrap();
    assert_eq!(init.status(), reqwest::StatusCode::GONE);
    let health: serde_json::Value = http
        .get(format!("{}/control/health", routes.origin))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["role"], "combined");
    assert_eq!(health["ready"], false);
}

/// Records the wallet's query bodies while forwarding every request.
struct Capture<'a> {
    inner: &'a Loopback,
    queries: std::sync::Mutex<Vec<Vec<u8>>>,
}
impl wallet::transport::Transport for Capture<'_> {
    async fn get(&self, url: &str, max_bytes: usize) -> Result<Vec<u8>, wallet::Error> {
        wallet::transport::Transport::get(self.inner, url, max_bytes).await
    }
    async fn post(
        &self,
        url: &str,
        body: Vec<u8>,
        max_bytes: usize,
    ) -> Result<Vec<u8>, wallet::Error> {
        if url.ends_with("/query") {
            self.queries.lock().unwrap().push(body.clone());
        }
        wallet::transport::Transport::post(self.inner, url, body, max_bytes).await
    }
}

/// A cancelled combined query keeps its per-client slot until the admitted
/// evaluation and packing finish, then releases it. A request deadline drops
/// the handler future the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_combined_query_holds_its_client_slot_until_work_ends() {
    use super::admission::Admission;
    use super::serving::{query, Evaluator, HttpState};
    use axum::{body::Bytes, extract::State};
    use pir_control::admission::ClientSlots;
    use std::sync::{Arc, Barrier};

    let snapshot = fixture::snapshot(32, false).unwrap();
    let (generation, _) = Generation::prepare(&snapshot, 1, 1, now_ms(), None).unwrap();
    let generation = generation.reobserved(now_ms());
    let dir = tempfile::tempdir().unwrap();
    let combined = activated(
        Role::Combined,
        dir.path(),
        String::new(),
        generation.clone(),
    )
    .await;
    let routes = serve(combined.routes()).await;
    let transport = Loopback {
        origin: routes.origin.clone(),
        http: reqwest::Client::new(),
        corrupt_answer: AtomicBool::new(false),
    };
    let capture = Capture {
        inner: &transport,
        queries: Default::default(),
    };
    let anchor = wallet::AcceptedAnchor {
        network: snapshot.network,
        height: snapshot.height,
        hash: snapshot.anchor,
    };
    let client = wallet::transport::PendingClient::fetch(
        &capture,
        "https://status-wallet.invalid",
        now_ms as fn() -> u64,
    )
    .await
    .unwrap()
    .accept(&capture, &anchor)
    .await
    .unwrap();
    let coverage = wallet::LocalCoverageContext {
        earliest_possible_inclusion: Some(snapshot.start),
        required_through: Some(snapshot.height),
    };
    client
        .observe(&capture, &fixture::txid(0), coverage)
        .await
        .unwrap();
    let body = capture.queries.lock().unwrap().pop().unwrap();

    let slots = ClientSlots::new(1);
    let slot = slots.try_acquire("wallet").unwrap();
    let (started, release) = (Arc::new(Barrier::new(2)), Arc::new(Barrier::new(2)));
    let state = HttpState {
        controller: Controller::new(generation),
        evaluator: Evaluator::Local,
        permits: Arc::new(Admission::production()),
        blocking_started: Some(Arc::new({
            let (started, release) = (started.clone(), release.clone());
            move || {
                started.wait();
                release.wait();
            }
        })),
    };
    let request = tokio::spawn(query(State(state), Bytes::from(body), Some(slot)));
    tokio::task::spawn_blocking({
        let started = started.clone();
        move || started.wait()
    })
    .await
    .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert_eq!(slots.tracked(), 1, "cancellation released a running slot");
    assert!(slots.try_acquire("wallet").is_none());
    tokio::task::spawn_blocking(move || release.wait())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while slots.tracked() != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "slot never released"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(slots.try_acquire("wallet").is_some());
}
