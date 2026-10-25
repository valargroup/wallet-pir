use super::*;
use axum::{
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use status_shared::Generation;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
enum Behavior {
    Expiring,
    FreshUnavailable,
    AlwaysUnavailable,
    WrongAnswer,
    Unchanged,
}

#[derive(Clone)]
struct Server {
    old: Arc<Generation>,
    new: Arc<Generation>,
    behavior: Behavior,
    init_count: Arc<AtomicUsize>,
    query_count: Arc<AtomicUsize>,
}

impl Server {
    fn manifest(&self) -> &Manifest {
        if matches!(self.behavior, Behavior::Unchanged)
            || (matches!(
                self.behavior,
                Behavior::Expiring | Behavior::AlwaysUnavailable
            ) && self.old.manifest().fresh(status::now_ms()).is_ok())
        {
            self.old.manifest()
        } else {
            self.new.manifest()
        }
    }
}

async fn init(State(server): State<Server>) -> Json<Manifest> {
    server.init_count.fetch_add(1, Ordering::SeqCst);
    Json(server.manifest().clone())
}

async fn session(State(server): State<Server>) -> Vec<u8> {
    server.new.public().as_ref().clone()
}

async fn query(State(server): State<Server>, body: Bytes) -> Response {
    server.query_count.fetch_add(1, Ordering::SeqCst);
    let old = body.get(4..36) == Some(server.old.manifest().id().as_slice());
    if old {
        let remaining =
            (server.old.manifest().observed_ms + MAX_AGE_MS + 1).saturating_sub(status::now_ms());
        tokio::time::sleep(Duration::from_millis(remaining)).await;
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if matches!(
        server.behavior,
        Behavior::FreshUnavailable | Behavior::AlwaysUnavailable
    ) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let bytes = tokio::task::spawn_blocking(move || {
        let coefficients = server.new.coefficients(&body).unwrap();
        let mut values = server.new.evaluate(&coefficients).unwrap();
        if matches!(server.behavior, Behavior::WrongAnswer) {
            // A well-formed encrypted absence answer must still fail the
            // independent node's mined-transaction expectation.
            values.fill(0);
        }
        server.new.pack(&body, &values).unwrap()
    })
    .await
    .unwrap();
    bytes.into_response()
}

async fn serve(server: Server) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let job = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/v1/status/init", get(init))
                .route("/v1/status/session/:id", get(session))
                .route("/v1/status/query", post(query))
                .route(
                    "/unavailable",
                    get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
                )
                .with_state(server),
        )
        .await
        .unwrap();
    });
    (origin, job)
}

fn rows(path: &std::path::Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expiry_refresh_keeps_offered_queries_and_exposes_failed_attempts() {
    let (rpc, rpc_job, mempool) = crate::source::tests::controlled_fixture(false, false).await;
    *mempool.write().unwrap() = "77".repeat(32);
    let observed = crate::source::observe(&rpc, [1; 32], 1).await.unwrap();
    let prepared = tokio::task::spawn_blocking(move || {
        Generation::prepare(&observed.snapshot, 7, 0, status::now_ms(), None)
            .unwrap()
            .0
    })
    .await
    .unwrap();

    for behavior in [
        Behavior::Expiring,
        Behavior::FreshUnavailable,
        Behavior::AlwaysUnavailable,
        Behavior::WrongAnswer,
    ] {
        let now = status::now_ms();
        let server = Server {
            old: Arc::new(prepared.reobserved(now - MAX_AGE_MS + 250)),
            // Same-generation source reobservation is a legitimate successor.
            new: Arc::new(prepared.reobserved(now)),
            behavior,
            init_count: Arc::new(AtomicUsize::new(0)),
            query_count: Arc::new(AtomicUsize::new(0)),
        };
        let (origin, job) = serve(server.clone()).await;
        let dir = tempfile::tempdir().unwrap();
        let result = live_load(origin, rpc.clone(), 1, dir.path().to_owned()).await;
        let summary = rows(&dir.path().join("summary.jsonl")).remove(0);
        let requests = rows(&dir.path().join("requests.jsonl"));
        assert_eq!(summary["offered"], 20);
        assert_eq!(summary["unstarted"], 0);
        assert_eq!(requests.len(), 20);
        if matches!(behavior, Behavior::Expiring) {
            assert!(result.is_ok(), "{summary}");
            assert_eq!(summary["correct"], 20);
            assert!(summary["p99_ms"].as_f64().unwrap() <= 1000.);
            assert!(requests.iter().any(|row| row["stage_completions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|stage| stage["stage"] == "query" && stage["http_status"] == 503)));
            assert!(requests.iter().any(|row| row["stage_completions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|stage| stage["stage"] == "expired_session")));
        } else {
            assert!(result.is_err(), "{summary}");
            assert_eq!(summary["failed"], 20);
            if matches!(behavior, Behavior::WrongAnswer) {
                assert!(requests.iter().all(|row| row["stage_completions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|stage| stage["stage"] == "query" && stage["http_status"] == 200)));
            }
            // Fresh failures and wrong answers get no second encrypted query.
            if !matches!(behavior, Behavior::AlwaysUnavailable) {
                assert_eq!(server.init_count.load(Ordering::SeqCst), 20);
                assert_eq!(server.query_count.load(Ordering::SeqCst), 20);
            }
        }
        job.abort();
    }

    // A node failure is not a session expiry response, even if its latency
    // carries the selected manifest across the freshness boundary.
    let now = status::now_ms();
    let server = Server {
        old: Arc::new(prepared.reobserved(now - MAX_AGE_MS + 250)),
        new: Arc::new(prepared.reobserved(now)),
        behavior: Behavior::Expiring,
        init_count: Arc::new(AtomicUsize::new(0)),
        query_count: Arc::new(AtomicUsize::new(0)),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rpc_origin = format!("http://{}", listener.local_addr().unwrap());
    let oracle = Router::new()
        .route(
            "/",
            post(
                |State(old): State<Manifest>, Json(request): Json<serde_json::Value>| async move {
                    if request["params"][0] == 0 {
                        Json(
                            json!({"result":zakura_server_chain::hash::display(&old.network),
                    "error":null,"id":"enhance-pir"}),
                        )
                        .into_response()
                    } else {
                        let remaining =
                            (old.observed_ms + MAX_AGE_MS + 1).saturating_sub(status::now_ms());
                        tokio::time::sleep(Duration::from_millis(remaining)).await;
                        StatusCode::SERVICE_UNAVAILABLE.into_response()
                    }
                },
            ),
        )
        .with_state(server.old.manifest().clone());
    let oracle_job = tokio::spawn(async move {
        axum::serve(listener, oracle).await.unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let cookie = dir.path().join("cookie");
    std::fs::write(&cookie, "user:password").unwrap();
    let failing_rpc = ZakuraClient::from_cookie_file(rpc_origin, cookie).unwrap();
    let (origin, job) = serve(server.clone()).await;
    assert!(live_load(origin, failing_rpc, 1, dir.path().to_owned())
        .await
        .is_err());
    assert_eq!(rows(&dir.path().join("summary.jsonl"))[0]["failed"], 20);
    assert_eq!(server.init_count.load(Ordering::SeqCst), 20);
    assert_eq!(server.query_count.load(Ordering::SeqCst), 0);
    job.abort();
    oracle_job.abort();

    for change_network in [false, true] {
        let old = prepared.reobserved(status::now_ms() - MAX_AGE_MS - 1);
        let mut replacement = prepared.reobserved(status::now_ms()).manifest().clone();
        if change_network {
            replacement.network[0] ^= 1;
        } else {
            replacement.observed_ms = old.manifest().observed_ms;
            replacement.generation += 1;
        }
        let server = Server {
            old: Arc::new(old),
            new: Arc::new(Generation::public_only(
                replacement,
                prepared.public().clone(),
            )),
            behavior: Behavior::FreshUnavailable,
            init_count: Arc::new(AtomicUsize::new(0)),
            query_count: Arc::new(AtomicUsize::new(0)),
        };
        let (origin, job) = serve(server.clone()).await;
        let result = next_manifest(
            &reqwest::Client::new(),
            &origin,
            Some(server.old.manifest()),
            Instant::now(),
            1,
            &mut Vec::new(),
        )
        .await;
        assert!(
            result.is_err(),
            "new identity needs a newer compatible observation"
        );
        assert_eq!(server.init_count.load(Ordering::SeqCst), 1);
        assert_eq!(server.query_count.load(Ordering::SeqCst), 0);
        job.abort();
    }

    let stale = prepared.reobserved(status::now_ms() - MAX_AGE_MS - 1);
    let server = Server {
        old: Arc::new(stale.clone()),
        new: Arc::new(stale),
        behavior: Behavior::Unchanged,
        init_count: Arc::new(AtomicUsize::new(0)),
        query_count: Arc::new(AtomicUsize::new(0)),
    };
    let (origin, job) = serve(server.clone()).await;
    let http = reqwest::Client::new();
    let start = Instant::now();
    let mut stages = Vec::new();
    let expired = server.old.manifest();
    let result = tokio::time::timeout_at(
        tokio::time::Instant::now() + Duration::from_millis(75),
        next_manifest(&http, &origin, Some(expired), start, 1, &mut stages),
    )
    .await;
    assert!(
        result.is_err(),
        "unchanged expired identity must exhaust the existing deadline"
    );
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(server.query_count.load(Ordering::SeqCst), 0);

    // The identical HTTP failure from an oracle/init boundary is ineligible,
    // even when the selected session has expired meanwhile.
    let error: AnyError = http
        .get(format!("{origin}/unavailable"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap_err()
        .into();
    assert!(!expired_session(&error, expired, status::now_ms(), None));
    assert!(expired_session(
        &error,
        expired,
        status::now_ms(),
        Some(503)
    ));
    assert!(!expired_session(
        &error,
        prepared.reobserved(status::now_ms()).manifest(),
        status::now_ms(),
        Some(503)
    ));
    job.abort();
    rpc_job.abort();
}
