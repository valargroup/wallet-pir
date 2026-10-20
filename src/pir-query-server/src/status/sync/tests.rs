use super::*;
use axum::{
    response::IntoResponse,
    routing::{get, post},
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    RwLock,
};
struct Origin {
    current: RwLock<Current>,
    canonical: AtomicBool,
}
#[path = "../../../tests/support/status_tls.rs"]
mod tls;

fn manifest() -> Manifest {
    Manifest {
        protocol: PROTOCOL.into(),
        network: [1; 32],
        salt: [2; 32],
        generation: 3,
        recovery_epoch: 2,
        coverage_start: 1,
        anchor_height: 1,
        anchor_hash: [3; 32],
        observed_ms: now_ms(),
        entries: 0,
        rows_digest: [4; 32],
        public_digest: [5; 32],
    }
}
fn pointer() -> Current {
    Current {
        schema: SCHEMA.into(),
        revision: 5,
        recovery_epoch: 2,
        network: [1; 32],
        salt: [2; 32],
        generation: 3,
        inventory: Some(Object::new(b"not an inventory")),
        eligible: true,
    }
}
async fn setup() -> (
    tempfile::TempDir,
    Service,
    StatusSync,
    Arc<Origin>,
    tls::Tls,
    tokio::task::JoinHandle<()>,
) {
    let root = tempfile::tempdir().unwrap();
    let current = Arc::new(Origin {
        current: RwLock::new(pointer()),
        canonical: AtomicBool::new(true),
    });
    let shared = current.clone();
    let anchors = current.clone();
    let app = Router::new()
        .route(
            &format!("{PREFIX}current"),
            get(move |headers: HeaderMap| {
                let shared = shared.clone();
                async move {
                    let c = shared.current.read().unwrap().clone();
                    let token = format!("\"{}-{}\"", c.revision, c.eligible);
                    if headers.get("if-none-match").and_then(|v| v.to_str().ok()) == Some(&token) {
                        StatusCode::NOT_MODIFIED.into_response()
                    } else {
                        Json(c).into_response()
                    }
                }
            }),
        )
        .route(
            &format!("{PREFIX}anchors/check"),
            post(move |Json(request): Json<AnchorRequest>| {
                let shared = anchors.clone();
                async move {
                    Json(AnchorResponse {
                        current: shared.current.read().unwrap().clone(),
                        canonical: vec![
                            shared.canonical.load(Ordering::SeqCst);
                            request.anchors.len()
                        ],
                    })
                }
            }),
        );
    let app = app.route(
        &format!("{PREFIX}objects/:id"),
        get(|| async { "not an inventory" }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let tls = tls::Tls::start(&origin).await;
    let config = RoleConfig {
        role: Role::Worker,
        network_hex: hex::encode([1; 32]),
        state_dir: root.path().join("worker"),
        listen: "127.0.0.1:1".parse().unwrap(),
        query_listen: None,
        publisher: tls.origin.clone(),
        publisher_ca: Some(tls.certificate.clone()),
        worker_origin: String::new(),
        cuda: false,
    };
    let service = Service::open(
        config.role,
        [1; 32],
        config.state_dir.clone(),
        String::new(),
    )
    .unwrap();
    service.state.lock().await.controller = Some(Controller::new(Generation::public_only(
        manifest(),
        Arc::new(vec![]),
    )));
    let task = StatusSync::new(service.clone(), &config).unwrap();
    (root, service, task, current, tls, server)
}
#[tokio::test]
async fn tls_withdrawal_fences_before_persisting_and_refuses_replay() {
    let (_root, service, mut task, pointer, _tls, server) = setup().await;
    let current = pointer.current.read().unwrap().clone();
    task.observe(current).await.unwrap();
    {
        let mut c = pointer.current.write().unwrap();
        c.revision += 1;
        c.eligible = false;
    }
    assert!(task.synchronize().await.is_err());
    assert!(service.serving().await.is_err());
    let saved: Floor =
        serde_json::from_slice(&fs::read(service.dir.join("publisher-high-water.json")).unwrap())
            .unwrap();
    assert!(!saved.current.unwrap().eligible);
    let mut replay = pointer.current.read().unwrap().clone();
    replay.eligible = true;
    assert!(task.observe(replay).await.is_err());
    server.abort();
}
#[tokio::test]
async fn unchanged_https_authority_never_renews_observation_age() {
    let (_root, service, mut task, _pointer, tls, server) = setup().await;
    assert!(task.synchronize().await.is_err()); // No prepared objects are available.
    assert!(service.serving().await.is_ok());
    let mut stale = manifest();
    stale.observed_ms = now_ms() - MAX_AGE_MS - 1;
    service.state.lock().await.controller = Some(Controller::new(Generation::public_only(
        stale,
        Arc::new(vec![]),
    )));
    assert!(task.synchronize().await.is_err()); // Real 304, then a live anchor check.
    assert!(service.serving().await.is_err());
    let requests = fs::read_to_string(format!("{}.requests", tls.control.display())).unwrap();
    assert!(requests.contains("anchors/check"));
    server.abort();
}
#[tokio::test]
async fn failure_to_persist_authority_closes_admission() {
    let (_root, service, mut task, pointer, _tls, server) = setup().await;
    fs::create_dir(service.dir.join("publisher-high-water.json")).unwrap();
    let current = pointer.current.read().unwrap().clone();
    assert!(task.observe(current).await.is_err());
    assert!(service.state.lock().await.poisoned);
    assert!(service.serving().await.is_err());
    server.abort();
}

#[tokio::test]
async fn skipped_withdrawal_fences_old_answers_before_candidate_download() {
    let (_root, service, mut task, source, _tls, server) = setup().await;
    let current = source.current.read().unwrap().clone();
    task.observe(current).await.unwrap();
    let active_id = service
        .state
        .lock()
        .await
        .controller
        .as_ref()
        .unwrap()
        .current()
        .manifest()
        .id();
    source.canonical.store(false, Ordering::SeqCst);
    source.current.write().unwrap().revision += 1;
    assert!(task.synchronize().await.is_err());
    assert!(service.serving().await.is_err());
    let floor: Floor =
        serde_json::from_slice(&fs::read(service.dir.join("publisher-high-water.json")).unwrap())
            .unwrap();
    assert!(floor.invalid.contains(&active_id));
    server.abort();
}
#[tokio::test]
async fn ordinary_https_failure_preserves_a_still_fresh_view() {
    let (_root, service, mut task, _source, tls, server) = setup().await;
    fs::write(&tls.control, "refused-body").unwrap();
    assert!(task.synchronize().await.is_err());
    assert!(service.serving().await.is_ok());
    assert!(task.floor.invalid.is_empty());
    server.abort();
}
#[tokio::test]
async fn truncated_objects_never_become_completed_cache_entries() {
    let (_root, service, mut task, _source, tls, server) = setup().await;
    let object = Object::new(b"not an inventory");
    fs::write(&tls.control, "truncated").unwrap();
    assert!(task.object(&object).await.is_err());
    assert!(!service.dir.join("objects").join(&object.sha256).exists());
    assert_eq!(
        fs::read_dir(service.dir.join("objects")).unwrap().count(),
        0
    );
    fs::write(&tls.control, "").unwrap();
    let path = task.object(&object).await.unwrap();
    assert_eq!(fs::read(path).unwrap(), b"not an inventory");
    server.abort();
}
