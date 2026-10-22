use super::*;
use crate::protocol::{MAX_AGE_MS, PROTOCOL};
use crate::service::routes::{provider_commit, provider_pin};
use axum::extract::{Path, State};
use axum::{
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    RwLock,
};
struct Origin {
    current: RwLock<Current>,
    canonical: AtomicBool,
    advance_during_check: AtomicBool,
    withdraw_during_check: AtomicBool,
    version: RwLock<String>,
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
    StatusSynchronization,
    Arc<Origin>,
    tls::Tls,
    tokio::task::JoinHandle<()>,
) {
    let root = tempfile::tempdir().unwrap();
    let current = Arc::new(Origin {
        current: RwLock::new(pointer()),
        canonical: AtomicBool::new(true),
        advance_during_check: AtomicBool::new(false),
        withdraw_during_check: AtomicBool::new(false),
        version: RwLock::new(pir_sync_manager::service_discovery::VERSION.into()),
    });
    let shared = current.clone();
    let anchors = current.clone();
    let discovery = current.clone();
    let app = Router::new()
        .route(
            pir_sync_manager::service_discovery::PATH,
            get(move || {
                let discovery = discovery.clone();
                async move {
                    let mut document = pir_sync_manager::service_discovery::Document::publisher();
                    document.version = discovery.version.read().unwrap().clone();
                    Json(document)
                }
            }),
        )
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
                    if shared.advance_during_check.swap(false, Ordering::SeqCst) {
                        let mut current = shared.current.write().unwrap();
                        current.revision += 1;
                        if shared.withdraw_during_check.load(Ordering::SeqCst) {
                            current.eligible = false;
                        }
                        return StatusCode::CONFLICT.into_response();
                    }
                    Json(AnchorResponse {
                        current: shared.current.read().unwrap().clone(),
                        canonical: vec![
                            shared.canonical.load(Ordering::SeqCst);
                            request.anchors.len()
                        ],
                    })
                    .into_response()
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
    let task = StatusSynchronization::new(service.clone(), &config).unwrap();
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
    // Withdrawal fences service but keeps the normal poll cadence.
    task.synchronize().await.unwrap();
    assert!(task.deferred);
    assert_eq!(
        service.state.lock().await.synchronization,
        "waiting_for_peer"
    );
    assert!(service.serving().await.is_err());
    let saved: PublisherHighWater = serde_json::from_slice(
        &fs::read(service.state_dir.join("publisher-high-water.json")).unwrap(),
    )
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
    assert!(task.synchronize().await.is_err()); // A real 304 still cannot make the expired view queryable.
    assert!(service.serving().await.is_err());
    let requests = fs::read_to_string(format!("{}.requests", tls.control.display())).unwrap();
    assert!(requests.contains("anchors/check"));
    server.abort();
}
#[tokio::test]
async fn failure_to_persist_authority_closes_admission() {
    let (_root, service, mut task, pointer, _tls, server) = setup().await;
    fs::create_dir(service.state_dir.join("publisher-high-water.json")).unwrap();
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
    let floor: PublisherHighWater = serde_json::from_slice(
        &fs::read(service.state_dir.join("publisher-high-water.json")).unwrap(),
    )
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
async fn withdrawn_view_does_not_block_downloading_a_same_epoch_successor() {
    let (_root, service, mut task, source, _tls, server) = setup().await;
    let old = service
        .state
        .lock()
        .await
        .controller
        .as_ref()
        .unwrap()
        .current();
    let mut withdrawn = pointer();
    withdrawn.eligible = false;
    task.observe(withdrawn).await.unwrap();
    assert!(service.serving().await.is_err());
    {
        let mut current = source.current.write().unwrap();
        current.revision += 1;
        current.generation += 1;
    }
    // This fixture deliberately has no valid prepared inventory. Recovery must
    // reach its download/validation without making the withdrawn view usable.
    assert!(task.synchronize().await.is_err());
    let object = pointer().inventory.unwrap();
    assert!(service
        .state_dir
        .join(pir_artifact::namespace("objects"))
        .join(object.checksum)
        .is_file());
    assert!(task.floor.invalid.contains(&old.manifest().id()));
    assert!(service.serving().await.is_err());
    server.abort();
}
#[tokio::test]
async fn truncated_objects_never_become_completed_cache_entries() {
    let (_root, service, mut task, _source, tls, server) = setup().await;
    let object = Object::new(b"not an inventory");
    fs::write(&tls.control, "truncated").unwrap();
    assert!(task.object(&object).await.is_err());
    assert!(!service
        .state_dir
        .join(pir_artifact::namespace("objects"))
        .join(&object.checksum)
        .exists());
    assert_eq!(
        fs::read_dir(service.state_dir.join(pir_artifact::namespace("objects")))
            .unwrap()
            .count(),
        0
    );
    fs::write(&tls.control, "").unwrap();
    let path = task.object(&object).await.unwrap();
    assert_eq!(fs::read(path).unwrap(), b"not an inventory");
    server.abort();
}

#[tokio::test]
async fn worker_retains_the_routers_view_until_handover_commits() {
    let (_root, service, _task, _source, _tls, server) = setup().await;
    let controller = service.state.lock().await.controller.clone().unwrap();
    let first = controller.current().manifest().id();
    let _ = provider_pin(State(service.clone()), Path(hex::encode(first)))
        .await
        .unwrap();
    let mut second = manifest();
    second.generation += 1;
    let second_id = second.id();
    controller
        .activate(Generation::public_only(second, Arc::new(vec![])))
        .unwrap();
    assert!(
        !controller.can_prepare(),
        "the router still serves the predecessor"
    );
    let _ = provider_pin(State(service.clone()), Path(hex::encode(second_id)))
        .await
        .unwrap();
    assert!(
        !controller.can_prepare(),
        "pinning a candidate must not release the active router view"
    );
    let _ = provider_commit(State(service.clone()), Path(hex::encode(second_id)))
        .await
        .unwrap();
    assert!(controller.can_prepare());
    assert_eq!(service.state.lock().await.router_holds.len(), 1);
    server.abort();
}
#[tokio::test]
async fn consumer_restart_requires_https_authority_despite_completed_objects() {
    let (_root, service, mut task, source, tls, server) = setup().await;
    let current = source.current.read().unwrap().clone();
    task.observe(current).await.unwrap();
    let object = Object::new(b"not an inventory");
    task.object(&object).await.unwrap();
    let path = service.state_dir.clone();
    drop(task);
    drop(service);
    fs::write(&tls.control, "refused-body").unwrap();
    let service = Service::open(Role::Worker, [1; 32], path.clone(), String::new()).unwrap();
    let config = RoleConfig {
        role: Role::Worker,
        network_hex: hex::encode([1; 32]),
        state_dir: path.clone(),
        listen: "127.0.0.1:1".parse().unwrap(),
        query_listen: None,
        publisher: tls.origin.clone(),
        publisher_ca: Some(tls.certificate.clone()),
        worker_origin: String::new(),
        cuda: false,
    };
    let mut task = StatusSynchronization::new(service.clone(), &config).unwrap();
    assert!(task.synchronize().await.is_err());
    assert!(service.serving().await.is_err());
    assert!(path
        .join(pir_artifact::namespace("objects"))
        .join(object.checksum)
        .is_file());
    server.abort();
}

#[tokio::test]
async fn loading_worker_keeps_normal_poll_cadence_without_serving() {
    let (root, service, mut task, _pointer, _tls, server) = setup().await;
    service.state.lock().await.controller = None;
    let worker = Service::open(
        Role::Worker,
        [1; 32],
        root.path().join("empty-worker"),
        String::new(),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    task.service.role = Role::Router;
    task.service.worker_origin = format!("http://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        axum::serve(listener, worker.control_routes())
            .await
            .unwrap()
    });
    assert_eq!(task.synchronize().await.unwrap(), 0);
    assert!(task.deferred);
    assert_eq!(
        service.state.lock().await.synchronization,
        "waiting_for_peer"
    );
    assert!(service.serving().await.is_err());
    peer.abort();
    server.abort();
}

#[tokio::test]
async fn incompatible_discovery_stops_polling_without_observing_product_state() {
    let (_root, service, mut task, origin, _tls, server) = setup().await;
    *origin.version.write().unwrap() = "999.0.0".into();
    let error = task.synchronize().await.unwrap_err();
    assert!(task.terminal(&error));
    assert!(task.floor.current.is_none());
    assert!(!service.state_dir.join("publisher-high-water.json").exists());
    let returned = pir_sync_manager::SyncManager::new(task).run().await;
    assert!(pir_sync_manager::is_incompatible(returned.as_ref()));
    server.abort();
}

#[tokio::test]
async fn ordinary_discovery_failure_is_retryable_and_never_renews_freshness() {
    let (_root, service, mut task, _origin, tls, server) = setup().await;
    let before = service
        .state
        .lock()
        .await
        .controller
        .as_ref()
        .unwrap()
        .current()
        .manifest()
        .observed_ms;
    fs::write(&tls.control, "refused-body").unwrap();
    let error = task.synchronize().await.unwrap_err();
    assert!(!task.terminal(&error));
    assert_eq!(
        service
            .state
            .lock()
            .await
            .controller
            .as_ref()
            .unwrap()
            .current()
            .manifest()
            .observed_ms,
        before
    );
    assert!(task.floor.current.is_none());
    server.abort();
}

#[tokio::test]
async fn a_new_publication_during_authority_check_retries_without_renewing_the_old_view() {
    let (_root, service, mut task, source, _tls, server) = setup().await;
    let before = service
        .state
        .lock()
        .await
        .controller
        .as_ref()
        .unwrap()
        .current()
        .manifest()
        .clone();
    source.advance_during_check.store(true, Ordering::SeqCst);
    assert_eq!(task.synchronize().await.unwrap(), before.generation);
    assert!(task.deferred);
    assert_eq!(
        service.state.lock().await.synchronization,
        "waiting_for_peer"
    );
    assert_eq!(
        service
            .state
            .lock()
            .await
            .controller
            .as_ref()
            .unwrap()
            .current()
            .manifest(),
        &before
    );
    assert!(task.floor.invalid.is_empty());
    assert_eq!(
        task.floor.current.as_ref().unwrap(),
        &*source.current.read().unwrap()
    );
    // The next attempt really runs; its invalid fixture must still be refused.
    assert!(task.synchronize().await.is_err());
    server.abort();
}

#[tokio::test]
async fn withdrawal_during_authority_check_fences_the_old_view_before_deferring() {
    let (_root, service, mut task, source, _tls, server) = setup().await;
    source.advance_during_check.store(true, Ordering::SeqCst);
    source.withdraw_during_check.store(true, Ordering::SeqCst);
    task.synchronize().await.unwrap();
    assert!(task.deferred);
    assert!(!task.floor.current.as_ref().unwrap().eligible);
    assert!(service.serving().await.is_err());
    server.abort();
}
