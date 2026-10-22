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
use pir_publication::{
    conditional_token, AuthorityCheckResponse, AUTHORITY_CHECK_PATH, CURRENT_PATH, OBJECTS_PATH,
};
use pir_sync_manager::SyncTask;
use std::fs;
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
        inventory: Some(ObjectReference::for_bytes(b"not an inventory")),
        eligible: true,
    }
}
fn role_config(role: Role, state_dir: std::path::PathBuf, tls: &tls::Tls) -> RoleConfig {
    RoleConfig {
        role,
        network_hex: hex::encode([1; 32]),
        state_dir,
        listen: "127.0.0.1:1".parse().unwrap(),
        query_listen: None,
        publisher: tls.origin.clone(),
        publisher_ca: Some(tls.certificate.clone()),
        worker_origin: String::new(),
        cuda: false,
    }
}
fn saved_floor(service: &Service) -> serde_json::Value {
    serde_json::from_slice(&fs::read(service.state_dir.join(FLOOR_FILE)).unwrap()).unwrap()
}
fn deferred(pass: PublicationPass<u64>) -> u64 {
    match pass {
        PublicationPass::Deferred { report } => report,
        PublicationPass::Committed { .. } => panic!("the fixture never commits"),
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
            &format!("{PREFIX}{CURRENT_PATH}"),
            get(move |headers: HeaderMap| {
                let shared = shared.clone();
                async move {
                    let c = shared.current.read().unwrap().clone();
                    let token = conditional_token(&c);
                    if headers.get("if-none-match").and_then(|v| v.to_str().ok()) == Some(&token) {
                        StatusCode::NOT_MODIFIED.into_response()
                    } else {
                        Json(c).into_response()
                    }
                }
            }),
        )
        .route(
            &format!("{PREFIX}{AUTHORITY_CHECK_PATH}"),
            post(move |Json(request): Json<AnchorCheckRequest>| {
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
                    Json(AuthorityCheckResponse {
                        pointer: shared.current.read().unwrap().clone(),
                        still_authoritative: vec![
                            shared.canonical.load(Ordering::SeqCst);
                            request.subjects.len()
                        ],
                    })
                    .into_response()
                }
            }),
        );
    let app = app.route(
        &format!("{PREFIX}{OBJECTS_PATH}:id"),
        get(|| async { "not an inventory" }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let tls = tls::Tls::start(&origin).await;
    let config = role_config(Role::Worker, root.path().join("worker"), &tls);
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
    let task = synchronization(service.clone(), &config).unwrap();
    (root, service, task, current, tls, server)
}
fn serving_manifest_id(service: &Service) -> impl std::future::Future<Output = [u8; 32]> + '_ {
    async move {
        service
            .state
            .lock()
            .await
            .controller
            .as_ref()
            .unwrap()
            .current()
            .manifest()
            .id()
    }
}

#[tokio::test]
async fn tls_withdrawal_fences_before_persisting_and_refuses_replay() {
    let (_root, service, mut task, source, _tls, server) = setup().await;
    // The fixture inventory is malformed, but the pointer floor is raised.
    assert!(task.synchronize().await.is_err());
    {
        let mut current = source.current.write().unwrap();
        current.revision += 1;
        current.eligible = false;
    }
    // Withdrawal fences service but keeps the normal poll cadence.
    deferred(task.synchronize().await.unwrap());
    assert_eq!(
        service.state.lock().await.synchronization,
        "waiting_for_peer"
    );
    assert!(service.serving().await.is_err());
    assert_eq!(saved_floor(&service)["current"]["eligible"], false);
    source.current.write().unwrap().eligible = true;
    let error = task.synchronize().await.unwrap_err();
    assert!(error.is::<pir_publication::PublicationReplay>(), "{error}");
    server.abort();
}

#[tokio::test]
async fn unchanged_https_authority_never_renews_observation_age() {
    let (_root, service, mut task, _source, tls, server) = setup().await;
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
    assert!(requests.contains(AUTHORITY_CHECK_PATH));
    server.abort();
}

#[tokio::test]
async fn failure_to_persist_authority_closes_admission() {
    let (_root, service, mut task, _source, _tls, server) = setup().await;
    fs::create_dir(service.state_dir.join(FLOOR_FILE)).unwrap();
    assert!(task.synchronize().await.is_err());
    assert!(service.state.lock().await.poisoned);
    assert!(service.serving().await.is_err());
    server.abort();
}

#[tokio::test]
async fn skipped_withdrawal_fences_old_answers_before_candidate_download() {
    let (_root, service, mut task, source, _tls, server) = setup().await;
    assert!(task.synchronize().await.is_err());
    let active_id = serving_manifest_id(&service).await;
    source.canonical.store(false, Ordering::SeqCst);
    source.current.write().unwrap().revision += 1;
    assert!(task.synchronize().await.is_err());
    assert!(service.serving().await.is_err());
    assert!(task.floor().is_invalidated(&active_id));
    let reopened: AuthorityFloor<Current> = AuthorityFloor::open(
        service.state_dir.join(FLOOR_FILE),
        task.floor().source().clone(),
    )
    .unwrap();
    assert!(reopened.is_invalidated(&active_id));
    server.abort();
}

#[tokio::test]
async fn ordinary_https_failure_preserves_a_still_fresh_view() {
    let (_root, service, mut task, _source, tls, server) = setup().await;
    fs::write(&tls.control, "refused-body").unwrap();
    assert!(task.synchronize().await.is_err());
    assert!(service.serving().await.is_ok());
    assert!(!task
        .floor()
        .is_invalidated(&serving_manifest_id(&service).await));
    server.abort();
}

#[tokio::test]
async fn withdrawn_view_does_not_block_downloading_a_same_epoch_successor() {
    let (_root, service, mut task, source, _tls, server) = setup().await;
    let old = serving_manifest_id(&service).await;
    source.current.write().unwrap().eligible = false;
    deferred(task.synchronize().await.unwrap());
    assert!(service.serving().await.is_err());
    {
        let mut current = source.current.write().unwrap();
        current.revision += 1;
        current.generation += 1;
        current.eligible = true;
    }
    // This fixture deliberately has no valid prepared inventory. Recovery must
    // reach its download/validation without making the withdrawn view usable.
    assert!(task.synchronize().await.is_err());
    let object = pointer().inventory.unwrap();
    assert!(service
        .state_dir
        .join("objects")
        .join(object.sha256)
        .is_file());
    assert!(task.floor().is_invalidated(&old));
    assert!(service.serving().await.is_err());
    server.abort();
}

#[tokio::test]
async fn truncated_objects_never_become_completed_cache_entries() {
    let (_root, service, mut task, _source, tls, server) = setup().await;
    let object = ObjectReference::for_bytes(b"not an inventory");
    fs::write(&tls.control, "truncated").unwrap();
    assert!(task.synchronize().await.is_err());
    assert_eq!(
        fs::read_dir(service.state_dir.join("objects"))
            .unwrap()
            .count(),
        0
    );
    fs::write(&tls.control, "").unwrap();
    // The complete object is promoted; its inventory is still refused.
    assert!(task.synchronize().await.is_err());
    let path = service.state_dir.join("objects").join(&object.sha256);
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
    let (_root, service, mut task, _source, tls, server) = setup().await;
    assert!(task.synchronize().await.is_err());
    let object = pointer().inventory.unwrap();
    let path = service.state_dir.clone();
    assert!(path.join("objects").join(&object.sha256).is_file());
    drop(task);
    drop(service);
    fs::write(&tls.control, "refused-body").unwrap();
    let service = Service::open(Role::Worker, [1; 32], path.clone(), String::new()).unwrap();
    let mut task = synchronization(
        service.clone(),
        &role_config(Role::Worker, path.clone(), &tls),
    )
    .unwrap();
    assert!(task.synchronize().await.is_err());
    assert!(service.serving().await.is_err());
    assert!(path.join("objects").join(object.sha256).is_file());
    server.abort();
}

#[tokio::test]
async fn floors_refuse_another_publisher_and_keep_unbound_replay_protection() {
    let (_root, service, task, _source, tls, server) = setup().await;
    let path = service.state_dir.clone();
    drop(task);
    drop(service);
    // A floor written before source binding keeps its revision floor.
    let mut legacy = pointer();
    legacy.revision = 9;
    fs::write(
        path.join(FLOOR_FILE),
        serde_json::to_vec(&serde_json::json!({ "current": legacy, "invalid": [] })).unwrap(),
    )
    .unwrap();
    let service = Service::open(Role::Worker, [1; 32], path.clone(), String::new()).unwrap();
    let config = role_config(Role::Worker, path.clone(), &tls);
    let mut task = synchronization(service.clone(), &config).unwrap();
    let error = task.synchronize().await.unwrap_err();
    assert!(error.is::<pir_publication::PublicationReplay>(), "{error}");
    assert!(saved_floor(&service)["source"].is_null());
    drop(task);

    // Once bound, another publisher origin or chain cannot reuse the floor.
    let mut bound: AuthorityFloor<Current> = AuthorityFloor::open(
        path.join(FLOOR_FILE),
        SourceBinding {
            publisher_origin: "https://other.example/".into(),
            product_namespace: PREFIX.into(),
            chain: [1; 32],
        },
    )
    .unwrap();
    bound.advance(legacy, []);
    bound.pending_write().unwrap().write().unwrap();
    assert!(synchronization(service.clone(), &config).is_err());
    server.abort();
}

#[tokio::test]
async fn loading_worker_keeps_normal_poll_cadence_without_serving() {
    let (root, _service, task, _pointer, tls, server) = setup().await;
    drop(task);
    let worker = Service::open(
        Role::Worker,
        [1; 32],
        root.path().join("empty-worker"),
        String::new(),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let worker_origin = format!("http://{}", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        axum::serve(listener, worker.control_routes())
            .await
            .unwrap()
    });
    let router = Service::open(
        Role::Router,
        [1; 32],
        root.path().join("router"),
        worker_origin,
    )
    .unwrap();
    let mut task = synchronization(
        router.clone(),
        &role_config(Role::Router, root.path().join("router"), &tls),
    )
    .unwrap();
    assert_eq!(deferred(task.synchronize().await.unwrap()), 0);
    assert_eq!(
        router.state.lock().await.synchronization,
        "waiting_for_peer"
    );
    assert!(router.serving().await.is_err());
    peer.abort();
    server.abort();
}

#[tokio::test]
async fn incompatible_discovery_stops_polling_without_observing_product_state() {
    let (_root, service, mut task, origin, _tls, server) = setup().await;
    *origin.version.write().unwrap() = "999.0.0".into();
    let error = task.synchronize().await.unwrap_err();
    assert!(task.terminal(&error));
    assert!(task.floor().pointer().is_none());
    assert!(!service.state_dir.join(FLOOR_FILE).exists());
    let returned = pir_sync_manager::SyncManager::new(task).run().await;
    assert!(pir_sync_manager::is_incompatible(returned.as_ref()));
    server.abort();
}

#[tokio::test]
async fn ordinary_discovery_failure_is_retryable_and_never_renews_freshness() {
    let (_root, service, mut task, _origin, tls, server) = setup().await;
    let observed_ms = |service: &Service| {
        let service = service.clone();
        async move {
            service
                .state
                .lock()
                .await
                .controller
                .as_ref()
                .unwrap()
                .current()
                .manifest()
                .observed_ms
        }
    };
    let before = observed_ms(&service).await;
    fs::write(&tls.control, "refused-body").unwrap();
    let error = task.synchronize().await.unwrap_err();
    assert!(!task.terminal(&error));
    assert_eq!(observed_ms(&service).await, before);
    assert!(task.floor().pointer().is_none());
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
    assert_eq!(
        deferred(task.synchronize().await.unwrap()),
        before.generation
    );
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
    assert!(!task.floor().is_invalidated(&before.id()));
    assert_eq!(
        task.floor().pointer().unwrap(),
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
    deferred(task.synchronize().await.unwrap());
    assert!(!task.floor().pointer().unwrap().eligible);
    assert!(service.serving().await.is_err());
    server.abort();
}
