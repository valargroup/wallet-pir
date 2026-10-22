use super::*;
use crate::service::routes::{provider_commit, provider_pin};
use axum::extract::{Path, State};
use axum::{
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use pir_publication::{
    conditional_token, SubjectAnswer, AUTHORITY_CHECK_PATH, CURRENT_PATH, OBJECTS_PATH,
};
use pir_sync_manager::{publication_lifecycle::PublicationLifecycleError, SyncTask};
use status_shared::protocol::{MAX_AGE_MS, PROTOCOL};
use std::fs;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    RwLock,
};
struct Origin {
    current: RwLock<Current>,
    canonical: AtomicBool,
    noncanonical: RwLock<BTreeSet<AnchorSubject>>,
    advance_during_check: AtomicBool,
    withdraw_during_check: AtomicBool,
    /// An earlier authority answer replayed instead of a live one.
    replayed_check: RwLock<Option<Vec<u8>>>,
    version: RwLock<String>,
    /// Objects served by digest; any other id serves a non-inventory.
    objects: RwLock<std::collections::HashMap<String, Vec<u8>>>,
    /// Every object id requested, in order.
    requested_objects: RwLock<Vec<String>>,
    /// An envelope published once the named object is requested.
    publish_on_request: RwLock<Option<(String, Current)>>,
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
        scope: scope([1; 32], [2; 32]),
        sequence: 5,
        eligible: true,
        fence: pir_publication::AuthorityFence {
            recovery_epoch: 2,
            latest_withdrawal: 0,
        },
        publication: Some(publication(3)),
    }
}
fn publication(generation: u64) -> pir_publication::PublicationReference<StatusDescriptor> {
    pir_publication::PublicationReference::new(
        [6; 32],
        ObjectReference::for_bytes(b"not an inventory"),
        StatusDescriptor { generation },
    )
}
/// The publisher's withdrawal transition.
fn withdraw(current: &mut Current) {
    current.sequence += 1;
    current.eligible = false;
    current.fence.latest_withdrawal = current.sequence;
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
        noncanonical: RwLock::default(),
        advance_during_check: AtomicBool::new(false),
        withdraw_during_check: AtomicBool::new(false),
        replayed_check: RwLock::default(),
        version: RwLock::new(pir_sync_manager::service_discovery::VERSION.into()),
        objects: RwLock::default(),
        requested_objects: RwLock::default(),
        publish_on_request: RwLock::default(),
    });
    let shared = current.clone();
    let anchors = current.clone();
    let discovery = current.clone();
    let objects = current.clone();
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
                        if shared.withdraw_during_check.load(Ordering::SeqCst) {
                            withdraw(&mut current);
                        } else {
                            current.sequence += 1;
                        }
                        return StatusCode::CONFLICT.into_response();
                    }
                    if let Some(replayed) = shared.replayed_check.read().unwrap().clone() {
                        return replayed.into_response();
                    }
                    let still_authoritative = shared.canonical.load(Ordering::SeqCst);
                    Json(AnchorCheckResponse {
                        authority: shared.current.read().unwrap().authority().unwrap(),
                        answers: request
                            .subjects
                            .into_iter()
                            .map(|subject| SubjectAnswer {
                                still_authoritative: still_authoritative
                                    && !shared.noncanonical.read().unwrap().contains(&subject),
                                subject,
                            })
                            .collect(),
                    })
                    .into_response()
                }
            }),
        );
    let app = app.route(
        &format!("{PREFIX}{OBJECTS_PATH}:id"),
        get(move |Path(id): Path<String>| {
            let objects = objects.clone();
            async move {
                objects.requested_objects.write().unwrap().push(id.clone());
                let mut publish = objects.publish_on_request.write().unwrap();
                if publish.as_ref().is_some_and(|(object, _)| *object == id) {
                    *objects.current.write().unwrap() = publish.take().unwrap().1;
                }
                let served = objects.objects.read().unwrap().get(&id).cloned();
                served.unwrap_or_else(|| b"not an inventory".to_vec())
            }
        }),
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
async fn serving_manifest_id(service: &Service) -> [u8; 32] {
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

#[tokio::test]
async fn tls_withdrawal_fences_before_persisting_and_refuses_replay() {
    let (_root, service, mut task, source, _tls, server) = setup().await;
    // The fixture inventory is malformed, but the pointer floor is raised.
    assert!(task.synchronize().await.is_err());
    withdraw(&mut source.current.write().unwrap());
    // Withdrawal fences service but keeps the normal poll cadence.
    deferred(task.synchronize().await.unwrap());
    assert_eq!(
        service.state.lock().await.synchronization,
        "waiting_for_peer"
    );
    assert!(service.serving().await.is_err());
    assert_eq!(saved_floor(&service)["current"]["eligible"], false);
    // Undoing the withdrawal at the same sequence is a divergent replay.
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
async fn a_refuted_anchor_leaves_other_status_sessions_available() {
    use axum::http::Request;
    use tower::ServiceExt;

    for refute_current in [false, true] {
        let (_root, service, mut task, source, _tls, server) = setup().await;
        let controller = service.state.lock().await.controller.clone().unwrap();
        let predecessor = controller.current().manifest().clone();
        let mut current = predecessor.clone();
        current.generation += 1;
        current.anchor_height += 1;
        current.anchor_hash = [9; 32];
        controller
            .activate(Generation::public_only(current.clone(), Arc::new(vec![])))
            .unwrap();
        {
            let mut envelope = source.current.write().unwrap();
            envelope.sequence += 1;
            envelope.publication = Some(publication(current.generation));
        }
        let (refuted, unaffected) = if refute_current {
            (&current, &predecessor)
        } else {
            (&predecessor, &current)
        };
        source
            .noncanonical
            .write()
            .unwrap()
            .insert((refuted.anchor_height, refuted.anchor_hash));

        // The fixture has no prepared candidate, but the real HTTPS authority
        // exchange must apply its negative answer before that download fails.
        assert!(task.synchronize().await.is_err());
        assert!(task.floor().is_invalidated(&view(refuted)));
        assert!(!task.floor().is_invalidated(&view(unaffected)));
        assert_eq!(
            controller.check(&controller.resolve(refuted.id()).unwrap()),
            Err(StatusCode::GONE)
        );
        assert!(controller
            .check(&controller.resolve(unaffected.id()).unwrap())
            .is_ok());

        // Session requests refer to their own view, even if the newest view
        // was the one invalidated. The public route keeps its existing 410.
        let router = Service {
            role: Role::Router,
            ..service.clone()
        };
        for (manifest, expected) in [(refuted, StatusCode::GONE), (unaffected, StatusCode::OK)] {
            let response = router
                .query_routes()
                .oneshot(
                    Request::get(format!("/v1/status/session/{}", hex::encode(manifest.id())))
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
        }
        let reopened: AuthorityFloor<StatusDescriptor> = AuthorityFloor::open(
            service.state_dir.join(FLOOR_FILE),
            task.floor().source().clone(),
        )
        .unwrap();
        assert!(reopened.is_invalidated(&view(refuted)));
        assert!(!reopened.is_invalidated(&view(unaffected)));
        server.abort();
    }
}

#[tokio::test]
async fn skipped_withdrawal_fences_old_answers_before_candidate_download() {
    let (_root, service, mut task, source, _tls, server) = setup().await;
    assert!(task.synchronize().await.is_err());
    let active_id = serving_manifest_id(&service).await;
    source.canonical.store(false, Ordering::SeqCst);
    source.current.write().unwrap().sequence += 1;
    assert!(task.synchronize().await.is_err());
    assert!(service.serving().await.is_err());
    assert!(task.floor().is_invalidated(&ViewIdentity(active_id)));
    let reopened: AuthorityFloor<StatusDescriptor> = AuthorityFloor::open(
        service.state_dir.join(FLOOR_FILE),
        task.floor().source().clone(),
    )
    .unwrap();
    assert!(reopened.is_invalidated(&ViewIdentity(active_id)));
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
        .is_invalidated(&ViewIdentity(serving_manifest_id(&service).await)));
    server.abort();
}

#[tokio::test]
async fn withdrawn_view_does_not_block_downloading_a_same_epoch_successor() {
    let (_root, service, mut task, source, _tls, server) = setup().await;
    let old = serving_manifest_id(&service).await;
    withdraw(&mut source.current.write().unwrap());
    deferred(task.synchronize().await.unwrap());
    assert!(service.serving().await.is_err());
    {
        let mut current = source.current.write().unwrap();
        current.sequence += 1;
        current.publication = Some(publication(4));
        current.eligible = true;
    }
    // This fixture deliberately has no valid prepared inventory. Recovery must
    // reach its download/validation without making the withdrawn view usable.
    assert!(task.synchronize().await.is_err());
    let object = pointer().inventory().cloned().unwrap();
    assert!(service
        .state_dir
        .join("objects")
        .join(object.sha256)
        .is_file());
    assert!(task.floor().is_invalidated(&ViewIdentity(old)));
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
    let object = pointer().inventory().cloned().unwrap();
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
    // A floor written before source binding keeps its sequence floor.
    let mut legacy = pointer();
    legacy.sequence = 9;
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
    let mut bound: AuthorityFloor<StatusDescriptor> = AuthorityFloor::open(
        path.join(FLOOR_FILE),
        SourceBinding {
            publisher_origin: "https://other.example/".into(),
            product_namespace: PREFIX.into(),
            chain: [1; 32],
        },
    )
    .unwrap();
    let checked = bound.classify(legacy).unwrap();
    bound.record(checked, []).unwrap();
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
    assert!(task.floor().envelope().is_none());
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
    assert!(task.floor().envelope().is_none());
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
    assert!(!task.floor().is_invalidated(&ViewIdentity(before.id())));
    assert_eq!(
        task.floor().envelope().unwrap(),
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
    assert!(!task.floor().envelope().unwrap().eligible);
    assert!(service.serving().await.is_err());
    server.abort();
}

#[tokio::test]
async fn a_failed_floor_save_blocks_progress_until_the_same_floor_is_saved() {
    let (_root, service, mut task, source, _tls, server) = setup().await;
    // The fixture inventory is malformed, but the floor is made durable.
    assert!(task.synchronize().await.is_err());
    assert!(task.floor().is_durable());
    let active_id = serving_manifest_id(&service).await;
    let floor_path = service.state_dir.join(FLOOR_FILE);
    // A non-empty directory replaces the floor file, so the rename fails.
    fs::remove_file(&floor_path).unwrap();
    fs::create_dir(&floor_path).unwrap();
    fs::write(floor_path.join("occupant"), b"").unwrap();

    // Negative evidence under the unchanged pointer cannot be saved.
    source.canonical.store(false, Ordering::SeqCst);
    assert!(task.synchronize().await.is_err());
    assert!(task.floor().is_invalidated(&ViewIdentity(active_id)));
    assert!(!task.floor().is_durable());
    assert!(service.state.lock().await.poisoned);

    // The identical pointer still makes no progress while the save fails.
    source.canonical.store(true, Ordering::SeqCst);
    let error = task.synchronize().await.unwrap_err();
    assert!(error.is::<std::io::Error>(), "{error}");
    assert!(!task.floor().is_durable());

    // Once the filesystem is repaired, the same pointer saves the floor first.
    fs::remove_dir_all(&floor_path).unwrap();
    assert!(task.synchronize().await.is_err()); // The inventory is still malformed.
    assert!(task.floor().is_durable());
    let reopened: AuthorityFloor<StatusDescriptor> =
        AuthorityFloor::open(floor_path, task.floor().source().clone()).unwrap();
    assert!(reopened.is_invalidated(&ViewIdentity(active_id)));
    let mut older = pointer();
    older.sequence -= 1;
    assert!(reopened.classify(older).is_err());
    server.abort();
}

/// Publish `inventory` observed now and return its object reference.
fn publish_inventory(origin: &Origin, inventory: &mut Inventory) -> ObjectReference {
    inventory.manifest.observed_ms = now_ms();
    let bytes = serde_json::to_vec(inventory).unwrap();
    let object = ObjectReference::for_bytes(&bytes);
    origin
        .objects
        .write()
        .unwrap()
        .insert(object.sha256.clone(), bytes);
    object
}
/// Store prepared `generation`'s objects at the origin. The objects do not
/// depend on the observation time, so a slow export cannot age the inventory.
fn export(origin: &Origin, generation: &Generation) -> Inventory {
    generation
        .export_prepared(|bytes| {
            let object = ObjectReference::for_bytes(bytes);
            origin
                .objects
                .write()
                .unwrap()
                .insert(object.sha256.clone(), bytes.to_vec());
            Ok(object)
        })
        .unwrap()
}
fn committed(pass: PublicationPass<u64>) -> (u64, PeerHandover) {
    match pass {
        PublicationPass::Committed { report, handover } => (report, handover),
        PublicationPass::Deferred { report } => panic!("deferred at generation {report}"),
    }
}

/// Router A is pinned at its worker, which already serves B, while the
/// publisher advanced to C with other material. The router commits B through
/// the real lifecycle and worker routes, its release frees A at the worker,
/// and the worker then advances to C. An equal-generation inventory the
/// latest publication does not authorize is refused before any of its
/// material is requested.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn router_commits_its_workers_retained_predecessor_and_releases_the_worker() {
    use status_shared::fixture::{self, NETWORK, SALT};
    let (root, _service, task, source, tls, server) = setup().await;
    drop(task);
    let (b, _) =
        Generation::prepare(&fixture::snapshot(32, false).unwrap(), 4, 2, now_ms(), None).unwrap();
    let (c, _) = Generation::prepare(
        &fixture::snapshot(32, true).unwrap(),
        5,
        2,
        now_ms(),
        Some(&b),
    )
    .unwrap();
    let (mut b_inventory, mut c_inventory) = (export(&source, &b), export(&source, &c));
    assert_ne!(
        b_inventory.material_identity(),
        c_inventory.material_identity()
    );
    let mut forged = b_inventory.clone();
    forged.manifest.generation = c_inventory.manifest.generation;
    let forged_object = publish_inventory(&source, &mut forged);
    let b_object = publish_inventory(&source, &mut b_inventory);
    let c_object = publish_inventory(&source, &mut c_inventory);
    let b = b.reobserved(b_inventory.manifest.observed_ms);
    *source.current.write().unwrap() = Current {
        scope: scope(NETWORK, SALT),
        sequence: 7,
        ..fixture::publishing(&c_inventory, &c_object)
    };
    let mut a = b_inventory.manifest.clone();
    a.generation = 3;
    let a = Generation::public_only(a, Arc::new(vec![]));

    let worker = Service::open(
        Role::Worker,
        NETWORK,
        root.path().join("peer"),
        String::new(),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let worker_origin = format!("http://{}", listener.local_addr().unwrap());
    let peer_routes = worker.control_routes();
    let peer = tokio::spawn(async move { axum::serve(listener, peer_routes).await.unwrap() });
    let router = Service::open(
        Role::Router,
        NETWORK,
        root.path().join("router"),
        worker_origin,
    )
    .unwrap();
    router.state.lock().await.controller = Some(Controller::new(a.clone()));
    let mut router_task = synchronization(
        router.clone(),
        &role_config(Role::Router, root.path().join("router"), &tls),
    )
    .unwrap();

    {
        let mut state = worker.state.lock().await;
        state.controller = Some(Controller::new(Generation::public_only(
            forged.manifest.clone(),
            Arc::new(vec![]),
        )));
        state.inventory = Some(forged_object.clone());
    }
    let error = router_task.synchronize().await.unwrap_err();
    assert!(error.to_string().contains("not authorized"), "{error}");
    assert_eq!(
        *source.requested_objects.read().unwrap(),
        std::slice::from_ref(&forged_object.sha256)
    );
    assert_eq!(serving_manifest_id(&router).await, a.manifest().id());

    // The worker activated B while the router still holds A.
    let worker_controller = Controller::new(a.clone());
    worker.state.lock().await.controller = Some(worker_controller.clone());
    let _ = provider_pin(State(worker.clone()), Path(hex::encode(a.manifest().id())))
        .await
        .unwrap();
    worker_controller.activate(b).unwrap();
    worker.state.lock().await.inventory = Some(b_object);
    assert!(!worker_controller.can_prepare());

    let (generation, handover) = committed(router_task.synchronize().await.unwrap());
    assert_eq!((generation, handover), (4, PeerHandover::Complete));
    assert_eq!(
        serving_manifest_id(&router).await,
        b_inventory.manifest.id()
    );
    assert_eq!(router.state.lock().await.synchronization, "synchronized");
    let holds = worker
        .state
        .lock()
        .await
        .router_holds
        .iter()
        .map(|g| g.manifest().id())
        .collect::<Vec<_>>();
    assert_eq!(holds, [b_inventory.manifest.id()]);
    assert!(worker_controller.can_prepare());

    let mut worker_task = synchronization(
        worker.clone(),
        &role_config(Role::Worker, root.path().join("peer"), &tls),
    )
    .unwrap();
    assert_eq!(committed(worker_task.synchronize().await.unwrap()).0, 5);
    assert_eq!(
        serving_manifest_id(&worker).await,
        c_inventory.manifest.id()
    );
    // The router's committed view stays resolvable at the worker.
    assert!(worker_controller.resolve(b_inventory.manifest.id()).is_ok());
    peer.abort();
    server.abort();
}

/// A publication that re-identifies already verified material commits without
/// downloading or loading the material again, but still needs live authority.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn metadata_only_refresh_reuses_verified_material_under_live_authority() {
    use status_shared::fixture::{self, NETWORK, SALT};
    let (root, _service, task, source, tls, server) = setup().await;
    drop(task);
    let (generation, _) =
        Generation::prepare(&fixture::snapshot(32, false).unwrap(), 4, 2, now_ms(), None).unwrap();
    let mut inventory = export(&source, &generation);
    let publish = |inventory: &mut Inventory, sequence: u64| {
        let object = publish_inventory(&source, inventory);
        *source.current.write().unwrap() = Current {
            scope: scope(NETWORK, SALT),
            sequence,
            ..fixture::publishing(inventory, &object)
        };
        object
    };
    publish(&mut inventory, 7);
    let worker = Service::open(
        Role::Worker,
        NETWORK,
        root.path().join("peer"),
        String::new(),
    )
    .unwrap();
    let mut task = synchronization(
        worker.clone(),
        &role_config(Role::Worker, root.path().join("peer"), &tls),
    )
    .unwrap();
    assert_eq!(committed(task.synchronize().await.unwrap()).0, 4);
    let loaded = worker
        .state
        .lock()
        .await
        .controller
        .clone()
        .unwrap()
        .current();
    // Only inventory metadata stays available: the bulk material is gone from
    // both the cache and the origin, so the refresh can only reuse what loaded.
    let cache = ObjectCache::open(root.path().join("peer/objects")).unwrap();
    // A worker caches its database units and the public parameters.
    let bulk: std::collections::BTreeMap<_, _> = inventory
        .units
        .iter()
        .chain([&inventory.public])
        .map(|object| (object.sha256.clone(), object))
        .collect();
    for object in bulk.values() {
        fs::remove_file(cache.object_path(object).unwrap()).unwrap();
    }
    for object in inventory.referenced_objects() {
        source.objects.write().unwrap().remove(&object.sha256);
    }

    let mut refreshed = inventory.clone();
    refreshed.manifest.generation = 5;
    let refreshed_object = publish(&mut refreshed, 8);
    assert_eq!(refreshed.material_identity(), inventory.material_identity());
    let requested = source.requested_objects.read().unwrap().len();
    assert_eq!(committed(task.synchronize().await.unwrap()).0, 5);
    assert_eq!(
        &source.requested_objects.read().unwrap()[requested..],
        std::slice::from_ref(&refreshed_object.sha256)
    );
    assert_eq!(serving_manifest_id(&worker).await, refreshed.manifest.id());
    let current = worker
        .state
        .lock()
        .await
        .controller
        .clone()
        .unwrap()
        .current();
    assert!(Arc::ptr_eq(current.public(), loaded.public()));
    // Held generations would make the controller defer the next preparation.
    drop((current, loaded));

    // Reusing material never skips the live authority check.
    let mut withheld = refreshed.clone();
    withheld.manifest.generation = 6;
    publish(&mut withheld, 9);
    source.canonical.store(false, Ordering::SeqCst);
    let error = task.synchronize().await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<PublicationLifecycleError>(),
        Some(&PublicationLifecycleError::CandidateInvalidated),
        "{error}"
    );
    assert_ne!(serving_manifest_id(&worker).await, withheld.manifest.id());
    assert!(worker.serving().await.is_err());
    server.abort();
}

/// The candidate's own pre-commit authority check is bound to the exact
/// envelope it was revalidated under: an advance, a withdrawal or a replayed
/// answer for an earlier envelope never activates it.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_candidate_activates_only_under_a_live_answer_for_its_exact_envelope() {
    use status_shared::fixture::{self, NETWORK, SALT};
    let (root, _service, task, source, tls, server) = setup().await;
    drop(task);
    let (generation, _) =
        Generation::prepare(&fixture::snapshot(32, false).unwrap(), 4, 2, now_ms(), None).unwrap();
    let mut inventory = export(&source, &generation);
    let object = publish_inventory(&source, &mut inventory);
    *source.current.write().unwrap() = Current {
        scope: scope(NETWORK, SALT),
        sequence: 7,
        ..fixture::publishing(&inventory, &object)
    };
    let worker = Service::open(
        Role::Worker,
        NETWORK,
        root.path().join("peer"),
        String::new(),
    )
    .unwrap();
    let mut task = synchronization(
        worker.clone(),
        &role_config(Role::Worker, root.path().join("peer"), &tls),
    )
    .unwrap();

    // The publisher advances while checking the candidate. Its live answer
    // for sequence 7 is captured first, to be replayed afterwards.
    let candidate = (
        inventory.manifest.anchor_height,
        inventory.manifest.anchor_hash,
    );
    let answer = |authority, subject, still_authoritative| AnchorCheckResponse {
        authority,
        answers: vec![SubjectAnswer {
            subject,
            still_authoritative,
        }],
    };
    let earlier = answer(
        source.current.read().unwrap().authority().unwrap(),
        candidate,
        true,
    );
    source.advance_during_check.store(true, Ordering::SeqCst);
    assert_eq!(deferred(task.synchronize().await.unwrap()), 0);
    assert!(worker.state.lock().await.controller.is_none());
    assert_eq!(task.floor().envelope().unwrap().sequence, 8);

    // An answer replayed from the earlier envelope is not live authority.
    *source.replayed_check.write().unwrap() = Some(serde_json::to_vec(&earlier).unwrap());
    let error = task.synchronize().await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<PublicationLifecycleError>(),
        Some(&PublicationLifecycleError::AuthorityResponseMismatch),
        "{error}"
    );
    assert!(worker.state.lock().await.controller.is_none());

    // A live-bound answer of the same count for another anchor neither
    // confirms the candidate nor, being negative, invalidates it.
    let mut other = candidate;
    other.1[0] ^= 1;
    let substituted = answer(
        source.current.read().unwrap().authority().unwrap(),
        other,
        false,
    );
    *source.replayed_check.write().unwrap() = Some(serde_json::to_vec(&substituted).unwrap());
    let error = task.synchronize().await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<PublicationLifecycleError>(),
        Some(&PublicationLifecycleError::AuthorityResponseMismatch),
        "{error}"
    );
    assert!(worker.state.lock().await.controller.is_none());
    assert!(!task
        .floor()
        .is_invalidated(&ViewIdentity(inventory.manifest.id())));
    *source.replayed_check.write().unwrap() = None;

    // A withdrawal during the candidate's check leaves nothing serving.
    source.advance_during_check.store(true, Ordering::SeqCst);
    source.withdraw_during_check.store(true, Ordering::SeqCst);
    deferred(task.synchronize().await.unwrap());
    assert!(!task.floor().envelope().unwrap().eligible);
    assert!(worker.state.lock().await.controller.is_none());

    // The same material republished under a live answer does activate.
    inventory.manifest.generation = 5;
    let object = publish_inventory(&source, &mut inventory);
    {
        let mut current = source.current.write().unwrap();
        current.sequence += 1;
        current.eligible = true;
        current.publication = Some(inventory.publication_reference(&object));
    }
    assert_eq!(committed(task.synchronize().await.unwrap()).0, 5);
    assert_eq!(serving_manifest_id(&worker).await, inventory.manifest.id());
    server.abort();
}

/// A publisher refreshing its observation while a candidate loads publishes
/// the same material under new metadata. The loaded material commits under
/// the latest metadata, after its live check, instead of failing as stale.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_metadata_refresh_during_the_load_commits_the_loaded_material_as_latest() {
    use status_shared::fixture::{self, NETWORK, SALT};
    let (root, _service, task, source, tls, server) = setup().await;
    drop(task);
    let (generation, _) =
        Generation::prepare(&fixture::snapshot(32, false).unwrap(), 4, 2, now_ms(), None).unwrap();
    let mut inventory = export(&source, &generation);
    let object = publish_inventory(&source, &mut inventory);
    *source.current.write().unwrap() = Current {
        scope: scope(NETWORK, SALT),
        sequence: 7,
        ..fixture::publishing(&inventory, &object)
    };
    let mut refreshed = inventory.clone();
    refreshed.manifest.generation = 5;
    let refreshed_object = publish_inventory(&source, &mut refreshed);
    *source.publish_on_request.write().unwrap() = Some((
        inventory.public.sha256.clone(),
        Current {
            scope: scope(NETWORK, SALT),
            sequence: 8,
            ..fixture::publishing(&refreshed, &refreshed_object)
        },
    ));
    let worker = Service::open(
        Role::Worker,
        NETWORK,
        root.path().join("peer"),
        String::new(),
    )
    .unwrap();
    let mut task = synchronization(
        worker.clone(),
        &role_config(Role::Worker, root.path().join("peer"), &tls),
    )
    .unwrap();

    assert_eq!(committed(task.synchronize().await.unwrap()).0, 5);
    assert!(source.publish_on_request.read().unwrap().is_none());
    assert_eq!(serving_manifest_id(&worker).await, refreshed.manifest.id());
    assert_eq!(task.floor().envelope().unwrap().sequence, 8);
    server.abort();
}
