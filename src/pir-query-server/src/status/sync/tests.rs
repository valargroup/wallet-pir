use super::*;
use crate::status::routes::{provider_commit, provider_pin};
use crate::synchronization::{publication_lifecycle::PublicationLifecycleError, SyncTask};
use axum::extract::{Path, State};
use axum::{
    body::Bytes,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use pir_publication::{
    conditional_token, SubjectAnswer, AUTHORITY_CHECK_PATH, CURRENT_PATH, OBJECTS_PATH,
};
use status_shared::protocol::{MAX_AGE_MS, PROTOCOL};
use std::fs;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex, OnceLock, RwLock,
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
    objects: RwLock<std::collections::HashMap<String, Bytes>>,
    /// Every object id requested, in order.
    requested_objects: RwLock<Vec<String>>,
    /// An envelope published once the named object is requested.
    publish_on_request: RwLock<Option<(String, Current)>>,
    pause_download: Mutex<Option<DownloadPause>>,
}
struct DownloadPause {
    object: String,
    reached: Arc<tokio::sync::Notify>,
    resume: Arc<tokio::sync::Notify>,
}
#[cfg(feature = "native-reinspiring")]
use crate::status::status_http as http;
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
        pause_download: Mutex::default(),
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
                {
                    let mut publish = objects.publish_on_request.write().unwrap();
                    if publish.as_ref().is_some_and(|(object, _)| *object == id) {
                        *objects.current.write().unwrap() = publish.take().unwrap().1;
                    }
                }
                let pause = {
                    let mut pause = objects.pause_download.lock().unwrap();
                    if pause.as_ref().is_some_and(|pause| pause.object == id) {
                        pause.take()
                    } else {
                        None
                    }
                };
                if let Some(pause) = pause {
                    pause.reached.notify_one();
                    pause.resume.notified().await;
                }
                let served = objects.objects.read().unwrap().get(&id).cloned();
                served.unwrap_or_else(|| Bytes::from_static(b"not an inventory"))
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
        "waiting_for_publication"
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

// Fast keeps one negative-answer outcome on a prepared candidate. This loop
// rebuilds the same session twice, once for each anchor, so qualification
// repeats it.
#[cfg(feature = "qualification-tests")]
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
    let _ = provider_commit(State(service.clone()), Path(hex::encode(first)))
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
async fn a_new_router_attempt_replaces_an_abandoned_hold_without_releasing_the_serving_view() {
    let (_root, service, _task, _source, _tls, server) = setup().await;
    let controller = service.state.lock().await.controller.clone().unwrap();
    let serving_id = controller.current().manifest().id();
    let _ = provider_pin(State(service.clone()), Path(hex::encode(serving_id)))
        .await
        .unwrap();
    let _ = provider_commit(State(service.clone()), Path(hex::encode(serving_id)))
        .await
        .unwrap();

    let mut incoming = manifest();
    incoming.generation += 1;
    let abandoned_id = incoming.id();
    let incoming = Generation::public_only(incoming, Arc::new(vec![1]));
    controller.activate(incoming.clone()).unwrap();
    let _ = provider_pin(State(service.clone()), Path(hex::encode(abandoned_id)))
        .await
        .unwrap();
    // Download failure or cancellation leaves this early reservation behind.
    // A normal metadata refresh can advance without allocating a third material.
    let newest = incoming.reidentified(incoming.manifest().generation + 1, 2, now_ms());
    let newest_id = newest.manifest().id();
    controller.activate(newest).unwrap();
    let pin = provider_pin(State(service.clone()), Path(hex::encode(newest_id))).await;
    assert!(
        pin.is_ok(),
        "a failed attempt cannot occupy the incoming slot: {pin:?}"
    );
    let held = service
        .state
        .lock()
        .await
        .router_holds
        .iter()
        .map(|g| g.manifest().id())
        .collect::<Vec<_>>();
    assert_eq!(held, [serving_id, newest_id]);
    assert!(controller.resolve(serving_id).is_ok());
    assert!(
        !controller.can_prepare(),
        "the serving predecessor still bounds preparation"
    );

    // Retried acknowledgments and reservations for the serving view cannot
    // cancel the next preparation, and a late abandoned request cannot win.
    let _ = provider_commit(State(service.clone()), Path(hex::encode(serving_id)))
        .await
        .unwrap();
    let _ = provider_pin(State(service.clone()), Path(hex::encode(serving_id)))
        .await
        .unwrap();
    assert_eq!(
        provider_pin(State(service.clone()), Path(hex::encode(abandoned_id)))
            .await
            .err(),
        Some(StatusCode::CONFLICT)
    );
    assert_eq!(
        service
            .state
            .lock()
            .await
            .router_holds
            .iter()
            .map(|g| g.manifest().id())
            .collect::<Vec<_>>(),
        held
    );

    // A refused reservation must not release either valid hold.
    assert_eq!(
        provider_pin(State(service.clone()), Path(hex::encode([0xff; 32])))
            .await
            .err(),
        Some(StatusCode::CONFLICT)
    );
    assert_eq!(
        service
            .state
            .lock()
            .await
            .router_holds
            .iter()
            .map(|g| g.manifest().id())
            .collect::<Vec<_>>(),
        held
    );
    assert_eq!(
        provider_commit(State(service.clone()), Path(hex::encode(abandoned_id)))
            .await
            .err(),
        Some(StatusCode::CONFLICT)
    );
    let _ = provider_commit(State(service.clone()), Path(hex::encode(newest_id)))
        .await
        .unwrap();
    drop(incoming);
    assert_eq!(service.state.lock().await.router_holds.len(), 1);
    assert!(controller.can_prepare());
    let mut adapter = StatusPublication {
        service: service.clone(),
        loaded_inventory: None,
        poll_soon: false,
    };
    adapter
        .invalidate_views(vec![ViewIdentity(newest_id)])
        .await;
    assert!(service.state.lock().await.router_holds.is_empty());
    assert_eq!(service.state.lock().await.router_committed, None);
    server.abort();
}

#[tokio::test]
async fn startup_attempts_do_not_accumulate_uncommitted_router_holds() {
    let (_root, service, _task, _source, _tls, server) = setup().await;
    let controller = service.state.lock().await.controller.clone().unwrap();
    for generation in 4..7 {
        let mut next = manifest();
        next.generation = generation;
        let id = next.id();
        controller
            .activate(Generation::public_only(next, Arc::new(vec![])))
            .unwrap();
        let _ = provider_pin(State(service.clone()), Path(hex::encode(id)))
            .await
            .unwrap();
        assert_eq!(
            service.state.lock().await.router_holds.len(),
            1,
            "before the first commit only the latest attempt needs retention"
        );
    }
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
        "waiting_for_worker"
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
    let returned = crate::synchronization::SyncManager::new(task).run().await;
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
        "waiting_for_authority"
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

// Fast already refuses a floor save that cannot replace the durable file.
// This repair-and-retry loop rebuilds that session, so qualification owns it.
#[cfg(feature = "qualification-tests")]
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
        .insert(object.sha256.clone(), bytes.into());
    object
}
/// Share immutable material bytes while giving each publisher its own object map.
/// Publication metadata is copied from this case's generation, and every
/// consumer still downloads and verifies objects into a fresh writable cache.
fn export(origin: &Origin, generation: &Generation) -> Inventory {
    struct Material {
        inventory: Inventory,
        objects: std::collections::HashMap<String, Bytes>,
    }
    type Key = ([u8; 32], [u8; 32]);
    static CACHE: OnceLock<Mutex<std::collections::HashMap<Key, Material>>> = OnceLock::new();
    let manifest = generation.manifest();
    let mut cache = CACHE.get_or_init(Default::default).lock().unwrap();
    let material = cache
        .entry((manifest.rows_digest, manifest.public_digest))
        .or_insert_with(|| {
            let mut objects = std::collections::HashMap::new();
            let inventory = generation
                .export_prepared(|bytes| {
                    let object = ObjectReference::for_bytes(bytes);
                    objects.insert(object.sha256.clone(), Bytes::copy_from_slice(bytes));
                    Ok(object)
                })
                .unwrap();
            Material { inventory, objects }
        });
    origin
        .objects
        .write()
        .unwrap()
        .extend(material.objects.clone());
    let mut inventory = material.inventory.clone();
    inventory.manifest = manifest.clone();
    inventory
}
fn committed(pass: PublicationPass<u64>) -> (u64, PeerHandover) {
    match pass {
        PublicationPass::Committed { report, handover } => (report, handover),
        PublicationPass::Deferred { report } => panic!("deferred at generation {report}"),
    }
}

/// One prepared database per snapshot, stamped with each case's publication identity.
///
/// Generation numbers and freshness do not change the database. Sync tests
/// exercise loading and authority over that material; preparation and incremental
/// updates have their own independent tests in status-shared.
fn cached_generation(
    snapshot: &status_shared::index::Snapshot,
    generation: u64,
    recovery_epoch: u64,
    observed_ms: u64,
    previous: Option<&Generation>,
) -> Generation {
    static CACHE: OnceLock<Mutex<std::collections::HashMap<[u8; 32], Generation>>> =
        OnceLock::new();
    let mut cache = CACHE.get_or_init(Default::default).lock().unwrap();
    let prepared = cache.entry(snapshot.digest).or_insert_with(|| {
        Generation::prepare(snapshot, generation, recovery_epoch, 1, previous)
            .unwrap()
            .0
    });
    prepared.reidentified(generation, recovery_epoch, observed_ms)
}

/// An expired publication is expected source coordination. It must not make
/// the next fresh publication wait behind transport-failure backoff.
#[tokio::test]
async fn expired_inventory_keeps_the_status_poll_cadence() {
    use status_shared::fixture::{geometric_inventory, publishing};
    let (_root, service, task, source, tls, server) = setup().await;
    let (mut active, _) = geometric_inventory(7, 3);
    active.manifest.observed_ms = now_ms();
    service.state.lock().await.controller = Some(Controller::new(Generation::public_only(
        active.manifest.clone(),
        Arc::new(Vec::new()),
    )));
    let task = PublicationLifecycle::new(
        StatusPublication {
            service: service.clone(),
            loaded_inventory: Some(active.clone()),
            poll_soon: false,
        },
        super::super::source(&role_config(Role::Worker, service.state_dir.clone(), &tls)).unwrap(),
        AuthorityFloor::open(
            service.state_dir.join(FLOOR_FILE),
            task.floor().source().clone(),
        )
        .unwrap(),
        ObjectCache::open(service.state_dir.join("objects")).unwrap(),
    )
    .unwrap();
    let mut expired = active.clone();
    expired.manifest.generation = 4;
    expired.manifest.observed_ms = now_ms() - MAX_AGE_MS - 1;
    let expired_bytes = serde_json::to_vec(&expired).unwrap();
    let expired_object = ObjectReference::for_bytes(&expired_bytes);
    source
        .objects
        .write()
        .unwrap()
        .insert(expired_object.sha256.clone(), expired_bytes.into());
    *source.current.write().unwrap() = publishing(&expired, &expired_object);
    let mut refreshed = active;
    refreshed.manifest.generation = 5;
    let refreshed_object = publish_inventory(&source, &mut refreshed);
    let mut next = publishing(&refreshed, &refreshed_object);
    next.sequence += 1;

    #[derive(Debug)]
    struct Finished;
    impl std::fmt::Display for Finished {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("the two observed polling passes completed")
        }
    }
    impl std::error::Error for Finished {}
    struct Polling {
        task: StatusSynchronization,
        source: Arc<Origin>,
        next: Current,
        waits: Arc<Mutex<Vec<Duration>>>,
        outcomes: Arc<Mutex<Vec<&'static str>>>,
        passes: usize,
    }
    impl SyncTask for Polling {
        type Report = PublicationPass<u64>;
        type Error = Error;
        async fn synchronize(&mut self) -> Result<Self::Report, Error> {
            if self.passes == 2 {
                return Err(Box::new(Finished));
            }
            self.passes += 1;
            let result = self.task.synchronize().await;
            self.outcomes.lock().unwrap().push(match &result {
                Ok(PublicationPass::Deferred { .. }) => "deferred",
                Ok(PublicationPass::Committed { .. }) => "committed",
                Err(_) => "error",
            });
            result
        }
        fn terminal(&self, error: &Error) -> bool {
            error.is::<Finished>()
        }
        async fn wait_until(&mut self, deadline: tokio::time::Instant) {
            self.waits
                .lock()
                .unwrap()
                .push(deadline.saturating_duration_since(tokio::time::Instant::now()));
            // Advance the publisher at the first wait without sleeping through
            // the recorded production delay. The next pass uses real HTTPS.
            *self.source.current.write().unwrap() = self.next.clone();
        }
    }
    let waits = Arc::new(Mutex::new(Vec::new()));
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let polling = Polling {
        task,
        source,
        next,
        waits: waits.clone(),
        outcomes: outcomes.clone(),
        passes: 0,
    };
    let error = crate::synchronization::SyncManager::new(polling)
        .with_success_interval(Duration::from_secs(1))
        .run()
        .await;
    assert!(error.is::<Finished>());
    eprintln!(
        "Status waits after expired/fresh inventories: {:?}",
        waits.lock().unwrap()
    );
    assert_eq!(*outcomes.lock().unwrap(), ["deferred", "committed"]);
    assert!(waits
        .lock()
        .unwrap()
        .iter()
        .all(|wait| *wait <= Duration::from_secs(1)));
    assert_eq!(serving_manifest_id(&service).await, refreshed.manifest.id());
    assert!(service.serving().await.is_ok());
    server.abort();
}

/// Run the real HTTPS lifecycle before asking its production polling wrapper
/// for the next deadline. No twenty-second wall-clock sleep is needed.
#[tokio::test]
async fn status_polling_reserves_time_for_a_fresh_handover() {
    use status_shared::fixture::{geometric_inventory, publishing};
    use tower::ServiceExt;
    for age in [0, MAX_AGE_MS / 4, MAX_AGE_MS + 1] {
        let (_root, service, original, source, tls, server) = setup().await;
        let (mut active, _) = geometric_inventory(7, 3);
        active.manifest.observed_ms = now_ms() - age;
        let bytes = serde_json::to_vec(&active).unwrap();
        let object = ObjectReference::for_bytes(&bytes);
        source
            .objects
            .write()
            .unwrap()
            .insert(object.sha256.clone(), bytes.into());
        *source.current.write().unwrap() = publishing(&active, &object);
        service.state.lock().await.controller = Some(Controller::new(Generation::public_only(
            active.manifest.clone(),
            Arc::new(Vec::new()),
        )));
        let task = PublicationLifecycle::new(
            StatusPublication {
                service: service.clone(),
                loaded_inventory: Some(active.clone()),
                poll_soon: false,
            },
            super::super::source(&role_config(Role::Worker, service.state_dir.clone(), &tls))
                .unwrap(),
            AuthorityFloor::open(
                service.state_dir.join(FLOOR_FILE),
                original.floor().source().clone(),
            )
            .unwrap(),
            ObjectCache::open(service.state_dir.join("objects")).unwrap(),
        )
        .unwrap();
        let mut polling = manager(task);
        assert!(polling.poll_once().await.is_ok());
        let before = tokio::time::Instant::now();
        let minimum = before + FAST_POLL;
        let deadline = polling.task().poll_deadline(minimum).await;
        if age < MAX_AGE_MS / 4 {
            assert!(deadline >= before + NORMAL_POLL);
        } else {
            assert_eq!(deadline, minimum);
            // A handover arriving 80ms after a poll, followed by 600ms of
            // loading, fits a final 800ms budget with the selected cadence.
            // The former one-second wait alone exhausts that same budget.
            let remaining = Duration::from_millis(800);
            assert!(deadline.duration_since(before) + Duration::from_millis(600) < remaining);
            assert!(NORMAL_POLL > remaining);
        }
        // Faster synchronization never makes an expired view answer a wallet.
        let mut router = service.clone();
        router.role = Role::Router;
        let response = router
            .query_routes()
            .oneshot(
                axum::http::Request::get("/v1/status/init")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if age > MAX_AGE_MS {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::OK
            }
        );
        if age == 0 {
            let controller = service.state.lock().await.controller.clone().unwrap();
            controller.invalidate([active.manifest.id()]);
            let minimum = tokio::time::Instant::now() + FAST_POLL;
            assert_eq!(
                polling.task().poll_deadline(minimum).await,
                minimum,
                "a young but invalidated observation cannot use the healthy cadence"
            );
            assert_eq!(
                controller.check(&controller.current()),
                Err(StatusCode::GONE)
            );
        }
        server.abort();
    }

    let (_root, service, task, source, _tls, server) = setup().await;
    service.state.lock().await.controller = None;
    withdraw(&mut source.current.write().unwrap());
    let mut polling = manager(task);
    assert!(polling.poll_once().await.is_ok());
    let minimum = tokio::time::Instant::now() + FAST_POLL;
    assert_eq!(
        polling.task().poll_deadline(minimum).await,
        minimum,
        "no serving observation needs a prompt next check"
    );
    server.abort();

    let (_root, _service, task, _source, _tls, server) = setup().await;
    let mut polling = manager(task);
    assert!(
        polling.poll_once().await.is_err(),
        "malformed inventory remains a failure"
    );
    let backoff = tokio::time::Instant::now() + Duration::from_secs(10);
    assert_eq!(
        polling.task().poll_deadline(backoff).await,
        backoff,
        "the manager's genuine failure backoff must remain intact"
    );
    server.abort();
}

#[tokio::test]
async fn retention_wait_polls_promptly_without_loading_a_third_material() {
    use status_shared::fixture::{geometric_inventory, publishing};
    let (_root, service, original, source, tls, server) = setup().await;
    let (mut older, _) = geometric_inventory(6, 2);
    let (mut active, _) = geometric_inventory(7, 3);
    let (mut next, _) = geometric_inventory(8, 4);
    for inventory in [&mut older, &mut active, &mut next] {
        inventory.manifest.observed_ms = now_ms();
    }
    let controller = Controller::new(Generation::public_only(older.manifest, Arc::new(vec![0])));
    let held = controller.current();
    controller
        .activate(Generation::public_only(
            active.manifest.clone(),
            Arc::new(vec![1]),
        ))
        .unwrap();
    service.state.lock().await.controller = Some(controller.clone());
    let object = publish_inventory(&source, &mut next);
    *source.current.write().unwrap() = publishing(&next, &object);
    let task = PublicationLifecycle::new(
        StatusPublication {
            service: service.clone(),
            loaded_inventory: Some(active),
            poll_soon: false,
        },
        super::super::source(&role_config(Role::Worker, service.state_dir.clone(), &tls)).unwrap(),
        AuthorityFloor::open(
            service.state_dir.join(FLOOR_FILE),
            original.floor().source().clone(),
        )
        .unwrap(),
        ObjectCache::open(service.state_dir.join("objects")).unwrap(),
    )
    .unwrap();
    let mut polling = manager(task);
    assert_eq!(deferred(polling.poll_once().await.unwrap()), 3);
    assert_eq!(
        service.state.lock().await.synchronization,
        "waiting_for_retention"
    );
    let minimum = tokio::time::Instant::now() + FAST_POLL;
    assert_eq!(polling.task().poll_deadline(minimum).await, minimum);
    assert!(
        !controller.can_prepare(),
        "the older material is still pinned"
    );
    assert_eq!(
        *source.requested_objects.read().unwrap(),
        [object.sha256],
        "polling fetched only metadata and did not load a third material"
    );
    drop(held);
    assert!(
        controller.can_prepare(),
        "releasing the old material permits progress"
    );
    server.abort();
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
    let b = cached_generation(&fixture::snapshot(32, false).unwrap(), 4, 2, now_ms(), None);
    let c = cached_generation(
        &fixture::snapshot(32, true).unwrap(),
        5,
        2,
        now_ms(),
        Some(&b),
    );
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
    let _ = provider_commit(State(worker.clone()), Path(hex::encode(a.manifest().id())))
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
    let generation =
        cached_generation(&fixture::snapshot(32, false).unwrap(), 4, 2, now_ms(), None);
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
    drop(current);

    // Keep the original view pinned, as the router does during a handover.
    // Metadata may advance without allocating another material generation.
    assert!(worker
        .state
        .lock()
        .await
        .controller
        .as_ref()
        .unwrap()
        .can_prepare());
    let mut newest = refreshed.clone();
    newest.manifest.generation = 6;
    let newest_object = publish(&mut newest, 9);
    let requested = source.requested_objects.read().unwrap().len();
    assert_eq!(committed(task.synchronize().await.unwrap()).0, 6);
    assert_eq!(
        &source.requested_objects.read().unwrap()[requested..],
        std::slice::from_ref(&newest_object.sha256)
    );
    assert!(Arc::ptr_eq(
        worker
            .state
            .lock()
            .await
            .controller
            .as_ref()
            .unwrap()
            .current()
            .public(),
        loaded.public()
    ));

    // A pin sharing the active allocations permits the next different material.
    let changed = cached_generation(
        &fixture::snapshot(32, true).unwrap(),
        7,
        2,
        now_ms(),
        Some(&loaded),
    );
    let mut changed = export(&source, &changed);
    publish(&mut changed, 10);
    assert_eq!(committed(task.synchronize().await.unwrap()).0, 7);
    let controller = worker.state.lock().await.controller.clone().unwrap();
    assert!(
        !controller.can_prepare(),
        "the predecessor now pins different material"
    );
    assert!(controller.resolve(loaded.manifest().id()).is_ok());
    drop(loaded);
    assert!(controller.can_prepare());

    // Reusing material never skips the live authority check.
    let mut withheld = changed.clone();
    withheld.manifest.generation = 8;
    publish(&mut withheld, 11);
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

/// A worker whose first candidate check saw the publisher advance: the
/// candidate was deferred, nothing serves, and the floor holds the newer
/// envelope. The sibling cases below continue from here.
struct AdvancedCandidate {
    _root: tempfile::TempDir,
    _tls: tls::Tls,
    _service: Service,
    source: Arc<Origin>,
    server: tokio::task::JoinHandle<()>,
    worker: Service,
    task: StatusSynchronization,
    inventory: Inventory,
    /// The publisher's live answer for sequence 7, captured before it
    /// advanced, to be replayed afterwards.
    earlier: AnchorCheckResponse,
}

async fn candidate_deferred_by_an_advance() -> AdvancedCandidate {
    use status_shared::fixture::{self, NETWORK, SALT};
    let (root, service, task, source, tls, server) = setup().await;
    drop(task);
    let generation =
        cached_generation(&fixture::snapshot(32, false).unwrap(), 4, 2, now_ms(), None);
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

    // The publisher advances while checking the candidate.
    let earlier = AnchorCheckResponse {
        authority: source.current.read().unwrap().authority().unwrap(),
        answers: vec![SubjectAnswer {
            subject: (
                inventory.manifest.anchor_height,
                inventory.manifest.anchor_hash,
            ),
            still_authoritative: true,
        }],
    };
    source.advance_during_check.store(true, Ordering::SeqCst);
    assert_eq!(deferred(task.synchronize().await.unwrap()), 0);
    assert!(worker.state.lock().await.controller.is_none());
    assert_eq!(task.floor().envelope().unwrap().sequence, 8);
    AdvancedCandidate {
        _root: root,
        _tls: tls,
        _service: service,
        source,
        server,
        worker,
        task,
        inventory,
        earlier,
    }
}

/// The candidate's own pre-commit authority check is bound to the exact
/// envelope it was revalidated under: neither an answer replayed from an
/// earlier envelope nor a live answer for another anchor activates it.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_candidate_requires_live_authority_before_activation() {
    let AdvancedCandidate {
        source,
        server,
        worker,
        mut task,
        mut inventory,
        earlier,
        // Hold the directories, TLS terminator and service until the end.
        _root,
        _tls,
        _service,
        ..
    } = candidate_deferred_by_an_advance().await;

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
    let mut other = (
        inventory.manifest.anchor_height,
        inventory.manifest.anchor_hash,
    );
    other.1[0] ^= 1;
    let substituted = AnchorCheckResponse {
        authority: source.current.read().unwrap().authority().unwrap(),
        answers: vec![SubjectAnswer {
            subject: other,
            still_authoritative: false,
        }],
    };
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
    let generation =
        cached_generation(&fixture::snapshot(32, false).unwrap(), 4, 2, now_ms(), None);
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

/// New data published while a worker loads does not require a quiet publisher
/// before the worker can activate its still-fresh, live-checked candidate.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn changed_material_during_loading_still_allows_forward_progress() {
    use status_shared::fixture::{self, NETWORK, SALT};
    let (root, _service, task, source, tls, server) = setup().await;
    drop(task);
    let first = cached_generation(&fixture::snapshot(32, false).unwrap(), 4, 2, now_ms(), None);
    let next = cached_generation(
        &fixture::snapshot(32, true).unwrap(),
        5,
        2,
        now_ms(),
        Some(&first),
    );
    // Fixture preparation can contend with other native tests. Start the
    // observation window once all synthetic material is ready, before any
    // publication or synchronization; loading must preserve this timestamp.
    let observed_ms = now_ms();
    let first = first.reobserved(observed_ms);
    let next = next.reobserved(observed_ms);
    // The reference profile's fast case already prepared two distinct
    // databases above. A third 96 MiB generation does not fit the case
    // deadline beside the rest of the pool, so the following generation
    // reuses the second database under a new publication identity. The native
    // wallet path still prepares the extra transaction below.
    #[cfg(not(feature = "native-reinspiring"))]
    let third = next.reidentified(6, 2, observed_ms);
    #[cfg(feature = "native-reinspiring")]
    let third = {
        let (blocks, mut mempool, forks) = fixture::source(32, true);
        mempool.push(fixture::txid(u64::MAX - 3));
        let third_snapshot =
            status_shared::index::Snapshot::build(NETWORK, SALT, &blocks, &mempool, &forks)
                .unwrap();
        cached_generation(&third_snapshot, 6, 2, observed_ms, Some(&next)).reobserved(observed_ms)
    };
    let mut inventory = export(&source, &first);
    let object = publish_inventory(&source, &mut inventory);
    let mut newer = export(&source, &next);
    let newer_object = publish_inventory(&source, &mut newer);
    let mut newest = export(&source, &third);
    let newest_object = publish_inventory(&source, &mut newest);
    assert_ne!(inventory.material_identity(), newer.material_identity());
    *source.current.write().unwrap() = Current {
        scope: scope(NETWORK, SALT),
        sequence: 7,
        ..fixture::publishing(&inventory, &object)
    };
    *source.publish_on_request.write().unwrap() = Some((
        inventory.public.sha256.clone(),
        Current {
            scope: scope(NETWORK, SALT),
            sequence: 8,
            ..fixture::publishing(&newer, &newer_object)
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

    assert_eq!(committed(task.synchronize().await.unwrap()).0, 4);
    assert_eq!(serving_manifest_id(&worker).await, inventory.manifest.id());
    assert_eq!(task.floor().envelope().unwrap().sequence, 8);
    assert_eq!(
        worker
            .state
            .lock()
            .await
            .controller
            .as_ref()
            .unwrap()
            .current()
            .manifest()
            .observed_ms,
        inventory.manifest.observed_ms,
        "loading and authority checks cannot renew observation age"
    );
    // Keep publishing during the next load too: no quiet pass is needed to
    // activate either candidate, and the consumer then reaches the newest one.
    let changed_unit = newer
        .units
        .iter()
        .find(|unit| !inventory.units.contains(unit))
        .unwrap();
    *source.publish_on_request.write().unwrap() = Some((
        changed_unit.sha256.clone(),
        Current {
            scope: scope(NETWORK, SALT),
            sequence: 9,
            ..fixture::publishing(&newest, &newest_object)
        },
    ));
    #[cfg(feature = "native-reinspiring")]
    {
        let peer = http::serve(worker.control_routes()).await;
        let router = Service::open(
            Role::Router,
            NETWORK,
            root.path().join("router"),
            peer.origin.clone(),
        )
        .unwrap();
        let mut router_task = synchronization(
            router.clone(),
            &role_config(Role::Router, root.path().join("router"), &tls),
        )
        .unwrap();
        assert_eq!(committed(router_task.synchronize().await.unwrap()).0, 4);
        let public = http::serve(router.query_routes()).await;
        let transport = http::Loopback {
            origin: public.origin.clone(),
            http: reqwest::Client::new(),
            corrupt_answer: AtomicBool::new(false),
        };
        use zakura_pir_client::status as wallet;
        for (step, mempool) in [false, true, true].into_iter().enumerate() {
            if step > 0 {
                // Wallet work on the preceding view can take seconds. Model
                // the publisher's continuing observations of the next state,
                // before loading it; never renew a candidate during its load.
                let mut observed = if step == 1 {
                    newer.clone()
                } else {
                    newest.clone()
                };
                observed.manifest.generation = 4 + 2 * step as u64;
                let object = publish_inventory(&source, &mut observed);
                *source.current.write().unwrap() = Current {
                    scope: scope(NETWORK, SALT),
                    sequence: 7 + 2 * step as u64,
                    ..fixture::publishing(&observed, &object)
                };
                if step == 1 {
                    // New material still appears during this load as well.
                    let mut advancing = newest.clone();
                    advancing.manifest.generation = 7;
                    let object = publish_inventory(&source, &mut advancing);
                    *source.publish_on_request.write().unwrap() = Some((
                        changed_unit.sha256.clone(),
                        Current {
                            scope: scope(NETWORK, SALT),
                            sequence: 10,
                            ..fixture::publishing(&advancing, &object)
                        },
                    ));
                } else {
                    newest = observed;
                }
                assert_eq!(
                    committed(task.synchronize().await.unwrap()).0,
                    4 + 2 * step as u64
                );
                assert_eq!(
                    committed(router_task.synchronize().await.unwrap()).0,
                    4 + 2 * step as u64
                );
            }
            let snapshot = fixture::snapshot(32, mempool).unwrap();
            let anchor = wallet::AcceptedAnchor {
                network: NETWORK,
                height: snapshot.height,
                hash: snapshot.anchor,
            };
            let coverage = wallet::LocalCoverageContext {
                earliest_possible_inclusion: Some(snapshot.start),
                required_through: Some(snapshot.height),
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
            // One changing answer proves each handover uses the right data.
            // The wallet suite covers all observation states separately; keep
            // this live-clock regression inside its twenty-second window.
            use status_shared::protocol::Observation;
            let (txid, expected) = if step == 2 {
                (fixture::txid(u64::MAX - 3), Observation::Mempool)
            } else {
                fixture::cases(32, mempool).swap_remove(1)
            };
            let expected = match expected {
                Observation::Mined(height) => wallet::Observation::Mined(height),
                Observation::Mempool => wallet::Observation::Mempool,
                Observation::Forked => wallet::Observation::Forked,
                Observation::NotFound => wallet::Observation::NotFound,
            };
            assert_eq!(
                client.observe(&transport, &txid, coverage).await.unwrap(),
                expected
            );
        }
    }
    #[cfg(not(feature = "native-reinspiring"))]
    {
        // Generation 6 reuses generation 5's database, so the load of the new
        // material follows that already-published identity in the same pass.
        assert_eq!(committed(task.synchronize().await.unwrap()).0, 6);
    }
    assert_eq!(serving_manifest_id(&worker).await, newest.manifest.id());
    assert!(source.publish_on_request.read().unwrap().is_none());
    assert_eq!(
        task.floor().envelope().unwrap().sequence,
        if cfg!(feature = "native-reinspiring") {
            11
        } else {
            9
        }
    );
    server.abort();
}

/// A new worker with one real, published candidate and no serving authority.
async fn worker_with_published_candidate() -> (
    tempfile::TempDir,
    Service,
    StatusSynchronization,
    Arc<Origin>,
    tls::Tls,
    tokio::task::JoinHandle<()>,
) {
    use status_shared::fixture::{self, NETWORK, SALT};
    let (root, _service, task, source, tls, server) = setup().await;
    drop(task);
    let generation =
        cached_generation(&fixture::snapshot(32, false).unwrap(), 4, 2, now_ms(), None);
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
    let task = synchronization(
        worker.clone(),
        &role_config(Role::Worker, root.path().join("peer"), &tls),
    )
    .unwrap();
    (root, worker, task, source, tls, server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn an_authority_conflict_reuses_loaded_work_but_requires_a_new_live_answer() {
    let (_root, worker, mut task, source, _tls, server) = worker_with_published_candidate().await;
    source.advance_during_check.store(true, Ordering::SeqCst);
    assert_eq!(deferred(task.synchronize().await.unwrap()), 0);
    assert!(worker.state.lock().await.controller.is_none());
    assert_eq!(
        worker.state.lock().await.synchronization,
        "waiting_for_authority"
    );
    // No cache or source bytes remain to load again. Only the in-memory
    // candidate can finish, and it still needs a fresh authority response.
    for file in fs::read_dir(worker.state_dir.join("objects")).unwrap() {
        fs::remove_file(file.unwrap().path()).unwrap();
    }
    source.objects.write().unwrap().clear();
    source.requested_objects.write().unwrap().clear();
    assert_eq!(committed(task.synchronize().await.unwrap()).0, 4);
    assert!(source.requested_objects.read().unwrap().is_empty());
    assert!(worker.serving().await.is_ok());
    server.abort();
}

/// A prepared candidate cannot carry authority across a withdrawal, recovery
/// or negative answer, even when all of its bytes were already loaded.
// Fast keeps the withdrawal outcome. Recovery and the two negative answers
// rebuild that same published candidate, so qualification repeats them.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn resumed_candidates_are_rejected_after_withdrawal() {
    resumed_candidate_is_rejected("withdrawal").await;
}

#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn resumed_candidates_are_rejected_after_recovery() {
    resumed_candidate_is_rejected("recovery").await;
}

#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn resumed_candidates_are_rejected_after_a_noncanonical_anchor() {
    resumed_candidate_is_rejected("noncanonical").await;
}

#[cfg(feature = "qualification-tests")]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn resumed_candidates_are_rejected_after_a_replayed_answer() {
    resumed_candidate_is_rejected("replayed_answer").await;
}

async fn resumed_candidate_is_rejected(mode: &str) {
    {
        let (_root, worker, mut task, source, _tls, server) =
            worker_with_published_candidate().await;
        let original = source.current.read().unwrap().authority().unwrap();
        source.advance_during_check.store(true, Ordering::SeqCst);
        assert_eq!(deferred(task.synchronize().await.unwrap()), 0);
        match mode {
            "withdrawal" => withdraw(&mut source.current.write().unwrap()),
            "recovery" => {
                let mut current = source.current.write().unwrap();
                current.sequence += 1;
                current.fence.recovery_epoch += 1;
                current.eligible = false;
            }
            "noncanonical" => source.canonical.store(false, Ordering::SeqCst),
            "replayed_answer" => {
                *source.replayed_check.write().unwrap() = Some(
                    serde_json::to_vec(&AnchorCheckResponse {
                        authority: original,
                        answers: vec![SubjectAnswer {
                            subject: (1, [0; 32]),
                            still_authoritative: true,
                        }],
                    })
                    .unwrap(),
                );
            }
            _ => unreachable!(),
        }
        let result = task.synchronize().await;
        if matches!(mode, "withdrawal" | "recovery") {
            assert_eq!(deferred(result.unwrap()), 0);
        } else {
            assert!(
                result.is_err(),
                "{mode} must not authorize a prepared candidate"
            );
        }
        assert!(worker.state.lock().await.controller.is_none(), "{mode}");
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_recovery_during_loading_discards_the_old_candidate_without_panicking() {
    let (_root, worker, mut task, source, _tls, server) = worker_with_published_candidate().await;
    let mut recovered = source.current.read().unwrap().clone();
    let object = recovered.inventory().unwrap().clone();
    let mut inventory: Inventory =
        serde_json::from_slice(&source.objects.read().unwrap()[&object.sha256]).unwrap();
    inventory.manifest.recovery_epoch += 1;
    inventory.manifest.generation += 1;
    let new_object = publish_inventory(&source, &mut inventory);
    recovered.sequence += 1;
    recovered.fence.recovery_epoch += 1;
    recovered.publication = Some(inventory.publication_reference(&new_object));
    *source.publish_on_request.write().unwrap() =
        Some((inventory.public.sha256.clone(), recovered));
    assert_eq!(deferred(task.synchronize().await.unwrap()), 0);
    assert!(worker.state.lock().await.controller.is_none());
    assert_eq!(committed(task.synchronize().await.unwrap()).0, 5);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn prepared_candidate_proof_binds_inventory_source_fence_and_original_age() {
    use status_shared::fixture;
    let (_root, worker, task, source, _tls, server) = worker_with_published_candidate().await;
    let observed = source.current.read().unwrap().clone();
    let inventory_object = observed.inventory().unwrap().clone();
    let inventory: Inventory =
        serde_json::from_slice(&source.objects.read().unwrap()[&inventory_object.sha256]).unwrap();
    let generation = cached_generation(
        &fixture::snapshot(32, false).unwrap(),
        4,
        2,
        inventory.manifest.observed_ms,
        None,
    );
    let mut loaded = StatusLoaded {
        inventory,
        inventory_object,
        authorization: InventoryAuthorization::Latest,
        generation,
        observed: observed.clone(),
    };
    assert!(task.adapter().can_resume(&loaded, &observed));
    for field in ["source", "withdrawal", "epoch", "sequence", "eligible"] {
        let mut changed = observed.clone();
        match field {
            "source" => changed.scope.source[0] ^= 1,
            "withdrawal" => changed.fence.latest_withdrawal += 1,
            "epoch" => changed.fence.recovery_epoch += 1,
            "sequence" => changed.sequence -= 1,
            "eligible" => changed.eligible = false,
            _ => unreachable!(),
        }
        assert!(!task.adapter().can_resume(&loaded, &changed), "{field}");
    }
    let valid = loaded.inventory_object.clone();
    loaded.inventory_object = ObjectReference::for_bytes(b"another inventory");
    assert!(!task.adapter().can_resume(&loaded, &observed));
    loaded.inventory_object = valid;
    // Give the expired inventory a matching original envelope as well: the
    // freshness check must still refuse it rather than renewing its timestamp.
    loaded.inventory.manifest.observed_ms = now_ms() - MAX_AGE_MS - 1000;
    loaded.inventory_object =
        ObjectReference::for_bytes(&serde_json::to_vec(&loaded.inventory).unwrap());
    loaded.observed.publication = Some(
        loaded
            .inventory
            .publication_reference(&loaded.inventory_object),
    );
    assert!(!task.adapter().can_resume(&loaded, &loaded.observed));
    assert!(worker.state.lock().await.controller.is_none());
    server.abort();
}

#[cfg(feature = "native-reinspiring")]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn router_rebinds_loaded_material_to_its_workers_fresh_metadata() {
    use status_shared::fixture::{self, NETWORK, SALT};
    for same_material in [true, false] {
        let (root, _service, initial_task, source, tls, server) = setup().await;
        drop(initial_task);
        let first = cached_generation(&fixture::snapshot(32, false).unwrap(), 4, 2, now_ms(), None)
            .reobserved(now_ms() - 1000);
        let next = if same_material {
            first.reidentified(5, 2, now_ms())
        } else {
            cached_generation(
                &fixture::snapshot(32, true).unwrap(),
                5,
                2,
                now_ms(),
                Some(&first),
            )
        };
        let mut inventory = export(&source, &first);
        let object = publish_inventory(&source, &mut inventory);
        let mut refreshed = export(&source, &next);
        let first = first.reobserved(inventory.manifest.observed_ms);
        let next = next.reobserved(refreshed.manifest.observed_ms);
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
        {
            let mut state = worker.state.lock().await;
            state.controller = Some(Controller::new(first));
            state.inventory = Some(object);
        }
        let peer = http::serve(worker.control_routes()).await;
        let router = Service::open(
            Role::Router,
            NETWORK,
            root.path().join("router"),
            peer.origin.clone(),
        )
        .unwrap();
        let mut task = synchronization(
            router.clone(),
            &role_config(Role::Router, root.path().join("router"), &tls),
        )
        .unwrap();
        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        *source.pause_download.lock().unwrap() = Some(DownloadPause {
            object: inventory.public.sha256.clone(),
            reached: reached.clone(),
            resume: resume.clone(),
        });
        let polling = tokio::spawn(async move { task.synchronize().await });
        tokio::time::timeout(Duration::from_secs(10), reached.notified())
            .await
            .unwrap();
        // The worker must keep the selected view while the router downloads
        // and loads it. Pinning only after that work lets ordinary worker
        // refreshes evict it and forces the router to load the bytes again.
        assert!(worker
            .state
            .lock()
            .await
            .router_holds
            .iter()
            .any(|held| { held.manifest().id() == inventory.manifest.id() }));
        let intermediate = next.clone();
        refreshed.manifest.generation += 1;
        let refreshed_object = publish_inventory(&source, &mut refreshed);
        let next = next.reidentified(
            refreshed.manifest.generation,
            refreshed.manifest.recovery_epoch,
            refreshed.manifest.observed_ms,
        );
        *source.current.write().unwrap() = Current {
            scope: scope(NETWORK, SALT),
            sequence: 8,
            ..fixture::publishing(&refreshed, &refreshed_object)
        };
        {
            let mut state = worker.state.lock().await;
            state
                .controller
                .as_ref()
                .unwrap()
                .activate(intermediate)
                .unwrap();
            state.controller.as_ref().unwrap().activate(next).unwrap();
            assert!(state
                .controller
                .as_ref()
                .unwrap()
                .resolve(inventory.manifest.id())
                .is_ok());
            state.inventory = Some(refreshed_object);
        }
        resume.notify_one();
        let expected = if same_material {
            &refreshed
        } else {
            &inventory
        };
        assert_eq!(
            committed(polling.await.unwrap().unwrap()).0,
            expected.manifest.generation
        );
        assert_eq!(serving_manifest_id(&router).await, expected.manifest.id());
        assert_eq!(
            router
                .state
                .lock()
                .await
                .controller
                .as_ref()
                .unwrap()
                .current()
                .manifest()
                .observed_ms,
            expected.manifest.observed_ms
        );
        assert_eq!(worker.state.lock().await.router_holds.len(), 1);
        assert_eq!(
            worker.state.lock().await.router_holds[0].manifest().id(),
            expected.manifest.id()
        );
        server.abort();
    }
}

#[cfg(feature = "native-reinspiring")]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn router_defers_when_worker_advanced_past_the_observed_envelope() {
    use status_shared::fixture::{self, NETWORK, SALT};
    let (root, _service, initial_task, source, tls, server) = setup().await;
    drop(initial_task);
    let first = cached_generation(&fixture::snapshot(32, false).unwrap(), 4, 2, now_ms(), None)
        .reobserved(now_ms());
    let next = first.reidentified(5, 2, now_ms());
    let mut inventory = export(&source, &first);
    let object = publish_inventory(&source, &mut inventory);
    let mut refreshed = export(&source, &next);
    let refreshed_object = publish_inventory(&source, &mut refreshed);
    let next = next.reobserved(refreshed.manifest.observed_ms);
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
    {
        let mut state = worker.state.lock().await;
        state.controller = Some(Controller::new(next));
        state.inventory = Some(refreshed_object.clone());
    }
    let peer = http::serve(worker.control_routes()).await;
    let router = Service::open(
        Role::Router,
        NETWORK,
        root.path().join("router"),
        peer.origin.clone(),
    )
    .unwrap();
    let mut task = synchronization(
        router.clone(),
        &role_config(Role::Router, root.path().join("router"), &tls),
    )
    .unwrap();
    let pass = task.synchronize().await;
    assert!(
        matches!(pass, Ok(PublicationPass::Deferred { .. })),
        "{pass:?}"
    );
    assert!(router.serving().await.is_err());
    *source.current.write().unwrap() = Current {
        scope: scope(NETWORK, SALT),
        sequence: 8,
        ..fixture::publishing(&refreshed, &refreshed_object)
    };
    assert_eq!(committed(task.synchronize().await.unwrap()).0, 5);
    assert_eq!(serving_manifest_id(&router).await, refreshed.manifest.id());
    server.abort();
}
