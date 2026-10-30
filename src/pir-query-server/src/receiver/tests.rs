use super::*;
use axum::{
    extract::Path,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use pir_pipeline::consumer::{polling::SyncTask, Consumer};
use pir_publication::{
    AuthorityCheckRequest, AuthorityCheckResponse, AuthorityTransition, ObjectReference,
    PublicationAuthority, SubjectAnswer,
};
use receiver_pir_server::distribution::{self, Directory, Inventory, Segment, Subject};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
};
#[path = "../../../receiver/receiver-store/tests/common/mod.rs"]
pub(super) mod common;
#[path = "../../../receiver/receiver-pir-server/tests/common/lookup.rs"]
pub(super) mod lookup;
#[path = "../../../receiver/receiver-pir-server/tests/common/prepared.rs"]
pub(super) mod prepared;
use crate::test_tls as tls;

struct Origin {
    authority: Mutex<PublicationAuthority<distribution::Descriptor>>,
    objects: Mutex<HashMap<String, Vec<u8>>>,
    canonical: AtomicBool,
    available: AtomicBool,
    corrupt: AtomicBool,
    reject_candidate: AtomicBool,
    advance_object: AtomicBool,
    advance_check_at: AtomicUsize,
    checks: AtomicUsize,
}
impl Origin {
    fn publish_filters(&self, height: u32) {
        use zakura_pir_query_server::receiver::blocks::HeightSet;
        let current = self.authority.lock().unwrap().current().clone();
        let reference = &current.publication.unwrap().inventory;
        let mut inventory: Inventory =
            serde_json::from_slice(&self.objects.lock().unwrap()[&reference.sha256]).unwrap();
        let set = HeightSet::build(&[1; 32], height, [&common::receiver()])
            .unwrap()
            .unwrap();
        let window = receiver_pir_server::blocks::BlockWindow::new(
            [1; 32],
            inventory.history_revision,
            height,
            [2; 32],
            vec![[3; 32]],
            vec![(height, set.bytes().to_vec())],
        )
        .unwrap();
        inventory.filters = Some(distribution::Filters {
            head: window.head().clone(),
            ranges: window
                .ranges()
                .unwrap()
                .iter()
                .map(|r| self.store(r))
                .collect(),
            hashes: self.store(window.hashes().as_flattened()),
        });
        let object = self.store(&serde_json::to_vec(&inventory).unwrap());
        self.authority
            .lock()
            .unwrap()
            .apply(AuthorityTransition::Publish(inventory.reference(object)))
            .unwrap();
    }
    fn store(&self, bytes: &[u8]) -> ObjectReference {
        let object = ObjectReference::for_bytes(bytes);
        self.objects
            .lock()
            .unwrap()
            .insert(object.sha256.clone(), bytes.to_vec());
        object
    }
    fn publish(&self, revision: u64, pages: u32) {
        self.publish_at(revision, pages, [3; 32]);
    }
    fn publish_at(&self, revision: u64, pages: u32, end_hash: [u8; 32]) {
        let mut coverage = prepared::coverage(end_hash[0]);
        (coverage.history_revision, coverage.end_hash) = (revision, end_hash);
        let paid: Vec<_> = (0..pages)
            .map(|page| {
                let mut paid = common::paid(page);
                paid.1.block_hash = end_hash;
                paid
            })
            .collect();
        let tables = prepared::tables(coverage, &paid, 2048);
        let serving = prepared::prepare(tables.clone()).unwrap();
        let prepared = &serving.segments()[0];
        let segment = Segment {
            publication: prepared.publication(),
            directory: self.store(&tables.directory.data),
            directory_public: self.store(prepared.directory.public()),
            choice: self.store(&tables.directory.choice),
            companion: self.store(&tables.companion.data),
            companion_public: self.store(prepared.companion.public()),
        };
        let inventory = Inventory {
            genesis: [1; 32],
            history_revision: revision,
            directory: Some(Directory {
                head: serving.head().clone(),
                shards: serving.inventory().shards().to_vec(),
                segments: vec![segment],
                filters: vec![],
            }),
            filters: None,
            recovery_epoch: 0,
        };
        let object = self.store(&serde_json::to_vec(&inventory).unwrap());
        self.authority
            .lock()
            .unwrap()
            .apply(AuthorityTransition::Publish(inventory.reference(object)))
            .unwrap();
    }
}
fn config(root: &std::path::Path, tls: &tls::Tls) -> Config {
    Config {
        publisher: tls.origin.clone(),
        publisher_ca: Some(tls.certificate.clone()),
        state_dir: root.join("query"),
        genesis: [1; 32],
        activation_height: 100,
        poll_seconds: 1,
        bootstrap_max_bytes: 128 << 20,
        table_memory_mib: 4096,
        retention_seconds: 600,
    }
}
async fn origin(root: &std::path::Path) -> (Arc<Origin>, tls::Tls, tokio::task::JoinHandle<()>) {
    let origin = Arc::new(Origin {
        authority: Mutex::new(
            PublicationAuthority::open(
                root.join("authority.json"),
                distribution::scope([1; 32], 100),
            )
            .unwrap(),
        ),
        objects: Mutex::default(),
        canonical: AtomicBool::new(true),
        available: AtomicBool::new(true),
        corrupt: AtomicBool::new(false),
        reject_candidate: AtomicBool::new(false),
        advance_object: AtomicBool::new(false),
        advance_check_at: AtomicUsize::new(usize::MAX),
        checks: AtomicUsize::new(0),
    });
    origin.publish(0, 1);
    let current = origin.clone();
    let objects = origin.clone();
    let check = origin.clone();
    let app = Router::new()
        .route(
            pir_sync_manager::service_discovery::PATH,
            get(|| async { Json(pir_sync_manager::service_discovery::Document::publisher()) }),
        )
        .route(
            &format!("{}current", distribution::PREFIX),
            get(move || {
                let s = current.clone();
                async move {
                    if !s.available.load(Ordering::SeqCst) {
                        return StatusCode::SERVICE_UNAVAILABLE.into_response();
                    }
                    Json(s.authority.lock().unwrap().current().clone()).into_response()
                }
            }),
        )
        .route(
            &format!("{}objects/:id", distribution::PREFIX),
            get(move |Path(id): Path<String>| {
                let s = objects.clone();
                async move {
                    let mut bytes = s.objects.lock().unwrap().get(&id).cloned().unwrap();
                    if bytes.len() > (1 << 20) && s.advance_object.swap(false, Ordering::SeqCst) {
                        s.publish_filters(102);
                    }
                    if s.corrupt.load(Ordering::SeqCst) {
                        bytes[0] ^= 1;
                    }
                    bytes.into_response()
                }
            }),
        )
        .route(
            &format!("{}authority/check", distribution::PREFIX),
            post(move |Json(request): Json<AuthorityCheckRequest<Subject>>| {
                let s = check.clone();
                async move {
                    let check = s.checks.fetch_add(1, Ordering::SeqCst) + 1;
                    if check == s.advance_check_at.load(Ordering::SeqCst) {
                        s.publish_filters(103);
                    }
                    assert!(request.subjects.len() <= 256);
                    assert!(request.subjects.iter().map(Vec::len).sum::<usize>() <= 4096);
                    let authority = s.authority.lock().unwrap();
                    if request.admit(authority.current()).is_err() {
                        return StatusCode::CONFLICT.into_response();
                    }
                    Json(AuthorityCheckResponse {
                        authority: request.authority,
                        answers: request
                            .subjects
                            .into_iter()
                            .map(|subject| {
                                let still_authoritative = s.canonical.load(Ordering::SeqCst)
                                    && !(s.reject_candidate.load(Ordering::SeqCst)
                                        && subject.iter().any(|(_, hash)| hash == &[4; 32]));
                                SubjectAnswer {
                                    subject,
                                    still_authoritative,
                                }
                            })
                            .collect(),
                    })
                    .into_response()
                }
            }),
        );
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", socket.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(socket, app).await.unwrap();
    });
    let tls = tls::Tls::start(&address).await;
    (origin, tls, task)
}
/// The first segment `wallet` captures, which every fixture holds its records in.
async fn segment(wallet: &lookup::Wallet) -> zakura_pir_query_server::receiver::Publication {
    wallet.inventory().await.unwrap().1[0].segments[0].clone()
}

/// Serves `publications` on the public receiver routes from a local listener,
/// returning the slot that swaps the served publications, the listener's origin and
/// the server task.
async fn serve_wallet(
    publications: &Publications,
) -> (crate::public::Slot, String, tokio::task::JoinHandle<()>) {
    let mount = crate::public::receiver();
    let slot = mount.slot.clone();
    mount.slot.install(router_with_health(
        publications.clone(),
        Arc::new(Default::default),
    ));
    let app = crate::public::router(&[mount])
        .unwrap()
        .into_make_service_with_connect_info::<std::net::SocketAddr>();
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", socket.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(socket, app).await.unwrap();
    });
    (slot, address, task)
}

// Keep the real wallet query and restart together here. Authority-only cases
// use service state so they do not prepare another wallet database, and run as
// separate cases so each stays inside the CI case deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn https_authority_controls_real_wallet_queries_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let (origin, tls, server) = origin(root.path()).await;
    let config = config(root.path(), &tls);
    let publications = Publications::default();
    let mut lifecycle = sync::synchronization(&config, publications.clone()).unwrap();
    lifecycle.synchronize().await.unwrap();
    lifecycle.context().adapter().check_unused_revocations();
    let first_inventory = publications.current().unwrap().id();
    let (wallet_slot, address, wallet) = serve_wallet(&publications).await;
    let http = lookup::Wallet::new(&address);
    let first = segment(&http).await;
    let read = http.lookup(&first, &common::receiver()).await.unwrap();
    assert_eq!(read[0].0.payment, common::record(0, 1).payment);
    assert_eq!(read[0].1, common::paid(0).2);
    for path in [
        "/v1/receiver/health",
        "/metrics",
        "/provider/receiver/v1/current",
    ] {
        assert_eq!(
            http.http
                .get(format!("{address}{path}"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
    }
    let predecessor_objects: Vec<_> = origin.objects.lock().unwrap().keys().cloned().collect();
    // A filter publication advances authority while keeping the one-record
    // wallet fixture; this test does not need another payment database.
    origin.publish_filters(102);
    lifecycle.synchronize().await.unwrap();
    for object in predecessor_objects {
        assert!(
            config.state_dir.join("objects").join(object).exists(),
            "ordinary positive successor keeps predecessor files"
        );
    }
    assert!(publications.inventory(&first_inventory).is_ok());
    origin.publish(0, 1);
    origin.available.store(false, Ordering::SeqCst);
    assert!(lifecycle.synchronize().await.is_err());
    assert!(
        publications.inventory(&first_inventory).is_ok(),
        "last approved answers survive publisher outage"
    );
    // A fresh process must verify downloads before it can serve.
    let fresh = Publications::default();
    let mut restarted = config.clone();
    restarted.state_dir = root.path().join("second-query");
    let mut second = sync::synchronization(&restarted, fresh.clone()).unwrap();
    origin.available.store(true, Ordering::SeqCst);
    origin.corrupt.store(true, Ordering::SeqCst);
    assert!(second.synchronize().await.is_err());
    assert!(
        fresh.current_anchor().is_none(),
        "corrupted downloads cannot authorize serving"
    );
    origin.corrupt.store(false, Ordering::SeqCst);
    second.synchronize().await.unwrap();
    assert!(fresh.current_anchor().is_some());
    fresh.revoke();
    drop(second);
    origin.available.store(false, Ordering::SeqCst);
    let restarted_publications = Publications::default();
    let mut after_restart =
        sync::synchronization(&restarted, restarted_publications.clone()).unwrap();
    assert!(after_restart.synchronize().await.is_err());
    assert!(
        restarted_publications.current_anchor().is_none(),
        "verified cached objects and saved active record do not restore authority"
    );
    origin.available.store(true, Ordering::SeqCst);
    after_restart.synchronize().await.unwrap();
    wallet_slot.install(router_with_health(
        restarted_publications.clone(),
        Arc::new(Default::default),
    ));
    let restored = segment(&http).await;
    let read = http.lookup(&restored, &common::receiver()).await.unwrap();
    assert_eq!(read[0].0.payment, common::record(0, 1).payment);
    wallet.abort();
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_candidate_keeps_approved_views_and_retained_sessions() {
    let root = tempfile::tempdir().unwrap();
    let (origin, tls, server) = origin(root.path()).await;
    let config = config(root.path(), &tls);
    let publications = Publications::default();
    let mut lifecycle = sync::synchronization(&config, publications.clone()).unwrap();
    lifecycle.synchronize().await.unwrap();
    let first_inventory = publications.current().unwrap().id();
    origin.publish_filters(102);
    lifecycle.synchronize().await.unwrap();
    let approved_inventory = publications.current().unwrap().id();
    let approved_epoch = publications.epoch();
    origin.reject_candidate.store(true, Ordering::SeqCst);
    origin.publish_at(0, 1, [4; 32]);
    let _ = lifecycle.synchronize().await;
    assert_eq!(
        publications.epoch(),
        approved_epoch,
        "a rejected candidate does not fence approved views"
    );
    assert!(publications.current_anchor().is_some());
    assert_eq!(publications.current().unwrap().id(), approved_inventory);
    assert!(
        publications.inventory(&first_inventory).is_ok(),
        "a rejected candidate preserves retained wallet sessions"
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missed_withdrawals_uncertain_records_and_negative_answers_close_admission() {
    let root = tempfile::tempdir().unwrap();
    let (origin, tls, server) = origin(root.path()).await;
    let config = config(root.path(), &tls);
    let publications = Publications::default();
    let mut lifecycle = sync::synchronization(&config, publications.clone()).unwrap();
    lifecycle.synchronize().await.unwrap();
    let (_, address, wallet) = serve_wallet(&publications).await;
    let http = lookup::Wallet::new(&address);
    let first = segment(&http).await;
    origin
        .authority
        .lock()
        .unwrap()
        .apply(AuthorityTransition::Withdraw)
        .unwrap();
    origin.publish(1, 1); // The consumer missed the withdrawal itself.
    lifecycle.synchronize().await.unwrap();
    assert_eq!(
        http.lookup(&first, &common::receiver()).await.unwrap_err(),
        StatusCode::GONE
    );
    assert_eq!(publications.history_revision(), 1);
    let active = config.state_dir.join("active.json");
    std::fs::remove_file(&active).unwrap();
    std::fs::create_dir(&active).unwrap();
    origin.publish_filters(103);
    assert!(lifecycle.synchronize().await.is_err());
    assert!(
        publications.current_anchor().is_none(),
        "an uncertain active-record write closes admission"
    );
    std::fs::remove_dir(&active).unwrap();
    lifecycle.synchronize().await.unwrap();
    assert!(publications.current_anchor().is_some());
    let approved = origin.authority.lock().unwrap().current().clone();
    origin.canonical.store(false, Ordering::SeqCst);
    assert!(lifecycle.synchronize().await.is_err());
    assert!(publications.current_anchor().is_none());
    origin.canonical.store(true, Ordering::SeqCst);
    // Replaying the same view cannot undo a durable negative answer.
    assert_eq!(*origin.authority.lock().unwrap().current(), approved);
    assert!(lifecycle.synchronize().await.is_err());
    assert!(publications.current_anchor().is_none());
    wallet.abort();
    server.abort();
}

#[test]
fn configuration_is_offline_and_refuses_node_credentials() {
    let mut value = serde_json::json!({"publisher":"https://publisher.example", "publisher_ca":"/absent/cert.pem", "state_dir":"/absent/state", "genesis":vec![1;32], "activation_height":100});
    let config: Config = serde_json::from_value(value.clone()).unwrap();
    config.validate().unwrap();
    value["rpc_url"] = serde_json::json!("http://node");
    assert!(serde_json::from_value::<Config>(value).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filter_successors_reuse_loaded_directory_across_authority_conflicts() {
    let root = tempfile::tempdir().unwrap();
    let (origin, tls, server) = origin(root.path()).await;
    let config = config(root.path(), &tls);
    let publications = Publications::default();
    let mut lifecycle = sync::synchronization(&config, publications.clone()).unwrap();
    origin.advance_object.store(true, Ordering::SeqCst);
    origin.advance_check_at.store(1, Ordering::SeqCst);
    let _ = lifecycle.synchronize().await;
    assert!(publications.current_anchor().is_none());
    assert_eq!(lifecycle.context().adapter().directory_loads(), 1);
    lifecycle.synchronize().await.unwrap();
    assert!(publications.current_anchor().is_some());
    assert_eq!(
        lifecycle.context().adapter().directory_loads(),
        1,
        "filter-only successors reuse the verified directory"
    );
    assert_eq!(publications.filter_window().unwrap().head().tip.height, 103);
    server.abort();
}

/// A restarted publisher withdraws, starts a new recovery epoch and publishes its
/// previous tables again. The query role serves them again on its next pass without
/// reloading them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_publisher_republishing_the_same_tables_is_served_on_the_next_pass() {
    let root = tempfile::tempdir().unwrap();
    let (origin, tls, server) = origin(root.path()).await;
    let config = config(root.path(), &tls);
    let publications = Publications::default();
    let mut lifecycle = sync::synchronization(&config, publications.clone()).unwrap();
    lifecycle.synchronize().await.unwrap();
    origin.publish_filters(103);
    lifecycle.synchronize().await.unwrap();
    let served = publications.current().unwrap().id();
    let loads = lifecycle.context().adapter().directory_loads();
    origin.available.store(false, Ordering::SeqCst);
    assert!(lifecycle.synchronize().await.is_err());
    {
        let mut authority = origin.authority.lock().unwrap();
        let previous = authority.current().publication.clone().unwrap();
        authority.apply(AuthorityTransition::Recover).unwrap();
        let mut inventory: Inventory =
            serde_json::from_slice(&origin.objects.lock().unwrap()[&previous.inventory.sha256])
                .unwrap();
        inventory.recovery_epoch = authority.current().fence.recovery_epoch;
        let object = origin.store(&serde_json::to_vec(&inventory).unwrap());
        authority
            .apply(AuthorityTransition::Publish(inventory.reference(object)))
            .unwrap();
    }
    origin.available.store(true, Ordering::SeqCst);
    lifecycle.synchronize().await.unwrap();
    assert_eq!(publications.current().map(|p| p.id()), Ok(served));
    assert!(publications.filter_window().is_some());
    assert!(publications.current_anchor().is_some());
    assert_eq!(
        lifecycle.context().adapter().directory_loads(),
        loads,
        "the same tables are not loaded again"
    );
    server.abort();
}

/// A publication without tables drops the cached ones, so tables that return
/// afterwards load from the files fetched for them rather than collected ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tables_returning_after_a_publication_without_them_load_again() {
    let root = tempfile::tempdir().unwrap();
    let (origin, tls, server) = origin(root.path()).await;
    let config = config(root.path(), &tls);
    let publications = Publications::default();
    let mut lifecycle = sync::synchronization(&config, publications.clone()).unwrap();
    lifecycle.synchronize().await.unwrap();
    origin.publish_filters(103);
    lifecycle.synchronize().await.unwrap();
    let served = publications.current().unwrap().id();
    let loads = lifecycle.context().adapter().directory_loads();
    let with_tables = {
        let authority = origin.authority.lock().unwrap();
        authority.current().publication.clone().unwrap()
    };
    let mut inventory: Inventory =
        serde_json::from_slice(&origin.objects.lock().unwrap()[&with_tables.inventory.sha256])
            .unwrap();
    inventory.directory = None;
    let object = origin.store(&serde_json::to_vec(&inventory).unwrap());
    origin
        .authority
        .lock()
        .unwrap()
        .apply(AuthorityTransition::Publish(inventory.reference(object)))
        .unwrap();
    lifecycle.synchronize().await.unwrap();
    origin
        .authority
        .lock()
        .unwrap()
        .apply(AuthorityTransition::Publish(with_tables))
        .unwrap();
    lifecycle.synchronize().await.unwrap();
    assert_eq!(publications.current().map(|p| p.id()), Ok(served));
    assert_eq!(lifecycle.context().adapter().directory_loads(), loads + 1);
    server.abort();
}

/// A publication replacing a served one has its new tables prepared before it is
/// served, so the first wallet query on it does not wait for that preparation.
/// The first publication after a start stays lazy, so readiness does not wait for
/// every table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replacing_publication_is_prepared_before_it_is_served() {
    let root = tempfile::tempdir().unwrap();
    let (origin, tls, server) = origin(root.path()).await;
    let config = config(root.path(), &tls);
    let publications = Publications::default();
    let mut lifecycle = sync::synchronization(&config, publications.clone()).unwrap();
    lifecycle.synchronize().await.unwrap();
    let first = publications.current().unwrap().id();
    assert_eq!(
        publications.tables().loads(),
        0,
        "a start loads tables lazily"
    );
    origin.publish(0, 2);
    lifecycle.synchronize().await.unwrap();
    assert_ne!(publications.current().unwrap().id(), first);
    assert_eq!(
        publications.tables().loads(),
        2,
        "the new directory and companion are prepared before serving"
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_proofs_are_batched_and_negative_chunks_refute_only_their_view() {
    use pir_publication::ViewIdentity;
    let root = tempfile::tempdir().unwrap();
    let (origin, tls, server) = origin(root.path()).await;
    let config = config(root.path(), &tls);
    let publications = Publications::default();
    let mut lifecycle = sync::synchronization(&config, publications).unwrap();
    // Proof batching checks live authority without loading wallet tables.
    let healthy = ViewIdentity([11; 32]);
    let rejected = ViewIdentity([12; 32]);
    let proofs = vec![
        (
            healthy,
            (0..1024).map(|height| (height, [255; 32])).collect(),
        ),
        (
            rejected,
            (0..4200)
                .map(|height| (height, if height == 4199 { [4; 32] } else { [255; 32] }))
                .collect(),
        ),
    ];
    check_proofs(&mut lifecycle, vec![proofs[0].clone()])
        .await
        .unwrap();
    // An authority change after a positive batch must defer the whole proof.
    origin
        .advance_check_at
        .store(origin.checks.load(Ordering::SeqCst) + 2, Ordering::SeqCst);
    assert!(check_proofs(&mut lifecycle, proofs.clone()).await.is_err());
    assert!(!lifecycle.context().floor().is_invalidated(&healthy));
    origin.reject_candidate.store(true, Ordering::SeqCst);
    check_proofs(&mut lifecycle, proofs).await.unwrap();
    assert!(!lifecycle.context().floor().is_invalidated(&healthy));
    assert!(lifecycle.context().floor().is_invalidated(&rejected));
    // More retained views than the publisher permits in a single request.
    let retained = (0..300u32)
        .map(|number| {
            let mut id = [13; 32];
            id[..4].copy_from_slice(&number.to_le_bytes());
            (ViewIdentity(id), vec![(number, [255; 32])])
        })
        .collect();
    check_proofs(&mut lifecycle, retained).await.unwrap();
    server.abort();
}

async fn check_proofs(
    lifecycle: &mut sync::Lifecycle,
    proofs: Vec<(pir_publication::ViewIdentity, Subject)>,
) -> Result<()> {
    let observed = lifecycle.context_mut().observe().await?;
    lifecycle
        .context_mut()
        .check_authority(&observed, proofs)
        .await
}

/// Rejecting a view revokes the block hints it installed even after newer hints replaced
/// them, so a request still holding the old window can't finish a negative answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_view_revokes_its_replaced_hints() {
    let root = tempfile::tempdir().unwrap();
    let (origin, tls, server) = origin(root.path()).await;
    let config = config(root.path(), &tls);
    let publications = Publications::default();
    let mut lifecycle = sync::synchronization(&config, publications.clone()).unwrap();
    origin.publish_filters(102);
    lifecycle.synchronize().await.unwrap();
    let held = publications.filter_window().unwrap();
    origin.publish_filters(103);
    lifecycle.synchronize().await.unwrap();
    let current = publications.filter_window().unwrap();
    assert!(!Arc::ptr_eq(&held, &current));
    assert!(
        held.authorized(),
        "a replaced window still answers until rejected"
    );
    lifecycle.context().adapter().reject_replaced_views().await;
    assert!(!held.authorized());
    assert!(current.authorized());
    server.abort();
}
