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
use zakura_pir_publisher::receiver::snapshot::SnapshotPublisherExt as _;
use zakura_pir_query_server::receiver::{
    session::{Error, MIN_ROWS},
    snapshot::Snapshot,
};
#[path = "../../../receiver/receiver-store/tests/common/mod.rs"]
pub(super) mod common;
#[path = "../../../receiver/receiver-pir-server/tests/common/history.rs"]
mod history;
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
        use zakura_pir_query_server::receiver::blocks::{BlockFilter, BlockId, MAX_BLOCKS};
        let current = self.authority.lock().unwrap().current().clone();
        let reference = &current.publication.unwrap().inventory;
        let mut inventory: Inventory =
            serde_json::from_slice(&self.objects.lock().unwrap()[&reference.sha256]).unwrap();
        let filter = BlockFilter::build(
            &[1; 32],
            BlockId {
                height,
                hash: [3; 32],
            },
            [2; 32],
            [&common::receiver()],
        )
        .unwrap();
        let window = receiver_pir_server::blocks::BlockWindow::new(
            [1; 32],
            inventory.history_revision,
            vec![filter],
        )
        .unwrap();
        let batch = window
            .batch(inventory.history_revision, height, MAX_BLOCKS)
            .unwrap();
        inventory.filters = Some(distribution::Filters {
            head: window.head().clone(),
            batches: vec![self.store(&batch)],
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
        let mut manifest = common::manifest(MIN_ROWS);
        manifest.history_revision = revision;
        manifest.end_hash = end_hash;
        let records: Vec<_> = (0..pages)
            .map(|page| {
                let mut record = common::record(page, pages);
                record.payment.block_hash = end_hash;
                record
            })
            .collect();
        let snapshot = Snapshot::build(manifest, &records).unwrap();
        let table = prepared::companion_table(&snapshot, &vec![[7; 512]; pages as usize]);
        let serving = prepared::prepare(snapshot.clone(), table.clone()).unwrap();
        let session = serving.segments()[0].publication();
        let segment = Segment {
            publication: session,
            payments: self.store(&snapshot.data),
            payment_public: self.store(serving.segments()[0].payments.public()),
            witnesses: self.store(&table.data),
            witness_public: self.store(serving.segments()[0].witnesses.public()),
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
struct Http(reqwest::Client);
impl zakura_pir_client::receiver::transport::Transport for Http {
    async fn get(&self, url: &str, limit: usize) -> std::result::Result<Vec<u8>, Error> {
        read(self.0.get(url), limit).await
    }
    async fn post(
        &self,
        url: &str,
        body: Vec<u8>,
        limit: usize,
    ) -> std::result::Result<Vec<u8>, Error> {
        read(self.0.post(url).body(body), limit).await
    }
}
async fn read(
    request: reqwest::RequestBuilder,
    limit: usize,
) -> std::result::Result<Vec<u8>, Error> {
    let response = request
        .send()
        .await
        .map_err(|e| Error::Transport(e.to_string()))?;
    if response.status() == StatusCode::GONE {
        return Err(Error::Revision);
    }
    if !response.status().is_success() {
        return Err(Error::Transport(response.status().to_string()));
    }
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| Error::Transport(e.to_string()))?
    {
        if bytes.len() + chunk.len() > limit {
            return Err(Error::Malformed);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
async fn connect<'a>(origin: &str, http: &'a Http) -> history::Session<&'a Http> {
    history::connect(
        origin,
        http,
        history::Accepted {
            genesis: [1; 32],
            required_start: 100,
            height: 101,
            hash: [3; 32],
        },
    )
    .await
    .unwrap()
}

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
    let mount = crate::public::receiver();
    let wallet_slot = mount.slot.clone();
    mount.slot.install(router_with_health(
        publications.clone(),
        Arc::new(Default::default),
    ));
    let app = crate::public::router(&[mount])
        .unwrap()
        .into_make_service_with_connect_info::<std::net::SocketAddr>();
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", socket.local_addr().unwrap());
    let wallet = tokio::spawn(async move {
        axum::serve(socket, app).await.unwrap();
    });
    let http = Http(reqwest::Client::new());
    let first = connect(&address, &http).await;
    let record = first.page(common::receiver(), 0).await.unwrap().unwrap();
    assert_eq!(record.payment, common::record(0, 1).payment);
    assert_eq!(first.witness(record.ordinal).await.unwrap(), [7; 512]);
    for path in [
        "/v1/receiver/health",
        "/metrics",
        "/provider/receiver/v1/current",
    ] {
        assert_eq!(
            http.0
                .get(format!("{address}{path}"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
    }
    let predecessor_objects: Vec<_> = origin.objects.lock().unwrap().keys().cloned().collect();
    origin.publish(0, 2);
    lifecycle.synchronize().await.unwrap();
    for object in predecessor_objects {
        assert!(
            config.state_dir.join("objects").join(object).exists(),
            "ordinary positive successor keeps predecessor files"
        );
    }
    assert!(publications.inventory(&first_inventory).is_ok());
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
    origin.reject_candidate.store(false, Ordering::SeqCst);
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
    let restored = connect(&address, &http).await;
    assert_eq!(
        restored
            .page(common::receiver(), 0)
            .await
            .unwrap()
            .unwrap()
            .payment,
        common::record(0, 1).payment
    );
    wallet_slot.install(router_with_health(
        publications.clone(),
        Arc::new(Default::default),
    ));
    origin
        .authority
        .lock()
        .unwrap()
        .apply(AuthorityTransition::Withdraw)
        .unwrap();
    origin.publish(1, 1); // The consumer missed the withdrawal itself.
    lifecycle.synchronize().await.unwrap();
    assert!(matches!(
        first.page(common::receiver(), 0).await,
        Err(Error::Revision)
    ));
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

pub(super) fn empty_snapshot(variant: u8) -> Snapshot {
    let mut manifest = common::manifest(MIN_ROWS);
    manifest.salt[0] = variant;
    manifest.end_hash = [variant; 32];
    Snapshot::build(manifest, &[]).unwrap()
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
