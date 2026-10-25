use super::*;
use crate::synchronization::SyncTask;
use axum::{
    extract::Path,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use pir_publication::{
    AuthorityCheckRequest, AuthorityCheckResponse, AuthorityTransition, ObjectReference,
    PublicationAuthority, SubjectAnswer,
};
use receiver_pir_server::distribution::{self, Directory, Inventory, Segment, Subject};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
};
use zakura_pir_publisher::receiver::snapshot::SnapshotPublisherExt as _;
use zakura_pir_query_server::receiver::{
    session::{Error, MIN_ROWS},
    snapshot::Snapshot,
};
#[path = "../../../receiver/receiver-store/tests/common/mod.rs"]
mod common;
#[path = "../../../receiver/receiver-pir-server/tests/common/history.rs"]
mod history;
#[path = "../../../receiver/receiver-pir-server/tests/common/prepared.rs"]
mod prepared;
#[path = "../../tests/support/status_tls.rs"]
mod tls;

struct Origin {
    authority: Mutex<PublicationAuthority<distribution::Descriptor>>,
    objects: Mutex<HashMap<String, Vec<u8>>>,
    canonical: AtomicBool,
    available: AtomicBool,
    corrupt: AtomicBool,
    reject_candidate: AtomicBool,
}
impl Origin {
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
    lifecycle.adapter().check_unused_revocations();
    let mount = crate::public::receiver();
    mount.slot.install(receiver_pir_server::router_with_health(
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
    let mut absent = common::receiver();
    absent[0] ^= 1;
    assert!(first.page(absent, 0).await.unwrap().is_none());
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
    assert!(http
        .0
        .get(format!("{address}/v1/receiver/inventory"))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    let predecessor_objects: Vec<_> = origin.objects.lock().unwrap().keys().cloned().collect();
    origin.publish(0, 2);
    lifecycle.synchronize().await.unwrap();
    for object in predecessor_objects {
        assert!(
            config.state_dir.join("objects").join(object).exists(),
            "ordinary positive successor keeps predecessor files"
        );
    }
    assert!(first.page(common::receiver(), 0).await.unwrap().is_some());
    let approved_epoch = publications.epoch();
    origin.reject_candidate.store(true, Ordering::SeqCst);
    origin.publish_at(0, 3, [4; 32]);
    let _ = lifecycle.synchronize().await;
    assert_eq!(
        publications.epoch(),
        approved_epoch,
        "a rejected candidate does not fence approved views"
    );
    assert!(publications.current_anchor().is_some());
    assert!(
        first.page(common::receiver(), 0).await.unwrap().is_some(),
        "a rejected candidate preserves retained wallet sessions"
    );
    let _ = lifecycle.synchronize().await;
    assert!(
        first.page(common::receiver(), 0).await.unwrap().is_some(),
        "repeated rejection preserves approved answers"
    );
    origin.reject_candidate.store(false, Ordering::SeqCst);
    origin.publish(0, 3);
    origin.corrupt.store(true, Ordering::SeqCst);
    assert!(lifecycle.synchronize().await.is_err());
    assert!(
        first.page(common::receiver(), 0).await.unwrap().is_some(),
        "failed replacement leaves retained answers available"
    );
    origin.corrupt.store(false, Ordering::SeqCst);
    origin.available.store(false, Ordering::SeqCst);
    assert!(lifecycle.synchronize().await.is_err());
    assert!(
        first.page(common::receiver(), 0).await.unwrap().is_some(),
        "last approved answers survive publisher outage"
    );
    // Independent process state has cached bytes but no serving permission.
    let fresh = Publications::default();
    let mut restarted = config.clone();
    restarted.state_dir = root.path().join("second-query");
    let mut second = sync::synchronization(&restarted, fresh.clone()).unwrap();
    assert!(second.synchronize().await.is_err());
    assert!(fresh.current_anchor().is_none());
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
    origin
        .authority
        .lock()
        .unwrap()
        .apply(AuthorityTransition::Withdraw)
        .unwrap();
    origin.publish(1, 2); // The consumer missed the withdrawal itself.
    lifecycle.synchronize().await.unwrap();
    assert!(matches!(
        first.page(common::receiver(), 0).await,
        Err(Error::Revision)
    ));
    let corrected = connect(&address, &http).await;
    assert_eq!(
        history::history(&corrected, common::receiver())
            .await
            .unwrap()
            .len(),
        2
    );
    let active = config.state_dir.join("active.json");
    std::fs::remove_file(&active).unwrap();
    std::fs::create_dir(&active).unwrap();
    origin.publish(1, 3);
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
