//! Public wallet routing follows publisher metadata and privately pinned routers.
use crate::{
    admission::{client_key, read_body, BodyError, ClientSlots},
    protocol::{self, digest, Manifest, QueryBinding, HEADER_BYTES, RETAINED_GENERATIONS},
    publication::METADATA_LIMIT,
    synchronization::{Authority, Client, Config as SourceConfig, Error, Guard, Lease, Safety},
    worker_sync::{Readiness, ReadinessRequest, Release, PIN_LIFETIME},
};
use axum::{
    extract::{ConnectInfo, Path as UrlPath, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use pir_sync_manager::SyncTask;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

const MAINTENANCE: Duration = Duration::from_secs(5);
const BODY_LIMIT: usize = 512 * 1024;
const SESSION_LIMIT: usize = 32 * 1024 * 1024;

/// The private readiness listener and the query listener can use different
/// addresses. Both are operator configuration and bind the same incarnation.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct RouterPeer {
    pub control_origin: String,
    pub query_origin: String,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(flatten)]
    pub source: SourceConfig,
    #[serde(deserialize_with = "crate::synchronization::domain_map")]
    pub routers: BTreeMap<u64, Vec<RouterPeer>>,
}
impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        self.source.validate()?;
        if self.routers.is_empty() || self.routers.len() > protocol::MAX_QUERY_SHARDS as usize {
            return Err("ingress needs configured router responsibilities".into());
        }
        for (shard, origins) in &self.routers {
            if *shard >= protocol::MAX_QUERY_SHARDS {
                return Err("ingress domain is outside the protocol ceiling".into());
            }
            if origins.is_empty()
                || origins.len() > 2
                || origins.iter().collect::<BTreeSet<_>>().len() != origins.len()
            {
                return Err("ingress needs one or two distinct routers per domain".into());
            }
            for origin in origins {
                crate::synchronization::peer_origin(&origin.control_origin)?;
                crate::synchronization::peer_origin(&origin.query_origin)?;
            }
        }
        Ok(())
    }
}
#[derive(Clone)]
struct Peer {
    url: String,
    control_origin: String,
    incarnation: String,
    deadline: Instant,
}
struct Loaded {
    manifest: Manifest,
    peers: Mutex<BTreeMap<u64, Vec<Peer>>>,
}
struct Retired {
    view: Weak<Loaded>,
    manifest: Manifest,
    peers: BTreeMap<u64, Vec<Peer>>,
}
#[derive(Default)]
struct Inner {
    active: Option<Arc<Loaded>>,
    retained: Vec<Arc<Loaded>>,
    retired: Vec<Retired>,
    cursor: usize,
}
#[derive(Clone)]
pub struct QueryIngress {
    config: Config,
    root: PathBuf,
    authority: Authority,
    inner: Arc<Mutex<Inner>>,
    http: reqwest::Client,
    admission: Arc<Semaphore>,
    uploads: Arc<Semaphore>,
    reads: Arc<Semaphore>,
    client_uploads: ClientSlots,
    synchronization: Arc<Semaphore>,
    metrics: crate::http_metrics::HttpMetrics,
    pub incarnation: String,
    _ownership: Arc<File>,
}
type ApiError = (StatusCode, String);
fn unavailable(e: impl ToString) -> ApiError {
    (StatusCode::SERVICE_UNAVAILABLE, e.to_string())
}
fn overloaded(e: &str) -> ApiError {
    (StatusCode::TOO_MANY_REQUESTS, e.into())
}
fn peer_failed(e: impl std::fmt::Display) -> ApiError {
    tracing::warn!(error = %e, "packing router request failed; no replay");
    unavailable("router unavailable")
}
impl QueryIngress {
    pub fn open(
        root: &Path,
        config: Config,
        requests: usize,
        uploads: usize,
    ) -> Result<Self, Error> {
        config.validate()?;
        if !(1..=64).contains(&requests) || uploads < requests || uploads > 4096 {
            return Err("ingress concurrency exceeds qualified limits".into());
        }
        fs::create_dir_all(root)?;
        let ownership = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("ingress.lock"))?;
        ownership
            .try_lock()
            .map_err(|e| format!("ingress state already owned: {e}"))?;
        if root.join("fence.json").exists() {
            return Err("autonomous ingress requires a separate state directory".into());
        }
        let safety = Safety::open(root, &config.source.publisher_origin)?;
        let authority = Authority::new(root, &config.source, safety)?;
        let metrics = crate::http_metrics::HttpMetrics::default();
        metrics.endpoint("init");
        metrics.endpoint("query");
        Ok(Self {
            root: root.into(),
            config,
            authority,
            inner: Default::default(),
            http: crate::internal_auth::client_builder()
                .timeout(Duration::from_secs(90))
                .build()?,
            admission: Arc::new(Semaphore::new(requests)),
            uploads: Arc::new(Semaphore::new(uploads)),
            reads: Arc::new(Semaphore::new(64)),
            client_uploads: ClientSlots::new(4),
            synchronization: Arc::new(Semaphore::new(1)),
            metrics,
            incarnation: hex::encode(rand::random::<[u8; 16]>()),
            _ownership: Arc::new(ownership),
        })
    }
    pub fn synchronizer(&self) -> Result<Synchronizer, Error> {
        Ok(Synchronizer {
            ingress: self.clone(),
            client: Client::new(
                &self.root,
                &self.config.source,
                self.authority.safety.clone(),
            )?,
            _ownership: self
                .synchronization
                .clone()
                .try_acquire_owned()
                .map_err(|_| "ingress synchronization already owned")?,
        })
    }
    pub fn public_router(&self) -> Router {
        Router::new()
            .route("/v1/enhance/init", get(init))
            .route("/v1/enhance/sessions/:generation/:shard", get(session))
            .route("/v1/enhance/session/:session_id", get(session_by_id))
            .route("/v1/enhance/query", post(query))
            .layer(axum::middleware::from_fn_with_state(
                self.metrics.clone(),
                crate::http_metrics::measure,
            ))
            .with_state(self.clone())
    }
    pub fn control_router(&self) -> Router {
        Router::new()
            .route("/internal/health", get(health))
            .layer(crate::internal_auth::Token::from_env().layer())
            .route("/internal/metrics", get(metrics))
            .with_state(self.clone())
    }
    async fn peers(&self, manifest: &Manifest) -> Result<Option<BTreeMap<u64, Vec<Peer>>>, Error> {
        let mut requests = tokio::task::JoinSet::new();
        for domain in &manifest.coverage.shards {
            let Some(origins) = self.config.routers.get(&domain.id) else {
                return Err("published domain has no configured router".into());
            };
            for origin in origins {
                let (http, endpoint, manifest, holder, shard) = (
                    self.http.clone(),
                    origin.clone(),
                    manifest.clone(),
                    self.incarnation.clone(),
                    domain.id,
                );
                requests.spawn(async move {
                    let started = Instant::now();
                    let result = async {
                        let response = http
                            .post(format!("{}/internal/readiness", endpoint.control_origin))
                            .timeout(Duration::from_secs(2))
                            .json(&ReadinessRequest {
                                manifest_sha256: digest(&manifest),
                                holder: Some(holder),
                            })
                            .send()
                            .await?
                            .error_for_status()?;
                        let bytes =
                            crate::query_serving::bounded(response, METADATA_LIMIT as usize)
                                .await?;
                        let ready: Readiness = serde_json::from_slice(&bytes)?;
                        if ready.manifest != manifest
                            || ready.manifest_sha256 != digest(&manifest)
                            || !ready.shards.contains(&shard)
                            || ready.incarnation.len() != 32
                            || hex::decode(&ready.incarnation).is_err()
                            || started.elapsed() >= PIN_LIFETIME
                        {
                            return Err::<Peer, Error>(
                                "router readiness does not bind requested material".into(),
                            );
                        }
                        Ok(Peer {
                            url: endpoint.query_origin,
                            control_origin: endpoint.control_origin,
                            incarnation: ready.incarnation,
                            deadline: started + PIN_LIFETIME,
                        })
                    }
                    .await;
                    (shard, result)
                });
            }
        }
        let mut peers: BTreeMap<u64, Vec<Peer>> = BTreeMap::new();
        while let Some(result) = requests.join_next().await {
            let (shard, result) = result?;
            if let Ok(peer) = result {
                peers.entry(shard).or_default().push(peer);
            }
        }
        Ok(manifest
            .coverage
            .shards
            .iter()
            .all(|domain| peers.get(&domain.id).is_some_and(|peers| !peers.is_empty()))
            .then_some(peers))
    }
    async fn release(&self, manifest: &Manifest, peers: &BTreeMap<u64, Vec<Peer>>) {
        let mut tasks = tokio::task::JoinSet::new();
        for (url, incarnation) in peers
            .values()
            .flatten()
            .map(|p| (p.control_origin.clone(), p.incarnation.clone()))
            .collect::<BTreeSet<_>>()
        {
            let http = self.http.clone();
            let request = Release {
                manifest_sha256: digest(manifest),
                holder: self.incarnation.clone(),
                worker_incarnation: incarnation,
            };
            tasks.spawn(async move {
                let _ = http
                    .post(format!("{url}/internal/release"))
                    .timeout(Duration::from_secs(2))
                    .json(&request)
                    .send()
                    .await;
            });
        }
        while tasks.join_next().await.is_some() {}
    }
    async fn maintain(&self) {
        let (views, released) = {
            let mut inner = self.inner.lock().unwrap();
            let mut views = inner
                .active
                .iter()
                .chain(inner.retained.iter())
                .cloned()
                .collect::<Vec<_>>();
            let mut released = Vec::new();
            inner.retired.retain(|old| match old.view.upgrade() {
                Some(view) => {
                    views.push(view);
                    true
                }
                None => {
                    released.push((old.manifest.clone(), old.peers.clone()));
                    false
                }
            });
            (views, released)
        };
        for (manifest, peers) in released {
            self.release(&manifest, &peers).await;
        }
        let mut seen = BTreeSet::new();
        for view in views {
            if seen.insert(digest(&view.manifest))
                && self.authority.fresh(&view.manifest).await.is_ok()
            {
                if let Ok(Some(peers)) = self.peers(&view.manifest).await {
                    *view.peers.lock().unwrap() = peers;
                }
            }
        }
    }
    async fn active(&self) -> Result<(Arc<Loaded>, Guard), ApiError> {
        let view = self
            .inner
            .lock()
            .unwrap()
            .active
            .clone()
            .ok_or_else(|| unavailable("ingress not ready"))?;
        let guard = self
            .authority
            .fresh(&view.manifest)
            .await
            .map_err(unavailable)?;
        Ok((view, guard))
    }
    async fn select(&self, view: &Loaded, shard: u64) -> Result<Peer, ApiError> {
        let stale = view
            .peers
            .lock()
            .unwrap()
            .get(&shard)
            .is_none_or(|peers| peers.iter().all(|p| Instant::now() >= p.deadline));
        if stale {
            let peers = self
                .peers(&view.manifest)
                .await
                .map_err(unavailable)?
                .ok_or_else(|| unavailable("compatible routers are not ready"))?;
            *view.peers.lock().unwrap() = peers;
        }
        let mut inner = self.inner.lock().unwrap();
        let peers = view.peers.lock().unwrap();
        let choices = peers
            .get(&shard)
            .into_iter()
            .flatten()
            .filter(|p| Instant::now() < p.deadline)
            .collect::<Vec<_>>();
        if choices.is_empty() {
            return Err(unavailable("no compatible router"));
        }
        let chosen = choices[inner.cursor % choices.len()].clone();
        inner.cursor = inner.cursor.wrapping_add(1);
        Ok(chosen)
    }
    fn retire(inner: &mut Inner, view: Arc<Loaded>) {
        inner.retired.push(Retired {
            view: Arc::downgrade(&view),
            manifest: view.manifest.clone(),
            peers: view.peers.lock().unwrap().clone(),
        });
    }
}
pub struct Synchronizer {
    ingress: QueryIngress,
    client: Client,
    _ownership: tokio::sync::OwnedSemaphorePermit,
}
impl Synchronizer {
    pub async fn poll_once(&mut self) -> Result<(), Error> {
        self.ingress.maintain().await;
        self.client.begin().await?;
        let response = self.client.current().await?;
        let manifest = response
            .current
            .manifest
            .clone()
            .ok_or("no complete Enhance publication")?;
        let observed =
            Lease::from_response(response.clone(), &manifest, &self.ingress.authority.safety)?;
        {
            let mut inner = self.ingress.inner.lock().unwrap();
            if inner
                .active
                .as_ref()
                .is_some_and(|view| view.manifest == manifest)
            {
                self.ingress.authority.install(manifest, observed)?;
                return Ok(());
            }
            // Make room before asking downstream to retain another generation.
            if inner.retained.len() >= RETAINED_GENERATIONS - 1 {
                let old = inner.retained.remove(0);
                QueryIngress::retire(&mut inner, old);
            }
        }
        self.ingress.maintain().await;
        let Some(peers) = self.ingress.peers(&manifest).await? else {
            return Ok(());
        };
        let lease = self.client.authority(&manifest).await?;
        if !lease.continues(&observed) {
            return Err("ingress publication withdrawn during readiness".into());
        }
        let guard = self.ingress.authority.install(manifest.clone(), lease)?;
        guard.check()?;
        let loaded = Arc::new(Loaded {
            manifest,
            peers: Mutex::new(peers),
        });
        let mut inner = self.ingress.inner.lock().unwrap();
        if let Some(old) = inner.active.replace(loaded) {
            inner.retained.push(old);
        }
        Ok(())
    }
}
impl SyncTask for Synchronizer {
    type Report = ();
    type Error = Error;
    async fn synchronize(&mut self) -> Result<(), Error> {
        self.poll_once().await
    }
    async fn wait_until(&mut self, deadline: tokio::time::Instant) {
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + MAINTENANCE)).await;
            self.ingress.maintain().await;
        }
    }
    fn report(&self, result: &Result<(), Error>) {
        if let Err(error) = result {
            tracing::warn!(%error, "Enhance ingress synchronization deferred");
        }
    }
}
async fn health(State(r): State<QueryIngress>) -> Json<serde_json::Value> {
    let inner = r.inner.lock().unwrap();
    Json(
        serde_json::json!({ "binary_sha256": pir_control::Identity::process().binary_sha256, "incarnation": r.incarnation, "active": inner.active.as_ref().map(|v| digest(&v.manifest)), "generation": inner.active.as_ref().map(|v| v.manifest.generation),
        "ready": inner.active.as_ref().is_some_and(|v| r.authority.check(&v.manifest).is_ok()) }),
    )
}
async fn metrics(State(r): State<QueryIngress>) -> impl IntoResponse {
    (
        [("content-type", "text/plain; version=0.0.4")],
        r.metrics.render(),
    )
}
async fn init(State(r): State<QueryIngress>) -> Response {
    let result = async {
        let permit = r
            .reads
            .clone()
            .try_acquire_owned()
            .map_err(|_| overloaded("public reads full"))?;
        let (view, guard) = r.active().await?;
        let bytes = serde_json::to_vec(&view.manifest).map_err(unavailable)?;
        Ok::<_, ApiError>(
            (
                [("content-type", "application/json")],
                crate::response_body::guarded(bytes, (permit, view), move || guard.check()),
            )
                .into_response(),
        )
    }
    .await;
    result.unwrap_or_else(crate::query_serving::public_error)
}
async fn session(
    State(r): State<QueryIngress>,
    UrlPath((generation, shard)): UrlPath<(u64, u64)>,
) -> Response {
    let id = {
        let inner = r.inner.lock().unwrap();
        inner
            .active
            .iter()
            .chain(inner.retained.iter())
            .find(|v| v.manifest.generation == generation)
            .ok_or((StatusCode::GONE, "expired session".into()))
            .and_then(|view| {
                view.manifest
                    .session_id(shard)
                    .map(hex::encode)
                    .map_err(|e| (StatusCode::BAD_REQUEST, e))
            })
    };
    match id {
        Ok(id) => forward_session(r, id).await,
        Err(e) => crate::query_serving::public_error(e),
    }
}
async fn session_by_id(State(r): State<QueryIngress>, UrlPath(id): UrlPath<String>) -> Response {
    if !protocol::canonical_hash(&id) {
        return crate::query_serving::public_error((
            StatusCode::BAD_REQUEST,
            "invalid session id".into(),
        ));
    }
    forward_session(r, id).await
}
async fn forward_session(r: QueryIngress, id: String) -> Response {
    let result = async {
        let permit = r
            .reads
            .clone()
            .try_acquire_owned()
            .map_err(|_| overloaded("public reads full"))?;
        let (view, shard) = {
            let inner = r.inner.lock().unwrap();
            inner
                .active
                .iter()
                .chain(inner.retained.iter())
                .find_map(|view| {
                    view.manifest
                        .coverage
                        .shards
                        .iter()
                        .find(|s| {
                            view.manifest
                                .session_id(s.id)
                                .is_ok_and(|hash| hex::encode(hash) == id)
                        })
                        .map(|s| (view.clone(), s.id))
                })
                .ok_or((StatusCode::GONE, "session_unavailable".into()))?
        };
        let guard = r
            .authority
            .fresh(&view.manifest)
            .await
            .map_err(unavailable)?;
        let peer = r.select(&view, shard).await?;
        let response = r
            .http
            .get(format!("{}/v1/enhance/session/{id}", peer.url))
            .header("x-enhance-router-incarnation", peer.incarnation)
            .send()
            .await
            .map_err(peer_failed)?;
        let status = response.status();
        let content_type = response.headers().get("content-type").cloned();
        let bytes = crate::query_serving::bounded(response, SESSION_LIMIT)
            .await
            .map_err(peer_failed)?;
        let guard = guard
            .fresh_for_release(&r.authority)
            .await
            .map_err(unavailable)?;
        let mut response = (
            status,
            crate::response_body::guarded(bytes, (permit, view), move || guard.check()),
        )
            .into_response();
        if let Some(value) = content_type {
            response.headers_mut().insert("content-type", value);
        }
        Ok::<_, ApiError>(response)
    }
    .await;
    result.unwrap_or_else(crate::query_serving::public_error)
}
async fn query(State(r): State<QueryIngress>, request: Request) -> Response {
    serve(r, request)
        .await
        .unwrap_or_else(crate::query_serving::public_error)
}
async fn serve(r: QueryIngress, request: Request) -> Result<Response, ApiError> {
    let upload = r
        .uploads
        .clone()
        .try_acquire_owned()
        .map_err(|_| overloaded("uploads full"))?;
    let key = client_key(
        request.headers(),
        request
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|info| info.0.ip()),
    );
    let slot = r
        .client_uploads
        .try_acquire(&key)
        .ok_or_else(|| overloaded("client uploads full"))?;
    let bytes = read_body(request.into_body(), BODY_LIMIT, Duration::from_secs(30))
        .await
        .map_err(|e| match e {
            BodyError::Timeout => (StatusCode::REQUEST_TIMEOUT, "upload deadline".into()),
            e if e.over_limit() => (StatusCode::PAYLOAD_TOO_LARGE, "oversized upload".into()),
            BodyError::Read(_) => (StatusCode::BAD_REQUEST, "truncated upload".into()),
        })?;
    drop(slot);
    drop(upload);
    if bytes.len() < HEADER_BYTES {
        return Err((StatusCode::BAD_REQUEST, "truncated prefix".into()));
    }
    let permit = r
        .admission
        .clone()
        .try_acquire_owned()
        .map_err(|_| overloaded("ingress full"))?;
    let binding =
        QueryBinding::decode(&bytes[..HEADER_BYTES]).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let (view, guard) = r.active().await?;
    if binding.generation != view.manifest.generation {
        return Err((StatusCode::CONFLICT, "stale_routing".into()));
    }
    crate::query_serving::validate_binding(&view.manifest, binding)?;
    let peer = r.select(&view, binding.shard_id).await?;
    // The admitted task owns its permit and exact view even if the wallet disconnects.
    tokio::spawn(async move {
        guard.check().map_err(unavailable)?;
        let response = r
            .http
            .post(format!("{}/v1/enhance/query", peer.url))
            .header("x-enhance-router-incarnation", peer.incarnation)
            .body(bytes)
            .send()
            .await
            .map_err(peer_failed)?;
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = crate::query_serving::bounded(response, 1024 * 1024)
            .await
            .map_err(peer_failed)?;
        let guard = guard
            .fresh_for_release(&r.authority)
            .await
            .map_err(unavailable)?;
        let mut response = (
            status,
            crate::response_body::guarded(bytes, (permit, view), move || guard.check()),
        )
            .into_response();
        for name in ["content-type", "retry-after"] {
            if let Some(value) = headers.get(name) {
                response.headers_mut().insert(name, value.clone());
            }
        }
        Ok::<_, ApiError>(response)
    })
    .await
    .map_err(unavailable)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;
    fn ingress() -> (tempfile::TempDir, QueryIngress) {
        let root = tempfile::tempdir().unwrap();
        let config = Config {
            source: SourceConfig {
                publisher_origin: "https://publisher.example".into(),
                trusted_ca: None,
                download_budget_bytes: 8 * METADATA_LIMIT,
                cache_limit_bytes: METADATA_LIMIT,
            },
            routers: [(
                0,
                vec![RouterPeer {
                    control_origin: "http://127.0.0.1:1".into(),
                    query_origin: "http://127.0.0.1:2".into(),
                }],
            )]
            .into(),
        };
        let ingress = QueryIngress::open(root.path(), config, 1, 4).unwrap();
        (root, ingress)
    }
    #[tokio::test]
    async fn upload_refusals_do_not_charge_router_admission_or_contact_peers() {
        let (_root, ingress) = ingress();
        for (body, status) in [
            (vec![0; 4], StatusCode::BAD_REQUEST),
            (vec![0; BODY_LIMIT + 1], StatusCode::PAYLOAD_TOO_LARGE),
        ] {
            let response = ingress
                .public_router()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/enhance/query")
                        .body(axum::body::Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), status);
            assert_eq!(ingress.admission.available_permits(), 1);
            assert_eq!(ingress.uploads.available_permits(), 4);
        }
        let _uploads = ingress.uploads.clone().acquire_many_owned(4).await.unwrap();
        let response = ingress
            .public_router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/enhance/query")
                    .body(axum::body::Body::from(vec![0; 4]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(ingress.admission.available_permits(), 1);
    }
    #[test]
    fn static_ingress_validation_rejects_insecure_publishers_and_duplicate_peers() {
        let (_root, ingress) = ingress();
        let mut config = ingress.config.clone();
        config.source.publisher_origin = "http://publisher.example".into();
        assert!(config.validate().is_err());
        let mut config = ingress.config.clone();
        config.routers.insert(
            0,
            vec![
                RouterPeer {
                    control_origin: "http://127.0.0.1:1".into(),
                    query_origin: "http://127.0.0.1:2".into(),
                };
                2
            ],
        );
        assert!(config.validate().is_err());
    }
}
