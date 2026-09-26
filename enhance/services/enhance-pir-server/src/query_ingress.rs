//! Bounded EPQ7 prefix routing. No upload-key decoding, packing or query replay.
use crate::packing_router::{Ack, Activation, CONTROL_VERSION};
use crate::worker::Revocation;
use axum::{
    body::{to_bytes, Bytes},
    extract::{ConnectInfo, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use enhance_pir::protocol::{canonical_hash, digest, QueryBinding, HEADER_BYTES};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

const BODY_LIMIT: usize = 512 * 1024;
const UPLOAD_DEADLINE: Duration = Duration::from_secs(30);
/// Concurrent in-flight uploads across all clients before admission is considered.
pub const DEFAULT_UPLOADS: usize = 256;
/// Concurrent in-flight uploads per client identity.
const CLIENT_UPLOADS: usize = 4;

#[derive(Clone, Serialize, Deserialize)]
pub struct IngressView {
    pub version: u16,
    pub controller_epoch: u64,
    pub generation: u64,
    pub revocation: Revocation,
    pub routes: BTreeMap<String, Vec<String>>,
}
struct Loaded {
    view: IngressView,
    digest: String,
}
struct Inner {
    active: Option<Arc<Loaded>>,
    candidate: Option<Arc<Loaded>>,
    epoch: u64,
    fence: Revocation,
    refreshed: Option<Instant>,
    cursor: usize,
}
#[derive(Clone)]
pub struct QueryIngress {
    metrics: crate::http_metrics::HttpMetrics,
    inner: Arc<Mutex<Inner>>,
    root: PathBuf,
    incarnation: String,
    admission: Arc<Semaphore>,
    uploads: Arc<Semaphore>,
    client_uploads: Arc<Mutex<BTreeMap<String, usize>>>,
    http: reqwest::Client,
    _lock: Arc<File>,
}
impl QueryIngress {
    pub fn open(root: &Path, requests: usize) -> Result<Self, String> {
        Self::open_with_uploads(root, requests, DEFAULT_UPLOADS)
    }
    /// `requests` bounds router forwarding; `uploads` bounds bodies being buffered.
    pub fn open_with_uploads(root: &Path, requests: usize, uploads: usize) -> Result<Self, String> {
        if requests == 0 || requests > 64 {
            return Err("ingress concurrency must be 1..64".into());
        }
        if uploads < requests || uploads > 4096 {
            return Err("ingress uploads must be requests..4096".into());
        }
        fs::create_dir_all(root).map_err(|e| e.to_string())?;
        let lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("ingress.lock"))
            .map_err(|e| e.to_string())?;
        lock.try_lock().map_err(|e| e.to_string())?;
        let (epoch, fence) = if root.join("fence.json").exists() {
            serde_json::from_slice(&fs::read(root.join("fence.json")).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?
        } else {
            (0, Revocation::default())
        };
        let metrics = crate::http_metrics::HttpMetrics::default();
        metrics.endpoint("query");
        Ok(Self {
            metrics,
            inner: Arc::new(Mutex::new(Inner {
                active: None,
                candidate: None,
                epoch,
                fence,
                refreshed: None,
                cursor: 0,
            })),
            root: root.into(),
            incarnation: hex::encode(rand::random::<[u8; 16]>()),
            admission: Arc::new(Semaphore::new(requests)),
            uploads: Arc::new(Semaphore::new(uploads)),
            client_uploads: Arc::new(Mutex::new(BTreeMap::new())),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(90))
                .build()
                .map_err(|e| e.to_string())?,
            _lock: Arc::new(lock),
        })
    }
    pub fn public_router(&self) -> Router {
        Router::new()
            .route("/v1/enhance/query", post(query))
            .layer(axum::middleware::from_fn_with_state(
                self.metrics.clone(),
                crate::http_metrics::measure,
            ))
            .with_state(self.clone())
    }
    pub fn control_router(&self) -> Router {
        Router::new()
            .route("/internal/prepare", post(prepare))
            .route("/internal/activate", post(activate))
            .route("/internal/refresh", post(refresh))
            .route("/internal/revoke", post(revoke))
            .route("/internal/health", get(health))
            .layer(crate::internal_auth::Token::from_env().layer())
            // Metrics stay scrape-able without the shared token.
            .route("/internal/metrics", get(metrics))
            .with_state(self.clone())
    }
    fn fence(&self, i: &mut Inner, epoch: u64, fence: Revocation) -> Result<(), String> {
        if epoch < i.epoch
            || fence.recovery_epoch < i.fence.recovery_epoch
            || !i.fence.sessions.is_subset(&fence.sessions)
            || fence.sessions.iter().any(|s| !canonical_hash(s))
        {
            return Err("nonmonotonic ingress fence".into());
        }
        if epoch == i.epoch
            && fence.sessions == i.fence.sessions
            && fence.recovery_epoch == i.fence.recovery_epoch
        {
            return Ok(());
        }
        crate::artifact::write_atomic(&self.root, "fence.json", |f| {
            serde_json::to_writer(f, &(epoch, &fence)).map_err(std::io::Error::other)
        })
        .map_err(|e| e.to_string())?;
        i.epoch = epoch;
        i.fence = fence;
        i.refreshed = None;
        Ok(())
    }
    fn client_slot(&self, client: String) -> Option<ClientSlot> {
        let mut clients = self.client_uploads.lock().unwrap();
        let count = clients.entry(client.clone()).or_default();
        if *count >= CLIENT_UPLOADS {
            return None;
        }
        *count += 1;
        Some(ClientSlot {
            clients: self.client_uploads.clone(),
            client,
        })
    }
    fn allowed(&self, session: &str, epoch: u64) -> Result<(), String> {
        let i = self.inner.lock().unwrap();
        if i.fence.sessions.contains(session) {
            return Err("noncanonical_session".into());
        }
        if i.epoch != epoch
            || i.refreshed
                .is_none_or(|t| t.elapsed() > Duration::from_secs(5))
        {
            return Err("ingress serving authority unavailable".into());
        }
        Ok(())
    }
}
struct ClientSlot {
    clients: Arc<Mutex<BTreeMap<String, usize>>>,
    client: String,
}
impl Drop for ClientSlot {
    fn drop(&mut self) {
        let mut clients = self.clients.lock().unwrap();
        if let Some(count) = clients.get_mut(&self.client) {
            *count -= 1;
            if *count == 0 {
                clients.remove(&self.client);
            }
        }
    }
}
/// Proxy-supplied client address when present, else the socket peer.
fn client_identity(request: &Request) -> String {
    let header = |name: &str| {
        request
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
    };
    header("x-forwarded-for")
        .or_else(|| header("x-real-ip"))
        .or_else(|| {
            request
                .extensions()
                .get::<ConnectInfo<SocketAddr>>()
                .map(|info| info.0.ip().to_string())
        })
        .unwrap_or_else(|| "unknown".into())
}
type Error = (StatusCode, String);
fn fail(e: impl ToString) -> Error {
    (StatusCode::SERVICE_UNAVAILABLE, e.to_string())
}
fn overloaded(e: &str) -> Error {
    (StatusCode::TOO_MANY_REQUESTS, e.into())
}
/// Router transport failures are logged privately; wallets see a fixed message.
fn router_unavailable(e: impl std::fmt::Display) -> Error {
    tracing::warn!(error = %e, "packing router request failed; no replay");
    fail("router unavailable")
}
async fn health(State(r): State<QueryIngress>) -> Json<serde_json::Value> {
    let i = r.inner.lock().unwrap();
    Json(
        serde_json::json!({"incarnation":r.incarnation,"control_version":CONTROL_VERSION,
        "active_digest":i.active.as_ref().map(|v|&v.digest),"ready":i.refreshed.is_some_and(|t|t.elapsed()<=Duration::from_secs(5))}),
    )
}
async fn prepare(
    State(r): State<QueryIngress>,
    Json(view): Json<IngressView>,
) -> Result<Json<Ack>, Error> {
    if view.version != CONTROL_VERSION {
        return Err(fail("unsupported ingress view"));
    }
    for (session, routes) in &view.routes {
        if !canonical_hash(session)
            || routes.is_empty()
            || view.revocation.sessions.contains(session)
        {
            return Err(fail("invalid session assignment"));
        }
        for url in routes {
            crate::packing_router::valid_origin(url).map_err(fail)?;
        }
    }
    let mut i = r.inner.lock().unwrap();
    r.fence(&mut i, view.controller_epoch, view.revocation.clone())
        .map_err(fail)?;
    let hash = digest(&view);
    let ack = Ack {
        incarnation: r.incarnation.clone(),
        digest: hash.clone(),
        controller_epoch: view.controller_epoch,
    };
    crate::artifact::write_atomic(&r.root, "prepared.json", |f| {
        serde_json::to_writer(f, &view).map_err(std::io::Error::other)
    })
    .map_err(fail)?;
    i.candidate = Some(Arc::new(Loaded { view, digest: hash }));
    Ok(Json(ack))
}
fn matches(r: &QueryIngress, i: &Inner, a: &Activation, v: &Loaded) -> bool {
    a.incarnation == r.incarnation
        && a.controller_epoch == i.epoch
        && a.controller_epoch == v.view.controller_epoch
        && a.digest == v.digest
        && i.fence.sessions.is_subset(&v.view.revocation.sessions)
}
async fn activate(
    State(r): State<QueryIngress>,
    Json(a): Json<Activation>,
) -> Result<StatusCode, Error> {
    let mut i = r.inner.lock().unwrap();
    if i.candidate.as_ref().is_some_and(|v| matches(&r, &i, &a, v)) {
        i.active = i.candidate.take();
    } else if !i.active.as_ref().is_some_and(|v| matches(&r, &i, &a, v)) {
        return Err(fail("ingress activation not prepared"));
    }
    i.refreshed = Some(Instant::now());
    Ok(StatusCode::NO_CONTENT)
}
async fn refresh(
    State(r): State<QueryIngress>,
    Json(a): Json<Activation>,
) -> Result<StatusCode, Error> {
    let mut i = r.inner.lock().unwrap();
    if !i.active.as_ref().is_some_and(|v| matches(&r, &i, &a, v)) {
        return Err(fail("ingress refresh stale"));
    }
    i.refreshed = Some(Instant::now());
    Ok(StatusCode::NO_CONTENT)
}
async fn revoke(
    State(r): State<QueryIngress>,
    Json(fence): Json<Revocation>,
) -> Result<StatusCode, Error> {
    let mut i = r.inner.lock().unwrap();
    let epoch = i.epoch;
    r.fence(&mut i, epoch, fence).map_err(fail)?;
    Ok(StatusCode::NO_CONTENT)
}
async fn query(State(r): State<QueryIngress>, request: Request) -> Response {
    match serve(r, request).await {
        Ok(response) => response,
        Err(error) => crate::packing_router::public_error(error),
    }
}
async fn serve(r: QueryIngress, request: Request) -> Result<Response, Error> {
    // Buffer the complete body before charging router admission, so a slow or
    // stalled wallet upload cannot hold one of the few forwarding permits.
    let upload = r
        .uploads
        .clone()
        .try_acquire_owned()
        .map_err(|_| overloaded("uploads full"))?;
    let slot = r
        .client_slot(client_identity(&request))
        .ok_or_else(|| overloaded("client uploads full"))?;
    let bytes: Bytes =
        tokio::time::timeout(UPLOAD_DEADLINE, to_bytes(request.into_body(), BODY_LIMIT))
            .await
            .map_err(|_| (StatusCode::REQUEST_TIMEOUT, "upload deadline".into()))?
            .map_err(|e| {
                if e.to_string().contains("length limit") {
                    (StatusCode::PAYLOAD_TOO_LARGE, "oversized upload".into())
                } else {
                    (StatusCode::BAD_REQUEST, "truncated upload".into())
                }
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
    let session = hex::encode(binding.session_id);
    let (url, epoch) = {
        let mut i = r.inner.lock().unwrap();
        if i.fence.sessions.contains(&session) {
            return Err((StatusCode::GONE, "noncanonical_session".into()));
        }
        if i.refreshed
            .is_none_or(|t| t.elapsed() > Duration::from_secs(5))
        {
            return Err(fail("ingress control stale"));
        }
        let active = i
            .active
            .as_ref()
            .ok_or_else(|| fail("ingress not activated"))?;
        if binding.generation != active.view.generation {
            return Err((StatusCode::CONFLICT, "stale_routing".into()));
        }
        let routes = active
            .view
            .routes
            .get(&session)
            .ok_or_else(|| (StatusCode::GONE, "session_unavailable".into()))?;
        let url = routes[i.cursor % routes.len()].clone();
        i.cursor = i.cursor.wrapping_add(1);
        (url, i.epoch)
    };
    // Never retry a forwarded body. The detached task retains admission even if
    // the wallet disconnects while the router may already be doing crypto work.
    let task = tokio::spawn(async move {
        let response = r
            .http
            .post(format!("{url}/v1/enhance/query"))
            .body(bytes)
            .send()
            .await
            .map_err(router_unavailable)?;
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = crate::packing_router::bounded(response, 1024 * 1024)
            .await
            .map_err(router_unavailable)?;
        r.allowed(&session, epoch).map_err(|e| {
            if e == "noncanonical_session" {
                (StatusCode::GONE, e)
            } else {
                fail(e)
            }
        })?;
        let mut response = (
            status,
            crate::response_body::guarded(bytes, permit, move || r.allowed(&session, epoch)),
        )
            .into_response();
        for name in ["content-type", "retry-after"] {
            if let Some(value) = headers.get(name) {
                response.headers_mut().insert(name, value.clone());
            }
        }
        Ok::<_, Error>(response)
    });
    task.await.map_err(fail)?
}

async fn metrics(
    State(r): State<QueryIngress>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        r.metrics.render(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;
    async fn activate_for(r: &QueryIngress, routes: Vec<String>) {
        let ack = prepare(
            State(r.clone()),
            Json(IngressView {
                version: CONTROL_VERSION,
                controller_epoch: 1,
                generation: 1,
                revocation: Revocation::default(),
                routes: [("aa".repeat(32), routes)].into(),
            }),
        )
        .await
        .unwrap()
        .0;
        activate(
            State(r.clone()),
            Json(Activation {
                incarnation: ack.incarnation,
                digest: ack.digest,
                controller_epoch: 1,
            }),
        )
        .await
        .unwrap();
    }
    fn body() -> Vec<u8> {
        let mut bytes = QueryBinding {
            generation: 1,
            shard_id: 0,
            epoch: [1; 8],
            recovery_epoch: 0,
            session_id: [0xaa; 32],
            request_id: [3; 16],
            anchor_hash: [4; 32],
        }
        .encode();
        bytes.extend([9; 256]);
        bytes
    }
    #[tokio::test]
    async fn fragmented_prefix_forwards_exact_bytes_without_crypto() {
        let expected = body();
        let answer = expected.clone();
        let upstream = Router::new().route(
            "/v1/enhance/query",
            post(move |request: Request| {
                let expected = expected.clone();
                async move {
                    let bytes = axum::body::to_bytes(request.into_body(), 1024)
                        .await
                        .unwrap();
                    assert_eq!(&bytes[..], &expected);
                    bytes
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
        let root = tempfile::tempdir().unwrap();
        let r = QueryIngress::open(root.path(), 1).unwrap();
        activate_for(&r, vec![url]).await;
        let chunks: Vec<_> = body()
            .chunks(7)
            .map(|c| Ok::<_, std::io::Error>(Bytes::copy_from_slice(c)))
            .collect();
        let response = r
            .public_router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/enhance/query")
                    .body(axum::body::Body::from_stream(tokio_stream::iter(chunks)))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(r.admission.available_permits(), 0);
        assert_eq!(
            &axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap()[..],
            &answer
        );
        assert_eq!(r.admission.available_permits(), 1);
        task.abort();
    }
    #[tokio::test]
    async fn upload_caps_reject_before_admission_is_charged() {
        let root = tempfile::tempdir().unwrap();
        let r = QueryIngress::open_with_uploads(root.path(), 1, 5).unwrap();
        let stalled = |client: &str| {
            let router = r.public_router();
            let request = Request::builder()
                .method("POST")
                .uri("/v1/enhance/query")
                .header("x-forwarded-for", format!("{client}, 10.0.0.1"))
                .body(axum::body::Body::from_stream(tokio_stream::pending::<
                    Result<Bytes, std::io::Error>,
                >()))
                .unwrap();
            tokio::spawn(async move { router.oneshot(request).await.unwrap() })
        };
        let mut stalled_uploads: Vec<_> = (0..CLIENT_UPLOADS).map(|_| stalled("1.2.3.4")).collect();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(r.uploads.available_permits(), 5 - CLIENT_UPLOADS);
        assert_eq!(r.admission.available_permits(), 1);
        let same_client = stalled("1.2.3.4").await.unwrap();
        assert_eq!(same_client.status(), StatusCode::TOO_MANY_REQUESTS);
        stalled_uploads.push(stalled("5.6.7.8"));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(r.uploads.available_permits(), 0);
        let other_client = stalled("9.9.9.9").await.unwrap();
        assert_eq!(other_client.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(r.admission.available_permits(), 1);
        for task in stalled_uploads {
            task.abort();
            let _ = task.await;
        }
        assert_eq!(r.uploads.available_permits(), 5);
        assert!(r.client_uploads.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn ambiguous_router_failure_is_never_replayed() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let upstream = Router::new().route(
            "/v1/enhance/query",
            post(move || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    StatusCode::BAD_GATEWAY
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
        let root = tempfile::tempdir().unwrap();
        let r = QueryIngress::open(root.path(), 1).unwrap();
        activate_for(&r, vec![url.clone(), url]).await;
        let response = r
            .public_router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/enhance/query")
                    .body(axum::body::Body::from(body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        task.abort();
    }
}
