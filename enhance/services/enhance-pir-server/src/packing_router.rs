//! Private control plane and public serving process for exact-session packing.
//! Control endpoints must only be reachable by the coordinator. Artifact origins
//! are configured locally, never supplied by a wallet or placement message.
use crate::{
    runtime::{self, Packing},
    worker::{Evaluate, Intermediate, Revocation},
};
use axum::{
    body::to_bytes,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use enhance_pir::protocol::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

pub const CONTROL_VERSION: u16 = 1;
const BODY_LIMIT: usize = 512 * 1024;
const ARTIFACT_LIMIT: usize = 256 * 1024 * 1024;
// A liveness watchdog, not permission to complete recovery without a fence ACK.
const CONTROL_WATCHDOG: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouterRegistration {
    pub name: String,
    pub url: String,
    pub query_url: String,
    /// Empty means all domains. Multiple registrations may replicate a domain.
    #[serde(default)]
    pub domains: BTreeSet<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServingSnapshot {
    pub manifest: Manifest,
    pub routes: BTreeMap<u64, Vec<String>>,
    /// Domain -> (owned artifact filename, SHA-256 of the complete artifact).
    pub artifacts: BTreeMap<u64, (String, String)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct View {
    pub version: u16,
    pub controller_epoch: u64,
    pub snapshots: Vec<ServingSnapshot>,
    pub revocation: Revocation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ack {
    pub incarnation: String,
    pub digest: String,
    pub controller_epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Activation {
    pub incarnation: String,
    pub digest: String,
    pub controller_epoch: u64,
}

struct Loaded {
    view: View,
    digest: String,
    packing: BTreeMap<String, Arc<Packing>>,
}

#[derive(Default, Serialize, Deserialize)]
struct DurableFence {
    controller_epoch: u64,
    revocation: Revocation,
}

struct Inner {
    fence: DurableFence,
    active: Option<Arc<Loaded>>,
    candidate: Option<Arc<Loaded>>,
    material: BTreeMap<String, Weak<Packing>>,
    refreshed: Option<Instant>,
    outstanding: BTreeMap<String, usize>,
    cursor: usize,
}

#[derive(Clone)]
pub struct PackingRouter {
    inner: Arc<Mutex<Inner>>,
    preparation: Arc<AsyncMutex<()>>,
    admission: Arc<Semaphore>,
    waiters: Arc<Semaphore>,
    root: PathBuf,
    origin: String,
    incarnation: String,
    http: reqwest::Client,
    max_objects: usize,
    stats: Arc<Stats>,
    _lock: Arc<File>,
}

#[derive(Default)]
struct Stats {
    successful: std::sync::atomic::AtomicU64,
    rejected: std::sync::atomic::AtomicU64,
    failed: std::sync::atomic::AtomicU64,
    retries: std::sync::atomic::AtomicU64,
    intermediate_bytes: std::sync::atomic::AtomicU64,
    packing_micros: std::sync::atomic::AtomicU64,
}

type Error = (StatusCode, String);
fn unavailable(e: impl ToString) -> Error {
    (StatusCode::SERVICE_UNAVAILABLE, e.to_string())
}
fn bad(e: impl ToString) -> Error {
    (StatusCode::BAD_REQUEST, e.to_string())
}

pub(crate) fn valid_origin(origin: &str) -> Result<(), String> {
    let url = reqwest::Url::parse(origin).map_err(|e| e.to_string())?;
    if url.scheme() != "http"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("expected private HTTP origin".into());
    }
    Ok(())
}

impl PackingRouter {
    pub fn configure_process_budget() -> Result<(), String> {
        crate::packing_budget::configure_router()
    }

    pub fn open(
        root: &Path,
        artifact_origin: &str,
        max_objects: usize,
        requests: usize,
    ) -> Result<Self, String> {
        valid_origin(artifact_origin)?;
        if !(1..=6).contains(&max_objects) || !(1..=4).contains(&requests) {
            return Err("router limits exceed the initial qualification target".into());
        }
        fs::create_dir_all(root.join("artifacts")).map_err(|e| e.to_string())?;
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("router.lock"))
            .map_err(|e| e.to_string())?;
        lock.try_lock().map_err(|e| e.to_string())?;
        let path = root.join("fence.json");
        let fence = if path.exists() {
            serde_json::from_slice(&fs::read(path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?
        } else {
            DurableFence::default()
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                fence,
                active: None,
                candidate: None,
                material: BTreeMap::new(),
                refreshed: None,
                outstanding: BTreeMap::new(),
                cursor: 0,
            })),
            preparation: Arc::new(AsyncMutex::new(())),
            admission: Arc::new(Semaphore::new(requests)),
            waiters: Arc::new(Semaphore::new(16)),
            root: root.into(),
            origin: artifact_origin.trim_end_matches('/').into(),
            incarnation: hex::encode(rand::random::<[u8; 16]>()),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|e| e.to_string())?,
            max_objects,
            stats: Arc::new(Stats::default()),
            _lock: Arc::new(lock),
        })
    }

    pub fn public_router(&self) -> Router {
        Router::new()
            .route("/v1/enhance/query", post(query))
            .with_state(self.clone())
    }
    pub fn control_router(&self) -> Router {
        Router::new()
            .route("/internal/prepare", post(prepare))
            .route("/internal/activate", post(activate))
            .route("/internal/refresh", post(refresh))
            .route("/internal/revoke", post(revoke))
            .route("/internal/drain", post(drain))
            .route("/internal/health", get(health))
            .route("/internal/metrics", get(metrics))
            .with_state(self.clone())
    }

    fn save_fence(
        &self,
        inner: &mut Inner,
        epoch: u64,
        revocation: Revocation,
    ) -> Result<(), String> {
        if epoch < inner.fence.controller_epoch
            || revocation.recovery_epoch < inner.fence.revocation.recovery_epoch
            || !inner
                .fence
                .revocation
                .sessions
                .is_subset(&revocation.sessions)
            || revocation.sessions.iter().any(|id| !canonical_hash(id))
        {
            return Err("stale or nonmonotonic serving fence".into());
        }
        if epoch == inner.fence.controller_epoch
            && revocation.recovery_epoch == inner.fence.revocation.recovery_epoch
            && revocation.sessions == inner.fence.revocation.sessions
        {
            return Ok(());
        }
        let fence = DurableFence {
            controller_epoch: epoch,
            revocation,
        };
        crate::artifact::write_atomic(&self.root, "fence.json", |f| {
            serde_json::to_writer(f, &fence).map_err(std::io::Error::other)
        })
        .map_err(|e| e.to_string())?;
        inner.fence = fence;
        // A new controller must explicitly activate after reconciliation.
        inner.refreshed = None;
        Ok(())
    }

    fn response_allowed(&self, session: &str, epoch: u64) -> Result<(), String> {
        let inner = self.inner.lock().unwrap();
        if inner.fence.revocation.sessions.contains(session) {
            return Err("noncanonical_session".into());
        }
        if inner.fence.controller_epoch != epoch
            || inner
                .refreshed
                .is_none_or(|t| t.elapsed() > CONTROL_WATCHDOG)
        {
            return Err("serving authority unavailable".into());
        }
        Ok(())
    }
}

async fn health(State(r): State<PackingRouter>) -> Json<serde_json::Value> {
    let i = r.inner.lock().unwrap();
    Json(
        serde_json::json!({"protocol":PROTOCOL_REVISION,"control_version":CONTROL_VERSION,
        "incarnation":r.incarnation,"controller_epoch":i.fence.controller_epoch,
        "active_digest":i.active.as_ref().map(|s| &s.digest),
        "ready":i.refreshed.is_some_and(|t| t.elapsed() <= CONTROL_WATCHDOG),
        "resident_objects":i.material.values().filter(|v| v.strong_count()>0).count(),
        "packing_charged_bytes":crate::packing_budget::charged_bytes(),
        "available_requests":r.admission.available_permits(),"outstanding":i.outstanding}),
    )
}

async fn metrics(State(r): State<PackingRouter>) -> impl IntoResponse {
    use std::sync::atomic::Ordering::Relaxed;
    let i = r.inner.lock().unwrap();
    let values = [
        ("successful_queries_total", r.stats.successful.load(Relaxed)),
        ("rejected_queries_total", r.stats.rejected.load(Relaxed)),
        ("failed_queries_total", r.stats.failed.load(Relaxed)),
        ("evaluation_retries_total", r.stats.retries.load(Relaxed)),
        (
            "intermediate_bytes_total",
            r.stats.intermediate_bytes.load(Relaxed),
        ),
        (
            "packing_microseconds_total",
            r.stats.packing_micros.load(Relaxed),
        ),
        (
            "resident_objects",
            i.material.values().filter(|p| p.strong_count() > 0).count() as u64,
        ),
        ("charged_bytes", crate::packing_budget::charged_bytes()),
        (
            "evaluations_outstanding",
            i.outstanding.values().sum::<usize>() as u64,
        ),
    ];
    let text = values
        .into_iter()
        .map(|(name, value)| format!("enhance_packing_router_{name} {value}\n"))
        .collect::<String>();
    ([("content-type", "text/plain; version=0.0.4")], text)
}

async fn prepare(
    State(r): State<PackingRouter>,
    Json(view): Json<View>,
) -> Result<Json<Ack>, Error> {
    let _preparing = Arc::new(
        r.preparation
            .clone()
            .try_lock_owned()
            .map_err(|_| unavailable("preparation busy"))?,
    );
    if view.version != CONTROL_VERSION
        || view.snapshots.is_empty()
        || view.snapshots.len() > RETAINED_GENERATIONS
    {
        return Err(bad("invalid serving view"));
    }
    let view_digest = digest(&view);
    {
        let mut i = r.inner.lock().unwrap();
        r.save_fence(&mut i, view.controller_epoch, view.revocation.clone())
            .map_err(unavailable)?;
        if i.active.as_ref().is_some_and(|v| v.digest == view_digest)
            || i.candidate
                .as_ref()
                .is_some_and(|v| v.digest == view_digest)
        {
            return Ok(Json(Ack {
                incarnation: r.incarnation.clone(),
                digest: view_digest,
                controller_epoch: view.controller_epoch,
            }));
        }
        // Discard an uncommitted preparation; active and request pins remain charged.
        i.candidate = None;
    }
    let mut packing = BTreeMap::new();
    let mut identities = BTreeSet::new();
    let mut last_generation = u64::MAX;
    for snapshot in &view.snapshots {
        snapshot.manifest.validate().map_err(bad)?;
        if snapshot.manifest.generation >= last_generation {
            return Err(bad("unordered snapshots"));
        }
        last_generation = snapshot.manifest.generation;
        if snapshot.artifacts.keys().ne(snapshot.routes.keys()) {
            return Err(bad("artifact/route mismatch"));
        }
        for (&id, (name, hash)) in &snapshot.artifacts {
            let manifest = &snapshot.manifest;
            let shard = manifest
                .coverage
                .shards
                .iter()
                .find(|s| s.id == id)
                .ok_or_else(|| bad("unknown domain"))?;
            let session = hex::encode(manifest.session_id(id).map_err(bad)?);
            if view.revocation.sessions.contains(&session) {
                return Err(bad("revoked assignment"));
            }
            let reference = manifest
                .sessions
                .iter()
                .find(|s| s.shard_id == id)
                .ok_or_else(|| bad("missing session"))?;
            if !canonical_hash(hash) || name != &format!("{}.bin", reference.public_params_sha256) {
                return Err(bad("invalid artifact identity"));
            }
            if snapshot.routes[&id].is_empty() {
                return Err(bad("no evaluation placement"));
            }
            for url in &snapshot.routes[&id] {
                valid_origin(url).map_err(bad)?;
            }
            let key = digest(&(
                hash,
                &reference.parameter_id,
                &reference.public_params_sha256,
            ));
            identities.insert(key.clone());
            if identities.len() > r.max_objects {
                return Err(unavailable("resident assignment limit"));
            }
            let cached = r
                .inner
                .lock()
                .unwrap()
                .material
                .get(&key)
                .and_then(Weak::upgrade);
            let pack = if let Some(pack) = cached {
                pack
            } else {
                {
                    let mut i = r.inner.lock().unwrap();
                    i.material.retain(|_, p| p.strong_count() > 0);
                    if i.material.len() >= r.max_objects + 1 {
                        return Err(unavailable("construction overlap limit"));
                    }
                }
                let response = r
                    .http
                    .get(format!("{}/internal/packing-artifact/{name}", r.origin))
                    .send()
                    .await
                    .map_err(unavailable)?
                    .error_for_status()
                    .map_err(unavailable)?;
                let bytes = bounded(response, ARTIFACT_LIMIT)
                    .await
                    .map_err(unavailable)?;
                if hex::encode(Sha256::digest(&bytes)) != *hash {
                    return Err(bad("artifact digest mismatch"));
                }
                let rows = shard.logical_rows;
                let reference = reference.clone();
                let router = r.clone();
                let material_key = key.clone();
                // Own preparation exclusion through cancellation of expensive construction.
                let guard = _preparing.clone();
                let built = tokio::task::spawn_blocking(move || {
                    let _guard = guard;
                    let params = parameters(rows)?;
                    let blocks = crate::wire::read_crs_blocks(
                        bytes.as_slice(),
                        params.db_cols / runtime::rlwe().d,
                        runtime::rlwe().d,
                    )
                    .map_err(|e| e.to_string())?;
                    let pack = Arc::new(Packing::new(rows, &blocks)?);
                    if pack.reference(id)? != reference {
                        return Err("artifact session mismatch".to_string());
                    }
                    router
                        .inner
                        .lock()
                        .unwrap()
                        .material
                        .insert(material_key, Arc::downgrade(&pack));
                    Ok::<_, String>(pack)
                })
                .await
                .map_err(unavailable)?
                .map_err(unavailable)?;
                built
            };
            packing.insert(session, pack);
        }
    }
    let loaded = Arc::new(Loaded {
        view,
        digest: view_digest.clone(),
        packing,
    });
    let mut i = r.inner.lock().unwrap();
    if i.fence.controller_epoch != loaded.view.controller_epoch
        || !i
            .fence
            .revocation
            .sessions
            .is_subset(&loaded.view.revocation.sessions)
    {
        return Err(unavailable("preparation crossed recovery fence"));
    }
    crate::artifact::write_atomic(&r.root, "prepared.json", |f| {
        serde_json::to_writer(f, &loaded.view).map_err(std::io::Error::other)
    })
    .map_err(unavailable)?;
    let ack = Ack {
        incarnation: r.incarnation.clone(),
        digest: view_digest,
        controller_epoch: loaded.view.controller_epoch,
    };
    i.candidate = Some(loaded);
    Ok(Json(ack))
}

fn matches(r: &PackingRouter, i: &Inner, a: &Activation, v: &Loaded) -> bool {
    a.incarnation == r.incarnation
        && a.controller_epoch == i.fence.controller_epoch
        && a.controller_epoch == v.view.controller_epoch
        && a.digest == v.digest
        && i.fence
            .revocation
            .sessions
            .is_subset(&v.view.revocation.sessions)
}
async fn activate(
    State(r): State<PackingRouter>,
    Json(a): Json<Activation>,
) -> Result<StatusCode, Error> {
    let mut i = r.inner.lock().unwrap();
    if i.candidate.as_ref().is_some_and(|v| matches(&r, &i, &a, v)) {
        i.active = i.candidate.take();
    } else if !i.active.as_ref().is_some_and(|v| matches(&r, &i, &a, v)) {
        return Err(unavailable(
            "activation was not prepared by this incarnation",
        ));
    }
    i.refreshed = Some(Instant::now());
    Ok(StatusCode::NO_CONTENT)
}
async fn refresh(
    State(r): State<PackingRouter>,
    Json(a): Json<Activation>,
) -> Result<StatusCode, Error> {
    let mut i = r.inner.lock().unwrap();
    if !i.active.as_ref().is_some_and(|v| matches(&r, &i, &a, v)) {
        return Err(unavailable("refresh requires current activated view"));
    }
    i.refreshed = Some(Instant::now());
    Ok(StatusCode::NO_CONTENT)
}
async fn revoke(
    State(r): State<PackingRouter>,
    Json(fence): Json<Revocation>,
) -> Result<StatusCode, Error> {
    let mut i = r.inner.lock().unwrap();
    let epoch = i.fence.controller_epoch;
    r.save_fence(&mut i, epoch, fence).map_err(unavailable)?;
    Ok(StatusCode::NO_CONTENT)
}
async fn drain(State(r): State<PackingRouter>) -> StatusCode {
    let mut i = r.inner.lock().unwrap();
    i.refreshed = None;
    i.active = None;
    i.candidate = None;
    StatusCode::NO_CONTENT
}

struct Outstanding {
    router: PackingRouter,
    worker: String,
}
impl Drop for Outstanding {
    fn drop(&mut self) {
        let mut i = self.router.inner.lock().unwrap();
        if let Some(n) = i.outstanding.get_mut(&self.worker) {
            *n -= 1;
        }
    }
}
fn select(
    r: &PackingRouter,
    eligible: &[String],
    excluded: Option<&str>,
) -> Result<Outstanding, Error> {
    let mut i = r.inner.lock().unwrap();
    let min = eligible
        .iter()
        .filter(|u| Some(u.as_str()) != excluded)
        .map(|u| *i.outstanding.get(u).unwrap_or(&0))
        .min()
        .ok_or_else(|| unavailable("no ready worker"))?;
    let tied: Vec<_> = eligible
        .iter()
        .filter(|u| Some(u.as_str()) != excluded && *i.outstanding.get(*u).unwrap_or(&0) == min)
        .collect();
    let worker = tied[i.cursor % tied.len()].clone();
    i.cursor = i.cursor.wrapping_add(1);
    *i.outstanding.entry(worker.clone()).or_default() += 1;
    Ok(Outstanding {
        router: r.clone(),
        worker,
    })
}

pub(crate) async fn bounded(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, String> {
    if response.content_length().is_some_and(|n| n > limit as u64) {
        return Err("oversized response".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
        if chunk.len() > limit - bytes.len() {
            return Err("oversized response".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub(crate) fn public_error(error: Error) -> Response {
    let code = match error.0 {
        StatusCode::CONFLICT => "stale_routing",
        StatusCode::GONE if error.1 == "noncanonical_session" => "noncanonical_session",
        StatusCode::GONE => "session_unavailable",
        StatusCode::TOO_MANY_REQUESTS => "overloaded",
        StatusCode::SERVICE_UNAVAILABLE | StatusCode::BAD_GATEWAY => "temporarily_unavailable",
        _ => "invalid_request",
    };
    let mut response = (
        error.0,
        Json(serde_json::json!({"code":code,"message":error.1})),
    )
        .into_response();
    if error.0 == StatusCode::TOO_MANY_REQUESTS {
        response
            .headers_mut()
            .insert("retry-after", "1".parse().unwrap());
    }
    response
}
async fn query(State(r): State<PackingRouter>, request: Request) -> Response {
    use std::sync::atomic::Ordering::Relaxed;
    match serve(r.clone(), request).await {
        Ok(response) => {
            r.stats.successful.fetch_add(1, Relaxed);
            response
        }
        Err(error) => {
            if error.0 == StatusCode::TOO_MANY_REQUESTS {
                r.stats.rejected.fetch_add(1, Relaxed);
            } else {
                r.stats.failed.fetch_add(1, Relaxed);
            }
            public_error(error)
        }
    }
}
async fn serve(r: PackingRouter, request: Request) -> Result<Response, Error> {
    let waiting = r
        .waiters
        .clone()
        .try_acquire_owned()
        .map_err(|_| (StatusCode::TOO_MANY_REQUESTS, "query queue full".into()))?;
    let permit = tokio::time::timeout(Duration::from_secs(2), r.admission.clone().acquire_owned())
        .await
        .map_err(|_| (StatusCode::TOO_MANY_REQUESTS, "admission deadline".into()))?
        .map_err(unavailable)?;
    drop(waiting);
    let bytes = tokio::time::timeout(
        Duration::from_secs(30),
        to_bytes(request.into_body(), BODY_LIMIT),
    )
    .await
    .map_err(|_| (StatusCode::REQUEST_TIMEOUT, "body deadline".into()))?
    .map_err(|e| (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()))?;
    let binding = QueryBinding::decode(&bytes).map_err(bad)?;
    let session = hex::encode(binding.session_id);
    let (loaded, pack) = {
        let i = r.inner.lock().unwrap();
        if i.fence.revocation.sessions.contains(&session) {
            return Err((StatusCode::GONE, "noncanonical_session".into()));
        }
        if i.refreshed.is_none_or(|t| t.elapsed() > CONTROL_WATCHDOG) {
            return Err(unavailable("control connection stale"));
        }
        let loaded = i
            .active
            .clone()
            .ok_or_else(|| unavailable("router not activated"))?;
        let m = &loaded.view.snapshots[0].manifest;
        if binding.generation != m.generation || binding.recovery_epoch != m.recovery_epoch {
            return Err((StatusCode::CONFLICT, "stale_routing".into()));
        }
        if hex::encode(binding.anchor_hash) != m.anchor_block_hash
            || m.session_id(binding.shard_id).map_err(bad)? != binding.session_id
        {
            return Err((StatusCode::GONE, "session_unavailable".into()));
        }
        let pack = loaded
            .packing
            .get(&session)
            .cloned()
            .ok_or_else(|| (StatusCode::GONE, "session_unavailable".into()))?;
        (loaded, pack)
    };
    // Once admitted, the task owns the permit through cancellation and CPU work.
    let router = r.clone();
    let task = tokio::spawn(async move {
        let coefficients = pack.query_coefficients(&bytes, binding).map_err(bad)?;
        let request = Evaluate {
            binding: Some(binding.encode()),
            generation: binding.generation,
            shard_id: binding.shard_id,
            epoch: hex::encode(binding.epoch),
            session_id: session.clone(),
            coefficients,
        };
        let routes = loaded.view.snapshots[0]
            .routes
            .get(&binding.shard_id)
            .ok_or_else(|| unavailable("unassigned session"))?;
        let mut excluded = None;
        let mut answer = None;
        for attempt in 0..2 {
            let selected = select(&router, routes, excluded.as_deref())?;
            if attempt > 0 {
                router
                    .stats
                    .retries
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            let response = router
                .http
                .post(format!("{}/internal/evaluate", selected.worker))
                .json(&request)
                .send()
                .await;
            match response {
                Err(e) if e.is_connect() && attempt == 0 && routes.len() > 1 => {
                    excluded = Some(selected.worker.clone());
                }
                Err(e) => return Err(unavailable(format!("evaluation acceptance unknown: {e}"))),
                Ok(response) => {
                    let rejected = response
                        .headers()
                        .get("x-enhance-evaluation")
                        .is_some_and(|v| v == "not-accepted")
                        && matches!(
                            response.status(),
                            StatusCode::TOO_MANY_REQUESTS
                                | StatusCode::GONE
                                | StatusCode::SERVICE_UNAVAILABLE
                        );
                    if rejected && attempt == 0 && routes.len() > 1 {
                        excluded = Some(selected.worker.clone());
                    } else {
                        let status = response.status();
                        if !status.is_success() {
                            return Err(if rejected && status == StatusCode::TOO_MANY_REQUESTS {
                                (StatusCode::TOO_MANY_REQUESTS, "workers not admitted".into())
                            } else {
                                unavailable("worker evaluation failed; no replay")
                            });
                        }
                        let intermediate =
                            bounded(response, 1024 * 1024).await.map_err(unavailable)?;
                        router.stats.intermediate_bytes.fetch_add(
                            intermediate.len() as u64,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        let result: Intermediate =
                            serde_json::from_slice(&intermediate).map_err(unavailable)?;
                        if result.binding.as_ref() != request.binding.as_ref()
                            || result.generation != request.generation
                            || result.shard_id != request.shard_id
                            || result.epoch != request.epoch
                        {
                            return Err(unavailable("worker binding mismatch"));
                        }
                        answer = Some(result.coefficients);
                    }
                }
            }
            drop(selected);
            if answer.is_some() {
                break;
            }
        }
        let answer = answer.ok_or_else(|| unavailable("no admitted evaluation"))?;
        router
            .response_allowed(&session, loaded.view.controller_epoch)
            .map_err(unavailable)?;
        let epoch = loaded.view.controller_epoch;
        let began = Instant::now();
        let (response, guards) = tokio::task::spawn_blocking(move || {
            let response = pack.pack(&bytes, &answer).map_err(bad)?;
            Ok::<_, Error>((response, (permit, loaded, pack)))
        })
        .await
        .map_err(unavailable)??;
        router.stats.packing_micros.fetch_add(
            began.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        router.response_allowed(&session, epoch).map_err(|e| {
            if e == "noncanonical_session" {
                (StatusCode::GONE, e)
            } else {
                unavailable(e)
            }
        })?;
        Ok::<_, Error>(
            crate::response_body::guarded(response, guards, move || {
                router.response_allowed(&session, epoch)
            })
            .into_response(),
        )
    });
    task.await.map_err(unavailable)?
}

#[cfg(test)]
mod tests {
    use super::*;
    fn router(root: &Path) -> PackingRouter {
        PackingRouter::open(root, "http://127.0.0.1:1234", 6, 4).unwrap()
    }
    fn view() -> View {
        let vector: serde_json::Value = serde_json::from_str(include_str!(
            "../../../crates/enhance-pir/tests/fixtures/v7-session.json"
        ))
        .unwrap();
        View {
            version: CONTROL_VERSION,
            controller_epoch: 1,
            revocation: Revocation::default(),
            snapshots: vec![ServingSnapshot {
                manifest: serde_json::from_value(vector["manifest"].clone()).unwrap(),
                routes: BTreeMap::new(),
                artifacts: BTreeMap::new(),
            }],
        }
    }
    #[tokio::test]
    async fn restart_requires_fresh_incarnation_readiness_and_revocations_are_durable() {
        let root = tempfile::tempdir().unwrap();
        let r = router(root.path());
        let ack = prepare(State(r.clone()), Json(view())).await.unwrap().0;
        let a = Activation {
            incarnation: ack.incarnation,
            digest: ack.digest,
            controller_epoch: ack.controller_epoch,
        };
        assert!(r.response_allowed(&"aa".repeat(32), 1).is_err());
        activate(State(r.clone()), Json(a.clone())).await.unwrap();
        assert!(r.response_allowed(&"aa".repeat(32), 1).is_ok());
        revoke(
            State(r.clone()),
            Json(Revocation {
                recovery_epoch: 1,
                sessions: ["aa".repeat(32)].into(),
            }),
        )
        .await
        .unwrap();
        assert!(r.response_allowed(&"aa".repeat(32), 1).is_err());
        assert!(refresh(State(r.clone()), Json(a.clone())).await.is_err());
        drop(r);
        let r = router(root.path());
        assert!(activate(State(r.clone()), Json(a)).await.is_err());
        assert!(revoke(State(r.clone()), Json(Revocation::default()))
            .await
            .is_err());
        assert!(prepare(State(r), Json(view())).await.is_err());
    }
    #[test]
    fn selection_counts_outstanding_and_rotates_ties() {
        let root = tempfile::tempdir().unwrap();
        let r = router(root.path());
        let routes = vec!["http://a".into(), "http://b".into()];
        let a = select(&r, &routes, None).unwrap();
        let b = select(&r, &routes, None).unwrap();
        assert_ne!(a.worker, b.worker);
        let excluded = a.worker.clone();
        drop(a);
        drop(b);
        assert_eq!(
            select(&r, &routes, Some(&excluded)).unwrap().worker,
            routes.iter().find(|s| **s != excluded).unwrap().clone()
        );
        assert!(select(&r, &routes[..1], Some(&routes[0])).is_err());
    }
    #[tokio::test]
    async fn watchdog_stops_admission_without_authorizing_recovery() {
        let root = tempfile::tempdir().unwrap();
        let r = router(root.path());
        let ack = prepare(State(r.clone()), Json(view())).await.unwrap().0;
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
        r.inner.lock().unwrap().refreshed = Some(Instant::now() - Duration::from_secs(6));
        assert!(r.response_allowed(&"bb".repeat(32), 1).is_err());
        assert!(r.inner.lock().unwrap().fence.revocation.sessions.is_empty());
    }
}
