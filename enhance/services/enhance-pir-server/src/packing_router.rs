//! Private control plane and public serving process for exact-session packing.
//! Control endpoints must only be reachable by the coordinator. Artifact origins
//! are configured locally, never supplied by a wallet or placement message.
use crate::{
    runtime::Packing,
    worker::{Evaluate, Revocation},
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

#[cfg(not(feature = "native-reinspiring"))]
pub const CONTROL_VERSION: u16 = 2;
#[cfg(feature = "native-reinspiring")]
pub const CONTROL_VERSION: u16 = 4;
const BODY_LIMIT: usize = 512 * 1024;
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
    #[serde(default)]
    pub preferred: BTreeMap<u64, Vec<String>>,
    /// Domain -> (owned artifact filename, SHA-256 of the complete artifact).
    pub artifacts: BTreeMap<u64, crate::prepared_packing::Artifact>,
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
    worker_health: BTreeMap<String, WorkerHealth>,
}

#[derive(Clone, Copy, Debug, Default)]
struct WorkerHealth {
    available: bool,
    successes: u8,
    failures: u8,
}

#[derive(Clone)]
pub struct PackingRouter {
    packing_budget: crate::packing_budget::PackingBudget,
    inner: Arc<Mutex<Inner>>,
    preparation: Arc<AsyncMutex<()>>,
    loader: std::sync::mpsc::Sender<Box<dyn FnOnce() + Send>>,
    admission: Arc<Semaphore>,
    waiters: Arc<Semaphore>,
    root: PathBuf,
    origin: String,
    incarnation: String,
    http: reqwest::Client,
    max_objects: usize,
    stats: Arc<Stats>,
    timing: crate::query_timing::QueryTiming,
    _lock: Arc<File>,
}

#[derive(Default)]
struct Stats {
    download_micros: std::sync::atomic::AtomicU64,
    download_bytes: std::sync::atomic::AtomicU64,
    load_micros: std::sync::atomic::AtomicU64,
    loads: std::sync::atomic::AtomicU64,
    load_failures: std::sync::atomic::AtomicU64,
    cache_hits: std::sync::atomic::AtomicU64,
    successful: std::sync::atomic::AtomicU64,
    rejected: std::sync::atomic::AtomicU64,
    failed: std::sync::atomic::AtomicU64,
    retries: std::sync::atomic::AtomicU64,
    intermediate_bytes: std::sync::atomic::AtomicU64,
    packing_micros: std::sync::atomic::AtomicU64,
    preferred_selections: std::sync::atomic::AtomicU64,
    health_demotions: std::sync::atomic::AtomicU64,
    health_recoveries: std::sync::atomic::AtomicU64,
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
        let (loader, jobs) = std::sync::mpsc::channel::<Box<dyn FnOnce() + Send>>();
        std::thread::Builder::new()
            .name("packing-loader".into())
            .spawn(move || {
                while let Ok(job) = jobs.recv() {
                    job();
                }
            })
            .map_err(|e| e.to_string())?;
        Ok(Self {
            loader,
            packing_budget: crate::packing_budget::PackingBudget::router(),
            inner: Arc::new(Mutex::new(Inner {
                fence,
                active: None,
                candidate: None,
                material: BTreeMap::new(),
                refreshed: None,
                outstanding: BTreeMap::new(),
                cursor: 0,
                worker_health: BTreeMap::new(),
            })),
            preparation: Arc::new(AsyncMutex::new(())),
            admission: Arc::new(Semaphore::new(requests)),
            waiters: Arc::new(Semaphore::new(16)),
            root: root.into(),
            origin: artifact_origin.trim_end_matches('/').into(),
            incarnation: hex::encode(rand::random::<[u8; 16]>()),
            http: crate::internal_auth::client_builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|e| e.to_string())?,
            max_objects,
            stats: Arc::new(Stats::default()),
            timing: Default::default(),
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
            .layer(crate::internal_auth::Token::from_env().layer())
            .with_state(self.clone())
    }

    /// Probe preferred workers independently of query traffic. A restarted
    /// worker is eligible only after it holds the exact active generation.
    pub fn start_worker_health_monitor(&self) {
        let router = self.clone();
        tokio::spawn(async move {
            loop {
                router.probe_preferred_workers().await;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    async fn probe_preferred_workers(&self) {
        let targets: BTreeMap<String, Vec<(u64, String)>> = {
            let inner = self.inner.lock().unwrap();
            let mut targets = BTreeMap::new();
            if let Some(active) = &inner.active {
                for snapshot in &active.view.snapshots {
                    for urls in snapshot.preferred.values() {
                        for url in urls {
                            targets
                                .entry(url.clone())
                                .or_insert_with(Vec::new)
                                .push((snapshot.manifest.generation, digest(&snapshot.manifest)));
                        }
                    }
                }
            }
            targets
        };
        for (url, generations) in targets {
            let healthy = match self
                .http
                .get(format!("{url}/internal/health"))
                .timeout(Duration::from_secs(1))
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => response
                    .json::<serde_json::Value>()
                    .await
                    .ok()
                    .is_some_and(|health| {
                        health["incarnation"]
                            .as_str()
                            .is_some_and(|id| !id.is_empty())
                            && generations.iter().all(|(generation, expected)| {
                                health["published_manifest_digests"][generation.to_string()]
                                    .as_str()
                                    == Some(expected.as_str())
                            })
                    }),
                _ => false,
            };
            let mut inner = self.inner.lock().unwrap();
            let state = inner.worker_health.entry(url).or_default();
            if healthy {
                state.failures = 0;
                state.successes = state.successes.saturating_add(1);
                if state.successes >= 2 && !state.available {
                    state.available = true;
                    self.stats
                        .health_recoveries
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            } else {
                state.successes = 0;
                state.failures = state.failures.saturating_add(1);
                if state.failures >= 2 && state.available {
                    state.available = false;
                    self.stats
                        .health_demotions
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    fn demote_worker(&self, url: &str) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(state) = inner.worker_health.get_mut(url) {
            if state.available {
                self.stats
                    .health_demotions
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            *state = WorkerHealth::default();
        }
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
        "packing_charged_bytes":r.packing_budget.charged_bytes(),
        "available_requests":r.admission.available_permits(),"outstanding":i.outstanding,
        "preferred_workers":i.worker_health.iter().map(|(url, state)| (url.clone(), state.available)).collect::<BTreeMap<_,_>>()}),
    )
}

async fn metrics(State(r): State<PackingRouter>) -> impl IntoResponse {
    use std::sync::atomic::Ordering::Relaxed;
    let i = r.inner.lock().unwrap();
    let values = [
        (
            "artifact_download_microseconds_total",
            r.stats.download_micros.load(Relaxed),
        ),
        (
            "artifact_download_bytes_total",
            r.stats.download_bytes.load(Relaxed),
        ),
        (
            "artifact_load_microseconds_total",
            r.stats.load_micros.load(Relaxed),
        ),
        ("artifact_loads_total", r.stats.loads.load(Relaxed)),
        (
            "artifact_load_failures_total",
            r.stats.load_failures.load(Relaxed),
        ),
        (
            "artifact_cache_hits_total",
            r.stats.cache_hits.load(Relaxed),
        ),
        ("successful_queries_total", r.stats.successful.load(Relaxed)),
        ("rejected_queries_total", r.stats.rejected.load(Relaxed)),
        ("failed_queries_total", r.stats.failed.load(Relaxed)),
        ("evaluation_retries_total", r.stats.retries.load(Relaxed)),
        (
            "preferred_selections_total",
            r.stats.preferred_selections.load(Relaxed),
        ),
        (
            "worker_health_demotions_total",
            r.stats.health_demotions.load(Relaxed),
        ),
        (
            "worker_health_recoveries_total",
            r.stats.health_recoveries.load(Relaxed),
        ),
        (
            "preferred_workers_available",
            i.worker_health.values().filter(|s| s.available).count() as u64,
        ),
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
        ("charged_bytes", r.packing_budget.charged_bytes()),
        (
            "evaluations_outstanding",
            i.outstanding.values().sum::<usize>() as u64,
        ),
    ];
    let mut text = values
        .into_iter()
        .map(|(name, value)| format!("enhance_packing_router_{name} {value}\n"))
        .collect::<String>();
    text.push_str(&r.timing.render());
    text.push_str(&crate::prepared_packing::metrics());
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
    // The preparation guard excludes concurrent downloads. Keep both the new
    // assignment and the currently active view; query pins own decoded state.
    let mut keep: BTreeSet<String> = view
        .snapshots
        .iter()
        .flat_map(|s| s.artifacts.values().map(|a| format!("{}.bin", a.sha256)))
        .collect();
    if let Some(active) = &r.inner.lock().unwrap().active {
        keep.extend(
            active
                .view
                .snapshots
                .iter()
                .flat_map(|s| s.artifacts.values().map(|a| format!("{}.bin", a.sha256))),
        );
    }
    for entry in fs::read_dir(r.root.join("artifacts")).map_err(unavailable)? {
        let entry = entry.map_err(unavailable)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let owned = name
            .strip_suffix(".bin")
            .or_else(|| name.strip_suffix(".part"))
            .is_some_and(canonical_hash);
        if owned && !keep.contains(&name) && entry.file_type().map_err(unavailable)?.is_file() {
            fs::remove_file(entry.path()).map_err(unavailable)?;
        }
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
        for (&id, artifact) in &snapshot.artifacts {
            let hash = &artifact.sha256;
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
            artifact
                .validate(shard.logical_rows, &reference.public_params_sha256)
                .map_err(bad)?;
            if !canonical_hash(hash) {
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
                    if i.material.len() > r.max_objects {
                        return Err(unavailable("construction overlap limit"));
                    }
                }
                let rows = shard.logical_rows;
                let reference = reference.clone();
                let artifact = artifact.clone();
                let router = r.clone();
                let material_key = key.clone();
                let guard = _preparing.clone();
                // Dedicated loading thread: never enter the query Rayon/blocking pools.
                let (tx, rx) = tokio::sync::oneshot::channel();
                let handle = tokio::runtime::Handle::current();
                r.loader
                    .send(Box::new(move || {
                        let _guard = guard;
                        let result = (|| -> Result<Arc<Packing>, String> {
                            use std::sync::atomic::Ordering::Relaxed;
                            let reservation =
                                crate::packing_budget::Charge::mapping(&router.packing_budget)?;
                            let path = router
                                .root
                                .join("artifacts")
                                .join(format!("{}.bin", artifact.sha256));
                            if crate::prepared_packing::verify(&path, &artifact).is_err() {
                                let began = Instant::now();
                                handle.block_on(download_prepared(&router, &artifact, &path))?;
                                router
                                    .stats
                                    .download_micros
                                    .fetch_add(began.elapsed().as_micros() as u64, Relaxed);
                                router
                                    .stats
                                    .download_bytes
                                    .fetch_add(artifact.bytes, Relaxed);
                            } else {
                                router.stats.cache_hits.fetch_add(1, Relaxed);
                            }
                            drop(reservation);
                            let began = Instant::now();
                            let pack = Arc::new(crate::prepared_packing::load(
                                &path,
                                &artifact,
                                rows,
                                &router.packing_budget,
                            )?);
                            if pack.reference(id)? != reference {
                                return Err("prepared artifact session mismatch".into());
                            }
                            router
                                .stats
                                .load_micros
                                .fetch_add(began.elapsed().as_micros() as u64, Relaxed);
                            router.stats.loads.fetch_add(1, Relaxed);
                            router
                                .inner
                                .lock()
                                .unwrap()
                                .material
                                .insert(material_key, Arc::downgrade(&pack));
                            Ok(pack)
                        })();
                        if result.is_err() {
                            router
                                .stats
                                .load_failures
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        let _ = tx.send(result);
                    }))
                    .map_err(unavailable)?;
                rx.await.map_err(unavailable)?.map_err(unavailable)?
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
    preferred: &[String],
    excluded: Option<&str>,
) -> Result<Outstanding, Error> {
    let mut i = r.inner.lock().unwrap();
    let ready_preferred = |url: &String| {
        i.worker_health
            .get(url)
            .is_some_and(|state| state.available)
    };
    let idle_preferred = preferred.iter().find(|url| {
        eligible.contains(url)
            && Some(url.as_str()) != excluded
            && ready_preferred(url)
            && i.outstanding.get(*url).copied().unwrap_or(0) == 0
    });
    let worker = if let Some(url) = idle_preferred {
        url.clone()
    } else {
        let ordinary: Vec<_> = eligible
            .iter()
            .filter(|url| !preferred.contains(url) && Some(url.as_str()) != excluded)
            .collect();
        let choices: Vec<_> = if ordinary.is_empty() {
            eligible
                .iter()
                .filter(|url| Some(url.as_str()) != excluded && ready_preferred(url))
                .collect()
        } else {
            ordinary
        };
        let min = choices
            .iter()
            .map(|u| *i.outstanding.get(*u).unwrap_or(&0))
            .min()
            .ok_or_else(|| unavailable("no ready worker"))?;
        let tied: Vec<_> = choices
            .iter()
            .filter(|u| *i.outstanding.get(**u).unwrap_or(&0) == min)
            .collect();
        (*tied[i.cursor % tied.len()]).clone()
    };
    if preferred.contains(&worker) {
        r.stats
            .preferred_selections
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    i.cursor = i.cursor.wrapping_add(1);
    *i.outstanding.entry(worker.clone()).or_default() += 1;
    Ok(Outstanding {
        router: r.clone(),
        worker,
    })
}

pub(crate) use crate::query_serving::{bounded, public_error};

async fn query(State(r): State<PackingRouter>, request: Request) -> Response {
    use std::sync::atomic::Ordering::Relaxed;
    let started = Instant::now();
    match serve(r.clone(), request).await {
        Ok(mut response) => {
            if let Some(stages) = response
                .extensions_mut()
                .remove::<crate::query_timing::CompletedStages>()
            {
                r.timing.observe(stages, started.elapsed());
            }
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
    // Read the bounded body before any queue or admission permit is charged.
    let body_started = Instant::now();
    let bytes = tokio::time::timeout(
        Duration::from_secs(30),
        to_bytes(request.into_body(), BODY_LIMIT),
    )
    .await
    .map_err(|_| (StatusCode::REQUEST_TIMEOUT, "body deadline".into()))?
    .map_err(|e| (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()))?;
    let body_read = body_started.elapsed();
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
    let binding = QueryBinding::decode(&bytes).map_err(bad)?;
    let session = hex::encode(binding.session_id);
    let (routes, preferred, epoch, pack) = {
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
        crate::query_serving::validate_binding(m, binding)?;
        let pack = loaded
            .packing
            .get(&session)
            .cloned()
            .ok_or_else(|| (StatusCode::GONE, "session_unavailable".into()))?;
        let snapshot = &loaded.view.snapshots[0];
        let routes = snapshot
            .routes
            .get(&binding.shard_id)
            .cloned()
            .ok_or_else(|| unavailable("unassigned session"))?;
        let preferred = snapshot
            .preferred
            .get(&binding.shard_id)
            .cloned()
            .unwrap_or_default();
        (routes, preferred, loaded.view.controller_epoch, pack)
    };
    // Once admitted, the task owns the permit through cancellation and CPU work.
    let router = r.clone();
    let task = crate::query_serving::admitted(async move {
        let coefficients = pack.query_coefficients(&bytes, binding).map_err(bad)?;
        let request = Evaluate {
            binding: Some(binding.encode()),
            generation: binding.generation,
            shard_id: binding.shard_id,
            epoch: hex::encode(binding.epoch),
            session_id: session.clone(),
            coefficients,
        };
        let answer =
            crate::query_serving::evaluate(&router.http, &request, routes.len(), |excluded| {
                if let Some(url) = excluded {
                    router.demote_worker(url);
                }
                let lease = select(&router, &routes, &preferred, excluded)?;
                if excluded.is_some() {
                    router
                        .stats
                        .retries
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Ok((lease.worker.clone(), lease))
            })
            .await?;
        router.stats.intermediate_bytes.fetch_add(
            answer.intermediate_bytes as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        let worker_time = answer.worker_time;
        let answer = answer.coefficients;
        router
            .response_allowed(&session, epoch)
            .map_err(unavailable)?;
        let (response, guards, packing_time) = tokio::task::spawn_blocking(move || {
            let began = Instant::now();
            let response = pack.pack(&bytes, &answer).map_err(bad)?;
            Ok::<_, Error>((response, (permit, pack), began.elapsed()))
        })
        .await
        .map_err(unavailable)??;
        router.stats.packing_micros.fetch_add(
            packing_time.as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        router.response_allowed(&session, epoch).map_err(|e| {
            if e == "noncanonical_session" {
                (StatusCode::GONE, e)
            } else {
                unavailable(e)
            }
        })?;
        let mut response = crate::response_body::guarded(response, guards, move || {
            router.response_allowed(&session, epoch)
        })
        .into_response();
        response
            .extensions_mut()
            .insert(crate::query_timing::CompletedStages {
                body_read,
                worker: worker_time,
                packing: packing_time,
            });
        Ok::<_, Error>(response)
    });
    task.await?
}

async fn download_prepared(
    router: &PackingRouter,
    artifact: &crate::prepared_packing::Artifact,
    path: &Path,
) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;
    let temporary = path.with_extension("part");
    let result = async {
        let mut response = router
            .http
            .get(format!(
                "{}/internal/prepared-packing-artifact/{}",
                router.origin, artifact.name
            ))
            .timeout(Duration::from_secs(600))
            .send()
            .await
            .map_err(|e| e.to_string())?
            .error_for_status()
            .map_err(|e| e.to_string())?;
        if response
            .content_length()
            .is_some_and(|n| n != artifact.bytes)
        {
            return Err("prepared artifact length mismatch".into());
        }
        let mut file = tokio::fs::File::create(&temporary)
            .await
            .map_err(|e| e.to_string())?;
        let mut bytes = 0u64;
        let mut hash = Sha256::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
            bytes = bytes
                .checked_add(chunk.len() as u64)
                .ok_or("artifact length overflow")?;
            if bytes > artifact.bytes {
                return Err("prepared artifact exceeds bound".into());
            }
            hash.update(&chunk);
            file.write_all(&chunk).await.map_err(|e| e.to_string())?;
        }
        if bytes != artifact.bytes || hex::encode(hash.finalize()) != artifact.sha256 {
            return Err("prepared artifact checksum/length mismatch".into());
        }
        file.sync_all().await.map_err(|e| e.to_string())?;
        let file = file.into_std().await;
        crate::artifact::release_file_cache(&file);
        drop(file);
        tokio::fs::rename(&temporary, path)
            .await
            .map_err(|e| e.to_string())?;
        File::open(path.parent().unwrap())
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
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
                preferred: BTreeMap::new(),
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
        let a = select(&r, &routes, &[], None).unwrap();
        let b = select(&r, &routes, &[], None).unwrap();
        assert_ne!(a.worker, b.worker);
        let excluded = a.worker.clone();
        drop(a);
        drop(b);
        assert_eq!(
            select(&r, &routes, &[], Some(&excluded)).unwrap().worker,
            routes.iter().find(|s| **s != excluded).unwrap().clone()
        );
        assert!(select(&r, &routes[..1], &[], Some(&routes[0])).is_err());
    }
    #[test]
    fn preferred_worker_is_skipped_when_unhealthy_or_busy() {
        let root = tempfile::tempdir().unwrap();
        let r = router(root.path());
        let routes = vec!["http://cpu".into(), "http://gpu".into()];
        let preferred = vec!["http://gpu".into()];
        assert_eq!(
            select(&r, &routes, &preferred, None).unwrap().worker,
            "http://cpu"
        );
        r.inner.lock().unwrap().worker_health.insert(
            "http://gpu".into(),
            WorkerHealth {
                available: true,
                successes: 2,
                failures: 0,
            },
        );
        let gpu = select(&r, &routes, &preferred, None).unwrap();
        assert_eq!(gpu.worker, "http://gpu");
        assert_eq!(
            select(&r, &routes, &preferred, None).unwrap().worker,
            "http://cpu"
        );
        drop(gpu);
        r.demote_worker("http://gpu");
        assert_eq!(
            select(&r, &routes, &preferred, None).unwrap().worker,
            "http://cpu"
        );
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
