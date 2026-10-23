//! Autonomous workers load published rows; they never prepare public hints.
use crate::enhance::synchronization::{Client, Config as SourceConfig, Error, Lease, Safety};
use crate::synchronization::SyncTask;
use axum::{
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use enhance_query_server::worker::{Evaluate, Intermediate};
use enhance_shared::{
    ipir::{RowPlaintextIter, ShardRuntime},
    matvec::MatvecConfig,
    placement::{PlacementPolicy, MIB, OVERHEAD, RESIDENT_LIMIT},
    protocol::{
        self, digest, Manifest, QueryBinding, ShardState, HEADER_BYTES, RETAINED_GENERATIONS,
    },
    publication::Domain,
    runtime::{rlwe, DomainPlan},
};
use pir_runtime::evaluation::{add_intermediate_assign_mod, PreparedDatabase};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

/// Peer promises span the existing private evaluation deadline. The router
/// renews them while waiting, loading, and serving; a lost process cannot pin forever.
pub const PIN_LIFETIME: Duration = Duration::from_secs(30);
const MAX_PINS: usize = 64;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(flatten)]
    pub source: SourceConfig,
    /// Operator-owned responsibilities; no placement commands come from the publisher.
    pub shards: BTreeSet<u64>,
}

impl Config {
    pub fn validate(&self, policy: PlacementPolicy) -> Result<(), Error> {
        self.source.validate()?;
        policy.validate()?;
        if self.shards.is_empty()
            || self.shards.len() > policy.sealed_shards
            || self
                .shards
                .iter()
                .any(|id| *id >= protocol::MAX_QUERY_SHARDS)
        {
            return Err("worker needs a bounded nonempty shard assignment".into());
        }
        Ok(())
    }
}

struct Loaded {
    plan: DomainPlan,
    units: Vec<Arc<ShardRuntime>>,
}

impl Loaded {
    fn evaluate(&self, coefficients: &[u64]) -> Result<Vec<u64>, String> {
        let params = protocol::parameters(self.plan.shard.logical_rows)?;
        if coefficients.len() != params.db_rows
            || coefficients
                .iter()
                .any(|q| *q >= enhance_shared::runtime::modulus())
        {
            return Err("invalid query coefficients".into());
        }
        let mut output = vec![0; params.db_cols];
        for (unit, identity) in self.units.iter().zip(&self.plan.units) {
            let start = identity.local_row_start as usize;
            let answer = unit
                .evaluate(
                    rlwe(),
                    &coefficients[start..start + identity.allocated_rows as usize],
                )
                .map_err(|e| e.to_string())?;
            add_intermediate_assign_mod(&mut output, &answer, enhance_shared::runtime::modulus())
                .map_err(|e| e.to_string())?;
        }
        Ok(output)
    }
}

struct View {
    manifest: Manifest,
    domains: BTreeMap<u64, Arc<Loaded>>,
    lease: Lease,
}

#[derive(Default)]
struct Inner {
    active: Option<String>,
    views: BTreeMap<String, View>,
    units: BTreeMap<String, Weak<ShardRuntime>>,
    pins: BTreeMap<(String, String), std::time::Instant>,
}

#[derive(Clone)]
pub struct Worker {
    root: PathBuf,
    config: Config,
    policy: PlacementPolicy,
    backend: MatvecConfig,
    safety: Safety,
    inner: Arc<Mutex<Inner>>,
    authority: Arc<AsyncMutex<Client>>,
    evaluation: Arc<Semaphore>,
    synchronization: Arc<Semaphore>,
    pub incarnation: String,
    matvec_latency: Arc<prometheus::Histogram>,
    _ownership: Arc<File>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadinessRequest {
    pub manifest_sha256: String,
    #[serde(default)]
    pub holder: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Release {
    pub manifest_sha256: String,
    pub holder: String,
    pub worker_incarnation: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Readiness {
    pub incarnation: String,
    pub manifest_sha256: String,
    pub manifest: Manifest,
    pub shards: BTreeSet<u64>,
}

type ApiResult<T> = Result<T, (StatusCode, String)>;
fn unavailable(error: impl ToString) -> (StatusCode, String) {
    (StatusCode::SERVICE_UNAVAILABLE, error.to_string())
}

impl Worker {
    pub fn open(
        root: &Path,
        config: Config,
        policy: PlacementPolicy,
        backend: MatvecConfig,
    ) -> Result<Self, Error> {
        config.validate(policy)?;
        backend.validate()?;
        fs::create_dir_all(root)?;
        let ownership = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("worker.lock"))?;
        ownership
            .try_lock()
            .map_err(|e| format!("worker state already owned: {e}"))?;
        if root.join("worker.json").exists() {
            return Err("autonomous worker requires a separate state directory".into());
        }
        let safety = Safety::open(root, &config.source.publisher_origin)?;
        let authority = Client::new(root, &config.source, safety.clone())?;
        Ok(Self {
            root: root.into(),
            config,
            policy,
            backend,
            safety,
            inner: Arc::new(Mutex::new(Inner::default())),
            authority: Arc::new(AsyncMutex::new(authority)),
            evaluation: Arc::new(Semaphore::new(2)),
            synchronization: Arc::new(Semaphore::new(1)),
            incarnation: format!("{:032x}", rand::random::<u128>()),
            matvec_latency: Arc::new(prometheus::Histogram::with_opts(
                prometheus::HistogramOpts::new(
                    "enhance_worker_matvec_duration_seconds",
                    "Successful matrix-vector evaluation time measured inside this worker",
                ),
            )?),
            _ownership: Arc::new(ownership),
        })
    }

    pub fn synchronizer(&self) -> Result<Synchronizer, Error> {
        Ok(Synchronizer {
            _ownership: self
                .synchronization
                .clone()
                .try_acquire_owned()
                .map_err(|_| "worker synchronization already owned")?,
            worker: self.clone(),
            client: Client::new(&self.root, &self.config.source, self.safety.clone())?,
        })
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/internal/health", get(health))
            .route("/internal/metrics", get(metrics))
            .route("/internal/readiness", post(readiness))
            .route("/internal/release", post(release))
            .route("/internal/evaluate", post(evaluate))
            .layer(axum::extract::DefaultBodyLimit::max(1024 * 1024))
            .layer(crate::enhance::internal_auth::Token::from_env().layer())
            .with_state(self)
    }

    async fn renew(&self, manifest: &Manifest) -> Result<Lease, Error> {
        let id = digest(manifest);
        if let Some(lease) = self
            .inner
            .lock()
            .unwrap()
            .views
            .get(&id)
            .map(|view| view.lease.clone())
            .filter(|lease| lease.check(manifest, &self.safety).is_ok() && lease.has_release_time())
        {
            return Ok(lease);
        }
        // Source authority has a five-second window, shorter than a delayed poll.
        // Admission may obtain fresh authority, never a timer-based extension.
        let mut client = self.authority.lock().await;
        if let Some(lease) = self
            .inner
            .lock()
            .unwrap()
            .views
            .get(&id)
            .map(|view| view.lease.clone())
            .filter(|lease| lease.check(manifest, &self.safety).is_ok() && lease.has_release_time())
        {
            return Ok(lease);
        }
        client.begin().await?;
        let lease = client.authority(manifest).await?;
        if let Some(view) = self.inner.lock().unwrap().views.get_mut(&id) {
            view.lease = lease.clone();
        }
        Ok(lease)
    }

    fn live(&self) -> BTreeMap<String, u64> {
        self.inner
            .lock()
            .unwrap()
            .units
            .iter()
            .filter_map(|(id, unit)| {
                unit.upgrade()
                    .map(|unit| (id.clone(), unit.server.db().len() as u64 * 2))
            })
            .collect()
    }

    fn admit(&self, domains: &[Domain]) -> Result<(), Error> {
        let plans = domains.iter().map(|d| &d.plan).collect::<Vec<_>>();
        self.reservation(&plans).map(drop)
    }

    fn reservation(&self, plans: &[&DomainPlan]) -> Result<u64, Error> {
        let active = plans.iter().any(|p| p.shard.state != ShardState::Sealed);
        if plans.len() > self.policy.limit(active)
            || plans
                .iter()
                .filter(|p| p.shard.state == ShardState::Growing)
                .count()
                > 1
        {
            return Err("worker shard role limit exceeded".into());
        }
        let mut sizes = self.live();
        let mut current = 0u64;
        for plan in plans {
            plan.validate()?;
            for unit in &plan.units {
                let bytes = unit
                    .allocated_rows
                    .checked_mul(enhance_shared::ROW_BYTES as u64)
                    .ok_or("unit size overflow")?;
                sizes.insert(unit.digest(), bytes);
                current = current.checked_add(bytes).ok_or("worker size overflow")?;
            }
        }
        let retained: u64 = sizes.values().sum();
        let growth = (plans.len() as u64 * 768 * MIB)
            .checked_sub(current)
            .ok_or("worker growth reservation overflow")?;
        let transition = if active {
            (5 * 192 * MIB).saturating_sub(retained - current) + 96 * MIB
        } else {
            0
        };
        if retained + growth + transition + OVERHEAD > RESIDENT_LIMIT {
            return Err("full growth/transition reservation does not fit".into());
        }
        Ok(retained + growth + transition + OVERHEAD)
    }

    fn load(&self, domains: Vec<Domain>) -> Result<BTreeMap<u64, Arc<Loaded>>, Error> {
        self.admit(&domains)?;
        let mut loaded = BTreeMap::new();
        for domain in domains {
            let mut units = Vec::new();
            for (identity, object) in domain.plan.units.iter().zip(&domain.rows) {
                let id = identity.digest();
                let existing = self
                    .inner
                    .lock()
                    .unwrap()
                    .units
                    .get(&id)
                    .and_then(Weak::upgrade);
                let unit = if let Some(unit) = existing {
                    unit
                } else {
                    let layout = enhance_shared::types::DatabaseLayout {
                        shard_rows: identity.allocated_rows as usize,
                        ..enhance_shared::types::ENHANCE_LAYOUT
                    };
                    let file = File::open(object.path(&self.root)?)?;
                    if file.metadata()?.len() != object.bytes {
                        return Err("cached rows have wrong length".into());
                    }
                    // SAFETY: cache inodes are immutable after atomic installation;
                    // cleanup only unlinks them. Bind the handle again before loading.
                    let map = unsafe { memmap2::MmapOptions::new().map(&file) }?;
                    use sha2::{Digest, Sha256};
                    if hex::encode(Sha256::digest(&map)) != object.sha256 {
                        return Err("cached rows have wrong checksum".into());
                    }
                    let (_, params) = enhance_shared::ipir::shard_parameters(&layout)?;
                    let coefficients = RowPlaintextIter::new(
                        &map,
                        layout.row_bytes(),
                        params.db_rows,
                        params.db_cols,
                        params.p.trailing_zeros() as usize,
                    );
                    let server =
                        PreparedDatabase::new(self.backend.server(params, coefficients, false)?);
                    let unit = Arc::new(ShardRuntime {
                        shard_id: domain.plan.shard.id,
                        query_row_start: identity.local_row_start as usize,
                        rows_sha256: object.sha256.clone(),
                        server,
                    });
                    enhance_shared::artifact::release_file_cache(&file);
                    self.inner
                        .lock()
                        .unwrap()
                        .units
                        .insert(id, Arc::downgrade(&unit));
                    unit
                };
                units.push(unit);
            }
            loaded.insert(
                domain.plan.shard.id,
                Arc::new(Loaded {
                    plan: domain.plan,
                    units,
                }),
            );
        }
        Ok(loaded)
    }
}

pub struct Synchronizer {
    _ownership: tokio::sync::OwnedSemaphorePermit,
    worker: Worker,
    client: Client,
}

impl Synchronizer {
    pub async fn poll_once(&mut self) -> Result<(), Error> {
        self.client.begin().await?;
        let response = self.client.current().await?;
        let Some(manifest) = response.current.manifest.clone() else {
            // A valid empty current is a publisher still preparing its first
            // publication. Keep the ordinary cadence without granting a lease.
            return Ok(());
        };
        let observed = Lease::from_response(response.clone(), &manifest, &self.worker.safety)?;
        let id = digest(&manifest);
        {
            let mut inner = self.worker.inner.lock().unwrap();
            let active = inner.active.clone();
            inner
                .pins
                .retain(|_, deadline| *deadline > std::time::Instant::now());
            let pinned: BTreeSet<_> = inner.pins.keys().map(|(_, id)| id.clone()).collect();
            inner.views.retain(|key, view| {
                self.worker.safety.permits(&view.manifest).is_ok()
                    && (Some(key) == active.as_ref()
                        || pinned.contains(key)
                        || view
                            .lease
                            .check(&view.manifest, &self.worker.safety)
                            .is_ok())
            });
            inner.units.retain(|_, unit| unit.strong_count() > 0);
            if let Some(view) = inner.views.get_mut(&id) {
                view.lease = observed;
                return Ok(());
            }
            // A fresh lease authorizes bytes, but only an upstream pin promises
            // that unadmitted requests may still need a prior view. Admitted
            // evaluations already own their loaded domains independently.
            if inner.views.len() >= RETAINED_GENERATIONS {
                let removable = inner
                    .views
                    .iter()
                    .filter(|(key, _)| Some(*key) != active.as_ref() && !pinned.contains(*key))
                    .map(|(key, view)| (view.manifest.generation, key.clone()))
                    .min();
                if let Some((_, key)) = removable {
                    inner.views.remove(&key);
                }
            }
            if !inner.views.contains_key(&id) && inner.views.len() >= RETAINED_GENERATIONS {
                // Waiting for old material is coordination, not a transport retry.
                return Ok(());
            }
        }
        let inventory = self
            .client
            .inventory(
                response
                    .current
                    .inventory
                    .as_ref()
                    .ok_or("missing inventory")?,
                &manifest,
            )
            .await?;
        let domains = inventory
            .domains
            .into_iter()
            .filter(|d| self.worker.config.shards.contains(&d.plan.shard.id))
            .collect::<Vec<_>>();
        self.worker.admit(&domains)?;
        let keep = domains
            .iter()
            .flat_map(|d| &d.rows)
            .map(|object| object.sha256.clone())
            .collect();
        self.client.collect(&keep)?;
        for domain in &domains {
            for object in &domain.rows {
                self.client.object(object).await?;
            }
        }
        let worker = self.worker.clone();
        let loaded = tokio::task::spawn_blocking(move || worker.load(domains)).await??;
        let lease = self.client.authority(&manifest).await?;
        if !lease.continues(&observed) {
            return Err("Enhance publication withdrawn while loading".into());
        }
        lease.check(&manifest, &self.worker.safety)?;
        let mut inner = self.worker.inner.lock().unwrap();
        inner.views.insert(
            id.clone(),
            View {
                manifest,
                domains: loaded,
                lease,
            },
        );
        inner.active = Some(id);
        Ok(())
    }
}

impl SyncTask for Synchronizer {
    type Report = ();
    type Error = Error;
    async fn synchronize(&mut self) -> Result<(), Error> {
        self.poll_once().await
    }
    fn report(&self, result: &Result<(), Error>) {
        if let Err(error) = result {
            tracing::warn!(%error, "Enhance worker synchronization deferred");
        }
    }
    /// Only a confirmed publisher contract version mismatch stops the host.
    fn terminal(&self, error: &Error) -> bool {
        pir_sync_manager::is_incompatible(error.as_ref())
    }
}

async fn health(State(worker): State<Worker>) -> Json<serde_json::Value> {
    let inner = worker.inner.lock().unwrap();
    Json(
        serde_json::json!({"binary_sha256": pir_control::Identity::process().binary_sha256,
        "incarnation": worker.incarnation, "matvec": worker.backend, "placement_policy": worker.policy,
        "active": inner.active, "protocol": protocol::PROTOCOL_REVISION,
        "ready": inner.active.as_ref().and_then(|id| inner.views.get(id))
            .is_some_and(|view| view.lease.check(&view.manifest, &worker.safety).is_ok())}),
    )
}

async fn metrics(State(worker): State<Worker>) -> impl IntoResponse {
    let (plans, retained, ready) = {
        let inner = worker.inner.lock().unwrap();
        let active = inner.active.as_ref().and_then(|id| inner.views.get(id));
        let plans = active
            .map(|view| {
                view.domains
                    .values()
                    .map(|d| d.plan.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let ready =
            active.is_some_and(|view| view.lease.check(&view.manifest, &worker.safety).is_ok());
        (plans, inner.views.len(), ready)
    };
    let mut metrics = crate::enhance::telemetry::Metrics::default();
    metrics.number("enhance_worker_up", 1);
    metrics.number("enhance_worker_ready", u64::from(ready));
    metrics.number("enhance_worker_retained_generations", retained as u64);
    metrics.number(
        "enhance_worker_live_database_bytes",
        worker.live().values().sum(),
    );
    metrics.number(
        "enhance_worker_queries_in_flight",
        (2 - worker.evaluation.available_permits()) as u64,
    );
    let plans = plans.iter().collect::<Vec<_>>();
    match worker.reservation(&plans) {
        Ok(reserved) => {
            metrics.number("enhance_worker_memory_sample_available", 1);
            metrics.number("enhance_worker_model_reserved_bytes", reserved);
            metrics.number("enhance_worker_model_within_limit", 1);
        }
        Err(_) => {
            metrics.number("enhance_worker_memory_sample_available", 0);
        }
    }
    use prometheus::{core::Collector, Encoder};
    let mut histogram = Vec::new();
    prometheus::TextEncoder::new()
        .encode(&worker.matvec_latency.collect(), &mut histogram)
        .unwrap();
    (
        [("content-type", "text/plain; version=0.0.4")],
        metrics.finish()
            + &String::from_utf8(histogram).unwrap()
            + &crate::enhance::memory_observation::observe().render("enhance_worker_"),
    )
}

async fn release(
    State(worker): State<Worker>,
    Json(request): Json<Release>,
) -> ApiResult<StatusCode> {
    if request.worker_incarnation != worker.incarnation {
        return Err((StatusCode::CONFLICT, "worker incarnation changed".into()));
    }
    worker
        .inner
        .lock()
        .unwrap()
        .pins
        .remove(&(request.holder, request.manifest_sha256));
    Ok(StatusCode::NO_CONTENT)
}

async fn readiness(
    State(worker): State<Worker>,
    Json(request): Json<ReadinessRequest>,
) -> ApiResult<Json<Readiness>> {
    let manifest = worker
        .inner
        .lock()
        .unwrap()
        .views
        .get(&request.manifest_sha256)
        .map(|view| view.manifest.clone())
        .ok_or_else(|| unavailable("requested worker material is not loaded"))?;
    let lease = worker.renew(&manifest).await.map_err(unavailable)?;
    lease
        .check(&manifest, &worker.safety)
        .map_err(unavailable)?;
    let mut inner = worker.inner.lock().unwrap();
    let view = inner
        .views
        .get(&request.manifest_sha256)
        .ok_or_else(|| unavailable("worker material retired"))?;
    let shards = view.domains.keys().copied().collect();
    if let Some(holder) = &request.holder {
        if holder.len() != 32
            || !holder
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err((StatusCode::BAD_REQUEST, "invalid peer incarnation".into()));
        }
        inner
            .pins
            .retain(|_, deadline| *deadline > std::time::Instant::now());
        let key = (holder.clone(), request.manifest_sha256.clone());
        if !inner.pins.contains_key(&key) && inner.pins.len() >= MAX_PINS {
            return Err(unavailable("worker handover pin capacity reached"));
        }
        inner
            .pins
            .insert(key, std::time::Instant::now() + PIN_LIFETIME);
    }
    Ok(Json(Readiness {
        incarnation: worker.incarnation.clone(),
        manifest_sha256: request.manifest_sha256,
        manifest,
        shards,
    }))
}

async fn evaluate(State(worker): State<Worker>, request: Request) -> Response {
    let admitted = async {
        if request
            .headers()
            .get("x-enhance-worker-incarnation")
            .is_some_and(|header| header != worker.incarnation.as_str())
        {
            return Err(unavailable("worker incarnation changed"));
        }
        let permit = worker
            .evaluation
            .clone()
            .try_acquire_owned()
            .map_err(|_| (StatusCode::TOO_MANY_REQUESTS, "evaluation limit".into()))?;
        let bytes = pir_control::admission::read_body(
            request.into_body(),
            1024 * 1024,
            Duration::from_secs(30),
        )
        .await
        .map_err(unavailable)?;
        let query: Evaluate =
            serde_json::from_slice(&bytes).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        let binding = QueryBinding::decode(
            query
                .binding
                .as_ref()
                .ok_or_else(|| unavailable("evaluation needs full session binding"))?,
        )
        .map_err(unavailable)?;
        if query.binding.as_ref().unwrap().len() != HEADER_BYTES
            || binding.generation != query.generation
            || binding.shard_id != query.shard_id
            || hex::encode(binding.session_id) != query.session_id
            || hex::encode(binding.epoch) != query.epoch
        {
            return Err(unavailable("evaluation binding mismatch"));
        }
        let (manifest, loaded) = {
            let inner = worker.inner.lock().unwrap();
            let view = inner
                .views
                .values()
                .find(|view| {
                    view.manifest.generation == query.generation
                        && view
                            .manifest
                            .session_id(query.shard_id)
                            .is_ok_and(|id| hex::encode(id) == query.session_id)
                })
                .ok_or_else(|| unavailable("requested worker material is unavailable"))?;
            if binding.recovery_epoch != view.manifest.recovery_epoch
                || hex::encode(binding.anchor_hash) != view.manifest.anchor_block_hash
                || !view.manifest.sessions.iter().any(|s| {
                    s.shard_id == query.shard_id && s.public_params_sha256[..16] == query.epoch
                })
            {
                return Err(unavailable("evaluation authority binding mismatch"));
            }
            (
                view.manifest.clone(),
                view.domains
                    .get(&query.shard_id)
                    .cloned()
                    .ok_or_else(|| unavailable("wrong worker shard"))?,
            )
        };
        let params = protocol::parameters(loaded.plan.shard.logical_rows).map_err(unavailable)?;
        if query.coefficients.len() != params.db_rows
            || query
                .coefficients
                .iter()
                .any(|q| *q >= enhance_shared::runtime::modulus())
        {
            return Err((StatusCode::BAD_REQUEST, "invalid query coefficients".into()));
        }
        let lease = worker.renew(&manifest).await.map_err(unavailable)?;
        lease
            .check(&manifest, &worker.safety)
            .map_err(unavailable)?;
        Ok::<_, (StatusCode, String)>((query, loaded, permit, manifest, lease))
    }
    .await;
    let (query, loaded, permit, manifest, lease) = match admitted {
        Ok(value) => value,
        Err(error) => {
            let mut response = error.into_response();
            response
                .headers_mut()
                .insert("x-enhance-evaluation", "not-accepted".parse().unwrap());
            return response;
        }
    };
    let result = tokio::task::spawn_blocking(move || {
        let began = std::time::Instant::now();
        let coefficients = loaded.evaluate(&query.coefficients)?;
        let micros = began.elapsed().as_micros().min(u64::MAX as u128) as u64;
        let bytes = serde_json::to_vec(&Intermediate {
            binding: query.binding,
            generation: query.generation,
            shard_id: query.shard_id,
            epoch: query.epoch,
            coefficients,
        })
        .map_err(|e| e.to_string())?;
        Ok::<_, String>((bytes, loaded, permit, micros))
    })
    .await;
    match result {
        Ok(Ok((bytes, loaded, permit, micros))) => {
            worker.matvec_latency.observe(micros as f64 / 1_000_000.);
            let refreshed = match worker.renew(&manifest).await {
                Ok(fresh) if fresh.continues(&lease) => fresh,
                Ok(_) => {
                    return unavailable("publication withdrawn during evaluation").into_response()
                }
                Err(error) => return unavailable(error).into_response(),
            };
            let lease = refreshed;
            let safety = worker.safety.clone();
            (
                [
                    ("content-type", "application/json".to_string()),
                    ("x-enhance-matvec-microseconds", micros.to_string()),
                ],
                crate::enhance::response_body::guarded(bytes, (loaded, permit), move || {
                    lease.check(&manifest, &safety)
                }),
            )
                .into_response()
        }
        Ok(Err(error)) => {
            tracing::error!(%error, "worker evaluation failed");
            unavailable("matrix-vector evaluation unavailable").into_response()
        }
        Err(error) => unavailable(error).into_response(),
    }
}

#[cfg(test)]
#[path = "worker_sync_tests.rs"]
mod backend_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn abandoned_readiness_promises_expire_and_old_incarnations_cannot_release_them() {
        let root = tempfile::tempdir().unwrap();
        let source = SourceConfig {
            publisher_origin: "https://publisher.example".into(),
            trusted_ca: None,
            download_budget_bytes: 16 * 1024 * 1024,
            cache_limit_bytes: 16 * 1024 * 1024,
        };
        let worker = Worker::open(
            root.path(),
            Config {
                source,
                shards: [0].into(),
            },
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let geometry = protocol::Geometry::default();
        let coverage = protocol::Lifecycle::default()
            .coverage(1, geometry)
            .unwrap();
        let plan = enhance_shared::runtime::plan(
            coverage.shards[0].clone(),
            enhance_shared::test_support::records,
        )
        .unwrap();
        let manifest = Manifest {
            recovery_epoch: 0,
            placement_revision: 1,
            domain_recovery_epochs: [(0, "0".into())].into(),
            schema_version: protocol::SCHEMA_VERSION,
            protocol_revision: protocol::PROTOCOL_REVISION.into(),
            network: "main".into(),
            pool: "ironwood".into(),
            generation: 1,
            anchor_height: 1,
            anchor_block_hash: "01".repeat(32),
            geometry,
            coverage,
            sessions: vec![protocol::SessionRef {
                shard_id: 0,
                public_params_sha256: "02".repeat(32),
                parameter_id: protocol::parameter_id(plan.shard.logical_rows).unwrap(),
            }],
            unit_identities: [(0, plan.units)].into(),
        };
        let id = digest(&manifest);
        let lease = Lease::from_response(
            enhance_shared::publication::AuthorityResponse {
                current: enhance_shared::publication::Current {
                    version: enhance_shared::publication::VERSION,
                    revision: 1,
                    withdrawal_revision: 1,
                    recovery_epoch: 0,
                    revoked: Default::default(),
                    eligible: true,
                    manifest: Some(manifest.clone()),
                    inventory: Some(enhance_shared::publication::Object {
                        sha256: "03".repeat(32),
                        bytes: 1,
                    }),
                },
                authorized_manifest_sha256: id.clone(),
                source: pir_publication::SourceObservation::starting_now(),
            },
            &manifest,
            &worker.safety,
        )
        .unwrap();
        worker.inner.lock().unwrap().views.insert(
            id.clone(),
            View {
                manifest,
                domains: Default::default(),
                lease,
            },
        );
        for index in 0..MAX_PINS {
            // Discard the response as if the holder never received its promise.
            let _ = readiness(
                State(worker.clone()),
                Json(ReadinessRequest {
                    manifest_sha256: id.clone(),
                    holder: Some(format!("{index:032x}")),
                }),
            )
            .await
            .unwrap();
        }
        let holder = "ff".repeat(16);
        assert_eq!(
            readiness(
                State(worker.clone()),
                Json(ReadinessRequest {
                    manifest_sha256: id.clone(),
                    holder: Some(holder.clone())
                })
            )
            .await
            .unwrap_err()
            .0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        let mut release_request = Release {
            manifest_sha256: id.clone(),
            holder: "00".repeat(16),
            worker_incarnation: "ee".repeat(16),
        };
        assert_eq!(
            release(State(worker.clone()), Json(release_request.clone()))
                .await
                .unwrap_err()
                .0,
            StatusCode::CONFLICT
        );
        assert_eq!(worker.inner.lock().unwrap().pins.len(), MAX_PINS);
        // Advance only the expired promise's deadline, without spending the
        // correctness budget waiting thirty real seconds.
        *worker
            .inner
            .lock()
            .unwrap()
            .pins
            .get_mut(&(release_request.holder.clone(), id.clone()))
            .unwrap() = std::time::Instant::now() - Duration::from_millis(1);
        let _ = readiness(
            State(worker.clone()),
            Json(ReadinessRequest {
                manifest_sha256: id.clone(),
                holder: Some(holder.clone()),
            }),
        )
        .await
        .unwrap();
        assert_eq!(worker.inner.lock().unwrap().pins.len(), MAX_PINS);
        release_request.holder = holder.clone();
        release_request.worker_incarnation = worker.incarnation.clone();
        assert_eq!(
            release(State(worker.clone()), Json(release_request))
                .await
                .unwrap(),
            StatusCode::NO_CONTENT
        );
        assert!(!worker
            .inner
            .lock()
            .unwrap()
            .pins
            .contains_key(&(holder, id)));
    }
}
