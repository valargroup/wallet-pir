//! Private worker API. A candidate never evicts a published assignment.
use super::control::{PlacementPolicy, MIB, OVERHEAD, RESIDENT_LIMIT};
use super::runtime::{DomainPlan, Engine, Evaluation};
use crate::matvec::MatvecConfig;
use axum::{
    body::{to_bytes, Body},
    extract::{Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use enhance_pir::protocol::{
    digest, Manifest, ShardState, UnitIdentity, PROTOCOL_REVISION, RETAINED_GENERATIONS,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Candidate {
    pub placement_policy: PlacementPolicy,
    pub operation: String,
    pub attempt: u64,
    pub epoch: u64,
    pub expected_revision: u64,
    pub generation: u64,
    pub plans: Vec<DomainPlan>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Activation {
    pub operation: String,
    pub attempt: u64,
    pub epoch: u64,
    pub manifest: Manifest,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Commit {
    pub epoch: u64,
    pub revision: u64,
    pub generation: u64,
    pub manifest_digest: String,
    pub retained: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Abort {
    pub epoch: u64,
    pub operation: String,
    pub attempt: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Retain {
    pub epoch: u64,
    pub generations: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Evaluate {
    /// Full launched wallet binding; absent only for the legacy coordinator path.
    #[serde(default)]
    pub binding: Option<Vec<u8>>,
    pub session_id: String,
    pub generation: u64,
    pub shard_id: u64,
    pub epoch: String,
    pub coefficients: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Intermediate {
    #[serde(default)]
    pub binding: Option<Vec<u8>>,
    pub generation: u64,
    pub shard_id: u64,
    pub epoch: String,
    pub coefficients: Vec<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Revocation {
    #[serde(with = "enhance_pir::protocol::decimal_u64")]
    pub recovery_epoch: u64,
    pub sessions: BTreeSet<String>,
}

async fn revoke(State(w): State<Worker>, Json(request): Json<Revocation>) -> ApiResult<StatusCode> {
    let mut inner = w.inner.lock().unwrap();
    if request.recovery_epoch < inner.disk.revocation.recovery_epoch
        || !inner.disk.revocation.sessions.is_subset(&request.sessions)
        || request
            .sessions
            .iter()
            .any(|s| !enhance_pir::protocol::canonical_hash(s))
    {
        return Err(unavailable("nonmonotonic revocation"));
    }
    let mut disk = inner.disk.clone();
    for (manifest, plans) in disk.published.values_mut() {
        plans.retain(|p| {
            manifest
                .session_id(p.shard.id)
                .is_ok_and(|id| !request.sessions.contains(&hex::encode(id)))
        });
    }
    disk.revocation = request;
    inner.save(disk).map_err(unavailable)?;
    let keep: BTreeMap<_, BTreeSet<_>> = inner
        .disk
        .published
        .iter()
        .map(|(g, (_, plans))| (*g, plans.iter().map(|p| p.shard.id).collect()))
        .collect();
    for (generation, assignments) in &mut inner.published {
        assignments.retain(|id, _| keep.get(generation).is_some_and(|ids| ids.contains(id)));
    }
    // Evaluation Arcs remain visible to the engine ledger until CPU work ends.
    inner.collect_unused().map_err(unavailable)?;
    Ok(StatusCode::OK)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DiskState {
    revocation: Revocation,
    #[serde(default)]
    placement_policy: PlacementPolicy,
    schema_version: u16,
    protocol_revision: String,
    epoch: u64,
    revision: u64,
    last_attempt: Option<(u64, u64)>,
    retention: Vec<u64>,
    candidate: Option<Candidate>,
    activated: Option<Manifest>,
    published: BTreeMap<u64, (Manifest, Vec<DomainPlan>)>,
}

impl Default for DiskState {
    fn default() -> Self {
        Self {
            revocation: Revocation::default(),
            placement_policy: PlacementPolicy::default(),
            schema_version: enhance_pir::protocol::SCHEMA_VERSION,
            protocol_revision: PROTOCOL_REVISION.into(),
            epoch: 0,
            revision: 0,
            last_attempt: None,
            retention: Vec::new(),
            candidate: None,
            activated: None,
            published: BTreeMap::new(),
        }
    }
}
impl DiskState {
    fn validate_format(&self) -> Result<(), String> {
        self.placement_policy.validate()?;
        if self.schema_version != enhance_pir::protocol::SCHEMA_VERSION
            || self.protocol_revision != PROTOCOL_REVISION
        {
            return Err(
                "incompatible worker state; rebuild protocol v7 in a separate data directory"
                    .into(),
            );
        }
        if self
            .candidate
            .as_ref()
            .is_some_and(|c| c.placement_policy != self.placement_policy)
        {
            return Err("persisted candidate placement policy mismatch".into());
        }
        for manifest in self
            .activated
            .iter()
            .chain(self.published.values().map(|(m, _)| m))
        {
            manifest.validate()?;
        }
        Ok(())
    }
}

struct Inner {
    disk: DiskState,
    engine: Arc<Mutex<Engine>>,
    candidate: BTreeMap<u64, Arc<Evaluation>>,
    published: BTreeMap<u64, BTreeMap<u64, Arc<Evaluation>>>,
    root: PathBuf,
    _lock: File,
}

#[derive(Clone)]
pub struct Worker {
    backend: MatvecConfig,
    inner: Arc<Mutex<Inner>>,
    preparation: Arc<Semaphore>,
    evaluation: Arc<Semaphore>,
    pub incarnation: String,
}

type ApiResult<T> = Result<T, (StatusCode, String)>;
fn unavailable(e: impl ToString) -> (StatusCode, String) {
    (StatusCode::SERVICE_UNAVAILABLE, e.to_string())
}

const MEMORY_REFUSAL: &str = "full growth/transition reservation does not fit";

fn admission_error(error: String) -> (StatusCode, String) {
    if error == MEMORY_REFUSAL {
        (StatusCode::INSUFFICIENT_STORAGE, error)
    } else {
        unavailable(error)
    }
}

struct Budget {
    union_database_bytes: u64,
    growth_reserved_bytes: u64,
    transition_reserved_bytes: u64,
    total_bytes: u64,
}

impl Inner {
    fn collect_unused(&mut self) -> Result<(), String> {
        let mut engine = self.engine.lock().unwrap();
        engine.collect_unused()?;
        let units = engine.live_sizes();
        let plans: BTreeSet<_> = self
            .published
            .values()
            .flat_map(|s| s.values())
            .map(|e| digest(&e.plan))
            .collect();
        for (directory, retained) in [
            ("rows", units.keys().cloned().collect::<BTreeSet<_>>()),
            ("hints", plans),
        ] {
            for entry in fs::read_dir(self.root.join(directory)).map_err(|e| e.to_string())? {
                let entry = entry.map_err(|e| e.to_string())?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.len() == 64
                    && hex::decode(&name).is_ok()
                    && !retained.contains(&name)
                    && entry.file_type().map_err(|e| e.to_string())?.is_file()
                {
                    fs::remove_file(entry.path()).map_err(|e| e.to_string())?;
                }
            }
        }
        Ok(())
    }

    fn save(&mut self, disk: DiskState) -> Result<(), String> {
        crate::artifact::write_atomic(&self.root, "worker.json", |f| {
            serde_json::to_writer(f, &disk).map_err(std::io::Error::other)
        })
        .map_err(|e| e.to_string())?;
        File::open(&self.root)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
        self.disk = disk;
        Ok(())
    }

    fn admission(&self, plans: &[DomainPlan]) -> Result<(), String> {
        let budget = self.budget(plans, &self.engine.lock().unwrap().live_sizes())?;
        if budget.total_bytes > RESIDENT_LIMIT {
            return Err(MEMORY_REFUSAL.into());
        }
        Ok(())
    }

    fn budget(&self, plans: &[DomainPlan], live: &BTreeMap<String, u64>) -> Result<Budget, String> {
        let growing = plans
            .iter()
            .filter(|p| p.shard.state == ShardState::Growing)
            .count();
        let active = plans.iter().any(|p| p.shard.state != ShardState::Sealed);
        if growing > 1 || plans.len() > self.disk.placement_policy.limit(active) {
            return Err("group role limit exceeded".into());
        }
        let mut ids = BTreeSet::new();
        let mut runtimes = BTreeMap::new();
        for plan in plans {
            plan.validate()?;
            if !ids.insert(plan.shard.id) {
                return Err("duplicate shard assignment".into());
            }
            for unit in &plan.units {
                runtimes.insert(unit.digest(), unit.allocated_rows * 24576);
            }
        }
        let current: u64 = runtimes.values().sum();
        for snapshot in self.published.values() {
            for evaluation in snapshot.values() {
                for unit in &evaluation.plan.units {
                    runtimes.insert(unit.digest(), unit.allocated_rows * 24576);
                }
            }
        }
        // Engine weak references also see snapshots pinned by admitted queries after expiry.
        runtimes.extend(live.iter().map(|(id, bytes)| (id.clone(), *bytes)));
        let retained: u64 = runtimes.values().sum();
        let distinct = retained;
        let growth = plans.len() as u64 * 768 * MIB - current;
        let extra = if active {
            (5 * 192 * MIB).saturating_sub(retained - current) + 96 * MIB
        } else {
            0
        };
        Ok(Budget {
            union_database_bytes: distinct,
            growth_reserved_bytes: growth,
            transition_reserved_bytes: extra,
            total_bytes: distinct + growth + extra + OVERHEAD,
        })
    }
}

// Staged rows are durable recovery input, not an authority: bind the entire
// padded file to the committed unit identity before supplying source records.
fn staged_records(
    root: &FsPath,
    plan: &DomainPlan,
    start: u64,
    count: usize,
) -> Result<Vec<u8>, String> {
    if !start.is_multiple_of(enhance_pir::RECORDS_PER_ROW as u64) {
        return Err("unaligned staged source".into());
    }
    let global = start / enhance_pir::RECORDS_PER_ROW as u64;
    let row = if plan.shard.composed() && global + 4096 == plan.shard.global_row_start {
        4096
    } else {
        global
            .checked_sub(plan.shard.global_row_start)
            .ok_or("staged source before shard")?
    };
    let unit = plan
        .units
        .iter()
        .find(|u| u.local_row_start == row)
        .ok_or("unknown unit source")?;
    let bytes = verified_rows(&root.join("rows"), unit)?;
    let end = count
        .checked_mul(enhance_pir::RECORD_BYTES)
        .ok_or("staged range overflow")?;
    bytes
        .get(..end)
        .map(<[u8]>::to_vec)
        .ok_or_else(|| "short staged rows".into())
}

fn verified_rows(directory: &FsPath, unit: &UnitIdentity) -> Result<Vec<u8>, String> {
    let expected = unit
        .allocated_rows
        .checked_mul(crate::types::ENHANCE_LAYOUT.row_bytes() as u64)
        .ok_or("staged size overflow")?;
    let mut bytes = Vec::new();
    let file = File::open(directory.join(unit.digest())).map_err(|e| e.to_string())?;
    (&file)
        .take(expected.checked_add(1).ok_or("staged size overflow")?)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 != expected || hex::encode(Sha256::digest(&bytes)) != unit.content_sha256
    {
        return Err("staged unit length or digest mismatch".into());
    }
    crate::artifact::release_file_cache(&file);
    Ok(bytes)
}

impl Worker {
    /// Restore only journal-referenced row files while the worker is stopped.
    /// Each replacement is atomic and independently resumable; committed metadata
    /// is never copied from the source or changed by repair.
    pub fn repair_rows(root: &FsPath, source_rows: &FsPath) -> Result<usize, String> {
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("worker.lock"))
            .map_err(|e| e.to_string())?;
        lock.try_lock()
            .map_err(|e| format!("stop worker before row repair: {e}"))?;
        let disk: DiskState =
            serde_json::from_slice(&fs::read(root.join("worker.json")).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        disk.validate_format()?;
        let mut units = BTreeMap::new();
        for plan in disk
            .published
            .values()
            .flat_map(|(_, plans)| plans)
            .chain(disk.candidate.iter().flat_map(|c| &c.plans))
        {
            plan.validate()?;
            for unit in &plan.units {
                units.insert(unit.digest(), unit.clone());
            }
        }
        let destination = root.join("rows");
        fs::create_dir_all(&destination).map_err(|e| e.to_string())?;
        let mut restored = 0;
        for (id, unit) in units {
            if verified_rows(&destination, &unit).is_ok() {
                continue;
            }
            let bytes = verified_rows(source_rows, &unit)
                .map_err(|e| format!("cannot repair unit {id}: {e}"))?;
            crate::artifact::write_atomic_cold(&destination, &id, |file| {
                std::io::Write::write_all(file, &bytes)
            })
            .map_err(|e| e.to_string())?;
            File::open(&destination)
                .and_then(|file| file.sync_all())
                .map_err(|e| e.to_string())?;
            restored += 1;
        }
        File::open(root)
            .and_then(|file| file.sync_all())
            .map_err(|e| e.to_string())?;
        Ok(restored)
    }

    pub fn open(root: &FsPath) -> Result<Self, String> {
        Self::open_with_policy(root, PlacementPolicy::default())
    }

    pub fn open_with_policy(root: &FsPath, policy: PlacementPolicy) -> Result<Self, String> {
        Self::open_with_backend(root, policy, MatvecConfig::default())
    }

    pub fn open_with_backend(
        root: &FsPath,
        policy: PlacementPolicy,
        mut backend: MatvecConfig,
    ) -> Result<Self, String> {
        policy.validate()?;
        backend.validate().map_err(|e| e.to_string())?;
        if backend.matvec_backend == crate::matvec::Backend::Cuda {
            backend.cuda_device = Some(backend.cuda_device.unwrap_or(0));
        }
        tracing::info!(?backend, "worker matrix-vector backend selected");
        fs::create_dir_all(root.join("rows")).map_err(|e| e.to_string())?;
        fs::create_dir_all(root.join("hints")).map_err(|e| e.to_string())?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("worker.lock"))
            .map_err(|e| e.to_string())?;
        lock.try_lock()
            .map_err(|e| format!("worker already open: {e}"))?;
        let path = root.join("worker.json");
        let disk: DiskState = if path.exists() {
            serde_json::from_slice(&fs::read(path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?
        } else {
            DiskState {
                placement_policy: policy,
                ..DiskState::default()
            }
        };
        disk.validate_format()?;
        if disk.placement_policy != policy {
            return Err("persisted worker placement policy mismatch; explicit redistribution or fresh state required".into());
        }
        crate::artifact::write_atomic(root, "worker.json", |f| {
            serde_json::to_writer(f, &disk).map_err(std::io::Error::other)
        })
        .map_err(|e| e.to_string())?;
        File::open(root)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
        let mut engine = Engine::with_backend(root, backend);
        let mut published = BTreeMap::new();
        for (generation, (manifest, plans)) in &disk.published {
            let mut assignments = BTreeMap::new();
            for plan in plans {
                if disk
                    .revocation
                    .sessions
                    .contains(&hex::encode(manifest.session_id(plan.shard.id)?))
                {
                    continue;
                }
                assignments.insert(
                    plan.shard.id,
                    engine.prepare(plan.clone(), |start, count| {
                        staged_records(root, plan, start, count)
                    })?,
                );
            }
            published.insert(*generation, assignments);
        }
        let incarnation = format!("{:032x}", rand::random::<u128>());
        Ok(Self {
            backend,
            inner: Arc::new(Mutex::new(Inner {
                disk,
                engine: Arc::new(Mutex::new(engine)),
                candidate: BTreeMap::new(),
                published,
                root: root.into(),
                _lock: lock,
            })),
            preparation: Arc::new(Semaphore::new(1)),
            evaluation: Arc::new(Semaphore::new(2)),
            incarnation,
        })
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/internal/revoke", post(revoke))
            .route("/internal/health", get(health))
            .route("/internal/metrics", get(metrics))
            .route("/internal/admit", post(admit))
            .route("/internal/reserve", post(reserve))
            .route("/internal/rows/:id", put(upload))
            .route("/internal/prepare", post(prepare))
            .route("/internal/hint/:shard", get(hint))
            .route("/internal/activate", post(activate))
            .route("/internal/commit", post(commit))
            .route("/internal/abort", post(abort))
            .route("/internal/retain", post(retain))
            .route("/internal/evaluate", post(evaluate))
            .layer(axum::extract::DefaultBodyLimit::max(1024 * 1024))
            .with_state(self)
    }
}

/// Advisory only: no runtime, reservation, epoch, or durable worker state changes.
/// The reserve command must repeat admission because query pins can change.
async fn admit(
    State(w): State<Worker>,
    Json(plans): Json<Vec<DomainPlan>>,
) -> ApiResult<StatusCode> {
    let inner = w.inner.lock().unwrap();
    if inner.disk.candidate.is_some() {
        return Err(unavailable("candidate already reserved"));
    }
    inner.admission(&plans).map_err(admission_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn metrics(
    State(w): State<Worker>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    let inner = w.inner.lock().unwrap();
    let mut metrics = super::telemetry::Metrics::default();
    metrics.number("enhance_worker_up", 1);
    metrics.number("enhance_worker_epoch", inner.disk.epoch);
    metrics.number("enhance_worker_revision", inner.disk.revision);
    metrics.number(
        "enhance_worker_retained_generations",
        inner.disk.published.len() as u64,
    );
    metrics.number(
        "enhance_worker_candidate_present",
        u64::from(inner.disk.candidate.is_some()),
    );
    metrics.number(
        "enhance_worker_preparation_busy",
        u64::from(w.preparation.available_permits() == 0),
    );
    metrics.number(
        "enhance_worker_queries_in_flight",
        (2 - w.evaluation.available_permits()) as u64,
    );
    // Preparation holds the engine lock. Never stall that work just to scrape,
    // and never substitute zero database usage when the sample is unavailable.
    if let Ok(engine) = inner.engine.try_lock() {
        let live = engine.live_sizes();
        metrics.number("enhance_worker_memory_sample_available", 1);
        metrics.number("enhance_worker_live_database_bytes", live.values().sum());
        let latest = inner
            .disk
            .published
            .last_key_value()
            .map(|(_, (_, plans))| plans.as_slice())
            .unwrap_or_default();
        let ids = |plans: &[DomainPlan]| -> BTreeSet<String> {
            plans
                .iter()
                .flat_map(|p| &p.units)
                .map(|u| u.digest())
                .collect()
        };
        let latest_ids = ids(latest);
        let published_ids: BTreeSet<_> = inner
            .published
            .values()
            .flat_map(|snapshot| snapshot.values())
            .flat_map(|e| e.plan.units.iter().map(|u| u.digest()))
            .collect();
        let candidate_plans = inner.disk.candidate.as_ref().map(|c| c.plans.as_slice());
        let candidate_ids = ids(candidate_plans.unwrap_or_default());
        let selected_bytes = |selected: &BTreeSet<String>| -> u64 {
            live.iter()
                .filter(|(id, _)| selected.contains(*id))
                .map(|(_, bytes)| bytes)
                .sum()
        };
        metrics.number(
            "enhance_worker_published_database_bytes",
            selected_bytes(&published_ids),
        );
        metrics.number(
            "enhance_worker_latest_database_bytes",
            selected_bytes(&latest_ids),
        );
        metrics.number(
            "enhance_worker_candidate_database_bytes",
            selected_bytes(&candidate_ids),
        );
        metrics.number(
            "enhance_worker_query_only_database_bytes",
            live.iter()
                .filter(|(id, _)| !published_ids.contains(*id) && !candidate_ids.contains(*id))
                .map(|(_, bytes)| bytes)
                .sum(),
        );
        metrics.number(
            "enhance_worker_source_reclamation_database_bytes",
            live.iter()
                .filter(|(id, _)| !latest_ids.contains(*id) && !candidate_ids.contains(*id))
                .map(|(_, bytes)| bytes)
                .sum(),
        );
        let budget = inner.budget(candidate_plans.unwrap_or(latest), &live);
        metrics.number(
            "enhance_worker_memory_model_available",
            u64::from(budget.is_ok()),
        );
        if let Ok(budget) = budget {
            metrics.number(
                "enhance_worker_memory_model_uses_candidate",
                u64::from(candidate_plans.is_some()),
            );
            metrics.number(
                "enhance_worker_model_union_database_bytes",
                budget.union_database_bytes,
            );
            metrics.number(
                "enhance_worker_model_growth_reserved_bytes",
                budget.growth_reserved_bytes,
            );
            metrics.number(
                "enhance_worker_model_transition_reserved_bytes",
                budget.transition_reserved_bytes,
            );
            metrics.number("enhance_worker_model_overhead_bytes", OVERHEAD);
            metrics.number("enhance_worker_model_total_bytes", budget.total_bytes);
            metrics.number("enhance_worker_model_limit_bytes", RESIDENT_LIMIT);
            metrics.number(
                "enhance_worker_model_within_limit",
                u64::from(budget.total_bytes <= RESIDENT_LIMIT),
            );
        }
    } else {
        metrics.number("enhance_worker_memory_sample_available", 0);
        metrics.number("enhance_worker_memory_model_available", 0);
    }
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        metrics.finish(),
    )
}

async fn health(State(w): State<Worker>) -> Json<serde_json::Value> {
    let inner = w.inner.lock().unwrap();
    Json(
        serde_json::json!({"matvec": w.backend, "protocol":PROTOCOL_REVISION,"placement_policy":inner.disk.placement_policy,"incarnation":w.incarnation,"epoch":inner.disk.epoch,"revision":inner.disk.revision,
        "resident_database_bytes":inner.engine.try_lock().ok().map(|e| e.live_bytes()),"published":inner.disk.published.keys().collect::<Vec<_>>(),
        "published_manifest_digests":inner.disk.published.iter().map(|(g,(m,_))| (g.to_string(), digest(m))).collect::<BTreeMap<_,_>>(),
        "candidate":inner.disk.candidate}),
    )
}

async fn abort(State(w): State<Worker>, Json(request): Json<Abort>) -> ApiResult<StatusCode> {
    let _permit = w
        .preparation
        .clone()
        .try_acquire_owned()
        .map_err(unavailable)?;
    let mut i = w.inner.lock().unwrap();
    if request.epoch < i.disk.epoch {
        return Err(unavailable("stale abort epoch"));
    }
    if let Some(c) = &i.disk.candidate {
        if c.operation != request.operation || c.attempt != request.attempt {
            return Err(unavailable("conflicting abort"));
        }
    }
    let mut disk = i.disk.clone();
    disk.epoch = request.epoch;
    // Fence delayed reservations even if this worker never saw the attempt.
    disk.last_attempt = Some(
        disk.last_attempt
            .unwrap_or((0, 0))
            .max((request.epoch, request.attempt)),
    );
    disk.candidate = None;
    disk.activated = None;
    i.save(disk).map_err(unavailable)?;
    i.candidate.clear();
    Ok(StatusCode::OK)
}

async fn retain(State(w): State<Worker>, Json(request): Json<Retain>) -> ApiResult<StatusCode> {
    let _permit = w
        .preparation
        .clone()
        .try_acquire_owned()
        .map_err(unavailable)?;
    let mut i = w.inner.lock().unwrap();
    let generations = &request.generations;
    if request.epoch < i.disk.epoch
        || generations.is_empty()
        || generations.len() > RETAINED_GENERATIONS
        || generations.windows(2).any(|pair| pair[0] <= pair[1])
        || (!i.disk.retention.is_empty()
            && (generations[0] < i.disk.retention[0]
                || (generations[0] == i.disk.retention[0] && generations != &i.disk.retention)))
    {
        return Err(unavailable("stale or conflicting retention command"));
    }
    let mut disk = i.disk.clone();
    disk.epoch = request.epoch;
    disk.retention = generations.clone();
    disk.published.retain(|g, _| generations.contains(g));
    i.save(disk).map_err(unavailable)?;
    i.published.retain(|g, _| generations.contains(g));
    if i.disk.candidate.is_none() {
        i.collect_unused().map_err(unavailable)?;
    }
    Ok(StatusCode::OK)
}

async fn reserve(
    State(w): State<Worker>,
    Json(candidate): Json<Candidate>,
) -> ApiResult<Json<Vec<String>>> {
    let _permit = w
        .preparation
        .clone()
        .try_acquire_owned()
        .map_err(unavailable)?;
    let mut i = w.inner.lock().unwrap();
    if candidate.placement_policy != i.disk.placement_policy {
        return Err(unavailable("worker placement policy mismatch"));
    }
    if candidate.epoch < i.disk.epoch || candidate.expected_revision != i.disk.revision {
        return Err(unavailable("stale epoch or placement revision"));
    }
    if let Some(existing) = &i.disk.candidate {
        if existing != &candidate {
            return Err(unavailable(
                "candidate already reserved; explicit recovery required",
            ));
        }
    } else if i
        .disk
        .last_attempt
        .is_some_and(|last| (candidate.epoch, candidate.attempt) <= last)
    {
        return Err(unavailable("retired operation attempt"));
    }
    if candidate.generation == 0
        || i.disk
            .published
            .keys()
            .next_back()
            .is_some_and(|g| *g >= candidate.generation)
    {
        return Err(unavailable("nonmonotonic candidate generation"));
    }
    i.admission(&candidate.plans).map_err(admission_error)?;
    let mut disk = i.disk.clone();
    disk.epoch = candidate.epoch;
    disk.last_attempt = Some((candidate.epoch, candidate.attempt));
    let missing = candidate
        .plans
        .iter()
        .flat_map(|p| &p.units)
        .map(|u| u.digest())
        .filter(|id| !i.root.join("rows").join(id).exists())
        .collect();
    disk.candidate = Some(candidate);
    i.save(disk).map_err(unavailable)?;
    Ok(Json(missing))
}

async fn upload(
    State(w): State<Worker>,
    Path(id): Path<String>,
    request: Request,
) -> ApiResult<StatusCode> {
    let permit = w
        .preparation
        .clone()
        .try_acquire_owned()
        .map_err(unavailable)?;
    let (root, size, hash) = {
        let i = w.inner.lock().unwrap();
        let candidate = i
            .disk
            .candidate
            .as_ref()
            .ok_or_else(|| unavailable("no reservation"))?;
        let matches_header = |name: &str, expected: String| {
            request.headers().get(name).and_then(|v| v.to_str().ok()) == Some(expected.as_str())
        };
        if !matches_header("x-enhance-epoch", candidate.epoch.to_string())
            || !matches_header("x-enhance-attempt", candidate.attempt.to_string())
            || !matches_header("x-enhance-operation", candidate.operation.clone())
        {
            return Err(unavailable("stale unit upload"));
        }
        let unit = candidate
            .plans
            .iter()
            .flat_map(|p| &p.units)
            .find(|u| u.digest() == id)
            .ok_or_else(|| unavailable("unreserved unit"))?;
        (
            i.root.clone(),
            unit.allocated_rows as usize * crate::types::ENHANCE_LAYOUT.row_bytes(),
            unit.content_sha256.clone(),
        )
    };
    let bytes = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        to_bytes(request.into_body(), size),
    )
    .await
    .map_err(unavailable)?
    .map_err(unavailable)?;
    if bytes.len() != size {
        return Err((StatusCode::BAD_REQUEST, "wrong unit byte length".into()));
    }
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        if hex::encode(Sha256::digest(&bytes)) != hash {
            return Err("unit content digest mismatch".into());
        }
        crate::artifact::write_atomic_cold(&root.join("rows"), &id, |f| {
            std::io::Write::write_all(f, &bytes)
        })
        .map_err(|e| e.to_string())?;
        Ok::<_, String>(())
    })
    .await
    .map_err(unavailable)?
    .map_err(unavailable)?;
    Ok(StatusCode::OK)
}

async fn prepare(
    State(w): State<Worker>,
    Json(candidate): Json<Candidate>,
) -> ApiResult<StatusCode> {
    let permit = w
        .preparation
        .clone()
        .try_acquire_owned()
        .map_err(unavailable)?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let (engine, root) = {
            let i = w.inner.lock().unwrap();
            if i.disk.candidate.as_ref() != Some(&candidate) {
                return Err("conflicting prepare command".into());
            }
            (i.engine.clone(), i.root.clone())
        };
        let mut prepared = BTreeMap::new();
        let mut engine = engine.lock().unwrap();
        for plan in &candidate.plans {
            let eval = engine.prepare(plan.clone(), |start, count| {
                staged_records(&root, plan, start, count)
            })?;
            prepared.insert(plan.shard.id, eval);
        }
        drop(engine);
        w.inner.lock().unwrap().candidate = prepared;
        Ok::<_, String>(())
    })
    .await
    .map_err(unavailable)?
    .map_err(unavailable)?;
    Ok(StatusCode::OK)
}

async fn hint(State(w): State<Worker>, Path(shard): Path<u64>) -> ApiResult<Response> {
    let permit = w
        .preparation
        .clone()
        .try_acquire_owned()
        .map_err(unavailable)?;
    let (eval, root) = {
        let i = w.inner.lock().unwrap();
        (
            i.candidate
                .get(&shard)
                .cloned()
                .ok_or_else(|| unavailable("shard not prepared"))?,
            i.root.join("hints"),
        )
    };
    let artifact = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let blocks = eval.hint()?;
        let name = digest(&eval.plan);
        let hash = crate::artifact::write_atomic_cold(&root, &name, |f| {
            crate::wire::write_crs_blocks(f, &blocks)
        })
        .map_err(|e| e.to_string())?;
        let length = crate::wire::crs_encoded_len(blocks.len(), super::runtime::rlwe().d)
            .map_err(|e| e.to_string())?;
        crate::artifact::PublicationArtifact::open(&root.join(name), length, hash)
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(unavailable)?
    .map_err(unavailable)?;
    Ok(Body::from_stream(artifact.stream()).into_response())
}

async fn activate(
    State(w): State<Worker>,
    Json(request): Json<Activation>,
) -> ApiResult<Json<serde_json::Value>> {
    let _permit = w
        .preparation
        .clone()
        .try_acquire_owned()
        .map_err(unavailable)?;
    request.manifest.validate().map_err(unavailable)?;
    let mut i = w.inner.lock().unwrap();
    let c = i
        .disk
        .candidate
        .as_ref()
        .ok_or_else(|| unavailable("no candidate"))?;
    if c.epoch != request.epoch
        || c.operation != request.operation
        || c.attempt != request.attempt
        || c.generation != request.manifest.generation
        || i.candidate.len() != c.plans.len()
        || c.plans.iter().any(|p| {
            !request.manifest.coverage.shards.contains(&p.shard)
                || request.manifest.unit_identities.get(&p.shard.id) != Some(&p.units)
        })
    {
        return Err(unavailable("candidate is incomplete or session differs"));
    }
    if i.disk
        .activated
        .as_ref()
        .is_some_and(|m| m != &request.manifest)
    {
        return Err(unavailable("conflicting activation"));
    }
    let hash = digest(&request.manifest);
    let mut disk = i.disk.clone();
    disk.activated = Some(request.manifest);
    i.save(disk).map_err(unavailable)?;
    Ok(Json(
        serde_json::json!({"incarnation":w.incarnation,"candidate_digest":hash}),
    ))
}

async fn commit(State(w): State<Worker>, Json(request): Json<Commit>) -> ApiResult<StatusCode> {
    let _permit = w
        .preparation
        .clone()
        .try_acquire_owned()
        .map_err(unavailable)?;
    let mut i = w.inner.lock().unwrap();
    if request.epoch < i.disk.epoch || request.revision < i.disk.revision {
        return Err(unavailable("stale commit"));
    }
    if let Some((m, _)) = i.disk.published.get(&request.generation) {
        return if digest(m) == request.manifest_digest && request.revision == i.disk.revision {
            Ok(StatusCode::OK)
        } else {
            Err(unavailable("conflicting commit"))
        };
    }
    let manifest = i
        .disk
        .activated
        .clone()
        .ok_or_else(|| unavailable("candidate not activated"))?;
    if manifest.generation != request.generation
        || digest(&manifest) != request.manifest_digest
        || request.retained.len() > RETAINED_GENERATIONS
        || request.retained.first() != Some(&request.generation)
        || request.retained.windows(2).any(|pair| pair[0] <= pair[1])
        || i.disk
            .retention
            .first()
            .is_some_and(|latest| *latest > request.generation)
        || request.revision != i.disk.revision + 1
    {
        return Err(unavailable("invalid commit decision"));
    }
    let c = i
        .disk
        .candidate
        .clone()
        .ok_or_else(|| unavailable("candidate missing"))?;
    if i.candidate.len() != c.plans.len() {
        return Err(unavailable("reprepare candidate after restart"));
    }
    let mut disk = i.disk.clone();
    disk.epoch = request.epoch;
    disk.revision = request.revision;
    disk.published
        .insert(request.generation, (manifest, c.plans));
    disk.published.retain(|g, _| request.retained.contains(g));
    disk.retention = request.retained.clone();
    disk.candidate = None;
    disk.activated = None;
    i.save(disk).map_err(unavailable)?;
    let mut prepared = std::mem::take(&mut i.candidate);
    let revoked = i.disk.revocation.sessions.clone();
    let committed = &i.disk.published[&request.generation].0;
    prepared.retain(|id, _| {
        committed
            .session_id(*id)
            .is_ok_and(|s| !revoked.contains(&hex::encode(s)))
    });
    i.published.insert(request.generation, prepared);
    i.published.retain(|g, _| request.retained.contains(g));
    i.collect_unused().map_err(unavailable)?;
    Ok(StatusCode::OK)
}

async fn evaluate(State(w): State<Worker>, request: Request) -> Response {
    let admitted: ApiResult<_> = async {
        // Admission precedes body buffering. The permit moves into the blocking evaluation.
        let permit = w
            .evaluation
            .clone()
            .try_acquire_owned()
            .map_err(|_| (StatusCode::TOO_MANY_REQUESTS, "evaluation limit".into()))?;
        let bytes = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            to_bytes(request.into_body(), 1024 * 1024),
        )
        .await
        .map_err(unavailable)?
        .map_err(unavailable)?;
        let query: Evaluate =
            serde_json::from_slice(&bytes).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        let evaluation = {
            let i = w.inner.lock().unwrap();
            if i.disk.revocation.sessions.contains(&query.session_id) {
                return Err((StatusCode::GONE, "noncanonical_session".into()));
            }
            let (manifest, assignment) =
                if let Some((manifest, _)) = i.disk.published.get(&query.generation) {
                    (manifest, &i.published[&query.generation])
                } else if let Some(manifest) = i
                    .disk
                    .activated
                    .as_ref()
                    .filter(|m| m.generation == query.generation)
                {
                    // Activation promises answerability before the coordinator's durable commit.
                    // The public router cannot expose this session before its commit decision.
                    (manifest, &i.candidate)
                } else {
                    return Err((StatusCode::GONE, "expired session".into()));
                };
            let session = manifest
                .sessions
                .iter()
                .find(|s| s.shard_id == query.shard_id)
                .ok_or((StatusCode::BAD_REQUEST, "wrong shard".into()))?;
            if hex::encode(manifest.session_id(query.shard_id).map_err(unavailable)?)
                != query.session_id
            {
                return Err((StatusCode::GONE, "session_unavailable".into()));
            }
            if let Some(bytes) = &query.binding {
                use enhance_pir::protocol::{QueryBinding, HEADER_BYTES};
                let binding =
                    QueryBinding::decode(bytes).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
                if bytes.len() != HEADER_BYTES
                    || binding.generation != query.generation
                    || binding.shard_id != query.shard_id
                    || hex::encode(binding.epoch) != query.epoch
                    || hex::encode(binding.session_id) != query.session_id
                    || binding.recovery_epoch != manifest.recovery_epoch
                    || hex::encode(binding.anchor_hash) != manifest.anchor_block_hash
                {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "evaluation binding mismatch".into(),
                    ));
                }
            }
            if session.public_params_sha256[..16] != query.epoch {
                return Err((StatusCode::BAD_REQUEST, "epoch mismatch".into()));
            }
            assignment
                .get(&query.shard_id)
                .cloned()
                .ok_or((StatusCode::BAD_REQUEST, "wrong group".into()))?
        };
        Ok((permit, query, evaluation))
    }
    .await;
    let (permit, query, evaluation) = match admitted {
        Ok(value) => value,
        Err(error) => {
            let mut response = error.into_response();
            response
                .headers_mut()
                .insert("x-enhance-evaluation", "not-accepted".parse().unwrap());
            return response;
        }
    };
    // Cancellation detaches CPU work. Its permit stays in the task, then moves
    // into the output body instead of being released at kernel completion.
    let result = tokio::task::spawn_blocking(move || {
        let coefficients = evaluation
            .evaluate(&query.coefficients)
            .map_err(evaluation_error)?;
        let bytes = serde_json::to_vec(&Intermediate {
            binding: query.binding,
            generation: query.generation,
            shard_id: query.shard_id,
            epoch: query.epoch,
            coefficients,
        })
        .map_err(unavailable)?;
        Ok::<_, (StatusCode, String)>((bytes, permit, evaluation))
    })
    .await;
    match result {
        Ok(Ok((bytes, permit, evaluation))) => (
            [("content-type", "application/json")],
            crate::response_body::guarded(bytes, (permit, evaluation), || Ok(())),
        )
            .into_response(),
        Ok(Err(error)) => error.into_response(),
        Err(error) => unavailable(error).into_response(),
    }
}

fn evaluation_error(error: super::runtime::EvaluationError) -> (StatusCode, String) {
    match error {
        super::runtime::EvaluationError::InvalidQuery => {
            (StatusCode::BAD_REQUEST, error.to_string())
        }
        super::runtime::EvaluationError::Unavailable(error) => {
            tracing::error!(%error, "worker evaluation failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "matrix-vector evaluation unavailable".into(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    #[test]
    fn device_errors_are_unavailable_without_exposing_details() {
        use super::super::runtime::EvaluationError;
        assert_eq!(
            evaluation_error(EvaluationError::InvalidQuery).0,
            StatusCode::BAD_REQUEST
        );
        let (status, body) =
            evaluation_error(EvaluationError::Unavailable("private device detail".into()));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(!body.contains("private device detail"));
    }

    #[tokio::test]
    async fn backend_evaluation_failure_over_http_releases_permit_and_recovers() {
        evaluation_failure_over_http(MatvecConfig::default()).await;
    }

    #[cfg(feature = "cuda")]
    #[tokio::test]
    #[ignore = "requires NVIDIA GPU and NVRTC"]
    async fn cuda_backend_evaluation_failure_over_http_releases_permit_and_recovers() {
        evaluation_failure_over_http(MatvecConfig {
            matvec_backend: crate::matvec::Backend::Cuda,
            cuda_device: None,
        })
        .await;
    }

    async fn evaluation_failure_over_http(backend: MatvecConfig) {
        use crate::{
            matvec::testing::{scoped, Faults},
            runtime::{plan, rlwe, Packing},
        };
        use enhance_pir::protocol::{Geometry, Lifecycle, SCHEMA_VERSION};
        use std::sync::atomic::Ordering::SeqCst;
        let root = tempfile::tempdir().unwrap();
        let mut worker =
            Worker::open_with_backend(root.path(), Default::default(), backend).unwrap();
        // With a single permit, the next successful request also proves error cleanup.
        worker.evaluation = Arc::new(Semaphore::new(1));
        let coverage = Lifecycle::default()
            .coverage(67, Geometry::default())
            .unwrap();
        let source = |_: u64, count: usize| Ok(vec![7; count * enhance_pir::RECORD_BYTES]);
        let plan = plan(coverage.shards[0].clone(), source).unwrap();
        let faults = Arc::new(Faults::default());
        let eval = scoped(&faults, || {
            worker
                .inner
                .lock()
                .unwrap()
                .engine
                .lock()
                .unwrap()
                .prepare(plan.clone(), source)
        })
        .unwrap();
        let packing = Packing::new(4096, &eval.hint().unwrap()).unwrap();
        let manifest = Manifest {
            recovery_epoch: 0,
            placement_revision: 0,
            domain_recovery_epochs: [(0, "0".into())].into(),
            schema_version: SCHEMA_VERSION,
            protocol_revision: PROTOCOL_REVISION.into(),
            network: "main".into(),
            pool: "ironwood".into(),
            generation: 1,
            anchor_height: 3428143,
            anchor_block_hash: "01".repeat(32),
            geometry: Geometry::default(),
            coverage,
            sessions: vec![packing.reference(0).unwrap()],
            unit_identities: [(0, plan.units.clone())].into(),
        };
        let query = Evaluate {
            binding: None,
            session_id: hex::encode(manifest.session_id(0).unwrap()),
            generation: 1,
            shard_id: 0,
            epoch: manifest.sessions[0].public_params_sha256[..16].into(),
            coefficients: vec![1; 4096],
        };
        let expected = eval.evaluate(&query.coefficients).unwrap();
        {
            let mut inner = worker.inner.lock().unwrap();
            inner
                .save(DiskState {
                    published: [(1, (manifest, vec![plan]))].into(),
                    ..DiskState::default()
                })
                .unwrap();
            inner.published.insert(1, [(0, eval)].into());
        }
        let durable = fs::read(root.path().join("worker.json")).unwrap();
        let router = worker.clone().router();
        async fn request(router: &Router, query: &Evaluate) -> (StatusCode, Vec<u8>) {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/internal/evaluate")
                        .header("content-type", "application/json")
                        .body(Body::from(serde_json::to_vec(query).unwrap()))
                        .unwrap(),
                )
                .await
                .unwrap();
            (
                response.status(),
                to_bytes(response.into_body(), 1024 * 1024)
                    .await
                    .unwrap()
                    .to_vec(),
            )
        }
        faults.fail_evaluation.store(true, SeqCst);
        let calls = faults.evaluation_calls.load(SeqCst);
        let (status, body) = request(&router, &query).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, b"matrix-vector evaluation unavailable");
        assert_eq!(faults.evaluation_calls.load(SeqCst), calls + 1);
        assert_eq!(worker.evaluation.available_permits(), 1);
        // Validation must reject malformed queries before calling even a failed backend.
        for coefficients in [vec![1], vec![rlwe().q; 4096]] {
            let bad = Evaluate {
                coefficients,
                ..query.clone()
            };
            assert_eq!(request(&router, &bad).await.0, StatusCode::BAD_REQUEST);
        }
        assert_eq!(faults.evaluation_calls.load(SeqCst), calls + 1);
        faults.fail_evaluation.store(false, SeqCst);
        let (status, body) = request(&router, &query).await;
        assert_eq!(status, StatusCode::OK);
        let result: Intermediate = serde_json::from_slice(&body).unwrap();
        assert_eq!(result.coefficients, expected);
        assert_eq!(result.generation, query.generation);
        assert_eq!(result.epoch, query.epoch);
        assert_eq!(faults.evaluation_calls.load(SeqCst), calls + 2);
        assert_eq!(worker.evaluation.available_permits(), 1);
        assert_eq!(fs::read(root.path().join("worker.json")).unwrap(), durable);
    }

    #[test]
    fn q46_worker_state_is_rejected_without_rewriting_it() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("worker.json");
        let mut old = serde_json::to_value(DiskState::default()).unwrap();
        old.as_object_mut().unwrap().remove("protocol_revision");
        for revision in [None, Some("ironwood-enhance-pir-v5")] {
            if let Some(revision) = revision {
                old["protocol_revision"] = revision.into();
            }
            let bytes = serde_json::to_vec(&old).unwrap();
            fs::write(&path, &bytes).unwrap();
            assert!(Worker::open(root.path()).is_err());
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }

    #[tokio::test]
    async fn placement_policy_is_persisted_and_fences_reservations() {
        let root = tempfile::tempdir().unwrap();
        let policy = PlacementPolicy { sealed_shards: 7 };
        let worker = Worker::open_with_policy(root.path(), policy).unwrap();
        let router = worker.router();
        let mut candidate = Candidate {
            placement_policy: PlacementPolicy::default(),
            operation: "policy".into(),
            attempt: 1,
            epoch: 1,
            expected_revision: 0,
            generation: 1,
            plans: vec![],
        };
        assert_eq!(
            command(&router, "/internal/reserve", &candidate).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        candidate.placement_policy = policy;
        assert_eq!(
            command(&router, "/internal/reserve", &candidate).await,
            StatusCode::OK
        );
        drop(router);
        assert!(Worker::open(root.path())
            .err()
            .unwrap()
            .contains("placement policy"));
        assert!(Worker::open_with_policy(root.path(), policy).is_ok());
        assert!(
            Worker::open_with_policy(root.path(), PlacementPolicy { sealed_shards: 8 }).is_err()
        );
    }

    #[test]
    fn restart_rebuilds_lost_cache_only_from_verified_durable_rows() {
        use super::super::runtime::{plan, unit_rows, Packing};
        use enhance_pir::protocol::{Geometry, Lifecycle, SCHEMA_VERSION};
        let root = tempfile::tempdir().unwrap();
        let worker = Worker::open(root.path()).unwrap();
        let coverage = Lifecycle::default()
            .coverage(1, Geometry::default())
            .unwrap();
        let source = |_: u64, count: usize| Ok(vec![7; count * enhance_pir::RECORD_BYTES]);
        let plan = plan(coverage.shards[0].clone(), source).unwrap();
        let rows = unit_rows(&plan.shard, &plan.shard.units[0], &mut { source }).unwrap();
        let path = root.path().join("rows").join(plan.units[0].digest());
        fs::write(&path, &rows).unwrap();
        let eval = worker
            .inner
            .lock()
            .unwrap()
            .engine
            .lock()
            .unwrap()
            .prepare(plan.clone(), source)
            .unwrap();
        let packing = Packing::new(4096, &eval.hint().unwrap()).unwrap();
        let manifest = Manifest {
            recovery_epoch: 0,
            placement_revision: 0,
            domain_recovery_epochs: [(0, "0".into())].into(),
            schema_version: SCHEMA_VERSION,
            protocol_revision: PROTOCOL_REVISION.into(),
            network: "main".into(),
            pool: "ironwood".into(),
            generation: 1,
            anchor_height: 3428143,
            anchor_block_hash: "01".repeat(32),
            geometry: Geometry::default(),
            coverage,
            sessions: vec![packing.reference(0).unwrap()],
            unit_identities: [(0, plan.units.clone())].into(),
        };
        worker
            .inner
            .lock()
            .unwrap()
            .save(DiskState {
                epoch: 3,
                revision: 1,
                retention: vec![1],
                published: [(1, (manifest, vec![plan.clone()]))].into(),
                ..DiskState::default()
            })
            .unwrap();
        let durable = fs::read(root.path().join("worker.json")).unwrap();
        let mut coefficients = vec![0; 4096];
        coefficients[0] = 1;
        let expected = eval.evaluate(&coefficients).unwrap();
        let peer = tempfile::tempdir().unwrap();
        let peer_path = peer.path().join(plan.units[0].digest());
        fs::write(&peer_path, &rows).unwrap();
        assert!(Worker::repair_rows(root.path(), peer.path()).is_err());
        drop(eval);
        drop(worker);
        fs::remove_dir_all(root.path().join("artifacts-9")).unwrap();
        // Reject corrupt padding too, even though the live prefix is intact.
        let mut corrupt = rows.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        fs::write(&path, corrupt).unwrap();
        assert!(Worker::open(root.path()).is_err());
        fs::remove_file(&path).unwrap();
        assert!(Worker::open(root.path()).is_err());
        fs::write(&peer_path, b"corrupt source").unwrap();
        assert!(Worker::repair_rows(root.path(), peer.path()).is_err());
        assert!(!path.exists());
        assert_eq!(fs::read(root.path().join("worker.json")).unwrap(), durable);
        fs::write(&peer_path, &rows).unwrap();
        fs::write(peer.path().join("unreferenced"), b"do not copy").unwrap();
        assert_eq!(Worker::repair_rows(root.path(), peer.path()).unwrap(), 1);
        assert_eq!(Worker::repair_rows(root.path(), peer.path()).unwrap(), 0);
        assert!(!root.path().join("rows/unreferenced").exists());
        let recovered = Worker::open(root.path()).unwrap();
        let inner = recovered.inner.lock().unwrap();
        assert_eq!(
            inner.published[&1][&0].evaluate(&coefficients).unwrap(),
            expected
        );
        assert_eq!(inner.disk.epoch, 3);
        assert_eq!(fs::read(root.path().join("worker.json")).unwrap(), durable);
        assert!(staged_records(root.path(), &plan, 1, 1).is_err());
        assert!(staged_records(root.path(), &plan, 0, usize::MAX).is_err());
    }

    #[test]
    fn offline_repair_includes_durable_candidate_and_rejects_missing_journal() {
        let root = tempfile::tempdir().unwrap();
        let source = tempfile::tempdir().unwrap();
        assert!(Worker::repair_rows(root.path(), source.path()).is_err());
        let worker = Worker::open(root.path()).unwrap();
        let coverage = enhance_pir::protocol::Lifecycle::default()
            .coverage(1, enhance_pir::protocol::Geometry::default())
            .unwrap();
        let plan = super::super::runtime::plan(coverage.shards[0].clone(), |_, count| {
            Ok(vec![0; count * enhance_pir::RECORD_BYTES])
        })
        .unwrap();
        let unit = &plan.units[0];
        fs::write(
            source.path().join(unit.digest()),
            vec![0; unit.allocated_rows as usize * crate::types::ENHANCE_LAYOUT.row_bytes()],
        )
        .unwrap();
        worker
            .inner
            .lock()
            .unwrap()
            .save(DiskState {
                candidate: Some(Candidate {
                    placement_policy: Default::default(),
                    operation: "pending".into(),
                    attempt: 2,
                    epoch: 3,
                    expected_revision: 0,
                    generation: 1,
                    plans: vec![plan],
                }),
                ..DiskState::default()
            })
            .unwrap();
        let journal = fs::read(root.path().join("worker.json")).unwrap();
        drop(worker);
        assert_eq!(Worker::repair_rows(root.path(), source.path()).unwrap(), 1);
        assert_eq!(fs::read(root.path().join("worker.json")).unwrap(), journal);
    }

    async fn command(router: &Router, path: &str, value: &impl Serialize) -> StatusCode {
        router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(value).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn metrics_do_not_block_preparation_or_invent_zero_memory() {
        let root = tempfile::tempdir().unwrap();
        let worker = Worker::open(root.path()).unwrap();
        let before = fs::read(root.path().join("worker.json")).ok();
        let engine = worker.inner.lock().unwrap().engine.clone();
        let (ready_send, ready_receive) = std::sync::mpsc::channel();
        let (finish_send, finish_receive) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let _guard = engine.lock().unwrap();
            ready_send.send(()).unwrap();
            finish_receive
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
        });
        ready_receive
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let (_, text) = metrics(State(worker.clone())).await;
        finish_send.send(()).unwrap();
        thread.join().unwrap();
        assert!(text.contains("enhance_worker_memory_sample_available 0\n"));
        assert!(!text.contains("enhance_worker_live_database_bytes "));
        let (_, text) = metrics(State(worker)).await;
        assert!(text.contains("enhance_worker_memory_sample_available 1\n"));
        assert!(text.contains("enhance_worker_live_database_bytes 0\n"));
        assert!(text.contains(&format!("enhance_worker_model_total_bytes {OVERHEAD}\n")));
        assert_eq!(fs::read(root.path().join("worker.json")).ok(), before);
    }

    #[tokio::test]
    async fn abort_before_reserve_fences_delayed_attempt() {
        let root = tempfile::tempdir().unwrap();
        let router = Worker::open(root.path()).unwrap().router();
        let abort = Abort {
            epoch: 1,
            operation: "cancelled".into(),
            attempt: 0,
        };
        assert_eq!(
            command(&router, "/internal/abort", &abort).await,
            StatusCode::OK
        );
        let mut candidate = Candidate {
            placement_policy: Default::default(),
            epoch: 1,
            operation: "cancelled".into(),
            attempt: 0,
            expected_revision: 0,
            generation: 1,
            plans: vec![],
        };
        assert_eq!(
            command(&router, "/internal/reserve", &candidate).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        candidate.attempt = 1;
        candidate.operation = "replacement".into();
        assert_eq!(
            command(&router, "/internal/reserve", &candidate).await,
            StatusCode::OK
        );
        assert_eq!(
            command(&router, "/internal/abort", &abort).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn aborted_attempts_conflicting_payloads_and_stale_epochs_are_fenced_over_http() {
        let root = tempfile::tempdir().unwrap();
        let router = Worker::open(root.path()).unwrap().router();
        let coverage = enhance_pir::protocol::Lifecycle::default()
            .coverage(1, enhance_pir::protocol::Geometry::default())
            .unwrap();
        let plan = super::super::runtime::plan(coverage.shards[0].clone(), |_, count| {
            Ok(vec![0; count * enhance_pir::RECORD_BYTES])
        })
        .unwrap();
        let mut candidate = Candidate {
            placement_policy: Default::default(),
            operation: "test".into(),
            attempt: 0,
            epoch: 1,
            expected_revision: 0,
            generation: 1,
            plans: vec![plan],
        };
        assert_eq!(
            command(&router, "/internal/reserve", &candidate).await,
            StatusCode::OK
        );
        assert_eq!(
            command(&router, "/internal/reserve", &candidate).await,
            StatusCode::OK
        );
        assert_eq!(
            command(
                &router,
                "/internal/abort",
                &Abort {
                    epoch: 0,
                    operation: "test".into(),
                    attempt: 0
                }
            )
            .await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/internal/rows/{}",
                        candidate.plans[0].units[0].digest()
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "uploads need the reservation's command identity"
        );
        assert_eq!(
            command(
                &router,
                "/internal/abort",
                &Abort {
                    epoch: 1,
                    operation: "test".into(),
                    attempt: 0
                }
            )
            .await,
            StatusCode::OK
        );
        assert_eq!(
            command(&router, "/internal/reserve", &candidate).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        candidate.attempt = 1;
        assert_eq!(
            command(&router, "/internal/reserve", &candidate).await,
            StatusCode::OK
        );
        candidate.plans[0].units[0].content_sha256 = "ff".repeat(32);
        assert_eq!(
            command(&router, "/internal/reserve", &candidate).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            command(
                &router,
                "/internal/retain",
                &Retain {
                    epoch: 0,
                    generations: vec![1]
                }
            )
            .await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            command(
                &router,
                "/internal/retain",
                &Retain {
                    epoch: 1,
                    generations: vec![1, 1]
                }
            )
            .await,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
