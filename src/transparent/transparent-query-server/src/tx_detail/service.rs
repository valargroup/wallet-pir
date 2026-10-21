//! Serving one display publication snapshot over HTTP.
//!
//! Queries go through the request path the history worker uses
//! ([`crate::request`]). What differs is the address: a revision is resolved
//! with its tier and this worker's role, a table is one bucket's entries, and
//! the binding names the bucket. Everything that bounds work is shared: the
//! runtime cache, admission, work-memory reservations and their status codes.

use zakura_pir_protocol::transparent::shard as transparent_shard;
use zakura_pir_query_server::transparent::native as transparent_native;

use crate::admission::{Admission, AdmissionConfig, Admitted};
use crate::config::ServiceConfig;
pub(crate) use crate::request::json;
use crate::request::{answer_query, within, Query, RequestError, Worker};
use axum::extract::{Path as AxumPath, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use transparent_shard::display::{self, DisplayTable};
use transparent_shard::layout::Geometry;
use transparent_shared::history::shardset::{SegmentSource, Table};
use transparent_shared::runtime::metrics::{Metrics, Snapshot};
use transparent_shared::runtime::{
    disk::{DiskCache, ShippedRuntimes},
    CacheError, Produced, RuntimeCache, RuntimeHandle, SharedParams,
};
use transparent_shared::tx_detail::set::{DisplayRevision, DisplaySet};
use transparent_shared::tx_detail::{kind, runtime_key, tier};

type ParamsKey = (&'static str, Table);

/// What every snapshot of one process shares: the cache, admission, metrics
/// and the native parameters, which are a function of geometry and kind only.
/// Outlives any one publication, so a worker can stage and prepare before its
/// first activation.
pub struct DisplayRuntime {
    pub config: ServiceConfig,
    required: bool,
    pub cache: Arc<RuntimeCache>,
    pub admission: Arc<Admission>,
    pub metrics: Arc<Metrics>,
    params: Mutex<HashMap<ParamsKey, Arc<SharedParams>>>,
}

impl DisplayRuntime {
    pub fn new(config: ServiceConfig, disk: Option<DiskCache>) -> Arc<Self> {
        Self::create(config, disk, false)
    }
    /// Managed consumers may only load publisher-prepared native files.
    pub fn new_required(config: ServiceConfig) -> Arc<Self> {
        Self::create(config, None, true)
    }
    fn create(config: ServiceConfig, disk: Option<DiskCache>, required: bool) -> Arc<Self> {
        let _ = pir_control::Identity::process();
        let metrics = Arc::new(Metrics::default());
        let cache = Arc::new(
            RuntimeCache::new(config.cache_bytes, config.build_slots, metrics.clone())
                .with_disk(disk),
        );
        let admission = Arc::new(Admission::new(
            AdmissionConfig {
                query_slots: config.query_slots,
                max_waiters: config.max_waiters,
                max_body_bytes: config.max_body_bytes,
                upload_deadline: config.upload_deadline,
                query_deadline: config.query_deadline,
            },
            metrics.clone(),
        ));
        Arc::new(Self {
            config,
            required,
            cache,
            admission,
            metrics,
            params: Mutex::new(HashMap::new()),
        })
    }

    /// The shared parameters of one geometry's table kind, derived once.
    pub fn params(
        &self,
        geometry: &'static Geometry,
        table: Table,
    ) -> Result<Arc<SharedParams>, String> {
        let mut params = self.params.lock().expect("display params");
        if let Some(shared) = params.get(&(geometry.name, table)) {
            return Ok(shared.clone());
        }
        let shared = Arc::new(SharedParams::build_display(geometry, table)?);
        params.insert((geometry.name, table), shared.clone());
        Ok(shared)
    }

    /// Finds or builds the runtime of one segment, retrying transient
    /// admission pressure as the prewarm does. Also says whether and how it
    /// had to be produced (`None` when it was resident): an operation reports
    /// its own builds this way, not as a delta of the process-wide counter,
    /// which concurrent stages and requests move.
    ///
    /// `shipped` is tried before a restore or build, for an unsealed revision
    /// only: archives are built once per seal by `stage`, and are never
    /// shipped.
    pub(crate) async fn runtime(
        &self,
        revision: &DisplayRevision,
        table: DisplayTable,
        segment: u32,
        cancelled: &AtomicBool,
        shipped: Option<&ShippedRuntimes>,
    ) -> Result<(RuntimeHandle, Option<Produced>), CacheError> {
        let source = revision
            .segment(table, segment)
            .cloned()
            .ok_or_else(|| CacheError::Failed("display segment is not held".into()))?;
        let key = runtime_key(&revision.digest, table, segment);
        if let Some(handle) = self.cache.cached(&key) {
            return Ok((handle, None));
        }
        let shared = self
            .params(revision.geometry, kind(table))
            .map_err(CacheError::Failed)?;
        let shipped = if self.required {
            Some(shipped.ok_or_else(|| {
                CacheError::Failed("managed display requires prepared material".into())
            })?)
        } else {
            shipped.filter(|_| !revision.manifest.sealed)
        };
        transparent_shared::runtime::prewarm::retry(
            cancelled,
            std::time::Duration::from_secs(30),
            || {
                self.cache.get_background_from(
                    key.clone(),
                    shared.clone(),
                    source.clone(),
                    shipped.cloned(),
                )
            },
        )
        .await
        .map(|(handle, produced)| (handle, Some(produced)))
    }
}

/// The prewarm's progress toward holding every runtime of the role.
struct WarmState {
    target: usize,
    finished: AtomicBool,
    cancelled: AtomicBool,
    stopped: AtomicBool,
    task: std::sync::Mutex<Option<tokio::task::AbortHandle>>,
    count: AtomicU64,
    /// Targets that were not resident when the prewarm reached them.
    built: AtomicU64,
    /// Of those, loaded from shipped runtimes, and refused ones produced
    /// here instead; and the self-checks' summed time.
    shipped: AtomicU64,
    shipped_fallbacks: AtomicU64,
    self_check_micros: AtomicU64,
}

impl WarmState {
    /// Counts how the prewarm came by one target.
    fn record(&self, produced: Option<Produced>) {
        let Some(produced) = produced else {
            return;
        };
        self.built.fetch_add(1, Ordering::Relaxed);
        match produced {
            Produced::Shipped { check_micros } => {
                self.shipped.fetch_add(1, Ordering::Relaxed);
                self.self_check_micros
                    .fetch_add(check_micros, Ordering::Relaxed);
            }
            Produced::Fallback => {
                self.shipped_fallbacks.fetch_add(1, Ordering::Relaxed);
            }
            Produced::Joined | Produced::Restored | Produced::Built => {}
        }
    }
}

struct Inner {
    set: DisplaySet,
    runtime: Arc<DisplayRuntime>,
    /// One parameter set per geometry the set names, per table kind.
    params: HashMap<ParamsKey, Arc<SharedParams>>,
    max_query_bytes: usize,
    warm: WarmState,
    prewarm_slots: usize,
    /// Set once a successor is active; see [`DisplayState::retire`].
    retired: AtomicBool,
}

/// One publication snapshot: a loaded set and the shared runtime.
#[derive(Clone)]
pub struct DisplayState {
    inner: Arc<Inner>,
}

/// One geometry's table kind, as `GET /v2/txid/init` publishes it.
///
/// `scheme` is the table's one scheme, the 8-bit profile's 28-bit dithered
/// query; the service accepts that length only.
#[derive(Serialize)]
pub struct TableInit {
    pub rows: u64,
    pub row_bytes: u32,
    pub scheme: transparent_native::NativeScheme,
    pub setup_seed: u64,
}

#[derive(Serialize)]
pub struct GeometryInit {
    pub name: String,
    pub txdirectory: TableInit,
}

/// What `GET /v2/txid/init` returns.
#[derive(Serialize)]
pub struct InitResponse {
    pub schema: String,
    pub codec: String,
    pub bucket_domain: String,
    /// The schema native profiles and setup seeds are derived under: display's
    /// own native schema. Display tables take only their row shape from the
    /// history kinds.
    pub native_schema: String,
    pub network: String,
    pub genesis_hash: String,
    pub map_sha256: String,
    pub start_height: u64,
    pub first_shard_id: u64,
    pub covered_through: u64,
    pub shards: usize,
    pub role: String,
    pub geometries: Vec<GeometryInit>,
}

/// What the setup route returns, per segment: the history fields plus the
/// bucket a directory table belongs to.
#[derive(Serialize)]
pub struct SetupResponse {
    pub shard_id: u64,
    pub manifest_digest: String,
    pub geometry: String,
    pub table: String,
    pub bucket: u32,
    pub segment: u32,
    pub segments: u32,
    pub public_params: String,
    pub public_params_sha256: String,
    pub public_params_epoch: String,
}

impl DisplayState {
    /// Prepares a snapshot of `set` over the shared runtime. In warm mode the
    /// role's runtimes must fit the cache with nothing evicted, as for a
    /// history worker's assignment.
    pub fn build(mut set: DisplaySet, runtime: &Arc<DisplayRuntime>) -> Result<Self, String> {
        if runtime.required {
            // Check required files by size even if runtimes are already resident.
            let root = set
                .current()
                .first()
                .and_then(|r| r.dir.parent())
                .ok_or("display set has no root")?
                .to_path_buf();
            let shipped = ShippedRuntimes::required(root);
            // The set above already checked the map and source tables. Verify
            // native files here without rebuilding a publisher distribution:
            // provider-owned stable revision links are not publisher inputs.
            let mut checked = 0usize;
            for revision in set.current() {
                for (table, segment) in revision.targets() {
                    let source = revision
                        .segment(table, segment)
                        .ok_or("missing display source")?;
                    let params = runtime.params(revision.geometry, kind(table))?;
                    let key = runtime_key(&revision.digest, table, segment);
                    shipped
                        .check_size(&key, &params, source.sha256())
                        .map_err(|e| e.to_string())?;
                    checked += 1;
                }
            }
            tracing::info!(checked, "display required runtime sizes checked");
            set.shipped = Some(shipped);
        }
        let mut params = HashMap::new();
        let mut max_query_bytes = 0usize;
        for geometry in set.geometries() {
            let shared = runtime.params(geometry, Table::TxDirectory)?;
            max_query_bytes = max_query_bytes.max(shared.query_bytes());
            params.insert((geometry.name, Table::TxDirectory), shared);
        }
        if params.is_empty() {
            return Err("the display set names no geometry to serve".into());
        }
        // Each held table at its built size, plus the bound for as many as
        // the prewarm can have in flight; see `runtime::warm_bytes`.
        let mut held = Vec::new();
        for revision in set.current().iter().filter(|r| r.held()) {
            for (table, _) in revision.targets() {
                held.push(params[&(revision.geometry.name, kind(table))].as_ref());
            }
        }
        let target = held.len();
        Metrics::set(&runtime.metrics.target_runtimes, target as u64);
        let prewarm_slots = runtime.cache.prewarm_concurrency();
        Ok(Self {
            inner: Arc::new(Inner {
                set,
                runtime: runtime.clone(),
                params,
                max_query_bytes,
                warm: WarmState {
                    target,
                    finished: AtomicBool::new(false),
                    cancelled: AtomicBool::new(false),
                    stopped: AtomicBool::new(false),
                    task: std::sync::Mutex::new(None),
                    count: AtomicU64::new(0),
                    built: AtomicU64::new(0),
                    shipped: AtomicU64::new(0),
                    shipped_fallbacks: AtomicU64::new(0),
                    self_check_micros: AtomicU64::new(0),
                },
                prewarm_slots,
                retired: AtomicBool::new(false),
            }),
        })
    }

    /// Builds every runtime of the role's current revisions and marks the
    /// snapshot warm when done; in warm mode the handles stay pinned.
    pub fn spawn_prewarm(&self) -> tokio::task::JoinHandle<()> {
        let mut task = self.inner.warm.task.lock().unwrap();
        let state = self.clone();
        let running = tokio::spawn(async move {
            let inner = &state.inner;
            let started = std::time::Instant::now();
            let queue = Arc::new(Mutex::new(std::collections::VecDeque::from(
                inner.set.warm_targets(),
            )));
            let mut workers = tokio::task::JoinSet::new();
            for _ in 0..inner.prewarm_slots {
                let state = state.clone();
                let queue = queue.clone();
                workers.spawn(async move {
                    let inner = &state.inner;
                    loop {
                        if inner.warm.cancelled.load(Ordering::Acquire) || inner.warm.stopped.load(Ordering::Acquire) {
                            break;
                        }
                        let next = queue.lock().unwrap().pop_front();
                        let Some((digest, table, segment)) = next else {
                            break;
                        };
                        let Some(revision) = inner.set.held(&digest) else {
                            continue;
                        };
                        if inner.warm.stopped.load(Ordering::Acquire) { break; }
                        match inner
                            .runtime
                            .runtime(
                                revision,
                                table,
                                segment,
                                &inner.warm.cancelled,
                                inner.set.shipped.as_ref(),
                            )
                            .await
                        {
                            Ok((handle, produced)) => {
                                if inner.warm.cancelled.load(Ordering::Acquire) || inner.warm.stopped.load(Ordering::Acquire) {
                                    break;
                                }
                                inner.warm.record(produced);
                                inner.warm.count.fetch_add(1, Ordering::Release);
                                drop(handle);
                                Metrics::incr(&inner.runtime.metrics.warm_runtimes);
                            }
                            Err(error) => {
                                tracing::warn!(revision = %digest, table = %table.label(), segment, %error, "display prewarm failed");
                                Metrics::incr(&inner.runtime.metrics.prewarm_failed);
                            }
                        }
                    }
                });
            }
            while workers.join_next().await.is_some() {}
            Metrics::set(
                &inner.runtime.metrics.prewarm_micros,
                started.elapsed().as_micros() as u64,
            );
            inner.warm.finished.store(true, Ordering::Release);
            tracing::info!(
                warm = inner.warm.count.load(Ordering::Acquire),
                target = inner.warm.target,
                seconds = started.elapsed().as_secs_f64(),
                map = %inner.set.map_digest,
                "display prewarm finished"
            );
        });
        *task = Some(running.abort_handle());
        running
    }

    pub fn set(&self) -> &DisplaySet {
        &self.inner.set
    }

    pub fn runtime(&self) -> &Arc<DisplayRuntime> {
        &self.inner.runtime
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.inner.runtime.metrics
    }

    /// The largest body any geometry of this set can legitimately send.
    pub fn max_query_bytes(&self) -> usize {
        self.inner.max_query_bytes
    }

    pub(crate) fn resident_targets(&self) -> usize {
        self.inner
            .set
            .warm_targets()
            .into_iter()
            .filter(|(digest, table, segment)| {
                self.inner
                    .runtime
                    .cache
                    .cached(&runtime_key(digest, *table, *segment))
                    .is_some()
            })
            .count()
    }

    pub fn warm_target(&self) -> usize {
        self.inner.warm.target
    }

    /// Warm targets the prewarm found not resident, so built (or restored)
    /// for this snapshot.
    pub fn built(&self) -> u64 {
        self.inner.warm.built.load(Ordering::Relaxed)
    }

    /// Of [`Self::built`], runtimes loaded from shipped files, shipped ones
    /// refused and produced here instead, and the self-checks' summed time.
    pub fn shipped(&self) -> (u64, u64, std::time::Duration) {
        let warm = &self.inner.warm;
        (
            warm.shipped.load(Ordering::Relaxed),
            warm.shipped_fallbacks.load(Ordering::Relaxed),
            std::time::Duration::from_micros(warm.self_check_micros.load(Ordering::Relaxed)),
        )
    }

    pub(crate) fn is_ready(&self) -> bool {
        !self.inner.warm.cancelled.load(Ordering::Acquire)
    }

    pub fn is_warm(&self) -> bool {
        !self.inner.warm.cancelled.load(Ordering::Acquire)
            && self.inner.warm.finished.load(Ordering::Acquire)
            && self.inner.warm.count.load(Ordering::Acquire) as usize >= self.inner.warm.target
    }

    pub(crate) fn has_other_holders(&self) -> bool {
        Arc::strong_count(&self.inner) > 1
    }

    /// Stop warming invalidated material without changing request authority.
    pub(crate) fn stop_warming(&self, digests: Option<&BTreeSet<String>>) {
        if digests.is_none_or(|digests| {
            self.inner
                .set
                .current()
                .iter()
                .any(|r| digests.contains(&r.digest))
        }) {
            self.inner.warm.stopped.store(true, Ordering::Release);
            if let Some(task) = self.inner.warm.task.lock().unwrap().take() {
                task.abort();
            }
        }
    }

    /// Marks a snapshot superseded by an activation and drops its pins. It
    /// keeps answering clients on its map, but only from runtimes still
    /// resident: the next prepare evicts them, and a cold build for a stale
    /// map would take a build slot from the candidate the next activation
    /// waits for. Such a client is sent to refresh its map (409) instead.
    pub(crate) fn retire(&self) {
        self.inner.retired.store(true, Ordering::Release);
        self.stop_warming(None);
    }

    pub fn is_retired(&self) -> bool {
        self.inner.retired.load(Ordering::Acquire)
    }

    /// The runtimes of `segments` of `table`, if all are resident; for a
    /// retired snapshot, which never builds.
    fn resident(
        &self,
        shard_id: u64,
        digest: &str,
        table: DisplayTable,
        segments: std::ops::Range<u32>,
    ) -> Result<Vec<RuntimeHandle>, RequestError> {
        segments
            .map(|segment| {
                self.inner
                    .runtime
                    .cache
                    .cached(&runtime_key(digest, table, segment))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                Metrics::incr(&self.inner.runtime.metrics.stale_revisions);
                stale(shard_id, digest)
            })
    }

    fn shared(&self, revision: &DisplayRevision, table: DisplayTable) -> Arc<SharedParams> {
        self.inner
            .params
            .get(&(revision.geometry.name, kind(table)))
            .expect("every named geometry has parameters")
            .clone()
    }

    /// Resolves a digest-addressed request inside this snapshot.
    ///
    /// The tier must be the revision's own and the role must hold it; both
    /// are routing faults (421), not stale clients. A digest this snapshot
    /// does not know is stale (409), and the client refreshes its map.
    fn revision(
        &self,
        sealed: bool,
        shard_id: u64,
        digest: &str,
    ) -> Result<&DisplayRevision, RequestError> {
        let metrics = &self.inner.runtime.metrics;
        let Some(revision) = self.inner.set.revision(digest) else {
            Metrics::incr(&metrics.stale_revisions);
            return Err(stale(shard_id, digest));
        };
        if revision.manifest.shard_id != shard_id {
            return Err(RequestError::Bad(format!(
                "revision {digest} is not a revision of display shard {shard_id}"
            )));
        }
        if revision.manifest.sealed != sealed {
            Metrics::incr(&metrics.unassigned_refusals);
            return Err(RequestError::Misdirected(format!(
                "revision {digest} of display shard {shard_id} is {}, not {}",
                tier(revision.manifest.sealed),
                tier(sealed)
            )));
        }
        if !revision.held() {
            Metrics::incr(&metrics.unassigned_refusals);
            return Err(RequestError::Misdirected(format!(
                "this {} worker does not serve {} display revisions",
                self.inner.set.role.as_str(),
                tier(sealed)
            )));
        }
        Ok(revision)
    }

    fn snapshot(&self) -> Snapshot {
        let inner = &self.inner;
        Snapshot {
            labels: vec![
                ("map_sha256".into(), inner.set.map_digest.clone()),
                ("role".into(), inner.set.role.as_str().into()),
            ],
            shards: inner.set.map.shards.len() as u64,
            assigned_shards: inner.set.current().iter().filter(|r| r.held()).count() as u64,
            revisions: inner.set.revisions().len() as u64,
            prunable_revisions: inner.set.excess.len() as u64,
            cache_budget_bytes: inner.runtime.cache.budget(),
            warm_runtimes: inner.warm.count.load(Ordering::Acquire),
            work_memory_reserved_bytes: inner.runtime.cache.work_memory().reserved_bytes(),
            process_rss_bytes: transparent_shared::runtime::procmem::process_rss_bytes(),
            process_cpu: transparent_shared::runtime::procmem::process_cpu(),
            cgroup_memory_bytes: transparent_shared::runtime::procmem::cgroup_memory_bytes(),
        }
    }

    /// The parts of this worker a request passes through.
    fn worker(&self) -> Worker<'_> {
        let runtime = &self.inner.runtime;
        Worker {
            admission: &runtime.admission,
            work_memory: runtime.cache.work_memory(),
            metrics: &runtime.metrics,
            max_query_bytes: self.max_query_bytes(),
            map_digest: &self.inner.set.map_digest,
        }
    }
}

/// The revision named is no longer served: the wallet refreshes its txid map.
fn stale(shard_id: u64, digest: &str) -> RequestError {
    RequestError::Stale {
        message: format!("revision {digest} of display shard {shard_id} is no longer served"),
        retry: "refresh the txid map",
    }
}

/// The request body limit: the longest query any display geometry accepts,
/// at any width its profile takes. Fixed at startup, before any publication
/// is loaded.
pub fn body_limit() -> usize {
    let native = transparent_native::profile_for(Table::TxDirectory.as_str());
    let bits = native.nearest_bits.unwrap_or(0).max(native.dithered_bits);
    display::DISPLAY_PROFILES
        .iter()
        .map(|geometry| 8 + native.request_len_bits(geometry.directory_rows as usize, bits))
        .max()
        .unwrap_or(0)
}

pub fn router(state: DisplayState) -> Router {
    const SETUP: &str = "shards/:shard_id/revisions/:digest/setup/:table/:segment";
    const QUERY: &str = "shards/:shard_id/revisions/:digest/query/:table";
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/v2/txid/init", get(init))
        .route("/v2/txid/shards", get(shard_map))
        .route("/v2/txid/map", get(recent_map))
        .route("/v2/txid/map/:base/:digest", get(index_chunk))
        .route(
            "/v2/txid/shards/:shard_id/revisions/:digest/manifest",
            get(manifest),
        )
        .route(&format!("/v2/txid/archive/{SETUP}"), get(setup_archive))
        .route(&format!("/v2/txid/recent/{SETUP}"), get(setup_recent))
        .route(&format!("/v2/txid/archive/{QUERY}"), post(query_archive))
        .route(&format!("/v2/txid/recent/{QUERY}"), post(query_recent))
        .with_state(state)
}

async fn health(State(state): State<DisplayState>) -> Response {
    let inner = &state.inner;
    let cache = &inner.runtime.cache;
    json(
        StatusCode::OK,
        serde_json::json!({
            "phase": "serving",
            "role": inner.set.role.as_str(),
            "map_sha256": inner.set.map_digest,
            "shards": inner.set.map.shards.len(),
            "held_revisions": inner.set.revisions().iter().filter(|r| r.held()).count(),
            "geometries": inner.set.geometries().iter().map(|g| g.name).collect::<Vec<_>>(),
            "cache_budget_bytes": cache.budget(),
            "cache_resident_bytes": cache.resident_bytes(),
            "runtimes_built": cache.entries(),
            "warm_runtimes": inner.warm.count.load(Ordering::Acquire),
            "target_runtimes": inner.warm.target,
            "work_memory_reserved_bytes": cache.work_memory().reserved_bytes(),
            "binary_sha256": pir_control::binary_sha256(),
            "incarnation": pir_control::Identity::process().incarnation,
            "started_unix": pir_control::Identity::process().started_unix,
        }),
    )
}

/// Ready once the role's runtimes are warm. Readiness reads only counters,
/// because the edge checks it every second.
async fn ready(State(state): State<DisplayState>) -> Response {
    let inner = &state.inner;
    let warm = state.is_warm();
    let mut body = serde_json::json!({
        "ready": state.is_ready(),
        "role": inner.set.role.as_str(),
        "map_sha256": inner.set.map_digest,
        "warm": warm,
        "warm_runtimes": inner.warm.count.load(Ordering::Acquire),
        "target_runtimes": inner.warm.target,
        "prewarm_finished": inner.warm.finished.load(Ordering::Acquire),
        "prewarm_failed": Metrics::get(&inner.runtime.metrics.prewarm_failed),
        "covered_through": inner.set.map.covered_through(),
        "binary_sha256": pir_control::binary_sha256(),
        "incarnation": pir_control::Identity::process().incarnation,
    });
    if body["ready"] == true {
        return json(StatusCode::OK, body);
    }
    body["reason"] = serde_json::json!(if inner.warm.finished.load(Ordering::Acquire) {
        "prewarm finished short of its target"
    } else {
        "prewarming"
    });
    json(StatusCode::SERVICE_UNAVAILABLE, body)
}

async fn metrics(State(state): State<DisplayState>) -> Response {
    let snapshot = state.snapshot();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        state.metrics().render(&snapshot) + &state.metrics().render_display(&snapshot),
    )
        .into_response()
}

async fn init(State(state): State<DisplayState>) -> Response {
    let inner = &state.inner;
    let set = &inner.set;
    let table = |geometry: &'static Geometry, kind: Table| {
        let shared = &inner.params[&(geometry.name, kind)];
        TableInit {
            rows: kind.rows(geometry),
            row_bytes: kind.row_bytes(geometry),
            scheme: shared.scheme().clone(),
            setup_seed: shared.setup_seed,
        }
    };
    let body = InitResponse {
        schema: display::DISPLAY_SCHEMA.into(),
        codec: transparent_shard::txid::CODEC.into(),
        bucket_domain: String::from_utf8_lossy(display::BUCKET_DOMAIN).into_owned(),
        native_schema: transparent_shard::display::NATIVE_SCHEMA.into(),
        network: set.map.network.clone(),
        genesis_hash: set.map.genesis_hash.clone(),
        map_sha256: set.map_digest.clone(),
        start_height: set.map.start_height,
        first_shard_id: set.map.first_shard_id,
        covered_through: set.map.covered_through().unwrap_or_default(),
        shards: set.map.shards.len(),
        role: set.role.as_str().into(),
        geometries: set
            .geometries()
            .into_iter()
            .map(|geometry| GeometryInit {
                name: geometry.name.into(),
                txdirectory: table(geometry, Table::TxDirectory),
            })
            .collect(),
    };
    json(
        StatusCode::OK,
        serde_json::to_value(body).expect("init json"),
    )
}

/// The map exactly as loaded. Not cacheable: it changes at every publication.
async fn shard_map(State(state): State<DisplayState>) -> Response {
    let set = &state.inner.set;
    (
        StatusCode::OK,
        [
            ("content-type", "application/json"),
            ("x-txid-map-sha256", set.map_digest.as_str()),
            ("cache-control", "no-cache"),
        ],
        set.map_json.clone(),
    )
        .into_response()
}

/// The recent map of the split. Not cacheable: it changes every block.
async fn recent_map(State(state): State<DisplayState>) -> Response {
    let split = &state.inner.set.split;
    (
        StatusCode::OK,
        [
            ("content-type", "application/json"),
            ("x-txid-map-sha256", split.recent_sha256.as_str()),
            ("cache-control", "no-cache"),
        ],
        split.recent_bytes.clone(),
    )
        .into_response()
}

/// One index chunk, by base shard id and digest. Addressed by its content, so
/// cacheable forever; a digest no map of this snapshot names is stale.
async fn index_chunk(
    State(state): State<DisplayState>,
    AxumPath((base, digest)): AxumPath<(u64, String)>,
) -> Response {
    let map_digest = state.inner.set.map_digest.clone();
    let Some((chunk_base, bytes)) = state.inner.set.chunk(&digest) else {
        Metrics::incr(&state.metrics().stale_revisions);
        return json(
            StatusCode::CONFLICT,
            serde_json::json!({
                "error": format!("index chunk {digest} is no longer served"),
                "retry": "refresh the txid map",
                "map_sha256": map_digest,
            }),
        );
    };
    if chunk_base != base {
        return RequestError::Bad(format!(
            "index chunk {digest} starts at shard {chunk_base}, not {base}"
        ))
        .into_response(&map_digest);
    }
    (
        StatusCode::OK,
        [
            ("content-type", "application/json"),
            ("cache-control", "public, max-age=31536000, immutable"),
        ],
        bytes.to_vec(),
    )
        .into_response()
}

/// Any revision's canonical manifest, held or not, so one worker answers the
/// metadata routes for the whole publication.
async fn manifest(
    State(state): State<DisplayState>,
    AxumPath((shard_id, digest)): AxumPath<(u64, String)>,
) -> Response {
    let map_digest = state.inner.set.map_digest.clone();
    let Some(revision) = state.inner.set.revision(&digest) else {
        Metrics::incr(&state.metrics().stale_revisions);
        return stale(shard_id, &digest).into_response(&map_digest);
    };
    if revision.manifest.shard_id != shard_id {
        return RequestError::Bad(format!(
            "revision {digest} is not a revision of display shard {shard_id}"
        ))
        .into_response(&map_digest);
    }
    Metrics::incr(&state.metrics().manifests);
    (
        StatusCode::OK,
        [
            ("content-type", "application/json"),
            ("cache-control", "public, max-age=31536000, immutable"),
            ("x-manifest-sha256", digest.as_str()),
        ],
        revision.canonical.clone(),
    )
        .into_response()
}

type SetupPath = AxumPath<(u64, String, String, u32)>;
type QueryPath = AxumPath<(u64, String, String)>;

async fn setup_archive(state: State<DisplayState>, path: SetupPath) -> Response {
    setup(state, true, path).await
}

async fn setup_recent(state: State<DisplayState>, path: SetupPath) -> Response {
    setup(state, false, path).await
}

async fn query_archive(state: State<DisplayState>, path: QueryPath, request: Request) -> Response {
    query(state, true, path, request).await
}

async fn query_recent(state: State<DisplayState>, path: QueryPath, request: Request) -> Response {
    query(state, false, path, request).await
}

/// A table label of `revision`, and its segment count. A bucket at or past
/// the shard's bucket count is a bad request, not a stale one.
fn display_table(
    revision: &DisplayRevision,
    label: &str,
) -> Result<(DisplayTable, u32), RequestError> {
    let table = DisplayTable::parse(label)
        .ok_or_else(|| RequestError::Bad(format!("unknown display table {label:?}")))?;
    let segments = revision.segments(table).ok_or_else(|| {
        RequestError::Bad(format!(
            "display shard {} has {} buckets; {label} is not one of them",
            revision.manifest.shard_id, revision.manifest.n_buckets
        ))
    })?;
    Ok((table, segments))
}

async fn setup(
    State(state): State<DisplayState>,
    sealed: bool,
    AxumPath((shard_id, digest, label, segment)): SetupPath,
) -> Response {
    let map_digest = state.inner.set.map_digest.clone();
    let runtime = state.runtime().clone();
    // Counted as a waiter while the runtime is acquired or built, as for a
    // history setup request.
    let pending = match runtime.admission.try_enter(None) {
        Ok(pending) => pending,
        Err(error) => return RequestError::from(error).into_response(&map_digest),
    };
    let resolved = state
        .revision(sealed, shard_id, &digest)
        .and_then(|revision| {
            let (table, segments) = display_table(revision, &label)?;
            if segment >= segments {
                return Err(RequestError::Bad(format!(
                    "{label} of display shard {shard_id} has {segments} segments"
                )));
            }
            let source = revision.segment(table, segment).cloned().ok_or_else(|| {
                RequestError::Bad(format!("{label} segment {segment} is not held"))
            })?;
            Ok((
                table,
                segments,
                revision.geometry.name,
                state.shared(revision, table),
                source,
            ))
        });
    let (table, segments, geometry, shared, source) = match resolved {
        Ok(resolved) => resolved,
        Err(error) => return error.into_response(&map_digest),
    };
    let handle = if state.is_retired() {
        match state.resident(shard_id, &digest, table, segment..segment + 1) {
            Ok(mut handles) => handles.pop().expect("one segment"),
            Err(error) => return error.into_response(&map_digest),
        }
    } else {
        let key = runtime_key(&digest, table, segment);
        let built = async { Ok::<_, RequestError>(runtime.cache.get(key, shared, source).await?) };
        match within(pending.remaining(), &runtime.metrics, built).await {
            Ok(handle) => handle,
            Err(error) => return error.into_response(&map_digest),
        }
    };
    pending.complete();
    let built = handle.get();
    let body = SetupResponse {
        shard_id,
        manifest_digest: digest,
        geometry: geometry.into(),
        table: table.label(),
        bucket: table.bucket(),
        segment,
        segments,
        public_params: BASE64_STANDARD.encode(&built.public_params),
        public_params_sha256: built.public_params_sha256.clone(),
        public_params_epoch: hex::encode(built.public_params_epoch),
    };
    Metrics::incr(&runtime.metrics.setups);
    json(
        StatusCode::OK,
        serde_json::to_value(body).expect("setup json"),
    )
}

async fn query(
    state: State<DisplayState>,
    sealed: bool,
    path: QueryPath,
    request: Request,
) -> Response {
    let mut timer =
        transparent_shared::runtime::metrics::QueryTimer::new(state.0.metrics().clone());
    let response = query_inner(state, sealed, path, request).await;
    timer.finish(response.status().is_success());
    response
}

/// A query addressed by tier, revision and display table, answered through
/// the request path the history worker uses.
async fn query_inner(
    State(state): State<DisplayState>,
    sealed: bool,
    AxumPath((shard_id, digest, label)): QueryPath,
    request: Request,
) -> Response {
    let worker = state.worker();
    let map_digest = worker.map_digest;
    let metrics = worker.metrics;
    let resolved = state
        .revision(sealed, shard_id, &digest)
        .and_then(|revision| {
            let (table, segments) = display_table(revision, &label)?;
            let sources = (0..segments)
                .map(|segment| revision.segment(table, segment).cloned())
                .collect::<Option<Vec<SegmentSource>>>()
                .ok_or_else(|| RequestError::Bad(format!("{label} is not held")))?;
            Ok((table, state.shared(revision, table), sources))
        });
    // A retired snapshot is refused before the body is queued if a segment
    // has already gone; the check is repeated when the handles are taken.
    let segments = |sources: &[SegmentSource]| 0..sources.len() as u32;
    let resolved = resolved.and_then(|(table, shared, sources)| {
        if state.is_retired() {
            state.resident(shard_id, &digest, table, segments(&sources))?;
        }
        Ok((table, shared, sources))
    });
    let (table, shared, sources) = match resolved {
        Ok(resolved) => resolved,
        Err(error) => {
            Metrics::incr(&metrics.query_errors);
            let refused = error.into_response(map_digest);
            return worker.refuse_unread(request, refused).await;
        }
    };
    let query = Query {
        query_rows: shared.profile.rows,
        shared: shared.clone(),
        table: label,
        revision: digest.clone(),
        // Names the revision and the bucket: a body prepared for another
        // bucket's table is refused by `SharedParams::parse`.
        binding: display::query_binding(&digest, table),
    };

    let acquire = async |admitted: &Admitted| {
        if state.is_retired() {
            return state
                .resident(shard_id, &digest, table, segments(&sources))
                .map_err(|error| {
                    Metrics::incr(&metrics.query_errors);
                    error.into_response(map_digest)
                });
        }
        let mut handles = Vec::with_capacity(sources.len());
        for (segment, source) in sources.into_iter().enumerate() {
            let key = runtime_key(&digest, table, segment as u32);
            let built = async {
                Ok::<_, RequestError>(
                    state
                        .runtime()
                        .cache
                        .get(key, shared.clone(), source)
                        .await?,
                )
            };
            match within(admitted.remaining(), metrics, built).await {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    Metrics::incr(&metrics.query_errors);
                    return Err(error.into_response(map_digest));
                }
            }
        }
        Ok(handles)
    };
    answer_query(worker, request, query, acquire).await
}
