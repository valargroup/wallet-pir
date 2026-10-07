//! Serving one display publication snapshot over HTTP.
//!
//! The query and setup paths are copies of the history worker's
//! (`crate::service`), kept separate so the history path does not change for
//! a proof of concept. What differs is the address: a revision is resolved
//! with its tier and this worker's role, a table is a bucket directory or the
//! shard's pages, and the binding names the bucket. Everything that bounds
//! work is the history worker's own: the runtime cache, admission, work-memory
//! reservations and their status codes.

use super::set::{DisplayRevision, DisplaySet};
use super::{kind, runtime_key, tier};
use crate::admission::{Admission, AdmissionConfig, AdmissionError};
use crate::metrics::{Metrics, Snapshot};
use crate::runtime::{
    disk::{DiskCache, ShippedRuntimes},
    warm_bytes, CacheError, Produced, RuntimeCache, RuntimeHandle, SharedParams,
};
use crate::service::{ReadinessMode, ServiceConfig};
use crate::shardset::{SegmentSource, Table};
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

type ParamsKey = (&'static str, Table);

/// What every snapshot of one process shares: the cache, admission, metrics
/// and the native parameters, which are a function of geometry and kind only.
/// Outlives any one publication, so a worker can stage and prepare before its
/// first activation.
pub struct DisplayRuntime {
    pub config: ServiceConfig,
    pub cache: Arc<RuntimeCache>,
    pub admission: Arc<Admission>,
    pub metrics: Arc<Metrics>,
    params: Mutex<HashMap<ParamsKey, Arc<SharedParams>>>,
}

impl DisplayRuntime {
    pub fn new(config: ServiceConfig, disk: Option<DiskCache>) -> Arc<Self> {
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
        let shared = Arc::new(SharedParams::build(geometry, table)?);
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
        let shipped = shipped.filter(|_| !revision.manifest.sealed);
        crate::prewarm::retry(cancelled, std::time::Duration::from_secs(30), || {
            self.cache.get_from(
                key.clone(),
                shared.clone(),
                source.clone(),
                shipped.cloned(),
            )
        })
        .await
        .map(|(handle, produced)| (handle, Some(produced)))
    }
}

/// The prewarm's progress toward holding every runtime of the role.
struct WarmState {
    mode: ReadinessMode,
    target: usize,
    finished: AtomicBool,
    cancelled: AtomicBool,
    count: AtomicU64,
    /// Targets that were not resident when the prewarm reached them.
    built: AtomicU64,
    /// Of those, loaded from shipped runtimes, and refused ones produced
    /// here instead; and the self-checks' summed time.
    shipped: AtomicU64,
    shipped_fallbacks: AtomicU64,
    self_check_micros: AtomicU64,
    pins: Mutex<Vec<(String, RuntimeHandle)>>,
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

/// One geometry's table kind, as `GET /v1/txid/init` publishes it.
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
    pub txpages: TableInit,
}

/// What `GET /v1/txid/init` returns.
#[derive(Serialize)]
pub struct InitResponse {
    pub schema: String,
    pub codec: String,
    pub bucket_domain: String,
    /// The schema native profiles and setup seeds are derived under. Display
    /// tables use the history kinds' parameters; only the binding is display.
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
    pub bucket: Option<u32>,
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
    pub fn build(set: DisplaySet, runtime: &Arc<DisplayRuntime>) -> Result<Self, String> {
        let config = runtime.config;
        let mut params = HashMap::new();
        let mut max_query_bytes = 0usize;
        for geometry in set.geometries() {
            for table in [Table::TxDirectory, Table::TxPages] {
                let shared = runtime.params(geometry, table)?;
                max_query_bytes = max_query_bytes.max(shared.query_bytes());
                params.insert((geometry.name, table), shared);
            }
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
        let needed = warm_bytes(held, runtime.cache.prewarm_concurrency());
        if config.readiness == ReadinessMode::Warm && needed > config.cache_bytes {
            return Err(format!(
                "the role's display tables need {needed} bytes of runtimes, the cache budget is {}",
                config.cache_bytes
            ));
        }
        Metrics::set(&runtime.metrics.target_runtimes, target as u64);
        let prewarm_slots = runtime.cache.prewarm_concurrency();
        Ok(Self {
            inner: Arc::new(Inner {
                set,
                runtime: runtime.clone(),
                params,
                max_query_bytes,
                warm: WarmState {
                    mode: config.readiness,
                    target,
                    finished: AtomicBool::new(false),
                    cancelled: AtomicBool::new(false),
                    count: AtomicU64::new(0),
                    built: AtomicU64::new(0),
                    shipped: AtomicU64::new(0),
                    shipped_fallbacks: AtomicU64::new(0),
                    self_check_micros: AtomicU64::new(0),
                    pins: Mutex::new(Vec::new()),
                },
                prewarm_slots,
                retired: AtomicBool::new(false),
            }),
        })
    }

    /// Builds every runtime of the role's current revisions and marks the
    /// snapshot warm when done; in warm mode the handles stay pinned.
    pub fn spawn_prewarm(&self) -> tokio::task::JoinHandle<()> {
        let state = self.clone();
        tokio::spawn(async move {
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
                        if inner.warm.cancelled.load(Ordering::Acquire) {
                            break;
                        }
                        let next = queue.lock().unwrap().pop_front();
                        let Some((digest, table, segment)) = next else {
                            break;
                        };
                        let Some(revision) = inner.set.held(&digest) else {
                            continue;
                        };
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
                                if inner.warm.cancelled.load(Ordering::Acquire) {
                                    break;
                                }
                                inner.warm.record(produced);
                                inner.warm.count.fetch_add(1, Ordering::Release);
                                if inner.warm.mode == ReadinessMode::Warm {
                                    inner.warm.pins.lock().unwrap().push((digest, handle));
                                }
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
        })
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

    pub fn is_warm(&self) -> bool {
        !self.inner.warm.cancelled.load(Ordering::Acquire)
            && self.inner.warm.finished.load(Ordering::Acquire)
            && self.inner.warm.count.load(Ordering::Acquire) as usize >= self.inner.warm.target
    }

    pub(crate) fn has_other_holders(&self) -> bool {
        Arc::strong_count(&self.inner) > 1
    }

    /// Drops residency pins: every one, or those of `digests`. An invalidated
    /// revision is no longer an obligation.
    pub(crate) fn release_pins(&self, digests: Option<&BTreeSet<String>>) {
        let mut pins = self.inner.warm.pins.lock().unwrap();
        match digests {
            None => pins.clear(),
            Some(digests) => pins.retain(|(digest, _)| !digests.contains(digest)),
        }
    }

    /// Marks a snapshot superseded by an activation and drops its pins. It
    /// keeps answering clients on its map, but only from runtimes still
    /// resident: the next prepare evicts them, and a cold build for a stale
    /// map would take a build slot from the candidate the next activation
    /// waits for. Such a client is sent to refresh its map (409) instead.
    pub(crate) fn retire(&self) {
        self.inner.retired.store(true, Ordering::Release);
        self.release_pins(None);
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
                RequestError::Stale {
                    shard_id,
                    digest: digest.to_string(),
                }
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
            return Err(RequestError::Stale {
                shard_id,
                digest: digest.to_string(),
            });
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
            work_memory_reserved_bytes: inner.runtime.cache.work_memory.reserved_bytes(),
            process_rss_bytes: crate::procmem::process_rss_bytes(),
            process_cpu: crate::procmem::process_cpu(),
            cgroup_memory_bytes: crate::procmem::cgroup_memory_bytes(),
        }
    }

    /// Reads and discards a refused query's body before answering; see the
    /// history worker's `refuse_unread`.
    async fn refuse_unread(&self, request: Request, refused: Response) -> Response {
        use http_body::Body as _;
        let limit = self.max_query_bytes();
        let wait = self
            .inner
            .runtime
            .admission
            .config()
            .upload_deadline
            .min(std::time::Duration::from_secs(5));
        let mut body = request.into_body();
        let drained = tokio::time::timeout(wait, async {
            let mut seen = 0usize;
            loop {
                match std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await
                {
                    None => return true,
                    Some(Ok(frame)) => {
                        seen += frame.data_ref().map_or(0, |data| data.len());
                        if seen > limit {
                            return false;
                        }
                    }
                    Some(Err(_)) => return false,
                }
            }
        })
        .await;
        if !matches!(drained, Ok(true)) {
            Metrics::incr(&self.inner.runtime.metrics.refusals_unread);
        }
        refused
    }
}

/// Why a request could not be answered.
pub(crate) enum RequestError {
    Bad(String),
    /// The revision named is not known. Retryable after a map refresh.
    Stale {
        shard_id: u64,
        digest: String,
    },
    /// No cache capacity could be freed. Retryable as-is.
    Overloaded,
    LengthRequired,
    Busy(&'static str),
    UploadTimeout,
    /// Wrong tier or wrong role: a routing fault, not retryable here.
    Misdirected(String),
}

impl From<AdmissionError> for RequestError {
    fn from(error: AdmissionError) -> Self {
        match error {
            AdmissionError::QueueFull => RequestError::Busy("every waiting place is taken"),
            AdmissionError::BodyBudget => RequestError::Busy("the body budget is full"),
            AdmissionError::DeadlineExceeded => {
                RequestError::Busy("the request waited its whole deadline without a slot")
            }
            AdmissionError::UploadTimeout => RequestError::UploadTimeout,
            AdmissionError::ShuttingDown => RequestError::Bad("server is shutting down".into()),
        }
    }
}

impl From<CacheError> for RequestError {
    fn from(error: CacheError) -> Self {
        match error {
            CacheError::Overloaded => RequestError::Overloaded,
            CacheError::Failed(message) => RequestError::Bad(message),
        }
    }
}

impl RequestError {
    pub(crate) fn into_response(self, map_digest: &str) -> Response {
        let retry = |reason: &str| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [("content-type", "application/json"), ("retry-after", "1")],
                serde_json::json!({"error": reason, "retry": "retry shortly"}).to_string(),
            )
                .into_response()
        };
        match self {
            RequestError::Bad(message) => json(
                StatusCode::BAD_REQUEST,
                serde_json::json!({"error": message}),
            ),
            RequestError::LengthRequired => json(
                StatusCode::LENGTH_REQUIRED,
                serde_json::json!({"error": "a query must declare its length"}),
            ),
            RequestError::UploadTimeout => json(
                StatusCode::REQUEST_TIMEOUT,
                serde_json::json!({"error": "the query body did not arrive in time"}),
            ),
            RequestError::Misdirected(message) => json(
                StatusCode::MISDIRECTED_REQUEST,
                serde_json::json!({"error": message, "map_sha256": map_digest}),
            ),
            RequestError::Busy(reason) => retry(reason),
            RequestError::Overloaded => retry("no cache capacity is free"),
            RequestError::Stale { shard_id, digest } => json(
                StatusCode::CONFLICT,
                serde_json::json!({
                    "error": format!("revision {digest} of display shard {shard_id} is no longer served"),
                    "retry": "refresh the txid map",
                    "map_sha256": map_digest,
                }),
            ),
        }
    }
}

/// The request body limit: the longest query any display geometry accepts.
/// Fixed at startup, before any publication is loaded.
pub fn body_limit() -> usize {
    display::DISPLAY_PROFILES
        .iter()
        .flat_map(|geometry| [geometry.directory_rows, geometry.page_rows])
        .map(|rows| 8 + transparent_native::request_len(rows as usize))
        .max()
        .unwrap_or(0)
}

pub(crate) fn json(status: StatusCode, body: serde_json::Value) -> Response {
    (
        status,
        [("content-type", "application/json")],
        body.to_string(),
    )
        .into_response()
}

pub fn router(state: DisplayState) -> Router {
    const SETUP: &str = "shards/:shard_id/revisions/:digest/setup/:table/:segment";
    const QUERY: &str = "shards/:shard_id/revisions/:digest/query/:table";
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/txid/init", get(init))
        .route("/v1/txid/shards", get(shard_map))
        .route("/v1/txid/map", get(recent_map))
        .route("/v1/txid/map/:base/:digest", get(index_chunk))
        .route(
            "/v1/txid/shards/:shard_id/revisions/:digest/manifest",
            get(manifest),
        )
        .route(&format!("/v1/txid/archive/{SETUP}"), get(setup_archive))
        .route(&format!("/v1/txid/recent/{SETUP}"), get(setup_recent))
        .route(&format!("/v1/txid/archive/{QUERY}"), post(query_archive))
        .route(&format!("/v1/txid/recent/{QUERY}"), post(query_recent))
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
            "work_memory_reserved_bytes": cache.work_memory.reserved_bytes(),
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
        "ready": warm || inner.warm.mode == ReadinessMode::LoadedOnly,
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
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        state.metrics().render(&state.snapshot()),
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
        native_schema: transparent_shard::manifest::SCHEMA.into(),
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
                txpages: table(geometry, Table::TxPages),
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
        return RequestError::Stale { shard_id, digest }.into_response(&map_digest);
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
        match tokio::time::timeout(pending.remaining(), runtime.cache.get(key, shared, source))
            .await
        {
            Ok(Ok(handle)) => handle,
            Ok(Err(error)) => return RequestError::from(error).into_response(&map_digest),
            Err(_) => {
                Metrics::incr(&runtime.metrics.deadline_exceeded);
                return RequestError::from(AdmissionError::DeadlineExceeded)
                    .into_response(&map_digest);
            }
        }
    };
    pending.complete();
    let built = handle.get();
    let body = SetupResponse {
        shard_id,
        manifest_digest: digest,
        geometry: geometry.into(),
        table: table.label(),
        bucket: match table {
            DisplayTable::Directory(bucket) => Some(bucket),
            DisplayTable::Pages => None,
        },
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
    let mut timer = crate::metrics::QueryTimer::new(state.0.metrics().clone());
    let response = query_inner(state, sealed, path, request).await;
    timer.finish(response.status().is_success());
    response
}

/// The history query path (`service::query_inner`), addressed by tier,
/// revision and display table.
async fn query_inner(
    State(state): State<DisplayState>,
    sealed: bool,
    AxumPath((shard_id, digest, label)): QueryPath,
    request: Request,
) -> Response {
    let map_digest = state.inner.set.map_digest.clone();
    let runtime = state.runtime().clone();
    let metrics = runtime.metrics.clone();
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
            let refused = error.into_response(&map_digest);
            return state.refuse_unread(request, refused).await;
        }
    };

    // The exact length is known before a byte is read; anything else is
    // refused before it is buffered or queued.
    let expected = shared.query_bytes();
    let declared = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .or_else(|| {
            use http_body::Body as _;
            request.body().size_hint().exact()
        });
    let Some(declared) = declared else {
        Metrics::incr(&metrics.query_length_rejections);
        return RequestError::LengthRequired.into_response(&map_digest);
    };
    if declared != expected as u64 {
        Metrics::incr(&metrics.query_length_rejections);
        let refused = RequestError::Bad(format!(
            "a {label} query for geometry {} must be exactly {expected} bytes, not {declared}",
            shared.geometry.name
        ))
        .into_response(&map_digest);
        return state.refuse_unread(request, refused).await;
    }

    let pending = match runtime.admission.try_enter(Some(expected)) {
        Ok(pending) => pending,
        Err(error) => {
            Metrics::incr(&metrics.query_errors);
            let refused = RequestError::from(error).into_response(&map_digest);
            return state.refuse_unread(request, refused).await;
        }
    };
    let upload_deadline = runtime.admission.config().upload_deadline;
    let body = match tokio::time::timeout(
        upload_deadline,
        axum::body::to_bytes(request.into_body(), expected),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(error)) => {
            Metrics::incr(&metrics.query_errors);
            return RequestError::Bad(format!("reading the query body: {error}"))
                .into_response(&map_digest);
        }
        Err(_) => {
            Metrics::incr(&metrics.upload_timeouts);
            Metrics::incr(&metrics.query_errors);
            return RequestError::UploadTimeout.into_response(&map_digest);
        }
    };
    if body.len() != expected {
        Metrics::incr(&metrics.query_length_rejections);
        Metrics::incr(&metrics.query_errors);
        return RequestError::Bad(format!(
            "the query body is {} bytes where {expected} were declared",
            body.len()
        ))
        .into_response(&map_digest);
    }

    // Names the revision and the bucket: a body prepared for another bucket's
    // table is refused by `SharedParams::parse`, and the prefix comes back in
    // the response so a misrouted answer fails at the client too.
    let binding = display::query_binding(&digest, table);

    let admitted = match runtime.admission.wait_slot(pending).await {
        Ok(admitted) => admitted,
        Err(error) => {
            Metrics::incr(&metrics.query_errors);
            return RequestError::from(error).into_response(&map_digest);
        }
    };

    // Every segment's runtime is held before any is evaluated, so a tight
    // budget cannot evict one segment while another builds.
    let handles = if state.is_retired() {
        match state.resident(shard_id, &digest, table, segments(&sources)) {
            Ok(handles) => handles,
            Err(error) => {
                Metrics::incr(&metrics.query_errors);
                return error.into_response(&map_digest);
            }
        }
    } else {
        let mut handles = Vec::with_capacity(sources.len());
        for (segment, source) in sources.into_iter().enumerate() {
            let key = runtime_key(&digest, table, segment as u32);
            match tokio::time::timeout(
                admitted.remaining(),
                runtime.cache.get(key, shared.clone(), source),
            )
            .await
            {
                Ok(Ok(handle)) => handles.push(handle),
                Ok(Err(error)) => {
                    Metrics::incr(&metrics.query_errors);
                    return RequestError::from(error).into_response(&map_digest);
                }
                Err(_) => {
                    Metrics::incr(&metrics.deadline_exceeded);
                    Metrics::incr(&metrics.query_errors);
                    return RequestError::from(AdmissionError::DeadlineExceeded)
                        .into_response(&map_digest);
                }
            }
        }
        handles
    };

    let memory = match runtime
        .cache
        .work_memory
        .reserve_query(
            shared.reserved_bytes().saturating_mul(2),
            admitted.remaining(),
        )
        .await
    {
        Some(memory) => memory,
        None => {
            if admitted.remaining().is_zero() {
                Metrics::incr(&metrics.deadline_exceeded);
                Metrics::incr(&metrics.query_errors);
                return RequestError::from(AdmissionError::DeadlineExceeded)
                    .into_response(&map_digest);
            }
            Metrics::incr(&metrics.overloads);
            return RequestError::Overloaded.into_response(&map_digest);
        }
    };
    let evaluation_metrics = metrics.clone();
    let evaluated = tokio::task::spawn_blocking(move || {
        let _memory = memory;
        let _timer = evaluation_metrics.evaluation_seconds.timer();
        let query = shared.parse(binding, &body)?;
        let mut answer = Vec::with_capacity(shared.response_bytes() * handles.len());
        for handle in &handles {
            answer.extend(handle.get().answer(binding, &query)?);
        }
        admitted.complete();
        Ok::<_, String>(answer)
    })
    .await;
    match evaluated {
        Ok(Ok(answer)) => {
            Metrics::incr(&metrics.queries);
            (
                StatusCode::OK,
                [("content-type", "application/octet-stream")],
                answer,
            )
                .into_response()
        }
        Ok(Err(error)) => {
            Metrics::incr(&metrics.query_errors);
            RequestError::Bad(error).into_response(&map_digest)
        }
        Err(error) => {
            Metrics::incr(&metrics.query_errors);
            RequestError::Bad(format!("evaluation task failed: {error}")).into_response(&map_digest)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::WorkMemory;
    use crate::runtime::SharedParams;
    use crate::shardset::Table;
    use std::sync::Arc;
    use transparent_shard::display::TXID_2K;

    /// recent-01 runs the replica under `MemoryMax=1.5G`. Admission allows
    /// `current + held + new <= 0.9 * max`; a cold build holds four times a
    /// table's reservation and each query twice. Two concurrent directory
    /// queries during a recent rebuild must be admitted, or every rebuild
    /// would refuse lookups with 503.
    #[test]
    fn a_recent_rebuild_admits_two_concurrent_queries_under_recent_01_limits() {
        let reserved = SharedParams::build(&TXID_2K, Table::TxDirectory)
            .unwrap()
            .reserved_bytes();
        assert_eq!(reserved, 75_545_144, "a 2,048-row display table");
        assert_eq!(
            reserved,
            SharedParams::build(&TXID_2K, Table::TxPages)
                .unwrap()
                .reserved_bytes()
        );
        let limit = 1536 << 20;
        // Charged memory with a build in flight: the process, the active and
        // retired recent runtimes and the build's own allocation so far.
        let current = 700 << 20;
        let memory = Arc::new(WorkMemory::default());
        let sample = Some((current, Some(limit)));
        let build = memory.reserve_at(4 * reserved, sample).expect("build");
        let first = memory
            .reserve_at(2 * reserved, sample)
            .expect("first query");
        let second = memory
            .reserve_at(2 * reserved, sample)
            .expect("second query");
        // The headroom is real but not unbounded: a third query waits.
        assert!(memory.reserve_at(2 * reserved, sample).is_none());
        drop((build, first, second));
        assert_eq!(memory.reserved_bytes(), 0);
        // The 900M limit first proposed for recent-01 refuses the second
        // query even at a steady-state charge.
        let tight = Some((350 << 20, Some(900 << 20)));
        let _build = memory.reserve_at(4 * reserved, tight).expect("build");
        let _first = memory.reserve_at(2 * reserved, tight).expect("first query");
        assert!(memory.reserve_at(2 * reserved, tight).is_none());
    }
}
