//! Serving one display publication snapshot over HTTP.
//!
//! Queries go through the request path the history worker uses
//! ([`crate::transparent::request`]). What differs is the address: a revision is resolved
//! with its tier and this worker's role, a table is one bucket's entries, and
//! the binding names the bucket. Everything that bounds work is shared: the
//! runtime cache, admission, work-memory reservations and their status codes.

use zakura_pir_protocol::transparent::shard as transparent_shard;
use zakura_pir_query_server::transparent::native as transparent_native;

use crate::transparent::admission::{Admission, AdmissionConfig};
use crate::transparent::config::ServiceConfig;
pub(crate) use crate::transparent::request::json;
use crate::transparent::request::{answer_query, within, RequestError, Worker};
use axum::extract::{Path as AxumPath, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use serde::Serialize;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use transparent_shard::display;
use transparent_shard::layout::Geometry;
use transparent_shared::history::shardset::Table;
use transparent_shared::runtime::metrics::{Metrics, Snapshot};
use transparent_shared::runtime::{disk::DiskCache, Produced, RuntimeCache};
use transparent_shared::tx_detail::serving::{DisplayParams, DisplayServing};
use transparent_shared::tx_detail::set::DisplaySet;

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
    params: DisplayParams,
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
            params: DisplayParams::default(),
        })
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
    validation: std::sync::OnceLock<crate::transparent::validation::Scope>,
    /// The loaded set, its parameters and the shared runtime cache.
    serving: DisplayServing,
    runtime: Arc<DisplayRuntime>,
    warm: WarmState,
    prewarm_slots: usize,
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
    /// Prepare a serving snapshot without loading its runtimes. Required
    /// publisher material is checked by size before activation.
    pub fn build(set: DisplaySet, runtime: &Arc<DisplayRuntime>) -> Result<Self, String> {
        let serving = DisplayServing::new(
            set,
            &runtime.params,
            runtime.cache.clone(),
            runtime.metrics.clone(),
            runtime.required,
        )?;
        // Each held table at its built size, plus the bound for as many as
        // the prewarm can have in flight; see `runtime::warm_bytes`.
        let target = serving.warm_target_count();
        Metrics::set(&runtime.metrics.target_runtimes, target as u64);
        let prewarm_slots = runtime.cache.prewarm_concurrency();
        let state = Self {
            inner: Arc::new(Inner {
                validation: std::sync::OnceLock::new(),
                serving,
                runtime: runtime.clone(),
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
            }),
        };
        state.validation();
        Ok(state)
    }

    /// Warm current revisions in the background under the shared cache budget.
    pub fn spawn_prewarm(&self) -> tokio::task::JoinHandle<()> {
        let mut task = self.inner.warm.task.lock().unwrap();
        let state = self.clone();
        let running = tokio::spawn(async move {
            let inner = &state.inner;
            let started = std::time::Instant::now();
            let queue = Arc::new(Mutex::new(std::collections::VecDeque::from(
                inner.serving.set().warm_targets(),
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
                        if state.validation().unavailable(&digest) { continue; }
                        if inner.warm.stopped.load(Ordering::Acquire) { break; }
                        let Some(produced) = inner
                            .serving
                            .warm(&digest, table, segment, &inner.warm.cancelled)
                            .await
                        else {
                            continue;
                        };
                        match produced
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
                map = %inner.serving.set().map_digest,
                "display prewarm finished"
            );
        });
        *task = Some(running.abort_handle());
        running
    }

    pub(crate) fn artifact_validation(&self) -> serde_json::Value {
        self.validation().status()
    }

    /// Whether background artifact validation left some current revision
    /// serving, as provider `/v1/ready` reports it.
    pub fn serves_current_artifacts(&self) -> bool {
        self.validation().serves_current()
    }

    pub(crate) fn start_validation(&self) {
        self.validation().activate();
    }

    fn validation(&self) -> &crate::transparent::validation::Scope {
        self.inner.validation.get_or_init(|| {
            let mut scope = crate::transparent::validation::Scope::default();
            for revision in self
                .inner
                .serving
                .set()
                .current()
                .iter()
                .filter(|r| r.held())
            {
                scope.current(revision.manifest.shard_id, &revision.digest);
            }
            // A wallet can still request a retained predecessor by its digest.
            for artifact in self.inner.serving.artifacts() {
                scope.add(
                    &artifact.digest,
                    artifact.shard_id,
                    &artifact.path,
                    &self.inner.runtime.cache,
                    &artifact.key,
                );
            }
            scope
        })
    }

    pub fn set(&self) -> &DisplaySet {
        self.inner.serving.set()
    }

    pub fn runtime(&self) -> &Arc<DisplayRuntime> {
        &self.inner.runtime
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.inner.runtime.metrics
    }

    /// The largest body any geometry of this set can legitimately send.
    pub fn max_query_bytes(&self) -> usize {
        self.inner.serving.max_query_bytes()
    }

    pub(crate) fn resident_targets(&self) -> usize {
        self.inner.serving.resident_targets()
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
                .serving
                .set()
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
        self.inner.serving.retire();
        self.stop_warming(None);
    }

    pub fn is_retired(&self) -> bool {
        self.inner.serving.is_retired()
    }

    fn snapshot(&self) -> Snapshot {
        let inner = &self.inner;
        Snapshot {
            labels: vec![
                ("map_sha256".into(), inner.serving.set().map_digest.clone()),
                ("role".into(), inner.serving.set().role.as_str().into()),
            ],
            shards: inner.serving.set().map.shards.len() as u64,
            assigned_shards: inner
                .serving
                .set()
                .current()
                .iter()
                .filter(|r| r.held())
                .count() as u64,
            revisions: inner.serving.set().revisions().len() as u64,
            prunable_revisions: inner.serving.set().excess.len() as u64,
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
            map_digest: &self.inner.serving.set().map_digest,
        }
    }
}

/// The revision named is no longer served: the wallet refreshes its txid map.
fn stale(shard_id: u64, digest: &str) -> RequestError {
    transparent_shared::tx_detail::serving::stale(shard_id, digest).into()
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
    let private = Router::new()
        .route("/v1/health", get(health))
        .route("/v1/ready", get(ready))
        .route("/metrics", get(metrics));
    crate::transparent::PUBLIC_ROUTES
        .mount(
            private,
            [
                ("/txid/init", get(init)),
                ("/txid/shards", get(shard_map)),
                ("/txid/map", get(recent_map)),
                ("/txid/map/:base/:digest", get(index_chunk)),
                (
                    "/txid/shards/:shard_id/revisions/:digest/manifest",
                    get(manifest),
                ),
                (
                    "/txid/archive/shards/:shard_id/revisions/:digest/setup/:table/:segment",
                    get(setup_archive),
                ),
                (
                    "/txid/recent/shards/:shard_id/revisions/:digest/setup/:table/:segment",
                    get(setup_recent),
                ),
                (
                    "/txid/archive/shards/:shard_id/revisions/:digest/query/:table",
                    post(query_archive),
                ),
                (
                    "/txid/recent/shards/:shard_id/revisions/:digest/query/:table",
                    post(query_recent),
                ),
            ],
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            validate_request,
        ))
        .with_state(state)
}

async fn validate_request(
    State(state): State<DisplayState>,
    request: Request,
    next: axum::middleware::Next,
) -> Response {
    state.validation().serve(request, next).await
}

async fn health(State(state): State<DisplayState>) -> Response {
    let inner = &state.inner;
    let cache = &inner.runtime.cache;
    json(
        StatusCode::OK,
        serde_json::json!({
            "phase": "serving",
            "role": inner.serving.set().role.as_str(),
            "map_sha256": inner.serving.set().map_digest,
            "shards": inner.serving.set().map.shards.len(),
            "held_revisions": inner.serving.set().revisions().iter().filter(|r| r.held()).count(),
            "geometries": inner.serving.set().geometries().iter().map(|g| g.name).collect::<Vec<_>>(),
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

/// Readiness is independent of background warming and reads no artifact bodies.
async fn ready(State(state): State<DisplayState>) -> Response {
    let inner = &state.inner;
    let warm = state.is_warm();
    let mut body = serde_json::json!({
        "artifact_validation": state.validation().status(),
        "ready": state.is_ready(),
        "role": inner.serving.set().role.as_str(),
        "map_sha256": inner.serving.set().map_digest,
        "warm": warm,
        "warm_runtimes": inner.warm.count.load(Ordering::Acquire),
        "target_runtimes": inner.warm.target,
        "prewarm_finished": inner.warm.finished.load(Ordering::Acquire),
        "prewarm_failed": Metrics::get(&inner.runtime.metrics.prewarm_failed),
        "covered_through": inner.serving.set().map.covered_through(),
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
        state.metrics().render(&snapshot)
            + &state.metrics().render_display(&snapshot)
            + &state.validation().metrics(),
    )
        .into_response()
}

async fn init(State(state): State<DisplayState>) -> Response {
    let inner = &state.inner;
    let set = &inner.serving.set();
    let table = |geometry: &'static Geometry, kind: Table| {
        let shared = inner
            .serving
            .params(geometry.name, kind)
            .expect("every named geometry has parameters");
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
    let set = &state.inner.serving.set();
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
    let split = &state.inner.serving.set().split;
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
    let map_digest = state.inner.serving.set().map_digest.clone();
    let Some((chunk_base, bytes)) = state.inner.serving.set().chunk(&digest) else {
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
    let map_digest = state.inner.serving.set().map_digest.clone();
    let Some(revision) = state.inner.serving.set().revision(&digest) else {
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

async fn setup(
    State(state): State<DisplayState>,
    sealed: bool,
    AxumPath((shard_id, digest, label, segment)): SetupPath,
) -> Response {
    let map_digest = state.inner.serving.set().map_digest.clone();
    let runtime = state.runtime().clone();
    // Counted as a waiter while the runtime is acquired or built, as for a
    // history setup request.
    let pending = match runtime.admission.try_enter(None) {
        Ok(pending) => pending,
        Err(error) => return RequestError::from(error).into_response(&map_digest),
    };
    let prepared = state
        .inner
        .serving
        .prepare_setup(sealed, shard_id, &digest, &label, segment)
        .map_err(RequestError::from);
    let setup = match prepared {
        Ok(prepared) => {
            let load = async { prepared.load().await.map_err(RequestError::from) };
            within(pending.remaining(), &runtime.metrics, load).await
        }
        Err(error) => Err(error),
    };
    let setup = match setup {
        Ok(setup) => setup,
        Err(error) => return error.into_response(&map_digest),
    };
    pending.complete();
    let built = setup.runtime.get();
    let body = SetupResponse {
        shard_id,
        manifest_digest: digest,
        geometry: setup.geometry.into(),
        table: setup.table,
        bucket: setup.bucket.expect("a display table names its bucket"),
        segment: setup.segment,
        segments: setup.segments,
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
    // A retired snapshot is refused before the body is queued if a segment
    // has already gone; the check is repeated when the handles are taken.
    let prepared = state
        .inner
        .serving
        .prepare_query(sealed, shard_id, &digest, &label);
    let query = match prepared {
        Ok(query) => query,
        Err(error) => {
            Metrics::incr(&metrics.query_errors);
            let refused = RequestError::from(error).into_response(map_digest);
            return worker.refuse_unread(request, refused).await;
        }
    };
    answer_query(worker, request, query).await
}

#[cfg(test)]
mod retirement_tests {
    use super::*;
    use std::{
        convert::Infallible,
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };
    use transparent_shared::tx_detail::{kind, synth, DisplayRole};
    use zakura_pir_protocol::transparent::shard::display::{
        DisplaySealParams, DisplayTable, TXID_2K8,
    };

    struct Upload(tokio::sync::mpsc::Receiver<axum::body::Bytes>);

    impl http_body::Body for Upload {
        type Data = axum::body::Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            self.0
                .poll_recv(context)
                .map(|frame| frame.map(|bytes| Ok(http_body::Frame::data(bytes))))
        }
    }

    /// Upload can outlive activation of a successor. Once the old snapshot
    /// is retired it must refuse a cold runtime, rather than build it using
    /// the next candidate's cache and build capacity.
    #[tokio::test]
    async fn retirement_during_upload_refuses_a_cold_runtime() {
        let root = tempfile::tempdir().unwrap();
        let shard = synth::write_shard(
            root.path(),
            &synth::ShardSpec {
                shard_id: 0,
                start_height: 10,
                end_height: 19,
                sealed: false,
                revision: 0,
                supersedes: String::new(),
                parent_manifest_digest: String::new(),
                n_buckets: 1,
                archive_target: 1,
                geometry: &TXID_2K8,
            },
            &synth::records(4, 7),
        )
        .unwrap();
        let (dir, _) = synth::write_candidate(
            root.path(),
            &DisplaySealParams {
                n_archive: 1,
                n_recent: 1,
                archive_target: 1,
                recent_window: 1,
                reorg_margin: 1,
            },
            std::slice::from_ref(&shard),
            "upload",
        )
        .unwrap();
        let runtime = DisplayRuntime::new(ServiceConfig::default(), None);
        let state = DisplayState::build(
            DisplaySet::open(&dir, 3, DisplayRole::Combined).unwrap(),
            &runtime,
        )
        .unwrap();
        let table = DisplayTable::parse("directory-0").unwrap();
        let params = state
            .inner
            .serving
            .params(TXID_2K8.name, kind(table))
            .unwrap();
        let mut body = vec![0; params.query_bytes()];
        body[..8].copy_from_slice(&display::query_binding(&shard.digest, table));
        let (send, receive) = tokio::sync::mpsc::channel(1);
        send.send(axum::body::Bytes::copy_from_slice(&body[..8]))
            .await
            .unwrap();
        let request = Request::post("/")
            .header("content-length", body.len())
            .body(axum::body::Body::new(Upload(receive)))
            .unwrap();
        let response = query_inner(
            State(state.clone()),
            false,
            AxumPath((0, shard.digest, table.label())),
            request,
        );
        tokio::pin!(response);
        // Poll through resolution and into the unfinished upload, without
        // depending on a timer or another task's scheduling.
        std::future::poll_fn(|context| {
            assert!(response.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        state.retire();
        assert_eq!(runtime.cache.entries(), 0);
        send.send(axum::body::Bytes::copy_from_slice(&body[8..]))
            .await
            .unwrap();
        drop(send);
        assert_eq!(response.await.status(), StatusCode::CONFLICT);
        assert_eq!(
            runtime.cache.entries(),
            0,
            "a retired upload built a runtime"
        );
    }
}
