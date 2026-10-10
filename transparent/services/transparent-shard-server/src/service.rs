//! Serving N shard revisions from one process.
//!
//! **Parameters are shared per geometry, published data is not.** Every shard
//! naming one geometry shares one native parameter set per table — query masks
//! and packing setup included — because the native profile is a function of
//! schema, geometry and table alone. A set mixing archive and recent shards
//! therefore leaks two parameter sets per geometry rather than two per shard,
//! and a client validates each once. The published masks do *not* follow,
//! because they are derived from each segment's own database — so a client
//! validates parameters once and fetches setup per segment.
//!
//! **Runtimes are built on demand and bounded.** See [`crate::runtime`]: the
//! cache reserves before it builds, evicts what nothing is holding, and refuses
//! work it cannot make room for rather than exceeding its budget.
//!
//! **Requests name a revision.** A growing tail is republished as a new
//! manifest digest beside the old one, and both may be held. A query carries
//! the digest it was prepared against, in its path and in its body's fixed
//! prefix, so a wallet that fetched setup from one revision cannot be answered
//! from another's bytes. A revision this worker no longer holds is refused with
//! `409` and the current map digest, which tells the wallet to refresh the map
//! and re-derive that range — the behaviour its provisional-coverage contract
//! already expects — rather than silently accepting rows for a range it did not
//! ask about.
//!
//! **A shard's segments are answered together.** A shard whose content did not
//! fit one segment of its geometry has several, and a query names the row
//! within a segment, never the segment. One request is evaluated against every
//! segment and the results are returned in segment order, so which segment
//! holds the selected script — a function of the script — is not something the
//! wallet has to disclose in order to ask.
//!
//! **It also serves the public range filters**, which an earlier revision
//! deliberately refused to do. See `shardset` for why that rule was relaxed and
//! what is kept: the filter service still serves the same bytes at the same
//! paths on its own host, so two origins remain available to a wallet that
//! wants them.

use crate::admission::{Admission, AdmissionConfig, AdmissionError};
use crate::metrics::{Metrics, Snapshot};
use crate::runtime::{warm_bytes, CacheError, RuntimeCache, RuntimeHandle, SharedParams};
use crate::shardset::{LoadedShard, ShardSet, Table};
use axum::extract::{Path as AxumPath, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use transparent_shard::manifest::query_binding;

/// How the worker is sized. Everything here is per process, not per shard.
#[derive(Clone, Copy, Debug)]
pub struct ServiceConfig {
    /// Bytes of prepared runtime the cache may reserve.
    ///
    /// Set below the host's real headroom: the reservation counts the database
    /// and pack matrices and nothing else, so allocator overhead, the transient
    /// plaintext a build reads, and in-flight requests all come out of the
    /// remainder.
    pub cache_bytes: u64,
    /// Runtime builds that may run at once.
    ///
    /// One by default. A build is a second of CPU and a few hundred megabytes
    /// of transient allocation; running several concurrently is how a worker
    /// inside its steady-state budget still dies during a burst of cold
    /// requests.
    pub build_slots: usize,
    /// Query evaluations that may run at once.
    ///
    /// Two by default, because evaluation is memory-bandwidth-bound and
    /// `shard-scaling` measured it saturating at two threads: one core takes 66
    /// of the ~87 GiB/s the kernel reaches and sixteen return 1.39x one core.
    /// Admitting more would add queueing without adding throughput, and would
    /// hold more runtimes pinned against eviction while it did.
    pub query_slots: usize,
    /// Requests that may wait beyond those running; see [`AdmissionConfig`].
    pub max_waiters: usize,
    /// Query body bytes that may be buffered at once.
    pub max_body_bytes: u64,
    /// How long a client has to deliver a query body once admitted.
    pub upload_deadline: std::time::Duration,
    /// How long a request may wait in total before a retryable refusal.
    pub query_deadline: std::time::Duration,
    /// What `/v1/ready` attests; see [`ReadinessMode`].
    pub readiness: ReadinessMode,
}

/// What readiness means for this process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadinessMode {
    /// Ready once a set is loaded. Runtimes build on demand, so a cold worker
    /// is serving correctly and slowly. The correctness pilot's mode, and the
    /// whole-set default.
    LoadedOnly,
    /// Ready once every assigned runtime is warm. A worker in a fleet is held
    /// out of rotation until it can answer at full speed, which is what lets
    /// a restarting replica rejoin without a wallet meeting its cold cache.
    Warm,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        let admission = AdmissionConfig::default();
        Self {
            cache_bytes: 4 << 30,
            build_slots: 1,
            query_slots: admission.query_slots,
            max_waiters: admission.max_waiters,
            max_body_bytes: admission.max_body_bytes,
            upload_deadline: admission.upload_deadline,
            query_deadline: admission.query_deadline,
            readiness: ReadinessMode::LoadedOnly,
        }
    }
}

/// The prewarm's progress toward holding every assigned runtime.
struct WarmState {
    mode: ReadinessMode,
    target: usize,
    finished: AtomicBool,
    cancelled: AtomicBool,
    count: std::sync::atomic::AtomicU64,
    pins: std::sync::Mutex<Vec<(String, RuntimeHandle)>>,
}

impl ServiceConfig {
    fn admission(&self) -> AdmissionConfig {
        AdmissionConfig {
            query_slots: self.query_slots,
            max_waiters: self.max_waiters,
            max_body_bytes: self.max_body_bytes,
            upload_deadline: self.upload_deadline,
            query_deadline: self.query_deadline,
        }
    }
}

/// A runtime's address before the digest is resolved.
type ParamsKey = (&'static str, Table);

pub struct Inner {
    set: ShardSet,
    /// One parameter set per geometry the loaded set actually uses, per table.
    params: HashMap<ParamsKey, Arc<SharedParams>>,
    cache: Arc<RuntimeCache>,
    admission: Arc<Admission>,
    metrics: Arc<Metrics>,
    max_query_bytes: usize,
    warm: WarmState,
    /// Prewarm jobs; the cache separately bounds cold builds and disk restores.
    prewarm_slots: usize,
}

#[derive(Clone)]
pub struct ServiceState {
    inner: Arc<Inner>,
}

/// One geometry's public parameters, as `GET /v1/shards/init` publishes them.
///
/// The dimensions are published beside the derived native parameter identity,
/// not instead of it. A client re-derives the identity — profile, bit widths,
/// query-mask seed and packing setup — from the schema, geometry, table, `rows`
/// and `row_bytes`, and refuses to proceed unless it reproduces what the
/// service sent, so the service cannot choose parameters for it — including
/// parameters that would leak the selection.
///
/// Each table publishes two schemes. `*_scheme` is the 49-bit query, which
/// wallets deployed before dithering read and send; they ignore the keys they
/// do not know. `*_scheme_dq44` is the 44-bit dithered query over the same
/// setup, which a newer wallet sends when it reproduces the scheme. The
/// service accepts either, by the body's exact length.
#[derive(Serialize)]
pub struct GeometryInit {
    pub name: String,
    pub directory_rows: u64,
    pub directory_row_bytes: u32,
    pub directory_scheme: transparent_native::NativeScheme,
    pub directory_scheme_dq44: transparent_native::NativeScheme,
    pub directory_setup_seed: u64,
    pub page_rows: u64,
    pub page_row_bytes: u32,
    pub pages_scheme: transparent_native::NativeScheme,
    pub pages_scheme_dq44: transparent_native::NativeScheme,
    pub pages_setup_seed: u64,
}

/// What `GET /v1/shards/init` returns.
#[derive(Serialize)]
pub struct InitResponse {
    pub schema: String,
    /// The range filter profile, not the table geometry.
    pub profile: String,
    pub network: String,
    pub genesis_hash: String,
    pub shards: usize,
    pub start_height: u64,
    pub covered_through: u64,
    /// Digest of the map bytes these geometries describe.
    ///
    /// A wallet that took the map from the filter service and its parameters
    /// from here can check the two are the same publication.
    pub map_sha256: String,
    /// One entry per geometry this worker holds, not per registered geometry.
    pub geometries: Vec<GeometryInit>,
    /// Shards this worker holds tables for. Equal to `shards` in whole-set mode.
    pub assigned_shards: usize,
    pub worker_id: Option<String>,
    pub role: Option<String>,
    pub assignment_sha256: Option<String>,
}

/// What the setup route returns, per segment.
#[derive(Serialize)]
pub struct SetupResponse {
    pub shard_id: u64,
    pub manifest_digest: String,
    pub geometry: String,
    pub table: String,
    pub segment: u32,
    pub segments: u32,
    pub public_params: String,
    pub public_params_sha256: String,
    pub public_params_epoch: String,
}

impl ServiceState {
    pub fn build(set: ShardSet, config: ServiceConfig) -> Result<Self, String> {
        Self::build_with_disk(set, config, None)
    }

    pub fn build_with_disk(
        set: ShardSet,
        config: ServiceConfig,
        disk: Option<crate::runtime::disk::DiskCache>,
    ) -> Result<Self, String> {
        Self::build_reusing(set, config, disk, None)
    }

    pub(crate) fn build_reusing(
        set: ShardSet,
        config: ServiceConfig,
        disk: Option<crate::runtime::disk::DiskCache>,
        previous: Option<&Self>,
    ) -> Result<Self, String> {
        let _ = pir_control::Identity::process();
        let metrics = previous
            .map(|s| s.inner.metrics.clone())
            .unwrap_or_else(|| Arc::new(Metrics::default()));
        // Every geometry the set names, held here or not: the init document
        // is set-wide and a wallet refuses a map it does not fully declare.
        // Not every registered geometry, which would publish parameters for
        // shapes no shard in the set uses.
        let mut params: HashMap<ParamsKey, Arc<SharedParams>> = HashMap::new();
        let mut max_query_bytes = 0usize;
        for geometry in set.geometries() {
            for table in Table::HISTORY {
                let shared =
                    match previous.and_then(|s| s.inner.params.get(&(geometry.name, table))) {
                        Some(shared) => shared.clone(),
                        None => Arc::new(SharedParams::build(geometry, table)?),
                    };
                max_query_bytes = max_query_bytes.max(shared.query_bytes());
                params.insert((geometry.name, table), shared);
            }
        }
        if params.is_empty() {
            return Err("the shard set names no geometry to serve".into());
        }
        let target = set
            .current()
            .map(|s| {
                Table::HISTORY
                    .iter()
                    .map(|t| s.segments(*t) as usize)
                    .sum::<usize>()
            })
            .sum();
        let cache = previous.map(|s| s.inner.cache.clone()).unwrap_or_else(|| {
            Arc::new(
                RuntimeCache::new(config.cache_bytes, config.build_slots, metrics.clone())
                    .with_disk(disk),
            )
        });
        // What every current assigned runtime holds together once built, plus
        // the bound for as many as the prewarm can have in flight. In warm
        // mode that has to fit the cache with nothing evicted: an assignment
        // that needs eviction to be served is a worker that thrashes, and it
        // is refused here rather than discovered under load.
        let mut assigned = Vec::new();
        for shard in set.current() {
            for table in Table::HISTORY {
                for _ in 0..shard.segments(table) {
                    assigned.push(params[&(shard.geometry.name, table)].as_ref());
                }
            }
        }
        let assigned_bytes = warm_bytes(assigned, cache.prewarm_concurrency());
        if config.readiness == ReadinessMode::Warm && assigned_bytes > config.cache_bytes {
            return Err(format!(
                "the assignment needs {assigned_bytes} bytes of runtimes, the cache budget is {}",
                config.cache_bytes
            ));
        }
        Metrics::set(&metrics.target_runtimes, target as u64);
        let prewarm_slots = cache.prewarm_concurrency();
        Ok(Self {
            inner: Arc::new(Inner {
                set,
                params,
                cache,
                admission: previous
                    .map(|s| s.inner.admission.clone())
                    .unwrap_or_else(|| {
                        Arc::new(Admission::new(config.admission(), metrics.clone()))
                    }),
                metrics,
                max_query_bytes,
                warm: WarmState {
                    mode: config.readiness,
                    target,
                    finished: AtomicBool::new(false),
                    cancelled: AtomicBool::new(false),
                    count: std::sync::atomic::AtomicU64::new(0),
                    pins: std::sync::Mutex::new(Vec::new()),
                },
                prewarm_slots,
            }),
        })
    }

    /// Builds every runtime this worker should hold, in the order the set
    /// prescribes, and marks the worker warm when done.
    ///
    /// Only current assigned revisions are warmed. Retained revisions remain
    /// available on demand without consuming the next build's headroom. A
    /// request arriving during the prewarm for a runtime it has not reached
    /// yet is served the ordinary way, and the prewarm finds that runtime
    /// resident when it gets there.
    pub fn spawn_prewarm(&self) -> tokio::task::JoinHandle<()> {
        let state = self.clone();
        tokio::spawn(async move {
            let inner = &state.inner;
            let started = std::time::Instant::now();
            let current: std::collections::BTreeSet<String> = inner
                .set
                .current()
                .map(|shard| shard.digest.clone())
                .collect();
            // Keep enough jobs active to use the restore slots. Cache misses
            // still queue behind the independent, smaller cold-build limit.
            let queue = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from(
                inner
                    .set
                    .warm_targets()
                    .into_iter()
                    .filter(|(digest, _, _)| current.contains(digest))
                    .collect::<Vec<_>>(),
            )));
            let current = Arc::new(current);
            let mut workers = tokio::task::JoinSet::new();
            for _ in 0..inner.prewarm_slots {
                let state = state.clone();
                let queue = queue.clone();
                let current = current.clone();
                workers.spawn(async move {
                    let inner = &state.inner;
                    loop {
                        if inner.warm.cancelled.load(Ordering::Acquire) {break;}
                        let next = queue.lock().unwrap().pop_front();
                        let Some((digest, table, segment)) = next else {
                            break;
                        };
                        let Some(shard) = inner.set.revision(&digest) else {
                            continue;
                        };
                        let shared = state.shared(shard, table);
                        let Some(source) = shard.segment(table, segment).cloned() else {
                            continue;
                        };
                        match crate::prewarm::retry(
                            &inner.warm.cancelled,
                            std::time::Duration::from_secs(30),
                            || inner.cache.get((digest.clone(), table, segment), shared.clone(), source.clone()),
                        ).await
                        {
                            Ok(handle) => {
                                if inner.warm.cancelled.load(Ordering::Acquire) {break;}
                                if current.contains(&digest) {
                                    inner.warm.count.fetch_add(1, Ordering::Release);
                                    if inner.warm.mode == ReadinessMode::Warm {
                                        inner.warm.pins.lock().unwrap().push((digest.clone(), handle));
                                    }
                                }
                                Metrics::incr(&inner.metrics.warm_runtimes);
                            }
                            Err(error) => {
                                tracing::warn!(shard = %digest, table = table.as_str(), segment, %error, "prewarm failed");
                                Metrics::incr(&inner.metrics.prewarm_failed);
                            }
                        }
                    }
                });
            }
            while workers.join_next().await.is_some() {}
            Metrics::set(
                &inner.metrics.prewarm_micros,
                started.elapsed().as_micros() as u64,
            );
            inner.warm.finished.store(true, Ordering::Release);
            tracing::info!(
                warm = inner.warm.count.load(Ordering::Acquire),
                target = inner.warm.target,
                failed = Metrics::get(&inner.metrics.prewarm_failed),
                seconds = started.elapsed().as_secs_f64(),
                "prewarm finished"
            );
        })
    }

    /// The worker's identity for reports: assignment digest, id and role when
    /// it loaded under an assignment.
    pub(crate) fn has_other_holders(&self) -> bool {
        Arc::strong_count(&self.inner) > 1
    }
    pub(crate) fn release_pins(&self) {
        self.inner.warm.pins.lock().unwrap().clear();
    }

    pub(crate) fn evict_unpinned(&self) {
        self.inner.cache.evict_unpinned();
    }

    pub(crate) fn prune_disk(
        &self,
        keep: &std::collections::HashSet<String>,
    ) -> Result<Option<u64>, String> {
        self.inner.cache.prune_disk(keep)
    }

    pub(crate) fn release_invalidated(&self, digests: &std::collections::BTreeSet<String>) {
        if self
            .inner
            .set
            .current()
            .any(|s| digests.contains(&s.digest))
        {
            self.inner.warm.cancelled.store(true, Ordering::Release);
        }
        self.inner
            .warm
            .pins
            .lock()
            .unwrap()
            .retain(|(digest, _)| !digests.contains(digest));
    }

    pub(crate) fn set(&self) -> &ShardSet {
        &self.inner.set
    }

    pub(crate) fn is_invalidated(&self) -> bool {
        self.inner.warm.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn is_warm(&self) -> bool {
        !self.inner.warm.cancelled.load(Ordering::Acquire)
            && self.inner.warm.finished.load(Ordering::Acquire)
            && self.inner.warm.count.load(Ordering::Acquire) as usize >= self.inner.warm.target
    }

    fn identity(&self) -> serde_json::Value {
        match self.inner.set.scope() {
            Some(scope) => serde_json::json!({
                "worker_id": scope.worker_id,
                "role": scope.role.as_str(),
                "replica_group": scope.replica_group,
                "assignment_sha256": scope.assignment_sha256,
                "worker_assignment_sha256": scope.worker_assignment_sha256,
            }),
            None => serde_json::json!({
                "worker_id": null,
                "role": null,
                "replica_group": null,
                "assignment_sha256": null,
                "worker_assignment_sha256": null,
            }),
        }
    }

    fn snapshot(&self) -> Snapshot {
        let inner = &self.inner;
        let mut labels = vec![("map_sha256".to_string(), inner.set.map_digest.clone())];
        if let Some(scope) = inner.set.scope() {
            labels.push(("worker_id".to_string(), scope.worker_id.clone()));
            labels.push(("role".to_string(), scope.role.as_str().to_string()));
            labels.push((
                "assignment_sha256".to_string(),
                scope.assignment_sha256.clone(),
            ));
        }
        Snapshot {
            labels,
            shards: inner.set.len() as u64,
            assigned_shards: inner.set.assigned_len() as u64,
            revisions: inner.set.revisions().len() as u64,
            prunable_revisions: inner.set.prunable().len() as u64,
            cache_budget_bytes: inner.cache.budget(),
            warm_runtimes: inner.warm.count.load(Ordering::Acquire),
            work_memory_reserved_bytes: inner.cache.work_memory.reserved_bytes(),
            process_rss_bytes: crate::procmem::process_rss_bytes(),
            process_cpu: crate::procmem::process_cpu(),
            cgroup_memory_bytes: crate::procmem::cgroup_memory_bytes(),
        }
    }

    /// The largest body any geometry this worker serves can legitimately send.
    ///
    /// The request body limit. A mixed set is bounded by its widest geometry,
    /// so a narrower one is not rejected for being under it.
    pub fn max_query_bytes(&self) -> usize {
        self.inner.max_query_bytes
    }

    /// Sends a query refusal decided before the body was read, after reading
    /// and discarding the body.
    ///
    /// A proxy streams the upload while the worker answers. Answering and
    /// closing with the upload unread tears the proxy's upload down mid-body
    /// ("broken pipe"): the wallet then gets the proxy's 502 rather than this
    /// refusal and its retry delay, and the proxy counts an error against the
    /// worker's health. On the 2026-09-27 bench fleet such errors from
    /// saturated workers ejected the whole pool.
    ///
    /// Discarding costs bandwidth, not memory: frames are dropped as they
    /// arrive, at most [`Self::max_query_bytes`] are accepted, and the wait is
    /// bounded. A body that is longer, slower or broken is abandoned, and the
    /// connection closes as it did before.
    async fn refuse_unread(&self, request: Request, refused: Response) -> Response {
        use http_body::Body as _;
        let limit = self.max_query_bytes();
        let wait = self
            .inner
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
            Metrics::incr(&self.inner.metrics.refusals_unread);
        }
        refused
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.inner.metrics
    }

    /// Holds one evaluation slot until the permit is dropped.
    ///
    /// For tests that need the worker busy without answering anything, so the
    /// bounds in front of the slot can be exercised.
    pub async fn hold_query_slot(&self) -> crate::admission::HeldSlot {
        self.inner.admission.hold_slot().await
    }

    fn shared(&self, shard: &LoadedShard, table: Table) -> Arc<SharedParams> {
        self.inner
            .params
            .get(&(shard.geometry.name, table))
            .expect("every held geometry has parameters")
            .clone()
    }

    /// Resolves a revision the request named.
    ///
    /// A digest this worker does not hold is not a malformed request: it is a
    /// wallet holding a revision that has since been superseded and pruned.
    /// Distinguishing the two is what lets the client refresh its map and
    /// re-derive, instead of treating a routine tail republication as a bug.
    fn revision(&self, shard_id: u64, digest: &str) -> Result<&LoadedShard, RequestError> {
        match self.inner.set.revision(digest) {
            Some(shard) if shard.manifest.shard_id == shard_id => Ok(shard),
            // A digest that exists but belongs to another shard is a routing
            // error, not a stale client.
            Some(_) => Err(RequestError::Bad(format!(
                "revision {digest} is not a revision of shard {shard_id}"
            ))),
            None => {
                // A shard the map names but this worker is not assigned is a
                // routing fault, and is said so: the wallet's map is not
                // stale, and refreshing it would not help.
                if self.inner.set.names(shard_id) && !self.inner.set.is_assigned(shard_id) {
                    Metrics::incr(&self.inner.metrics.unassigned_refusals);
                    return Err(RequestError::NotAssigned {
                        shard_id,
                        worker_id: self
                            .inner
                            .set
                            .scope()
                            .map(|scope| scope.worker_id.clone())
                            .unwrap_or_default(),
                    });
                }
                Metrics::incr(&self.inner.metrics.stale_revisions);
                Err(RequestError::Stale {
                    shard_id,
                    digest: digest.to_string(),
                })
            }
        }
    }

    async fn runtime(
        &self,
        shard: &LoadedShard,
        table: Table,
        segment: u32,
    ) -> Result<RuntimeHandle, RequestError> {
        let source = shard.segment(table, segment).cloned().ok_or_else(|| {
            RequestError::Bad(format!(
                "shard {} has no {} segment {segment}",
                shard.manifest.shard_id,
                table.as_str()
            ))
        })?;
        let key = (shard.digest.clone(), table, segment);
        self.inner
            .cache
            .get(key, self.shared(shard, table), source)
            .await
            .map_err(|error| match error {
                CacheError::Overloaded => RequestError::Overloaded,
                CacheError::Failed(message) => RequestError::Bad(message),
            })
    }
}

/// Why a request could not be answered.
enum RequestError {
    Bad(String),
    /// The revision named is not held. Retryable after a map refresh.
    Stale {
        shard_id: u64,
        digest: String,
    },
    /// No cache capacity could be freed. Retryable as-is.
    Overloaded,
    /// A query arrived without saying how long it is.
    LengthRequired,
    /// The worker is at a bound it will not exceed. Retryable as-is; the body
    /// names which bound.
    Busy(&'static str),
    /// The body did not arrive in time. The client is the slow party.
    UploadTimeout,
    /// The shard exists but this worker does not hold its tables. A routing
    /// fault; not retryable here and not a reason to refresh the map.
    NotAssigned {
        shard_id: u64,
        worker_id: String,
    },
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

impl RequestError {
    fn into_response(self, map_digest: &str) -> Response {
        match self {
            RequestError::Bad(message) => json(
                StatusCode::BAD_REQUEST,
                serde_json::json!({ "error": message }),
            ),
            RequestError::LengthRequired => json(
                StatusCode::LENGTH_REQUIRED,
                serde_json::json!({ "error": "a query must declare its length" }),
            ),
            RequestError::UploadTimeout => json(
                StatusCode::REQUEST_TIMEOUT,
                serde_json::json!({ "error": "the query body did not arrive in time" }),
            ),
            // 421: the request reached a server that is not configured to
            // answer it. No retry delay, so a wallet does not spin on it, and
            // the map digest so an operator can see which set the worker holds.
            RequestError::NotAssigned {
                shard_id,
                worker_id,
            } => json(
                StatusCode::MISDIRECTED_REQUEST,
                serde_json::json!({
                    "error": format!("shard {shard_id} is not assigned to worker {worker_id}"),
                    "map_sha256": map_digest,
                }),
            ),
            // The same shape as the cache refusal: 503 with a delay, which is
            // what the wallet keys its retry on. The body says which bound.
            RequestError::Busy(reason) => (
                StatusCode::SERVICE_UNAVAILABLE,
                [("content-type", "application/json"), ("retry-after", "1")],
                serde_json::json!({
                    "error": reason,
                    "retry": "retry shortly",
                })
                .to_string(),
            )
                .into_response(),
            // 409 rather than 404: the shard exists and the wallet's request is
            // well formed, but it is addressed to a publication that has been
            // replaced. The current map digest is included so the client can
            // tell whether refreshing will actually help.
            RequestError::Stale { shard_id, digest } => json(
                StatusCode::CONFLICT,
                serde_json::json!({
                    "error": format!(
                        "revision {digest} of shard {shard_id} is no longer served"
                    ),
                    "retry": "refresh the shard map and re-derive coverage for this shard",
                    "map_sha256": map_digest,
                }),
            ),
            RequestError::Overloaded => (
                StatusCode::SERVICE_UNAVAILABLE,
                [("content-type", "application/json"), ("retry-after", "1")],
                serde_json::json!({
                    "error": "no cache capacity is free",
                    "retry": "retry shortly",
                })
                .to_string(),
            )
                .into_response(),
        }
    }
}

fn json(status: StatusCode, body: serde_json::Value) -> Response {
    (
        status,
        [("content-type", "application/json")],
        body.to_string(),
    )
        .into_response()
}

pub fn router(state: ServiceState) -> Router {
    state.inner.metrics.http.initialize(&[
        "init",
        "map",
        "filter",
        "manifest",
        "setup",
        "query_directory",
        "query_pages",
        "query_other",
    ]);
    let telemetry = state.inner.metrics.http.clone();
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/shards", get(shard_map))
        // The same paths the filter service serves on its own host, so a wallet
        // can point at either origin without changing anything but the base URL.
        .route("/v1/filters/shards", get(shard_map))
        .route("/v1/filters/shards/:shard_id/filter", get(shard_filter))
        .route("/v1/shards/init", get(init))
        .route(
            "/v1/shards/:shard_id/revisions/:digest/manifest",
            get(manifest),
        )
        .route(
            "/v1/shards/:shard_id/revisions/:digest/setup/:table/:segment",
            get(setup),
        )
        .route(
            "/v1/shards/:shard_id/revisions/:digest/query/:table",
            post(query),
        )
        .layer(axum::middleware::from_fn_with_state(
            telemetry,
            pir_observability::observe,
        ))
        .with_state(state)
}

async fn health(State(state): State<ServiceState>) -> Response {
    let inner = &state.inner;
    let mut body = serde_json::json!({
        "phase": "serving",
        "shards": inner.set.len(),
        "assigned_shards": inner.set.assigned_len(),
        "revisions_held": inner.set.revisions().len(),
        "prunable_revisions": inner.set.prunable().len(),
        "geometries": inner.set.geometries().iter().map(|g| g.name).collect::<Vec<_>>(),
        "map_sha256": inner.set.map_digest,
        "cache_budget_bytes": inner.cache.budget(),
        "cache_resident_bytes": inner.cache.resident_bytes(),
        "runtimes_built": inner.cache.entries(),
        "readiness_mode": match inner.warm.mode {
            ReadinessMode::LoadedOnly => "loaded-only",
            ReadinessMode::Warm => "warm",
        },
        "warm_runtimes": inner.warm.count.load(Ordering::Acquire),
        "prewarm_operations_total": Metrics::get(&inner.metrics.warm_runtimes),
        "work_memory_reserved_bytes": inner.cache.work_memory.reserved_bytes(),
        "target_runtimes": inner.warm.target,
        "binary_sha256": pir_control::binary_sha256(),
        "incarnation": pir_control::Identity::process().incarnation,
        "started_unix": pir_control::Identity::process().started_unix,
    });
    if let (Some(body), Some(identity)) = (body.as_object_mut(), state.identity().as_object()) {
        for (key, value) in identity {
            body.insert(key.clone(), value.clone());
        }
    }
    json(StatusCode::OK, body)
}

/// What readiness attests depends on the mode.
///
/// Loaded-only: a set is loaded, which is the only precondition for serving
/// correctly. Runtimes build on demand, so a cold worker is slow, not wrong;
/// this is the correctness pilot's mode.
///
/// Warm: every assigned runtime has been built, so a router that health-checks
/// this route holds a restarting worker out of rotation until it can answer at
/// full speed. The body carries the map and assignment digests either way, so
/// a deploy can assert which set and assignment a worker is actually running.
async fn ready(State(state): State<ServiceState>) -> Response {
    let inner = &state.inner;
    // The router checks this route every second and ejects a worker that
    // does not answer, so nothing here may wait on query work or on the
    // filesystem. The readiness decision reads only counters; the disk
    // cache's size is a directory walk, done off the async threads and
    // reported as unknown if it is slow.
    let runtime_cache = {
        let state = state.clone();
        match tokio::time::timeout(
            std::time::Duration::from_millis(500),
            tokio::task::spawn_blocking(move || state.inner.cache.disk_status()),
        )
        .await
        {
            Ok(Ok(status)) => status,
            _ => serde_json::json!({"status": "unavailable: the cache directory was slow to list"}),
        }
    };
    let warm = inner.warm.count.load(Ordering::Acquire);
    let mut body = serde_json::json!({
        "mode": match inner.warm.mode {
            ReadinessMode::LoadedOnly => "loaded-only",
            ReadinessMode::Warm => "warm",
        },
        "map_sha256": inner.set.map_digest,
        "shards": inner.set.len(),
        "assigned_shards": inner.set.assigned_len(),
        "warm_runtimes": warm,
        "target_runtimes": inner.warm.target,
        "binary_sha256": pir_control::binary_sha256(),
        "incarnation": pir_control::Identity::process().incarnation,
        "started_unix": pir_control::Identity::process().started_unix,
        "runtime_cache": runtime_cache,
        "prewarm_failed": Metrics::get(&inner.metrics.prewarm_failed),
        "prewarm_finished": inner.warm.finished.load(Ordering::Acquire),
        "prewarm_seconds": Metrics::get(&inner.metrics.prewarm_micros) as f64 / 1e6,
    });
    if let (Some(body), Some(identity)) = (body.as_object_mut(), state.identity().as_object()) {
        for (key, value) in identity {
            body.insert(key.clone(), value.clone());
        }
    }
    let object = body.as_object_mut().expect("an object");
    if state.is_invalidated() {
        object.insert("ready".into(), serde_json::json!(false));
        object.insert(
            "reason".into(),
            serde_json::json!("publication invalidated"),
        );
        return json(StatusCode::SERVICE_UNAVAILABLE, body);
    }
    if inner.set.is_empty() {
        object.insert("ready".into(), serde_json::json!(false));
        object.insert("reason".into(), serde_json::json!("no shards loaded"));
        return json(StatusCode::SERVICE_UNAVAILABLE, body);
    }
    if inner.warm.mode == ReadinessMode::Warm && (warm as usize) < inner.warm.target {
        object.insert("ready".into(), serde_json::json!(false));
        object.insert(
            "reason".into(),
            serde_json::json!(if inner.warm.finished.load(Ordering::Acquire) {
                "prewarm finished short of its target"
            } else {
                "prewarming"
            }),
        );
        return json(StatusCode::SERVICE_UNAVAILABLE, body);
    }
    object.insert("ready".into(), serde_json::json!(true));
    json(StatusCode::OK, body)
}

async fn metrics(State(state): State<ServiceState>) -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        state.inner.metrics.render(&state.snapshot()),
    )
        .into_response()
}

/// The published height-to-shard map.
///
/// Served from the bytes the set was loaded with, and digested, so this origin
/// and the filter service publish the *same* bytes for the same set. Serializing
/// per request would let field ordering or whitespace differ between the two,
/// and a wallet comparing them would read that as disagreement.
async fn shard_map(State(state): State<ServiceState>) -> Response {
    (
        StatusCode::OK,
        [
            ("content-type", "application/json"),
            ("x-shard-map-sha256", state.inner.set.map_digest.as_str()),
            ("cache-control", "no-cache"),
        ],
        state.inner.set.map_json.clone(),
    )
        .into_response()
}

/// One shard's public range filter.
///
/// Immutable once published, hence the long cache. Asking for a shard discloses
/// nothing: which shards exist is public, and a syncing wallet fetches the
/// filters of every shard in its range, not only the ones it will query.
async fn shard_filter(
    State(state): State<ServiceState>,
    AxumPath(shard_id): AxumPath<u64>,
) -> Response {
    // From the set-wide metadata, so a worker answers for every shard the
    // map names whether or not it holds that shard's tables.
    let Some(meta) = state.inner.set.meta(shard_id) else {
        return json(
            StatusCode::NOT_FOUND,
            serde_json::json!({ "error": format!("no shard {shard_id}") }),
        );
    };
    (
        StatusCode::OK,
        [
            ("content-type", "application/octet-stream"),
            ("cache-control", "public, max-age=31536000, immutable"),
        ],
        meta.filter.clone(),
    )
        .into_response()
}

async fn init(State(state): State<ServiceState>) -> Response {
    let inner = &state.inner;
    let Some(last) = inner.set.map.shards.last() else {
        return json(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"error": "no shards"}),
        );
    };
    let geometries = inner
        .set
        .geometries()
        .into_iter()
        .map(|geometry| {
            let directory = &inner.params[&(geometry.name, Table::Directory)];
            let pages = &inner.params[&(geometry.name, Table::Pages)];
            GeometryInit {
                name: geometry.name.to_string(),
                directory_rows: geometry.directory_rows,
                directory_row_bytes: geometry.directory_row_bytes as u32,
                directory_scheme: directory.scheme().clone(),
                directory_scheme_dq44: directory.dithered_scheme().clone(),
                directory_setup_seed: directory.setup_seed,
                page_rows: geometry.page_rows,
                page_row_bytes: geometry.page_row_bytes as u32,
                pages_scheme: pages.scheme().clone(),
                pages_scheme_dq44: pages.dithered_scheme().clone(),
                pages_setup_seed: pages.setup_seed,
            }
        })
        .collect();
    let body = InitResponse {
        schema: transparent_shard::manifest::SCHEMA.to_string(),
        profile: inner.set.map.profile.clone(),
        network: inner.set.map.network.clone(),
        genesis_hash: inner.set.map.genesis_hash.clone(),
        shards: inner.set.len(),
        start_height: inner.set.map.start_height,
        covered_through: last.end_height,
        map_sha256: inner.set.map_digest.clone(),
        geometries,
        assigned_shards: inner.set.assigned_len(),
        worker_id: inner.set.scope().map(|scope| scope.worker_id.clone()),
        role: inner
            .set
            .scope()
            .map(|scope| scope.role.as_str().to_string()),
        assignment_sha256: inner
            .set
            .scope()
            .map(|scope| scope.assignment_sha256.clone()),
    };
    json(
        StatusCode::OK,
        serde_json::to_value(body).expect("init json"),
    )
}

/// One revision's manifest, in the canonical bytes its digest is the hash of.
///
/// Immutable: a manifest is named by its own digest, so the bytes at this path
/// can never change. A wallet recomputes the digest from what it receives and
/// compares it with the map, then checks every field it relies on against the
/// map entry and the registry. What that establishes is that the publication
/// is internally consistent and unaltered in transit; it does not establish
/// that the index is complete, because the same publisher produced both.
///
/// Superseded revisions still held are served too, so a wallet holding a
/// revision the map has moved past can verify what it holds before deciding
/// whether to refresh.
async fn manifest(
    State(state): State<ServiceState>,
    AxumPath((shard_id, digest)): AxumPath<(u64, String)>,
) -> Response {
    let map_digest = state.inner.set.map_digest.clone();
    // The current revision of any shard the map names, or a superseded
    // revision this worker still holds. Every worker holds every current
    // manifest, so the route needs no routing by owner.
    let Some((manifest, canonical, _)) = state.inner.set.meta_by_digest(&digest) else {
        Metrics::incr(&state.inner.metrics.stale_revisions);
        return RequestError::Stale { shard_id, digest }.into_response(&map_digest);
    };
    if manifest.shard_id != shard_id {
        return RequestError::Bad(format!(
            "revision {digest} is not a revision of shard {shard_id}"
        ))
        .into_response(&map_digest);
    }
    Metrics::incr(&state.inner.metrics.manifests);
    (
        StatusCode::OK,
        [
            ("content-type", "application/json"),
            ("cache-control", "public, max-age=31536000, immutable"),
            ("x-manifest-sha256", digest.as_str()),
        ],
        canonical.to_vec(),
    )
        .into_response()
}

async fn setup(
    State(state): State<ServiceState>,
    AxumPath((shard_id, digest, table, segment)): AxumPath<(u64, String, String, u32)>,
) -> Response {
    let map_digest = state.inner.set.map_digest.clone();
    let Some(table) = Table::parse(&table) else {
        return RequestError::Bad("unknown table".into()).into_response(&map_digest);
    };
    // Counted as a waiter while the runtime is acquired or built, so a burst
    // of setup requests for a cold shard is bounded the same way queries are.
    let pending = match state.inner.admission.try_enter(None) {
        Ok(pending) => pending,
        Err(error) => return RequestError::from(error).into_response(&map_digest),
    };
    let (segments, geometry, handle) = {
        let shard = match state.revision(shard_id, &digest) {
            Ok(shard) => shard,
            Err(error) => return error.into_response(&map_digest),
        };
        let segments = shard.segments(table);
        let geometry = shard.geometry.name.to_string();
        match tokio::time::timeout(pending.remaining(), state.runtime(shard, table, segment)).await
        {
            Ok(Ok(handle)) => (segments, geometry, handle),
            Ok(Err(error)) => return error.into_response(&map_digest),
            Err(_) => {
                Metrics::incr(&state.inner.metrics.deadline_exceeded);
                return RequestError::from(AdmissionError::DeadlineExceeded)
                    .into_response(&map_digest);
            }
        }
    };
    pending.complete();
    let runtime = handle.get();
    let body = SetupResponse {
        shard_id,
        manifest_digest: digest,
        geometry,
        table: table.as_str().to_string(),
        segment,
        segments,
        public_params: BASE64_STANDARD.encode(&runtime.public_params),
        public_params_sha256: runtime.public_params_sha256.clone(),
        public_params_epoch: hex::encode(runtime.public_params_epoch),
    };
    Metrics::incr(&state.inner.metrics.setups);
    json(
        StatusCode::OK,
        serde_json::to_value(body).expect("setup json"),
    )
}

async fn query(
    state: State<ServiceState>,
    path: AxumPath<(u64, String, String)>,
    request: Request,
) -> Response {
    let mut timer = crate::metrics::QueryTimer::new(state.0.inner.metrics.clone());
    let response = query_inner(state, path, request).await;
    timer.finish(response.status().is_success());
    response
}

async fn query_inner(
    State(state): State<ServiceState>,
    AxumPath((shard_id, digest, table)): AxumPath<(u64, String, String)>,
    request: Request,
) -> Response {
    let map_digest = state.inner.set.map_digest.clone();
    let metrics = state.inner.metrics.clone();
    let Some(table) = Table::parse(&table) else {
        Metrics::incr(&metrics.query_errors);
        let refused = RequestError::Bad("unknown table".into()).into_response(&map_digest);
        return state.refuse_unread(request, refused).await;
    };
    let (segments, shared, query_rows) = {
        let shard = match state.revision(shard_id, &digest) {
            Ok(shard) => shard,
            Err(error) => {
                let refused = error.into_response(&map_digest);
                return state.refuse_unread(request, refused).await;
            }
        };
        if shard.segments(table) == 0 {
            return state
                .refuse_unread(
                    request,
                    RequestError::Bad("table capability unavailable".into())
                        .into_response(&map_digest),
                )
                .await;
        }
        let shared = state.shared(shard, table);
        // A growing tail's pages queries may select over only the rows its
        // manifest says can hold data; every other table selects every row.
        let query_rows = match table {
            Table::Pages => shard.manifest.pages_query_rows() as usize,
            _ => shared.profile.rows,
        };
        (shard.segments(table), shared, query_rows)
    };

    // The exact lengths this table's query may have — 49-bit or 44-bit
    // dithered, over every row or, for a growing tail's pages, over only the
    // rows its manifest says can hold data — are known before a single body
    // byte is read, so a body of any other length is refused here — before it
    // is buffered, before it queues, before any runtime is built for it. The
    // global body limit is the ceiling for the widest geometry; this is the
    // check for the one actually addressed.
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
        // Not discarded: with no declared length there is no bound to read to.
        Metrics::incr(&metrics.query_length_rejections);
        return RequestError::LengthRequired.into_response(&map_digest);
    };
    let Some(expected) = usize::try_from(declared)
        .ok()
        .filter(|&len| shared.accepts_query_bytes(query_rows, len))
    else {
        Metrics::incr(&metrics.query_length_rejections);
        let prefix = if query_rows < shared.profile.rows {
            format!(
                ", or {} or {} over its first {query_rows} rows",
                8 + transparent_native::request_len_bits(
                    query_rows,
                    transparent_native::QUERY_BITS
                ),
                8 + transparent_native::request_len_bits(
                    query_rows,
                    transparent_native::DITHERED_QUERY_BITS
                ),
            )
        } else {
            String::new()
        };
        let refused = RequestError::Bad(format!(
            "a {} query for geometry {} must be exactly {} or {} bytes{prefix}, not {declared}",
            table.as_str(),
            shared.geometry.name,
            shared.query_bytes(),
            shared.dithered_query_bytes(),
        ))
        .into_response(&map_digest);
        return state.refuse_unread(request, refused).await;
    };

    // Counted, and its bytes budgeted, before the body is read. A worker that
    // buffered first and counted afterwards would be bounded only by how many
    // clients chose to send at once.
    let pending = match state.inner.admission.try_enter(Some(expected)) {
        Ok(pending) => pending,
        Err(error) => {
            Metrics::incr(&metrics.query_errors);
            // The capacity refusal is the one a saturated worker sends most,
            // and the one whose retry delay matters most to reach the wallet.
            let refused = RequestError::from(error).into_response(&map_digest);
            return state.refuse_unread(request, refused).await;
        }
    };
    let upload_deadline = state.inner.admission.config().upload_deadline;
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
    // A declared length is a claim; the bytes that arrived are the fact.
    if body.len() != expected {
        Metrics::incr(&metrics.query_length_rejections);
        Metrics::incr(&metrics.query_errors);
        return RequestError::Bad(format!(
            "the query body is {} bytes where {expected} were declared",
            body.len()
        ))
        .into_response(&map_digest);
    }

    // The prefix names the revision and table, and the same bytes come back in
    // the response, so a query answered by the wrong runtime fails a check at
    // both ends rather than decoding into rows from a range nobody asked for.
    let binding = query_binding(&digest, table.as_str());

    // Admission before work: bounded evaluation is what keeps a burst from
    // pinning every runtime in the cache against eviction at once. The wait is
    // bounded by the request's deadline, and a request that outwaits it is
    // refused retryably rather than kept.
    let query_stage = std::time::Instant::now();
    let admitted = match state.inner.admission.wait_slot(pending).await {
        Ok(admitted) => admitted,
        Err(error) => {
            Metrics::incr(&metrics.query_errors);
            return RequestError::from(error).into_response(&map_digest);
        }
    };

    tracing::debug!(revision = %digest, table = table.as_str(), seconds = query_stage.elapsed().as_secs_f64(), stage = "query_admission", "query stage");
    let query_stage = std::time::Instant::now();

    // Every segment answers the same query, and the results come back in
    // segment order. The client keeps the row whose contents it can identify
    // and discards the rest; asking only the segment that holds the script
    // would disclose the script's placement, which is a function of the script.
    //
    // Every segment's runtime is acquired *before* any of them is evaluated,
    // and all the handles are held until the last one is done. Acquiring them
    // one at a time would let a tight budget evict segment 0 while segment 1
    // builds, and the next query would rebuild what this one just discarded —
    // a multi-segment shard would thrash rather than be answered. Holding them
    // together also means a shard whose segments do not fit the budget is
    // refused once, retryably, instead of making progress it cannot keep.
    let mut handles = Vec::with_capacity(segments as usize);
    for segment in 0..segments {
        let shard = match state.revision(shard_id, &digest) {
            Ok(shard) => shard,
            Err(error) => return error.into_response(&map_digest),
        };
        match tokio::time::timeout(admitted.remaining(), state.runtime(shard, table, segment)).await
        {
            Ok(Ok(handle)) => handles.push(handle),
            Ok(Err(error)) => {
                Metrics::incr(&metrics.query_errors);
                return error.into_response(&map_digest);
            }
            Err(_) => {
                Metrics::incr(&metrics.deadline_exceeded);
                Metrics::incr(&metrics.query_errors);
                return RequestError::from(AdmissionError::DeadlineExceeded)
                    .into_response(&map_digest);
            }
        }
    }

    tracing::debug!(revision = %digest, table = table.as_str(), seconds = query_stage.elapsed().as_secs_f64(), stage = "query_runtime", "query stage");

    // Evaluated off the async runtime, holding the slot and every handle for
    // exactly as long as the work runs. The admission moves into the closure:
    // if the client goes away meanwhile the evaluation still finishes and is
    // discarded, but the slot is released when it does, not when the dropped
    // future would have been polled.
    let memory_started = std::time::Instant::now();
    let memory = match state
        .inner
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
            tracing::debug!(revision = %digest, table = table.as_str(), seconds = memory_started.elapsed().as_secs_f64(), admitted = false, stage = "query_memory_admission", "query stage");
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
    tracing::debug!(revision = %digest, table = table.as_str(), seconds = memory_started.elapsed().as_secs_f64(), admitted = true, stage = "query_memory_admission", "query stage");
    let evaluation_metrics = metrics.clone();
    let query_stage = std::time::Instant::now();
    let query_revision = digest.clone();
    let evaluated = tokio::task::spawn_blocking(move || {
        let _memory = memory;
        tracing::debug!(revision = %query_revision, table = table.as_str(), seconds = query_stage.elapsed().as_secs_f64(), stage = "query_dispatch", "query stage");
        let query_stage = std::time::Instant::now();
        let _timer = evaluation_metrics.evaluation_seconds.timer();
        // The key and selection are parsed once: every segment shares the
        // table's packing setup and query masks, so one parse serves them all.
        let query = shared.parse(binding, &body, query_rows)?;
        let mut answer = Vec::with_capacity(shared.response_bytes() * handles.len());
        for handle in &handles {
            match handle.get().answer(binding, &query) {
                Ok(response) => answer.extend(response),
                Err(error) => return Err(error),
            }
        }
        tracing::debug!(revision = %query_revision, table = table.as_str(), seconds = query_stage.elapsed().as_secs_f64(), stage = "query_evaluate", "query stage");
        admitted.complete();
        Ok(answer)
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
