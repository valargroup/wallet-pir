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
//! **Runtimes are built on demand and bounded.** See [`transparent_shared::runtime`]: the
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

use zakura_pir_protocol::transparent::shard as transparent_shard;
use zakura_pir_query_server::transparent::native as transparent_native;

use crate::transparent::admission::Admission;
pub use crate::transparent::config::ServiceConfig;
use crate::transparent::request::{answer_query, json, within, RequestError, Worker};
use axum::extract::{Path as AxumPath, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use transparent_shared::history::serving::HistoryServing;
use transparent_shared::history::shardset::{ShardSet, Table, TableRef};
use transparent_shared::runtime::disk::{DiskCache, ShippedRuntimes};
use transparent_shared::runtime::metrics::{Metrics, Snapshot};
use transparent_shared::runtime::{Produced, RuntimeCache};

/// The prewarm's progress toward holding every assigned runtime.
struct WarmState {
    target: usize,
    finished: AtomicBool,
    cancelled: AtomicBool,
    stopped: AtomicBool,
    task: std::sync::Mutex<Option<tokio::task::AbortHandle>>,
    count: std::sync::atomic::AtomicU64,
    /// How this snapshot's prewarm came by the targets it found not
    /// resident; see [`ServiceState::preparation`].
    produced: PrewarmProduced,
}

/// Counts of how one prewarm produced its targets.
#[derive(Default)]
struct PrewarmProduced {
    shipped: std::sync::atomic::AtomicU64,
    shipped_fallbacks: std::sync::atomic::AtomicU64,
    restored: std::sync::atomic::AtomicU64,
    built: std::sync::atomic::AtomicU64,
    self_check_micros: std::sync::atomic::AtomicU64,
}

impl PrewarmProduced {
    fn record(&self, produced: Produced) {
        let counter = match produced {
            Produced::Joined => return,
            Produced::Shipped { check_micros } => {
                self.self_check_micros
                    .fetch_add(check_micros, Ordering::Relaxed);
                &self.shipped
            }
            Produced::Fallback => &self.shipped_fallbacks,
            Produced::Restored => &self.restored,
            Produced::Built => &self.built,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

pub struct Inner {
    validation: std::sync::OnceLock<crate::transparent::validation::Scope>,
    /// The loaded set, its parameters, runtime cache and shipped runtimes.
    serving: HistoryServing,
    admission: Arc<Admission>,
    forwarding_admission: Arc<Admission>,
    metrics: Arc<Metrics>,
    warm: WarmState,
    /// Prewarm jobs; the cache separately bounds cold builds and disk restores.
    prewarm_slots: usize,
    archive: Option<Arc<crate::transparent::routing::archive::Archive>>,
}

#[derive(Clone)]
pub struct ServiceState {
    inner: Arc<Inner>,
}

/// One geometry's public parameters, as `GET /v2/shards/init` publishes them.
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub directory_scheme_dq44: Option<transparent_native::NativeScheme>,
    pub directory_setup_seed: u64,
    pub page_rows: u64,
    pub page_row_bytes: u32,
    pub pages_scheme: transparent_native::NativeScheme,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pages_scheme_dq44: Option<transparent_native::NativeScheme>,
    pub pages_setup_seed: u64,
}

/// What `GET /v2/shards/init` returns.
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
    /// Attach one trusted terminal archive before this snapshot is shared.
    pub fn with_archive(
        mut self,
        archive: Arc<crate::transparent::routing::archive::Archive>,
    ) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("attach archive before sharing state")
            .archive = Some(archive);
        self
    }

    pub(crate) fn archive(&self) -> Option<&Arc<crate::transparent::routing::archive::Archive>> {
        self.inner.archive.as_ref()
    }

    pub(crate) async fn qualify_retirement(&self, old: &Self) -> Result<(), String> {
        for shard in old.set().current() {
            let id = shard.manifest.shard_id;
            if self.set().is_assigned(id) {
                continue;
            }
            let next = self.set().meta(id).ok_or("retirement removes a shard")?;
            if next.digest != shard.digest {
                return Err("retirement must preserve the exact shard revision".into());
            }
            let archive = self
                .archive()
                .ok_or("local retirement requires a qualified archive route")?;
            tokio::time::timeout(
                self.worker().admission.config().query_deadline,
                archive.qualify(self, id, &shard.digest),
            )
            .await
            .map_err(|_| "archive retirement qualification timed out")??;
        }
        Ok(())
    }
    pub fn build(set: ShardSet, config: ServiceConfig) -> Result<Self, String> {
        Self::build_with_disk(set, config, None)
    }

    pub fn build_with_disk(
        set: ShardSet,
        config: ServiceConfig,
        disk: Option<DiskCache>,
    ) -> Result<Self, String> {
        Self::build_reusing(set, config, disk, None, None)
    }

    /// [`Self::build_with_disk`] for a publication staged with the runtimes
    /// its publisher prepared, in `shipped`.
    pub fn build_with_runtimes(
        set: ShardSet,
        config: ServiceConfig,
        disk: Option<DiskCache>,
        shipped: Option<ShippedRuntimes>,
    ) -> Result<Self, String> {
        Self::build_reusing(set, config, disk, None, shipped)
    }

    pub(crate) fn build_reusing(
        set: ShardSet,
        config: ServiceConfig,
        disk: Option<DiskCache>,
        previous: Option<&Self>,
        shipped: Option<ShippedRuntimes>,
    ) -> Result<Self, String> {
        let _ = pir_control::Identity::process();
        let metrics = previous
            .map(|s| s.inner.metrics.clone())
            .unwrap_or_else(|| Arc::new(Metrics::default()));
        let current: std::collections::HashSet<&str> =
            set.current().map(|shard| shard.digest.as_str()).collect();
        let target = set
            .warm_targets()
            .iter()
            .filter(|(digest, _, _)| current.contains(digest.as_str()))
            .count();
        let cache = previous
            .map(|s| s.inner.serving.cache().clone())
            .unwrap_or_else(|| {
                Arc::new(
                    RuntimeCache::new(config.cache_bytes, config.build_slots, metrics.clone())
                        .with_disk(disk),
                )
            });
        Metrics::set(&metrics.target_runtimes, target as u64);
        let serving = HistoryServing::new(
            set,
            cache.clone(),
            metrics.clone(),
            shipped,
            previous.map(|s| &s.inner.serving),
        )?;
        let prewarm_slots = cache.prewarm_concurrency();
        let admission = previous
            .map(|s| s.inner.admission.clone())
            .unwrap_or_else(|| Arc::new(Admission::new(config.admission(), metrics.clone())));
        let forwarding_admission = match previous {
            Some(state) => state.inner.forwarding_admission.clone(),
            None => {
                // Reserve uploads for the original local execution capacity,
                // even when later snapshots introduce another approved geometry.
                // This derives parameters only; no PIR runtime is constructed.
                let largest = HistoryServing::largest_query_bytes()?;
                Arc::new(
                    admission.forwarding(largest.saturating_mul(config.query_slots.max(1) as u64)),
                )
            }
        };
        let state = Self {
            inner: Arc::new(Inner {
                validation: std::sync::OnceLock::new(),
                serving,
                admission,
                forwarding_admission,
                metrics,
                warm: WarmState {
                    target,
                    finished: AtomicBool::new(false),
                    cancelled: AtomicBool::new(false),
                    stopped: AtomicBool::new(false),
                    task: std::sync::Mutex::new(None),
                    count: std::sync::atomic::AtomicU64::new(0),
                    produced: PrewarmProduced::default(),
                },
                prewarm_slots,
                archive: previous.and_then(|s| s.inner.archive.clone()),
            }),
        };
        state.validation();
        Ok(state)
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
        let mut task = self.inner.warm.task.lock().unwrap();
        let state = self.clone();
        let running = tokio::spawn(async move {
            let inner = &state.inner;
            let started = std::time::Instant::now();
            let current: std::collections::BTreeSet<String> = inner
                .serving
                .set()
                .current()
                .map(|shard| shard.digest.clone())
                .collect();
            // Keep enough jobs active to use the restore slots. Cache misses
            // still queue behind the independent, smaller cold-build limit.
            let queue = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from(
                inner
                    .serving
                    .set()
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
                        if inner.warm.cancelled.load(Ordering::Acquire) || inner.warm.stopped.load(Ordering::Acquire) {break;}
                        let next = queue.lock().unwrap().pop_front();
                        let Some((digest, table, segment)) = next else {
                            break;
                        };
                        if state.validation().unavailable(&digest) { continue; }
                        let Some(target) = inner.serving.warm_target(&digest, table, segment) else {
                            continue;
                        };
                        if inner.warm.stopped.load(Ordering::Acquire) { break; }
                        match transparent_shared::runtime::prewarm::retry(
                            &inner.warm.cancelled,
                            std::time::Duration::from_secs(30),
                            || target.produce(),
                        ).await
                        {
                            Ok((handle, produced)) => {
                                if inner.warm.cancelled.load(Ordering::Acquire) || inner.warm.stopped.load(Ordering::Acquire) {break;}
                                inner.warm.produced.record(produced);
                                if current.contains(&digest) {
                                    inner.warm.count.fetch_add(1, Ordering::Release);
                                    drop(handle);
                                }
                                Metrics::incr(&inner.metrics.warm_runtimes);
                            }
                            Err(error) => {
                                tracing::warn!(shard = %digest, table = %table.name(), segment, %error, "prewarm failed");
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
        });
        *task = Some(running.abort_handle());
        running
    }

    /// The worker's identity for reports: assignment digest, id and role when
    /// it loaded under an assignment.
    pub(crate) fn has_other_holders(&self) -> bool {
        Arc::strong_count(&self.inner) > 1
    }
    /// How this snapshot's prewarm came by its targets: of those it found
    /// not resident, loaded from shipped files, shipped ones refused and
    /// produced here instead, restored from this worker's disk cache and
    /// built here with none offered; and the shipped self-checks' summed
    /// time. Healthy shipping builds nothing here.
    pub(crate) fn preparation(&self) -> serde_json::Value {
        let produced = &self.inner.warm.produced;
        let get = |counter: &std::sync::atomic::AtomicU64| counter.load(Ordering::Relaxed);
        serde_json::json!({
            "runtimes_offered": self.inner.serving.shipped().is_some(),
            "shipped": get(&produced.shipped),
            "shipped_fallbacks": get(&produced.shipped_fallbacks),
            "restored": get(&produced.restored),
            "built": get(&produced.built),
            "self_check_seconds": get(&produced.self_check_micros) as f64 / 1e6,
        })
    }

    pub(crate) fn stop_warming(&self) {
        self.inner.warm.stopped.store(true, Ordering::Release);
        if let Some(task) = self.inner.warm.task.lock().unwrap().take() {
            task.abort();
        }
    }

    /// Reclaim idle runtimes of revisions this snapshot no longer serves as
    /// current. Its current runtimes, directory and page tables alike, are
    /// residency obligations while it stays active, so preparing a candidate
    /// beside it never turns its in-flight and next queries into cold builds.
    pub fn evict_unpinned_except_current(&self) {
        self.inner.serving.evict_unpinned_except_current();
    }

    pub(crate) fn prune_disk(
        &self,
        keep: &std::collections::HashSet<String>,
    ) -> Result<Option<u64>, String> {
        self.inner.serving.cache().prune_disk(keep)
    }

    pub(crate) fn required_material(&self) -> bool {
        self.inner
            .serving
            .shipped()
            .is_some_and(|s| s.is_required())
    }
    /// Check required files without loading runtimes or scanning their contents.
    pub fn check_required_material(&self) -> Result<usize, String> {
        self.inner.serving.check_required_material()
    }

    pub(crate) fn release_invalidated(&self, digests: &std::collections::BTreeSet<String>) {
        if self
            .inner
            .serving
            .set()
            .current()
            .any(|s| digests.contains(&s.digest))
        {
            self.inner.warm.cancelled.store(true, Ordering::Release);
        }
    }

    pub(crate) fn artifact_validation(&self) -> serde_json::Value {
        self.validation().status()
    }

    /// Whether background artifact validation left some current revision
    /// serving, as provider `/v1/ready` reports it.
    pub fn serves_current_artifacts(&self) -> bool {
        self.validation().serves_current()
    }

    fn validation(&self) -> &crate::transparent::validation::Scope {
        self.inner.validation.get_or_init(|| {
            let mut scope = crate::transparent::validation::Scope::default();
            for shard in self.inner.serving.set().current() {
                scope.current(shard.manifest.shard_id, &shard.digest);
            }
            // A wallet can still request a retained predecessor by its digest.
            for artifact in self.inner.serving.artifacts() {
                scope.add(
                    &artifact.digest,
                    artifact.shard_id,
                    &artifact.path,
                    self.inner.serving.cache(),
                    &artifact.key,
                );
            }
            scope
        })
    }

    pub(crate) fn set(&self) -> &ShardSet {
        self.inner.serving.set()
    }

    /// Waits until local and forwarded requests admitted by any snapshot
    /// sharing this admission have ended.
    pub(crate) async fn drained(&self) {
        self.inner.admission.drained().await;
        self.inner.forwarding_admission.drained().await;
    }

    pub(crate) fn is_invalidated(&self) -> bool {
        self.inner.warm.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn is_ready(&self) -> bool {
        !self.is_invalidated()
    }

    pub(crate) fn is_warm(&self) -> bool {
        !self.inner.warm.cancelled.load(Ordering::Acquire)
            && self.inner.warm.finished.load(Ordering::Acquire)
            && self.inner.warm.count.load(Ordering::Acquire) as usize >= self.inner.warm.target
    }

    fn identity(&self) -> serde_json::Value {
        match self.inner.serving.set().scope() {
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
        let mut labels = vec![(
            "map_sha256".to_string(),
            inner.serving.set().map_digest.clone(),
        )];
        if let Some(scope) = inner.serving.set().scope() {
            labels.push(("worker_id".to_string(), scope.worker_id.clone()));
            labels.push(("role".to_string(), scope.role.as_str().to_string()));
            labels.push((
                "assignment_sha256".to_string(),
                scope.assignment_sha256.clone(),
            ));
        }
        Snapshot {
            labels,
            shards: inner.serving.set().len() as u64,
            assigned_shards: inner.serving.set().assigned_len() as u64,
            revisions: inner.serving.set().revisions().len() as u64,
            prunable_revisions: inner.serving.set().prunable().len() as u64,
            cache_budget_bytes: inner.serving.cache().budget(),
            warm_runtimes: inner.warm.count.load(Ordering::Acquire),
            work_memory_reserved_bytes: inner.serving.cache().work_memory().reserved_bytes(),
            process_rss_bytes: transparent_shared::runtime::procmem::process_rss_bytes(),
            process_cpu: transparent_shared::runtime::procmem::process_cpu(),
            cgroup_memory_bytes: transparent_shared::runtime::procmem::cgroup_memory_bytes(),
        }
    }

    /// The largest body any geometry this worker serves can legitimately send.
    ///
    /// The request body limit. A mixed set is bounded by its widest geometry,
    /// so a narrower one is not rejected for being under it.
    pub fn max_query_bytes(&self) -> usize {
        self.inner.serving.max_query_bytes()
    }

    /// The parts of this worker a request passes through.
    pub(crate) fn worker(&self) -> Worker<'_> {
        Worker {
            admission: &self.inner.admission,
            work_memory: self.inner.serving.cache().work_memory(),
            metrics: &self.inner.metrics,
            max_query_bytes: self.max_query_bytes(),
            map_digest: &self.inner.serving.set().map_digest,
        }
    }

    pub(crate) fn forwarding_admission(&self) -> &Admission {
        &self.inner.forwarding_admission
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.inner.metrics
    }

    /// Holds one evaluation slot until the permit is dropped.
    ///
    /// For tests that need the worker busy without answering anything, so the
    /// bounds in front of the slot can be exercised.
    pub async fn hold_query_slot(&self) -> crate::transparent::admission::HeldSlot {
        self.inner.admission.hold_slot().await
    }
}

/// The revision named is no longer served: the wallet refreshes its map and
/// re-derives the shard's coverage.
fn stale(shard_id: u64, digest: &str) -> RequestError {
    transparent_shared::history::serving::stale(shard_id, digest).into()
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
    let private = Router::new()
        .route("/v1/health", get(health))
        .route("/v1/ready", get(ready))
        .route("/metrics", get(metrics));
    crate::transparent::PUBLIC_ROUTES
        .mount(
            private,
            [
                ("/shards", get(shard_map)),
                // The same operations the filter service serves on its own
                // host, so a wallet can point at either origin without
                // changing anything but the base URL.
                ("/filters/shards", get(shard_map)),
                ("/filters/shards/:shard_id/filter", get(shard_filter)),
                ("/shards/init", get(init)),
                (
                    "/shards/:shard_id/revisions/:digest/manifest",
                    get(manifest),
                ),
                (
                    "/shards/:shard_id/revisions/:digest/setup/:table/:segment",
                    get(setup),
                ),
                (
                    "/shards/:shard_id/revisions/:digest/query/:table",
                    post(query),
                ),
            ],
        )
        .layer(axum::middleware::from_fn_with_state(
            telemetry,
            transparent_shared::http::observe,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.archive().is_some(),
            archive_hop,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            validate_request,
        ))
        .with_state(state)
}

async fn validate_request(
    State(state): State<ServiceState>,
    request: Request,
    next: axum::middleware::Next,
) -> Response {
    state.validation().serve(request, next).await
}

async fn archive_hop(
    State(forwarding): State<bool>,
    request: Request,
    next: axum::middleware::Next,
) -> Response {
    if forwarding && request.headers().contains_key("x-transparent-archive-hop") {
        return RequestError::Misdirected("an archive hop must terminate at an owner".into())
            .into_response("");
    }
    next.run(request).await
}

async fn health(State(state): State<ServiceState>) -> Response {
    let inner = &state.inner;
    let mut body = serde_json::json!({
        "artifact_validation": state.validation().status(),
        "phase": "serving",
        "shards": inner.serving.set().len(),
        "assigned_shards": inner.serving.set().assigned_len(),
        "revisions_held": inner.serving.set().revisions().len(),
        "prunable_revisions": inner.serving.set().prunable().len(),
        "geometries": inner.serving.set().geometries().iter().map(|g| g.name).collect::<Vec<_>>(),
        "map_sha256": inner.serving.set().map_digest,
        "cache_budget_bytes": inner.serving.cache().budget(),
        "cache_resident_bytes": inner.serving.cache().resident_bytes(),
        "runtimes_built": inner.serving.cache().entries(),
        "readiness_mode": "loaded-only",
        "warm_runtimes": inner.warm.count.load(Ordering::Acquire),
        "prewarm_operations_total": Metrics::get(&inner.metrics.warm_runtimes),
        "work_memory_reserved_bytes": inner.serving.cache().work_memory().reserved_bytes(),
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

/// Ready after metadata and file-size checks, independently of warming.
/// The response identifies the active map and assignment for operators.
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
            tokio::task::spawn_blocking(move || state.inner.serving.cache().disk_status()),
        )
        .await
        {
            Ok(Ok(status)) => status,
            _ => serde_json::json!({"status": "unavailable: the cache directory was slow to list"}),
        }
    };
    let warm = inner.warm.count.load(Ordering::Acquire);
    let mut body = serde_json::json!({
        "artifact_validation": state.validation().status(),
        "archive_forwarding": inner.archive.is_some(),
        "mode": "loaded-only",
        "map_sha256": inner.serving.set().map_digest,
        "shards": inner.serving.set().len(),
        "assigned_shards": inner.serving.set().assigned_len(),
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
    if inner.serving.set().is_empty() {
        object.insert("ready".into(), serde_json::json!(false));
        object.insert("reason".into(), serde_json::json!("no shards loaded"));
        return json(StatusCode::SERVICE_UNAVAILABLE, body);
    }
    object.insert("ready".into(), serde_json::json!(true));
    json(StatusCode::OK, body)
}

async fn metrics(State(state): State<ServiceState>) -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        state.inner.metrics.render(&state.snapshot()) + &state.validation().metrics(),
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
            (
                "x-shard-map-sha256",
                state.inner.serving.set().map_digest.as_str(),
            ),
            ("cache-control", "no-cache"),
        ],
        state.inner.serving.set().map_json.clone(),
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
    let Some(meta) = state.inner.serving.set().meta(shard_id) else {
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
    let Some(last) = inner.serving.set().map.shards.last() else {
        return json(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"error": "no shards"}),
        );
    };
    let geometries = inner
        .serving
        .set()
        .geometries()
        .into_iter()
        .map(|geometry| {
            let params = |table| {
                inner
                    .serving
                    .params(geometry.name, table)
                    .expect("every named geometry has parameters")
            };
            let directory = params(Table::Directory);
            let pages = params(Table::Pages);
            GeometryInit {
                name: geometry.name.to_string(),
                directory_rows: geometry.directory_rows,
                directory_row_bytes: geometry.directory_row_bytes as u32,
                directory_scheme: directory.scheme().clone(),
                directory_scheme_dq44: directory.dithered_scheme().cloned(),
                directory_setup_seed: directory.setup_seed,
                page_rows: geometry.page_rows,
                page_row_bytes: geometry.page_row_bytes as u32,
                pages_scheme: pages.scheme().clone(),
                pages_scheme_dq44: pages.dithered_scheme().cloned(),
                pages_setup_seed: pages.setup_seed,
            }
        })
        .collect();
    let body = InitResponse {
        schema: transparent_shard::manifest::SCHEMA.to_string(),
        profile: inner.serving.set().map.profile.clone(),
        network: inner.serving.set().map.network.clone(),
        genesis_hash: inner.serving.set().map.genesis_hash.clone(),
        shards: inner.serving.set().len(),
        start_height: inner.serving.set().map.start_height,
        covered_through: last.end_height,
        map_sha256: inner.serving.set().map_digest.clone(),
        geometries,
        assigned_shards: inner.serving.set().assigned_len(),
        worker_id: inner
            .serving
            .set()
            .scope()
            .map(|scope| scope.worker_id.clone()),
        role: inner
            .serving
            .set()
            .scope()
            .map(|scope| scope.role.as_str().to_string()),
        assignment_sha256: inner
            .serving
            .set()
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
    let map_digest = state.inner.serving.set().map_digest.clone();
    // The current revision of any shard the map names, or a superseded
    // revision this worker still holds. Every worker holds every current
    // manifest, so the route needs no routing by owner.
    let Some((manifest, canonical, _)) = state.inner.serving.set().meta_by_digest(&digest) else {
        Metrics::incr(&state.inner.metrics.stale_revisions);
        return stale(shard_id, &digest).into_response(&map_digest);
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
    let map_digest = state.inner.serving.set().map_digest.clone();
    let Some(table) = TableRef::parse(&table) else {
        return RequestError::Bad("unknown table".into()).into_response(&map_digest);
    };
    if state.set().revision(&digest).is_none() && state.set().meta_by_digest(&digest).is_some() {
        if let Some(archive) = state.archive() {
            return archive
                .setup(&state, shard_id, &digest, table, segment)
                .await;
        }
    }
    if state.archive().is_some()
        && state.set().names(shard_id)
        && state.set().meta_by_digest(&digest).is_none()
    {
        return stale(shard_id, &digest).into_response(&map_digest);
    }
    // Counted as a waiter while the runtime is acquired or built, so a burst
    // of setup requests for a cold shard is bounded the same way queries are.
    let pending = match state.inner.admission.try_enter(None) {
        Ok(pending) => pending,
        Err(error) => return RequestError::from(error).into_response(&map_digest),
    };
    let prepared = state
        .inner
        .serving
        .prepare_setup(shard_id, &digest, table, segment)
        .map_err(RequestError::from);
    let setup = match prepared {
        Ok(prepared) => {
            let load = async { prepared.load().await.map_err(RequestError::from) };
            within(pending.remaining(), &state.inner.metrics, load).await
        }
        Err(error) => Err(error),
    };
    let setup = match setup {
        Ok(setup) => setup,
        Err(error) => return error.into_response(&map_digest),
    };
    pending.complete();
    let runtime = setup.runtime.get();
    let body = SetupResponse {
        shard_id,
        manifest_digest: digest,
        geometry: setup.geometry.to_string(),
        table: setup.table,
        segment: setup.segment,
        segments: setup.segments,
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
    let mut timer =
        transparent_shared::runtime::metrics::QueryTimer::new(state.0.inner.metrics.clone());
    let response = query_inner(state, path, request).await;
    timer.finish(response.status().is_success());
    response
}

async fn query_inner(
    State(state): State<ServiceState>,
    AxumPath((shard_id, digest, table)): AxumPath<(u64, String, String)>,
    request: Request,
) -> Response {
    let worker = state.worker();
    let map_digest = worker.map_digest;
    let metrics = worker.metrics;
    let Some(table) = TableRef::parse(&table) else {
        Metrics::incr(&metrics.query_errors);
        let refused = RequestError::Bad("unknown table".into()).into_response(map_digest);
        return worker.refuse_unread(request, refused).await;
    };
    if state.set().revision(&digest).is_none() && state.set().meta_by_digest(&digest).is_some() {
        if let Some(archive) = state.archive() {
            return archive
                .query(&state, shard_id, &digest, table, request)
                .await;
        }
    }
    if state.archive().is_some()
        && state.set().names(shard_id)
        && state.set().meta_by_digest(&digest).is_none()
    {
        return worker
            .refuse_unread(request, stale(shard_id, &digest).into_response(map_digest))
            .await;
    }
    // Every segment answers the same query, and the results come back in
    // segment order. The client keeps the row whose contents it can identify
    // and discards the rest; asking only the segment that holds the script
    // would disclose the script's placement, which is a function of the script.
    let query = match state.inner.serving.prepare_query(shard_id, &digest, table) {
        Ok(query) => query,
        Err(error) => {
            let refused = RequestError::from(error).into_response(map_digest);
            return worker.refuse_unread(request, refused).await;
        }
    };
    answer_query(worker, request, query).await
}
