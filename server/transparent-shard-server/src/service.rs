//! Serving N shard revisions from one process.
//!
//! **Parameters are shared per geometry, published data is not.** Every shard
//! naming one geometry shares one `YpirSchemeParams` per table, because
//! `params_for_simplepir` is a function of geometry alone. A set mixing archive
//! and recent shards therefore leaks two parameter sets per geometry rather
//! than two per shard, and a client validates each once. The published `c1`
//! does *not* follow, because it is derived from each segment's own database —
//! so a client validates parameters once and fetches setup per segment.
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

use crate::metrics::Metrics;
use crate::runtime::{CacheError, RuntimeCache, RuntimeHandle, SharedParams};
use crate::shardset::{LoadedShard, ShardSet, Table};
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use ipir_sp::YpirSchemeParams;
use serde::Serialize;
use std::collections::HashMap;
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
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            cache_bytes: 4 << 30,
            build_slots: 1,
            query_slots: 2,
        }
    }
}

/// A runtime's address before the digest is resolved.
type ParamsKey = (&'static str, Table);

pub struct Inner {
    set: ShardSet,
    /// One parameter set per geometry the loaded set actually uses, per table.
    params: HashMap<ParamsKey, Arc<SharedParams>>,
    cache: RuntimeCache,
    query_slots: tokio::sync::Semaphore,
    metrics: Arc<Metrics>,
    max_query_bytes: usize,
}

#[derive(Clone)]
pub struct ServiceState {
    inner: Arc<Inner>,
}

/// One geometry's public parameters, as `GET /v1/shards/init` publishes them.
///
/// The dimensions are published beside the derived scheme, not instead of it.
/// A client re-derives the scheme from `rows` and `row_bytes` and refuses to
/// proceed unless it reproduces what the service sent, so the service cannot
/// choose parameters for it — including parameters that would leak the
/// selection.
#[derive(Serialize)]
pub struct GeometryInit {
    pub name: String,
    pub directory_rows: u64,
    pub directory_row_bytes: u32,
    pub directory_scheme: YpirSchemeParams,
    pub directory_setup_seed: u64,
    pub page_rows: u64,
    pub page_row_bytes: u32,
    pub pages_scheme: YpirSchemeParams,
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
        let metrics = Arc::new(Metrics::default());
        // Only the geometries this worker actually holds. Preparing every
        // registered one would publish parameters for shapes no shard here uses,
        // which a client would reasonably read as an offer to serve them.
        let mut params: HashMap<ParamsKey, Arc<SharedParams>> = HashMap::new();
        let mut max_query_bytes = 0usize;
        for geometry in set.geometries() {
            for table in [Table::Directory, Table::Pages] {
                let shared = Arc::new(SharedParams::build(geometry, table)?);
                max_query_bytes = max_query_bytes.max(shared.query_bytes());
                params.insert((geometry.name, table), shared);
            }
        }
        if params.is_empty() {
            return Err("the shard set names no geometry to serve".into());
        }
        Ok(Self {
            inner: Arc::new(Inner {
                set,
                params,
                cache: RuntimeCache::new(config.cache_bytes, config.build_slots, metrics.clone()),
                query_slots: tokio::sync::Semaphore::new(config.query_slots.max(1)),
                metrics,
                max_query_bytes,
            }),
        })
    }

    /// The largest body any geometry this worker serves can legitimately send.
    ///
    /// The request body limit. A mixed set is bounded by its widest geometry,
    /// so a narrower one is not rejected for being under it.
    pub fn max_query_bytes(&self) -> usize {
        self.inner.max_query_bytes
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.inner.metrics
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
}

impl RequestError {
    fn into_response(self, map_digest: &str) -> Response {
        match self {
            RequestError::Bad(message) => json(
                StatusCode::BAD_REQUEST,
                serde_json::json!({ "error": message }),
            ),
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
            "/v1/shards/:shard_id/revisions/:digest/setup/:table/:segment",
            get(setup),
        )
        .route(
            "/v1/shards/:shard_id/revisions/:digest/query/:table",
            post(query),
        )
        .with_state(state)
}

async fn health(State(state): State<ServiceState>) -> Response {
    let inner = &state.inner;
    json(
        StatusCode::OK,
        serde_json::json!({
            "phase": "serving",
            "shards": inner.set.len(),
            "revisions_held": inner.set.revisions().len(),
            "geometries": inner.set.geometries().iter().map(|g| g.name).collect::<Vec<_>>(),
            "map_sha256": inner.set.map_digest,
            "cache_budget_bytes": inner.cache.budget(),
            "cache_resident_bytes": inner.cache.resident_bytes(),
            "runtimes_built": inner.cache.entries(),
        }),
    )
}

/// Ready once a set is loaded, which is the only precondition this service has.
///
/// Deliberately not "ready once runtimes are warm". Runtimes are built on
/// demand, so a worker with a cold cache is serving correctly and slowly, and
/// reporting it unready would take it out of rotation exactly when the first
/// wallet needs it.
async fn ready(State(state): State<ServiceState>) -> Response {
    if state.inner.set.is_empty() {
        return json(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({ "ready": false, "reason": "no shards loaded" }),
        );
    }
    json(StatusCode::OK, serde_json::json!({ "ready": true }))
}

async fn metrics(State(state): State<ServiceState>) -> Response {
    let inner = &state.inner;
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        inner.metrics.render(
            inner.set.len(),
            inner.set.revisions().len(),
            inner.cache.budget(),
        ),
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
    let Some(shard) = state.inner.set.get(shard_id) else {
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
        shard.filter.clone(),
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
                directory_scheme: directory.scheme.clone(),
                directory_setup_seed: directory.setup_seed,
                page_rows: geometry.page_rows,
                page_row_bytes: geometry.page_row_bytes as u32,
                pages_scheme: pages.scheme.clone(),
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
    };
    json(
        StatusCode::OK,
        serde_json::to_value(body).expect("init json"),
    )
}

async fn setup(
    State(state): State<ServiceState>,
    AxumPath((shard_id, digest, table, segment)): AxumPath<(u64, String, String, u32)>,
) -> Response {
    let map_digest = state.inner.set.map_digest.clone();
    let Some(table) = Table::parse(&table) else {
        return RequestError::Bad("unknown table".into()).into_response(&map_digest);
    };
    let (segments, geometry, handle) = {
        let shard = match state.revision(shard_id, &digest) {
            Ok(shard) => shard,
            Err(error) => return error.into_response(&map_digest),
        };
        let segments = shard.segments(table);
        let geometry = shard.geometry.name.to_string();
        match state.runtime(shard, table, segment).await {
            Ok(handle) => (segments, geometry, handle),
            Err(error) => return error.into_response(&map_digest),
        }
    };
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
    State(state): State<ServiceState>,
    AxumPath((shard_id, digest, table)): AxumPath<(u64, String, String)>,
    body: axum::body::Bytes,
) -> Response {
    let map_digest = state.inner.set.map_digest.clone();
    let Some(table) = Table::parse(&table) else {
        Metrics::incr(&state.inner.metrics.query_errors);
        return RequestError::Bad("unknown table".into()).into_response(&map_digest);
    };
    let (segments, shared) = {
        let shard = match state.revision(shard_id, &digest) {
            Ok(shard) => shard,
            Err(error) => return error.into_response(&map_digest),
        };
        (shard.segments(table), state.shared(shard, table))
    };

    // The prefix names the revision and table, and the same bytes come back in
    // the response, so a query answered by the wrong runtime fails a check at
    // both ends rather than decoding into rows from a range nobody asked for.
    let binding = query_binding(&digest, table.as_str());

    // Admission before work: bounded evaluation is what keeps a burst from
    // pinning every runtime in the cache against eviction at once.
    let waited = std::time::Instant::now();
    let Ok(_permit) = state.inner.query_slots.acquire().await else {
        return RequestError::Bad("server is shutting down".into()).into_response(&map_digest);
    };
    Metrics::add(
        &state.inner.metrics.query_queue_micros,
        waited.elapsed().as_micros() as u64,
    );

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
        match state.runtime(shard, table, segment).await {
            Ok(handle) => handles.push(handle),
            Err(error) => {
                Metrics::incr(&state.inner.metrics.query_errors);
                return error.into_response(&map_digest);
            }
        }
    }

    let mut answer = Vec::new();
    for handle in &handles {
        match handle.get().evaluate(&shared, binding, &body) {
            Ok(response) => answer.extend(response),
            Err(error) => {
                Metrics::incr(&state.inner.metrics.query_errors);
                return RequestError::Bad(error).into_response(&map_digest);
            }
        }
    }
    Metrics::incr(&state.inner.metrics.queries);
    (
        StatusCode::OK,
        [("content-type", "application/octet-stream")],
        answer,
    )
        .into_response()
}
