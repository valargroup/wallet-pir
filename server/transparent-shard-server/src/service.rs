//! Serving N shards from one process.
//!
//! The predecessor of this service held exactly one generation and said so.
//! This holds a whole set, which changes two things.
//!
//! **Runtimes are built lazily.** A shard's PIR preprocessing is far larger
//! than its plaintext, and a fleet-sized set built eagerly would spend minutes
//! at startup constructing state for shards no wallet may query. Runtimes are
//! therefore built on first use and kept, so the cost is paid by the first
//! query against a shard rather than by every restart.
//!
//! **Parameters are shared, published data is not.** Every shard uses the same
//! pinned geometry, and `params_for_simplepir` is a function of geometry alone,
//! so all shards share one `YpirSchemeParams` per table. The published `c1`
//! does *not* follow, because it is derived from each shard's own database — so
//! a client validates parameters once and fetches setup per shard.
//!
//! **A shard's segments are answered together.** A shard whose content did not
//! fit one segment of the pinned geometry has several, and a query names the
//! row within a segment, never the segment. One request is evaluated against
//! every segment of the shard and the results are returned in segment order, so
//! which segment holds the selected script — a function of the script — is not
//! something the wallet has to disclose in order to ask.
//!
//! **It also serves the public range filters**, which an earlier revision
//! deliberately refused to do. See `shardset` for why that rule was relaxed and
//! what is kept: the filter service still serves the same bytes at the same
//! paths on its own host, so two origins remain available to a wallet that
//! wants them.

use crate::shardset::{ShardSet, Table};
use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use enhance_pir_server::ipir::{deserialize_first_dim_query, RowPlaintextIter};
use inspiring::{QueryPackPreprocessed, RlweParams, TopKeyImages};
use ipir_sp::serialize::{deserialize_packing_keys, serialized_packing_keys_len};
use ipir_sp::server::IPIRServer;
use ipir_sp::server::{
    build_pack_preprocessed_blocks, pack_intermediate_blocks, published_c1_rows,
};
use ipir_sp::{IPIRClient, YpirSchemeParams};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The geometry both tables share with every shard.
///
/// Leaked once for the process. Unlike the one-generation predecessor, which
/// leaked a parameter set per table it loaded, this leaks exactly two for the
/// whole fleet however many shards it serves — which is the concrete saving
/// that pinning the geometry buys.
pub struct SharedParams {
    pub rlwe: &'static RlweParams,
    pub scheme: YpirSchemeParams,
    pub top_key_images: TopKeyImages<'static>,
}

impl SharedParams {
    pub fn build(table: Table) -> Result<Self, String> {
        let (rlwe, scheme) =
            ipir_sp::params_for_simplepir(table.rows(), (table.row_bytes() as u64) * 8)
                .map_err(|error| error.to_string())?;
        let rlwe: &'static RlweParams = Box::leak(Box::new(rlwe));
        let top_key_images = TopKeyImages::build(rlwe);
        Ok(Self {
            rlwe,
            scheme,
            top_key_images,
        })
    }

    fn max_query_bytes(&self) -> usize {
        16 + serialized_packing_keys_len(self.rlwe)
            + (self.scheme.db_rows * self.scheme.query_bits).div_ceil(8)
    }
}

/// One shard's table, prepared to answer queries.
pub struct TableRuntime {
    preprocessed: Vec<QueryPackPreprocessed<'static>>,
    server: IPIRServer<u16>,
    public_params: Vec<u8>,
    public_params_sha256: String,
    public_params_epoch: [u8; 8],
}

impl TableRuntime {
    pub fn build(shared: &SharedParams, table: Table, rows: &[u8]) -> Result<Self, String> {
        let coefficients = RowPlaintextIter::new(
            rows,
            table.row_bytes() as usize,
            shared.scheme.db_rows,
            shared.scheme.db_cols,
            shared.scheme.p.trailing_zeros() as usize,
        );
        let server =
            IPIRServer::<u16>::new_auto_kernel(shared.scheme.clone(), coefficients, false, true);

        // The setup is derived from a published seed, so a client reproduces it
        // exactly. It is public: it carries no secret and no selection.
        let mut seed = [0u8; 32];
        seed[..8].copy_from_slice(&table.setup_seed().to_le_bytes());
        let setup = IPIRClient::new(shared.rlwe, &shared.scheme)
            .generate_public_query_setup_simplepir_from_seed(seed);
        let crs_blocks = server
            .perform_offline_precomputation_simplepir(shared.rlwe, &setup)
            .crs_blocks;
        let preprocessed =
            build_pack_preprocessed_blocks(shared.rlwe, &crs_blocks).map_err(|e| e.to_string())?;
        let public_params = published_c1_rows(&preprocessed, shared.rlwe.q);
        let digest = Sha256::digest(&public_params);
        let mut epoch = [0u8; 8];
        epoch.copy_from_slice(&digest[..8]);

        Ok(Self {
            preprocessed,
            server,
            public_params,
            public_params_sha256: hex::encode(digest),
            public_params_epoch: epoch,
        })
    }

    /// Answers one query. The row selected is never known to this function.
    fn evaluate(
        &self,
        shared: &SharedParams,
        shard_id: u64,
        body: &[u8],
    ) -> Result<Vec<u8>, String> {
        // The shard id is in the prefix, so a query decoded against the wrong
        // shard fails the client's own check rather than returning a row that
        // decodes to plausible nonsense.
        let prefix: [u8; 8] = body
            .get(..8)
            .ok_or_else(|| "query is truncated".to_string())?
            .try_into()
            .expect("eight bytes");
        if u64::from_le_bytes(prefix) != shard_id {
            return Err("query names a different shard".to_string());
        }
        let packing_len = serialized_packing_keys_len(shared.rlwe);
        let switched_len = (shared.scheme.db_rows * shared.scheme.query_bits).div_ceil(8);
        // A fixed length for every query: a body that varied with the selection
        // would leak through its size alone.
        if body.len() != 8 + packing_len + switched_len {
            return Err("query has the wrong fixed length".to_string());
        }
        let packing_keys = deserialize_packing_keys(shared.rlwe, &body[8..8 + packing_len])
            .map_err(|e| e.to_string())?;
        let query =
            deserialize_first_dim_query(shared.rlwe, &shared.scheme, &body[8 + packing_len..])
                .map_err(|e| e.to_string())?;
        let intermediate = self.server.multiply_query(shared.rlwe, &query);
        let packed = pack_intermediate_blocks(
            &intermediate,
            &packing_keys,
            &shared.top_key_images,
            &self.preprocessed,
        )
        .map_err(|e| e.to_string())?;
        let c2 = ipir_sp::modulus_switch::serialize_rlwe_response_bodies(
            &packed,
            shared.scheme.q_prime_1,
        );
        let mut response = Vec::with_capacity(16 + c2.len());
        response.extend_from_slice(&shard_id.to_le_bytes());
        response.extend_from_slice(&self.public_params_epoch);
        response.extend_from_slice(&c2);
        Ok(response)
    }
}

/// A runtime's address: shard, table, and segment within that table.
type RuntimeKey = (u64, &'static str, u32);

pub struct Inner {
    set: ShardSet,
    directory_params: SharedParams,
    pages_params: SharedParams,
    /// Built on first use and kept. Keyed by shard, table and segment.
    runtimes: Mutex<HashMap<RuntimeKey, Arc<TableRuntime>>>,
}

#[derive(Clone)]
pub struct ServiceState {
    inner: Arc<Inner>,
}

/// What `GET /v1/shards/init` returns.
///
/// One scheme per table for the whole fleet, not one per shard. A client
/// validates these once and reuses them everywhere, which is what pinning the
/// geometry is for.
#[derive(Serialize)]
pub struct InitResponse {
    pub schema: String,
    pub profile: String,
    pub network: String,
    pub genesis_hash: String,
    pub shards: usize,
    pub start_height: u64,
    pub covered_through: u64,
    pub directory_scheme: YpirSchemeParams,
    pub directory_setup_seed: u64,
    pub pages_scheme: YpirSchemeParams,
    pub pages_setup_seed: u64,
}

/// What `GET /v1/shards/{id}/setup/{table}/{segment}` returns.
///
/// Per segment, because the published `c1` is derived from each segment's own
/// database. A shard with one segment — the ordinary case — has one of these
/// per table.
#[derive(Serialize)]
pub struct SetupResponse {
    pub shard_id: u64,
    pub table: String,
    pub segment: u32,
    pub segments: u32,
    pub public_params: String,
    pub public_params_sha256: String,
    pub public_params_epoch: String,
}

impl ServiceState {
    pub fn build(set: ShardSet) -> Result<Self, String> {
        Ok(Self {
            inner: Arc::new(Inner {
                set,
                directory_params: SharedParams::build(Table::Directory)?,
                pages_params: SharedParams::build(Table::Pages)?,
                runtimes: Mutex::new(HashMap::new()),
            }),
        })
    }

    fn shared(&self, table: Table) -> &SharedParams {
        match table {
            Table::Directory => &self.inner.directory_params,
            Table::Pages => &self.inner.pages_params,
        }
    }

    /// How many segments a shard's table has.
    fn segments(&self, shard_id: u64, table: Table) -> Result<u32, String> {
        Ok(self
            .inner
            .set
            .get(shard_id)
            .ok_or_else(|| format!("no shard {shard_id}"))?
            .segments(table))
    }

    /// The runtime for one segment of one shard's table, building it if this is
    /// its first use.
    fn runtime(
        &self,
        shard_id: u64,
        table: Table,
        segment: u32,
    ) -> Result<Arc<TableRuntime>, String> {
        let key = (shard_id, table.as_str(), segment);
        if let Some(runtime) = self.inner.runtimes.lock().expect("runtimes").get(&key) {
            return Ok(runtime.clone());
        }
        let shard = self
            .inner
            .set
            .get(shard_id)
            .ok_or_else(|| format!("no shard {shard_id}"))?;
        let rows = shard.table(table, segment).ok_or_else(|| {
            format!(
                "shard {shard_id} has no {} segment {segment}",
                table.as_str()
            )
        })?;
        let built = Arc::new(TableRuntime::build(self.shared(table), table, rows)?);
        // A concurrent request may have built it first; either copy is
        // equivalent, since the build is a pure function of the shard's bytes.
        let mut runtimes = self.inner.runtimes.lock().expect("runtimes");
        Ok(runtimes.entry(key).or_insert(built).clone())
    }

    pub fn max_query_bytes(&self) -> usize {
        self.inner
            .directory_params
            .max_query_bytes()
            .max(self.inner.pages_params.max_query_bytes())
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

fn bad_request(message: impl std::fmt::Display) -> Response {
    json(
        StatusCode::BAD_REQUEST,
        serde_json::json!({ "error": message.to_string() }),
    )
}

pub fn router(state: ServiceState) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/shards", get(shard_map))
        // The same paths the filter service serves on its own host, so a wallet
        // can point at either origin without changing anything but the base URL.
        .route("/v1/filters/shards", get(shard_map))
        .route("/v1/filters/shards/:shard_id/filter", get(shard_filter))
        .route("/v1/shards/init", get(init))
        .route("/v1/shards/:shard_id/setup/:table/:segment", get(setup))
        .route("/v1/shards/:shard_id/query/:table", post(query))
        .with_state(state)
}

fn parse_table(text: &str) -> Option<Table> {
    match text {
        "directory" => Some(Table::Directory),
        "pages" => Some(Table::Pages),
        _ => None,
    }
}

async fn health(State(state): State<ServiceState>) -> Response {
    let inner = &state.inner;
    json(
        StatusCode::OK,
        serde_json::json!({
            "phase": "serving",
            "shards": inner.set.shards.len(),
            "runtimes_built": inner.runtimes.lock().expect("runtimes").len(),
        }),
    )
}

/// The published height-to-shard map.
///
/// Served here as a convenience for a client that already trusts this service
/// for private queries. It is the same bytes the filter service publishes, and
/// a wallet that wants the two to be independent should take the map from
/// there and compare.
async fn shard_map(State(state): State<ServiceState>) -> Response {
    json(
        StatusCode::OK,
        serde_json::to_value(&state.inner.set.map).expect("map json"),
    )
}

/// One shard's public range filter.
///
/// Served here as well as by the filter service. The earlier rule kept public
/// bytes off this origin entirely, so that a filter download and a private query
/// could not be correlated. In practice the shard id of a query is already
/// public in its own URL and one operator runs both services, so that
/// correlation was available anyway. Serving them here gives the transparent
/// host a complete API, and the filter service still offers the same bytes on a
/// separate origin for a wallet that wants to fetch the two over different
/// network paths.
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
    let body = InitResponse {
        schema: transparent_shard::manifest::SCHEMA.to_string(),
        profile: inner.set.map.profile.clone(),
        network: inner.set.map.network.clone(),
        genesis_hash: inner.set.map.genesis_hash.clone(),
        shards: inner.set.shards.len(),
        start_height: inner.set.map.start_height,
        covered_through: last.end_height,
        directory_scheme: inner.directory_params.scheme.clone(),
        directory_setup_seed: Table::Directory.setup_seed(),
        pages_scheme: inner.pages_params.scheme.clone(),
        pages_setup_seed: Table::Pages.setup_seed(),
    };
    json(
        StatusCode::OK,
        serde_json::to_value(body).expect("init json"),
    )
}

async fn setup(
    State(state): State<ServiceState>,
    AxumPath((shard_id, table, segment)): AxumPath<(u64, String, u32)>,
) -> Response {
    let Some(table) = parse_table(&table) else {
        return bad_request("unknown table");
    };
    let segments = match state.segments(shard_id, table) {
        Ok(segments) => segments,
        Err(error) => return bad_request(error),
    };
    let runtime = match state.runtime(shard_id, table, segment) {
        Ok(runtime) => runtime,
        Err(error) => return bad_request(error),
    };
    let body = SetupResponse {
        shard_id,
        table: table.as_str().to_string(),
        segment,
        segments,
        public_params: BASE64_STANDARD.encode(&runtime.public_params),
        public_params_sha256: runtime.public_params_sha256.clone(),
        public_params_epoch: hex::encode(runtime.public_params_epoch),
    };
    json(
        StatusCode::OK,
        serde_json::to_value(body).expect("setup json"),
    )
}

async fn query(
    State(state): State<ServiceState>,
    AxumPath((shard_id, table)): AxumPath<(u64, String)>,
    body: axum::body::Bytes,
) -> Response {
    let Some(table) = parse_table(&table) else {
        return bad_request("unknown table");
    };
    let segments = match state.segments(shard_id, table) {
        Ok(segments) => segments,
        Err(error) => return bad_request(error),
    };
    // Every segment answers the same query, and the results come back in
    // segment order. The client keeps the row whose contents it can identify
    // and discards the rest; asking only the segment that holds the script
    // would disclose the script's placement, which is a function of the script.
    let mut answer = Vec::new();
    for segment in 0..segments {
        let runtime = match state.runtime(shard_id, table, segment) {
            Ok(runtime) => runtime,
            Err(error) => return bad_request(error),
        };
        match runtime.evaluate(state.shared(table), shard_id, &body) {
            Ok(response) => answer.extend(response),
            Err(error) => return bad_request(error),
        }
    }
    (
        StatusCode::OK,
        [("content-type", "application/octet-stream")],
        answer,
    )
        .into_response()
}
