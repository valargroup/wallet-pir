//! Table runtimes and the HTTP surface.
//!
//! One process holds both tables of one generation. There is no sharding: the
//! tables of a bounded-range generation are small enough to serve from a single
//! process, and the point of this service is to put real transport under a
//! measurement, not to reproduce the coordinator's distribution.

use crate::generation::{LoadedGeneration, LoadedTable};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;

use enhance_pir_server::ipir::{deserialize_first_dim_query, RowPlaintextIter};
use inspiring::{QueryPackPreprocessed, RlweParams, TopKeyImages};
use ipir_sp::client::reusable::QueryPool;
use ipir_sp::serialize::{deserialize_packing_keys, serialized_packing_keys_len};
use ipir_sp::server::IPIRServer;
use ipir_sp::server::{
    build_pack_preprocessed_blocks, pack_intermediate_blocks, published_c1_rows,
};
use ipir_sp::{IPIRClient, YpirSchemeParams};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use transparent_history_pir::types::{
    public_params_commitment, HistorySession, HistoryTableGeneration, HistoryTableSession, Table,
    NETWORK, PROTOCOL_REVISION, PUBLIC_SETS, SCHEMA_VERSION,
};

fn seed_bytes(seed: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    bytes
}

/// Largest batch body accepted, computed from the geometry at build time.
fn max_query_bytes(rlwe: &RlweParams, params: &YpirSchemeParams) -> usize {
    let switched = (params.db_rows * params.query_bits).div_ceil(8);
    12 + serialized_packing_keys_len(rlwe) + 1 + PUBLIC_SETS * (1 + switched)
}

/// Server-side state for one published public matrix set.
pub struct SetRuntime {
    pub preprocessed: Vec<QueryPackPreprocessed<'static>>,
    pub published_c1: Vec<u8>,
}

pub struct TableRuntime {
    pub table: Table,
    /// Leaked so the packing preprocessing and top key images, which borrow it,
    /// can be held for the process lifetime. One generation is loaded once at
    /// startup and never replaced, so this leaks a fixed, small amount rather
    /// than growing: the coordinator does the same for the same reason.
    pub rlwe: &'static RlweParams,
    pub params: YpirSchemeParams,
    pub server: IPIRServer<u16>,
    /// One entry per published set; a query names the slot it used.
    pub sets: Vec<SetRuntime>,
    /// SHA-256 of each set's published bytes, in slot order.
    pub set_digests: Vec<[u8; 32]>,
    pub top_key_images: TopKeyImages<'static>,
    pub public_params_sha256: String,
    pub public_params_epoch: [u8; 8],
    pub row_bytes: u32,
    pub rows: u64,
}

impl TableRuntime {
    /// Build the serving state for one table.
    ///
    /// Expensive and done once at startup: encoding the rows, the offline
    /// precomputation and the packing preprocessing all scale with the table,
    /// and the pages table is the large one.
    pub fn build(table: Table, loaded: &LoadedTable) -> Result<Self, String> {
        let rows = loaded.geometry.rows;
        let row_bytes = loaded.geometry.row_bytes;
        let (rlwe, params) = ipir_sp::params_for_simplepir(rows, (row_bytes as u64) * 8)
            .map_err(|error| error.to_string())?;
        let rlwe: &'static RlweParams = Box::leak(Box::new(rlwe));

        let coefficients = RowPlaintextIter::new(
            &loaded.rows,
            row_bytes as usize,
            params.db_rows,
            params.db_cols,
            params.p.trailing_zeros() as usize,
        );
        let server = IPIRServer::<u16>::new_auto_kernel(params.clone(), coefficients, false, true);

        // The sets are derived from a published seed, so a client reproduces
        // them exactly. They are public: they carry no secret and no selection.
        // Precomputing several is what lets a client share one set of packing
        // keys across that many queries, which is where the upload saving is.
        let pool = QueryPool::new(
            IPIRClient::new(rlwe, &params),
            seed_bytes(table.setup_seed()),
            PUBLIC_SETS,
        )
        .map_err(|error| error.to_string())?;

        let mut sets = Vec::with_capacity(PUBLIC_SETS);
        let mut set_digests = Vec::with_capacity(PUBLIC_SETS);
        for set in pool.sets().iter().take(PUBLIC_SETS) {
            let crs_blocks = server
                .perform_offline_precomputation_simplepir(rlwe, set)
                .crs_blocks;
            let preprocessed =
                build_pack_preprocessed_blocks(rlwe, &crs_blocks).map_err(|e| e.to_string())?;
            let published_c1 = published_c1_rows(&preprocessed, rlwe.q);
            set_digests.push(<[u8; 32]>::from(Sha256::digest(&published_c1)));
            sets.push(SetRuntime {
                preprocessed,
                published_c1,
            });
        }
        // Every set has the same length; a client re-derives it from the
        // geometry, so a table whose sets disagreed would be unserveable rather
        // than quietly wrong.
        if sets
            .iter()
            .any(|set| set.published_c1.len() != sets[0].published_c1.len())
        {
            return Err("published sets have different lengths".to_string());
        }
        let top_key_images = TopKeyImages::build(rlwe);
        // One digest over every set's digest, so a client cannot be handed a
        // mixture assembled from different publications that each verify alone,
        // and a client holding only one set can still check it.
        let digest = public_params_commitment(&set_digests);
        let mut epoch = [0u8; 8];
        epoch.copy_from_slice(&digest[..8]);

        Ok(Self {
            table,
            rlwe,
            params,
            server,
            sets,
            set_digests,
            top_key_images,
            public_params_sha256: hex::encode(digest),
            public_params_epoch: epoch,
            row_bytes,
            rows,
        })
    }

    fn session(&self, generation_id: &str, generation: u64) -> HistoryTableSession {
        HistoryTableSession {
            generation: HistoryTableGeneration {
                schema_version: SCHEMA_VERSION,
                protocol_revision: PROTOCOL_REVISION.to_string(),
                network: NETWORK.to_string(),
                generation_id: generation_id.to_string(),
                generation,
                table: self.table,
                rows: self.rows,
                row_bytes: self.row_bytes,
                setup_seed: self.table.setup_seed(),
                table_sha256: String::new(),
            },
            scheme: self.params.clone(),
            public_sets: self.sets.len(),
            public_params_set_bytes: self.sets[0].published_c1.len() as u64,
            public_params_set_sha256: self.set_digests.iter().map(hex::encode).collect(),
            public_params_sha256: self.public_params_sha256.clone(),
            public_params_epoch: hex::encode(self.public_params_epoch),
        }
    }

    /// Answer one batch. Which rows were selected is never known here.
    ///
    /// The batch shares one set of packing keys across its queries, each naming
    /// the public set it was built against. Answering them together is what
    /// makes the shared keys a saving rather than a repetition.
    fn evaluate(&self, generation: u64, body: &[u8]) -> Result<Vec<u8>, String> {
        let prefix: [u8; 8] = body
            .get(..8)
            .ok_or_else(|| "batch is truncated".to_string())?
            .try_into()
            .expect("eight-byte generation");
        if u64::from_le_bytes(prefix) != generation {
            return Err("batch names a different generation".to_string());
        }
        let key_len = u32::from_le_bytes(
            body.get(8..12)
                .ok_or_else(|| "batch is truncated".to_string())?
                .try_into()
                .expect("four-byte key length"),
        ) as usize;
        if key_len != serialized_packing_keys_len(self.rlwe) {
            return Err("packing keys have the wrong length".to_string());
        }
        let keys_end = 12 + key_len;
        let packing_keys = deserialize_packing_keys(
            self.rlwe,
            body.get(12..keys_end)
                .ok_or_else(|| "batch is truncated".to_string())?,
        )
        .map_err(|e| e.to_string())?;

        let count = *body
            .get(keys_end)
            .ok_or_else(|| "batch is truncated".to_string())? as usize;
        if count == 0 || count > self.sets.len() {
            return Err(format!("a batch carries 1..={} queries", self.sets.len()));
        }
        let switched_len = (self.params.db_rows * self.params.query_bits).div_ceil(8);
        // Fixed length for every batch of a given count: a body that varied
        // with the selections would leak them through its size alone.
        if body.len() != keys_end + 1 + count * (1 + switched_len) {
            return Err("batch has the wrong fixed length".to_string());
        }

        let mut c2 = Vec::new();
        let mut seen = vec![false; self.sets.len()];
        let mut offset = keys_end + 1;
        for _ in 0..count {
            let slot = body[offset] as usize;
            offset += 1;
            let Some(set) = self.sets.get(slot) else {
                return Err("query names an unpublished set".to_string());
            };
            // One query per set per batch. Two queries under the same set would
            // share both matrix and secret, which is exactly the case that
            // exposes the selector difference by subtraction.
            if std::mem::replace(&mut seen[slot], true) {
                return Err("batch reuses a public set".to_string());
            }
            let query = deserialize_first_dim_query(
                self.rlwe,
                &self.params,
                &body[offset..offset + switched_len],
            )
            .map_err(|e| e.to_string())?;
            offset += switched_len;
            let intermediate = self.server.multiply_query(self.rlwe, &query);
            let packed = pack_intermediate_blocks(
                &intermediate,
                &packing_keys,
                &self.top_key_images,
                &set.preprocessed,
            )
            .map_err(|e| e.to_string())?;
            c2.extend(ipir_sp::modulus_switch::serialize_rlwe_response_bodies(
                &packed,
                self.params.q_prime_1,
            ));
        }

        let mut response = Vec::with_capacity(17 + c2.len());
        response.extend_from_slice(&generation.to_le_bytes());
        response.extend_from_slice(&self.public_params_epoch);
        response.push(count as u8);
        response.extend_from_slice(&c2);
        Ok(response)
    }
}

pub struct Inner {
    pub generation_id: String,
    pub generation: u64,
    pub start_height: u64,
    pub end_height: u64,
    pub anchor: String,
    pub directory: TableRuntime,
    pub pages: TableRuntime,
}

#[derive(Clone)]
pub struct ServiceState {
    inner: Arc<Inner>,
}

impl ServiceState {
    pub fn build(loaded: &LoadedGeneration) -> Result<Self, String> {
        let directory = TableRuntime::build(Table::Directory, &loaded.directory)?;
        let pages = TableRuntime::build(Table::Pages, &loaded.pages)?;
        Ok(Self {
            inner: Arc::new(Inner {
                generation_id: loaded.generation_id.clone(),
                generation: loaded.generation,
                start_height: loaded.manifest.start,
                end_height: loaded.manifest.end,
                anchor: loaded.manifest.anchor.clone(),
                directory,
                pages,
            }),
        })
    }

    pub fn inner(&self) -> &Arc<Inner> {
        &self.inner
    }

    fn table(&self, table: Table) -> &TableRuntime {
        match table {
            Table::Directory => &self.inner.directory,
            Table::Pages => &self.inner.pages,
        }
    }

    pub fn max_query_bytes(&self) -> usize {
        max_query_bytes(self.inner.directory.rlwe, &self.inner.directory.params).max(
            max_query_bytes(self.inner.pages.rlwe, &self.inner.pages.params),
        )
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
        .route("/v1/transparent-history/init", get(init))
        .route(
            "/v1/transparent-history/directory/query",
            post(directory_query),
        )
        .route("/v1/transparent-history/pages/query", post(pages_query))
        .route(
            "/v1/transparent-history/directory/params/:slot",
            get(directory_params),
        )
        .route(
            "/v1/transparent-history/pages/params/:slot",
            get(pages_params),
        )
        .with_state(state)
}

async fn health(State(state): State<ServiceState>) -> Response {
    let inner = state.inner();
    json(
        StatusCode::OK,
        serde_json::json!({
            "generation_id": inner.generation_id,
            "start_height": inner.start_height,
            "end_height": inner.end_height,
            "directory_rows": inner.directory.rows,
            "pages_rows": inner.pages.rows,
        }),
    )
}

async fn init(State(state): State<ServiceState>) -> Response {
    let inner = state.inner();
    let session = HistorySession {
        generation_id: inner.generation_id.clone(),
        start_height: inner.start_height,
        end_height: inner.end_height,
        anchor_block_hash: inner.anchor.clone(),
        directory: inner
            .directory
            .session(&inner.generation_id, inner.generation),
        pages: inner.pages.session(&inner.generation_id, inner.generation),
    };
    json(
        StatusCode::OK,
        serde_json::to_value(session).expect("session json"),
    )
}

/// Serve one published set's parameters.
///
/// Public and identical for every client, and the slot requested says nothing
/// about a selection: it says how many queries the client intends to batch,
/// which the fixed padding already fixes. Splitting the sets across requests is
/// what lets a client that will make one query avoid downloading four.
fn params(state: ServiceState, table: Table, slot: usize) -> Response {
    let runtime = state.table(table);
    match runtime.sets.get(slot) {
        Some(set) => (
            StatusCode::OK,
            [("content-type", "application/octet-stream")],
            set.published_c1.clone(),
        )
            .into_response(),
        None => json(
            StatusCode::NOT_FOUND,
            serde_json::json!({ "error": "no such published set" }),
        ),
    }
}

async fn directory_params(
    State(state): State<ServiceState>,
    axum::extract::Path(slot): axum::extract::Path<usize>,
) -> Response {
    params(state, Table::Directory, slot)
}

async fn pages_params(
    State(state): State<ServiceState>,
    axum::extract::Path(slot): axum::extract::Path<usize>,
) -> Response {
    params(state, Table::Pages, slot)
}

async fn directory_query(State(state): State<ServiceState>, body: axum::body::Bytes) -> Response {
    query(state, Table::Directory, body)
}

async fn pages_query(State(state): State<ServiceState>, body: axum::body::Bytes) -> Response {
    query(state, Table::Pages, body)
}

fn query(state: ServiceState, table: Table, body: axum::body::Bytes) -> Response {
    let generation = state.inner().generation;
    match state.table(table).evaluate(generation, &body) {
        Ok(response) => (
            StatusCode::OK,
            [("content-type", "application/octet-stream")],
            response,
        )
            .into_response(),
        Err(error) => json(
            StatusCode::BAD_REQUEST,
            serde_json::json!({ "error": error }),
        ),
    }
}
