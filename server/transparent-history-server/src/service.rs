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
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use enhance_pir_server::ipir::{deserialize_first_dim_query, RowPlaintextIter};
use inspiring::{QueryPackPreprocessed, RlweParams, TopKeyImages};
use ipir_sp::serialize::{deserialize_packing_keys, serialized_packing_keys_len};
use ipir_sp::server::IPIRServer;
use ipir_sp::server::{
    build_pack_preprocessed_blocks, pack_intermediate_blocks, published_c1_rows,
};
use ipir_sp::{IPIRClient, YpirSchemeParams};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use transparent_history_pir::types::{
    HistorySession, HistoryTableGeneration, HistoryTableSession, Table, NETWORK, PROTOCOL_REVISION,
    SCHEMA_VERSION,
};

/// Largest query body accepted, computed from the geometry at build time.
fn max_query_bytes(rlwe: &RlweParams, params: &YpirSchemeParams) -> usize {
    8 + serialized_packing_keys_len(rlwe) + (params.db_rows * params.query_bits).div_ceil(8)
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
    pub preprocessed: Vec<QueryPackPreprocessed<'static>>,
    pub top_key_images: TopKeyImages<'static>,
    pub public_params: Vec<u8>,
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

        // The setup is derived from a published seed, so a client reproduces it
        // exactly. It is public: it carries no secret and no selection.
        let mut seed = [0u8; 32];
        seed[..8].copy_from_slice(&table.setup_seed().to_le_bytes());
        let setup =
            IPIRClient::new(rlwe, &params).generate_public_query_setup_simplepir_from_seed(seed);
        let crs_blocks = server
            .perform_offline_precomputation_simplepir(rlwe, &setup)
            .crs_blocks;
        let preprocessed =
            build_pack_preprocessed_blocks(rlwe, &crs_blocks).map_err(|e| e.to_string())?;
        let top_key_images = TopKeyImages::build(rlwe);
        let public_params = published_c1_rows(&preprocessed, rlwe.q);
        let digest = Sha256::digest(&public_params);
        let mut epoch = [0u8; 8];
        epoch.copy_from_slice(&digest[..8]);

        Ok(Self {
            table,
            rlwe,
            params,
            server,
            preprocessed,
            top_key_images,
            public_params,
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
            public_params: BASE64_STANDARD.encode(&self.public_params),
            public_params_sha256: self.public_params_sha256.clone(),
            public_params_epoch: hex::encode(self.public_params_epoch),
        }
    }

    /// Answer one query. The row selected is never known to this function.
    fn evaluate(&self, generation: u64, body: &[u8]) -> Result<Vec<u8>, String> {
        let prefix: [u8; 8] = body
            .get(..8)
            .ok_or_else(|| "query is truncated".to_string())?
            .try_into()
            .expect("eight-byte generation");
        if u64::from_le_bytes(prefix) != generation {
            return Err("query names a different generation".to_string());
        }
        let packing_len = serialized_packing_keys_len(self.rlwe);
        let switched_len = (self.params.db_rows * self.params.query_bits).div_ceil(8);
        // A fixed length for every query: a body that varies with the selection
        // would leak through its size alone.
        if body.len() != 8 + packing_len + switched_len {
            return Err("query has the wrong fixed length".to_string());
        }
        let packing_keys = deserialize_packing_keys(self.rlwe, &body[8..8 + packing_len])
            .map_err(|e| e.to_string())?;
        let query = deserialize_first_dim_query(self.rlwe, &self.params, &body[8 + packing_len..])
            .map_err(|e| e.to_string())?;
        let intermediate = self.server.multiply_query(self.rlwe, &query);
        let packed = pack_intermediate_blocks(
            &intermediate,
            &packing_keys,
            &self.top_key_images,
            &self.preprocessed,
        )
        .map_err(|e| e.to_string())?;
        let c2 =
            ipir_sp::modulus_switch::serialize_rlwe_response_bodies(&packed, self.params.q_prime_1);
        let mut response = Vec::with_capacity(16 + c2.len());
        response.extend_from_slice(&generation.to_le_bytes());
        response.extend_from_slice(&self.public_params_epoch);
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
