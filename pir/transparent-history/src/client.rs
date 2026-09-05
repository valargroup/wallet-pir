//! Client for private transparent-history retrieval.
//!
//! Modelled on `pir/transparent-spend/src/client.rs`, which established the
//! validation this reuses: re-derive the scheme parameters locally and reject a
//! session whose published parameters do not match, and bind every response to
//! both the generation and the parameter epoch.

use crate::types::{
    HistorySession, HistoryTableGeneration, HistoryTableSession, Table, NETWORK, PROTOCOL_REVISION,
    SCHEMA_VERSION,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use ipir_sp::modulus_switch::{published_c1_len, recover_published_c1, response_body_len};
use ipir_sp::serialize::serialize_packing_keys;
use ipir_sp::{IPIRClient, YpirSchemeParams};
use rand::{rngs::OsRng, Rng};
use sha2::{Digest, Sha256};

/// Largest response this client will read into memory, per query.
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
/// Largest init document this client will read.
const MAX_INIT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("base64 error: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("incompatible session: {0}")]
    Session(String),
    #[error("PIR error: {0}")]
    Pir(String),
    #[error("malformed response: {0}")]
    Response(String),
}

pub struct PreparedQuery {
    pub body: Vec<u8>,
    seed: ipir_sp::IPIRSeed,
}

/// Bytes actually moved for a batch, so a measurement charges real transport
/// rather than an estimate of it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ByteCharges {
    pub upload_bytes: u64,
    pub download_bytes: u64,
    pub queries: u64,
}

pub struct TableClient {
    generation: HistoryTableGeneration,
    params: YpirSchemeParams,
    client: IPIRClient,
    setup: Vec<Vec<u64>>,
    published_c1: Vec<Vec<u64>>,
    epoch: [u8; 8],
}

impl TableClient {
    pub fn new(session: HistoryTableSession, table: Table) -> Result<Self, ClientError> {
        let generation = session.generation;
        if generation.schema_version != SCHEMA_VERSION
            || generation.protocol_revision != PROTOCOL_REVISION
            || generation.network != NETWORK
            || generation.table != table
            || generation.setup_seed != table.setup_seed()
            || generation.rows == 0
            || generation.row_bytes == 0
        {
            return Err(ClientError::Session("invalid table metadata".to_string()));
        }

        // Re-derive rather than trust: the server sends its parameters, and a
        // client that adopted them would decode against whatever geometry the
        // server chose, including one that leaks the selection.
        let item_size_bits = (generation.row_bytes as u64) * 8;
        let (rlwe, expected_params) =
            ipir_sp::params_for_simplepir(generation.rows, item_size_bits)
                .map_err(|error| ClientError::Pir(error.to_string()))?;
        if session.scheme != expected_params {
            return Err(ClientError::Session(
                "published scheme parameters do not match the geometry".to_string(),
            ));
        }

        let public_params = BASE64_STANDARD.decode(session.public_params.as_bytes())?;
        let digest = Sha256::digest(&public_params);
        if hex::encode(digest) != session.public_params_sha256 {
            return Err(ClientError::Session(
                "published parameter digest mismatch".to_string(),
            ));
        }
        let mut epoch = [0u8; 8];
        epoch.copy_from_slice(&digest[..8]);
        if hex::encode(epoch) != session.public_params_epoch {
            return Err(ClientError::Session(
                "published parameter epoch mismatch".to_string(),
            ));
        }
        let blocks = expected_params.db_cols / rlwe.d;
        if public_params.len() != blocks * published_c1_len(rlwe.d, rlwe.q) {
            return Err(ClientError::Session(
                "published parameter length mismatch".to_string(),
            ));
        }
        let published_c1 = recover_published_c1(&public_params, rlwe.d, blocks, rlwe.q);
        let client = IPIRClient::new(&rlwe, &expected_params);
        let setup = client
            .generate_public_query_setup_simplepir_from_seed(seed_bytes(generation.setup_seed));

        Ok(Self {
            generation,
            params: expected_params,
            client,
            setup,
            published_c1,
            epoch,
        })
    }

    pub fn rows(&self) -> usize {
        self.params.db_rows
    }

    pub fn row_bytes(&self) -> usize {
        self.generation.row_bytes as usize
    }

    pub fn prepare(&self, row: usize) -> Result<PreparedQuery, ClientError> {
        if row >= self.params.db_rows {
            return Err(ClientError::Session("row outside table".to_string()));
        }
        let (query, packing_keys, seed) =
            self.client.generate_fresh_query_simplepir(&self.setup, row);
        let mut body = self.generation.generation.to_le_bytes().to_vec();
        body.extend(
            serialize_packing_keys(self.client.rlwe_params(), &packing_keys)
                .map_err(|error| ClientError::Pir(error.to_string()))?,
        );
        body.extend(query.to_switched_bytes(self.client.rlwe_params().q, self.params.query_bits));
        Ok(PreparedQuery { body, seed })
    }

    /// A query for a uniformly random row.
    ///
    /// Used to pad a batch to a fixed size. It is indistinguishable on the wire
    /// from a real selection, which is the point: without padding the number of
    /// queries reveals how many blocks matched, and hiding which blocks matched
    /// while publishing how many is not a meaningful improvement.
    pub fn prepare_pad(&self) -> Result<PreparedQuery, ClientError> {
        self.prepare(OsRng.gen_range(0..self.params.db_rows))
    }

    pub fn decode(&self, query: PreparedQuery, response: &[u8]) -> Result<Vec<u8>, ClientError> {
        if response.get(..8) != Some(self.generation.generation.to_le_bytes().as_slice()) {
            return Err(ClientError::Response("generation mismatch".to_string()));
        }
        if response.get(8..16) != Some(self.epoch.as_slice()) {
            return Err(ClientError::Response(
                "parameter epoch mismatch".to_string(),
            ));
        }
        let expected = (self.params.db_cols / self.client.rlwe_params().d)
            * response_body_len(self.client.rlwe_params().d, self.params.q_prime_1);
        if response.len() != 16 + expected {
            return Err(ClientError::Response(
                "response length mismatch".to_string(),
            ));
        }
        let decoded =
            self.client
                .decode_response_simplepir(query.seed, &self.published_c1, &response[16..]);
        decoded
            .get(..self.row_bytes())
            .map(<[u8]>::to_vec)
            .ok_or_else(|| ClientError::Response("decoded row is too short".to_string()))
    }
}

fn seed_bytes(seed: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    bytes
}

pub struct TransparentHistoryClient {
    http: reqwest::Client,
    base_url: String,
    session: HistorySession,
    directory: TableClient,
    pages: TableClient,
}

impl TransparentHistoryClient {
    pub async fn connect(base_url: &str) -> Result<Self, ClientError> {
        let base_url = base_url.trim_end_matches('/').to_string();
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()?;
        let response = http
            .get(format!("{base_url}/v1/transparent-history/init"))
            .send()
            .await?
            .error_for_status()?;
        let body = response.bytes().await?;
        if body.len() > MAX_INIT_BYTES {
            return Err(ClientError::Session("init document too large".to_string()));
        }
        let session: HistorySession = serde_json::from_slice(&body)?;

        // Both tables must describe the same generation, or a client could be
        // steered into reading page locators from one generation against pages
        // from another.
        if session.directory.generation.generation_id != session.generation_id
            || session.pages.generation.generation_id != session.generation_id
        {
            return Err(ClientError::Session(
                "tables come from different generations".to_string(),
            ));
        }

        let directory = TableClient::new(session.directory.clone(), Table::Directory)?;
        let pages = TableClient::new(session.pages.clone(), Table::Pages)?;
        Ok(Self {
            http,
            base_url,
            session,
            directory,
            pages,
        })
    }

    pub fn session(&self) -> &HistorySession {
        &self.session
    }

    pub fn table(&self, table: Table) -> &TableClient {
        match table {
            Table::Directory => &self.directory,
            Table::Pages => &self.pages,
        }
    }

    /// Fetch `rows` from one table, padded to exactly `pad_to` queries.
    ///
    /// Returns the decoded rows in the order requested, and the bytes actually
    /// moved including the padding. Padding queries are issued and their
    /// responses decoded and discarded; skipping the decode would leave a
    /// timing difference between a real query and a pad.
    pub async fn fetch_rows(
        &self,
        table: Table,
        rows: &[usize],
        pad_to: usize,
    ) -> Result<(Vec<Vec<u8>>, ByteCharges), ClientError> {
        if rows.len() > pad_to {
            return Err(ClientError::Session(
                "more selections than the fixed query budget allows".to_string(),
            ));
        }
        let client = self.table(table);
        let mut charges = ByteCharges::default();
        let mut decoded = Vec::with_capacity(rows.len());
        for index in 0..pad_to {
            let real = index < rows.len();
            let query = if real {
                client.prepare(rows[index])?
            } else {
                client.prepare_pad()?
            };
            charges.upload_bytes += query.body.len() as u64;
            charges.queries += 1;
            let response = self.request(table, &query.body).await?;
            charges.download_bytes += response.len() as u64;
            let row = client.decode(query, &response)?;
            if real {
                decoded.push(row);
            }
        }
        Ok((decoded, charges))
    }

    async fn request(&self, table: Table, body: &[u8]) -> Result<Vec<u8>, ClientError> {
        let response = self
            .http
            .post(format!(
                "{}/v1/transparent-history/{}/query",
                self.base_url,
                table.as_str()
            ))
            .header("content-type", "application/octet-stream")
            .body(body.to_vec())
            .send()
            .await?
            .error_for_status()?;
        let bytes = response.bytes().await?;
        if bytes.len() > MAX_RESPONSE_BYTES {
            return Err(ClientError::Response("response too large".to_string()));
        }
        Ok(bytes.to_vec())
    }
}
