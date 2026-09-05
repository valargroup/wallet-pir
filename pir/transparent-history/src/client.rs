//! Client for private transparent-history retrieval.
//!
//! Modelled on `pir/transparent-spend/src/client.rs`, which established the
//! validation this reuses: re-derive the scheme parameters locally and reject a
//! session whose published parameters do not match, and bind every response to
//! both the generation and the parameter epoch.

use crate::types::{
    HistorySession, HistoryTableGeneration, HistoryTableSession, Table, COLUMN_BITS, NETWORK,
    PROTOCOL_REVISION, PUBLIC_SETS, SCHEMA_VERSION,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use ipir_sp::bits::write_bits;
use ipir_sp::client::reusable::QueryPool;
use ipir_sp::modulus_switch::{published_c1_len, recover_published_c1};
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

/// Bytes actually moved for a batch, so a measurement charges real transport
/// rather than an estimate of it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ByteCharges {
    pub upload_bytes: u64,
    pub download_bytes: u64,
    pub queries: u64,
    /// Batches sent. Packing keys are uploaded once per batch, so this is what
    /// separates the reused-key cost from the fresh-key cost.
    pub batches: u64,
    pub key_upload_bytes: u64,
}

/// One batch of up to `PUBLIC_SETS` queries sharing a single set of packing keys.
///
/// Borrows the pool it was started from: the batch owns the secret those
/// queries were made under, and decoding needs it.
pub struct PreparedBatch<'a> {
    /// The wire body: generation, key length, keys, then each slot and query.
    pub body: Vec<u8>,
    key_bytes: usize,
    slots: Vec<usize>,
    batch: ipir_sp::client::reusable::ReusableBatch<'a>,
}

impl PreparedBatch<'_> {
    pub fn queries(&self) -> usize {
        self.slots.len()
    }
}

pub struct TableClient {
    generation: HistoryTableGeneration,
    params: YpirSchemeParams,
    rlwe: &'static inspiring::RlweParams,
    pool: QueryPool,
    /// One recovered `c1` per published set, indexed by slot.
    published_c1: Vec<Vec<Vec<u64>>>,
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
        if session.public_params.len() != PUBLIC_SETS {
            return Err(ClientError::Session(format!(
                "expected {PUBLIC_SETS} published sets, got {}",
                session.public_params.len()
            )));
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
        let rlwe: &'static inspiring::RlweParams = Box::leak(Box::new(rlwe));

        let mut decoded_sets = Vec::with_capacity(PUBLIC_SETS);
        let mut concatenated = Vec::new();
        for encoded in &session.public_params {
            let bytes = BASE64_STANDARD.decode(encoded.as_bytes())?;
            concatenated.extend_from_slice(&bytes);
            decoded_sets.push(bytes);
        }
        // One digest over every set together. Digesting each set separately
        // would accept a mixture assembled from different publications, each
        // individually well formed.
        let digest = Sha256::digest(&concatenated);
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
        let expected_len = blocks * published_c1_len(rlwe.d, rlwe.q);
        if decoded_sets.iter().any(|set| set.len() != expected_len) {
            return Err(ClientError::Session(
                "published parameter length mismatch".to_string(),
            ));
        }
        let published_c1 = decoded_sets
            .iter()
            .map(|set| recover_published_c1(set, rlwe.d, blocks, rlwe.q))
            .collect();

        let client = IPIRClient::new(rlwe, &expected_params);
        let pool = QueryPool::new(client, seed_bytes(generation.setup_seed), PUBLIC_SETS)
            .map_err(|error| ClientError::Pir(error.to_string()))?;

        Ok(Self {
            generation,
            params: expected_params,
            rlwe,
            pool,
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

    /// Build one batch of up to `PUBLIC_SETS` queries.
    ///
    /// The packing keys are serialized once for the whole batch, which is the
    /// entire point: they dominate a query's upload, so a batch of four costs
    /// roughly one key upload rather than four.
    pub fn prepare_batch(&self, rows: &[usize]) -> Result<PreparedBatch<'_>, ClientError> {
        if rows.is_empty() || rows.len() > PUBLIC_SETS {
            return Err(ClientError::Session(format!(
                "a batch carries 1..={PUBLIC_SETS} queries, got {}",
                rows.len()
            )));
        }
        if rows.iter().any(|&row| row >= self.params.db_rows) {
            return Err(ClientError::Session("row outside table".to_string()));
        }
        let mut batch = self.pool.start_batch();
        let keys = serialize_packing_keys(self.rlwe, batch.keys())
            .map_err(|error| ClientError::Pir(error.to_string()))?;

        let mut body = self.generation.generation.to_le_bytes().to_vec();
        body.extend((keys.len() as u32).to_le_bytes());
        body.extend(&keys);
        body.push(rows.len() as u8);
        let mut slots = Vec::with_capacity(rows.len());
        for &row in rows {
            let query = batch
                .next_query(row)
                .map_err(|error| ClientError::Pir(error.to_string()))?;
            let slot = query.slot();
            if slot >= PUBLIC_SETS {
                return Err(ClientError::Pir("slot outside published sets".to_string()));
            }
            body.push(slot as u8);
            body.extend(query.bytes());
            slots.push(slot);
        }
        Ok(PreparedBatch {
            body,
            key_bytes: keys.len(),
            slots,
            batch,
        })
    }

    /// A batch of uniformly random rows, used to pad to a fixed shape.
    ///
    /// Indistinguishable on the wire from a real batch, which is the point:
    /// without padding the number of queries reveals how many blocks matched,
    /// and hiding which blocks matched while publishing how many is not a
    /// meaningful improvement.
    pub fn prepare_pad_batch(&self, count: usize) -> Result<PreparedBatch<'_>, ClientError> {
        let rows: Vec<usize> = (0..count)
            .map(|_| OsRng.gen_range(0..self.params.db_rows))
            .collect();
        self.prepare_batch(&rows)
    }

    pub fn decode_batch(
        &self,
        batch: PreparedBatch<'_>,
        response: &[u8],
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        if response.get(..8) != Some(self.generation.generation.to_le_bytes().as_slice()) {
            return Err(ClientError::Response("generation mismatch".to_string()));
        }
        if response.get(8..16) != Some(self.epoch.as_slice()) {
            return Err(ClientError::Response(
                "parameter epoch mismatch".to_string(),
            ));
        }
        let count = *response
            .get(16)
            .ok_or_else(|| ClientError::Response("response is truncated".to_string()))?
            as usize;
        if count != batch.slots.len() {
            return Err(ClientError::Response(
                "response does not answer every query in the batch".to_string(),
            ));
        }
        let body = &response[17..];
        if body.len() % count != 0 {
            return Err(ClientError::Response(
                "response bodies are not uniform".to_string(),
            ));
        }
        let each = body.len() / count;

        let mut rows = Vec::with_capacity(count);
        for (index, &slot) in batch.slots.iter().enumerate() {
            let (values, error) = batch.batch.decode_with_margin(
                &self.published_c1[slot],
                &body[index * each..(index + 1) * each],
            );
            // A decode that only just fit is not a success. The margin check is
            // what separates a correct row from one that happened to round the
            // right way, and a silently wrong row here becomes a wrong balance.
            if error >= self.rlwe.delta / 8 {
                return Err(ClientError::Response(format!(
                    "decoding error margin {error} is too close to the threshold"
                )));
            }
            if values.len() != self.params.db_cols {
                return Err(ClientError::Response(
                    "decoded row has the wrong column count".to_string(),
                ));
            }
            let mut bytes = vec![0u8; self.row_bytes()];
            for (column, &value) in values.iter().enumerate() {
                write_bits(&mut bytes, value, column * COLUMN_BITS, COLUMN_BITS);
            }
            rows.push(bytes);
        }
        Ok(rows)
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
    /// Queries are sent in batches of at most `PUBLIC_SETS`, sharing one set of
    /// packing keys per batch. Real selections come first and padding follows,
    /// but both are decoded, so a pad costs a real query's work. Padding is
    /// rounded up to a whole batch: a short final batch would be visibly
    /// different from a full one and would undo the padding it is there to
    /// provide.
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
        let total = pad_to.div_ceil(PUBLIC_SETS) * PUBLIC_SETS;
        let mut charges = ByteCharges::default();
        let mut decoded = Vec::with_capacity(rows.len());

        let mut sent = 0;
        while sent < total {
            let count = PUBLIC_SETS.min(total - sent);
            let real = rows.len().saturating_sub(sent).min(count);
            let batch = if real == count {
                client.prepare_batch(&rows[sent..sent + count])?
            } else if real == 0 {
                client.prepare_pad_batch(count)?
            } else {
                // A partly real batch: the real selections, then random rows to
                // fill it, so every batch on the wire has the same shape.
                let mut selection = rows[sent..sent + real].to_vec();
                selection.extend((0..count - real).map(|_| OsRng.gen_range(0..client.rows())));
                client.prepare_batch(&selection)?
            };
            charges.upload_bytes += batch.body.len() as u64;
            charges.key_upload_bytes += batch.key_bytes as u64;
            charges.queries += batch.queries() as u64;
            charges.batches += 1;

            let response = self.request(table, &batch.body).await?;
            charges.download_bytes += response.len() as u64;
            let mut answered = client.decode_batch(batch, &response)?;
            answered.truncate(real);
            decoded.extend(answered);
            sent += count;
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
