//! Client for private transparent-history retrieval.
//!
//! Modelled on `pir/transparent-spend/src/client.rs`, which established the
//! validation this reuses: re-derive the scheme parameters locally and reject a
//! session whose published parameters do not match, and bind every response to
//! both the generation and the parameter epoch.

use crate::types::{
    public_params_commitment, HistorySession, HistoryTableGeneration, HistoryTableSession, Table,
    COLUMN_BITS, MAX_PUBLIC_SETS, NETWORK, PROTOCOL_REVISION, SCHEMA_VERSION,
};
use ipir_sp::bits::write_bits;
use ipir_sp::client::reusable::QueryPool;
use ipir_sp::modulus_switch::{published_c1_len, recover_published_c1, response_body_len};
use ipir_sp::serialize::{serialize_packing_keys, serialized_packing_keys_len};
use ipir_sp::{IPIRClient, YpirSchemeParams};
use rand::{rngs::OsRng, Rng};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

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
    /// Published parameters downloaded for this call, in bytes, and the number
    /// of sets that bought. This is the cost reuse is traded against, so it is
    /// counted in bytes actually received rather than inferred from the plan.
    pub setup_download_bytes: u64,
    pub public_sets: u64,
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

/// Wire framing charged per batch and per query, so the policy compares real
/// bodies rather than payloads.
///
/// A batch body opens with the eight-byte generation, a four-byte key length
/// and a one-byte count; each query adds the one-byte slot it used. A response
/// opens with the generation, the parameter epoch and a one-byte count.
const BATCH_HEADER_BYTES: u64 = 8 + 4 + 1;
const QUERY_HEADER_BYTES: u64 = 1;
const RESPONSE_HEADER_BYTES: u64 = 8 + 8 + 1;

/// What one table's retrieval costs, per unit, for the policy to add up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableCosts {
    /// Published parameters for one public set.
    pub set_bytes: u64,
    /// Serialized packing keys, uploaded once per batch.
    pub key_bytes: u64,
    /// The encrypted selector, uploaded once per query.
    pub selector_bytes: u64,
    /// One query's response body.
    pub response_bytes: u64,
    /// Sets this client already holds and would not download again.
    pub held_sets: usize,
}

/// How a sync will query one table: how many public sets to download, and how
/// many queries to put under one set of packing keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyPlan {
    /// Sets to hold, which is also the batch size.
    pub sets: usize,
    pub batches: usize,
    /// Queries issued, including the padding that rounds up to whole batches.
    pub queries: usize,
    /// Total bytes this plan is predicted to move.
    pub bytes: u64,
}

impl TableCosts {
    /// Bytes a plan of `sets` costs for `pad_to` padded selections.
    ///
    /// Padding rounds up to whole batches, so a larger batch can add queries;
    /// those extra queries carry a full selector and a full response and are
    /// charged here. Leaving them out is what makes reuse look free.
    pub fn plan_of(&self, pad_to: usize, sets: usize) -> KeyPlan {
        let batches = pad_to.div_ceil(sets);
        let queries = batches * sets;
        let bytes = self.set_bytes * (sets.saturating_sub(self.held_sets)) as u64
            + batches as u64 * (BATCH_HEADER_BYTES + self.key_bytes + RESPONSE_HEADER_BYTES)
            + queries as u64 * (QUERY_HEADER_BYTES + self.selector_bytes + self.response_bytes);
        KeyPlan {
            sets,
            batches,
            queries,
            bytes,
        }
    }

    /// The cheapest plan for `pad_to` padded selections against `public_sets`
    /// published sets.
    ///
    /// This is the per-table decision. Sharing packing keys saves
    /// `(queries - batches) * key_bytes` of upload and costs the published
    /// parameters of every extra set, once. Which side wins depends on the
    /// query count of *this* table in *this* sync: a sparse wallet making one
    /// directory lookup and no page lookups would pay four sets of both tables'
    /// parameters for a saving it never collects.
    ///
    /// Ties go to the smaller plan, so a sync that gains nothing measurable
    /// does not acquire published parameters it would have to hold.
    pub fn best_plan(&self, pad_to: usize, public_sets: usize) -> KeyPlan {
        if pad_to == 0 {
            return KeyPlan {
                sets: 0,
                batches: 0,
                queries: 0,
                bytes: 0,
            };
        }
        (1..=public_sets.max(1))
            .map(|sets| self.plan_of(pad_to, sets))
            .min_by_key(|plan| (plan.bytes, plan.sets))
            .expect("at least one set")
    }
}

pub struct TableClient {
    generation: HistoryTableGeneration,
    params: YpirSchemeParams,
    rlwe: &'static inspiring::RlweParams,
    pool: QueryPool,
    /// SHA-256 the session published for each set, indexed by slot.
    set_digests: Vec<[u8; 32]>,
    /// Encoded length of one published set, re-derived from the geometry.
    set_bytes: usize,
    /// One recovered `c1` per published set, indexed by slot, filled in as the
    /// sets this sync decided to use are downloaded. A slot left empty is a
    /// set this client chose not to pay for.
    published_c1: Vec<OnceLock<Vec<Vec<u64>>>>,
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
        if session.public_sets == 0
            || session.public_sets > MAX_PUBLIC_SETS
            || session.public_params_set_sha256.len() != session.public_sets
        {
            return Err(ClientError::Session(format!(
                "a table publishes 1..={MAX_PUBLIC_SETS} sets and one digest each, got {} sets and {} digests",
                session.public_sets,
                session.public_params_set_sha256.len()
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

        // Commit to the whole publication from the per-set digests, so a client
        // that will download one set still binds it to the same publication
        // every other set came from.
        let mut set_digests = Vec::with_capacity(session.public_sets);
        for encoded in &session.public_params_set_sha256 {
            let raw = hex::decode(encoded)
                .map_err(|_| ClientError::Session("malformed set digest".to_string()))?;
            let digest: [u8; 32] = raw
                .try_into()
                .map_err(|_| ClientError::Session("malformed set digest".to_string()))?;
            set_digests.push(digest);
        }
        let digest = public_params_commitment(&set_digests);
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

        // Re-derived, not adopted: the length of a set is fixed by the
        // geometry, so a session announcing another one is describing
        // parameters this client could not decode against anyway.
        let blocks = expected_params.db_cols / rlwe.d;
        let set_bytes = blocks * published_c1_len(rlwe.d, rlwe.q);
        if session.public_params_set_bytes != set_bytes as u64 {
            return Err(ClientError::Session(
                "published parameter length mismatch".to_string(),
            ));
        }

        let client = IPIRClient::new(rlwe, &expected_params);
        let pool = QueryPool::new(
            client,
            seed_bytes(generation.setup_seed),
            session.public_sets,
        )
        .map_err(|error| ClientError::Pir(error.to_string()))?;

        Ok(Self {
            generation,
            params: expected_params,
            rlwe,
            pool,
            set_digests,
            set_bytes,
            published_c1: (0..session.public_sets).map(|_| OnceLock::new()).collect(),
            epoch,
        })
    }

    /// How many public sets this table publishes.
    pub fn public_sets(&self) -> usize {
        self.published_c1.len()
    }

    /// Encoded length of one published set.
    pub fn set_bytes(&self) -> usize {
        self.set_bytes
    }

    /// Whether this client already holds slot `slot`'s parameters.
    pub fn holds_set(&self, slot: usize) -> bool {
        self.published_c1
            .get(slot)
            .is_some_and(|cell| cell.get().is_some())
    }

    /// Adopt one downloaded set after checking it against the session.
    ///
    /// The digest check is what makes fetching sets separately safe: the
    /// session commits to each one, so a set served on its own cannot be
    /// substituted for a set from another publication.
    pub fn install_set(&self, slot: usize, bytes: &[u8]) -> Result<(), ClientError> {
        let cell = self
            .published_c1
            .get(slot)
            .ok_or_else(|| ClientError::Session("slot outside published sets".to_string()))?;
        if bytes.len() != self.set_bytes {
            return Err(ClientError::Session(
                "published parameter length mismatch".to_string(),
            ));
        }
        if Sha256::digest(bytes).as_slice() != self.set_digests[slot] {
            return Err(ClientError::Session(
                "published set does not match its digest in the session".to_string(),
            ));
        }
        let blocks = self.params.db_cols / self.rlwe.d;
        // Set once. A second, differing set for a slot answered against the
        // first would decode to a different row without any check failing.
        let _ = cell.set(recover_published_c1(
            bytes,
            self.rlwe.d,
            blocks,
            self.rlwe.q,
        ));
        Ok(())
    }

    pub fn rows(&self) -> usize {
        self.params.db_rows
    }

    pub fn row_bytes(&self) -> usize {
        self.generation.row_bytes as usize
    }

    /// Per-unit costs for this table, all re-derived from the geometry.
    pub fn costs(&self) -> TableCosts {
        let held = (0..self.public_sets())
            .filter(|&slot| self.holds_set(slot))
            .count();
        TableCosts {
            set_bytes: self.set_bytes as u64,
            key_bytes: serialized_packing_keys_len(self.rlwe) as u64,
            selector_bytes: (self.params.db_rows * self.params.query_bits).div_ceil(8) as u64,
            response_bytes: ((self.params.db_cols / self.rlwe.d)
                * response_body_len(self.rlwe.d, self.params.q_prime_1))
                as u64,
            held_sets: held,
        }
    }

    /// The cheapest plan for `pad_to` padded selections against this table.
    pub fn plan(&self, pad_to: usize) -> KeyPlan {
        self.costs().best_plan(pad_to, self.public_sets())
    }

    /// Build one batch of up to `public_sets()` queries.
    ///
    /// The packing keys are serialized once for the whole batch, which is the
    /// entire point: they dominate a query's upload, so a batch of four costs
    /// roughly one key upload rather than four.
    pub fn prepare_batch(&self, rows: &[usize]) -> Result<PreparedBatch<'_>, ClientError> {
        if rows.is_empty() || rows.len() > self.public_sets() {
            return Err(ClientError::Session(format!(
                "a batch carries 1..={} queries, got {}",
                self.public_sets(),
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
            // A slot whose parameters were not downloaded cannot be decoded, so
            // refuse before sending rather than after paying for the response.
            if !self.holds_set(slot) {
                return Err(ClientError::Session(format!(
                    "slot {slot} was not downloaded for this table"
                )));
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
        if !body.len().is_multiple_of(count) {
            return Err(ClientError::Response(
                "response bodies are not uniform".to_string(),
            ));
        }
        let each = body.len() / count;

        let mut rows = Vec::with_capacity(count);
        for (index, &slot) in batch.slots.iter().enumerate() {
            let published = self.published_c1[slot]
                .get()
                .ok_or_else(|| ClientError::Session(format!("slot {slot} was not downloaded")))?;
            let (values, error) = batch
                .batch
                .decode_with_margin(published, &body[index * each..(index + 1) * each]);
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

    /// Download the published sets a plan needs, and charge what that cost.
    ///
    /// Only the slots this table will actually use are fetched. A sync that
    /// queries the directory once and the pages table not at all downloads one
    /// directory set and nothing for pages, where a session that inlined every
    /// set would have charged it for eight.
    async fn ensure_sets(&self, table: Table, sets: usize) -> Result<u64, ClientError> {
        let client = self.table(table);
        let mut downloaded = 0u64;
        for slot in 0..sets {
            if client.holds_set(slot) {
                continue;
            }
            let response = self
                .http
                .get(format!(
                    "{}/v1/transparent-history/{}/params/{}",
                    self.base_url,
                    table.as_str(),
                    slot
                ))
                .send()
                .await?
                .error_for_status()?;
            let bytes = response.bytes().await?;
            if bytes.len() > MAX_RESPONSE_BYTES {
                return Err(ClientError::Session(
                    "published set is too large".to_string(),
                ));
            }
            client.install_set(slot, &bytes)?;
            downloaded += bytes.len() as u64;
        }
        Ok(downloaded)
    }

    /// Fetch `rows` from one table, padded to exactly `pad_to` queries.
    ///
    /// The batch size is chosen per table by [`TableClient::plan`], from the
    /// number of queries this call will make against *this* table. Sharing
    /// packing keys across a batch is a saving only once enough queries are
    /// made to repay the extra published parameters, and that threshold differs
    /// between the two tables because their parameters differ in size.
    ///
    /// Real selections come first and padding follows, but both are decoded, so
    /// a pad costs a real query's work. Padding is rounded up to a whole batch:
    /// a short final batch would be visibly different from a full one and would
    /// undo the padding it is there to provide.
    pub async fn fetch_rows(
        &self,
        table: Table,
        rows: &[usize],
        pad_to: usize,
    ) -> Result<(Vec<Vec<u8>>, ByteCharges), ClientError> {
        self.fetch_rows_with(table, rows, pad_to, None).await
    }

    /// `fetch_rows` with the per-table decision overridden.
    ///
    /// Only for measurement: forcing a fixed number of sets is how the fresh
    /// and always-share baselines are produced under exactly the same
    /// accounting as the policy, so the comparison is not between two builds
    /// that charge differently. A wallet uses [`Self::fetch_rows`].
    pub async fn fetch_rows_with(
        &self,
        table: Table,
        rows: &[usize],
        pad_to: usize,
        sets: Option<usize>,
    ) -> Result<(Vec<Vec<u8>>, ByteCharges), ClientError> {
        if rows.len() > pad_to {
            return Err(ClientError::Session(
                "more selections than the fixed query budget allows".to_string(),
            ));
        }
        let client = self.table(table);
        let mut charges = ByteCharges::default();
        if pad_to == 0 {
            return Ok((Vec::new(), charges));
        }
        let plan = match sets {
            None => client.plan(pad_to),
            Some(forced) => {
                if forced == 0 || forced > client.public_sets() {
                    return Err(ClientError::Session(format!(
                        "forced plan of {forced} sets is outside the {} published",
                        client.public_sets()
                    )));
                }
                client.costs().plan_of(pad_to, forced)
            }
        };
        charges.setup_download_bytes = self.ensure_sets(table, plan.sets).await?;
        charges.public_sets = plan.sets as u64;

        let total = plan.queries;
        let mut decoded = Vec::with_capacity(rows.len());

        let mut sent = 0;
        while sent < total {
            let count = plan.sets.min(total - sent);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PUBLIC_SETS;
    use ipir_sp::modulus_switch::published_c1_len;

    /// The generation the HTTP evidence was measured over: directory
    /// 4,096 x 3,584 and pages 4,096 x 17,920.
    const DIRECTORY: (u64, u32) = (4096, 3584);
    const PAGES: (u64, u32) = (4096, 17920);

    fn costs_for((rows, row_bytes): (u64, u32)) -> TableCosts {
        let (rlwe, params) = ipir_sp::params_for_simplepir(rows, (row_bytes as u64) * 8).unwrap();
        let blocks = params.db_cols / rlwe.d;
        TableCosts {
            set_bytes: (blocks * published_c1_len(rlwe.d, rlwe.q)) as u64,
            key_bytes: serialized_packing_keys_len(&rlwe) as u64,
            selector_bytes: (params.db_rows * params.query_bits).div_ceil(8) as u64,
            response_bytes: (blocks * response_body_len(rlwe.d, params.q_prime_1)) as u64,
            held_sets: 0,
        }
    }

    /// The first query count at which a plan of `sets` beats fresh keys.
    fn beats_fresh_from(costs: &TableCosts, sets: usize) -> usize {
        (1..1000)
            .find(|&n| costs.plan_of(n, sets).bytes < costs.plan_of(n, 1).bytes)
            .expect("sharing pays somewhere")
    }

    /// The first query count at which a plan of `sets` is the cheapest one.
    fn chosen_from(costs: &TableCosts, sets: usize) -> usize {
        (1..1000)
            .find(|&n| costs.best_plan(n, PUBLIC_SETS).sets == sets)
            .expect("plan is chosen somewhere")
    }

    /// Both tables hold 4,096 rows, so a query against either uploads the same
    /// selector under the same size of packing keys. Only the published
    /// parameters differ, because they scale with row bytes and a page row is
    /// five times a directory row. That difference is the entire reason one
    /// decision cannot serve both tables.
    #[test]
    fn the_tables_differ_only_in_what_a_published_set_costs() {
        let directory = costs_for(DIRECTORY);
        let pages = costs_for(PAGES);

        assert_eq!(directory.key_bytes, pages.key_bytes);
        assert_eq!(directory.selector_bytes, pages.selector_bytes);
        assert_eq!(directory.set_bytes * 5, pages.set_bytes);

        // Pinned absolutely, not only relatively: `tools/transparent_pir_break_even.py`
        // restates these to compute where retrieval stops beating ordinary
        // download, and a silent geometry change would leave that analysis
        // describing a table this build no longer serves.
        assert_eq!(
            directory,
            TableCosts {
                set_bytes: 14_336,
                key_bytes: 86_016,
                selector_bytes: 20_480,
                response_bytes: 5_120,
                held_sets: 0,
            }
        );
        assert_eq!(
            pages,
            TableCosts {
                set_bytes: 71_680,
                key_bytes: 86_016,
                selector_bytes: 20_480,
                response_bytes: 25_600,
                held_sets: 0,
            }
        );

        // Four sets is what the earlier all-or-nothing measurement offered, and
        // it is where the two tables diverge sharply: the directory repays four
        // sets almost at once, the pages table only once there are enough
        // batches to amortize them.
        //
        // A four-set batch beats fresh keys from the third directory query and
        // the fourth page query. Both are later than a comparison of key bytes
        // against published bytes alone predicts, because a batch of four
        // rounds the padding up to four queries and each added query carries a
        // full selector and a full response.
        assert_eq!(beats_fresh_from(&directory, PUBLIC_SETS), 3);
        assert_eq!(beats_fresh_from(&pages, PUBLIC_SETS), 4);

        // Beating fresh keys is not the same as being the best plan available.
        // Four sets is the cheapest choice for the directory from four queries
        // and for the pages table from seven: below that a two- or three-set
        // batch pays for fewer parameters and pads less. An all-or-nothing
        // switch has neither of those to offer.
        assert_eq!(chosen_from(&directory, PUBLIC_SETS), 4);
        assert_eq!(chosen_from(&pages, PUBLIC_SETS), 7);

        // Two sets, which an all-or-nothing switch could not offer, beats fresh
        // keys from the second query on either table.
        assert_eq!(beats_fresh_from(&directory, 2), 2);
        assert_eq!(beats_fresh_from(&pages, 2), 2);
    }

    /// A sync makes different numbers of queries against the two tables, and
    /// the cheapest plan for one is not the cheapest for the other.
    #[test]
    fn the_cheapest_plan_differs_between_the_tables() {
        let directory = costs_for(DIRECTORY);
        let pages = costs_for(PAGES);

        // Four page queries: the directory shares one set of keys across four
        // sets, while the pages table pays less by sending two batches of two,
        // because its third and fourth set cost more than the key upload each
        // would save at that count.
        assert_eq!(directory.best_plan(4, PUBLIC_SETS).sets, 4);
        assert_eq!(pages.best_plan(4, PUBLIC_SETS).sets, 2);

        // Ten page queries: four sets would round the padding up to twelve
        // queries, and two extra page responses cost more than the batch of
        // keys they save.
        assert_eq!(pages.best_plan(10, PUBLIC_SETS).queries, 10);
        assert_eq!(directory.best_plan(10, PUBLIC_SETS).queries, 12);

        // A single lookup never shares anything: there is no second query to
        // share with, and the second set would be paid for regardless.
        assert_eq!(directory.best_plan(1, PUBLIC_SETS).sets, 1);
        assert_eq!(pages.best_plan(1, PUBLIC_SETS).sets, 1);
    }

    /// The policy must never lose to the fresh-key path it replaces. This is
    /// the property the whole change exists to restore: reuse as a global
    /// switch made the sparse profile worse.
    #[test]
    fn no_query_count_is_made_worse_than_fresh_keys() {
        for geometry in [DIRECTORY, PAGES] {
            let costs = costs_for(geometry);
            for pad_to in 1..=200 {
                let chosen = costs.best_plan(pad_to, PUBLIC_SETS);
                assert!(
                    chosen.bytes <= costs.plan_of(pad_to, 1).bytes,
                    "{geometry:?} at {pad_to} chose {chosen:?} over fresh keys"
                );
                assert!((1..=PUBLIC_SETS).contains(&chosen.sets));
                assert_eq!(chosen.queries, chosen.batches * chosen.sets);
                assert!(chosen.queries >= pad_to);
            }
        }
    }
}
