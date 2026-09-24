//! Enhance PIR client. Shard selection is public; row and record-slot selection stay local.
use crate::protocol::*;
use crate::{EnhanceRecord, ITEM_SIZE_BITS, RECORDS_PER_ROW, RECORD_BYTES, ROW_BYTES};
use base64::{engine::general_purpose::STANDARD, Engine};
use ipir_sp::modulus_switch::{published_c1_len, recover_published_c1, response_body_len};
use ipir_sp::serialize::serialize_packing_keys;
use ipir_sp::{IPIRClient, IPIRSeed, YpirSchemeParams};
use rand::{rngs::OsRng, Rng};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("server returned HTTP {0}")]
    HttpStatus(u16),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid public parameters base64: {0}")]
    PublicParamsBase64(#[from] base64::DecodeError),
    #[error("server generation is incompatible: {0}")]
    Generation(String),
    #[error("position {0} is outside advertised coverage")]
    OutsideCoverage(u64),
    #[error("PIR error: {0}")]
    Pir(String),
    #[error("malformed PIR response: {0}")]
    Response(String),
}

#[derive(Clone, Copy, Debug)]
pub struct QueryTiming {
    pub prepare: Duration,
    pub http: Duration,
    pub decode: Duration,
    pub total: Duration,
}

pub struct PreparedQuery {
    binding: QueryBinding,
    seed: IPIRSeed,
    body: Vec<u8>,
}
impl PreparedQuery {
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

pub struct QuerySession {
    binding: QueryBinding,
    shard: QueryShard,
    routes: Vec<crate::protocol::Route>,
    records: u64,
    params: YpirSchemeParams,
    client: IPIRClient,
    setup: ipir_sp::PublicQuerySetup,
    public: Vec<Vec<u64>>,
}

impl QuerySession {
    pub fn session_id(&self) -> [u8; 32] {
        self.binding.session_id
    }

    /// Only the routing binding changes; the content-bound PIR material is reused.
    pub fn rebind(&mut self, manifest: &Manifest) -> Result<(), ClientError> {
        manifest.validate().map_err(ClientError::Generation)?;
        if manifest
            .session_id(self.shard.id)
            .map_err(ClientError::Generation)?
            != self.binding.session_id
        {
            return Err(ClientError::Generation("session content changed".into()));
        }
        self.binding.generation = manifest.generation;
        self.binding.recovery_epoch = manifest.recovery_epoch;
        self.binding.anchor_hash = hex::decode(&manifest.anchor_block_hash)
            .map_err(|e| ClientError::Generation(e.to_string()))?
            .try_into()
            .map_err(|_| ClientError::Generation("anchor length".into()))?;
        self.routes = manifest
            .coverage
            .routes
            .iter()
            .filter(|r| r.domain_id == self.shard.id)
            .cloned()
            .collect();
        self.records = manifest.coverage.records;
        Ok(())
    }

    pub fn new(manifest: &Manifest, session: ShardSession) -> Result<Self, ClientError> {
        manifest.validate().map_err(ClientError::Generation)?;
        let shard = manifest
            .coverage
            .shards
            .iter()
            .find(|s| s.id == session.shard_id)
            .ok_or_else(|| ClientError::Generation("unknown shard".into()))?
            .clone();
        let reference = manifest
            .sessions
            .iter()
            .find(|s| s.shard_id == shard.id)
            .unwrap();
        if session.session_id
            != hex::encode(
                manifest
                    .session_id(shard.id)
                    .map_err(ClientError::Generation)?,
            )
            || session.params != parameters(shard.logical_rows).map_err(ClientError::Generation)?
        {
            return Err(ClientError::Generation(
                "session binding or parameters mismatch".into(),
            ));
        }
        let (rlwe, params) = ipir_sp::params_for_simplepir_profile(
            shard.logical_rows,
            ITEM_SIZE_BITS,
            ipir_sp::SimplePirProfile::P16Q48,
        )
        .map_err(|e| ClientError::Pir(e.to_string()))?;
        let bytes = STANDARD.decode(session.public_params_base64)?;
        let hash = Sha256::digest(&bytes);
        let blocks = params.db_cols / rlwe.d;
        if hex::encode(hash) != reference.public_params_sha256
            || bytes.len() != blocks * published_c1_len(rlwe.d, rlwe.q)
        {
            return Err(ClientError::Generation(
                "public material digest or length mismatch".into(),
            ));
        }
        let client = IPIRClient::from_profile(
            shard.logical_rows,
            ITEM_SIZE_BITS,
            ipir_sp::SimplePirProfile::P16Q48,
        )
        .map_err(|e| ClientError::Pir(e.to_string()))?;
        let setup = client.generate_public_query_setup_simplepir_from_seed(setup_seed(shard.id));
        let public = recover_published_c1(&bytes, rlwe.d, blocks, rlwe.q);
        Ok(Self {
            binding: QueryBinding {
                generation: manifest.generation,
                shard_id: shard.id,
                epoch: hash[..8].try_into().unwrap(),
                recovery_epoch: manifest.recovery_epoch,
                session_id: manifest
                    .session_id(shard.id)
                    .map_err(ClientError::Generation)?,
                request_id: [0; 16],
                anchor_hash: hex::decode(&manifest.anchor_block_hash)
                    .map_err(|e| ClientError::Generation(e.to_string()))?
                    .try_into()
                    .map_err(|_| ClientError::Generation("anchor length".into()))?,
            },
            routes: manifest
                .coverage
                .routes
                .iter()
                .filter(|r| r.domain_id == shard.id)
                .cloned()
                .collect(),
            records: manifest.coverage.records,
            shard,
            params,
            client,
            setup,
            public,
        })
    }

    pub fn prepare_position(&self, position: u64) -> Result<(PreparedQuery, usize), ClientError> {
        if position >= self.records {
            return Err(ClientError::OutsideCoverage(position));
        }
        let global = position / 33;
        let route = self
            .routes
            .iter()
            .find(|r| r.global_start <= global && global < r.global_end)
            .ok_or(ClientError::OutsideCoverage(position))?;
        let row = (route.local_start + global - route.global_start) as usize;
        let slot = (position % 33) as usize;
        Ok((self.prepare_row(row)?, slot))
    }

    pub fn prepare_dummy(&self) -> Result<PreparedQuery, ClientError> {
        self.prepare_row(OsRng.gen_range(0..self.params.db_rows))
    }

    pub fn prepare_row(&self, row: usize) -> Result<PreparedQuery, ClientError> {
        if row >= self.params.db_rows {
            return Err(ClientError::OutsideCoverage(row as u64));
        }
        let (query, keys, seed) = self.client.generate_fresh_query_simplepir(&self.setup, row);
        let mut binding = self.binding;
        binding.request_id = OsRng.gen();
        let mut body = binding.encode();
        body.extend(
            serialize_packing_keys(self.client.rlwe_params(), &keys)
                .map_err(|e| ClientError::Pir(e.to_string()))?,
        );
        body.extend(query.to_switched_bytes(self.client.rlwe_params().q, self.params.query_bits));
        Ok(PreparedQuery {
            binding,
            seed,
            body,
        })
    }

    pub fn decode(&self, query: PreparedQuery, response: &[u8]) -> Result<Vec<u8>, ClientError> {
        let binding = QueryBinding::decode(response).map_err(ClientError::Response)?;
        let size = self.params.db_cols / self.client.rlwe_params().d
            * response_body_len(self.client.rlwe_params().d, self.params.q_prime_1);
        if binding != query.binding
            || binding.session_id != self.binding.session_id
            || binding.anchor_hash != self.binding.anchor_hash
            || response.len() != HEADER_BYTES + size
        {
            return Err(ClientError::Response(
                "response binding or length mismatch".into(),
            ));
        }
        let decoded = self.client.decode_response_simplepir(
            query.seed,
            &self.public,
            &response[HEADER_BYTES..],
        );
        decoded
            .get(..ROW_BYTES)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| ClientError::Response("short row".into()))
    }
}

/// A bounded, lazy cache for one generation. Callers refresh explicitly after HTTP 410.
pub struct EnhancePirClient {
    origin: String,
    http: reqwest::Client,
    manifest: Manifest,
    sessions: BTreeMap<u64, Arc<QuerySession>>,
    refreshed: Instant,
}

impl EnhancePirClient {
    pub async fn connect(origin: &str) -> Result<Self, ClientError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()?;
        let origin = origin.trim_end_matches('/').to_owned();
        let response = http.get(format!("{origin}/v1/enhance/init")).send().await?;
        let manifest: Manifest =
            serde_json::from_slice(&read_limited(response, 1024 * 1024).await?)?;
        manifest.validate().map_err(ClientError::Generation)?;
        Ok(Self {
            origin,
            http,
            manifest,
            sessions: BTreeMap::new(),
            refreshed: Instant::now(),
        })
    }

    pub async fn refresh(&mut self) -> Result<(), ClientError> {
        let latest = Self::connect(&self.origin).await?;
        if latest.manifest.generation < self.manifest.generation
            || latest.manifest.recovery_epoch < self.manifest.recovery_epoch
        {
            return Err(ClientError::Generation("routing revision regressed".into()));
        }
        self.sessions.retain(|_, session| {
            Arc::get_mut(session).is_some_and(|s| s.rebind(&latest.manifest).is_ok())
        });
        self.manifest = latest.manifest;
        self.refreshed = Instant::now();
        Ok(())
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub async fn query_dummy(&mut self, shard_id: u64) -> Result<(), ClientError> {
        let session = self.session(shard_id).await?;
        let query = session.prepare_dummy()?;
        self.send(&session, query).await?;
        Ok(())
    }

    async fn send(
        &self,
        session: &QuerySession,
        query: PreparedQuery,
    ) -> Result<Vec<u8>, ClientError> {
        let response = self
            .http
            .post(format!("{}/v1/enhance/query", self.origin))
            .body(query.body.clone())
            .send()
            .await?;
        let bytes = read_limited(response, 1024 * 1024).await?;
        session.decode(query, &bytes)
    }

    /// Deduplicate within each immutable shard session and preserve caller order.
    /// Expiry discards partial results before one bounded retry against a fresh manifest.
    pub async fn query_positions(
        &mut self,
        positions: &[u64],
    ) -> Result<Vec<EnhanceRecord>, ClientError> {
        if self.refreshed.elapsed() >= Duration::from_secs(30) {
            self.refresh().await?;
        }
        for attempt in 0..2 {
            let result = self.query_positions_once(positions).await;
            if matches!(result, Err(ClientError::HttpStatus(409 | 410))) && attempt == 0 {
                self.refresh().await?;
                continue;
            }
            return result;
        }
        unreachable!("bounded refresh loop")
    }

    async fn query_positions_once(
        &mut self,
        positions: &[u64],
    ) -> Result<Vec<EnhanceRecord>, ClientError> {
        let mut groups = BTreeMap::<(u64, usize), Vec<(usize, usize)>>::new();
        for (index, position) in positions.iter().enumerate() {
            let (shard, row, slot) = self
                .manifest
                .coverage
                .locate(*position)
                .ok_or(ClientError::OutsideCoverage(*position))?;
            groups
                .entry((shard.id, row))
                .or_default()
                .push((index, slot));
        }
        let mut result = vec![None; positions.len()];
        for ((shard, row), slots) in groups {
            let session = self.session(shard).await?;
            let decoded = self.send(&session, session.prepare_row(row)?).await?;
            for (index, slot) in slots {
                result[index] = Some(record_in_row(&decoded, slot)?);
            }
        }
        Ok(result
            .into_iter()
            .map(|r| r.expect("every input position assigned once"))
            .collect())
    }

    pub async fn session(&mut self, shard_id: u64) -> Result<Arc<QuerySession>, ClientError> {
        if let Some(session) = self.sessions.get(&shard_id) {
            return Ok(session.clone());
        }
        if !self
            .manifest
            .sessions
            .iter()
            .any(|s| s.shard_id == shard_id)
        {
            return Err(ClientError::Generation("unknown shard".into()));
        }
        let response = self
            .http
            .get(format!(
                "{}/v1/enhance/session/{}",
                self.origin,
                hex::encode(
                    self.manifest
                        .session_id(shard_id)
                        .map_err(ClientError::Generation)?
                )
            ))
            .send()
            .await?;
        let session: ShardSession =
            serde_json::from_slice(&read_limited(response, 32 * 1024 * 1024).await?)?;
        let session = Arc::new(QuerySession::new(&self.manifest, session)?);
        // Do not retain every historical shard's expanded public setup in a wallet.
        if self.sessions.len() >= 2 {
            self.sessions.clear();
        }
        self.sessions.insert(shard_id, session.clone());
        Ok(session)
    }

    pub async fn query_position_with_timing(
        &mut self,
        position: u64,
    ) -> Result<(EnhanceRecord, QueryTiming), ClientError> {
        let total_start = Instant::now();
        if self.refreshed.elapsed() >= Duration::from_secs(30) {
            self.refresh().await?;
        }
        for attempt in 0..2 {
            let result = self.query_once(position).await;
            if matches!(&result, Err(ClientError::HttpStatus(409 | 410))) && attempt == 0 {
                self.refresh().await?;
                continue;
            }
            return result.map(|(record, mut timing)| {
                timing.total = total_start.elapsed();
                (record, timing)
            });
        }
        unreachable!("bounded refresh loop returns on its second attempt")
    }

    async fn query_once(
        &mut self,
        position: u64,
    ) -> Result<(EnhanceRecord, QueryTiming), ClientError> {
        let start = Instant::now();
        let shard = self
            .manifest
            .coverage
            .locate(position)
            .ok_or(ClientError::OutsideCoverage(position))?
            .0
            .id;
        let session = self.session(shard).await?;
        let (query, slot) = session.prepare_position(position)?;
        let prepare = start.elapsed();
        let at = Instant::now();
        let response = self
            .http
            .post(format!("{}/v1/enhance/query", self.origin))
            .body(query.body.clone())
            .send()
            .await?;
        let bytes = read_limited(response, 1024 * 1024).await?;
        let http = at.elapsed();
        let at = Instant::now();
        let row = session.decode(query, &bytes)?;
        let record = record_in_row(&row, slot)?;
        Ok((
            record,
            QueryTiming {
                prepare,
                http,
                decode: at.elapsed(),
                total: start.elapsed(),
            },
        ))
    }
}

async fn read_limited(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, ClientError> {
    if !response.status().is_success() {
        return Err(ClientError::HttpStatus(response.status().as_u16()));
    }
    if response.content_length().is_some_and(|n| n > limit as u64) {
        return Err(ClientError::Response("oversized body".into()));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > limit - bytes.len() {
            return Err(ClientError::Response("oversized body".into()));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub fn record_in_row(row: &[u8], slot: usize) -> Result<EnhanceRecord, ClientError> {
    if slot >= RECORDS_PER_ROW {
        return Err(ClientError::Response("record slot outside row".into()));
    }
    let start = slot * RECORD_BYTES;
    let bytes = row
        .get(start..start + RECORD_BYTES)
        .ok_or_else(|| ClientError::Response("record outside decoded row".into()))?
        .try_into()
        .expect("fixed record length");
    EnhanceRecord::from_bytes(bytes).map_err(|e| ClientError::Response(e.to_string()))
}
