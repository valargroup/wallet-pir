//! Status-PIR wire contract and private client. No transaction payload API.
use ipir_sp::{IPIRClient, IPIRSeed, PublicQuerySetup, SimplePirProfile};
use rand::{rngs::OsRng, Rng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};

pub const ROWS: usize = 8192;
pub const SLOTS: usize = 256;
pub const SLOT_BYTES: usize = 40;
pub const ROW_BYTES: usize = 12288;
pub const ITEM_BITS: u64 = (ROW_BYTES * 8) as u64;
pub const MAX_ENTRIES: usize = ROWS * SLOTS * 3 / 4;
pub const PROTOCOL: &str = "status-pir-v2-q48";
pub const HEADER_BYTES: usize = 52;
pub const MAX_AGE_MS: u64 = 20_000;
pub type Hash = [u8; 32];

fn query_timed_out(elapsed: Duration) -> bool {
    elapsed > Duration::from_millis(MAX_AGE_MS)
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("malformed status data")]
    Malformed,
    #[error("unsupported status protocol")]
    Unsupported,
    #[error("status coverage incomplete")]
    CoverageIncomplete,
    #[error("status observation stale")]
    Stale,
    #[error("status capacity exceeded")]
    Capacity,
    #[error("status service unavailable")]
    Unavailable,
    #[error("status PIR operation failed")]
    Pir,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Observation {
    NotFound,
    Mempool,
    Mined(u32),
    Forked,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub txid: Hash,
    pub tag: u8,
    pub height: u32,
}

impl Record {
    pub fn validate(&self) -> Result<(), Error> {
        match self.tag {
            1 if self.height == 0 => Ok(()),
            2 | 3 if self.height > 0 => Ok(()),
            _ => Err(Error::Malformed),
        }
    }
    pub fn encode(&self) -> Result<[u8; SLOT_BYTES], Error> {
        self.validate()?;
        let mut out = [0; SLOT_BYTES];
        out[..32].copy_from_slice(&self.txid);
        out[32] = self.tag;
        out[36..40].copy_from_slice(&self.height.to_le_bytes());
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> Result<Option<Self>, Error> {
        if bytes.len() != SLOT_BYTES {
            return Err(Error::Malformed);
        }
        if bytes.iter().all(|b| *b == 0) {
            return Ok(None);
        }
        if bytes[33..36].iter().any(|b| *b != 0) {
            return Err(Error::Malformed);
        }
        let r = Self {
            txid: bytes[..32].try_into().unwrap(),
            tag: bytes[32],
            height: u32::from_le_bytes(bytes[36..40].try_into().unwrap()),
        };
        r.validate()?;
        Ok(Some(r))
    }
    pub fn observation(&self) -> Result<Observation, Error> {
        self.validate()?;
        Ok(match self.tag {
            1 => Observation::Mempool,
            2 => Observation::Mined(self.height),
            _ => Observation::Forked,
        })
    }
}

pub fn bucket(network: &Hash, salt: &Hash, txid: &Hash) -> usize {
    let mut h = Sha256::new();
    h.update(b"status-pir/v2/bucket\0");
    h.update(network);
    h.update(salt);
    h.update(txid);
    let d = h.finalize();
    usize::from(u16::from_le_bytes([d[0], d[1]])) & (ROWS - 1)
}

pub fn setup_seed(network: &Hash, salt: &Hash) -> Hash {
    let mut h = Sha256::new();
    h.update(b"status-pir/v2/setup\0");
    h.update(network);
    h.update(salt);
    h.finalize().into()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub protocol: String,
    pub network: Hash,
    pub salt: Hash,
    pub generation: u64,
    pub recovery_epoch: u64,
    pub coverage_start: u32,
    pub anchor_height: u32,
    pub anchor_hash: Hash,
    pub observed_ms: u64,
    pub entries: usize,
    pub rows_digest: Hash,
    pub public_digest: Hash,
}

impl Manifest {
    /// Canonical fixed-width identity commits to coverage and observation time too.
    pub fn id(&self) -> Hash {
        let mut h = Sha256::new();
        h.update(b"status-pir/v2/manifest\0");
        h.update((self.protocol.len() as u64).to_le_bytes());
        h.update(self.protocol.as_bytes());
        h.update(self.network);
        h.update(self.salt);
        h.update(self.generation.to_le_bytes());
        h.update(self.recovery_epoch.to_le_bytes());
        h.update(self.coverage_start.to_le_bytes());
        h.update(self.anchor_height.to_le_bytes());
        h.update(self.anchor_hash);
        h.update(self.observed_ms.to_le_bytes());
        h.update((self.entries as u64).to_le_bytes());
        h.update(self.rows_digest);
        h.update(self.public_digest);
        h.finalize().into()
    }
    pub fn validate(&self) -> Result<(), Error> {
        if self.protocol != PROTOCOL {
            return Err(Error::Unsupported);
        }
        if self.coverage_start == 0
            || self.coverage_start > self.anchor_height
            || self.entries > MAX_ENTRIES
            || self.anchor_hash == [0; 32]
            || self.generation == 0
        {
            return Err(Error::Malformed);
        }
        Ok(())
    }
    pub fn fresh(&self, now_ms: u64) -> Result<(), Error> {
        self.validate()?;
        // No future timestamp tolerance in the synthetic backend.
        match now_ms.checked_sub(self.observed_ms) {
            Some(age) if age <= MAX_AGE_MS => Ok(()),
            Some(_) => Err(Error::Stale),
            None => Err(Error::Malformed),
        }
    }
}

/// Supplied by wallet chain verification, never copied from server metadata.
pub struct AcceptedAnchor {
    pub network: Hash,
    pub height: u32,
    pub hash: Hash,
}

pub fn decode_row(
    manifest: &Manifest,
    txid: &Hash,
    earliest: Option<u32>,
    row: &[u8],
) -> Result<Observation, Error> {
    if row.len() != ROW_BYTES || row[SLOTS * SLOT_BYTES..].iter().any(|b| *b != 0) {
        return Err(Error::Malformed);
    }
    let mut found = None;
    let mut previous = None;
    let mut empty = false;
    for bytes in row[..SLOTS * SLOT_BYTES].chunks_exact(SLOT_BYTES) {
        match Record::decode(bytes)? {
            None => empty = true,
            Some(r) => {
                if empty
                    || previous.is_some_and(|p| p >= r.txid)
                    || bucket(&manifest.network, &manifest.salt, &r.txid)
                        != bucket(&manifest.network, &manifest.salt, txid)
                    || (r.tag != 1
                        && (r.height < manifest.coverage_start
                            || r.height > manifest.anchor_height))
                {
                    return Err(Error::Malformed);
                }
                previous = Some(r.txid);
                if r.txid == *txid {
                    found = Some(r.observation()?);
                }
            }
        }
    }
    if let Some(value) = found {
        return Ok(value);
    }
    match earliest {
        Some(h) if h >= manifest.coverage_start && h <= manifest.anchor_height => {
            Ok(Observation::NotFound)
        }
        _ => Err(Error::CoverageIncomplete),
    }
}

pub struct Query {
    pub body: Vec<u8>,
    seed: IPIRSeed,
    txid: Hash,
    earliest: Option<u32>,
    issued_at: Instant,
}

pub struct Client {
    pub manifest: Manifest,
    client: IPIRClient,
    setup: PublicQuerySetup,
    public: Vec<Vec<u64>>,
}

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    #[error("Status endpoint returned HTTP {0}")]
    Status(u16),
    #[error(transparent)]
    Protocol(#[from] Error),
}

/// A wallet-owned session. The anchor callback must consult independently
/// accepted chain state on every initialization, including a conflict retry.
pub struct HttpClient {
    origin: String,
    http: reqwest::Client,
    session: Option<Client>,
}

impl HttpClient {
    pub fn new(origin: impl Into<String>, http: reqwest::Client) -> Self {
        Self {
            origin: origin.into().trim_end_matches('/').to_owned(),
            http,
            session: None,
        }
    }

    pub fn clear_session(&mut self) {
        self.session = None;
    }

    async fn initialize<F>(&mut self, accepted_anchor: &mut F) -> Result<(), HttpError>
    where
        F: FnMut(&Manifest) -> Result<AcceptedAnchor, Error>,
    {
        let response = self
            .http
            .get(format!("{}/v1/status/init", self.origin))
            .send()
            .await?;
        let response = successful(response)?;
        let manifest: Manifest = response.json().await?;
        manifest.fresh(current_ms())?;
        let accepted = accepted_anchor(&manifest)?;
        let response = self
            .http
            .get(format!(
                "{}/v1/status/session/{}",
                self.origin,
                hex::encode(manifest.id())
            ))
            .send()
            .await?;
        let response = successful(response)?;
        let public_len = expected_public_len()?;
        let public = read_bounded(response, public_len).await?;
        self.session = Some(Client::new(manifest, &public, &accepted)?);
        Ok(())
    }

    /// One bounded retry for an evicted or revoked session. A retry always
    /// reinitializes and encrypts a new query; it never replays the old body.
    pub async fn lookup<F>(
        &mut self,
        txid: Hash,
        earliest: Option<u32>,
        mut accepted_anchor: F,
    ) -> Result<Observation, HttpError>
    where
        F: FnMut(&Manifest) -> Result<AcceptedAnchor, Error>,
    {
        for attempt in 0..2 {
            if let Some(session) = &self.session {
                let accepted = accepted_anchor(&session.manifest)?;
                if !anchor_matches(&session.manifest, &accepted) {
                    self.session = None;
                }
            }
            if self
                .session
                .as_ref()
                .is_some_and(|s| s.manifest.fresh(current_ms()).is_err())
            {
                self.session = None;
            }
            let result = async {
                if self.session.is_none() {
                    self.initialize(&mut accepted_anchor).await?;
                }
                let session = self.session.as_ref().expect("initialized session");
                let query = session.prepare(&txid, earliest, current_ms())?;
                let response = self
                    .http
                    .post(format!("{}/v1/status/query", self.origin))
                    .body(query.body.clone())
                    .send()
                    .await?;
                let response = successful(response)?;
                let bytes = read_bounded(response, expected_response_len()?).await?;
                let result = session.decode(query, &bytes, current_ms())?;
                let accepted = accepted_anchor(&session.manifest)?;
                if !anchor_matches(&session.manifest, &accepted) {
                    return Err(HttpError::Protocol(Error::Malformed));
                }
                Ok::<_, HttpError>(result)
            }
            .await;
            if matches!(result, Err(HttpError::Status(409 | 410))) && attempt == 0 {
                self.session = None;
                continue;
            }
            return result;
        }
        unreachable!("bounded retry returns on second attempt")
    }
}

fn successful(response: reqwest::Response) -> Result<reqwest::Response, HttpError> {
    if response.status().is_success() {
        Ok(response)
    } else {
        Err(HttpError::Status(response.status().as_u16()))
    }
}

fn anchor_matches(manifest: &Manifest, accepted: &AcceptedAnchor) -> bool {
    manifest.network == accepted.network
        && manifest.anchor_height == accepted.height
        && manifest.anchor_hash == accepted.hash
}

fn expected_public_len() -> Result<usize, Error> {
    let (rlwe, params) =
        ipir_sp::params_for_simplepir_profile(ROWS as u64, ITEM_BITS, SimplePirProfile::P16Q48)
            .map_err(|_| Error::Pir)?;
    Ok(params.db_cols / rlwe.d * ipir_sp::modulus_switch::published_c1_len(rlwe.d, rlwe.q))
}

fn expected_response_len() -> Result<usize, Error> {
    let (rlwe, params) =
        ipir_sp::params_for_simplepir_profile(ROWS as u64, ITEM_BITS, SimplePirProfile::P16Q48)
            .map_err(|_| Error::Pir)?;
    Ok(HEADER_BYTES
        + params.db_cols / rlwe.d
            * ipir_sp::modulus_switch::response_body_len(rlwe.d, params.q_prime_1))
}

async fn read_bounded(mut response: reqwest::Response, max: usize) -> Result<Vec<u8>, HttpError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > max.saturating_sub(bytes.len()) {
            return Err(HttpError::Protocol(Error::Malformed));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != max {
        return Err(HttpError::Protocol(Error::Malformed));
    }
    Ok(bytes)
}

fn current_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

impl Client {
    pub fn new(
        manifest: Manifest,
        public: &[u8],
        accepted: &AcceptedAnchor,
    ) -> Result<Self, Error> {
        manifest.validate()?;
        if manifest.network != accepted.network
            || manifest.anchor_height != accepted.height
            || manifest.anchor_hash != accepted.hash
        {
            return Err(Error::Malformed);
        }
        let client = IPIRClient::from_profile(ROWS as u64, ITEM_BITS, SimplePirProfile::P16Q48)
            .map_err(|_| Error::Pir)?;
        let (rlwe, params) =
            ipir_sp::params_for_simplepir_profile(ROWS as u64, ITEM_BITS, SimplePirProfile::P16Q48)
                .map_err(|_| Error::Pir)?;
        let blocks = params.db_cols / rlwe.d;
        if public.len() != blocks * ipir_sp::modulus_switch::published_c1_len(rlwe.d, rlwe.q)
            || Hash::from(Sha256::digest(public)) != manifest.public_digest
        {
            return Err(Error::Malformed);
        }
        let setup = client.generate_public_query_setup_simplepir_from_seed(setup_seed(
            &manifest.network,
            &manifest.salt,
        ));
        let public = ipir_sp::modulus_switch::recover_published_c1(public, rlwe.d, blocks, rlwe.q);
        Ok(Self {
            manifest,
            client,
            setup,
            public,
        })
    }
    pub fn prepare(&self, txid: &[u8], earliest: Option<u32>, now_ms: u64) -> Result<Query, Error> {
        let txid: Hash = txid.try_into().map_err(|_| Error::Malformed)?;
        self.manifest.fresh(now_ms)?;
        let issued_at = Instant::now();
        let row = bucket(&self.manifest.network, &self.manifest.salt, &txid);
        let (query, keys, seed) = self.client.generate_fresh_query_simplepir(&self.setup, row);
        let mut body = b"SPQ2".to_vec();
        body.extend(self.manifest.id());
        body.extend(OsRng.gen::<[u8; 16]>());
        body.extend(
            ipir_sp::serialize::serialize_packing_keys(self.client.rlwe_params(), &keys)
                .map_err(|_| Error::Pir)?,
        );
        body.extend(query.to_switched_bytes(self.client.rlwe_params().q, 48));
        Ok(Query {
            body,
            seed,
            txid,
            earliest,
            issued_at,
        })
    }
    pub fn decode(&self, query: Query, response: &[u8], now_ms: u64) -> Result<Observation, Error> {
        // Wall-clock rollback cannot extend a query's freshness lifetime.
        if query_timed_out(query.issued_at.elapsed()) {
            return Err(Error::Stale);
        }
        self.manifest.fresh(now_ms)?;
        let (rlwe, params) =
            ipir_sp::params_for_simplepir_profile(ROWS as u64, ITEM_BITS, SimplePirProfile::P16Q48)
                .map_err(|_| Error::Pir)?;
        let len = params.db_cols / rlwe.d
            * ipir_sp::modulus_switch::response_body_len(rlwe.d, params.q_prime_1);
        if response.len() != HEADER_BYTES + len
            || response[..HEADER_BYTES] != query.body[..HEADER_BYTES]
        {
            return Err(Error::Malformed);
        }
        let row = self.client.decode_response_simplepir(
            query.seed,
            &self.public,
            &response[HEADER_BYTES..],
        );
        decode_row(&self.manifest, &query.txid, query.earliest, &row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn manifest() -> Manifest {
        Manifest {
            protocol: PROTOCOL.into(),
            network: [1; 32],
            salt: [2; 32],
            generation: 1,
            recovery_epoch: 0,
            coverage_start: 10,
            anchor_height: 20,
            anchor_hash: [3; 32],
            observed_ms: 1000,
            entries: 1,
            rows_digest: [4; 32],
            public_digest: [5; 32],
        }
    }
    #[test]
    fn v2_geometry_and_v1_rejection() {
        assert_eq!((ROWS, SLOTS, SLOT_BYTES, ROW_BYTES), (8192, 256, 40, 12288));
        assert_eq!(ROWS * ROW_BYTES, 96 * 1024 * 1024);
        let mut m = manifest();
        m.protocol = "status-pir-v1-q48".into();
        assert_eq!(m.validate(), Err(Error::Unsupported));
        assert_eq!(Record::decode(&[0; 80]), Err(Error::Malformed));
    }
    #[test]
    fn independent_python_hash_vector() {
        let txid = std::array::from_fn(|i| i as u8);
        assert_eq!(bucket(&[1; 32], &[2; 32], &txid), 6818);
        assert_eq!(
            hex::encode(setup_seed(&[1; 32], &[2; 32])),
            "44ada24f0ddb452f8d8a6902d2c32d02fe64fde4088e71e60a81c453d81e9ea8"
        );
    }
    #[test]
    fn slot_codec_rejects_reserved_bytes_empty_garbage_and_invalid_states() {
        for (tag, height) in [(1, 0), (2, 10), (3, 10)] {
            let r = Record {
                txid: [4; 32],
                tag,
                height,
            };
            let bytes = r.encode().unwrap();
            assert_eq!(Record::decode(&bytes), Ok(Some(r)));
            let mut bad = bytes;
            bad[33] = 1;
            assert_eq!(Record::decode(&bad), Err(Error::Malformed));
            let mut bad = bytes;
            bad[32] = 4;
            assert_eq!(Record::decode(&bad), Err(Error::Malformed));
        }
        let mut empty = [0; SLOT_BYTES];
        empty[0] = 1;
        assert_eq!(Record::decode(&empty), Err(Error::Malformed));
        assert!(Record {
            txid: [0; 32],
            tag: 1,
            height: 1,
        }
        .encode()
        .is_err());
        assert!(Record {
            txid: [0; 32],
            tag: 2,
            height: 0,
        }
        .encode()
        .is_err());
    }
    #[test]
    fn freshness_coverage_and_binding_are_separate() {
        let m = manifest();
        let row = vec![0; ROW_BYTES];
        assert_eq!(m.fresh(21_000), Ok(()));
        assert_eq!(m.fresh(21_001), Err(Error::Stale));
        assert_eq!(m.fresh(999), Err(Error::Malformed));
        assert!(!query_timed_out(Duration::from_millis(MAX_AGE_MS)));
        assert!(query_timed_out(Duration::from_millis(MAX_AGE_MS + 1)));
        for earliest in [None, Some(9), Some(21)] {
            assert_eq!(
                decode_row(&m, &[9; 32], earliest, &row),
                Err(Error::CoverageIncomplete)
            );
        }
        assert_eq!(
            decode_row(&m, &[9; 32], Some(10), &row),
            Ok(Observation::NotFound)
        );
        let id = m.id();
        let mut changed = m.clone();
        changed.observed_ms += 1;
        assert_ne!(changed.id(), id);
        let mut changed = m.clone();
        changed.recovery_epoch += 1;
        assert_ne!(changed.id(), id);
        let mut changed = m.clone();
        changed.coverage_start += 1;
        assert_ne!(changed.id(), id);
        let mut changed = m;
        changed.protocol = "other".into();
        assert_eq!(changed.validate(), Err(Error::Unsupported));
    }
    #[test]
    fn complete_row_is_validated_even_after_a_match() {
        let m = manifest();
        let txid = [9; 32];
        let r = Record {
            txid,
            tag: 2,
            height: 11,
        };
        let mut row = vec![0; ROW_BYTES];
        row[..SLOT_BYTES].copy_from_slice(&r.encode().unwrap());
        assert_eq!(
            decode_row(&m, &txid, None, &row),
            Ok(Observation::Mined(11))
        );
        row[ROW_BYTES - 1] = 1;
        assert_eq!(decode_row(&m, &txid, None, &row), Err(Error::Malformed));
        row[ROW_BYTES - 1] = 0;
        row[SLOT_BYTES..SLOT_BYTES * 2].copy_from_slice(&r.encode().unwrap());
        assert_eq!(decode_row(&m, &txid, None, &row), Err(Error::Malformed));
    }
}
