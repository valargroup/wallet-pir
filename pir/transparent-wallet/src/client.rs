//! Private retrieval of directory rows and pages from a shard set.
//!
//! One `TableClient` per table geometry, reused across every shard. That reuse
//! is the point of pinning the geometry: the expensive part of a session — the
//! parameter re-derivation and the public query setup — is done once for the
//! whole fleet rather than once per shard.
//!
//! What *is* per shard is the published `c1`, because it is derived from that
//! shard's own database. So a shard is "opened" by fetching its setup, and the
//! bytes for that are charged once per shard per sync.
//!
//! Everything the server sends is re-derived rather than trusted. A client that
//! adopted the server's parameters would decode against whatever geometry the
//! server chose, including one that leaks the selection.

use crate::transport::{ByteCharges, ShardTransport};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use ipir_sp::modulus_switch::{published_c1_len, recover_published_c1, response_body_len};
use ipir_sp::serialize::serialize_packing_keys;
use ipir_sp::{IPIRClient, YpirSchemeParams};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("session: {0}")]
    Session(String),
    #[error("response: {0}")]
    Response(String),
    #[error("transport: {0}")]
    Transport(String),
    #[error("pir: {0}")]
    Pir(String),
}

/// Which table a query addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Table {
    Directory,
    Pages,
}

impl Table {
    pub fn as_str(self) -> &'static str {
        match self {
            Table::Directory => "directory",
            Table::Pages => "pages",
        }
    }
}

/// A prepared query and the secret needed to decode its response.
pub struct PreparedQuery {
    pub body: Vec<u8>,
    seed: ipir_sp::IPIRSeed,
}

/// One shard's published setup for one table.
struct ShardSetup {
    published_c1: Vec<Vec<u64>>,
    epoch: [u8; 8],
}

/// A client for one table geometry, across every shard.
pub struct TableClient {
    table: Table,
    params: YpirSchemeParams,
    client: IPIRClient,
    setup: Vec<Vec<u64>>,
    row_bytes: usize,
    /// Per shard, because the published `c1` comes from each shard's database.
    shards: HashMap<u64, ShardSetup>,
}

impl TableClient {
    /// Builds a client for one table, re-deriving the geometry rather than
    /// adopting what the service published.
    pub fn new(
        table: Table,
        rows: u64,
        row_bytes: u32,
        setup_seed: u64,
        served: &YpirSchemeParams,
    ) -> Result<Self, ClientError> {
        let (rlwe, params) = ipir_sp::params_for_simplepir(rows, (row_bytes as u64) * 8)
            .map_err(|error| ClientError::Pir(error.to_string()))?;
        if served != &params {
            return Err(ClientError::Session(format!(
                "{} scheme does not match the pinned geometry",
                table.as_str()
            )));
        }
        let mut seed = [0u8; 32];
        seed[..8].copy_from_slice(&setup_seed.to_le_bytes());
        let client = IPIRClient::new(&rlwe, &params);
        let setup = client.generate_public_query_setup_simplepir_from_seed(seed);
        Ok(Self {
            table,
            params,
            client,
            setup,
            row_bytes: row_bytes as usize,
            shards: HashMap::new(),
        })
    }

    pub fn rows(&self) -> usize {
        self.params.db_rows
    }

    /// Records a shard's published setup after checking it against its digest.
    pub fn open_shard(
        &mut self,
        shard_id: u64,
        public_params_base64: &str,
        public_params_sha256: &str,
    ) -> Result<(), ClientError> {
        let public_params = BASE64_STANDARD
            .decode(public_params_base64.as_bytes())
            .map_err(|error| ClientError::Session(error.to_string()))?;
        let digest = Sha256::digest(&public_params);
        if hex::encode(digest) != public_params_sha256 {
            return Err(ClientError::Session(format!(
                "shard {shard_id} published parameter digest mismatch"
            )));
        }
        let blocks = self.params.db_cols / self.client.rlwe_params().d;
        if public_params.len()
            != blocks * published_c1_len(self.client.rlwe_params().d, self.client.rlwe_params().q)
        {
            return Err(ClientError::Session(format!(
                "shard {shard_id} published parameter length mismatch"
            )));
        }
        let mut epoch = [0u8; 8];
        epoch.copy_from_slice(&digest[..8]);
        self.shards.insert(
            shard_id,
            ShardSetup {
                published_c1: recover_published_c1(
                    &public_params,
                    self.client.rlwe_params().d,
                    blocks,
                    self.client.rlwe_params().q,
                ),
                epoch,
            },
        );
        Ok(())
    }

    pub fn is_open(&self, shard_id: u64) -> bool {
        self.shards.contains_key(&shard_id)
    }

    pub fn prepare(&self, shard_id: u64, row: usize) -> Result<PreparedQuery, ClientError> {
        if row >= self.params.db_rows {
            return Err(ClientError::Session("row outside table".into()));
        }
        let (query, packing_keys, seed) =
            self.client.generate_fresh_query_simplepir(&self.setup, row);
        let mut body = shard_id.to_le_bytes().to_vec();
        body.extend(
            serialize_packing_keys(self.client.rlwe_params(), &packing_keys)
                .map_err(|error| ClientError::Pir(error.to_string()))?,
        );
        body.extend(query.to_switched_bytes(self.client.rlwe_params().q, self.params.query_bits));
        Ok(PreparedQuery { body, seed })
    }

    pub fn decode(
        &self,
        shard_id: u64,
        query: PreparedQuery,
        response: &[u8],
    ) -> Result<Vec<u8>, ClientError> {
        let setup = self
            .shards
            .get(&shard_id)
            .ok_or_else(|| ClientError::Session(format!("shard {shard_id} is not open")))?;
        // The prefix binds the response to the shard the query named, so a
        // response from another shard fails here rather than decoding to
        // plausible nonsense.
        if response.get(..8) != Some(shard_id.to_le_bytes().as_slice()) {
            return Err(ClientError::Response("shard mismatch".into()));
        }
        if response.get(8..16) != Some(setup.epoch.as_slice()) {
            return Err(ClientError::Response("parameter epoch mismatch".into()));
        }
        let expected = (self.params.db_cols / self.client.rlwe_params().d)
            * response_body_len(self.client.rlwe_params().d, self.params.q_prime_1);
        if response.len() != 16 + expected {
            return Err(ClientError::Response("response length mismatch".into()));
        }
        let decoded =
            self.client
                .decode_response_simplepir(query.seed, &setup.published_c1, &response[16..]);
        decoded
            .get(..self.row_bytes)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| ClientError::Response("decoded row is too short".into()))
    }

    /// Fetches one row, charging what it cost.
    pub fn fetch_row(
        &self,
        transport: &mut impl ShardTransport,
        shard_id: u64,
        row: usize,
        charges: &mut ByteCharges,
    ) -> Result<Vec<u8>, ClientError> {
        let query = self.prepare(shard_id, row)?;
        let uploaded = query.body.len() as u64;
        let response = transport
            .query(shard_id, self.table, &query.body)
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        charges.add_query(uploaded, response.len() as u64);
        self.decode(shard_id, query, &response)
    }
}
