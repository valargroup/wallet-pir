//! Private retrieval of directory rows and pages from a shard set.
//!
//! One `TableClient` per table geometry, reused across every shard. That reuse
//! is the point of pinning the geometry: the expensive part of a session — the
//! parameter re-derivation and the public query setup — is done once for the
//! whole fleet rather than once per shard.
//!
//! What *is* per segment is the published `c1`, because it is derived from that
//! segment's own database. So a shard is "opened" by fetching the setup of each
//! of its segments, and those bytes are charged once per segment per sync. A
//! shard normally has one segment; one that did not fit the pinned geometry has
//! more, and every one of them answers each query, so the segment holding a
//! script is never named in a request.
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

/// One segment's published setup for one table.
struct SegmentSetup {
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
    /// Per segment, because the published `c1` comes from each segment's own
    /// database. Keyed by shard and segment index.
    segments: HashMap<(u64, u32), SegmentSetup>,
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
            segments: HashMap::new(),
        })
    }

    pub fn rows(&self) -> usize {
        self.params.db_rows
    }

    /// Records one segment's published setup after checking it against its
    /// digest.
    pub fn open_segment(
        &mut self,
        shard_id: u64,
        segment: u32,
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
        self.segments.insert(
            (shard_id, segment),
            SegmentSetup {
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

    pub fn is_open(&self, shard_id: u64, segment: u32) -> bool {
        self.segments.contains_key(&(shard_id, segment))
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

    /// Decodes one answer: one row per segment, in segment order.
    ///
    /// Every segment answered the same query, so the response carries as many
    /// bodies as the shard has segments and the caller decides which row it was
    /// looking for — by the exact script bytes, never by position.
    pub fn decode(
        &self,
        shard_id: u64,
        segments: u32,
        query: PreparedQuery,
        response: &[u8],
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        let body_len = (self.params.db_cols / self.client.rlwe_params().d)
            * response_body_len(self.client.rlwe_params().d, self.params.q_prime_1);
        let each = 16 + body_len;
        // A fixed length per segment, and a fixed segment count from the
        // published map: a response of any other size is not this shard's.
        if response.len() != each * segments as usize {
            return Err(ClientError::Response("response length mismatch".into()));
        }

        let mut rows = Vec::with_capacity(segments as usize);
        for segment in 0..segments {
            let part = &response[segment as usize * each..(segment as usize + 1) * each];
            let setup = self.segments.get(&(shard_id, segment)).ok_or_else(|| {
                ClientError::Session(format!("shard {shard_id} segment {segment} is not open"))
            })?;
            // The prefix binds the response to the shard the query named, so a
            // response from another shard fails here rather than decoding to
            // plausible nonsense. The epoch does the same for the segment,
            // since each segment publishes its own `c1`.
            if part.get(..8) != Some(shard_id.to_le_bytes().as_slice()) {
                return Err(ClientError::Response("shard mismatch".into()));
            }
            if part.get(8..16) != Some(setup.epoch.as_slice()) {
                return Err(ClientError::Response("parameter epoch mismatch".into()));
            }
            let decoded =
                self.client
                    .decode_response_simplepir(query.seed, &setup.published_c1, &part[16..]);
            rows.push(
                decoded
                    .get(..self.row_bytes)
                    .map(<[u8]>::to_vec)
                    .ok_or_else(|| ClientError::Response("decoded row is too short".into()))?,
            );
        }
        Ok(rows)
    }

    /// Fetches one row from every segment of a shard, charging what it cost.
    pub fn fetch_row(
        &self,
        transport: &mut impl ShardTransport,
        shard_id: u64,
        segments: u32,
        row: usize,
        charges: &mut ByteCharges,
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        let query = self.prepare(shard_id, row)?;
        let uploaded = query.body.len() as u64;
        let response = transport
            .query(shard_id, self.table, &query.body)
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        charges.add_query(self.table, uploaded, response.len() as u64);
        self.decode(shard_id, segments, query, &response)
    }
}
