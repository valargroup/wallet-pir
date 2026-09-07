//! Private retrieval of directory rows and pages from a shard set.
//!
//! One `TableClient` per geometry per table, reused across every shard naming
//! that geometry. That reuse is the point of naming geometries from a closed
//! registry rather than letting each shard choose: the expensive part of a
//! session — the parameter re-derivation and the public query setup — is done
//! once per geometry rather than once per shard. A sync spanning an archive
//! tier and a recent tier pays it twice, not once per shard in either.
//!
//! What *is* per segment is the published `c1`, because it is derived from that
//! segment's own database. So a shard *revision* is "opened" by fetching the
//! setup of each of its segments, and those bytes are charged once per segment
//! per sync. A shard normally has one segment; one whose content did not fit
//! its geometry has more, and every one of them answers each query, so the
//! segment holding a script is never named in a request.
//!
//! Setup is cached against the **manifest digest**, not the shard id. A growing
//! tail is republished as a new revision with different bytes, so setup cached
//! under the id would be reused against a database it was not derived from —
//! which does not error, it decodes to plausible nonsense.
//!
//! Everything the server sends is re-derived rather than trusted. A client that
//! adopted the server's parameters would decode against whatever geometry the
//! server chose, including one that leaks the selection.

use crate::transport::{ByteCharges, Overloaded, ShardTransport, StaleRevision};
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
    /// The service no longer serves the revision this query named.
    ///
    /// Kept as a type rather than folded into `Transport`: it is recoverable by
    /// refreshing the map and re-deriving the shard, and a string cannot be
    /// matched on. Every query goes through here, so collapsing it here would
    /// lose the refusal for all of them.
    #[error(transparent)]
    Stale(#[from] StaleRevision),
    /// The service had no cache capacity for this query. Retryable as it
    /// stands, without refreshing anything.
    #[error(transparent)]
    Overloaded(#[from] Overloaded),
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
    /// database. Keyed by manifest digest and segment index, so a superseded
    /// revision's setup is never reused for the revision that replaced it.
    segments: HashMap<(String, u32), SegmentSetup>,
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
        revision: &str,
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
                "revision {revision} published parameter digest mismatch"
            )));
        }
        let blocks = self.params.db_cols / self.client.rlwe_params().d;
        if public_params.len()
            != blocks * published_c1_len(self.client.rlwe_params().d, self.client.rlwe_params().q)
        {
            return Err(ClientError::Session(format!(
                "revision {revision} published parameter length mismatch"
            )));
        }
        let mut epoch = [0u8; 8];
        epoch.copy_from_slice(&digest[..8]);
        self.segments.insert(
            (revision.to_string(), segment),
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

    pub fn is_open(&self, revision: &str, segment: u32) -> bool {
        self.segments.contains_key(&(revision.to_string(), segment))
    }

    /// Prepares one query against a named revision.
    ///
    /// The body opens with the revision-and-table binding, which the service
    /// re-derives and checks and the response repeats. It is a fixed-width
    /// public value — the revision is already in the request path and in the
    /// published map — so it discloses nothing while making a query answered by
    /// the wrong runtime fail rather than decode.
    pub fn prepare(&self, revision: &str, row: usize) -> Result<PreparedQuery, ClientError> {
        if row >= self.params.db_rows {
            return Err(ClientError::Session("row outside table".into()));
        }
        let (query, packing_keys, seed) =
            self.client.generate_fresh_query_simplepir(&self.setup, row);
        let mut body =
            transparent_shard::manifest::query_binding(revision, self.table.as_str()).to_vec();
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
        revision: &str,
        segments: u32,
        query: PreparedQuery,
        response: &[u8],
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        let binding = transparent_shard::manifest::query_binding(revision, self.table.as_str());
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
            let setup = self
                .segments
                .get(&(revision.to_string(), segment))
                .ok_or_else(|| {
                    ClientError::Session(format!(
                        "revision {revision} segment {segment} is not open"
                    ))
                })?;
            // The prefix binds the response to the revision and table the query
            // named, so an answer produced from another revision's bytes fails
            // here rather than decoding to plausible rows for a range the
            // wallet did not ask about. The epoch does the same for the
            // segment, since each segment publishes its own `c1`.
            if part.get(..8) != Some(binding.as_slice()) {
                return Err(ClientError::Response("revision or table mismatch".into()));
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
        revision: &str,
        segments: u32,
        row: usize,
        charges: &mut ByteCharges,
    ) -> Result<Vec<Vec<u8>>, ClientError> {
        let query = self.prepare(revision, row)?;
        let uploaded = query.body.len() as u64;
        let response = transport
            .query(shard_id, revision, self.table, &query.body)
            .map_err(classify_transport)?;
        charges.add_query(self.table, uploaded, response.len() as u64);
        self.decode(revision, segments, query, &response)
    }
}

/// Sorts a boxed transport error into the refusals a caller can act on.
///
/// The refusals are recovered before the fallback, because both are boxed into
/// the same `BoxError` as an ordinary failure and stringifying first would
/// throw away the only thing that distinguishes them.
pub(crate) fn classify_transport(error: crate::transport::BoxError) -> ClientError {
    if let Some(stale) = StaleRevision::found_in(&error) {
        return ClientError::Stale(stale.clone());
    }
    if let Some(overloaded) = Overloaded::found_in(&error) {
        return ClientError::Overloaded(overloaded.clone());
    }
    ClientError::Transport(error.to_string())
}
