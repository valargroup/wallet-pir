//! What a wallet fetches, and what it is charged for.
//!
//! Filters and private queries come from *different* sources by construction.
//! A wallet must not learn to fetch public bytes from the same place it makes
//! private requests, or the two become correlated whatever the protocol says,
//! so the two are separate traits and a caller has to supply both.
//!
//! # Accounting
//!
//! Bytes are counted as delivered — envelope and framing included, and
//! including bytes paid for on an attempt that later failed. A measurement that
//! counted only payloads would understate what a wallet actually pays, which is
//! the number the whole design has to beat.
//!
//! Stages are kept apart because they answer different questions. Filter bytes
//! are paid by *every* wallet unconditionally and are the floor. Setup is paid
//! once per shard a wallet opens. Query bytes scale with matches. Collapsing
//! them into one total would hide which part is actually expensive.

use crate::client::Table;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Bytes charged, split by stage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ByteCharges {
    /// The shard map. Small, but a wallet cannot start without it.
    pub map_bytes: u64,
    /// Public activity filters. Paid by every wallet whether or not it matches.
    pub filter_bytes: u64,
    /// Published PIR setup, once per segment opened. A shard normally has one
    /// segment per table; one that did not fit the pinned geometry has more,
    /// and each publishes its own `c1`.
    pub setup_bytes: u64,
    /// Private query uploads.
    pub query_upload: u64,
    /// Private query responses.
    pub query_download: u64,
    pub queries: u64,
    /// Setups fetched: one per segment of each table a sync opened.
    pub shards_opened: u64,
    pub filters_checked: u64,
}

impl ByteCharges {
    pub fn add_query(&mut self, upload: u64, download: u64) {
        self.query_upload += upload;
        self.query_download += download;
        self.queries += 1;
    }

    pub fn total(&self) -> u64 {
        self.map_bytes
            + self.filter_bytes
            + self.setup_bytes
            + self.query_upload
            + self.query_download
    }

    /// What a wallet pays before issuing a single private query.
    ///
    /// This is the floor the design has to beat on its own, before any of the
    /// retrieval it gates is counted.
    pub fn public_floor(&self) -> u64 {
        self.map_bytes + self.filter_bytes
    }
}

/// Source of public shard filters.
///
/// Separate from [`ShardTransport`] on purpose. These bytes are identical for
/// every wallet and reveal nothing; the private ones do not have that property,
/// and taking both from one place would correlate them.
pub trait FilterSource {
    /// The published shard map, as JSON, with the bytes it cost.
    fn shard_map(&mut self) -> Result<(Vec<u8>, u64), BoxError>;

    /// One shard's filter bytes, with what they cost.
    fn filter(&mut self, shard_id: u64) -> Result<(Vec<u8>, u64), BoxError>;
}

/// Source of private shard retrieval.
pub trait ShardTransport {
    /// The service's init document, as JSON, with the bytes it cost.
    fn init(&mut self) -> Result<(Vec<u8>, u64), BoxError>;

    /// One segment's published setup for one table, as JSON, with its cost.
    fn setup(
        &mut self,
        shard_id: u64,
        table: Table,
        segment: u32,
    ) -> Result<(Vec<u8>, u64), BoxError>;

    /// Answers one private query, against every segment of the shard.
    ///
    /// The body is opaque and fixed length, and names a row within a segment
    /// rather than a segment: the answer carries one body per segment, in
    /// segment order.
    fn query(&mut self, shard_id: u64, table: Table, body: &[u8]) -> Result<Vec<u8>, BoxError>;
}
