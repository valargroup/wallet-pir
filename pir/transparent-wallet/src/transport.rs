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
//!
//! Private bytes are split by *table* for the same reason. The directory is
//! what a script costs to find and the pages are what its history costs to
//! read, and geometry moves cost between them rather than removing it: wider
//! shards mean fewer directory lookups, but a script's two inline events are
//! granted per shard, so the same history spread over fewer shards keeps fewer
//! of them inline and pays a page query instead. A single private total cannot
//! show that transfer, and a geometry cannot be chosen without seeing it.

use crate::client::Table;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Bytes charged for one private table.
///
/// Every field here is per table because the tables have different geometry —
/// the directory is 2,048 rows and the pages 8,192 — so their queries do not
/// even cost the same number of bytes. Summing them first and dividing later
/// would attribute an average to both.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableCharges {
    /// Published PIR setup, once per segment opened. A shard normally has one
    /// segment per table; one that did not fit the pinned geometry has more,
    /// and each publishes its own `c1`.
    pub setup_bytes: u64,
    /// Private query uploads.
    pub query_upload: u64,
    /// Private query responses.
    pub query_download: u64,
    pub queries: u64,
    /// Segments opened, which is also the number of setups fetched.
    pub segments_opened: u64,
}

impl TableCharges {
    pub fn total(&self) -> u64 {
        self.setup_bytes + self.query_upload + self.query_download
    }
}

/// Bytes charged, split by stage and, for the private stages, by table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ByteCharges {
    /// The shard map. Small, but a wallet cannot start without it.
    pub map_bytes: u64,
    /// Public activity filters. Paid by every wallet whether or not it matches.
    pub filter_bytes: u64,
    /// Finding a script.
    pub directory: TableCharges,
    /// Reading the history the directory located.
    pub pages: TableCharges,
    pub filters_checked: u64,
}

impl ByteCharges {
    fn table_mut(&mut self, table: Table) -> &mut TableCharges {
        match table {
            Table::Directory => &mut self.directory,
            Table::Pages => &mut self.pages,
        }
    }

    pub fn add_query(&mut self, table: Table, upload: u64, download: u64) {
        let charges = self.table_mut(table);
        charges.query_upload += upload;
        charges.query_download += download;
        charges.queries += 1;
    }

    pub fn add_setup(&mut self, table: Table, cost: u64) {
        let charges = self.table_mut(table);
        charges.setup_bytes += cost;
        charges.segments_opened += 1;
    }

    pub fn setup_bytes(&self) -> u64 {
        self.directory.setup_bytes + self.pages.setup_bytes
    }

    pub fn query_upload(&self) -> u64 {
        self.directory.query_upload + self.pages.query_upload
    }

    pub fn query_download(&self) -> u64 {
        self.directory.query_download + self.pages.query_download
    }

    pub fn queries(&self) -> u64 {
        self.directory.queries + self.pages.queries
    }

    /// Setups fetched: one per segment of each table a sync opened.
    ///
    /// Named for the field the archived measurements report, so a new run stays
    /// comparable with them.
    pub fn shards_opened(&self) -> u64 {
        self.directory.segments_opened + self.pages.segments_opened
    }

    pub fn total(&self) -> u64 {
        self.map_bytes + self.filter_bytes + self.directory.total() + self.pages.total()
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
