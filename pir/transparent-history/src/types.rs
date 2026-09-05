//! Protocol types for private transparent-history retrieval.
//!
//! The tables served here are the `directory.bin` and `pages.bin` of a
//! generation built by the research harness (`tools/transparent_pir_incremental.py`,
//! `build_generation`). This crate does not build them: it fixes the wire
//! contract for retrieving rows from them privately, so that a measurement runs
//! over real transport rather than an in-process round trip.

use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u32 = 1;
pub const PROTOCOL_REVISION: &str = "transparent-history-pir-v1";
pub const NETWORK: &str = "main";

/// Width of one encoded database column, in bits.
///
/// This is the packing width the server uses when it lays a row out across
/// columns, not the size of a row. A row's item size is `row_bytes * 8`, which
/// is what the scheme parameters are derived from.
pub const COLUMN_BITS: usize = 14;

/// Distinct published setup seeds per table.
///
/// The two tables have different geometries, so their public parameters differ
/// and cannot be shared. Separate seeds make a client that mixed them up fail
/// its own re-derivation check rather than decode a row from the wrong table.
pub const DIRECTORY_SETUP_SEED: u64 = 0x7472_616e_7364_6972;
pub const PAGES_SETUP_SEED: u64 = 0x7472_616e_7370_6167;

/// Which of a generation's two tables a query addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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

    pub fn setup_seed(self) -> u64 {
        match self {
            Table::Directory => DIRECTORY_SETUP_SEED,
            Table::Pages => PAGES_SETUP_SEED,
        }
    }
}

/// Identity and geometry of one served table within a generation.
///
/// `generation` is the harness's generation digest, truncated to eight bytes
/// for the wire prefix. The full hex digest travels in `generation_id` so a
/// client can bind results to the published generation rather than to a
/// truncation of it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryTableGeneration {
    pub schema_version: u32,
    pub protocol_revision: String,
    pub network: String,
    pub generation_id: String,
    pub generation: u64,
    pub table: Table,
    pub rows: u64,
    pub row_bytes: u32,
    pub setup_seed: u64,
    /// SHA-256 of the table file, as recorded in the generation manifest.
    pub table_sha256: String,
}

/// One table's session: its identity plus the published public parameters.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryTableSession {
    pub generation: HistoryTableGeneration,
    pub scheme: ipir_sp::YpirSchemeParams,
    /// Base64 published `c1`, prefixed with an eight-byte parameter epoch.
    pub public_params: String,
    pub public_params_sha256: String,
    /// First eight bytes of the parameter digest, in hex. Every response
    /// carries it so a client cannot decode against superseded parameters.
    pub public_params_epoch: String,
}

/// What `GET /v1/transparent-history/init` returns.
///
/// Both tables come from one generation and are described together, so a client
/// cannot assemble a session from two different generations by making two
/// requests that race a publication.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistorySession {
    pub generation_id: String,
    /// Chain range the generation covers, from its manifest.
    pub start_height: u64,
    pub end_height: u64,
    pub anchor_block_hash: String,
    pub directory: HistoryTableSession,
    pub pages: HistoryTableSession,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names_and_seeds_are_distinct() {
        assert_ne!(Table::Directory.setup_seed(), Table::Pages.setup_seed());
        assert_ne!(Table::Directory.as_str(), Table::Pages.as_str());
    }

    #[test]
    fn tables_round_trip_through_json_as_lowercase_names() {
        let encoded = serde_json::to_string(&Table::Pages).unwrap();
        assert_eq!(encoded, "\"pages\"");
        let decoded: Table = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, Table::Pages);
    }
}
