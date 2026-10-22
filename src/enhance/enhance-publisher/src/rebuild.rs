//! Offline repair of historical Enhance fee metadata and its adoption.
//!
//! The journal stores only encoded records, so a record published without a
//! fee can only be repaired by re-deriving it from chain. `rebuild_journal`
//! re-runs the coordinator's producer over a copy of the live journal's
//! history and accepts a regenerated record only if it differs from the old
//! one in the fee fields alone. `adopt_staged` swaps a completed rebuild in
//! before the coordinator opens its journal, after checking it again against
//! the live journal; it refuses, and keeps the live journal, on any doubt.

mod adoption;
mod comparison;
mod reconstruction;

use crate::{
    chain_source::ZakuraError,
    journal::{JournalBlock, JournalError},
};
pub use adoption::adopt_staged;
pub use comparison::fee_only_change;
pub use reconstruction::rebuild_journal;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};

/// Staged rebuild directory inside a coordinator data directory.
pub const STAGED_DIR: &str = "enhance-staged";
/// Completion receipt, written last inside the staged directory.
pub const RECEIPT_FILE: &str = "rebuild.json";
const RECEIPT_FORMAT: &str = "enhance-journal-rebuild-v1";
/// Records compared per read when streaming two journals.
const COMPARE_RECORDS: usize = 4096;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum RebuildError {
    #[error("journal error: {0}")]
    Store(#[from] JournalError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Zakura error at height {0}: {1}")]
    Zakura(u64, ZakuraError),
    #[error("rebuild refused: {0}")]
    Refused(String),
    #[error("record {position} differs outside the fee fields: {reason}")]
    Oracle { position: u64, reason: String },
}

/// Completion receipt of a staged rebuild. Digests bind the staged files.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RebuildReceipt {
    pub format: String,
    pub receipt_id: String,
    /// Compile-time `WALLET_PIR_SOURCE_REVISION`, or `unrecorded`.
    pub binary_source_revision: String,
    pub binary_sha256: String,
    pub height: u64,
    pub block_hash: String,
    pub tree_size: u64,
    pub records_sha256: String,
    pub manifest_sha256: String,
    pub changed_records: u64,
    /// Records still without a fee: Ironwood outputs of coinbase transactions.
    pub absent_fee_records: u64,
    pub completed_unix_seconds: u64,
}

/// Source history and bounded canonical fetch settings for an offline staged rebuild.
pub struct RebuildConfig {
    pub source: PathBuf,
    pub output: PathBuf,
    pub through_height: Option<u64>,
    pub concurrency: usize,
    pub cache_outputs: usize,
}

fn is_prefix(staged: &[JournalBlock], live: &[JournalBlock]) -> bool {
    staged.len() <= live.len() && staged.iter().zip(live).all(|(s, l)| s == l)
}

fn now_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[derive(Debug, PartialEq, Eq)]
/// Startup outcome of validating and installing a completed staged rebuild.
pub enum JournalAdoption {
    /// No completed staged rebuild is present.
    None,
    /// The verified journal is live; the predecessor remains at `previous`.
    Adopted {
        receipt_id: String,
        height: u64,
        changed_records: u64,
        previous: PathBuf,
    },
    /// The staged journal failed validation and was moved aside for inspection.
    Rejected { reason: String, moved_to: PathBuf },
}

#[cfg(test)]
mod tests;
