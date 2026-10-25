//! Transparent chain extraction shared by History, transaction details and the
//! offline tools: block fetches, event extraction, previous-output resolution
//! and the event journals built from them.

pub mod blocks;
pub mod extract;
pub mod journal;
pub mod prevout;
pub mod snapshot;
#[cfg(feature = "rocksdb-reader")]
pub mod state;
pub mod zakura;

// The extractor's tests reach the journal through their grandparent module, so
// the standalone txid sizing exporter can compile the same file beside its
// own copies of these modules.
#[cfg(all(test, feature = "fixtures"))]
use transparent_shared::journal::{display_journal, events};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
