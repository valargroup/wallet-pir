//! Time-tiered txid display publication (proof of concept).
//!
//! A display publication is separate from the history shards: its own map
//! (`txid-shards.json`), manifests and revisions, built from the display
//! sidecars of the event journal. Archive shards seal once and never change;
//! one recent shard covering the tip is rebuilt as blocks arrive.
//!
//! - [`cache`] holds the unsealed range in memory, so a rebuild never re-reads
//!   sidecars.
//! - [`publisher`] writes revisions and candidate publications, bootstraps a
//!   root from the journal and verifies what is on disk.
//! - [`source`] supplies blocks: live from the node, replayed from an ingested
//!   journal, or scripted by tests.
//! - [`serving`] drives display workers over their control protocol.
//! - [`controller`] is the loop that seals, rebuilds and publishes.
//! - [`timeline`] records what happened and when, for measurement.

pub mod cache;
pub mod controller;
pub mod publisher;
pub mod serving;
pub mod source;
pub mod timeline;

pub use crate::publication::BoxError;

/// Wall-clock milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) mod fixture;
