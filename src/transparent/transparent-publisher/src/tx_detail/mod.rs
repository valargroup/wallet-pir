//! Time-tiered transaction-detail publication for the root publisher.
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
//! - [`controller`] supplies source-bound work to the root executor.
//! - [`distribution`] serves finalized publications to query servers over HTTPS.
//! - [`offline`] verifies a root against its journal, plans a bootstrap
//!   start, benches recent shards and writes synthetic journals.
//! - [`timeline`] records what happened and when, for measurement.
//! - [`metrics`] is what the controller reports on `/metrics`.

mod activation;
pub mod cache;
pub mod controller;
pub mod distribution;
pub mod metrics;
pub mod offline;
pub mod publisher;
pub mod source;
pub mod timeline;

pub use crate::history::publication::BoxError;

/// Wall-clock milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) mod fixture;

pub mod runtime_preparation;
pub mod service;

#[cfg(all(test, feature = "qualification-tests", feature = "fixtures"))]
#[path = "../../../../pir-publisher/tests/included/http_sync_tests.rs"]
mod http_sync_tests;

// The serving tests' independent wallet transport, mounted once for both
// qualification harnesses below this module.
#[cfg(all(test, feature = "qualification-tests", feature = "fixtures"))]
#[path = "../../../transparent-query-server/tests/support/display_world.rs"]
pub(crate) mod display_world;
