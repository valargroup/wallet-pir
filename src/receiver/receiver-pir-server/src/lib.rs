//! Receiver PIR serving with immutable publications and atomic session revocation.
#[cfg(test)]
#[path = "../../receiver-store/tests/common/mod.rs"]
mod common;
#[cfg(test)]
extern crate self as receiver_pir_server;
pub mod blocks;
pub mod distribution;
#[cfg(test)]
#[path = "../tests/common/prepared.rs"]
mod prepared;

/// An empty minimum-geometry snapshot whose salt and end hash are `variant`.
///
/// Publication and table tests share these manifests, so the process cache
/// prepares each one once. This stays in the serving crate: the indexer includes
/// the preparation cache without this fixture module.
#[cfg(test)]
use zakura_pir_publisher::receiver::snapshot::SnapshotPublisherExt as _;
#[cfg(test)]
fn empty_snapshot(variant: u8) -> zakura_pir_query_server::receiver::snapshot::Snapshot {
    let mut manifest = common::manifest(zakura_pir_query_server::receiver::session::MIN_ROWS);
    manifest.salt[0] = variant;
    manifest.end_hash = [variant; 32];
    zakura_pir_query_server::receiver::snapshot::Snapshot::build(manifest, &[]).unwrap()
}
pub mod publication;
pub mod tables;
pub use publication::{Publication, Publications};
