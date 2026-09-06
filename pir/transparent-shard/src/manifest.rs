//! What a shard publishes about itself, and how it is named.
//!
//! A shard's identity is the digest of its own canonical manifest, and the
//! directory holding it must be named by that digest. Recomputing the digest on
//! load is what makes a manifest edited after publication a load failure rather
//! than a shard served under the identity of the one it replaced.
//!
//! The manifest binds everything a consumer needs to decide whether the bytes
//! it received are the bytes that were published: the chain, the range, the
//! geometry, and a digest per table. It does not, and cannot, establish that
//! the indexer included every event — that stays a separate trust decision.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Identifies the layout this manifest describes.
///
/// An opaque string, refused rather than guessed at: a shard whose entry or
/// page encoding changed would decode to plausible nonsense instead of failing.
pub const SCHEMA: &str = "transparent-shard-v1";

/// Geometry and digest of one table.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TableGeometry {
    pub rows: u64,
    pub row_bytes: u32,
    /// SHA-256 of the table file, in hex.
    pub sha256: String,
}

/// What a shard actually holds. Published so an operator's boundary can be
/// checked against the seal parameters rather than taken on trust.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ManifestOccupancy {
    pub scripts: u64,
    pub page_rows: u64,
    pub events: u64,
    pub blocks: u64,
    /// Distinct transaction ids. Reported only; the transaction-detail table
    /// they would size is not built.
    pub txids: u64,
    /// Scripts present in the filter but absent from the directory because they
    /// exceed `max_script_bytes`.
    ///
    /// A wallet holding such a script is outside coverage and must not read a
    /// directory miss as absence. Publishing the count makes the gap a measured
    /// quantity rather than a silent one.
    pub excluded_scripts: u64,
}

/// The pinned geometry, restated per shard.
///
/// Redundant with the schema by design. A consumer checks these against its own
/// constants before decoding anything, so a build that changed a width fails
/// loudly at the first shard instead of returning misaligned records.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ManifestLayout {
    pub max_script_bytes: u32,
    pub inline_events: u32,
    pub events_per_page: u32,
    pub directory_choices: u32,
}

/// Seal parameters, as schema rather than tuning.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ManifestSeal {
    pub scripts_target: u64,
    pub scripts_capacity: u64,
    pub page_rows_target: u64,
    pub page_rows_capacity: u64,
}

/// One shard's complete public description.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ShardManifest {
    pub schema: String,
    /// The range filter profile, distinct from the per-block one.
    pub profile: String,
    pub network: String,
    /// Chain identity, in display hex.
    pub genesis_hash: String,
    pub shard_id: u64,
    pub start_height: u64,
    pub end_height: u64,
    /// The block before `start_height`, in display hex.
    pub parent_block_hash: String,
    /// The block at `end_height`, in display hex.
    pub terminal_block_hash: String,
    /// Digest of the preceding shard's manifest, or empty for shard zero.
    ///
    /// This is what makes the shard set a committed sequence rather than a bag
    /// of independently published ranges: a consumer can walk it and see that
    /// nothing was substituted or omitted between two shards it accepts.
    pub parent_manifest_digest: String,
    /// False for the tail, which is still growing.
    ///
    /// A consumer may use an unsealed shard for a current balance but must not
    /// cache it as immutable, and must re-fetch rather than trust a retained
    /// copy.
    pub sealed: bool,
    pub seal: ManifestSeal,
    pub layout: ManifestLayout,
    /// Double-SHA-256 of the serialized filter bytes, in display hex.
    pub filter_hash: String,
    pub directory: TableGeometry,
    pub pages: TableGeometry,
    pub occupancy: ManifestOccupancy,
}

impl ShardManifest {
    /// The canonical bytes this manifest digests to.
    ///
    /// Field order is the struct's declaration order, which serde preserves, so
    /// the encoding is stable without needing a separate canonicalization pass.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a manifest serializes")
    }

    /// The shard's identity: the digest of its canonical bytes.
    pub fn digest(&self) -> String {
        hex::encode(Sha256::digest(self.canonical_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> ShardManifest {
        ShardManifest {
            schema: SCHEMA.to_string(),
            profile: "zcash-transparent-range-v1".to_string(),
            network: "main".to_string(),
            genesis_hash: "00".repeat(32),
            shard_id: 3,
            start_height: 3_428_143,
            end_height: 3_430_000,
            parent_block_hash: "11".repeat(32),
            terminal_block_hash: "22".repeat(32),
            parent_manifest_digest: "33".repeat(32),
            sealed: true,
            seal: ManifestSeal {
                scripts_target: 8_192,
                scripts_capacity: 16_384,
                page_rows_target: 2_048,
                page_rows_capacity: 4_096,
            },
            layout: ManifestLayout {
                max_script_bytes: 40,
                inline_events: 2,
                events_per_page: 185,
                directory_choices: 2,
            },
            filter_hash: "44".repeat(32),
            directory: TableGeometry {
                rows: 2_048,
                row_bytes: 3_584,
                sha256: "55".repeat(32),
            },
            pages: TableGeometry {
                rows: 4_096,
                row_bytes: 17_920,
                sha256: "66".repeat(32),
            },
            occupancy: ManifestOccupancy {
                scripts: 8_193,
                page_rows: 1_879,
                events: 39_088,
                blocks: 1_858,
                txids: 10_355,
                excluded_scripts: 0,
            },
        }
    }

    #[test]
    fn a_manifest_round_trips_through_its_canonical_bytes() {
        let original = manifest();
        let decoded: ShardManifest =
            serde_json::from_slice(&original.canonical_bytes()).expect("decode");
        assert_eq!(decoded, original);
    }

    #[test]
    fn the_digest_is_stable_across_encodings() {
        assert_eq!(manifest().digest(), manifest().digest());
        assert_eq!(manifest().digest().len(), 64);
    }

    /// Every field must reach the digest. A field that fell out of it could be
    /// changed after publication without changing the shard's identity, which
    /// is exactly what naming the directory by the digest is meant to prevent.
    #[test]
    fn changing_any_field_changes_the_identity() {
        let base = manifest().digest();
        type Mutation = Box<dyn Fn(&mut ShardManifest)>;
        let mutations: Vec<Mutation> = vec![
            Box::new(|m| m.shard_id += 1),
            Box::new(|m| m.start_height += 1),
            Box::new(|m| m.end_height += 1),
            Box::new(|m| m.parent_block_hash = "99".repeat(32)),
            Box::new(|m| m.terminal_block_hash = "99".repeat(32)),
            Box::new(|m| m.parent_manifest_digest = "99".repeat(32)),
            Box::new(|m| m.sealed = false),
            Box::new(|m| m.filter_hash = "99".repeat(32)),
            Box::new(|m| m.directory.sha256 = "99".repeat(32)),
            Box::new(|m| m.pages.sha256 = "99".repeat(32)),
            Box::new(|m| m.directory.rows += 1),
            Box::new(|m| m.seal.scripts_target += 1),
            Box::new(|m| m.layout.inline_events += 1),
            Box::new(|m| m.occupancy.scripts += 1),
            Box::new(|m| m.occupancy.excluded_scripts += 1),
            Box::new(|m| m.genesis_hash = "99".repeat(32)),
            Box::new(|m| m.profile = "other".to_string()),
        ];
        for mutate in mutations {
            let mut changed = manifest();
            mutate(&mut changed);
            assert_ne!(
                changed.digest(),
                base,
                "a mutation did not change the digest"
            );
        }
    }
}
