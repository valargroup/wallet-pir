//! What a shard publishes about itself, and how it is named.
//!
//! A shard's identity is the digest of its own canonical manifest, and the
//! directory holding it must be named by that digest. Recomputing the digest on
//! load is what makes a manifest edited after publication a load failure rather
//! than a shard served under the identity of the one it replaced.
//!
//! The manifest binds everything a consumer needs to decide whether the bytes
//! it received are the bytes that were published: the chain, the range, the
//! geometry, and a digest per segment of each table. It does not, and cannot,
//! establish that the indexer included every event — that stays a separate
//! trust decision.
//!
//! # Revisions
//!
//! A shard is *open* while it is being built, which is not a published state.
//! What is published is either a **provisional** revision of a tail — immutable
//! under its own digest, expected to be superseded by a revision covering more
//! blocks — or a **sealed** shard, which is final and is the only kind that
//! enters the parent-digest chain. Superseding publishes a new digest beside
//! the old one rather than changing bytes under one that has been served.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Identifies the layout this manifest describes.
///
/// An opaque string, refused rather than guessed at: a shard whose entry or
/// page encoding changed would decode to plausible nonsense instead of failing.
pub const SCHEMA: &str = "transparent-shard-v7";

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
    /// Page rows the shard's tables hold, after packing.
    pub page_rows: u64,
    /// Fragments across every indexed history, which is what a wallet's page
    /// queries count. Larger than `page_rows`, because short histories share.
    pub fragments: u64,
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
    /// Events in one fragment, which is the most a single history contributes
    /// to one row.
    pub events_per_page: u32,
    /// Bytes at the head of a page row, before its first entry.
    pub page_row_header_bytes: u32,
    /// Bytes of header on each entry within a page row. With `events_per_page`
    /// this is the whole packing geometry: a reader can derive how many
    /// histories of a given length share a row without being told.
    pub page_entry_header_bytes: u32,
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
    ///
    /// Not the table geometry: this string is keyed into the shard's filter and
    /// into its directory bucket salt, and it names the *filter* profile. The
    /// geometry has its own field below, and confusing the two would silently
    /// change where a script hashes to.
    pub profile: String,
    /// The table geometry this shard was built at, by registry name.
    ///
    /// A set may mix them — archive geometry for old history, a narrower recent
    /// geometry for the window wallets synchronise constantly — so a consumer
    /// cannot infer one shard's shape from another's. The name selects one
    /// validated parameter set; the per-segment `rows` and `row_bytes` below
    /// remain, and are checked against it, so a manifest that named one shape
    /// and published another is refused rather than served.
    pub geometry: String,
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
    /// False for the tail, whose published form is a provisional revision.
    ///
    /// A consumer may use a provisional revision for a current balance, but the
    /// coverage it takes from one is provisional: recorded with this manifest's
    /// digest, never cached as immutable, and re-derived when a later revision
    /// or the sealed shard appears.
    pub sealed: bool,
    /// Which published revision of this shard this manifest describes.
    ///
    /// Zero for a shard published once. A tail's revision advances each time it
    /// is republished with more blocks; sealing is the last such publication.
    pub revision: u32,
    /// Digest of the revision this one supersedes, or empty for the first.
    ///
    /// A wallet holding the superseded revision can see that what it covered
    /// has been replaced rather than extended, and re-derive rather than merge.
    pub supersedes: String,
    pub seal: ManifestSeal,
    pub layout: ManifestLayout,
    /// Double-SHA-256 of the serialized filter bytes, in display hex.
    pub filter_hash: String,
    /// One entry per directory segment, in segment order.
    ///
    /// Ordinarily one. More means this shard's content did not fit a single
    /// segment of the pinned geometry, and the digest binds the whole list, so
    /// a shard cannot gain, lose or reorder a segment without becoming a
    /// different shard.
    pub directory_segments: Vec<TableGeometry>,
    /// One entry per pages segment, in segment order. The segments concatenate
    /// into the shard's page space, which a directory extent indexes.
    pub page_segments: Vec<TableGeometry>,
    pub occupancy: ManifestOccupancy,
}

/// Binds a private query to the revision and table it names.
///
/// Carried in a fixed-width prefix of every query body and echoed in every
/// response. The revision is already public — it is in the map and in the
/// request path — so this discloses nothing; what it buys is that a query
/// routed to the wrong revision, or a response returned from one, fails a
/// check instead of decoding into plausible rows from a range the wallet never
/// asked about.
///
/// Eight bytes because it is a consistency check, not an authenticator: the
/// server has already matched the full digest from the path, and both ends
/// re-derive this from data they hold. Fixed width, so it leaks nothing about
/// the selection.
pub fn query_binding(manifest_digest: &str, table: &str) -> [u8; 8] {
    let mut hasher = Sha256::new();
    hasher.update(SCHEMA.as_bytes());
    hasher.update(b"/query-binding\0");
    hasher.update(manifest_digest.as_bytes());
    hasher.update(b"\0");
    hasher.update(table.as_bytes());
    let digest = hasher.finalize();
    digest[..8].try_into().expect("eight bytes")
}

/// What a shard id already has published, as the publisher reads it back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishedRevision {
    pub digest: String,
    pub revision: u32,
    pub supersedes: String,
    pub sealed: bool,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RevisionError {
    #[error("shard {0} is sealed; a sealed shard's content cannot change")]
    SealedChanged(u64),
}

impl PublishedRevision {
    /// The revision numbering a publication of this shard takes.
    ///
    /// `reproduced` says whether rebuilding under the published numbering
    /// produced the published digest — whether this is the same shard being
    /// published again, or a tail that has grown since.
    ///
    /// Republishing something identical must reproduce the identity it already
    /// has, or an idempotent re-run over the same journal would publish the
    /// same content under a second digest and the map would name the new one.
    /// A tail that has grown takes the next revision and records what it
    /// supersedes. A *sealed* shard whose content changed is neither: sealed
    /// shards are immutable, so that is an error rather than a revision.
    pub fn next(
        shard_id: u64,
        previous: Option<&PublishedRevision>,
        reproduced: bool,
    ) -> Result<(u32, String), RevisionError> {
        match previous {
            None => Ok((0, String::new())),
            Some(previous) if reproduced => Ok((previous.revision, previous.supersedes.clone())),
            Some(previous) if previous.sealed => Err(RevisionError::SealedChanged(shard_id)),
            Some(previous) => Ok((previous.revision + 1, previous.digest.clone())),
        }
    }
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
            geometry: "recent-8k".to_string(),
            network: "main".to_string(),
            genesis_hash: "00".repeat(32),
            shard_id: 3,
            start_height: 3_428_143,
            end_height: 3_430_000,
            parent_block_hash: "11".repeat(32),
            terminal_block_hash: "22".repeat(32),
            parent_manifest_digest: "33".repeat(32),
            sealed: true,
            revision: 0,
            supersedes: String::new(),
            seal: ManifestSeal {
                scripts_target: 8_192,
                scripts_capacity: 16_384,
                page_rows_target: 2_048,
                page_rows_capacity: 4_096,
            },
            layout: ManifestLayout {
                max_script_bytes: 40,
                inline_events: 2,
                events_per_page: 36,
                page_row_header_bytes: 4,
                page_entry_header_bytes: 64,
                directory_choices: 2,
            },
            filter_hash: "44".repeat(32),
            directory_segments: vec![TableGeometry {
                rows: 2_048,
                row_bytes: 3_584,
                sha256: "55".repeat(32),
            }],
            page_segments: vec![TableGeometry {
                rows: 4_096,
                row_bytes: 3_584,
                sha256: "66".repeat(32),
            }],
            occupancy: ManifestOccupancy {
                scripts: 8_193,
                page_rows: 1_879,
                fragments: 3_204,
                events: 39_088,
                blocks: 1_858,
                txids: 10_355,
                excluded_scripts: 0,
            },
        }
    }

    fn published(revision: u32, sealed: bool) -> PublishedRevision {
        PublishedRevision {
            digest: "aa".repeat(32),
            revision,
            supersedes: if revision == 0 {
                String::new()
            } else {
                "bb".repeat(32)
            },
            sealed,
        }
    }

    /// A first publication supersedes nothing.
    #[test]
    fn a_shard_published_for_the_first_time_is_revision_zero() {
        assert_eq!(
            PublishedRevision::next(3, None, false).unwrap(),
            (0, String::new())
        );
    }

    /// Re-running over the same journal must reproduce the identity already
    /// published, not publish the same content again under a new digest.
    #[test]
    fn republishing_the_same_shard_keeps_its_revision() {
        let previous = published(2, false);
        assert_eq!(
            PublishedRevision::next(3, Some(&previous), true).unwrap(),
            (2, previous.supersedes.clone())
        );
    }

    /// A tail that has grown is a new revision beside the old one, recording
    /// what it replaces so a wallet can tell replacement from extension.
    #[test]
    fn a_grown_tail_takes_the_next_revision_and_names_its_predecessor() {
        let previous = published(2, false);
        assert_eq!(
            PublishedRevision::next(3, Some(&previous), false).unwrap(),
            (3, previous.digest.clone())
        );
    }

    /// Sealed shards are immutable. Content that disagrees with a sealed shard
    /// is a build or journal fault, and publishing it as a revision would hide
    /// that behind a version number.
    #[test]
    fn a_sealed_shard_whose_content_changed_is_refused() {
        let previous = published(1, true);
        assert_eq!(
            PublishedRevision::next(3, Some(&previous), false),
            Err(RevisionError::SealedChanged(3))
        );
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
            Box::new(|m| m.revision += 1),
            Box::new(|m| m.supersedes = "99".repeat(32)),
            Box::new(|m| m.filter_hash = "99".repeat(32)),
            Box::new(|m| m.directory_segments[0].sha256 = "99".repeat(32)),
            Box::new(|m| m.page_segments[0].sha256 = "99".repeat(32)),
            Box::new(|m| m.directory_segments[0].rows += 1),
            // A shard cannot gain a segment without becoming a different
            // shard: the segment list is what a wallet asks every segment of.
            Box::new(|m| {
                let extra = m.directory_segments[0].clone();
                m.directory_segments.push(extra);
            }),
            Box::new(|m| {
                let extra = m.page_segments[0].clone();
                m.page_segments.push(extra);
            }),
            Box::new(|m| m.seal.scripts_target += 1),
            Box::new(|m| m.layout.inline_events += 1),
            Box::new(|m| m.occupancy.scripts += 1),
            Box::new(|m| m.occupancy.fragments += 1),
            Box::new(|m| m.layout.page_entry_header_bytes += 1),
            Box::new(|m| m.layout.page_row_header_bytes += 1),
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
