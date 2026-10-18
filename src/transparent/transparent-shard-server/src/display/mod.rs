//! Serving the time-tiered, bucketed txid display publication (proof of
//! concept).
//!
//! A display publication is not a history shard set: it has its own map
//! (`txid-shards.json`), its own manifests and its own revisions, and the
//! history loader would refuse every one of them. It is therefore served by a
//! separate process and a separate set of routes under `/v2/txid/`, which
//! reuse the history worker's machinery unchanged: the runtime cache, the
//! shared native parameters, admission, work-memory accounting, metrics and
//! the disk cache.
//!
//! **Roles.** An archive owner holds the sealed archive shards, a recent
//! replica holds the recent shard, and a combined worker holds both, so one
//! host can serve the whole publication. A worker reads every manifest of a
//! publication, so any worker can answer the map and manifest routes, but it
//! verifies, builds and serves only the tables of its own role's revisions. A
//! request for another role's revision is refused with 421, as a history
//! worker refuses an unassigned shard.
//!
//! **The split map.** Besides the full map (`/v2/txid/shards`, kept for
//! clients built before the split), every worker serves the recent map
//! (`/v2/txid/map`) and the archive index chunks
//! (`/v2/txid/map/{base}/{sha256}`) derived from the same map; see
//! `transparent_shard::display::split`.
//!
//! **Tiers in the path.** Data-plane routes name the tier,
//! `/v2/txid/{archive|recent}/...`, so a static edge route can send archive
//! traffic to its owner without knowing which shard ids are sealed. The tier
//! must equal the revision's sealed flag, which is what the map already
//! publishes, so naming it discloses nothing beyond the shard id.
//!
//! **Buckets are tables.** Every bucket of a shard is its own native table,
//! `directory-{b}`, sharing the native parameters of its geometry's
//! `txdirectory` table with every other bucket and shard. Runtimes are cached
//! under `"{digest}/directory-{b}"`, so two buckets of one revision never
//! share a cache entry.

pub mod live;
pub mod service;

pub mod prebuild;
pub mod set;
pub mod synth;

use transparent_server_backend::runtime::RuntimeKey;
use transparent_server_backend::shardset::Table;
use transparent_shard::display::{DisplayKind, DisplayTable};

/// The history table kind whose row count and row width a display table
/// uses. Its native profile and setup seeds are derived under display's own
/// [`NATIVE_SCHEMA`](transparent_shard::display::NATIVE_SCHEMA) and this
/// kind's name, not under the history schema.
pub fn kind(table: DisplayTable) -> Table {
    match table.kind() {
        DisplayKind::TxDirectory => Table::TxDirectory,
    }
}

/// The cache address of one segment of one display table.
///
/// Keyed by digest and bucket, so a revision's buckets never collide.
/// Disk-cache pruning keeps entries by these strings, so every keep set must
/// be built from here.
pub fn runtime_key(digest: &str, table: DisplayTable, segment: u32) -> RuntimeKey {
    (
        format!("{digest}/{}", table.label()),
        Table::TxDirectory,
        segment,
    )
}

/// Which tiers of a display publication a worker holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DisplayRole {
    /// The unsealed recent shard only.
    RecentReplica,
    /// The sealed archive shards only.
    ArchiveOwner,
    /// Both: the sealed archives and the recent shard.
    Combined,
}

impl DisplayRole {
    pub const ALL: [Self; 3] = [Self::RecentReplica, Self::ArchiveOwner, Self::Combined];

    /// The name on the command line, in status replies and in the
    /// controller's worker list. The first two are history's role names.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RecentReplica => "recent-replica",
            Self::ArchiveOwner => "archive-owner",
            Self::Combined => "combined",
        }
    }
}

/// Parses a role by its [`DisplayRole::as_str`] name.
pub fn parse_role(text: &str) -> Option<DisplayRole> {
    DisplayRole::ALL
        .into_iter()
        .find(|role| role.as_str() == text)
}

/// Whether a worker of `role` holds the tables of a revision with this sealed
/// flag: owners hold sealed archives, replicas hold the unsealed recent shard,
/// and a combined worker holds both.
pub fn serves(role: DisplayRole, sealed: bool) -> bool {
    match role {
        DisplayRole::ArchiveOwner => sealed,
        DisplayRole::RecentReplica => !sealed,
        DisplayRole::Combined => true,
    }
}

/// The path tier of a revision with this sealed flag.
pub fn tier(sealed: bool) -> &'static str {
    if sealed {
        "archive"
    } else {
        "recent"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_keys_separate_buckets_tables_and_revisions() {
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let keys = [
            runtime_key(&a, DisplayTable::Directory(0), 0),
            runtime_key(&a, DisplayTable::Directory(1), 0),
            runtime_key(&a, DisplayTable::Directory(0), 1),
            runtime_key(&b, DisplayTable::Directory(0), 0),
        ];
        for (i, x) in keys.iter().enumerate() {
            for y in &keys[i + 1..] {
                assert_ne!(x, y);
            }
        }
        assert_eq!(keys[0], (format!("{a}/directory-0"), Table::TxDirectory, 0));
        // Disk pruning keeps entries by the hash of the key string, so the
        // strings, not just the tuples, must differ per bucket.
        assert_ne!(keys[0].0, keys[1].0);
    }

    #[test]
    fn roles_parse_by_name_and_split_tiers() {
        for role in DisplayRole::ALL {
            assert_eq!(parse_role(role.as_str()), Some(role));
        }
        assert_eq!(parse_role("archive"), None);
        assert!(serves(DisplayRole::ArchiveOwner, true));
        assert!(!serves(DisplayRole::ArchiveOwner, false));
        assert!(serves(DisplayRole::RecentReplica, false));
        assert!(!serves(DisplayRole::RecentReplica, true));
        assert!(serves(DisplayRole::Combined, true));
        assert!(serves(DisplayRole::Combined, false));
        assert_eq!(tier(true), "archive");
        assert_eq!(tier(false), "recent");
    }
}
