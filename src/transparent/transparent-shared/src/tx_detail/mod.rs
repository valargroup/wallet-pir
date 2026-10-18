//! Display publications and prepared runtimes shared by the publisher and worker.

pub mod prebuild;
pub mod set;
#[cfg(any(test, feature = "test-support"))]
pub mod synth;

use crate::history::shardset::Table;
use crate::runtime::RuntimeKey;
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
