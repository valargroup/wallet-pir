//! Serving the time-tiered, bucketed txid display publication (proof of
//! concept).
//!
//! A display publication is not a history shard set: it has its own map
//! (`txid-shards.json`), its own manifests and its own revisions, and the
//! history loader would refuse every one of them. It is therefore served by a
//! separate process and a separate set of routes under `/v1/txid/`, which
//! reuse the history worker's machinery unchanged: the runtime cache, the
//! shared native parameters, admission, work-memory accounting, metrics and
//! the disk cache.
//!
//! **Roles.** A worker either owns the sealed archive shards or replicates the
//! recent shard, never both. It reads every manifest of a publication, so any
//! worker can answer the map and manifest routes, but it verifies, builds and
//! serves only the tables of its own role's revisions. A request for another
//! role's revision is refused with 421, as a history worker refuses an
//! unassigned shard.
//!
//! **Tiers in the path.** Data-plane routes name the tier,
//! `/v1/txid/{archive|recent}/...`, so a static edge route can send archive
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
pub mod set;
pub mod synth;

use crate::assignment::WorkerRole;
use crate::runtime::RuntimeKey;
use crate::shardset::Table;
use transparent_shard::display::DisplayTable;

/// The history table kind whose native parameters, row count and row width
/// a display table uses. Seeds are derived from the history schema and this
/// kind's name, so the parameters are exactly the history display tables'.
pub fn kind(table: DisplayTable) -> Table {
    match table {
        DisplayTable::Directory(_) => Table::TxDirectory,
        DisplayTable::Pages => Table::TxPages,
    }
}

/// The cache address of one segment of one display table.
///
/// Pages are keyed by the bare digest, directories by digest and bucket, so a
/// revision's buckets and its pages never collide. Disk-cache pruning keeps
/// entries by these strings, so every keep set must be built from here.
pub fn runtime_key(digest: &str, table: DisplayTable, segment: u32) -> RuntimeKey {
    match table {
        DisplayTable::Directory(bucket) => (
            format!("{digest}/directory-{bucket}"),
            Table::TxDirectory,
            segment,
        ),
        DisplayTable::Pages => (digest.to_string(), Table::TxPages, segment),
    }
}

/// Parses a role by the names history assignments use.
pub fn parse_role(text: &str) -> Option<WorkerRole> {
    [WorkerRole::RecentReplica, WorkerRole::ArchiveOwner]
        .into_iter()
        .find(|role| role.as_str() == text)
}

/// Whether a worker of `role` holds the tables of a revision with this sealed
/// flag: owners hold sealed archives, replicas hold the unsealed recent shard.
pub fn serves(role: WorkerRole, sealed: bool) -> bool {
    match role {
        WorkerRole::ArchiveOwner => sealed,
        WorkerRole::RecentReplica => !sealed,
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
            runtime_key(&a, DisplayTable::Pages, 0),
            runtime_key(&a, DisplayTable::Directory(0), 1),
            runtime_key(&b, DisplayTable::Directory(0), 0),
        ];
        for (i, x) in keys.iter().enumerate() {
            for y in &keys[i + 1..] {
                assert_ne!(x, y);
            }
        }
        assert_eq!(keys[0].0, format!("{a}/directory-0"));
        assert_eq!(keys[2], (a.clone(), Table::TxPages, 0));
        // Disk pruning keeps entries by the hash of the key string, so the
        // strings, not just the tuples, must differ per bucket.
        assert_ne!(keys[0].0, keys[1].0);
    }

    #[test]
    fn roles_parse_by_history_names_and_split_tiers() {
        for role in [WorkerRole::RecentReplica, WorkerRole::ArchiveOwner] {
            assert_eq!(parse_role(role.as_str()), Some(role));
        }
        assert_eq!(parse_role("archive"), None);
        assert!(serves(WorkerRole::ArchiveOwner, true));
        assert!(!serves(WorkerRole::ArchiveOwner, false));
        assert!(serves(WorkerRole::RecentReplica, false));
        assert!(!serves(WorkerRole::RecentReplica, true));
        assert_eq!(tier(true), "archive");
        assert_eq!(tier(false), "recent");
    }
}
