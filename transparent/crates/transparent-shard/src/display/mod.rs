//! Time-tiered, hash-bucketed txid display tables (proof of concept).
//!
//! A display publication is its own sequence of shards, independent of the
//! history shards: sealed archive shards that are built once, and one recent
//! shard covering the tip that is rebuilt as blocks arrive. Inside a shard,
//! every txid belongs to exactly one bucket, `H(domain, txid) mod N`, and each
//! bucket is a separate native table, so a lookup scans and downloads only its
//! own bucket. Both candidate directory rows lie inside that bucket's table.
//! Overflow pages are scoped to the shard and are not bucketed.
//!
//! The record codec, directory entry, row container and fragment format are
//! the ones in [`crate::txid`]; only placement, table identity and the
//! manifest differ. The bucket a lookup names and the tier its shard belongs
//! to are disclosed to the server, in addition to the range, page count and
//! timing that the single-table capability already discloses.

pub mod geometry;
pub mod manifest;
pub mod seal;
pub mod tables;

pub use geometry::{display_by_name, DISPLAY_PROFILES, TXID_2K, TXID_4K};
pub use manifest::{
    DisplayBucket, DisplayLayout, DisplayManifest, DisplayMap, DisplayMapEntry, ManifestHeader,
};
pub use seal::{archive_boundary, height_counts, plan_seals, DisplaySealParams, HeightCounts};
pub use tables::{
    build_shard, directory_rows_for, find_directory_unique, verify, verify_rows, BuiltBucket,
    BuiltDisplay, VerifiedDisplay,
};

use crate::layout::Geometry;
use crate::txid::{self, DirectoryEntry};
use sha2::{Digest, Sha256};
use transparent_events::Txid;

/// Identifies the display shard layout: bucketed directory tables beside one
/// shard-scoped pages table, with the `txid` record codec inside.
pub const DISPLAY_SCHEMA: &str = "transparent-txid-display-shard-v1";
/// Domain of the bucket hash. Shard-independent, so a txid's bucket is the
/// same whichever shard its height selects.
pub const BUCKET_DOMAIN: &[u8] = b"transparent-txid-display/bucket/v1";
/// Upper bound on buckets per shard; a decoder bound, not a target.
pub const MAX_BUCKETS: u32 = 64;

/// The bucket `txid` belongs to among `n_buckets`.
pub fn bucket(txid: &Txid, n_buckets: u32) -> u32 {
    assert!(n_buckets > 0, "a shard has at least one bucket");
    let digest = Sha256::new()
        .chain_update(BUCKET_DOMAIN)
        .chain_update(txid.0)
        .finalize();
    (u64::from_le_bytes(digest[..8].try_into().unwrap()) % u64::from(n_buckets)) as u32
}

/// The two candidate directory rows of `txid` inside its bucket's table.
pub fn candidate_rows(txid: &Txid, shard_id: u64, bucket: u32, rows: u64) -> [u64; 2] {
    std::array::from_fn(|choice| {
        let digest = Sha256::new()
            .chain_update(DISPLAY_SCHEMA)
            .chain_update(b"/directory/")
            .chain_update([choice as u8])
            .chain_update(shard_id.to_le_bytes())
            .chain_update(bucket.to_le_bytes())
            .chain_update(txid.0)
            .finalize();
        u64::from_le_bytes(digest[..8].try_into().unwrap()) % rows
    })
}

/// One private table of a display shard.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DisplayTable {
    /// The directory of one bucket.
    Directory(u32),
    /// The shard's overflow pages.
    Pages,
}

impl DisplayTable {
    /// Wire and binding name: `directory-{b}` or `pages`.
    pub fn label(&self) -> String {
        match self {
            Self::Directory(bucket) => format!("directory-{bucket}"),
            Self::Pages => "pages".to_string(),
        }
    }

    /// Parses a canonical label; leading zeros and out-of-range buckets are
    /// refused so one table never has two names.
    pub fn parse(label: &str) -> Option<Self> {
        if label == "pages" {
            return Some(Self::Pages);
        }
        let digits = label.strip_prefix("directory-")?;
        if digits.is_empty()
            || !digits.bytes().all(|b| b.is_ascii_digit())
            || (digits.len() > 1 && digits.starts_with('0'))
        {
            return None;
        }
        let bucket: u32 = digits.parse().ok()?;
        (bucket < MAX_BUCKETS).then_some(Self::Directory(bucket))
    }

    /// Segment file name inside a revision directory.
    pub fn file_name(&self, segment: usize) -> String {
        format!("{}.{segment}.bin", self.label())
    }

    pub fn is_directory(&self) -> bool {
        matches!(self, Self::Directory(_))
    }
}

impl DisplayTable {
    /// The native table kind this table is served as.
    pub fn kind(&self) -> DisplayKind {
        match self {
            Self::Directory(_) => DisplayKind::TxDirectory,
            Self::Pages => DisplayKind::TxPages,
        }
    }
}

/// The history table kind whose native parameters, row count and row width
/// a display table uses. Seeds are derived from the history schema and this
/// kind's name, so the parameters are exactly the history display tables'.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DisplayKind {
    TxDirectory,
    TxPages,
}

impl DisplayKind {
    /// The history table name, which the native seeds are derived from.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TxDirectory => "txdirectory",
            Self::TxPages => "txpages",
        }
    }

    /// Rows in one segment of this table, at `geometry`.
    pub fn rows(self, geometry: &Geometry) -> u64 {
        match self {
            Self::TxDirectory => geometry.directory_rows,
            Self::TxPages => geometry.page_rows,
        }
    }

    /// Bytes in one row of this table, at any geometry.
    pub fn row_bytes(self, _geometry: &Geometry) -> u32 {
        txid::ROW_BYTES as u32
    }
}

/// The published seed a native table's public query setup is derived from,
/// for the table named `table` (history or display kind) of `geometry_name`.
///
/// Distinct per geometry *and* per table. The two tables of one geometry have
/// different row counts, so their public parameters already differ; two
/// geometries have different row counts again. Deriving the seed from both
/// names means a client that mixed any of them up fails its own re-derivation
/// check rather than decoding a row against the wrong table's setup — which
/// would not error, it would return plausible nonsense.
///
/// Domain-separated by the history schema string, so a future schema reusing
/// these names does not reuse their seeds.
pub fn setup_seed_named(geometry_name: &str, table: &str) -> u64 {
    let digest = Sha256::new()
        .chain_update(crate::manifest::SCHEMA.as_bytes())
        .chain_update(b"/setup-seed\0")
        .chain_update(geometry_name.as_bytes())
        .chain_update(b"\0")
        .chain_update(table.as_bytes())
        .finalize();
    u64::from_le_bytes(digest[..8].try_into().expect("eight bytes"))
}

/// The setup seed of a display table kind at `geometry`, as `/v1/txid/init`
/// publishes it.
pub fn setup_seed(geometry: &Geometry, kind: DisplayKind) -> u64 {
    setup_seed_named(geometry.name, kind.as_str())
}

/// Binds a query to one revision and one table, buckets included, so a body
/// prepared for one bucket is refused by another bucket's runtime.
pub fn query_binding(manifest_digest: &str, table: DisplayTable) -> [u8; 8] {
    crate::manifest::query_binding_for_schema(DISPLAY_SCHEMA, manifest_digest, &table.label())
}

/// Decodes every directory entry of one directory row.
pub fn row_entries(row: &[u8]) -> Result<Vec<DirectoryEntry>, txid::Error> {
    txid::entries(row)?
        .into_iter()
        .map(DirectoryEntry::decode)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_round_trip_and_are_canonical() {
        for table in [
            DisplayTable::Pages,
            DisplayTable::Directory(0),
            DisplayTable::Directory(7),
            DisplayTable::Directory(MAX_BUCKETS - 1),
        ] {
            assert_eq!(DisplayTable::parse(&table.label()), Some(table));
        }
        for bad in [
            "directory-",
            "directory-01",
            "directory-+1",
            "directory-64",
            "directory--1",
            "txdirectory",
            "Pages",
        ] {
            assert_eq!(DisplayTable::parse(bad), None, "{bad}");
        }
        assert_eq!(DisplayTable::Directory(3).file_name(1), "directory-3.1.bin");
        assert_eq!(DisplayTable::Pages.file_name(0), "pages.0.bin");
    }

    /// Seeds are published by every running server; they must never move.
    #[test]
    fn setup_seeds_are_golden() {
        for (geometry, kind, seed) in [
            (&TXID_2K, DisplayKind::TxDirectory, 0x57a7_3ced_5e98_120a),
            (&TXID_2K, DisplayKind::TxPages, 0x6231_4f48_472b_6e15),
            (
                &geometry::TXID_4K,
                DisplayKind::TxDirectory,
                0x28bc_e699_56cc_2d7e,
            ),
            (
                &geometry::TXID_4K,
                DisplayKind::TxPages,
                0x95df_53a3_e525_1353,
            ),
        ] {
            assert_eq!(
                setup_seed(geometry, kind),
                seed,
                "{} {kind:?}",
                geometry.name
            );
        }
        assert_eq!(DisplayTable::Directory(5).kind(), DisplayKind::TxDirectory);
        assert_eq!(DisplayTable::Pages.kind(), DisplayKind::TxPages);
    }

    #[test]
    fn binding_separates_buckets_and_revisions() {
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let t0 = DisplayTable::Directory(0);
        let t1 = DisplayTable::Directory(1);
        assert_ne!(query_binding(&a, t0), query_binding(&a, t1));
        assert_ne!(query_binding(&a, t0), query_binding(&b, t0));
        assert_ne!(
            query_binding(&a, t0),
            query_binding(&a, DisplayTable::Pages)
        );
        // Distinct from the history binding of the same digest and kind.
        assert_ne!(
            query_binding(&a, DisplayTable::Pages),
            crate::manifest::query_binding(&a, "pages")
        );
    }

    #[test]
    fn buckets_are_balanced_and_shard_independent() {
        let n = 4;
        let mut counts = [0u32; 4];
        for i in 0u32..40_000 {
            let txid = Txid(Sha256::digest(i.to_le_bytes()).into());
            counts[bucket(&txid, n) as usize] += 1;
        }
        // Binomial(40,000, 1/4): sigma is about 87; five sigma either way.
        for count in counts {
            assert!((10_000 - 435..=10_000 + 435).contains(&count), "{counts:?}");
        }
        let txid = Txid([9; 32]);
        assert_eq!(bucket(&txid, 1), 0);
        let rows = candidate_rows(&txid, 3, bucket(&txid, 4), 2048);
        assert!(rows.iter().all(|r| *r < 2048));
        assert_ne!(rows, candidate_rows(&txid, 4, bucket(&txid, 4), 2048));
    }
}
