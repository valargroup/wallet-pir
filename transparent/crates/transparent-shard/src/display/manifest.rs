//! What a display shard publishes about itself, and the display map.
//!
//! Identity works as for history shards: a revision is named by the digest of
//! its canonical manifest. Sealed display manifests are content-pure (revision
//! 0, nothing superseded), so the same range rebuilt from the same journal
//! has the same digest however and whenever it was sealed. Only the recent
//! shard has a revision lineage.

use super::seal::DisplaySealParams;
use super::tables::BuiltDisplay;
use super::{display_by_name, DisplayTable, BUCKET_DOMAIN, DISPLAY_SCHEMA, MAX_BUCKETS};
use crate::layout::Geometry;
use crate::manifest::TableGeometry;
use crate::txid::{CODEC, FRAGMENT_BYTES, INLINE_BYTES, ROW_BYTES};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// The fixed encoding parameters, restated so a consumer refuses a build that
/// changed one instead of misreading rows.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DisplayLayout {
    pub codec: String,
    pub inline_bytes: u32,
    pub row_bytes: u32,
    pub fragment_bytes: u32,
    pub directory_choices: u32,
    pub bucket_domain: String,
}

impl DisplayLayout {
    pub fn current() -> Self {
        Self {
            codec: CODEC.to_string(),
            inline_bytes: INLINE_BYTES as u32,
            row_bytes: ROW_BYTES as u32,
            fragment_bytes: FRAGMENT_BYTES as u32,
            directory_choices: 2,
            bucket_domain: String::from_utf8(BUCKET_DOMAIN.to_vec()).unwrap(),
        }
    }
}

/// One bucket's directory table and what it holds.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DisplayBucket {
    pub bucket: u32,
    pub records: u64,
    pub inline_records: u64,
    pub directory_segments: Vec<TableGeometry>,
    /// Records by page count, 0 for inline: the size of each page-count
    /// class a lookup in this bucket can fall into.
    pub page_histogram: BTreeMap<u32, u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DisplayManifest {
    pub schema: String,
    pub network: String,
    pub genesis_hash: String,
    pub shard_id: u64,
    pub start_height: u64,
    pub end_height: u64,
    /// The block before `start_height`, in display hex.
    pub parent_block_hash: String,
    /// The block at `end_height`, in display hex.
    pub terminal_block_hash: String,
    /// The previous *sealed* display shard, or empty for the first one.
    pub parent_manifest_digest: String,
    pub sealed: bool,
    pub revision: u32,
    pub supersedes: String,
    pub geometry: String,
    pub n_buckets: u32,
    /// Per-bucket record target the seal rule applied; it decides where a
    /// sealed shard ends, so it is part of the shard's identity.
    pub archive_target: u64,
    pub layout: DisplayLayout,
    pub blocks: u64,
    pub records: u64,
    pub payload_bytes: u64,
    pub page_rows_used: u64,
    pub buckets: Vec<DisplayBucket>,
    pub page_segments: Vec<TableGeometry>,
}

/// Identity and placement inputs of a manifest; the tables supply the rest.
#[derive(Clone, Debug)]
pub struct ManifestHeader {
    pub network: String,
    pub genesis_hash: String,
    pub shard_id: u64,
    pub start_height: u64,
    pub end_height: u64,
    pub parent_block_hash: String,
    pub terminal_block_hash: String,
    pub parent_manifest_digest: String,
    pub sealed: bool,
    pub revision: u32,
    pub supersedes: String,
    pub archive_target: u64,
}

fn digest_tables(segments: &[Vec<u8>], rows: u64) -> Vec<TableGeometry> {
    segments
        .iter()
        .map(|segment| TableGeometry {
            rows,
            row_bytes: ROW_BYTES as u32,
            sha256: hex::encode(Sha256::digest(segment)),
        })
        .collect()
}

impl DisplayManifest {
    pub fn new(header: ManifestHeader, geometry: &Geometry, built: &BuiltDisplay) -> Self {
        Self {
            schema: DISPLAY_SCHEMA.to_string(),
            network: header.network,
            genesis_hash: header.genesis_hash,
            shard_id: header.shard_id,
            start_height: header.start_height,
            end_height: header.end_height,
            parent_block_hash: header.parent_block_hash,
            terminal_block_hash: header.terminal_block_hash,
            parent_manifest_digest: header.parent_manifest_digest,
            sealed: header.sealed,
            revision: header.revision,
            supersedes: header.supersedes,
            geometry: geometry.name.to_string(),
            n_buckets: built.buckets.len() as u32,
            archive_target: header.archive_target,
            layout: DisplayLayout::current(),
            blocks: header.end_height - header.start_height + 1,
            records: built.records,
            payload_bytes: built.payload_bytes,
            page_rows_used: built.page_rows_used,
            buckets: built
                .buckets
                .iter()
                .enumerate()
                .map(|(b, bucket)| DisplayBucket {
                    bucket: b as u32,
                    records: bucket.records,
                    inline_records: bucket.inline_records,
                    directory_segments: digest_tables(&bucket.directory, geometry.directory_rows),
                    page_histogram: bucket.page_histogram.clone(),
                })
                .collect(),
            page_segments: digest_tables(&built.pages, geometry.page_rows),
        }
    }

    /// Field order is declaration order, which serde preserves.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a display manifest serializes")
    }

    pub fn digest(&self) -> String {
        hex::encode(Sha256::digest(self.canonical_bytes()))
    }

    pub fn min_bucket_records(&self) -> u64 {
        self.buckets.iter().map(|b| b.records).min().unwrap_or(0)
    }

    /// Segment geometries of `table`, if the shard has it.
    pub fn segments(&self, table: DisplayTable) -> Option<&[TableGeometry]> {
        match table {
            DisplayTable::Directory(b) => self
                .buckets
                .get(b as usize)
                .map(|b| b.directory_segments.as_slice()),
            DisplayTable::Pages => Some(&self.page_segments),
        }
    }

    /// Every table, directories first.
    pub fn tables(&self) -> Vec<DisplayTable> {
        (0..self.n_buckets)
            .map(DisplayTable::Directory)
            .chain([DisplayTable::Pages])
            .collect()
    }

    pub fn display_geometry(&self) -> Option<&'static Geometry> {
        display_by_name(&self.geometry)
    }

    /// Internal consistency, checked before any table is opened.
    pub fn validate(&self) -> Result<&'static Geometry, String> {
        let geometry = self
            .display_geometry()
            .ok_or_else(|| format!("unknown display geometry {:?}", self.geometry))?;
        if self.schema != DISPLAY_SCHEMA {
            return Err(format!("unsupported display schema {:?}", self.schema));
        }
        if self.layout != DisplayLayout::current() {
            return Err("display layout differs from this build".into());
        }
        if self.n_buckets == 0 || self.n_buckets > MAX_BUCKETS {
            return Err(format!("{} buckets", self.n_buckets));
        }
        if self.end_height < self.start_height
            || self.blocks != self.end_height - self.start_height + 1
        {
            return Err("display shard range".into());
        }
        for hash in [
            &self.genesis_hash,
            &self.parent_block_hash,
            &self.terminal_block_hash,
        ] {
            if hash.len() != 64 {
                return Err("display shard block hash".into());
            }
        }
        if !self.parent_manifest_digest.is_empty() && self.parent_manifest_digest.len() != 64 {
            return Err("parent manifest digest".into());
        }
        if self.sealed && (self.revision != 0 || !self.supersedes.is_empty()) {
            return Err("a sealed display shard is content-pure: revision 0".into());
        }
        if self.buckets.len() != self.n_buckets as usize {
            return Err("bucket list length".into());
        }
        let mut total = 0;
        for (index, bucket) in self.buckets.iter().enumerate() {
            if bucket.bucket != index as u32 {
                return Err(format!("bucket {index} is labelled {}", bucket.bucket));
            }
            check_segments(&bucket.directory_segments, geometry.directory_rows)?;
            let classes: u64 = bucket.page_histogram.values().sum();
            if classes != bucket.records
                || bucket.page_histogram.get(&0).copied().unwrap_or(0) != bucket.inline_records
            {
                return Err(format!("bucket {index} histogram"));
            }
            total += bucket.records;
        }
        if total != self.records {
            return Err("record total".into());
        }
        check_segments(&self.page_segments, geometry.page_rows)?;
        Ok(geometry)
    }
}

fn check_segments(segments: &[TableGeometry], rows: u64) -> Result<(), String> {
    if segments.is_empty() {
        return Err("a display table has no segment".into());
    }
    if segments
        .iter()
        .any(|s| s.rows != rows || s.row_bytes != ROW_BYTES as u32 || s.sha256.len() != 64)
    {
        return Err("display segment geometry".into());
    }
    Ok(())
}

/// One shard as the map lists it.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DisplayMapEntry {
    pub shard_id: u64,
    pub start_height: u64,
    pub end_height: u64,
    pub parent_block_hash: String,
    pub terminal_block_hash: String,
    pub geometry: String,
    pub n_buckets: u32,
    /// Segments per bucket directory, in bucket order.
    pub directory_segments: Vec<u32>,
    pub page_segments: u32,
    pub records: u64,
    pub min_bucket_records: u64,
    pub manifest_digest: String,
    pub revision: u32,
    pub sealed: bool,
}

impl DisplayMapEntry {
    pub fn from_manifest(manifest: &DisplayManifest, digest: &str) -> Self {
        Self {
            shard_id: manifest.shard_id,
            start_height: manifest.start_height,
            end_height: manifest.end_height,
            parent_block_hash: manifest.parent_block_hash.clone(),
            terminal_block_hash: manifest.terminal_block_hash.clone(),
            geometry: manifest.geometry.clone(),
            n_buckets: manifest.n_buckets,
            directory_segments: manifest
                .buckets
                .iter()
                .map(|b| b.directory_segments.len() as u32)
                .collect(),
            page_segments: manifest.page_segments.len() as u32,
            records: manifest.records,
            min_bucket_records: manifest.min_bucket_records(),
            manifest_digest: digest.to_string(),
            revision: manifest.revision,
            sealed: manifest.sealed,
        }
    }

    /// Whether this entry describes exactly `manifest` under `digest`.
    pub fn describes(&self, manifest: &DisplayManifest) -> bool {
        manifest.digest() == self.manifest_digest
            && *self == Self::from_manifest(manifest, &self.manifest_digest)
    }

    /// The tier a lookup in this shard names; the sealed flag, nothing more.
    pub fn tier(&self) -> &'static str {
        if self.sealed {
            "archive"
        } else {
            "recent"
        }
    }
}

/// `GET /v1/txid/shards`: the shards currently served, oldest first. Clients
/// read the [split map](super::split) instead; this full listing is kept for
/// clients built before it.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DisplayMap {
    pub schema: String,
    pub network: String,
    pub genesis_hash: String,
    pub seal: DisplaySealParams,
    /// First height served; advances when the oldest archive leaves the window.
    pub start_height: u64,
    /// Id of the first listed shard. Ids are absolute and never renumbered.
    pub first_shard_id: u64,
    pub shards: Vec<DisplayMapEntry>,
}

impl DisplayMap {
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec_pretty(self).expect("a display map serializes")
    }

    pub fn sha256(&self) -> String {
        hex::encode(Sha256::digest(self.to_bytes()))
    }

    pub fn shard_for_height(&self, height: u64) -> Option<&DisplayMapEntry> {
        find_by_height(&self.shards, height)
    }

    pub fn shard(&self, shard_id: u64) -> Option<&DisplayMapEntry> {
        let index = shard_id.checked_sub(self.first_shard_id)?;
        self.shards.get(usize::try_from(index).ok()?)
    }

    pub fn covered_through(&self) -> Option<u64> {
        self.shards.last().map(|s| s.end_height)
    }

    /// Checks the map is well formed before anything is fetched.
    pub fn check_shape(&self) -> Result<(), String> {
        if self.schema != DISPLAY_SCHEMA {
            return Err(format!("unsupported display map schema {:?}", self.schema));
        }
        let Some(first) = self.shards.first() else {
            return Err("display map is empty".into());
        };
        if first.start_height != self.start_height || first.shard_id != self.first_shard_id {
            return Err("display map start".into());
        }
        for (index, shard) in self.shards.iter().enumerate() {
            if shard.shard_id != self.first_shard_id + index as u64 {
                return Err(format!(
                    "display shard at position {index} claims id {}",
                    shard.shard_id
                ));
            }
            check_entry(&self.seal, shard)?;
            if !shard.sealed && index + 1 != self.shards.len() {
                return Err(format!(
                    "unsealed display shard {} is not last",
                    shard.shard_id
                ));
            }
            if let Some(previous) = index.checked_sub(1).map(|i| &self.shards[i]) {
                check_chain(previous, shard)?;
            }
        }
        Ok(())
    }
}

/// Checks one listed shard on its own: range, geometry, tables, digest, and
/// for a sealed shard the bucket target and content purity.
pub(crate) fn check_entry(seal: &DisplaySealParams, shard: &DisplayMapEntry) -> Result<(), String> {
    if shard.end_height < shard.start_height {
        return Err(format!(
            "display shard {} ends before it starts",
            shard.shard_id
        ));
    }
    if display_by_name(&shard.geometry).is_none() {
        return Err(format!("display shard {} geometry", shard.shard_id));
    }
    let expected_buckets = if shard.sealed {
        seal.n_archive
    } else {
        seal.n_recent
    };
    if shard.n_buckets != expected_buckets
        || shard.directory_segments.len() != shard.n_buckets as usize
        || shard.directory_segments.contains(&0)
        || shard.page_segments == 0
    {
        return Err(format!("display shard {} tables", shard.shard_id));
    }
    if shard.manifest_digest.len() != 64 {
        return Err(format!("display shard {} digest", shard.shard_id));
    }
    if shard.sealed && (shard.revision != 0 || shard.min_bucket_records < seal.archive_target) {
        return Err(format!(
            "sealed display shard {} is below its bucket target or not content-pure",
            shard.shard_id
        ));
    }
    Ok(())
}

/// Checks `shard` starts right after `previous`, at the next height and on
/// its terminal block.
pub(crate) fn check_chain(
    previous: &DisplayMapEntry,
    shard: &DisplayMapEntry,
) -> Result<(), String> {
    if shard.start_height != previous.end_height + 1
        || shard.parent_block_hash != previous.terminal_block_hash
    {
        return Err(format!(
            "display shard {} does not chain to {}",
            shard.shard_id, previous.shard_id
        ));
    }
    Ok(())
}

/// The listed shard covering `height`, in a contiguous list ordered by height.
pub(crate) fn find_by_height(shards: &[DisplayMapEntry], height: u64) -> Option<&DisplayMapEntry> {
    let index = shards
        .binary_search_by(|shard| {
            if shard.end_height < height {
                std::cmp::Ordering::Less
            } else if shard.start_height > height {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .ok()?;
    shards.get(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::display::{build_shard, TXID_2K};
    use crate::txid::{DisplayOutput, TransparentDisplayRecord};
    use transparent_events::{FeeState, TransactionMetadata, Txid};

    fn built() -> BuiltDisplay {
        let records: Vec<_> = (0u8..20)
            .map(|i| TransparentDisplayRecord {
                txid: Txid([i; 32]),
                coinbase: false,
                metadata: TransactionMetadata {
                    fee: FeeState::Exact(1),
                    transparent_input_count: 1,
                    has_shielded_components: false,
                },
                outputs: vec![DisplayOutput {
                    value: 1,
                    script: vec![0; if i == 3 { 300 } else { 25 }],
                }],
            })
            .collect();
        build_shard(5, &TXID_2K, 2, &records).unwrap()
    }

    fn header(sealed: bool) -> ManifestHeader {
        ManifestHeader {
            network: "mainnet".into(),
            genesis_hash: "00".repeat(32),
            shard_id: 5,
            start_height: 100,
            end_height: 199,
            parent_block_hash: "11".repeat(32),
            terminal_block_hash: "22".repeat(32),
            parent_manifest_digest: "33".repeat(32),
            sealed,
            revision: 0,
            supersedes: String::new(),
            archive_target: 10,
        }
    }

    fn manifest() -> DisplayManifest {
        DisplayManifest::new(header(true), &TXID_2K, &built())
    }

    #[test]
    fn manifest_validates_and_every_field_reaches_the_digest() {
        let base = manifest();
        base.validate().unwrap();
        let digest = base.digest();
        type Mutation = Box<dyn Fn(&mut DisplayManifest)>;
        let mutations: Vec<Mutation> = vec![
            Box::new(|m| m.schema.push('x')),
            Box::new(|m| m.network.push('x')),
            Box::new(|m| m.genesis_hash = "99".repeat(32)),
            Box::new(|m| m.shard_id += 1),
            Box::new(|m| m.start_height += 1),
            Box::new(|m| m.end_height += 1),
            Box::new(|m| m.parent_block_hash = "99".repeat(32)),
            Box::new(|m| m.terminal_block_hash = "99".repeat(32)),
            Box::new(|m| m.parent_manifest_digest = "99".repeat(32)),
            Box::new(|m| m.sealed = false),
            Box::new(|m| m.revision += 1),
            Box::new(|m| m.supersedes = "99".repeat(32)),
            Box::new(|m| m.geometry = "txid-4k".into()),
            Box::new(|m| m.n_buckets += 1),
            Box::new(|m| m.archive_target += 1),
            Box::new(|m| m.layout.inline_bytes += 1),
            Box::new(|m| m.blocks += 1),
            Box::new(|m| m.records += 1),
            Box::new(|m| m.payload_bytes += 1),
            Box::new(|m| m.page_rows_used += 1),
            Box::new(|m| m.buckets[0].records += 1),
            Box::new(|m| m.buckets[1].directory_segments[0].sha256 = "99".repeat(32)),
            Box::new(|m| {
                let extra = m.buckets[0].directory_segments[0].clone();
                m.buckets[0].directory_segments.push(extra);
            }),
            Box::new(|m| *m.buckets[0].page_histogram.entry(9).or_default() += 1),
            Box::new(|m| m.page_segments[0].sha256 = "99".repeat(32)),
        ];
        for mutate in mutations {
            let mut changed = base.clone();
            mutate(&mut changed);
            assert_ne!(changed.digest(), digest);
        }
    }

    #[test]
    fn manifest_validation_refuses_inconsistent_content() {
        let mut m = manifest();
        m.revision = 1;
        assert!(m.validate().is_err());
        let mut m = manifest();
        m.buckets[0].records += 1;
        assert!(m.validate().is_err());
        let mut m = manifest();
        m.geometry = "archive-wide".into();
        assert!(m.validate().is_err());
        let mut m = manifest();
        m.buckets[1].directory_segments[0].rows = 4096;
        assert!(m.validate().is_err());
        let mut m = DisplayManifest::new(header(false), &TXID_2K, &built());
        m.revision = 3;
        m.supersedes = "44".repeat(32);
        m.validate().unwrap();
    }

    fn map() -> DisplayMap {
        let seal = DisplaySealParams {
            n_archive: 2,
            n_recent: 2,
            archive_target: 5,
            recent_floor: 5,
            reorg_margin: 100,
        };
        let mut shards = Vec::new();
        let mut parent = "11".repeat(32);
        for (i, sealed) in [true, true, false].into_iter().enumerate() {
            let mut h = header(sealed);
            h.shard_id = 7 + i as u64;
            h.start_height = 100 + i as u64 * 100;
            h.end_height = h.start_height + 99;
            h.parent_block_hash = parent.clone();
            h.terminal_block_hash = format!("{:064x}", i + 1);
            parent = h.terminal_block_hash.clone();
            let m = DisplayManifest::new(h, &TXID_2K, &built());
            shards.push(DisplayMapEntry::from_manifest(&m, &m.digest()));
        }
        DisplayMap {
            schema: DISPLAY_SCHEMA.into(),
            network: "mainnet".into(),
            genesis_hash: "00".repeat(32),
            seal,
            start_height: 100,
            first_shard_id: 7,
            shards,
        }
    }

    #[test]
    fn map_shape_and_lookup() {
        let m = map();
        m.check_shape().unwrap();
        assert_eq!(m.shard_for_height(250).unwrap().shard_id, 8);
        assert_eq!(m.shard(9).unwrap().tier(), "recent");
        assert!(m.shard(6).is_none() && m.shard_for_height(99).is_none());
        assert_eq!(m.covered_through(), Some(399));

        type Mutation = Box<dyn Fn(&mut DisplayMap)>;
        let broken: Vec<Mutation> = vec![
            Box::new(|m| m.first_shard_id = 0),
            Box::new(|m| m.shards[1].shard_id = 20),
            Box::new(|m| m.shards[1].start_height += 1),
            Box::new(|m| m.shards[1].parent_block_hash = "99".repeat(32)),
            Box::new(|m| m.shards[0].sealed = false),
            Box::new(|m| m.shards[0].min_bucket_records = 4),
            Box::new(|m| m.shards[0].revision = 1),
            Box::new(|m| m.shards[2].n_buckets = 1),
            Box::new(|m| m.shards[0].directory_segments[1] = 0),
            Box::new(|m| m.shards[0].geometry = "recent-4k".into()),
            Box::new(|m| m.shards.clear()),
        ];
        for mutate in broken {
            let mut changed = map();
            mutate(&mut changed);
            assert!(changed.check_shape().is_err());
        }
        // A window that dropped its oldest shard is still well formed.
        let mut dropped = map();
        dropped.shards.remove(0);
        dropped.first_shard_id = 8;
        dropped.start_height = 200;
        dropped.check_shape().unwrap();
    }
}
