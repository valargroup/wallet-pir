//! Loading a published shard set from disk.
//!
//! The publisher writes one directory per shard, named by the digest of that
//! shard's manifest, plus a `shards.json` holding the map. This reads them back
//! and refuses anything that does not verify.
//!
//! A shard's tables are stored one file per segment. Ordinarily there is one of
//! each; a shard whose content did not fit a single segment of the pinned
//! geometry has more, and every segment has the same geometry, which is what
//! keeps one parameter set serving the whole fleet.
//!
//! Verification here is deliberately paranoid about *the operator's own files*,
//! not only about a hostile network. A shard whose table no longer matches its
//! manifest digest would be served as if it were the published shard, and every
//! client would reject its rows as a PIR fault rather than as a corrupt shard.
//! Failing at load turns a silent, confusing failure into a loud one.
//!
//! `filter.bin` is loaded, verified against the map, and — since the transparent
//! host was asked to serve a complete API — also served from here.
//!
//! That is a deliberate relaxation of an earlier rule, which kept public bytes
//! and private requests on separate origins so the two could not be correlated
//! by construction. What the rule actually bought was narrower than it looked:
//! the shard id of a private query is public in its own URL, and one operator
//! runs both services, so the correlation was available from logs regardless.
//! What it did preserve was a wallet's ability to reach the two over different
//! network paths, and that option is kept — the filter service still serves the
//! same bytes at the same paths on its own host, so a wallet that wants two
//! origins still has them.

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use transparent_filter::{filter_hash, ShardMap};
use transparent_shard::layout::{DIRECTORY_ROWS, DIRECTORY_ROW_BYTES, PAGE_ROWS, PAGE_ROW_BYTES};
use transparent_shard::manifest::{ShardManifest, SCHEMA};

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("reading {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{path} is not valid JSON: {source}")]
    Json {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("{0}")]
    Invalid(String),
}

fn read(path: &Path) -> Result<Vec<u8>, LoadError> {
    std::fs::read(path).map_err(|source| LoadError::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// Which of a shard's two private tables a query addresses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Table {
    Directory,
    Pages,
}

impl Table {
    pub fn as_str(self) -> &'static str {
        match self {
            Table::Directory => "directory",
            Table::Pages => "pages",
        }
    }

    /// Distinct published setup seeds per table.
    ///
    /// The two tables have different geometries, so their public parameters
    /// differ and cannot be shared. Separate seeds make a client that mixed
    /// them up fail its own re-derivation check rather than decode a row from
    /// the wrong table.
    pub fn setup_seed(self) -> u64 {
        match self {
            Table::Directory => 0x7368_6172_6464_6972,
            Table::Pages => 0x7368_6172_6470_6167,
        }
    }

    pub fn rows(self) -> u64 {
        match self {
            Table::Directory => DIRECTORY_ROWS as u64,
            Table::Pages => PAGE_ROWS as u64,
        }
    }

    pub fn row_bytes(self) -> u32 {
        match self {
            Table::Directory => DIRECTORY_ROW_BYTES as u32,
            Table::Pages => PAGE_ROW_BYTES as u32,
        }
    }
}

/// One shard's manifest and its table bytes, all verified.
pub struct LoadedShard {
    pub manifest: ShardManifest,
    /// The manifest digest, which is also the directory name.
    pub digest: String,
    /// The shard's public range filter, as published.
    ///
    /// Retained rather than dropped after verification because this service now
    /// serves it. A filter is around a hundred kilobytes, so a worker's whole
    /// assignment costs a few megabytes -- negligible beside the runtimes.
    pub filter: Vec<u8>,
    /// One entry per segment, in segment order.
    pub directory: Vec<Vec<u8>>,
    pub pages: Vec<Vec<u8>>,
}

impl LoadedShard {
    /// One segment of a table, or `None` if this shard has no such segment.
    pub fn table(&self, table: Table, segment: u32) -> Option<&[u8]> {
        let segments = match table {
            Table::Directory => &self.directory,
            Table::Pages => &self.pages,
        };
        segments.get(segment as usize).map(Vec::as_slice)
    }

    /// How many segments this shard's table has. Ordinarily one.
    pub fn segments(&self, table: Table) -> u32 {
        match table {
            Table::Directory => self.directory.len() as u32,
            Table::Pages => self.pages.len() as u32,
        }
    }

    fn open(dir: &Path) -> Result<Self, LoadError> {
        let raw = read(&dir.join("manifest.json"))?;
        let manifest: ShardManifest =
            serde_json::from_slice(&raw).map_err(|source| LoadError::Json {
                path: dir.join("manifest.json"),
                source,
            })?;

        // The publisher names the directory by the manifest digest. Recomputing
        // it here means a manifest edited after publication is refused rather
        // than served under the identity of the shard it replaced.
        let digest = manifest.digest();
        let name = dir
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| LoadError::Invalid("shard path has no name".into()))?;
        if name != digest {
            return Err(LoadError::Invalid(format!(
                "shard directory is named {name} but its manifest digests to {digest}"
            )));
        }
        if manifest.schema != SCHEMA {
            return Err(LoadError::Invalid(format!(
                "shard declares schema {}, this server serves {SCHEMA}",
                manifest.schema
            )));
        }
        // Geometry is pinned and shared across every shard; that sharing is why
        // one parameter set serves the fleet. A shard with its own widths would
        // silently need its own parameters.
        // Every segment must use the pinned geometry, not just the first: a
        // segment with its own widths would silently need its own parameters,
        // and the sharing is the whole point of pinning it.
        for (table, segments) in [
            (Table::Directory, &manifest.directory_segments),
            (Table::Pages, &manifest.page_segments),
        ] {
            if segments.is_empty() {
                return Err(LoadError::Invalid(format!(
                    "shard {} declares no {} segments",
                    manifest.shard_id,
                    table.as_str()
                )));
            }
            if segments
                .iter()
                .any(|s| s.rows != table.rows() || s.row_bytes != table.row_bytes())
            {
                return Err(LoadError::Invalid(format!(
                    "shard {} does not use this build's pinned geometry",
                    manifest.shard_id
                )));
            }
        }

        let mut tables: Vec<Vec<Vec<u8>>> = Vec::new();
        for (table, segments) in [
            (Table::Directory, &manifest.directory_segments),
            (Table::Pages, &manifest.page_segments),
        ] {
            let mut loaded = Vec::with_capacity(segments.len());
            for (index, geometry) in segments.iter().enumerate() {
                let bytes = read(&dir.join(format!("{}.{index}.bin", table.as_str())))?;
                let expected = geometry.rows as usize * geometry.row_bytes as usize;
                if bytes.len() != expected {
                    return Err(LoadError::Invalid(format!(
                        "{} segment {index} of shard {} is {} bytes, the manifest says {expected}",
                        table.as_str(),
                        manifest.shard_id,
                        bytes.len()
                    )));
                }
                if hex::encode(Sha256::digest(&bytes)) != geometry.sha256 {
                    return Err(LoadError::Invalid(format!(
                        "{} segment {index} of shard {} does not match its manifest digest",
                        table.as_str(),
                        manifest.shard_id
                    )));
                }
                loaded.push(bytes);
            }
            tables.push(loaded);
        }
        let mut tables = tables.into_iter();
        let directory = tables.next().expect("directory segments");
        let pages = tables.next().expect("page segments");

        // The filter is not served here, but its digest is part of the shard's
        // identity, so a mismatch means this directory is not the shard the
        // manifest describes.
        let filter = read(&dir.join("filter.bin"))?;
        if filter_hash(&filter).to_display_hex() != manifest.filter_hash {
            return Err(LoadError::Invalid(format!(
                "filter of shard {} does not match its manifest digest",
                manifest.shard_id
            )));
        }

        Ok(Self {
            manifest,
            digest,
            filter,
            directory,
            pages,
        })
    }
}

/// A published shard set: the map and every shard it names.
pub struct ShardSet {
    pub map: ShardMap,
    /// Ascending by shard id, and index `i` is shard `i`.
    pub shards: Vec<LoadedShard>,
}

impl ShardSet {
    /// Loads and verifies the set rooted at `dir`.
    ///
    /// Every shard the map names must be present and verify. A partial set is
    /// refused rather than served with holes: a wallet syncing a range whose
    /// shards were silently missing would advance coverage over history it
    /// never retrieved.
    pub fn open(dir: &Path) -> Result<Self, LoadError> {
        let raw = read(&dir.join("shards.json"))?;
        let map: ShardMap = serde_json::from_slice(&raw).map_err(|source| LoadError::Json {
            path: dir.join("shards.json"),
            source,
        })?;
        map.check_shape()
            .map_err(|error| LoadError::Invalid(format!("shard map is malformed: {error}")))?;

        // Index the directories by the shard id their manifest claims, so the
        // map's order rather than the filesystem's decides the assignment.
        let mut by_id: std::collections::BTreeMap<u64, LoadedShard> = Default::default();
        for entry in std::fs::read_dir(dir).map_err(|source| LoadError::Io {
            path: dir.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|source| LoadError::Io {
                path: dir.to_path_buf(),
                source,
            })?;
            if !entry.path().is_dir() {
                continue;
            }
            let shard = LoadedShard::open(&entry.path())?;
            if let Some(previous) = by_id.insert(shard.manifest.shard_id, shard) {
                return Err(LoadError::Invalid(format!(
                    "two directories both claim shard {}",
                    previous.manifest.shard_id
                )));
            }
        }

        let mut shards = Vec::with_capacity(map.shards.len());
        let mut parent_digest = String::new();
        for entry in &map.shards {
            let shard = by_id.remove(&entry.shard_id).ok_or_else(|| {
                LoadError::Invalid(format!(
                    "the map names shard {} but it is absent",
                    entry.shard_id
                ))
            })?;
            let manifest = &shard.manifest;
            if manifest.start_height != entry.start_height
                || manifest.end_height != entry.end_height
                || manifest.terminal_block_hash != entry.terminal_block_hash
                || manifest.filter_hash != entry.filter_hash
                || manifest.sealed != entry.sealed
                || manifest.revision != entry.revision
                || shard.digest != entry.manifest_digest
                // A wallet reads the segment counts from the map and asks every
                // segment. A map that understated them would leave a segment
                // unqueried, and the wallet would advance coverage over history
                // it never read.
                || manifest.directory_segments.len() as u32 != entry.directory_segments
                || manifest.page_segments.len() as u32 != entry.page_segments
            {
                return Err(LoadError::Invalid(format!(
                    "shard {} disagrees with its map entry",
                    entry.shard_id
                )));
            }
            // The manifest chain is what makes the set a committed sequence
            // rather than a bag of ranges that happen to be adjacent.
            if manifest.parent_manifest_digest != parent_digest {
                return Err(LoadError::Invalid(format!(
                    "shard {} does not chain to its predecessor",
                    entry.shard_id
                )));
            }
            parent_digest = shard.digest.clone();
            shards.push(shard);
        }
        if let Some((id, _)) = by_id.into_iter().next() {
            return Err(LoadError::Invalid(format!(
                "shard {id} is on disk but not in the map"
            )));
        }

        Ok(Self { map, shards })
    }

    pub fn get(&self, shard_id: u64) -> Option<&LoadedShard> {
        self.shards.get(usize::try_from(shard_id).ok()?)
    }
}
