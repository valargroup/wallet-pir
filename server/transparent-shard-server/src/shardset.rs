//! Loading a published shard set from disk.
//!
//! The publisher writes one directory per shard, named by the digest of that
//! shard's manifest, plus a `shards.json` holding the map. This reads them back
//! and refuses anything that does not verify.
//!
//! Verification here is deliberately paranoid about *the operator's own files*,
//! not only about a hostile network. A shard whose table no longer matches its
//! manifest digest would be served as if it were the published shard, and every
//! client would reject its rows as a PIR fault rather than as a corrupt shard.
//! Failing at load turns a silent, confusing failure into a loud one.
//!
//! `filter.bin` is loaded but never served from here. Filters are public and
//! belong to the filter service; a wallet must not learn to fetch them from the
//! same place it makes private requests, or the two become correlated by
//! construction. It is read only so its digest can be checked against the map.

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
    pub directory: Vec<u8>,
    pub pages: Vec<u8>,
}

impl LoadedShard {
    pub fn table(&self, table: Table) -> &[u8] {
        match table {
            Table::Directory => &self.directory,
            Table::Pages => &self.pages,
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
        if manifest.directory.rows != Table::Directory.rows()
            || manifest.directory.row_bytes != Table::Directory.row_bytes()
            || manifest.pages.rows != Table::Pages.rows()
            || manifest.pages.row_bytes != Table::Pages.row_bytes()
        {
            return Err(LoadError::Invalid(format!(
                "shard {} does not use this build's pinned geometry",
                manifest.shard_id
            )));
        }

        let directory = read(&dir.join("directory.bin"))?;
        let pages = read(&dir.join("pages.bin"))?;
        for (name, bytes, geometry) in [
            ("directory", &directory, &manifest.directory),
            ("pages", &pages, &manifest.pages),
        ] {
            let expected = geometry.rows as usize * geometry.row_bytes as usize;
            if bytes.len() != expected {
                return Err(LoadError::Invalid(format!(
                    "{name} of shard {} is {} bytes, the manifest says {expected}",
                    manifest.shard_id,
                    bytes.len()
                )));
            }
            if hex::encode(Sha256::digest(bytes)) != geometry.sha256 {
                return Err(LoadError::Invalid(format!(
                    "{name} of shard {} does not match its manifest digest",
                    manifest.shard_id
                )));
            }
        }

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
