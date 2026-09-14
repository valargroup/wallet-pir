//! The public half of a published shard set: the map, and one range filter per
//! shard.
//!
//! A wallet syncs filter-first. It takes the map, downloads the range filter of
//! each shard the map names, tests its own scripts against those filters
//! locally, and only then makes a private query — for the shards that matched.
//! The filters are what make the private queries rare, so without them the
//! retrieval service is reachable but unusable: a wallet would have to query
//! every shard to find the ones holding its history, which is the cost the
//! design exists to avoid.
//!
//! These bytes are served here rather than by the shard retrieval service, and
//! that separation is deliberate. `transparent-shard-server` loads every
//! `filter.bin` only to check it against its manifest digest and refuses to
//! serve it: a wallet that fetched public bytes from the same origin it makes
//! private requests to would correlate the two by construction. So the private
//! service holds them and will not hand them out, and this service, which never
//! sees a private request, does.
//!
//! Everything here is immutable once published, so the set is read once at
//! startup and held. A shard's filter is a few hundred kilobytes and the whole
//! published set's filters are well under a megabyte, so holding them costs
//! little and removes the filesystem from the request path.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use transparent_filter::digest::filter_hash;
use transparent_filter::wire::ShardMap;

#[derive(Debug, thiserror::Error)]
pub enum ShardFilterError {
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

fn read(path: &Path) -> Result<Vec<u8>, ShardFilterError> {
    std::fs::read(path).map_err(|source| ShardFilterError::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// A published set's map and filters, verified against the map.
pub struct ShardFilters {
    map: ShardMap,
    /// Filter bytes by shard id. The map is gapless and ascending, but this is
    /// keyed rather than indexed so a lookup cannot silently answer with the
    /// wrong shard if those invariants ever weaken.
    filters: BTreeMap<u64, Vec<u8>>,
    /// Serialized once: the map is immutable and every wallet asks for it.
    map_json: Vec<u8>,
}

impl ShardFilters {
    /// Loads the set rooted at `dir`.
    ///
    /// Verification is against the operator's own files rather than a hostile
    /// network. A filter that does not match the digest the map publishes would
    /// be served as if it were the published filter, and every wallet would
    /// silently under-match — skipping history it should have retrieved, which
    /// is a wrong answer rather than an error. Failing at load turns that into
    /// a service that does not start.
    pub fn open(dir: &Path) -> Result<Self, ShardFilterError> {
        let map_path = dir.join("shards.json");
        let raw = read(&map_path)?;
        let map: ShardMap =
            serde_json::from_slice(&raw).map_err(|source| ShardFilterError::Json {
                path: map_path,
                source,
            })?;
        map.check_shape().map_err(|error| {
            ShardFilterError::Invalid(format!("shard map is malformed: {error}"))
        })?;

        let mut filters = BTreeMap::new();
        for entry in &map.shards {
            // The publisher names each shard's directory by its manifest
            // digest, which the map carries, so the map alone locates them.
            let path = dir.join(&entry.manifest_digest).join("filter.bin");
            let bytes = read(&path)?;
            if filter_hash(&bytes).to_display_hex() != entry.filter_hash {
                return Err(ShardFilterError::Invalid(format!(
                    "filter of shard {} does not match the digest its map entry publishes",
                    entry.shard_id
                )));
            }
            if filters.insert(entry.shard_id, bytes).is_some() {
                return Err(ShardFilterError::Invalid(format!(
                    "the map names shard {} twice",
                    entry.shard_id
                )));
            }
        }

        let map_json = serde_json::to_vec(&map).map_err(|source| ShardFilterError::Json {
            path: dir.join("shards.json"),
            source,
        })?;
        Ok(Self {
            map,
            filters,
            map_json,
        })
    }

    pub fn map(&self) -> &ShardMap {
        &self.map
    }

    /// The serialized map, as served.
    pub fn map_json(&self) -> &[u8] {
        &self.map_json
    }

    pub fn filter(&self, shard_id: u64) -> Option<&[u8]> {
        self.filters.get(&shard_id).map(Vec::as_slice)
    }

    pub fn shard_count(&self) -> usize {
        self.map.shards.len()
    }

    /// The last height the set covers, for the info surface.
    pub fn covered_through(&self) -> Option<u64> {
        self.map.shards.last().map(|s| s.end_height)
    }

    /// SHA-256 of the serialized map, so a wallet can pin the revision it
    /// synced against and notice when the set is republished.
    pub fn map_digest(&self) -> String {
        hex::encode(Sha256::digest(&self.map_json))
    }
}
