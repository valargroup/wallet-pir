//! Loading a generation published by the research harness.
//!
//! The harness (`tools/transparent_pir_incremental.py`, `build_generation`)
//! writes an immutable directory containing `manifest.json`, `directory.bin`,
//! `pages.bin` and `filters.bin`, named by the digest of its manifest. This
//! server serves the two retrieval tables from such a directory; it does not
//! build them. Keeping construction in the harness means the measurement runs
//! against exactly the generations the existing evidence was produced from.
//!
//! `filters.bin` is deliberately ignored here. Filters are public and are
//! served by `transparent-filter-server`; a wallet must not learn to fetch them
//! from the same place it makes private requests, or the two become correlated
//! by construction.

use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum GenerationError {
    #[error("reading {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("manifest is not valid JSON: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("{0}")]
    Invalid(String),
}

/// Geometry of one table, as the manifest records it.
#[derive(Clone, Debug, Deserialize)]
pub struct TableGeometry {
    pub rows: u64,
    pub row_bytes: u32,
    pub sha256: String,
}

/// Schema identifiers the harness writes. They are opaque strings, and this
/// server refuses any value it was not written against rather than guessing at
/// a layout: a generation whose entry or page encoding changed would decode to
/// plausible nonsense instead of failing.
pub const SCHEMA: &str = "transparent-incremental-research-v1";
pub const GENERATION_SCHEMA: &str = "transparent-incremental-generation-v1";

#[derive(Clone, Debug, Deserialize)]
pub struct Manifest {
    pub schema: String,
    pub generation_schema: String,
    pub network: String,
    pub start: u64,
    pub end: u64,
    pub anchor: String,
    pub directory: TableGeometry,
    pub pages: TableGeometry,
}

/// One table's rows, verified against the manifest.
pub struct LoadedTable {
    pub geometry: TableGeometry,
    pub rows: Vec<u8>,
}

pub struct LoadedGeneration {
    /// Digest naming the generation directory, which is the manifest digest.
    pub generation_id: String,
    /// First eight bytes of that digest, as the wire prefix.
    pub generation: u64,
    pub manifest: Manifest,
    pub directory: LoadedTable,
    pub pages: LoadedTable,
}

fn read(path: &Path) -> Result<Vec<u8>, GenerationError> {
    std::fs::read(path).map_err(|source| GenerationError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn load_table(
    dir: &Path,
    name: &str,
    geometry: &TableGeometry,
) -> Result<LoadedTable, GenerationError> {
    let rows = read(&dir.join(format!("{name}.bin")))?;
    let expected = (geometry.rows as usize)
        .checked_mul(geometry.row_bytes as usize)
        .ok_or_else(|| GenerationError::Invalid(format!("{name} geometry overflows")))?;
    if rows.len() != expected {
        return Err(GenerationError::Invalid(format!(
            "{name} is {} bytes, manifest says {expected}",
            rows.len()
        )));
    }
    // The manifest digest is what a client is told to expect, so a table that
    // does not match it must not be served: the client would reject every
    // decoded row anyway, and serving it would look like a PIR fault rather
    // than a corrupt generation.
    if hex::encode(Sha256::digest(&rows)) != geometry.sha256 {
        return Err(GenerationError::Invalid(format!(
            "{name} does not match its manifest digest"
        )));
    }
    Ok(LoadedTable {
        geometry: geometry.clone(),
        rows,
    })
}

impl LoadedGeneration {
    pub fn open(dir: &Path) -> Result<Self, GenerationError> {
        let raw = read(&dir.join("manifest.json"))?;
        let manifest: Manifest = serde_json::from_slice(&raw)?;

        // The harness names the directory by the manifest digest. Recomputing
        // it here means a manifest edited after publication is refused rather
        // than served under the identity of the generation it replaced.
        let generation_id = hex::encode(Sha256::digest(&raw));
        let name = dir
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| GenerationError::Invalid("generation path has no name".to_string()))?;
        if name != generation_id {
            return Err(GenerationError::Invalid(format!(
                "generation directory is named {name} but its manifest digests to {generation_id}"
            )));
        }
        if manifest.schema != SCHEMA || manifest.generation_schema != GENERATION_SCHEMA {
            return Err(GenerationError::Invalid(format!(
                "generation declares schema {}/{}, this server serves {SCHEMA}/{GENERATION_SCHEMA}",
                manifest.schema, manifest.generation_schema
            )));
        }
        if manifest.network != transparent_history_pir::types::NETWORK {
            return Err(GenerationError::Invalid(format!(
                "generation is for network {}",
                manifest.network
            )));
        }
        if manifest.end < manifest.start {
            return Err(GenerationError::Invalid(
                "generation ends before it starts".to_string(),
            ));
        }

        let directory = load_table(dir, "directory", &manifest.directory)?;
        let pages = load_table(dir, "pages", &manifest.pages)?;
        let mut prefix = [0u8; 8];
        prefix.copy_from_slice(
            &hex::decode(&generation_id)
                .map_err(|error| GenerationError::Invalid(error.to_string()))?[..8],
        );
        Ok(Self {
            generation_id,
            generation: u64::from_le_bytes(prefix),
            manifest,
            directory,
            pages,
        })
    }
}
