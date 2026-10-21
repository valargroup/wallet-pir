//! Building a revision's runtimes ahead of its workers, to ship with it.
//!
//! A recent replica otherwise prepares every table of each new recent
//! revision itself, at every block: encoding, hint and two-mask
//! preprocessing, while it also answers queries. The publisher builds them
//! once instead and writes each in the runtime disk-cache entry format as a
//! plain file of the candidate directory, beside the revision directories:
//! never inside one, whose files must be exactly its manifest's, and never
//! as a subdirectory, which a worker would read as a revision.
//!
//! A worker loads such a file read-only, checks it end to end against the
//! segment it verified itself (`TableRuntime::self_check`), and builds
//! locally when the file is missing, rejected or fails that check.

use crate::runtime::disk::ShippedRuntimes;
use crate::runtime::prepare::{prepare_segment, Prepared};
use crate::runtime::SharedParams;
use crate::tx_detail::kind;
use crate::tx_detail::runtime_key;
use crate::tx_detail::set::DisplayRevision;
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;

/// One runtime file written by [`build_shipped`].
#[derive(Clone, Debug, Serialize)]
pub struct Shipped {
    pub name: String,
    pub table: String,
    pub segment: u32,
    pub bytes: u64,
    /// Encode, hint and preprocessing, without reading or writing.
    pub build_seconds: f64,
    pub write_seconds: f64,
}

impl From<Prepared> for Shipped {
    fn from(prepared: Prepared) -> Self {
        Self {
            name: prepared.name,
            table: prepared.table,
            segment: prepared.segment,
            bytes: prepared.bytes,
            build_seconds: prepared.build_seconds,
            write_seconds: prepared.write_seconds,
        }
    }
}

/// Builds every table segment of the revision in `revision_dir` and writes
/// each runtime into `out_dir`, the candidate directory holding it.
///
/// The revision is read and every table verified first, as a worker loading
/// it would. Builds run on the runtime build pool, one at a time. Returns one
/// record per file.
pub fn build_shipped(revision_dir: &Path, out_dir: &Path) -> Result<Vec<Shipped>, String> {
    build_shipped_reusing(revision_dir, out_dir, &[])
}

/// Reuse verified immutable runtimes across full-coverage publications.
pub fn build_shipped_reusing(
    revision_dir: &Path,
    out_dir: &Path,
    retained: &[std::path::PathBuf],
) -> Result<Vec<Shipped>, String> {
    let revision = DisplayRevision::read(revision_dir)
        .and_then(DisplayRevision::verify_tables)
        .map_err(|error| error.to_string())?;
    build_shipped_verified(&revision, out_dir, retained)
}

/// [`build_shipped_reusing`] for a revision whose tables the caller already
/// verified, such as a held revision of a loaded `DisplaySet`, without
/// reading or verifying them again. A segment that must be built is still
/// rehashed by [`crate::history::shardset::SegmentSource::load`].
/// The error [`build_shipped_verified_until`] returns after a stop.
pub const STOPPED: &str = "display runtime preparation stopped";

pub fn build_shipped_verified(
    revision: &DisplayRevision,
    out_dir: &Path,
    retained: &[std::path::PathBuf],
) -> Result<Vec<Shipped>, String> {
    build_shipped_verified_until(revision, out_dir, retained, crate::history::shardset::NEVER)
}

/// [`build_shipped_verified`], asking `stop` before each runtime segment; a
/// segment already being prepared finishes and is written atomically first.
pub fn build_shipped_verified_until(
    revision: &DisplayRevision,
    out_dir: &Path,
    retained: &[std::path::PathBuf],
    stop: crate::history::shardset::Stop<'_>,
) -> Result<Vec<Shipped>, String> {
    if !revision.held() {
        return Err(format!(
            "display revision {} holds no verified tables",
            revision.digest
        ));
    }
    let shipped = ShippedRuntimes::new(out_dir.to_path_buf());
    let retained: Vec<_> = retained.iter().cloned().map(ShippedRuntimes::new).collect();
    let mut params = HashMap::new();
    let mut written = Vec::new();
    for (table, segment) in revision.targets() {
        if stop() {
            return Err(STOPPED.into());
        }
        let kind = kind(table);
        let shared = match params.entry(kind) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(SharedParams::build_display(revision.geometry, kind)?)
            }
        };
        let source = revision
            .segment(table, segment)
            .ok_or("a verified revision holds every target")?;
        let key = runtime_key(&revision.digest, table, segment);
        // The recent revision is new at every block, so nothing retained
        // can share its identity: every file is built.
        let prepared = prepare_segment(&shipped, &retained, &key, shared, source, table.label())
            .map_err(|error| error.to_string())?;
        written.push(prepared.into());
    }
    Ok(written)
}
