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

use super::kind;
use super::runtime_key;
use super::set::DisplayRevision;
use crate::runtime::disk::ShippedRuntimes;
use crate::runtime::{build_pool, SharedParams, TableRuntime};
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

/// Builds every table segment of the revision in `revision_dir` and writes
/// each runtime into `out_dir`, the candidate directory holding it.
///
/// The revision is read and every table verified first, as a worker loading
/// it would. Builds run on the runtime build pool, one at a time. Returns one
/// record per file.
pub fn build_shipped(revision_dir: &Path, out_dir: &Path) -> Result<Vec<Shipped>, String> {
    let revision = DisplayRevision::read(revision_dir)
        .and_then(DisplayRevision::verify_tables)
        .map_err(|error| error.to_string())?;
    let shipped = ShippedRuntimes::new(out_dir.to_path_buf());
    let mut params = HashMap::new();
    let mut written = Vec::new();
    for (table, segment) in revision.targets() {
        let kind = kind(table);
        if !params.contains_key(&kind) {
            params.insert(kind, SharedParams::build(revision.geometry, kind)?);
        }
        let shared = &params[&kind];
        let source = revision
            .segment(table, segment)
            .ok_or("a verified revision holds every target")?;
        let rows = source.load().map_err(|error| error.to_string())?;
        let started = std::time::Instant::now();
        let runtime = build_pool().install(|| TableRuntime::build(shared, &rows))?;
        let build_seconds = started.elapsed().as_secs_f64();
        drop(rows);
        let key = runtime_key(&revision.digest, table, segment);
        let started = std::time::Instant::now();
        let bytes = shipped
            .write(&key, shared, &source.sha256, &runtime)
            .map_err(|error| format!("writing the {} runtime: {error}", table.label()))?;
        let name = shipped
            .path(&key, shared, &source.sha256)
            .file_name()
            .expect("an entry path has a name")
            .to_string_lossy()
            .into_owned();
        written.push(Shipped {
            name,
            table: table.label(),
            segment,
            bytes,
            build_seconds,
            write_seconds: started.elapsed().as_secs_f64(),
        });
    }
    Ok(written)
}
