//! Preparing a history publication's runtimes on its publisher.
//!
//! A worker otherwise builds every directory and page segment it is assigned
//! whenever a publication changes it: the recent tail's open directory shard
//! at every block, and each page shard once it holds rows. The publisher
//! prepares each once instead, as a plain file of the publication directory
//! beside its revision directories (see [`crate::runtime::prepare`]): never
//! inside one, whose manifest closes it, and never as a subdirectory, which
//! a worker would read as a revision. The map, manifests, setup and journal
//! are unchanged; a worker that is offered no files builds as before.
//!
//! The targets are discovered from the publication as a worker reads it: a
//! verified [`ShardSet`], every revision it holds, every table with rows,
//! each runtime once by its key. A page shard's runtime is keyed by its own
//! identity, which a sealed page shard keeps across every revision of its
//! directory shard, and a sealed directory shard's by its unchanged digest;
//! their files carry over from the retained publication that holds them. The
//! open directory shard's digest changes at every block, so its files, and
//! those of page shards whose rows changed, are built.

use crate::runtime::disk::ShippedRuntimes;
use crate::runtime::prepare::{prepare_segment, Prepared, SegmentError};
use crate::runtime::SharedParams;
use crate::shardset::{ShardSet, Table};
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// What one publication's preparation produced.
#[derive(Clone, Debug, Default, Serialize)]
pub struct HistoryPrepared {
    /// One record per runtime file the publication now holds.
    pub files: Vec<Prepared>,
    /// Runtimes that could not be prepared; workers build these.
    pub failures: Vec<String>,
    pub seconds: f64,
}

impl HistoryPrepared {
    pub fn built(&self) -> usize {
        self.files.iter().filter(|file| !file.linked).count()
    }

    pub fn linked(&self) -> usize {
        self.files.iter().filter(|file| file.linked).count()
    }

    pub fn built_bytes(&self) -> u64 {
        self.files
            .iter()
            .filter(|file| !file.linked)
            .map(|file| file.bytes)
            .sum()
    }

    pub fn linked_bytes(&self) -> u64 {
        self.files
            .iter()
            .filter(|file| file.linked)
            .map(|file| file.bytes)
            .sum()
    }

    pub fn build_seconds(&self) -> f64 {
        self.files.iter().map(|file| file.build_seconds).sum()
    }

    /// The summary a cycle record and the fleet request carry.
    pub fn report(&self) -> serde_json::Value {
        serde_json::json!({
            "seconds": self.seconds,
            "runtimes": self.files.len(),
            "built": self.built(),
            "linked": self.linked(),
            "built_bytes": self.built_bytes(),
            "linked_bytes": self.linked_bytes(),
            "build_seconds": self.build_seconds(),
            "failures": self.failures,
        })
    }
}

/// Why a publication's preparation stopped.
#[derive(Debug)]
pub enum PrepareError {
    /// A segment's bytes did not verify: the publication must not be
    /// published.
    Source(String),
    /// The publication was invalidated while it was being prepared.
    Superseded,
}

impl std::fmt::Display for PrepareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrepareError::Source(error) => write!(f, "{error}"),
            PrepareError::Superseded => {
                write!(
                    f,
                    "the publication was invalidated while its runtimes were prepared"
                )
            }
        }
    }
}

impl std::error::Error for PrepareError {}

/// Prepares every runtime of `set`, the verified publication in `out`, into
/// `out`: linked from the first of `retained` publications holding a
/// completed file of the same identity, built otherwise.
///
/// `superseded` is asked before each segment; once it answers true the
/// preparation stops, so an invalidated publication never finishes, and is
/// never offered. A build or write failure is recorded and skipped: the
/// worker builds that runtime itself. A segment whose bytes fail
/// verification fails the whole preparation.
pub fn prepare_publication(
    set: &ShardSet,
    out: &Path,
    retained: &[PathBuf],
    superseded: &dyn Fn() -> bool,
) -> Result<HistoryPrepared, PrepareError> {
    let started = Instant::now();
    let shipped = ShippedRuntimes::new(out.to_path_buf());
    let retained: Vec<ShippedRuntimes> = retained
        .iter()
        .filter(|dir| dir.as_path() != out)
        .map(|dir| ShippedRuntimes::new(dir.clone()))
        .collect();
    let mut params: HashMap<(&'static str, Table), SharedParams> = HashMap::new();
    let mut prepared = HistoryPrepared::default();
    // `warm_targets` is every revision held, each runtime once by its key:
    // what any worker serving this set can be asked to produce.
    for (digest, table, segment) in set.warm_targets() {
        if superseded() {
            return Err(PrepareError::Superseded);
        }
        let shard = set
            .revision(&digest)
            .expect("a warm target names a held revision");
        let source = shard
            .segment(table, segment)
            .expect("a warm target names a held segment");
        let kind = table.kind();
        let shared = match params.entry((shard.geometry.name, kind)) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                match SharedParams::build(shard.geometry, kind) {
                    Ok(shared) => entry.insert(shared),
                    Err(error) => {
                        prepared.failures.push(format!(
                            "{} parameters of {}: {error}",
                            kind.as_str(),
                            shard.geometry.name
                        ));
                        continue;
                    }
                }
            }
        };
        let key = (shard.runtime_identity(table).to_string(), kind, segment);
        let label = format!("shard {} {}", shard.manifest.shard_id, table.name());
        match prepare_segment(&shipped, &retained, &key, shared, source, label.clone()) {
            Ok(file) => prepared.files.push(file),
            Err(SegmentError::Source(error)) => return Err(PrepareError::Source(error)),
            Err(SegmentError::Runtime(error)) => {
                tracing::warn!(%label, segment, %error, "runtime preparation failed; workers build it");
                prepared
                    .failures
                    .push(format!("{label} segment {segment}: {error}"));
            }
        }
    }
    if superseded() {
        return Err(PrepareError::Superseded);
    }
    prepared.seconds = started.elapsed().as_secs_f64();
    Ok(prepared)
}
