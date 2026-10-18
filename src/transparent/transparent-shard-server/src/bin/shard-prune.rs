//! Removes superseded revisions a set no longer needs to keep.
//!
//! A republished tail leaves its predecessor on disk, and the worker keeps a
//! bounded number of those so a wallet mid-sync can still be answered from the
//! revision it fetched setup for. Past that bound they are dead weight, and a
//! set that is never pruned grows by a tail revision per republication.
//!
//! What is never touched: a revision the map names, a revision named on the
//! command line with `--keep` (the next tail a rollout is about to advertise),
//! and anything under an assignment that belongs to a shard this worker does
//! not hold. Without `--apply` nothing is removed and the report says what
//! would be.

use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use transparent_shard_server::assignment::Assignment;
use transparent_shard_server::shardset::{
    LoadOptions, LoadScope, ShardSet, DEFAULT_RETAIN_REVISIONS,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "shard-prune",
    about = "Remove superseded shard revisions past the retention bound"
)]
struct Cli {
    #[arg(long)]
    shard_dir: PathBuf,
    #[arg(long, default_value_t = DEFAULT_RETAIN_REVISIONS)]
    retain_revisions: usize,
    #[arg(long)]
    retain_bytes: Option<u64>,
    #[arg(long, requires = "worker_id")]
    assignment: Option<PathBuf>,
    #[arg(long, requires = "assignment")]
    worker_id: Option<String>,
    /// Digests never to remove, whatever the bound says.
    #[arg(long = "keep", value_delimiter = ',')]
    keep: Vec<String>,
    /// Remove rather than report.
    #[arg(long)]
    apply: bool,
    #[arg(long)]
    out: Option<PathBuf>,
}

fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let scope = match (&cli.assignment, &cli.worker_id) {
        (Some(path), Some(worker_id)) => LoadScope::Assigned {
            assignment: Arc::new(Assignment::load(path)?),
            worker_id: worker_id.clone(),
        },
        _ => LoadScope::Whole,
    };
    let set = ShardSet::open_with(
        &cli.shard_dir,
        &LoadOptions {
            retain_revisions: cli.retain_revisions,
            retain_bytes: cli.retain_bytes,
            prune_excess: true,
            scope,
        },
    )?;
    let named: std::collections::BTreeSet<&str> = set
        .map
        .shards
        .iter()
        .map(|entry| entry.manifest_digest.as_str())
        .collect();
    let mut deleted = Vec::new();
    let mut kept = Vec::new();
    for prunable in set.prunable() {
        // Belt and braces: the loader never lists a map-named revision as
        // prunable, and a `--keep` is the operator's word.
        if named.contains(prunable.digest.as_str()) || cli.keep.contains(&prunable.digest) {
            kept.push(prunable);
            continue;
        }
        if cli.apply {
            std::fs::remove_dir_all(&prunable.path)?;
        }
        deleted.push(prunable);
    }
    let record = serde_json::json!({
        "schema": "transparent-prune-v1",
        "shard_dir": cli.shard_dir,
        "applied": cli.apply,
        "retain_revisions": cli.retain_revisions,
        "retain_bytes": cli.retain_bytes,
        "worker_id": cli.worker_id,
        "deleted": deleted.iter().map(|p| serde_json::json!({
            "digest": p.digest, "shard_id": p.shard_id, "revision": p.revision, "bytes": p.bytes,
        })).collect::<Vec<_>>(),
        "kept": kept.iter().map(|p| serde_json::json!({
            "digest": p.digest, "shard_id": p.shard_id, "revision": p.revision, "bytes": p.bytes,
        })).collect::<Vec<_>>(),
        "bytes_freed": deleted.iter().map(|p| p.bytes).sum::<u64>(),
    });
    let bytes = serde_json::to_vec_pretty(&record)?;
    match &cli.out {
        Some(path) => std::fs::write(path, &bytes)?,
        None => println!("{}", String::from_utf8(bytes)?),
    }
    eprintln!(
        "{} revision(s) {}, {} kept by name, {} bytes",
        deleted.len(),
        if cli.apply {
            "removed"
        } else {
            "would be removed"
        },
        kept.len(),
        deleted.iter().map(|p| p.bytes).sum::<u64>()
    );
    Ok(())
}
