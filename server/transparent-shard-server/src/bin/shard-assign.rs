//! Plans which worker holds which shards, and renders the router that sends
//! requests there.
//!
//! `plan` reads a published set and a roster of workers, gives every recent
//! shard to every recent replica and splits the archive into contiguous
//! ranges balanced by the bytes their runtimes reserve, and refuses a roster
//! that cannot hold its share without eviction. `check` confirms an
//! assignment describes a set. `files` lists what one worker needs copied.
//! `caddyfile` renders the router's configuration from an assignment alone.
//!
//! Everything an assignment routes on is public: shard ids, revision digests
//! and table names in request paths. Nothing here sees a script, a row or a
//! page locator.

use clap::{Parser, Subcommand};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use transparent_filter::ShardMap;
use transparent_shard_server::assignment::{Assignment, GeneratedBy};
use transparent_shard_server::router::{files_for, plan, render_caddyfile, RosterEntry};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "shard-assign",
    about = "Plan and check fleet assignments for a shard set"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Plan an assignment over a roster and render its router.
    Plan {
        #[arg(long)]
        shard_dir: PathBuf,
        /// JSON array of roster entries: id, role, replica_group, ssh_host,
        /// upstream, cache_bytes.
        #[arg(long)]
        roster: PathBuf,
        /// First shard id of the recent tier. Must be a shard boundary at the
        /// recorded cutoff height, which `--recent-from-height` resolves.
        #[arg(long, conflicts_with = "recent_from_height")]
        recent_from_shard: Option<u64>,
        /// The recorded cutoff height; the shard beginning there starts the
        /// recent tier.
        #[arg(long)]
        recent_from_height: Option<u64>,
        /// Fraction of each cache kept free of assigned runtimes.
        #[arg(long, default_value_t = 0.15)]
        headroom: f64,
        #[arg(long)]
        out_assignment: PathBuf,
        #[arg(long)]
        out_caddyfile: Option<PathBuf>,
        #[arg(long)]
        public_host: Option<String>,
        #[arg(long)]
        source_sha: Option<String>,
    },
    /// Confirm an assignment describes a set.
    Check {
        #[arg(long)]
        shard_dir: PathBuf,
        #[arg(long)]
        assignment: PathBuf,
    },
    /// List the files one worker needs from the set, relative to its root.
    Files {
        #[arg(long)]
        shard_dir: PathBuf,
        #[arg(long)]
        assignment: PathBuf,
        #[arg(long)]
        worker_id: String,
    },
    /// Render the router's Caddyfile from an assignment.
    Caddyfile {
        #[arg(long)]
        assignment: PathBuf,
        #[arg(long)]
        public_host: String,
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

/// The map as the service serializes and digests it.
fn read_map(shard_dir: &std::path::Path) -> Result<(ShardMap, String), BoxError> {
    let raw = std::fs::read(shard_dir.join("shards.json"))?;
    let map: ShardMap = serde_json::from_slice(&raw)?;
    map.check_shape()?;
    let served = serde_json::to_vec(&map)?;
    Ok((map, hex::encode(Sha256::digest(&served))))
}

fn main() -> Result<(), BoxError> {
    match Cli::parse().command {
        Command::Plan {
            shard_dir,
            roster,
            recent_from_shard,
            recent_from_height,
            headroom,
            out_assignment,
            out_caddyfile,
            public_host,
            source_sha,
        } => {
            let (map, map_sha256) = read_map(&shard_dir)?;
            let roster: Vec<RosterEntry> = serde_json::from_slice(&std::fs::read(&roster)?)?;
            let recent_from_shard = match (recent_from_shard, recent_from_height) {
                (Some(shard), None) => shard,
                (None, Some(height)) => map
                    .shards
                    .iter()
                    .find(|entry| entry.start_height == height)
                    .map(|entry| entry.shard_id)
                    .ok_or_else(|| {
                        format!("no shard begins at height {height}; the cutoff is not a shard boundary")
                    })?,
                _ => return Err("give --recent-from-shard or --recent-from-height".into()),
            };
            let assignment = plan(
                &map,
                &map_sha256,
                &roster,
                recent_from_shard,
                headroom,
                GeneratedBy {
                    tool: "shard-assign".into(),
                    source_sha,
                    generated_at: chrono_free_now(),
                },
            )?;
            std::fs::write(&out_assignment, serde_json::to_vec_pretty(&assignment)?)?;
            eprintln!(
                "assignment {} written to {}",
                assignment.digest(),
                out_assignment.display()
            );
            for worker in &assignment.workers {
                eprintln!(
                    "  {:<14} {:<15} {:>4} shards {:>13} bytes reserved of {:>13} cache  {}",
                    worker.id,
                    worker.role.as_str(),
                    worker.shards.len(),
                    worker.estimated_resident_bytes,
                    worker.cache_bytes,
                    match (worker.shards.first(), worker.shards.last()) {
                        (Some(a), Some(b)) => format!("{a}-{b}"),
                        _ => "none".into(),
                    }
                );
            }
            if let Some(path) = out_caddyfile {
                let host = public_host.ok_or("--out-caddyfile needs --public-host")?;
                std::fs::write(&path, render_caddyfile(&assignment, &host))?;
                eprintln!("router config written to {}", path.display());
            }
        }
        Command::Check {
            shard_dir,
            assignment,
        } => {
            let (map, map_sha256) = read_map(&shard_dir)?;
            let assignment = Assignment::load(&assignment)?;
            assignment.check_against(&map, &map_sha256)?;
            println!(
                "{} describes the set at {} ({} shards, map {map_sha256})",
                assignment.digest(),
                shard_dir.display(),
                map.shards.len()
            );
        }
        Command::Files {
            shard_dir,
            assignment,
            worker_id,
        } => {
            let (map, map_sha256) = read_map(&shard_dir)?;
            let assignment = Assignment::load(&assignment)?;
            assignment.check_against(&map, &map_sha256)?;
            for file in files_for(&shard_dir, &map, &assignment, &worker_id)? {
                println!("{file}");
            }
        }
        Command::Caddyfile {
            assignment,
            public_host,
            out,
        } => {
            let assignment = Assignment::load(&assignment)?;
            let rendered = render_caddyfile(&assignment, &public_host);
            match out {
                Some(path) => std::fs::write(path, rendered)?,
                None => print!("{rendered}"),
            }
        }
    }
    Ok(())
}

/// UTC now as RFC 3339, without a date dependency this crate does not need.
fn chrono_free_now() -> String {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Civil-from-days, Howard Hinnant's algorithm.
    let days = (seconds / 86_400) as i64;
    let rem = seconds % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}
