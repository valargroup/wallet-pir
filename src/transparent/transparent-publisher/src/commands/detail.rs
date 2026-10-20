//! Time-tiered txid display publication: bootstrap, run, verify and bench.
use zakura_pir_publisher::transparent as transparent_shard;
use zakura_pir_publisher::transparent::filter as transparent_filter;

use crate::history::publication::BoxError;
use crate::tx_detail::{
    offline,
    publisher::{self, DisplayRoot},
};
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;
use transparent_shard::display::DisplaySealParams;
use transparent_shared::journal::events::EventStore;

#[derive(Parser)]
#[command(
    name = "pir-publisher detail",
    about = "Seal, rebuild and publish tiered txid display shards"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Args, Clone)]
struct SealArgs {
    #[arg(long, default_value = "txid-2k8")]
    geometry: String,
    #[arg(long, default_value_t = 1)]
    n_archive: u32,
    #[arg(long, default_value_t = 1)]
    n_recent: u32,
    /// Real txids every archive bucket must hold before it seals.
    #[arg(long, default_value_t = 90_000)]
    archive_target: u64,
    /// Real txids every recent bucket holds: the recent shard reaches back
    /// over the newest archives until it does.
    #[arg(long, default_value_t = 90_000)]
    recent_window: u64,
    /// Blocks a sealed range must lie below the tip.
    #[arg(long, default_value_t = 100)]
    reorg_margin: u64,
    /// Archives the map lists at most; a seal beyond it drops the oldest.
    #[arg(long, default_value_t = 24)]
    max_archive_shards: u64,
}

impl SealArgs {
    fn params(&self) -> DisplaySealParams {
        DisplaySealParams {
            n_archive: self.n_archive,
            n_recent: self.n_recent,
            archive_target: self.archive_target,
            recent_window: self.recent_window,
            reorg_margin: self.reorg_margin,
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Publish a fresh root from the journal: every seal through `--through`,
    /// the recent shard over its window, candidate 0 and `active.json`. No
    /// serving.
    Bootstrap {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        journal: PathBuf,
        /// First display height; must be above the journal's start.
        #[arg(long)]
        start: u64,
        #[arg(long)]
        through: u64,
        #[command(flatten)]
        seal: SealArgs,
    },
    /// Choose `--start` and `--through` from the journal's counts.
    PlanStart {
        #[arg(long)]
        journal: PathBuf,
        /// Archives the bootstrap should make.
        #[arg(long)]
        archives: u64,
        /// Seals replay should make after it.
        #[arg(long)]
        replay_seals: u64,
        #[command(flatten)]
        seal: SealArgs,
    },
    /// Re-bootstrap at the journal end and check every recorded seal against
    /// it; verify every revision of the active candidate. Exits 1 on a
    /// mismatch.
    Verify {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        journal: PathBuf,
        /// Where to rebuild; defaults to a temporary directory in the root.
        #[arg(long)]
        scratch: Option<PathBuf>,
    },
    /// Write a journal of synthetic blocks with display sidecars, for local
    /// replay and benches only.
    SynthJournal {
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        start: u64,
        #[arg(long)]
        blocks: u64,
        #[arg(long, default_value_t = 8)]
        mean_records: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
    },
    /// Build recent shards of synthetic records and report shape and time.
    BenchRecent {
        #[arg(
            long,
            value_delimiter = ',',
            default_value = "10000,20000,30000,40000,50000,60000,70000"
        )]
        records: Vec<u64>,
        #[arg(long, default_value = "txid-2k8")]
        geometry: String,
        #[arg(long, default_value_os_t = std::env::temp_dir())]
        scratch: PathBuf,
        #[arg(long, default_value_t = 1)]
        seed: u64,
    },
}

fn print(value: &serde_json::Value) {
    println!("{}", serde_json::to_string(value).expect("JSON"));
}

pub async fn run(args: Vec<std::ffi::OsString>) -> Result<(), BoxError> {
    match Cli::parse_from(args).command {
        Command::Bootstrap {
            root,
            journal,
            start,
            through,
            seal,
        } => {
            // Names a missing journal before the lock file would be created.
            drop(EventStore::open_existing(&journal)?);
            // Refuse a concurrent ingest rather than read through a rollback.
            let lock = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(journal.join("writer.lock"))?;
            lock.try_lock_shared()
                .map_err(|e| format!("journal is being written; stop its writer first: {e}"))?;
            let store = EventStore::open_existing(&journal)?;
            let layout = DisplayRoot {
                geometry: seal.geometry.clone(),
                seal: seal.params(),
                max_archive_shards: seal.max_archive_shards,
                network: transparent_filter::NETWORK.to_string(),
                genesis_hash: store.genesis_hash().to_string(),
                start_height: start,
                base_parent: store
                    .block_at(start.checked_sub(1).ok_or("--start must be above 0")?)
                    .ok_or("the journal does not hold the display start's parent")?
                    .block_hash
                    .to_display_hex(),
            };
            let done = tokio::task::spawn_blocking(move || {
                publisher::bootstrap(&store, &root, &layout, through)
            })
            .await??;
            print(&serde_json::json!({
                "seals": done.seals.len(), "map_sha256": done.map_sha256,
                "candidate": done.candidate, "first_shard_id": done.map.first_shard_id,
                "recent": done.recent.summary(),
            }));
        }
        Command::PlanStart {
            journal,
            archives,
            replay_seals,
            seal,
        } => {
            let store = EventStore::open_existing(&journal)?;
            print(&offline::plan_start(
                &store,
                &seal.params(),
                archives,
                replay_seals,
                seal.max_archive_shards,
            )?);
        }
        Command::Verify {
            root,
            journal,
            scratch,
        } => {
            let report = tokio::task::spawn_blocking(move || {
                offline::verify(&root, &journal, scratch.as_deref())
            })
            .await??;
            print(&report);
            if report["ok"] != true {
                std::process::exit(1);
            }
        }
        Command::SynthJournal {
            out,
            start,
            blocks,
            mean_records,
            seed,
        } => {
            let records = offline::synth_journal(&out, start, blocks, mean_records, seed)?;
            print(&serde_json::json!({
                "journal": out, "start": start, "blocks": blocks, "records": records,
                "genesis": offline::SYNTHETIC_GENESIS,
            }));
        }
        Command::BenchRecent {
            records,
            geometry,
            scratch,
            seed,
        } => {
            for line in offline::bench_recent(&records, &geometry, &scratch, seed)? {
                print(&line);
            }
        }
    }
    Ok(())
}
