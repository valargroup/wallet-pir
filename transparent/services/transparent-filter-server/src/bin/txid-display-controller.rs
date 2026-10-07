//! Time-tiered txid display publication: bootstrap, run, verify and bench.
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use std::time::Duration;
use transparent_filter_server::events::EventStore;
use transparent_filter_server::publication::BoxError;
use transparent_filter_server::txid_display::{
    controller::{self, Controller, Outcome, Settings},
    publisher::{self, DisplayRoot},
    serving::WorkersFile,
    source::{open_writer, LiveSource, ReplaySource, Source},
};
use transparent_filter_server::zakura::ZakuraClient;
use transparent_shard::display::DisplaySealParams;

#[derive(Parser)]
#[command(
    name = "txid-display-controller",
    about = "Seal, rebuild and publish tiered txid display shards"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Args, Clone)]
struct SealArgs {
    #[arg(long, default_value = "txid-2k")]
    geometry: String,
    #[arg(long, default_value_t = 1)]
    n_archive: u32,
    #[arg(long, default_value_t = 1)]
    n_recent: u32,
    /// Real txids every archive bucket must hold before it seals.
    #[arg(long, default_value_t = 20_000)]
    archive_target: u64,
    /// Real txids every recent bucket keeps after a seal.
    #[arg(long, default_value_t = 10_000)]
    recent_floor: u64,
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
            recent_floor: self.recent_floor,
            reorg_margin: self.reorg_margin,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum Mode {
    Live,
    Replay,
    ReplayThenLive,
}

#[derive(Subcommand)]
enum Command {
    /// Publish a fresh root from the journal: every seal through `--through`,
    /// the recent shard after it, candidate 0 and `active.json`. No serving.
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
    /// Follow a source and publish to the display workers.
    Run {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        journal: PathBuf,
        #[arg(long, value_enum)]
        mode: Mode,
        /// `{"workers": [...]}`; an empty list publishes locally only.
        #[arg(long)]
        workers: PathBuf,
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        rpc_url: String,
        #[arg(long)]
        rpc_cookie: Option<PathBuf>,
        #[arg(long, default_value_t = 500)]
        poll_ms: u64,
        #[arg(long, default_value_t = 25)]
        blocks_per_step: u64,
        #[arg(long, default_value_t = 2_000)]
        step_interval_ms: u64,
        /// Last height replay makes visible; defaults to the journal end.
        #[arg(long)]
        replay_end: Option<u64>,
        /// Skip re-verifying sealed revisions at startup.
        #[arg(long)]
        trust_sealed: bool,
        #[arg(long, default_value = "127.0.0.1:8099")]
        status_listen: std::net::SocketAddr,
        /// Alert when the recent shard holds more records than this.
        #[arg(long)]
        max_recent_records: Option<u64>,
        /// Exit once replay is exhausted and published (bounded runs).
        #[arg(long)]
        exit_when_idle: bool,
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
        #[arg(long, default_value = "txid-2k")]
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

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    match Cli::parse().command {
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
            print(&controller::plan_start(
                &store,
                &seal.params(),
                archives,
                replay_seals,
                seal.max_archive_shards,
            )?);
        }
        Command::Run {
            root,
            journal,
            mode,
            workers,
            rpc_url,
            rpc_cookie,
            poll_ms,
            blocks_per_step,
            step_interval_ms,
            replay_end,
            trust_sealed,
            status_listen,
            max_recent_records,
            exit_when_idle,
        } => {
            let workers = WorkersFile::load(&workers)?.workers;
            let store = open_writer(&journal)?;
            let live = match mode {
                Mode::Replay => None,
                Mode::Live | Mode::ReplayThenLive => {
                    let cookie = rpc_cookie.ok_or("live modes need --rpc-cookie")?;
                    let rpc = ZakuraClient::from_cookie_file(&rpc_url, &cookie)?;
                    if rpc.genesis_hash().await? != store.genesis_hash() {
                        return Err("the node's genesis differs from the journal's".into());
                    }
                    Some(LiveSource::new(rpc, Duration::from_millis(poll_ms)))
                }
            };
            let end = store.covered_through().ok_or("the journal is empty")?;
            let replay = || {
                ReplaySource::new(
                    blocks_per_step,
                    Duration::from_millis(step_interval_ms),
                    replay_end.unwrap_or(end).min(end),
                )
            };
            let source = match (mode, live) {
                (Mode::Live, Some(live)) => Source::Live(live),
                (_, live) => Source::Replay(replay(), live),
            };
            let settings = Settings {
                trust_sealed,
                max_recent_records,
                exit_when_idle,
                ..Settings::default()
            };
            controller::serve_status(status_listen, settings.status.clone()).await?;
            let mut controller = Controller::open(&root, store, source, workers, settings)?;
            match controller.run().await? {
                Outcome::Idle => {}
                Outcome::Halted(reason) => {
                    // Stay up so the status endpoint shows why; a restart
                    // would only find halted.json and stop here again.
                    tracing::error!(alert = "txid_display_halt", %reason, "halted; idling");
                    std::future::pending::<()>().await;
                }
            }
        }
        Command::Verify {
            root,
            journal,
            scratch,
        } => {
            let report = tokio::task::spawn_blocking(move || {
                controller::verify(&root, &journal, scratch.as_deref())
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
            let records = controller::synth_journal(&out, start, blocks, mean_records, seed)?;
            print(&serde_json::json!({
                "journal": out, "start": start, "blocks": blocks, "records": records,
                "genesis": controller::SYNTHETIC_GENESIS,
            }));
        }
        Command::BenchRecent {
            records,
            geometry,
            scratch,
            seed,
        } => {
            for line in controller::bench_recent(&records, &geometry, &scratch, seed)? {
                print(&line);
            }
        }
    }
    Ok(())
}
