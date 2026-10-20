//! Enhance publisher diagnostics and offline maintenance.
use clap::{Parser, Subcommand};
use enhance_publisher::{coordinator::Coordinator, zakura::ZakuraClient};
use std::path::PathBuf;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Report compiled capabilities without opening state or loading a GPU driver.
    BuildInfo,
    /// Confirm retained sessions already have router-prepared packing artifacts.
    PreparePackingArtifacts {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Offline bridge for rolling back to coordinator packing without losing recovery history.
    RestoreLegacyPlacement {
        #[arg(long)]
        control_dir: PathBuf,
    },
    /// Offline re-derivation of a coordinator journal's records from chain into
    /// a staged journal that a restarted coordinator adopts. Reads the source
    /// without its lock; only fee metadata may differ.
    RebuildJournal {
        /// Coordinator data directory holding `enhance/`.
        #[arg(long)]
        source: PathBuf,
        /// Staged journal, normally `<data-dir>/enhance-staged`.
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        zakura_rpc_url: String,
        #[arg(long)]
        zakura_cookie: PathBuf,
        /// Last height to rebuild; defaults to the source journal's tip.
        #[arg(long)]
        through_height: Option<u64>,
        /// Non-empty blocks fetched concurrently.
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        #[arg(long, default_value_t = enhance_publisher::zakura::DEFAULT_CACHE_OUTPUTS)]
        cache_outputs: usize,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::BuildInfo => {
            println!(
                "{}",
                serde_json::json!({
                    "native_reinspiring": cfg!(feature = "native-reinspiring"),
                    "source_revision": option_env!("WALLET_PIR_SOURCE_REVISION"),
                })
            );
        }
        Command::PreparePackingArtifacts { data_dir } => Coordinator::backfill_prepared(&data_dir)?,
        Command::RestoreLegacyPlacement { control_dir } => {
            enhance_publisher::control::Store::open(&control_dir)?.restore_legacy_placement()?;
        }
        Command::RebuildJournal {
            source,
            output,
            zakura_rpc_url,
            zakura_cookie,
            through_height,
            concurrency,
            cache_outputs,
        } => {
            let receipt = enhance_publisher::rebuild::rebuild_journal(
                ZakuraClient::from_cookie_file(&zakura_rpc_url, &zakura_cookie)?,
                enhance_publisher::rebuild::RebuildConfig {
                    source,
                    output,
                    through_height,
                    concurrency,
                    cache_outputs,
                },
            )
            .await?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
    }
    Ok(())
}
