//! Enhance PIR coordinator entrypoint.
use clap::{Parser, Subcommand};
use enhance_publisher::{control::PlacementPolicy, coordinator::Coordinator, zakura::ZakuraClient};
use std::{net::SocketAddr, path::PathBuf};

#[derive(Parser)]
struct Cli {
    /// Deployment policy; seven sealed shards require independent qualification.
    #[arg(long, global = true, default_value_t = 6)]
    sealed_shards: usize,
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
    Coordinator {
        /// Independently scheduled live Status controller; omitted disables Status.
        #[arg(long)]
        status_config: Option<PathBuf>,
        /// Private packing-router inventory; omitted retains legacy query serving.
        #[arg(long)]
        packing_router_config: Option<PathBuf>,
        #[arg(long, requires = "packing_router_config")]
        query_ingress: Vec<String>,
        #[arg(long, requires = "packing_router_config")]
        pool_placement: bool,
        #[arg(long, default_value_t = 2, requires = "pool_placement")]
        frontier_replicas: usize,
        /// Per-domain required counts and optional accelerator mirror rules.
        #[arg(long, requires = "pool_placement")]
        pool_policy: Option<PathBuf>,
        /// Immutable packing artifacts; never expose this listener publicly.
        #[arg(long, requires = "packing_router_config")]
        artifact_listen: Option<SocketAddr>,
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: SocketAddr,
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        worker_config: PathBuf,
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        zakura_rpc_url: String,
        #[arg(long)]
        zakura_cookie: Option<PathBuf>,
        /// Dedicated synthetic fixture environment only. Never use on serving production data.
        #[arg(long)]
        isolated_fixture: bool,
        #[arg(long, default_value_t = 67)]
        fixture_records: u64,
        #[arg(long, default_value_t = 1)]
        fixture_append_records: u64,
        #[arg(long, default_value_t = 10)]
        poll_seconds: u64,
        /// Transparent outputs retained across blocks to price Ironwood transactions.
        #[arg(long, default_value_t = enhance_publisher::zakura::DEFAULT_CACHE_OUTPUTS)]
        prevout_cache_outputs: usize,
        #[arg(long, default_value_t = 1.0)]
        capacity_fallback_rows_per_second: f64,
        #[arg(long, default_value_t = 21600.0)]
        capacity_readiness_seconds: f64,
        #[arg(long, default_value_t = 4096)]
        capacity_burst_rows: u64,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    let placement_policy = PlacementPolicy {
        sealed_shards: cli.sealed_shards,
    };
    placement_policy.validate()?;
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
        Command::Coordinator {
            status_config,
            packing_router_config,
            query_ingress,
            pool_placement,
            frontier_replicas,
            pool_policy,
            artifact_listen,
            listen,
            data_dir,
            worker_config,
            zakura_rpc_url,
            zakura_cookie,
            isolated_fixture,
            fixture_records,
            fixture_append_records,
            poll_seconds,
            prevout_cache_outputs,
            capacity_fallback_rows_per_second,
            capacity_readiness_seconds,
            capacity_burst_rows,
        } => {
            enhance_publisher::service::run(
                enhance_publisher::service::Config {
                    status_config,
                    packing_router_config,
                    query_ingress,
                    pool_placement,
                    frontier_replicas,
                    pool_policy,
                    artifact_listen,
                    listen,
                    data_dir,
                    worker_config,
                    zakura_rpc_url,
                    zakura_cookie,
                    isolated_fixture,
                    fixture_records,
                    fixture_append_records,
                    poll_seconds,
                    prevout_cache_outputs,
                    capacity_fallback_rows_per_second,
                    capacity_readiness_seconds,
                    capacity_burst_rows,
                    sealed_shards: placement_policy.sealed_shards,
                },
                pir_pipeline::service::Http::new(|addr, app| async move {
                    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await
                }),
                pir_pipeline::service::Shutdown::default(),
            )
            .await?;
        }
    }
    Ok(())
}
