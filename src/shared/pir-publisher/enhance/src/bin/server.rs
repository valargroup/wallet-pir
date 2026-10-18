//! Enhance PIR coordinator and worker entrypoint.
use clap::{Parser, Subcommand};
use enhance_publisher::{control::PlacementPolicy, coordinator::Coordinator, zakura::ZakuraClient};
use enhance_query_server::worker::Worker;
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
    /// Offline migration: coordinator-owned artifacts for all retained sessions.
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
    /// Offline repair of journal-referenced rows from a peer row archive.
    RepairRows {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        source_rows: PathBuf,
    },
    Worker {
        #[command(flatten)]
        matvec: enhance_query_server::matvec::MatvecConfig,
        #[arg(long, default_value = "127.0.0.1:8091")]
        listen: SocketAddr,
        #[arg(long)]
        data_dir: PathBuf,
    },
    QueryIngress {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long, default_value = "127.0.0.1:8082")]
        listen: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8083")]
        control_listen: SocketAddr,
        #[arg(long, default_value_t = 16)]
        requests: usize,
        /// Bodies buffered concurrently before router admission; 4 per client.
        #[arg(long, default_value_t = enhance_query_server::query_ingress::DEFAULT_UPLOADS)]
        uploads: usize,
    },
    /// Decode and pack wallet queries on a dedicated private host.
    PackingRouter {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        artifact_origin: String,
        #[arg(long, default_value = "127.0.0.1:8092")]
        listen: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8093")]
        control_listen: SocketAddr,
        #[arg(long, default_value_t = 6)]
        max_objects: usize,
        #[arg(long, default_value_t = 4)]
        requests: usize,
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
        Command::RepairRows {
            data_dir,
            source_rows,
        } => {
            let restored = Worker::repair_rows(&data_dir, &source_rows)?;
            println!(
                "{}",
                serde_json::json!({"status":"rows_verified", "restored_units":restored,
                "readiness":"requires_worker_restart", "qualification":"unqualified"})
            );
        }
        Command::Worker {
            listen,
            data_dir,
            matvec,
        } => {
            let worker = Worker::open_with_backend(&data_dir, placement_policy, matvec)?;
            // A stopped replica drains accepted evaluations; new ones are
            // refused and retried by the packing router on another replica.
            enhance_query_server::shutdown::serve_until(
                tokio::net::TcpListener::bind(listen).await?,
                worker.router(),
                enhance_query_server::shutdown::signal(),
            )
            .await?;
        }
        Command::QueryIngress {
            data_dir,
            listen,
            control_listen,
            requests,
            uploads,
        } => {
            let ingress = enhance_query_server::query_ingress::QueryIngress::open_with_uploads(
                &data_dir, requests, uploads,
            )?;
            tokio::try_join!(
                async {
                    axum::serve(
                        tokio::net::TcpListener::bind(listen).await?,
                        ingress
                            .public_router()
                            .into_make_service_with_connect_info::<SocketAddr>(),
                    )
                    .await
                },
                async {
                    axum::serve(
                        tokio::net::TcpListener::bind(control_listen).await?,
                        ingress.control_router(),
                    )
                    .await
                },
            )?;
        }
        Command::PackingRouter {
            data_dir,
            artifact_origin,
            listen,
            control_listen,
            max_objects,
            requests,
        } => {
            let router = enhance_query_server::packing_router::PackingRouter::open(
                &data_dir,
                &artifact_origin,
                max_objects,
                requests,
            )?;
            router.start_worker_health_monitor();
            tokio::try_join!(
                async {
                    axum::serve(
                        tokio::net::TcpListener::bind(listen).await?,
                        router.public_router(),
                    )
                    .await
                },
                async {
                    axum::serve(
                        tokio::net::TcpListener::bind(control_listen).await?,
                        router.control_router(),
                    )
                    .await
                },
            )?;
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
