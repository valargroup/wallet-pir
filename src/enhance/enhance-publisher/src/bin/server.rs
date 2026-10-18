//! Enhance PIR coordinator and worker entrypoint.
use clap::{Parser, Subcommand};
use enhance_publisher::{
    application::load_inventory, control::PlacementPolicy, coordinator::Coordinator,
    ingest::EnhanceJournal, zakura::ZakuraClient,
};
use enhance_query_server::worker::Worker;
use std::{net::SocketAddr, path::PathBuf, time::Duration};

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
            let capacity_policy = enhance_publisher::capacity::Policy {
                fallback_rows_per_second: capacity_fallback_rows_per_second,
                readiness_seconds: capacity_readiness_seconds,
                burst_rows: capacity_burst_rows,
            };
            capacity_policy.validate()?;
            if poll_seconds == 0
                || (isolated_fixture && zakura_cookie.is_some())
                || (!isolated_fixture && zakura_cookie.is_none())
            {
                return Err("select either a canonical RPC cookie or an isolated fixture; polling must be nonzero".into());
            }
            let mut groups = load_inventory(&worker_config)?;
            for group in &mut groups {
                group.placement_policy = placement_policy;
            }
            std::fs::create_dir_all(&data_dir)?;
            let mode_path = data_dir.join("source-mode");
            let mode = if isolated_fixture {
                "synthetic-fixture"
            } else {
                "canonical-archive"
            };
            if mode_path.exists() {
                if std::fs::read_to_string(&mode_path)? != mode {
                    return Err("cannot mix fixture and canonical data directories".into());
                }
            } else {
                std::fs::write(mode_path, mode)?;
            }
            // Before control state, the HTTP listener and the journal open, so
            // serving resumes on the adopted journal. Verification streams both
            // journals; chain catch-up stays in the poll loop after serving
            // starts. A rejected rebuild never stops startup.
            match enhance_publisher::rebuild::adopt_staged(&data_dir)? {
                enhance_publisher::rebuild::Adoption::None => {}
                enhance_publisher::rebuild::Adoption::Adopted {
                    receipt_id,
                    height,
                    changed_records,
                    previous,
                } => tracing::info!(
                    %receipt_id, height, changed_records, previous = %previous.display(),
                    "adopted staged Enhance journal rebuild"
                ),
                enhance_publisher::rebuild::Adoption::Rejected { reason, moved_to } => {
                    tracing::error!(
                        %reason, moved_to = %moved_to.display(),
                        "rejected staged Enhance journal rebuild; serving the live journal"
                    )
                }
            }
            let routers = packing_router_config
                .map(
                    |path| -> Result<
                        Vec<enhance_query_server::packing_router::RouterRegistration>,
                        Box<dyn std::error::Error + Send + Sync>,
                    > { Ok(serde_json::from_slice(&std::fs::read(path)?)?) },
                )
                .transpose()?
                .unwrap_or_default();
            if !routers.is_empty() && artifact_listen.is_none() {
                return Err("remote packing requires a private --artifact-listen".into());
            }
            let coordinator = Coordinator::open_with_serving(
                &data_dir.join("control"),
                groups,
                routers,
                query_ingress,
            )?;
            if let Some(listen) = artifact_listen {
                let artifacts = coordinator.artifact_router();
                let listener = tokio::net::TcpListener::bind(listen).await?;
                tokio::spawn(async move {
                    if let Err(error) = axum::serve(listener, artifacts).await {
                        tracing::error!(%error,"artifact listener stopped");
                    }
                });
                let heartbeat = coordinator.clone();
                tokio::spawn(async move {
                    loop {
                        heartbeat.refresh_routers().await;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                });
            }
            let status_publisher = status_config
                .map(
                    |path| -> Result<_, Box<dyn std::error::Error + Send + Sync>> {
                        if isolated_fixture {
                            return Err("live Status cannot use isolated Enhance fixtures".into());
                        }
                        let config: status_publisher::publisher::Config =
                            serde_json::from_slice(&std::fs::read(path)?)?;
                        let enabled = config.public_enabled;
                        let publisher = status_publisher::publisher::Publisher::new(config)?;
                        let cookie = zakura_cookie
                            .as_ref()
                            .ok_or("Status requires canonical node authentication")?;
                        publisher
                            .start(ZakuraClient::from_cookie_file(&zakura_rpc_url, cookie)?)?;
                        Ok((publisher, enabled))
                    },
                )
                .transpose()?;
            let serving = coordinator.clone();
            let mut routes = serving.router();
            if let Some((publisher, true)) = status_publisher {
                routes = routes.merge(publisher.routes());
            }
            let task = tokio::spawn(async move {
                axum::serve(tokio::net::TcpListener::bind(listen).await?, routes).await
            });
            let journal = EnhanceJournal::open(&data_dir)?;
            let rpc = zakura_cookie
                .map(|cookie| ZakuraClient::from_cookie_file(&zakura_rpc_url, &cookie))
                .transpose()?;
            let prevouts = enhance_publisher::zakura::Prevouts::new(prevout_cache_outputs);
            enhance_publisher::application::run(
                enhance_publisher::application::Config {
                    worker_config,
                    placement_policy,
                    pool_placement,
                    pool_policy,
                    frontier_replicas,
                    capacity_policy,
                    poll: Duration::from_secs(poll_seconds),
                },
                coordinator,
                journal,
                match rpc {
                    Some(rpc) => enhance_publisher::application::Source::Canonical {
                        rpc,
                        prevouts: Box::new(prevouts),
                    },
                    None => enhance_publisher::application::Source::Fixture {
                        records: fixture_records,
                        append_records: fixture_append_records,
                    },
                },
                task,
            )
            .await?;
        }
    }
    Ok(())
}
