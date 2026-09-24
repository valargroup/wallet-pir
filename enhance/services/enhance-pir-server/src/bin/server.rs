//! Enhance PIR coordinator and worker entrypoint.
use clap::{Parser, Subcommand};
use enhance_pir_server::{
    control::{Group, Ledger, PlacementPolicy, Replica},
    coordinator::Coordinator,
    ingest::EnhanceJournal,
    worker::Worker,
    zakura::ZakuraClient,
};
use serde::Deserialize;
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
    /// Offline bridge for rolling back to coordinator packing without losing recovery history.
    RestoreLegacyPlacement {
        #[arg(long)]
        control_dir: PathBuf,
    },
    /// Synthetic workloads for isolated, unregistered workers only. Never grants qualification.
    Exercise {
        #[arg(long, required = true)]
        isolated_workers: bool,
        #[arg(long, value_enum)]
        profile: enhance_pir_server::exercise::Profile,
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        worker_config: PathBuf,
        #[arg(long, default_value = "127.0.0.1:8280")]
        listen: SocketAddr,
        #[arg(long, default_value_t = 21600)]
        seconds: u64,
        #[arg(long, default_value_t = 300)]
        min_publications: u64,
        #[arg(long, default_value_t = 60)]
        publication_interval: u64,
        #[arg(long, default_value_t = 2)]
        concurrency: usize,
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
        matvec: enhance_pir_server::matvec::MatvecConfig,
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
        #[arg(long, default_value_t = 1.0)]
        capacity_fallback_rows_per_second: f64,
        #[arg(long, default_value_t = 21600.0)]
        capacity_readiness_seconds: f64,
        #[arg(long, default_value_t = 4096)]
        capacity_burst_rows: u64,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    #[serde(default)]
    groups: Vec<InventoryGroup>,
    #[serde(default)]
    workers: Vec<InventoryReplica>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryGroup {
    name: String,
    replicas: Vec<InventoryReplica>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryReplica {
    name: String,
    url: String,
}

fn load_inventory(
    path: &std::path::Path,
) -> Result<Vec<Group>, Box<dyn std::error::Error + Send + Sync>> {
    let config: Inventory = serde_json::from_slice(&std::fs::read(path)?)?;
    Ok(config
        .groups
        .into_iter()
        .chain(config.workers.into_iter().map(|r| InventoryGroup {
            name: format!("pool:{}", r.name),
            replicas: vec![r],
        }))
        .enumerate()
        .map(|(sequence, g)| Group {
            placement_policy: Default::default(),
            id: g.name,
            sequence: sequence as u64,
            replicas: g
                .replicas
                .into_iter()
                .map(|r| Replica {
                    name: r.name,
                    url: r.url.trim_end_matches('/').into(),
                    incarnation: String::new(),
                    ledger: Ledger::default(),
                })
                .collect(),
        })
        .collect())
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
        Command::RestoreLegacyPlacement { control_dir } => {
            enhance_pir_server::control::Store::open(&control_dir)?.restore_legacy_placement()?;
        }
        Command::Exercise {
            isolated_workers,
            profile,
            data_dir,
            worker_config,
            listen,
            seconds,
            min_publications,
            publication_interval,
            concurrency,
        } => {
            if !isolated_workers {
                return Err("isolated worker acknowledgement required".into());
            }
            let mut groups = load_inventory(&worker_config)?;
            for group in &mut groups {
                group.placement_policy = placement_policy;
            }
            enhance_pir_server::exercise::run(
                enhance_pir_server::exercise::Config {
                    profile,
                    root: data_dir,
                    listen,
                    seconds,
                    min_publications,
                    interval: Duration::from_secs(publication_interval),
                    concurrency,
                },
                groups,
            )
            .await?;
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
            axum::serve(
                tokio::net::TcpListener::bind(listen).await?,
                worker.router(),
            )
            .await?;
        }
        Command::QueryIngress {
            data_dir,
            listen,
            control_listen,
            requests,
        } => {
            let ingress =
                enhance_pir_server::query_ingress::QueryIngress::open(&data_dir, requests)?;
            tokio::try_join!(
                async {
                    axum::serve(
                        tokio::net::TcpListener::bind(listen).await?,
                        ingress.public_router(),
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
            let router = enhance_pir_server::packing_router::PackingRouter::open(
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
            capacity_fallback_rows_per_second,
            capacity_readiness_seconds,
            capacity_burst_rows,
        } => {
            let capacity_policy = enhance_pir_server::capacity::Policy {
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
            let routers = packing_router_config
                .map(
                    |path| -> Result<
                        Vec<enhance_pir_server::packing_router::RouterRegistration>,
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
            let serving = coordinator.clone();
            let task = tokio::spawn(async move {
                axum::serve(
                    tokio::net::TcpListener::bind(listen).await?,
                    serving.router(),
                )
                .await
            });
            let mut journal = EnhanceJournal::open(&data_dir)?;
            let rpc = zakura_cookie
                .map(|cookie| ZakuraClient::from_cookie_file(&zakura_rpc_url, &cookie))
                .transpose()?;
            loop {
                if let Err(error) = coordinator.reconcile().await {
                    tracing::error!(%error, "publication recovery blocked");
                    tokio::time::sleep(Duration::from_secs(poll_seconds)).await;
                    continue;
                }
                // Capacity registration is independent of journal advancement. A bad
                // or unavailable addition must not stop serving/publishing on the
                // already registered fleet. The infrastructure writer replaces this
                // file atomically only after provisioning its replica pair.
                let inventory_result = match load_inventory(&worker_config) {
                    Ok(mut groups) => {
                        for group in &mut groups {
                            group.placement_policy = placement_policy;
                        }
                        coordinator.reconcile_inventory(groups).await
                    }
                    Err(error) => Err(error.to_string()),
                };
                if let Err(error) = inventory_result {
                    tracing::error!(%error, "inventory reconciliation deferred");
                }
                if pool_placement {
                    let policy = pool_policy
                        .as_ref()
                        .map(|path| {
                            std::fs::read(path)
                                .map_err(|e| e.to_string())
                                .and_then(|bytes| {
                                    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
                                })
                        })
                        .transpose();
                    if let Err(error) = policy.and_then(|p| {
                        coordinator.configure_pool(frontier_replicas, p.unwrap_or_default())
                    }) {
                        tracing::error!(%error,"pool policy waiting for inventory or durable decisions");
                        tokio::time::sleep(Duration::from_secs(poll_seconds)).await;
                        continue;
                    }
                }
                let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = async {
                    let (height, hash) = if let Some(rpc) = &rpc {
                        let tip = rpc.tip_height().await?;
                        let mut ancestor = None;
                        for block in journal.records.blocks().iter().rev() {
                            if block.height <= tip
                                && rpc.block_hash(block.height).await? == block.hash
                            {
                                ancestor =
                                    Some((block.height, block.first_position + block.action_count));
                                break;
                            }
                        }
                        if journal.committed_height() != ancestor.map(|a| a.0) {
                            coordinator
                                .revoke_after(ancestor.map_or(0, |a| a.1))
                                .await?;
                            journal.rewind_to_height(ancestor.map(|a| a.0))?;
                        }
                        let next = journal
                            .committed_height()
                            .map_or(enhance_pir::ACTIVATION_HEIGHT, |h| h + 1);
                        for height in next..=tip {
                            journal.append_block(&rpc.block(height).await?)?;
                        }
                        journal
                            .highest_committed()
                            .ok_or("empty canonical journal")?
                    } else {
                        let target = if journal.records.tree_size() == 0 {
                            fixture_records
                        } else {
                            journal
                                .records
                                .tree_size()
                                .checked_add(fixture_append_records)
                                .ok_or("fixture overflow")?
                        };
                        if target == 0 || target > 23 * 32768 * 33 {
                            return Err("fixture outside fleet coverage".into());
                        }
                        while journal.records.tree_size() < target {
                            let start = journal.records.tree_size();
                            let count = (target - start).min(4096);
                            let records: Vec<_> = (start..start + count)
                                .map(|p| {
                                    let mut r = vec![0u8; enhance_pir::RECORD_BYTES];
                                    r[..8].copy_from_slice(&p.to_le_bytes());
                                    r
                                })
                                .collect();
                            let height = journal
                                .committed_height()
                                .map_or(enhance_pir::ACTIVATION_HEIGHT, |h| h + 1);
                            journal.records.append_block(
                                height,
                                format!("{height:064x}"),
                                &records,
                            )?;
                        }
                        journal.highest_committed().ok_or("empty fixture")?
                    };
                    coordinator.observe_capacity(
                        journal.records.tree_size(),
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)?
                            .as_secs(),
                        capacity_policy,
                    )?;
                    if coordinator
                        .manifest()
                        .await
                        .is_some_and(|m| m.anchor_height == height && m.anchor_block_hash == hash)
                    {
                        return Ok(());
                    }
                    let verify_hash = hash.clone();
                    coordinator
                        .publish_checked(&journal.records, height, hash, || async {
                            if let Some(rpc) = &rpc {
                                if rpc.block_hash(height).await.map_err(|e| e.to_string())?
                                    != verify_hash
                                {
                                    return Err(
                                        "canonical anchor changed during preparation".into()
                                    );
                                }
                            }
                            Ok(())
                        })
                        .await?;
                    Ok(())
                }
                .await;
                if let Err(error) = result {
                    tracing::error!(%error, "ingestion/publication blocked");
                }
                if task.is_finished() {
                    return Err("HTTP server stopped".into());
                }
                tokio::time::sleep(Duration::from_secs(poll_seconds)).await;
            }
        }
    }
    Ok(())
}
