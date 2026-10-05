//! Private retrieval from a tiered, bucketed txid display publication.

use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use tower_http::limit::RequestBodyLimitLayer;
use tracing_subscriber::EnvFilter;
use transparent_shard_server::assignment::WorkerRole;
use transparent_shard_server::display::live::{DisplayLive, DisplayPublication};
use transparent_shard_server::display::service::{
    body_limit, router, DisplayRuntime, DisplayState,
};
use transparent_shard_server::display::set::DisplaySet;
use transparent_shard_server::display::{parse_role, serves};
use transparent_shard_server::service::{ReadinessMode, ServiceConfig};
use transparent_shard_server::shardset::DEFAULT_RETAIN_REVISIONS;

#[derive(Parser)]
#[command(
    name = "transparent-txid-server",
    about = "Private retrieval from a tiered txid display publication"
)]
struct Cli {
    /// Loopback by default; a worker binds its private address.
    #[arg(long, default_value = "127.0.0.1:8095")]
    listen: SocketAddr,
    /// `archive-owner` serves sealed shards, `recent-replica` the recent one.
    #[arg(long, value_parser = role)]
    role: WorkerRole,
    /// A publication directory to serve at start. With a control socket, the
    /// active record takes precedence; without either, the worker waits for
    /// its first prepare and activation.
    #[arg(long)]
    publication_dir: Option<PathBuf>,
    /// Bytes of prepared runtime the cache may hold. Staged revisions count.
    #[arg(long, default_value_t = 4 << 30)]
    cache_bytes: u64,
    /// Local disposable cache of prepared public runtimes; disabled if omitted.
    #[arg(long)]
    runtime_cache_dir: Option<PathBuf>,
    #[arg(long, requires = "runtime_cache_dir")]
    runtime_cache_max_bytes: Option<u64>,
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u16).range(1..=16))]
    runtime_restore_slots: u16,
    #[arg(long, default_value_t = 1)]
    build_slots: usize,
    #[arg(long, default_value_t = 2)]
    query_slots: usize,
    /// Superseded recent revisions on disk to keep serving.
    #[arg(long, default_value_t = DEFAULT_RETAIN_REVISIONS)]
    retain_revisions: usize,
    #[arg(long, default_value_t = 64)]
    query_waiters: usize,
    #[arg(long, default_value_t = 64 << 20)]
    body_bytes: u64,
    #[arg(long, default_value_t = 10)]
    upload_deadline_secs: u64,
    #[arg(long, default_value_t = 30)]
    query_deadline_secs: u64,
    /// Report ready once loaded rather than once the role's runtimes are warm.
    #[arg(long)]
    loaded_only: bool,
    /// Load and verify the publication, print what would be served, and exit.
    #[arg(long)]
    verify_only: bool,
    /// Root-only control socket; see `txid-control`.
    #[arg(long, requires = "active_record")]
    control_socket: Option<PathBuf>,
    /// Durable record of the active publication, used on restart.
    #[arg(long, requires = "control_socket")]
    active_record: Option<PathBuf>,
    /// Directory whose unused shipped candidates and staged revisions
    /// `collect` may delete. Omit when the root is shared with the controller
    /// or another worker: nothing on disk is then removed.
    #[arg(long, requires = "control_socket")]
    collect_root: Option<PathBuf>,
}

fn role(text: &str) -> Result<WorkerRole, String> {
    parse_role(text).ok_or_else(|| format!("{text} is not recent-replica or archive-owner"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli = Cli::parse();
    let mut directory = cli.publication_dir.clone();
    if let Some(record) = cli.active_record.as_ref().filter(|r| r.exists()) {
        let active: DisplayPublication = serde_json::from_slice(&std::fs::read(record)?)?;
        directory = Some(active.directory);
    }
    let config = ServiceConfig {
        cache_bytes: cli.cache_bytes,
        build_slots: cli.build_slots,
        query_slots: cli.query_slots,
        max_waiters: cli.query_waiters,
        max_body_bytes: cli.body_bytes,
        upload_deadline: std::time::Duration::from_secs(cli.upload_deadline_secs),
        query_deadline: std::time::Duration::from_secs(cli.query_deadline_secs),
        readiness: if cli.loaded_only {
            ReadinessMode::LoadedOnly
        } else {
            ReadinessMode::Warm
        },
    };
    let disk = cli
        .runtime_cache_dir
        .clone()
        .map(|dir| {
            let mut disk = transparent_shard_server::runtime::disk::DiskCache::new(
                dir,
                cli.runtime_cache_max_bytes
                    .unwrap_or(cli.cache_bytes.saturating_mul(2)),
            )?;
            disk.restore_slots = usize::from(cli.runtime_restore_slots);
            Ok::<_, std::io::Error>(disk)
        })
        .transpose()?;
    let runtime = DisplayRuntime::new(config, disk);

    // Loading verifies every revision of the role, segment digests and rows.
    let initial = match &directory {
        Some(dir) => {
            let set = DisplaySet::open(dir, cli.retain_revisions, cli.role)?;
            tracing::info!(
                role = cli.role.as_str(),
                map_sha256 = set.map_digest,
                shards = set.map.shards.len(),
                held = set.current().iter().filter(|r| r.held()).count(),
                retained = set.revisions().len() - set.map.shards.len(),
                start = set.map.start_height,
                covered_through = set.map.covered_through(),
                "loaded display publication"
            );
            let publication = DisplayPublication {
                directory: dir.clone(),
                map_sha256: set.map_digest.clone(),
            };
            Some((DisplayState::build(set, &runtime)?, publication))
        }
        None => None,
    };
    if cli.verify_only {
        let Some((state, publication)) = &initial else {
            return Err("--verify-only needs --publication-dir or an active record".into());
        };
        let set = state.set();
        println!(
            "{}",
            serde_json::json!({
                "verified": true,
                "role": cli.role.as_str(),
                "map_sha256": publication.map_sha256,
                "shards": set.map.shards.len(),
                "held": set
                    .current()
                    .iter()
                    .filter(|r| serves(cli.role, r.manifest.sealed))
                    .map(|r| &r.digest)
                    .collect::<Vec<_>>(),
                "warm_target": state.warm_target(),
            })
        );
        return Ok(());
    }
    let prewarm = initial.as_ref().map(|(state, _)| state.spawn_prewarm());
    let app = match cli.control_socket {
        Some(socket) => {
            let live = DisplayLive::new(
                runtime,
                cli.role,
                cli.retain_revisions,
                cli.active_record.expect("clap requires the record"),
                cli.collect_root,
                initial,
            )?;
            let app = live.router();
            tokio::spawn(async move {
                if let Err(error) = live.listen(&socket).await {
                    tracing::error!(%error, "control socket failed");
                    std::process::exit(1);
                }
            });
            app
        }
        None => {
            let Some((state, _)) = initial else {
                return Err("serving without a control socket needs --publication-dir".into());
            };
            router(state)
        }
    }
    .layer(RequestBodyLimitLayer::new(body_limit()));
    let listener = tokio::net::TcpListener::bind(cli.listen).await?;
    tracing::info!(listen = %cli.listen, role = cli.role.as_str(), "serving txid display");
    let _prewarm = prewarm;
    axum::serve(listener, app).await?;
    Ok(())
}
