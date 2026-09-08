//! Private transparent shard retrieval service.

use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::limit::RequestBodyLimitLayer;
use tracing_subscriber::EnvFilter;
use transparent_shard_server::assignment::Assignment;
use transparent_shard_server::service::{router, ReadinessMode, ServiceConfig, ServiceState};
use transparent_shard_server::shardset::{
    LoadOptions, LoadScope, ShardSet, DEFAULT_RETAIN_REVISIONS,
};

#[derive(Parser)]
#[command(
    name = "transparent-shard-server",
    about = "Private retrieval from a published transparent shard set"
)]
struct Cli {
    /// Loopback by default. Deployment exposes only the required wallet routes
    /// through its proxy and keeps operational endpoints private.
    #[arg(long, default_value = "127.0.0.1:8092")]
    listen: SocketAddr,
    #[arg(long, default_value = "./transparent-shards")]
    shard_dir: PathBuf,
    /// Bytes of prepared runtime the cache may hold.
    ///
    /// Set this below the host's real headroom. The cache counts each runtime's
    /// database and pack matrices and nothing else, so allocator overhead, the
    /// transient plaintext each build reads, the shared parameters, and every
    /// in-flight request come out of what is left. On the 16 GiB worker with
    /// `MemoryMax=12G`, 8 GiB leaves 4 GiB of that slack.
    #[arg(long, default_value_t = 8 << 30)]
    cache_bytes: u64,
    /// Local disposable cache of prepared public runtimes; disabled if omitted.
    #[arg(long)]
    runtime_cache_dir: Option<PathBuf>,
    #[arg(long, requires = "runtime_cache_dir")]
    runtime_cache_max_bytes: Option<u64>,
    /// Prune only cache revisions absent from all named active/rollback sets.
    #[arg(long, requires = "runtime_cache_dir")]
    runtime_cache_prune_set: Vec<PathBuf>,
    /// Runtime builds that may run at once.
    ///
    /// Build latency and transient allocation depend on geometry and hardware.
    /// Bound construction separately from evaluation and measure process RSS
    /// while cold requests overlap the steady-state cache.
    #[arg(long, default_value_t = 1)]
    build_slots: usize,
    /// Query evaluations that may run at once.
    ///
    /// Start at two and benchmark the selected host. The recorded c-8 run
    /// saturated at one evaluation thread; more slots are not a throughput
    /// guarantee and hold more runtimes pinned against eviction.
    #[arg(long, default_value_t = 2)]
    query_slots: usize,
    /// Superseded revisions to keep per shard, beyond the one the map names.
    ///
    /// A republished tail leaves its predecessor on disk. Keeping some means a
    /// wallet that fetched setup for the old revision is answered from it
    /// rather than told to start over; keeping them all means an unbounded set.
    #[arg(long, default_value_t = DEFAULT_RETAIN_REVISIONS)]
    retain_revisions: usize,
    /// Requests that may wait for a slot or a runtime beyond those running.
    ///
    /// A full queue refuses with 503 and a retry delay. The queue is a buffer
    /// against jitter, not a backlog: past this, a wallet is better told to
    /// retry than kept waiting behind the rest.
    #[arg(long, default_value_t = 64)]
    query_waiters: usize,
    /// Query body bytes that may be buffered at once across every waiting and
    /// running query.
    #[arg(long, default_value_t = 64 << 20)]
    body_bytes: u64,
    /// Seconds a client has to deliver a query body once admitted.
    #[arg(long, default_value_t = 10)]
    upload_deadline_secs: u64,
    /// Seconds a request may wait in total before it is refused retryably.
    #[arg(long, default_value_t = 30)]
    query_deadline_secs: u64,
    /// The fleet assignment this worker loads its subset from. With
    /// `--worker-id`. Without it the whole set is loaded and served.
    #[arg(long, requires = "worker_id")]
    assignment: Option<PathBuf>,
    /// This worker's id in the assignment.
    #[arg(long, requires = "assignment")]
    worker_id: Option<String>,
    /// Bytes of superseded revisions to keep per shard, on disk, beyond the
    /// current one. Unbounded by default; the count bound still applies.
    #[arg(long)]
    retain_bytes: Option<u64>,
    /// Report superseded revisions past the retention bound as prunable
    /// rather than refusing to start. The fleet's mode; the deploy prunes
    /// after activation with `shard-prune`.
    #[arg(long)]
    prune_excess: bool,
    /// Report ready as soon as the set is loaded, without prewarming. The
    /// correctness pilot's mode. Under an assignment the default is to
    /// prewarm every assigned runtime and report ready only then.
    #[arg(long)]
    pilot_cold: bool,
    /// Load and verify the set under these options, print what would be
    /// served, and exit without listening. The deploy runs this with the
    /// staged binary before it stops the running service.
    #[arg(long)]
    verify_only: bool,
    /// Root-only publication control socket. Omit to retain static serving.
    #[arg(long)]
    control_socket: Option<PathBuf>,
    /// Durable active publication record used on restart.
    #[arg(long, requires = "control_socket")]
    active_record: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let mut cli = Cli::parse();
    let active_record = cli
        .active_record
        .clone()
        .unwrap_or_else(|| cli.shard_dir.with_extension("active.json"));
    if cli.control_socket.is_some() && active_record.exists() {
        let active: transparent_shard_server::live::Publication =
            serde_json::from_slice(&std::fs::read(&active_record)?)?;
        cli.shard_dir = active.directory;
        cli.assignment = active.assignment;
    }
    let disk = cli
        .runtime_cache_dir
        .clone()
        .map(|dir| {
            transparent_shard_server::runtime::disk::DiskCache::new(
                dir,
                cli.runtime_cache_max_bytes
                    .unwrap_or(cli.cache_bytes.saturating_mul(2)),
            )
        })
        .transpose()?;
    if !cli.runtime_cache_prune_set.is_empty() {
        let mut keep = std::collections::HashSet::new();
        for set in &cli.runtime_cache_prune_set {
            keep.extend(transparent_shard_server::runtime::disk::retained_digests(
                set,
            )?);
        }
        let freed = disk
            .as_ref()
            .expect("clap requires cache directory")
            .prune(&keep)?;
        println!("runtime cache pruned: {freed} bytes freed");
        return Ok(());
    }

    let scope = match (&cli.assignment, &cli.worker_id) {
        (Some(path), Some(worker_id)) => LoadScope::Assigned {
            assignment: Arc::new(Assignment::load(path)?),
            worker_id: worker_id.clone(),
        },
        _ => LoadScope::Whole,
    };
    let options = LoadOptions {
        retain_revisions: cli.retain_revisions,
        retain_bytes: cli.retain_bytes,
        prune_excess: cli.prune_excess,
        scope,
    };
    // Loading verifies every shard against its own manifest digest. A corrupt
    // shard must fail here rather than be served, or every client would reject
    // its rows as a PIR fault instead of as a corrupt shard.
    let set = ShardSet::open_with(&cli.shard_dir, &options)?;
    tracing::info!(
        shards = set.len(),
        assigned = set.assigned_len(),
        revisions = set.revisions().len(),
        prunable = set.prunable().len(),
        geometries = ?set.geometries().iter().map(|g| g.name).collect::<Vec<_>>(),
        map_sha256 = set.map_digest,
        worker_id = set.scope().map(|scope| scope.worker_id.as_str()),
        assignment_sha256 = set.scope().map(|scope| scope.assignment_sha256.as_str()),
        start = set.map.start_height,
        covered_through = set.map.shards.last().map(|s| s.end_height),
        "loaded shard set"
    );
    for prunable in set.prunable() {
        tracing::warn!(
            shard = prunable.shard_id,
            revision = prunable.revision,
            bytes = prunable.bytes,
            path = %prunable.path.display(),
            "superseded revision past the retention bound; prune it"
        );
    }

    let readiness = if cli.assignment.is_some() && !cli.pilot_cold {
        ReadinessMode::Warm
    } else {
        ReadinessMode::LoadedOnly
    };
    let config = ServiceConfig {
        cache_bytes: cli.cache_bytes,
        build_slots: cli.build_slots,
        query_slots: cli.query_slots,
        max_waiters: cli.query_waiters,
        max_body_bytes: cli.body_bytes,
        upload_deadline: std::time::Duration::from_secs(cli.upload_deadline_secs),
        query_deadline: std::time::Duration::from_secs(cli.query_deadline_secs),
        readiness,
    };
    if cli.verify_only {
        let report = disk.as_ref().map(|disk| disk.check_set(&set)).transpose()?;
        // Everything the service would check before listening has been
        // checked, including that the assignment fits the cache in warm mode.
        ServiceState::build(set, config)
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
        println!(
            "{}",
            serde_json::json!({"verified":true,"runtime_cache":report})
        );
        return Ok(());
    }
    let map_sha256 = set.map_digest.clone();
    let state = ServiceState::build_with_disk(set, config, disk)
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
    let limit = state.max_query_bytes();
    let app = if let Some(socket) = cli.control_socket {
        let live = transparent_shard_server::live::LiveService::new(
            state.clone(),
            transparent_shard_server::live::Publication {
                directory: cli.shard_dir,
                assignment: cli.assignment,
                map_sha256,
            },
            config,
            options,
            active_record,
        )?;
        let app = live.router();
        tokio::spawn(async move {
            if let Err(error) = live.listen(&socket).await {
                tracing::error!(%error, "control socket failed");
                std::process::exit(1);
            }
        });
        app
    } else {
        router(state.clone())
    }
    .layer(RequestBodyLimitLayer::new(limit));

    // Bound before the prewarm starts, so the operator routes answer while
    // the runtimes build and a router's health check sees "not ready" rather
    // than "connection refused".
    let listener = tokio::net::TcpListener::bind(cli.listen).await?;
    tracing::info!(listen = %cli.listen, max_query_bytes = limit, ?readiness, "serving");
    let _prewarm = state.spawn_prewarm();
    axum::serve(listener, app).await?;
    Ok(())
}
