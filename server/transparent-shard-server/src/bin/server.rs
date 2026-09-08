//! Private transparent shard retrieval service.

use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use tower_http::limit::RequestBodyLimitLayer;
use tracing_subscriber::EnvFilter;
use transparent_shard_server::service::{router, ServiceConfig, ServiceState};
use transparent_shard_server::shardset::{ShardSet, DEFAULT_RETAIN_REVISIONS};

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli = Cli::parse();

    // Loading verifies every shard against its own manifest digest. A corrupt
    // shard must fail here rather than be served, or every client would reject
    // its rows as a PIR fault instead of as a corrupt shard.
    let set = ShardSet::open(&cli.shard_dir, cli.retain_revisions)?;
    tracing::info!(
        shards = set.len(),
        revisions = set.revisions().len(),
        geometries = ?set.geometries().iter().map(|g| g.name).collect::<Vec<_>>(),
        map_sha256 = set.map_digest,
        start = set.map.start_height,
        covered_through = set.map.shards.last().map(|s| s.end_height),
        "loaded shard set"
    );

    let config = ServiceConfig {
        cache_bytes: cli.cache_bytes,
        build_slots: cli.build_slots,
        query_slots: cli.query_slots,
    };
    let state = ServiceState::build(set, config)
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
    let limit = state.max_query_bytes();
    let app = router(state).layer(RequestBodyLimitLayer::new(limit));

    let listener = tokio::net::TcpListener::bind(cli.listen).await?;
    tracing::info!(listen = %cli.listen, max_query_bytes = limit, "serving");
    axum::serve(listener, app).await?;
    Ok(())
}
