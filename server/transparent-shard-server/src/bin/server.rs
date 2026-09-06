//! Private transparent shard retrieval service.

use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use tower_http::limit::RequestBodyLimitLayer;
use tracing_subscriber::EnvFilter;
use transparent_shard_server::service::{router, ServiceState};
use transparent_shard_server::shardset::ShardSet;

#[derive(Parser)]
#[command(
    name = "transparent-shard-server",
    about = "Private retrieval from a published transparent shard set"
)]
struct Cli {
    /// Loopback by default: the wallet-facing API is not designed yet, and
    /// nothing should be reachable from the internet until it is.
    #[arg(long, default_value = "127.0.0.1:8092")]
    listen: SocketAddr,
    #[arg(long, default_value = "./transparent-shards")]
    shard_dir: PathBuf,
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
    let set = ShardSet::open(&cli.shard_dir)?;
    tracing::info!(
        shards = set.shards.len(),
        start = set.map.start_height,
        covered_through = set.map.shards.last().map(|s| s.end_height),
        "loaded shard set"
    );

    let state = ServiceState::build(set)
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
    let limit = state.max_query_bytes();
    let app = router(state).layer(RequestBodyLimitLayer::new(limit));

    let listener = tokio::net::TcpListener::bind(cli.listen).await?;
    tracing::info!(listen = %cli.listen, max_query_bytes = limit, "serving");
    axum::serve(listener, app).await?;
    Ok(())
}
