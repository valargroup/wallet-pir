//! Loopback-only, immutable-publication server for paired parent-filter trials.
//! Deliberately does not start whole-publication prewarming or live activation.
//! The driver warms the selected baseline workload before each paired wave.
use clap::Parser;
use std::{net::SocketAddr, path::PathBuf};
use transparent_shard_server::{
    service::{router, ServiceConfig, ServiceState},
    shardset::{ShardSet, DEFAULT_RETAIN_REVISIONS},
};
#[derive(Parser)]
struct Args {
    #[arg(long)]
    shard_dir: PathBuf,
    #[arg(long, default_value = "127.0.0.1:18092")]
    listen: SocketAddr,
    #[arg(long,default_value_t=28u64<<30)]
    cache_bytes: u64,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    if !args.listen.ip().is_loopback() {
        return Err("evaluation server must bind loopback".into());
    }
    let set = ShardSet::open(&args.shard_dir, DEFAULT_RETAIN_REVISIONS)?;
    let state = ServiceState::build(
        set,
        ServiceConfig {
            cache_bytes: args.cache_bytes,
            ..ServiceConfig::default()
        },
    )?;
    let app = router(state).route(
        "/v1/parent-evaluation",
        axum::routing::get(|| async {
            axum::Json(serde_json::json!({"warmup":"paired-baseline","live_activation":false}))
        }),
    );
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    eprintln!(
        "isolated parent evaluation server listening on {}",
        args.listen
    );
    axum::serve(listener, app).await?;
    Ok(())
}
