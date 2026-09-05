//! Serve one harness-built generation's retrieval tables over HTTP.

use clap::Parser;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tower_http::limit::RequestBodyLimitLayer;
use transparent_history_server::generation::LoadedGeneration;
use transparent_history_server::service::{router, ServiceState};

#[derive(Parser)]
struct Args {
    /// Loopback by default: this is a research service with no public route.
    #[arg(long, default_value = "127.0.0.1:8092")]
    listen: String,
    /// A generation directory published by tools/transparent_pir_incremental.py.
    #[arg(long)]
    generation_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();

    let loaded = LoadedGeneration::open(&args.generation_dir)?;
    tracing::info!(
        generation = %loaded.generation_id,
        start = loaded.manifest.start,
        end = loaded.manifest.end,
        directory_rows = loaded.directory.geometry.rows,
        pages_rows = loaded.pages.geometry.rows,
        "loaded generation; building table runtimes"
    );
    let started = std::time::Instant::now();
    let state = ServiceState::build(&loaded)?;
    tracing::info!(
        seconds = started.elapsed().as_secs_f64(),
        "table runtimes ready"
    );

    let limit = state.max_query_bytes();
    let app = router(state).layer(RequestBodyLimitLayer::new(limit));
    let listener = TcpListener::bind(&args.listen).await?;
    tracing::info!(listen = %args.listen, max_query_bytes = limit, "transparent history service started");
    axum::serve(listener, app).await?;
    Ok(())
}
