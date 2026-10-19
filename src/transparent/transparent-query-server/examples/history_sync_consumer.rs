//! Benchmark entry point: identical full-coverage serving with pushed files or
//! the production HTTPS manager. No protocol or production CLI changes.
use clap::{Parser, ValueEnum};
use std::{net::SocketAddr, path::PathBuf, time::Instant};
use transparent_query_server::{
    history::{
        live::{Command, LiveService, Publication},
        service::{ReadinessMode, ServiceConfig, ServiceState},
    },
    routing::{bootstrap, manager::Manager},
};
use transparent_shared::{
    history::shardset::{LoadOptions, ShardSet},
    runtime::disk::ShippedRuntimes,
};

#[derive(Clone, Copy, ValueEnum)]
enum Method {
    Rsync,
    Https,
}

#[derive(Parser)]
struct Cli {
    #[arg(long, value_enum)]
    method: Method,
    #[arg(long)]
    state: PathBuf,
    #[arg(long)]
    publication: Option<PathBuf>,
    #[arg(long)]
    publisher: Option<String>,
    #[arg(long)]
    ca: Option<PathBuf>,
    #[arg(long)]
    genesis: String,
    #[arg(long)]
    listen: SocketAddr,
    #[arg(long)]
    cache_bytes: u64,
    #[arg(long)]
    download_bytes: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter("info")
        .init();
    let cli = Cli::parse();
    if !cli.listen.ip().is_loopback() {
        return Err("benchmark wallet listener must be loopback".into());
    }
    let started = Instant::now();
    std::fs::create_dir_all(&cli.state)?;
    let service = ServiceConfig {
        cache_bytes: cli.cache_bytes,
        readiness: ReadinessMode::Warm,
        build_slots: 1,
        query_slots: 2,
        max_waiters: 8,
        max_body_bytes: 64 << 20,
        upload_deadline: std::time::Duration::from_secs(30),
        query_deadline: std::time::Duration::from_secs(30),
    };
    let (router, report) = match cli.method {
        Method::Rsync => {
            let directory = cli.publication.ok_or("rsync requires --publication")?;
            let loading = Instant::now();
            let set = ShardSet::open_with(&directory, &LoadOptions::whole(2))?;
            if set.map.genesis_hash != cli.genesis {
                return Err("wrong journal network".into());
            }
            let digest = set.map_digest.clone();
            let state = ServiceState::build_with_runtimes(
                set,
                service,
                None,
                Some(ShippedRuntimes::required(directory.clone())),
            )?;
            let loading_seconds = loading.elapsed().as_secs_f64();
            let warming = Instant::now();
            state.spawn_prewarm().await?;
            let warming_seconds = warming.elapsed().as_secs_f64();
            let live = LiveService::new(
                state,
                Publication {
                    directory: directory.clone(),
                    assignment: None,
                    map_sha256: digest,
                    runtimes: Some(directory),
                },
                service,
                LoadOptions::whole(2),
                cli.state.join("active.json"),
            )?;
            let status = live.command(Command::Status).await?;
            if status["warm"] != true
                || status["runtimes"]["built"] != 0
                || status["runtimes"]["shipped_fallbacks"] != 0
            {
                return Err("pushed publication did not become warm without local builds".into());
            }
            (
                live.router(),
                serde_json::json!({"status":status,"stages":{"loading_seconds":loading_seconds,"warming_seconds":warming_seconds}}),
            )
        }
        Method::Https => {
            let roots = cli
                .ca
                .map(|p| -> Result<_, Box<dyn std::error::Error + Send + Sync>> {
                    Ok(reqwest::Certificate::from_pem(&std::fs::read(p)?)?)
                })
                .transpose()?
                .into_iter()
                .collect();
            let config = bootstrap::Config {
                publisher: cli.publisher.ok_or("HTTPS requires --publisher")?,
                network: "main".into(),
                genesis_hash: cli.genesis,
                directory: cli.state,
                cache_bytes: cli.cache_bytes,
                max_download_bytes: cli.download_bytes,
                roots,
            };
            let mut manager = Manager::new(config, service)?.with_full_holdings();
            let progress = manager.poll_once().await?;
            let status = manager
                .live()
                .ok_or("no active publication")?
                .command(Command::Status)
                .await?;
            if status["warm"] != true
                || status["runtimes"]["built"] != 0
                || status["runtimes"]["shipped_fallbacks"] != 0
            {
                return Err("HTTPS publication did not become warm without local builds".into());
            }
            // This comparison pins one publication. The production manager
            // performed its real transfer/authority/activation checks above;
            // later polling would change the workload between paired runs.
            (
                manager.router()?,
                serde_json::json!({"status":status,"progress":progress}),
            )
        }
    };
    let listener = tokio::net::TcpListener::bind(cli.listen).await?;
    println!(
        "{}",
        serde_json::json!({"ready_seconds":started.elapsed().as_secs_f64(),"report":report})
    );
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
