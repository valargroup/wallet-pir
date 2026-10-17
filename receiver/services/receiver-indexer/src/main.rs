//! Resumable public-chain indexer and continuous canonical receiver PIR service.
//! `receiver-directory probe ...` runs the monitor's probe instead.
use clap::Parser;
use receiver_indexer::{
    application::{self, PROVIDERS},
    blocks::{MAX_RECEIVER_BATCH_BLOCKS, MAX_RECEIVER_CONCURRENCY},
    canonical::freshest,
    near::{Explorer, Reads},
};
use receiver_pir_server::Publications;
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    time::Duration,
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use zakura_server_chain::{Network, NodeClient};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    data_dir: PathBuf,
    /// Minimum served row count. Grows by powers of two up to 65536.
    #[arg(long, default_value_t = receiver_pir::MIN_ROWS)]
    min_rows: u32,
    /// Publish common witness data. Requires commitment history from position zero.
    #[arg(long)]
    witnesses: bool,
    /// The most commitments a witness build reads. A longer history publishes nothing,
    /// so the served publication goes stale instead of exhausting memory. The default,
    /// 2^24, peaks near 3.4 GB at about 200 bytes per commitment, within the unit's
    /// 7 GiB `MemoryMax`, and leaves years of growth at about 7,600 a day.
    #[arg(long, default_value_t = 1 << 24)]
    max_witness_commitments: u64,
    /// A node's RPC endpoint. Repeat it for more nodes: each pass runs on the mainnet
    /// one with the highest tip (see [`NodeClient::ranked`]).
    #[arg(long, required = true)]
    rpc_url: Vec<String>,
    #[arg(long, required_unless_present = "no_auth", conflicts_with = "no_auth")]
    cookie: Option<PathBuf>,
    #[arg(long)]
    no_auth: bool,
    #[arg(long, default_value_t = receiver_indexer::blocks::ironwood_activation())]
    start_height: u32,
    #[arg(long)]
    end_height: Option<u32>,
    /// Blocks below the node's tip to publish at, so a short reorg never revokes
    /// sessions. Wallets accept publications up to 100 blocks behind. A stored tip
    /// above that, as after a restart with a larger depth, is published as it is until
    /// the chain passes it: it is canonical, and the depth applies to new blocks.
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u32).range(0..=50))]
    depth: u32,
    /// Continuously reconcile the canonical chain and atomically rotate the service,
    /// publishing from memory. Without it, one run writes its publication's files.
    #[arg(long, conflicts_with = "end_height")]
    serve: bool,
    /// The service's listener: loopback, or a private IPv4 or unique-local IPv6 address
    /// that only the TLS proxy and monitoring hosts on the private network can reach.
    #[arg(long, default_value = "127.0.0.1:18380")]
    bind: SocketAddr,
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
    poll_seconds: u64,
    /// Maximum blocks retained for one validated batch.
    #[arg(long, default_value_t = MAX_RECEIVER_BATCH_BLOCKS, value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_RECEIVER_BATCH_BLOCKS)))]
    batch_size: u32,
    /// Maximum simultaneous raw-block RPC requests.
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_RECEIVER_CONCURRENCY)))]
    concurrency: u32,
    /// Where the NEAR feed starts on its first read, in Unix seconds; a day ago by
    /// default. The feed runs while serving when `NEAR_INTENTS_EXPLORER` holds a key.
    #[arg(long)]
    near_since: Option<i64>,
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(10..))]
    near_poll_seconds: u64,
    /// The partner key `NEAR_INTENTS_EXPLORER` holds, if any. Without one, publications
    /// carry no provider sets, whatever an earlier run left in `providers.sqlite`.
    #[arg(skip)]
    near_key: Option<String>,
}

impl From<&Args> for application::Config {
    fn from(args: &Args) -> Self {
        Self {
            data_dir: args.data_dir.clone(),
            min_rows: args.min_rows,
            witnesses: args.witnesses,
            max_witness_commitments: args.max_witness_commitments,
            start_height: args.start_height,
            end_height: args.end_height,
            depth: args.depth,
            batch_size: args.batch_size,
            concurrency: args.concurrency,
            providers_enabled: args.near_key.is_some(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::args().nth(1).as_deref() == Some("serve-publication") {
        // Serves what an operator stages; never indexes, opens no database and reads
        // no partner key. See `receiver_indexer::serve_publication`.
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
            .init();
        let args = receiver_indexer::serve_publication::Args::parse_from(std::env::args().skip(1));
        return receiver_indexer::serve_publication::main(args).await;
    }
    if std::env::args().nth(1).as_deref() == Some("probe") {
        let args = receiver_indexer::probe::Args::parse_from(std::env::args().skip(1));
        std::process::exit(if receiver_indexer::probe::run(args).await {
            0
        } else {
            1
        });
    }
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let mut args = Args::parse();
    args.near_key = std::env::var("NEAR_INTENTS_EXPLORER")
        .ok()
        .filter(|k| !k.is_empty());
    receiver_pir::validate_rows(args.min_rows)?;
    if args.start_height < receiver_indexer::blocks::ironwood_activation() {
        return Err("start must be at or after Ironwood activation".into());
    }
    // Witnesses need every commitment, and wallets refuse served coverage that starts
    // later; a one-shot run without witnesses may.
    if (args.witnesses || args.serve)
        && args.start_height != receiver_indexer::blocks::ironwood_activation()
    {
        return Err("witnesses and serving need history from Ironwood activation".into());
    }
    let private = match args.bind.ip() {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
    };
    if args.serve && !private {
        return Err("continuous serving requires a loopback or private bind".into());
    }
    let nodes = args
        .rpc_url
        .iter()
        .map(|url| NodeClient::new(url.clone(), args.cookie.as_deref()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    // This indexer currently requires mainnet; fail now if no node is on it.
    let genesis = Network::Mainnet.genesis_hash();
    freshest(&nodes, genesis).await?;
    let config = application::Config::from(&args);
    let mut state = application::State::default();
    if !args.serve {
        application::refresh(&config, &nodes, genesis, None, &mut state).await?;
        return Ok(());
    }
    let publications = Publications::default();
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    let reads = Reads::default();
    let app = receiver_pir_server::router_with_health(publications.clone(), {
        let reads = reads.clone();
        std::sync::Arc::new(move || reads.health())
    });
    info!(
        address = %listener.local_addr()?,
        "receiver PIR listening; waiting for a canonical publication"
    );
    // Canonical checks continue while indexing, proofs and PIR preparation are in progress.
    let guard = tokio::spawn(receiver_indexer::canonical::guard(
        publications.clone(),
        nodes.clone(),
        genesis,
        Duration::from_secs(args.poll_seconds),
    ));
    if let Some(key) = args.near_key.clone() {
        let path = args.data_dir.join(PROVIDERS);
        let since = args
            .near_since
            .unwrap_or_else(|| application::unix_now() - receiver_indexer::near::RECENT_SECS);
        let poll = Duration::from_secs(args.near_poll_seconds);
        let mut explorer = Explorer::new(key)?;
        tokio::spawn(async move {
            loop {
                receiver_indexer::near::poll(&mut explorer, &path, since, &reads).await;
                tokio::time::sleep(poll).await;
            }
        });
    }
    let http = tokio::spawn(async move { axum::serve(listener, app).await });
    let refresh_loop = async {
        loop {
            let started = std::time::Instant::now();
            if let Err(error) =
                application::refresh(&config, &nodes, genesis, Some(&publications), &mut state)
                    .await
            {
                warn!(%error, "ingestion or publication deferred");
            }
            info!(
                elapsed_ms = started.elapsed().as_millis() as u64,
                "receiver refresh"
            );
            tokio::time::sleep(Duration::from_secs(args.poll_seconds)).await;
        }
    };
    tokio::select! {
        result = http => { result??; },
        result = guard => { result?; return Err("canonical guard stopped".into()); },
        _ = refresh_loop => {},
        _ = shutdown() => {},
    }
    Ok(())
}

/// Resolves on Ctrl-C, or on SIGTERM on Unix.
async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
