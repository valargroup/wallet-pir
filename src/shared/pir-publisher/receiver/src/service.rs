//! Resumable public-chain indexer and continuous canonical receiver PIR service.
//! `receiver-directory probe ...` runs the monitor's probe instead.
use crate::{
    application::{self, PROVIDERS},
    blocks::{MAX_RECEIVER_BATCH_BLOCKS, MAX_RECEIVER_CONCURRENCY},
    canonical::freshest,
    near::{Explorer, Reads},
};
use clap::Parser;
use receiver_pir_server::Publications;
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    time::Duration,
};
use tracing::info;
use zakura_server_chain::{Network, NodeClient};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[arg(long)]
    pub data_dir: PathBuf,
    /// Minimum served row count. Grows by powers of two up to 65536.
    #[arg(long, default_value_t = receiver_pir::MIN_ROWS)]
    pub min_rows: u32,
    /// Publish common witness data. Requires commitment history from position zero.
    #[arg(long)]
    #[serde(default)]
    pub witnesses: bool,
    /// The most commitments a witness build reads. A longer history publishes nothing,
    /// so the served publication goes stale instead of exhausting memory. The default,
    /// 2^24, peaks near 3.4 GB at about 200 bytes per commitment, within the unit's
    /// 7 GiB `MemoryMax`, and leaves years of growth at about 7,600 a day.
    #[arg(long, default_value_t = 1 << 24)]
    pub max_witness_commitments: u64,
    /// A node's RPC endpoint. Repeat it for more nodes: each pass runs on the mainnet
    /// one with the highest tip (see [`NodeClient::ranked`]).
    #[arg(long, required = true)]
    pub rpc_url: Vec<String>,
    #[arg(long, required_unless_present = "no_auth", conflicts_with = "no_auth")]
    pub cookie: Option<PathBuf>,
    #[arg(long)]
    #[serde(default)]
    pub no_auth: bool,
    #[arg(long, default_value_t = crate::blocks::ironwood_activation())]
    pub start_height: u32,
    #[arg(long)]
    pub end_height: Option<u32>,
    /// Blocks below the node's tip to publish at, so a short reorg never revokes
    /// sessions. Wallets accept publications up to 100 blocks behind. A stored tip
    /// above that, as after a restart with a larger depth, is published as it is until
    /// the chain passes it: it is canonical, and the depth applies to new blocks.
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u32).range(0..=50))]
    pub depth: u32,
    /// Continuously reconcile the canonical chain and atomically rotate the service,
    /// publishing from memory. Without it, one run writes its publication's files.
    #[arg(long, conflicts_with = "end_height")]
    #[serde(default)]
    pub serve: bool,
    /// The service's listener: loopback, or a private IPv4 or unique-local IPv6 address
    /// that only the TLS proxy and monitoring hosts on the private network can reach.
    #[arg(long, default_value = "127.0.0.1:18380")]
    pub bind: SocketAddr,
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
    pub poll_seconds: u64,
    /// Maximum blocks retained for one validated batch.
    #[arg(long, default_value_t = MAX_RECEIVER_BATCH_BLOCKS, value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_RECEIVER_BATCH_BLOCKS)))]
    pub batch_size: u32,
    /// Maximum simultaneous raw-block RPC requests.
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_RECEIVER_CONCURRENCY)))]
    pub concurrency: u32,
    /// Where the NEAR feeds' history starts, in Unix seconds, before the first
    /// one-time address a supported wallet issued. Sweeps backfill the history down to
    /// it, resuming across restarts, and a later, earlier value backfills the
    /// difference, and a later value never shrinks the history. Required while serving
    /// with a key: the feed runs then, when `NEAR_INTENTS_EXPLORER` holds one.
    #[arg(long)]
    pub near_since: Option<i64>,
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(10..))]
    pub near_poll_seconds: u64,
    /// The partner key `NEAR_INTENTS_EXPLORER` holds, if any. Without one, publications
    /// carry no provider sets, whatever an earlier run left in `providers.sqlite`.
    #[serde(skip)]
    #[arg(skip)]
    pub near_key: Option<String>,
}

impl From<&Config> for application::Config {
    fn from(args: &Config) -> Self {
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

impl Config {
    pub fn validate(&self) -> Result<()> {
        receiver_pir::validate_rows(self.min_rows)?;
        if self.start_height < crate::blocks::ironwood_activation() {
            return Err("start must be at or after Ironwood activation".into());
        }
        // Witnesses need every commitment, and wallets refuse served coverage that starts
        // later; a one-shot run without witnesses may.
        if (self.witnesses || self.serve)
            && self.start_height != crate::blocks::ironwood_activation()
        {
            return Err("witnesses and serving need history from Ironwood activation".into());
        }
        if self.serve && self.near_key.is_some() && self.near_since.is_none() {
            return Err("--near-since is required to serve with a NEAR key".into());
        }
        let private = match self.bind.ip() {
            IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
            IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
        };
        if self.serve && !private {
            return Err("continuous serving requires a loopback or private bind".into());
        }
        if self.rpc_url.is_empty()
            || self.cookie.is_some() == self.no_auth
            || self.poll_seconds == 0
            || self.near_poll_seconds < 10
            || self.depth > 50
            || self.batch_size == 0
            || self.batch_size > MAX_RECEIVER_BATCH_BLOCKS
            || self.concurrency == 0
            || self.concurrency > MAX_RECEIVER_CONCURRENCY
            || (self.serve && self.end_height.is_some())
            || self
                .near_since
                .is_some_and(|since| since > application::unix_now())
        {
            return Err("invalid receiver source, polling, or batch settings".into());
        }
        Ok(())
    }
}
pub async fn run(
    mut args: Config,
    http: pir_pipeline::service::Http<axum::Router>,
    shutdown: pir_pipeline::service::Shutdown,
) -> Result<()> {
    args.near_key = std::env::var("NEAR_INTENTS_EXPLORER")
        .ok()
        .filter(|k| !k.is_empty());
    args.validate()?;
    let _lock = pir_pipeline::service::lock(&args.data_dir)?;
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
    struct Fence(Publications);
    impl Drop for Fence {
        fn drop(&mut self) {
            self.0.revoke();
        }
    }
    let _fence = Fence(publications.clone());
    let mut tasks = pir_pipeline::service::Tasks::default();
    let reads = Reads::default();
    let app = receiver_pir_server::router_with_health(publications.clone(), {
        let (reads, refusal) = (reads.clone(), state.refusal());
        std::sync::Arc::new(move || {
            let mut fields = reads.health();
            fields.extend(refusal.health());
            fields
        })
    });
    info!(
        address = %args.bind,
        "receiver PIR listening; waiting for a canonical publication"
    );
    // Canonical checks continue while indexing, proofs and PIR preparation are in progress.
    let guard = tokio::spawn(crate::canonical::guard(
        publications.clone(),
        nodes.clone(),
        genesis,
        Duration::from_secs(args.poll_seconds),
    ));
    tasks.own(&guard);
    if let (Some(key), Some(start)) = (args.near_key.clone(), args.near_since) {
        let path = args.data_dir.join(PROVIDERS);
        let poll = Duration::from_secs(args.near_poll_seconds);
        let mut explorer = Explorer::new(key)?;
        let provider = tokio::spawn(async move {
            loop {
                crate::near::poll(&mut explorer, &path, start, &reads).await;
                tokio::time::sleep(poll).await;
            }
        });
        tasks.own(&provider);
    }
    let mut serving = tokio::spawn(http.serve(args.bind, app));
    tasks.own(&serving);
    let failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let failed_task = failed.clone();
    let stop = shutdown.clone();
    let watched = tokio::spawn(async move {
        let mut guard = guard;
        tokio::select! { _ = &mut guard => {}, _ = &mut serving => {} }
        failed_task.store(true, std::sync::atomic::Ordering::SeqCst);
        stop.request();
    });
    tasks.own(&watched);
    application::run_until(
        &config,
        &nodes,
        genesis,
        &publications,
        &mut state,
        Duration::from_secs(args.poll_seconds),
        shutdown,
    )
    .await?;
    if failed.load(std::sync::atomic::Ordering::SeqCst) {
        return Err("receiver background service stopped".into());
    }
    Ok(())
}

/// Receiver SQLite connections stay on their owner thread; no Sync wrapper is introduced.
pub async fn run_isolated(
    config: Config,
    http: pir_pipeline::service::Http<axum::Router>,
    shutdown: pir_pipeline::service::Shutdown,
) -> Result<()> {
    let thread = std::thread::Builder::new()
        .name("receiver-publisher".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .max_blocking_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(run(config, http, shutdown))
        })?;
    tokio::task::spawn_blocking(move || {
        thread
            .join()
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                "Receiver publisher thread panicked".into()
            })?
    })
    .await?
}
