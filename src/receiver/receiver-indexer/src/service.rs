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
    #[arg(long, default_value_t = zakura_pir_publisher::receiver::session::MIN_ROWS)]
    pub min_rows: u32,
    /// A node's RPC endpoint. Repeat it for more nodes: each pass runs on the mainnet
    /// one with the highest tip (see [`NodeClient::ranked`]).
    #[arg(long, required = true)]
    pub rpc_url: Vec<String>,
    #[arg(long, required_unless_present = "no_auth", conflicts_with = "no_auth")]
    pub cookie: Option<PathBuf>,
    #[arg(long)]
    #[serde(default)]
    pub no_auth: bool,
    /// Must be Ironwood activation: companion witness tables need every commitment of each
    /// Ironwood shard, and wallets refuse served coverage that starts later.
    #[arg(long, default_value_t = crate::blocks::ironwood_activation())]
    pub start_height: u32,
    #[arg(long)]
    pub end_height: Option<u32>,
    /// Blocks below the node's tip to publish at, so a short reorg never revokes
    /// sessions. A wallet waits for a publication that reaches the frontier it asks
    /// for, so depth adds to its lookup delay. A stored tip above the depth, as after a
    /// restart with a larger one, is published as it is until the chain passes it: it
    /// is canonical, and the depth applies to new blocks.
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
    /// Caps the rolling filter window in blocks. By default it spans seven days of target
    /// time, which is more blocks after NU7 (see `blocks::filter_window_blocks`); it never
    /// spans more. PIR history is retained.
    #[arg(long)]
    #[serde(default)]
    pub filter_blocks: Option<u32>,
    /// Pinned payment table rows of every sealed Receiver shard segment. Sealing keeps
    /// each segment placeable at this geometry; changing it affects only later seals.
    #[arg(long, default_value_t = zakura_pir_publisher::receiver::session::MAX_ROWS)]
    #[serde(default = "default_shard_rows")]
    pub shard_rows: u32,
    /// Payments after which a Receiver shard seals at the next block. By default one
    /// seventh below the smaller of the segment's payment slots and the largest witness
    /// table's payloads.
    #[arg(long)]
    #[serde(default)]
    pub shard_target: Option<u64>,
    /// How long a displaced inventory, with every session, chunk and shard filter it
    /// names, keeps serving, in seconds: long enough for a wallet that captured it to
    /// finish a restore pass. At most 16 are retained; a new publication waits for one to
    /// expire rather than drop one early.
    #[arg(long, default_value_t = 600, value_parser = clap::value_parser!(u64).range(60..=86_400))]
    #[serde(default = "default_retention_seconds")]
    pub retention_seconds: u64,
    /// The most memory prepared PIR tables may occupy, in MiB, counting cached tables,
    /// retained inventories' and in-flight queries' tables. Sealed tables live in the data
    /// directory's `tables/` and load on demand within it. The default leaves the
    /// deployed unit's 7 GiB `MemoryMax` room for preparation, the index and witness
    /// paths. It must hold the largest payment and witness pair, 1536 MiB, which a cold
    /// witness load needs at once.
    #[arg(long, default_value_t = 4096)]
    #[serde(default = "default_table_memory_mib")]
    pub table_memory_mib: u64,
    /// Increase after correcting indexed history on an unchanged chain. Never decrease on restart.
    #[arg(long, default_value_t = 0)]
    #[serde(default)]
    pub history_revision: u64,
    /// Where the NEAR feed starts on its first read, in Unix seconds; a day ago by
    /// default. The feed runs while serving when `NEAR_INTENTS_EXPLORER` holds a key.
    #[arg(long)]
    pub near_since: Option<i64>,
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(10..))]
    pub near_poll_seconds: u64,
    /// Optional payout-audit partner key. It has no role in wallet discovery or issuance.
    #[serde(skip)]
    #[arg(skip)]
    pub near_key: Option<String>,
}

impl From<&Config> for application::Config {
    fn from(args: &Config) -> Self {
        Self {
            data_dir: args.data_dir.clone(),
            min_rows: args.min_rows,
            start_height: args.start_height,
            end_height: args.end_height,
            depth: args.depth,
            batch_size: args.batch_size,
            concurrency: args.concurrency,
            audit_enabled: args.near_key.is_some(),
            filter_blocks: args.filter_blocks,
            history_revision: args.history_revision,
            shard_rows: args.shard_rows,
            shard_target: args.shard_target,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        zakura_pir_publisher::receiver::session::validate_rows(self.min_rows)?;
        crate::seal::Policy::new(self.shard_rows, self.shard_target)
            .map_err(|_| "invalid shard rows or target")?;
        // Sealed segments are served at these rows; staged serving refuses fewer than
        // the advertised minimum.
        if self.shard_rows < self.min_rows {
            return Err("--shard-rows must be at least --min-rows".into());
        }
        let floor = receiver_pir_server::tables::MIN_BUDGET as u64 >> 20;
        if self.table_memory_mib < floor {
            return Err(format!(
                "the table memory budget must be at least {floor} MiB: a cold witness load \
                 holds the largest payment and witness tables together"
            )
            .into());
        }
        if !(60..=86_400).contains(&self.retention_seconds) {
            return Err("retention must be between one minute and one day".into());
        }
        if self.start_height != crate::blocks::ironwood_activation() {
            return Err("witness tables and serving need history from Ironwood activation".into());
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
            || self.filter_blocks.is_some_and(|blocks| {
                blocks == 0 || blocks > receiver_store::blocks::MAX_FILTER_BLOCKS
            })
            || self.history_revision > i64::MAX as u64
            || self.depth > 50
            || self.batch_size == 0
            || self.batch_size > MAX_RECEIVER_BATCH_BLOCKS
            || self.concurrency == 0
            || self.concurrency > MAX_RECEIVER_CONCURRENCY
            || (self.serve && self.end_height.is_some())
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
    let publications = Publications::new(
        Duration::from_secs(args.retention_seconds),
        usize::try_from(args.table_memory_mib << 20)?,
    );
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
        let reads = reads.clone();
        std::sync::Arc::new(move || reads.health())
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
    if let Some(key) = args.near_key.clone() {
        let path = args.data_dir.join(PROVIDERS);
        let since = args
            .near_since
            .unwrap_or_else(|| application::unix_now() - crate::near::AUDIT_LOOKBACK_SECS);
        let poll = Duration::from_secs(args.near_poll_seconds);
        let mut explorer = Explorer::new(key)?;
        let provider = tokio::spawn(async move {
            loop {
                crate::near::poll(&mut explorer, &path, since, &reads).await;
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

/// Default sealed segment rows, shared by JSON configuration and command-line parsing.
fn default_shard_rows() -> u32 {
    zakura_pir_publisher::receiver::session::MAX_ROWS
}

/// Default prepared table budget in MiB, shared by JSON configuration and command-line
/// parsing.
fn default_table_memory_mib() -> u64 {
    4096
}

/// Default inventory retention in seconds, shared by JSON configuration and command-line
/// parsing.
fn default_retention_seconds() -> u64 {
    600
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A budget too small for a cold witness load of the largest pair is refused, since
    /// once such a table was evicted its queries could never load it again; the pair
    /// itself is accepted.
    #[test]
    fn the_table_budget_must_hold_the_largest_pair() {
        let config = |mib: u64| {
            Config::try_parse_from([
                "receiver-directory",
                "--data-dir",
                "/srv/receiver",
                "--rpc-url",
                "http://127.0.0.1:8232",
                "--no-auth",
                "--table-memory-mib",
                &mib.to_string(),
            ])
            .unwrap()
        };
        assert_eq!(receiver_pir_server::tables::MIN_BUDGET, 1536 << 20);
        for mib in [256, 1024, 1535] {
            assert!(config(mib).validate().is_err(), "{mib}");
        }
        assert!(config(1536).validate().is_ok());
        assert!(config(4096).validate().is_ok());
    }

    /// Sealed segments are published at `--shard-rows`, so it may not be below the
    /// advertised `--min-rows`.
    #[test]
    fn shard_rows_must_reach_the_minimum() {
        let config = |shard: u32, min: u32| {
            Config::try_parse_from([
                "receiver-directory",
                "--data-dir",
                "/srv/receiver",
                "--rpc-url",
                "http://127.0.0.1:8232",
                "--no-auth",
                "--shard-rows",
                &shard.to_string(),
                "--min-rows",
                &min.to_string(),
            ])
            .unwrap()
        };
        assert!(config(8192, 16384).validate().is_err());
        assert!(config(16384, 16384).validate().is_ok());
        assert!(config(65536, 8192).validate().is_ok());
    }
}
