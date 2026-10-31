//! Receiver preparation settings and one-shot publication maintenance.
use crate::{
    application,
    blocks::{MAX_RECEIVER_BATCH_BLOCKS, MAX_RECEIVER_CONCURRENCY},
    canonical::freshest,
};
use clap::Parser;
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};
use zakura_server_chain::{Network, NodeClient};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Parser, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[arg(long)]
    pub data_dir: PathBuf,
    /// A node's RPC endpoint. Repeat it for more nodes: each pass runs on the mainnet
    /// one with the highest tip (see [`NodeClient::ranked`]).
    #[arg(long, required = true)]
    pub rpc_url: Vec<String>,
    #[arg(long, required_unless_present = "no_auth", conflicts_with = "no_auth")]
    pub cookie: Option<PathBuf>,
    #[arg(long)]
    #[serde(default)]
    pub no_auth: bool,
    /// Must be Ironwood activation: companion payloads need every commitment of each
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
    /// Managed preparation applies continuous-service validation internally.
    /// This is not a command-line service mode.
    #[arg(skip)]
    #[serde(skip)]
    pub serve: bool,
    /// The service's listener: loopback, or a private IPv4 or unique-local IPv6 address
    /// that only the TLS proxy and monitoring hosts on the private network can reach.
    #[arg(skip = SocketAddr::from(([127, 0, 0, 1], 18380)))]
    pub bind: SocketAddr,
    #[arg(skip = 10u64)]
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
    /// paths. It must hold the largest directory beside the largest companion's
    /// preparation (`receiver_pir_server::tables::MIN_BUDGET`).
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
            start_height: args.start_height,
            end_height: args.end_height,
            depth: args.depth,
            batch_size: args.batch_size,
            concurrency: args.concurrency,
            audit_enabled: args.near_key.is_some(),
            filter_blocks: args.filter_blocks,
            history_revision: args.history_revision,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        let floor = receiver_pir_server::tables::MIN_BUDGET as u64 >> 20;
        if self.table_memory_mib < floor {
            return Err(format!(
                "the table memory budget must be at least {floor} MiB: a cold companion \
                 load prepares beside the largest directory"
            )
            .into());
        }
        if !(60..=86_400).contains(&self.retention_seconds) {
            return Err("retention must be between one minute and one day".into());
        }
        if self.start_height != crate::blocks::ironwood_activation() {
            return Err(
                "companion payloads and serving need history from Ironwood activation".into(),
            );
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
/// Prepare one publication without starting a polling loop or HTTP service.
pub async fn publish(mut args: Config) -> Result<()> {
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
    let genesis = Network::Mainnet.genesis_hash();
    freshest(&nodes, genesis).await?;
    let config = application::Config::from(&args);
    application::refresh(
        &config,
        &nodes,
        genesis,
        None,
        &mut application::State::default(),
    )
    .await?;
    Ok(())
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

    /// A budget too small to prepare the largest companion beside the largest directory
    /// is refused, since once such a table was evicted its queries could never load it
    /// again; that budget itself is accepted.
    #[test]
    fn the_table_budget_must_hold_the_largest_preparation() {
        let config = |extra: &[&str]| {
            let base = [
                "pir-publisher receiver publish",
                "--data-dir",
                "/srv/receiver",
                "--rpc-url",
                "http://127.0.0.1:8232",
                "--no-auth",
            ];
            Config::try_parse_from(base.iter().chain(extra))
        };
        let floor = receiver_pir_server::tables::MIN_BUDGET >> 20;
        assert_eq!(floor, 920);
        for mib in [256, floor - 1] {
            let mib = mib.to_string();
            assert!(config(&["--table-memory-mib", &mib])
                .unwrap()
                .validate()
                .is_err());
        }
        for mib in [floor, 4096] {
            let mib = mib.to_string();
            assert!(config(&["--table-memory-mib", &mib])
                .unwrap()
                .validate()
                .is_ok());
        }
    }
}
