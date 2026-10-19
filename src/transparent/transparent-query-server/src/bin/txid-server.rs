//! Private retrieval from a tiered, bucketed txid display publication.

use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;
use transparent_query_server::tx_detail::process::role_from_text;
use transparent_shared::history::shardset::DEFAULT_RETAIN_REVISIONS;
use transparent_shared::tx_detail::DisplayRole;

#[derive(Parser)]
#[command(
    name = "transparent-txid-server",
    about = "Private retrieval from a tiered txid display publication"
)]
struct Cli {
    /// Full-coverage managed synchronization from a trusted HTTPS publisher.
    #[arg(long, requires_all = ["state_dir", "publisher_network", "publisher_genesis"], conflicts_with_all = ["publication_dir", "control_socket", "runtime_cache_dir", "loaded_only"])]
    publisher: Option<String>,
    #[arg(long, requires = "publisher")]
    state_dir: Option<PathBuf>,
    #[arg(long, requires = "publisher")]
    publisher_network: Option<String>,
    #[arg(long, requires = "publisher")]
    publisher_genesis: Option<String>,
    #[arg(long, requires = "publisher")]
    publisher_ca: Option<PathBuf>,
    #[arg(long, default_value_t = 64 << 30)]
    bootstrap_max_bytes: u64,

    /// Loopback by default; a worker binds its private address.
    #[arg(long, default_value = "127.0.0.1:8095")]
    listen: SocketAddr,
    /// `archive-owner` serves sealed shards, `recent-replica` the recent one,
    /// `combined` both.
    #[arg(long, value_parser = role)]
    role: DisplayRole,
    /// A publication directory to serve at start. With a control socket, the
    /// active record takes precedence; without either, the worker waits for
    /// its first prepare and activation.
    #[arg(long)]
    publication_dir: Option<PathBuf>,
    /// Bytes of prepared runtime the cache may hold. Staged revisions count.
    #[arg(long, default_value_t = 4 << 30)]
    cache_bytes: u64,
    /// Local disposable cache of prepared public runtimes; disabled if omitted.
    #[arg(long)]
    runtime_cache_dir: Option<PathBuf>,
    #[arg(long, requires = "runtime_cache_dir")]
    runtime_cache_max_bytes: Option<u64>,
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u16).range(1..=16))]
    runtime_restore_slots: u16,
    #[arg(long, default_value_t = 1)]
    build_slots: usize,
    #[arg(long, default_value_t = 2)]
    query_slots: usize,
    /// Superseded recent revisions on disk to keep serving.
    #[arg(long, default_value_t = DEFAULT_RETAIN_REVISIONS)]
    retain_revisions: usize,
    #[arg(long, default_value_t = 64)]
    query_waiters: usize,
    #[arg(long, default_value_t = 64 << 20)]
    body_bytes: u64,
    #[arg(long, default_value_t = 10)]
    upload_deadline_secs: u64,
    #[arg(long, default_value_t = 30)]
    query_deadline_secs: u64,
    /// Report ready once loaded rather than once the role's runtimes are warm.
    #[arg(long)]
    loaded_only: bool,
    /// Load and verify the publication, print what would be served, and exit.
    #[arg(long)]
    verify_only: bool,
    /// Root-only control socket; see `txid-control`.
    #[arg(long, requires = "active_record")]
    control_socket: Option<PathBuf>,
    /// Durable record of the active publication, used on restart.
    #[arg(long, requires = "control_socket")]
    active_record: Option<PathBuf>,
    /// Directory whose unused shipped candidates and staged revisions
    /// `collect` may delete; repeat it when candidates and staged revisions
    /// are shipped to separate roots. Omit a root shared with the controller
    /// or another worker: nothing on disk is then removed.
    #[arg(long, requires = "control_socket")]
    collect_root: Vec<PathBuf>,
}

fn role(text: &str) -> Result<DisplayRole, String> {
    role_from_text(text)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli = Cli::parse();
    transparent_query_server::tx_detail::process::run(
        transparent_query_server::tx_detail::process::DetailProcess {
            publisher: cli.publisher,
            state_dir: cli.state_dir,
            publisher_network: cli.publisher_network,
            publisher_genesis: cli.publisher_genesis,
            publisher_ca: cli.publisher_ca,
            bootstrap_max_bytes: cli.bootstrap_max_bytes,
            listen: cli.listen,
            role: cli.role,
            publication_dir: cli.publication_dir,
            cache_bytes: cli.cache_bytes,
            runtime_cache_dir: cli.runtime_cache_dir,
            runtime_cache_max_bytes: cli.runtime_cache_max_bytes,
            runtime_restore_slots: cli.runtime_restore_slots,
            build_slots: cli.build_slots,
            query_slots: cli.query_slots,
            retain_revisions: cli.retain_revisions,
            query_waiters: cli.query_waiters,
            body_bytes: cli.body_bytes,
            upload_deadline_secs: cli.upload_deadline_secs,
            query_deadline_secs: cli.query_deadline_secs,
            loaded_only: cli.loaded_only,
            verify_only: cli.verify_only,
            control_socket: cli.control_socket,
            active_record: cli.active_record,
            collect_root: cli.collect_root,
        },
    )
    .await
}
