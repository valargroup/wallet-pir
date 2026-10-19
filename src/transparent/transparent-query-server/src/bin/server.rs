//! Private transparent shard retrieval service.

use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;
use transparent_shared::history::shardset::DEFAULT_RETAIN_REVISIONS;

#[derive(Parser)]
#[command(
    name = "transparent-query-server",
    about = "Private retrieval from a published transparent shard set"
)]
struct Cli {
    /// Verify a prepared recent publication from an authenticated HTTPS origin.
    /// Without --verify-only, follow publications and serve a recent worker.
    #[arg(long, requires_all = ["publisher_network", "publisher_genesis"])]
    publisher: Option<String>,
    /// Fixed trusted terminal Valargroup archive origin; never supplied by wallets.
    #[arg(long, requires = "publisher")]
    archive_upstream: Option<String>,
    /// Optional archive CA with ordinary TLS verification.
    #[arg(long, requires = "archive_upstream")]
    archive_ca: Option<PathBuf>,
    #[arg(long, requires = "publisher")]
    publisher_network: Option<String>,
    #[arg(long, requires = "publisher")]
    publisher_genesis: Option<String>,
    /// Optional private CA, with ordinary hostname and certificate validation.
    #[arg(long, requires = "publisher")]
    publisher_ca: Option<PathBuf>,
    #[arg(long, default_value_t = 64 << 30)]
    bootstrap_max_bytes: u64,
    /// Synchronize every history shard, without an archive forwarding dependency.
    #[arg(long, requires = "publisher", conflicts_with = "archive_upstream")]
    full_holdings: bool,
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
    /// Local disposable cache of prepared public runtimes; disabled if omitted.
    #[arg(long)]
    runtime_cache_dir: Option<PathBuf>,
    #[arg(long, requires = "runtime_cache_dir")]
    runtime_cache_max_bytes: Option<u64>,
    /// Streaming disk restores that may run at once, independently of builds.
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u16).range(1..=16))]
    runtime_restore_slots: u16,
    /// Prune only cache revisions absent from all named active/rollback sets.
    #[arg(long, requires = "runtime_cache_dir")]
    runtime_cache_prune_set: Vec<PathBuf>,
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
    /// Requests that may wait for a slot or a runtime beyond those running.
    ///
    /// A full queue refuses with 503 and a retry delay. The queue is a buffer
    /// against jitter, not a backlog: past this, a wallet is better told to
    /// retry than kept waiting behind the rest.
    #[arg(long, default_value_t = 64)]
    query_waiters: usize,
    /// Query body bytes that may be buffered at once across every waiting and
    /// running query.
    #[arg(long, default_value_t = 64 << 20)]
    body_bytes: u64,
    /// Seconds a client has to deliver a query body once admitted.
    #[arg(long, default_value_t = 10)]
    upload_deadline_secs: u64,
    /// Seconds a request may wait in total before it is refused retryably.
    #[arg(long, default_value_t = 30)]
    query_deadline_secs: u64,
    /// The fleet assignment this worker loads its subset from. With
    /// `--worker-id`. Without it the whole set is loaded and served.
    #[arg(long, requires = "worker_id")]
    assignment: Option<PathBuf>,
    /// This worker's id in the assignment.
    #[arg(long, requires = "assignment")]
    worker_id: Option<String>,
    /// Bytes of superseded revisions to keep per shard, on disk, beyond the
    /// current one. Unbounded by default; the count bound still applies.
    #[arg(long)]
    retain_bytes: Option<u64>,
    /// Report superseded revisions past the retention bound as prunable
    /// rather than refusing to start. The fleet's mode; the deploy prunes
    /// after activation with `shard-prune`.
    #[arg(long)]
    prune_excess: bool,
    /// Report ready as soon as the set is loaded, without prewarming. The
    /// correctness pilot's mode. Under an assignment the default is to
    /// prewarm every assigned runtime and report ready only then.
    #[arg(long)]
    pilot_cold: bool,
    /// Load and verify the set under these options, print what would be
    /// served, and exit without listening. The deploy runs this with the
    /// staged binary before it stops the running service.
    #[arg(long)]
    verify_only: bool,
    /// Root-only publication control socket. Omit to retain static serving.
    #[arg(long)]
    control_socket: Option<PathBuf>,
    /// Durable active publication record used on restart.
    #[arg(long, requires = "control_socket")]
    active_record: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli = Cli::parse();
    transparent_query_server::history::process::run(
        transparent_query_server::history::process::HistoryProcess {
            publisher: cli.publisher,
            archive_upstream: cli.archive_upstream,
            archive_ca: cli.archive_ca,
            publisher_network: cli.publisher_network,
            publisher_genesis: cli.publisher_genesis,
            publisher_ca: cli.publisher_ca,
            bootstrap_max_bytes: cli.bootstrap_max_bytes,
            full_holdings: cli.full_holdings,
            listen: cli.listen,
            shard_dir: cli.shard_dir,
            cache_bytes: cli.cache_bytes,
            runtime_cache_dir: cli.runtime_cache_dir,
            runtime_cache_max_bytes: cli.runtime_cache_max_bytes,
            runtime_restore_slots: cli.runtime_restore_slots,
            runtime_cache_prune_set: cli.runtime_cache_prune_set,
            build_slots: cli.build_slots,
            query_slots: cli.query_slots,
            retain_revisions: cli.retain_revisions,
            query_waiters: cli.query_waiters,
            body_bytes: cli.body_bytes,
            upload_deadline_secs: cli.upload_deadline_secs,
            query_deadline_secs: cli.query_deadline_secs,
            assignment: cli.assignment,
            worker_id: cli.worker_id,
            retain_bytes: cli.retain_bytes,
            prune_excess: cli.prune_excess,
            pilot_cold: cli.pilot_cold,
            verify_only: cli.verify_only,
            control_socket: cli.control_socket,
            active_record: cli.active_record,
        },
    )
    .await
}
