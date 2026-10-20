use clap::Parser;
use pir_pipeline::service::Shutdown;
#[derive(Parser)]
#[command(subcommand_negates_reqs = true, args_conflicts_with_subcommands = true)]
struct Args {
    #[arg(long, required = true)]
    config: Option<std::path::PathBuf>,
    /// Check static configuration without opening state, listeners, or credentials.
    #[arg(long, requires = "config")]
    check: bool,
    #[command(subcommand)]
    command: Option<Command>,
}
/// Remaining arguments are parsed by the owning product's maintenance command.
#[derive(clap::Args)]
#[command(disable_help_flag = true)]
struct Maintenance {
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<std::ffi::OsString>,
}
#[derive(clap::Subcommand)]
enum Command {
    #[cfg(feature = "enhance")]
    /// Enhance offline maintenance and build information.
    Enhance(Maintenance),
    #[cfg(feature = "status")]
    /// Status observations and encrypted-query diagnostics.
    Status(Maintenance),
    #[cfg(feature = "transparent-history")]
    /// Publish a history shard set once.
    PublishHistory(Maintenance),
    #[cfg(feature = "transparent-history")]
    /// Verify a staged filter publication without opening node state.
    VerifyFilters(Maintenance),
    #[cfg(feature = "transparent-detail")]
    /// Prepare, verify and benchmark transaction-detail publications offline.
    Detail(Maintenance),
    #[cfg(feature = "rocksdb-reader")]
    /// Backfill a Transparent event journal without starting a service.
    IngestEvents(Maintenance),
}
impl Command {
    async fn run(self) -> pir_publisher::Result<()> {
        match self {
            #[cfg(feature = "enhance")]
            Self::Enhance(args) => {
                enhance_publisher::commands::run(arguments("enhance", args)).await
            }
            #[cfg(feature = "status")]
            Self::Status(args) => status_publisher::commands::run(arguments("status", args)).await,
            #[cfg(feature = "transparent-history")]
            Self::VerifyFilters(args) => {
                transparent_publisher::commands::filters::run(arguments("verify-filters", args))
                    .await
            }
            #[cfg(feature = "transparent-history")]
            Self::PublishHistory(args) => {
                transparent_publisher::commands::history::run(arguments("publish-history", args))
                    .await
            }
            #[cfg(feature = "transparent-detail")]
            Self::Detail(args) => {
                transparent_publisher::commands::detail::run(arguments("detail", args)).await
            }
            #[cfg(feature = "rocksdb-reader")]
            Self::IngestEvents(args) => {
                transparent_publisher::commands::ingest::run(arguments("ingest-events", args)).await
            }
        }
    }
}
#[allow(dead_code)]
fn arguments(name: &str, args: Maintenance) -> Vec<std::ffi::OsString> {
    std::iter::once(format!("pir-publisher {name}").into())
        .chain(args.args)
        .collect()
}
#[tokio::main]
async fn main() -> pir_publisher::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    if let Some(command) = args.command {
        return command.run().await;
    }
    let config_path = args.config.ok_or("publisher requires --config FILE")?;
    let config: pir_publisher::config::Config =
        serde_json::from_slice(&std::fs::read(config_path)?)?;
    config.validate()?;
    if args.check {
        println!("Publisher configuration is valid; runtime readiness was not checked");
        return Ok(());
    }
    let stop = Shutdown::default();
    let signals = stop.clone();
    let mut tasks = pir_pipeline::service::Tasks::default();
    let signal = tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! { result = tokio::signal::ctrl_c() => result?, _ = term.recv() => {} }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await?;
        }
        signals.request();
        Ok::<_, std::io::Error>(())
    });
    tasks.own(&signal);
    pir_publisher::run(config, stop).await
}
