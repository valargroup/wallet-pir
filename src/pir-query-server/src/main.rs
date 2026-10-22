use clap::{Parser, Subcommand};
use pir_pipeline::service::Shutdown;

#[derive(Parser)]
#[command(subcommand_negates_reqs = true, args_conflicts_with_subcommands = true)]
struct Args {
    /// Role configuration. Starts one process for the roles in this file.
    #[arg(long, required = true)]
    config: Option<std::path::PathBuf>,
    /// Check static configuration without opening state, listeners, or credentials.
    #[arg(long, requires = "config")]
    check: bool,
    /// Verify the one enabled Transparent history or transaction-detail
    /// section through its one-shot path, print the report, and exit without
    /// binding management or wallet listeners.
    #[arg(long, requires = "config", conflicts_with = "check")]
    verify_only: bool,
    #[command(subcommand)]
    command: Option<MaintenanceCommand>,
}

#[derive(Subcommand)]
enum MaintenanceCommand {
    /// Hash exact file bytes with the shared artifact checksum policy.
    ArtifactChecksum {
        #[arg(long)]
        input: std::path::PathBuf,
    },
    #[cfg(feature = "enhance")]
    /// Check the selected evaluator and GPU driver without opening worker state.
    CheckBackend {
        #[command(flatten)]
        matvec: enhance_query_server::matvec::MatvecConfig,
    },
}

fn maintenance_command(command: MaintenanceCommand) -> pir_query_server::Result<()> {
    match command {
        MaintenanceCommand::ArtifactChecksum { input } => {
            let checksum = pir_query_server::hash_artifact(&input)?;
            println!("{}", serde_json::to_string(&checksum)?);
            Ok(())
        }
        #[cfg(feature = "enhance")]
        MaintenanceCommand::CheckBackend { matvec } => {
            matvec.validate()?;
            Ok(())
        }
    }
}

fn main() -> pir_query_server::Result<()> {
    let mut args = Args::parse();
    if let Some(command) = args.command.take() {
        return maintenance_command(command);
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(args))
}

async fn run(args: Args) -> pir_query_server::Result<()> {
    if args.verify_only {
        // Stdout carries only the verification report; diagnostics go to
        // stderr at the level the Transparent verifiers have always logged.
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info".into()),
            )
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    }
    let config_path = args.config.ok_or("query server requires --config FILE")?;
    let config: pir_query_server::config::Config =
        serde_json::from_slice(&std::fs::read(config_path)?)?;
    if args.verify_only {
        return pir_query_server::verify(config).await;
    }
    config.validate()?;
    if args.check {
        println!("Query server configuration is valid; runtime readiness was not checked");
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
    pir_query_server::run(config, stop).await
}
