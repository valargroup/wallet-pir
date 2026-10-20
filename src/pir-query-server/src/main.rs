use clap::Parser;
#[cfg(feature = "enhance")]
use clap::Subcommand;
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
    #[cfg(feature = "enhance")]
    #[command(subcommand)]
    command: Option<EnhanceCommand>,
}

#[cfg(feature = "enhance")]
#[derive(Subcommand)]
enum EnhanceCommand {
    /// Check the selected evaluator and GPU driver without opening worker state.
    CheckBackend {
        #[command(flatten)]
        matvec: enhance_query_server::matvec::MatvecConfig,
    },
    /// Offline repair of journal-referenced rows from a peer row archive.
    RepairRows {
        #[arg(long)]
        data_dir: std::path::PathBuf,
        #[arg(long)]
        source_rows: std::path::PathBuf,
    },
}

#[cfg(feature = "enhance")]
fn enhance_command(command: EnhanceCommand) -> pir_query_server::Result<()> {
    match command {
        EnhanceCommand::CheckBackend { matvec } => {
            matvec.validate()?;
            Ok(())
        }
        EnhanceCommand::RepairRows {
            data_dir,
            source_rows,
        } => {
            let restored =
                enhance_query_server::worker::Worker::repair_rows(&data_dir, &source_rows)?;
            println!(
                "{}",
                serde_json::json!({"status":"rows_verified", "restored_units":restored,
                "readiness":"requires_worker_restart", "qualification":"unqualified"})
            );
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() -> pir_query_server::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    #[cfg(feature = "enhance")]
    if let Some(command) = args.command {
        return enhance_command(command);
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
