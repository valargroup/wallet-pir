use clap::Parser;
use pir_pipeline::service::Shutdown;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: std::path::PathBuf,
    /// Check static configuration without opening state, listeners, or credentials.
    #[arg(long)]
    check: bool,
}

#[tokio::main]
async fn main() -> pir_query_server::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let config: pir_query_server::config::Config =
        serde_json::from_slice(&std::fs::read(args.config)?)?;
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
