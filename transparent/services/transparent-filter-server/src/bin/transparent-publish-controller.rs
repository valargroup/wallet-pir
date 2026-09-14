use clap::Parser;
#[derive(Parser)]
struct Cli {
    #[arg(long)]
    config: std::path::PathBuf,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    transparent_filter_server::controller::run(serde_json::from_slice(&std::fs::read(cli.config)?)?)
        .await
}
