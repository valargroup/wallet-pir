//! One-shot Receiver maintenance; continuous ingestion belongs to pir-publisher.
use clap::Parser;
#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(clap::Subcommand)]
enum Command {
    /// Prepare a publication once, resuming the existing Receiver store.
    Publish(crate::service::Config),
}
pub async fn run(
    args: impl IntoIterator<Item = std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match Args::parse_from(args).command {
        Command::Publish(config) => crate::service::publish(config).await,
    }
}
