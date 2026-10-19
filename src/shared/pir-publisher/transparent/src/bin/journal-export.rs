//! Copy committed history without acquiring or changing its writer's state.
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    source: PathBuf,
    #[arg(long)]
    destination: PathBuf,
    #[arg(long)]
    start_height: u64,
    #[arg(long)]
    end_height: u64,
    /// Refuse blocks without a sidecar when the source contract requires one.
    #[arg(long)]
    require_sidecars: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();
    let report = transparent_publisher::ingest::journal_export::export(
        &cli.source,
        &cli.destination,
        cli.start_height,
        cli.end_height,
        cli.require_sidecars,
    )?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
