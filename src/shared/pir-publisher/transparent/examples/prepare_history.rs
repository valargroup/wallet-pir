//! Offline preparation for the fixed-range synchronization comparison.
use clap::Parser;
use std::path::PathBuf;
use transparent_shared::{history::shardset::ShardSet, publication::Finalized, runtime::prebuild};

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    publication: PathBuf,
    #[arg(long)]
    recent_from: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();
    let set = ShardSet::open(&cli.publication, 0)?;
    let prepared = prebuild::prepare_publication(&set, &cli.publication, &[], &|| false)?;
    if !prepared.failures.is_empty() {
        return Err(format!("incomplete prepared publication: {:?}", prepared.failures).into());
    }
    let finalized = Finalized::prepare_full(&cli.publication, &set, cli.recent_from)?;
    std::fs::write(
        cli.publication.join("prepared-inventory.json"),
        finalized.bytes(),
    )?;
    println!(
        "{}",
        serde_json::json!({"preparation":prepared.report(),
        "map_sha256":set.map_digest,"inventory_sha256":finalized.sha256(),
        "artifact_bytes":finalized.inventory().artifacts.iter().map(|a| a.bytes).sum::<u64>()})
    );
    Ok(())
}
