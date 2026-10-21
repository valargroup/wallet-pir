//! Completed-publication evidence for a pinned History publication, prepared
//! beside a running publisher: no journal writer, listener or other product.
use crate::history::{controller::Config, publication::write_atomic};
use clap::Parser;
use std::path::PathBuf;
use transparent_shared::{
    history::shardset::{ShardSet, DEFAULT_RETAIN_REVISIONS},
    publication::Finalized,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "pir-publisher prepare-history-restore",
    about = "Fully verify a completed History publication and write its restore record"
)]
struct Cli {
    /// Publisher configuration; only its History settings are read.
    #[arg(long)]
    config: PathBuf,
    /// The pinned publication directory to verify.
    #[arg(long)]
    publication: PathBuf,
    /// Where to write the restore record.
    #[arg(long)]
    output: PathBuf,
}

pub async fn run(args: Vec<std::ffi::OsString>) -> Result<(), BoxError> {
    let cli = Cli::parse_from(args);
    let raw: serde_json::Value = serde_json::from_slice(&std::fs::read(&cli.config)?)?;
    let settings = raw
        .get("transparent_history")
        .and_then(|product| product.get("settings"))
        .ok_or("configuration has no transparent_history settings")?;
    let config: Config = serde_json::from_value(settings.clone())?;
    config.validate()?;
    if !config.distribute_prepared {
        return Err("restore records describe prepared distributions only".into());
    }
    tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
        let started = std::time::Instant::now();
        // The same checks a new publication passes before it is offered:
        // every table against its manifest, every artifact hashed and every
        // runtime loaded and checked against its rows.
        let set = ShardSet::open(&cli.publication, DEFAULT_RETAIN_REVISIONS)?;
        let finalized = if config.http_only {
            Finalized::prepare_full(&cli.publication, &set, config.recent_from)?
        } else {
            Finalized::prepare(&cli.publication, &set, config.recent_from)?
        };
        let end = set.map.shards.last().ok_or("empty publication")?;
        let record = finalized.restore_record(
            config.http_only,
            config.recent_from,
            end.end_height,
            &end.terminal_block_hash,
        )?;
        write_atomic(&cli.output, &serde_json::to_vec(&record)?)?;
        println!(
            "{}",
            serde_json::json!({
                "inventory_sha256": record.inventory_sha256,
                "map_sha256": record.map_sha256,
                "anchor_height": record.anchor_height,
                "anchor_hash": record.anchor_hash,
                "seconds": started.elapsed().as_secs_f64(),
            })
        );
        Ok(())
    })
    .await?
}
