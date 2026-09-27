//! One-shot shard publication; continuous operation uses the same builder.
use clap::Parser;
use transparent_filter::BlockHash;
use transparent_filter_server::{
    events::EventStore,
    publication::{publish, BoxError, PublishOptions},
    zakura::ZakuraClient,
};
#[derive(Parser)]
#[command(name = "shard-publish", about = "Publish a transparent shard set")]
struct Cli {
    #[command(flatten)]
    options: PublishOptions,
    /// Display hash of the block before the journal's first height, instead
    /// of asking the node for it.
    ///
    /// For publishing a journal slice offline, where the one thing the node
    /// is needed for is already known. Ignored for a journal starting at zero.
    #[arg(long)]
    parent_block_hash: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let Cli {
        options: cli,
        parent_block_hash,
    } = Cli::parse();
    // A standalone build has no controller snapshot. Refuse concurrent ingest
    // rather than reading through a truncation/reorg under an old checkpoint.
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(cli.data_dir.join("writer.lock"))?;
    lock.try_lock_shared().map_err(|e| {
        format!("journal is being ingested; pause its writer or use controller publication: {e}")
    })?;
    let store = EventStore::open_existing(&cli.data_dir)?;
    let parent = if store.start_height() == 0 {
        BlockHash::from_internal_bytes([0; 32])
    } else if let Some(parent) = parent_block_hash {
        BlockHash::from_display_hex(&parent)?
    } else {
        let rpc = ZakuraClient::from_cookie_file(&cli.zakura_rpc_url, &cli.zakura_cookie)?;
        BlockHash::from_display_hex(&rpc.block_hash(store.start_height() - 1).await?)?
    };
    publish(&cli, &store, parent)?;
    Ok(())
}
