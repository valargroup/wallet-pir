//! One-shot shard publication; continuous operation uses the same builder.
use clap::Parser;
use transparent_filter::BlockHash;
use transparent_filter_server::{
    events::EventStore,
    publication::{publish, BoxError, PublishOptions},
    zakura::ZakuraClient,
};
#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cli = PublishOptions::parse();
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
    } else {
        let rpc = ZakuraClient::from_cookie_file(&cli.zakura_rpc_url, &cli.zakura_cookie)?;
        BlockHash::from_display_hex(&rpc.block_hash(store.start_height() - 1).await?)?
    };
    publish(&cli, &store, parent)?;
    Ok(())
}
