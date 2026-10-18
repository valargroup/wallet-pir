//! One-shot shard publication; continuous operation uses the same builder.
use clap::Parser;
use transparent_filter::BlockHash;
use transparent_ingest::{
    events::EventStore,
    publication::{publish, BoxError, PublishOptions},
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
    /// Read a journal another process is writing, without its lock.
    ///
    /// For building a publication beside a live writer, such as the first v12
    /// set from the journal v11's controller ingests. Nothing is written
    /// beside the journal. Give `--through` a margin below the tip: a rollback
    /// rewrites the suffix it drops, so the publish refuses its result unless
    /// the block at `--through` is still committed when it finishes.
    #[arg(long, requires = "through")]
    follow_journal: bool,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let Cli {
        options: cli,
        parent_block_hash,
        follow_journal,
    } = Cli::parse();
    // A standalone build has no controller snapshot. Unless told to follow a
    // live writer, refuse concurrent ingest rather than reading through a
    // truncation/reorg under an old checkpoint.
    let _lock = if follow_journal {
        None
    } else {
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(cli.data_dir.join("writer.lock"))?;
        lock.try_lock_shared().map_err(|e| {
            format!(
                "journal is being ingested; pause its writer, use controller publication \
                 or pass --follow-journal: {e}"
            )
        })?;
        Some(lock)
    };
    let store = EventStore::open_existing(&cli.data_dir)?;
    let parent = if store.start_height() == 0 {
        BlockHash::from_internal_bytes([0; 32])
    } else if let Some(parent) = parent_block_hash {
        BlockHash::from_display_hex(&parent)?
    } else {
        let rpc = transparent_ingest::zakura::connect(&cli.zakura_rpc_url, &cli.zakura_cookie)?;
        BlockHash::from_internal_bytes(rpc.block_hash(store.start_height() - 1).await?)
    };
    let endpoint = cli
        .through
        .and_then(|through| store.block_at(through).map(|b| (through, b)));
    publish(&cli, &store, parent)?;
    if follow_journal {
        let (through, entry) = endpoint.ok_or("--through is not committed in the journal")?;
        if !store.still_committed(through, entry)? {
            return Err(format!(
                "the journal's writer replaced block {through} while it was published; \
                 the publication is not of the chain the journal now holds"
            )
            .into());
        }
    }
    Ok(())
}
