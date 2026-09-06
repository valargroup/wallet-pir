//! Transparent event journal backfill.
//!
//! Builds the event journal that a shard builder consumes, using exactly the
//! extraction and previous-output resolution the deployed filter service uses —
//! `ingest::build_block_events`, which `build_block_filter` is itself a
//! projection of. Reusing that path rather than writing a second collector is
//! the point: two independent extractions would eventually disagree, and the
//! disagreement would show up as a wallet that cannot find its own money.
//!
//! This is a tool, not a service. It has no listener, serves nothing, and holds
//! no lock any other process wants. It reads Zakura over its loopback RPC and
//! writes one directory of its own, so it can run beside a live filter service
//! without touching that service's data or its coverage.
//!
//! Interrupting it is safe and expected. Coverage advances only at a commit, so
//! a kill loses at most the blocks since the last one, and rerunning the same
//! command resumes from the last committed height.

use clap::Parser;
use std::path::PathBuf;
use std::time::Instant;
use tracing_subscriber::EnvFilter;
use transparent_filter_server::events::EventStore;
use transparent_filter_server::ingest::{build_block_events, BoxError};
use transparent_filter_server::prevout::OutputCache;
use transparent_filter_server::zakura::ZakuraClient;

#[derive(Parser, Clone)]
#[command(
    name = "transparent-event-ingest",
    about = "Backfill the transparent event journal from a Zakura archive node"
)]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8232")]
    zakura_rpc_url: String,
    #[arg(long)]
    zakura_cookie: PathBuf,
    /// Its own directory. Must not be the filter service's data directory.
    #[arg(long, default_value = "./transparent-event-data")]
    data_dir: PathBuf,
    /// First height to index. Defaults to Ironwood activation.
    #[arg(long, default_value_t = transparent_filter::START_HEIGHT)]
    start_height: u64,
    /// Last height to index. Defaults to the node's tip when the run begins.
    ///
    /// The tip is read once and then fixed. A backfill that chased a moving tip
    /// would never report itself finished, and this tool is meant to terminate.
    #[arg(long)]
    stop_height: Option<u64>,
    /// Transactions retained in the previous-output cache.
    ///
    /// The dominant cost of a backfill is resolving previous outputs, and most
    /// of them were created within a few thousand blocks. A larger cache trades
    /// resident memory for RPC round trips.
    #[arg(long, default_value_t = transparent_filter_server::prevout::DEFAULT_CACHE_TRANSACTIONS)]
    cache_transactions: usize,
    /// Blocks between durable checkpoints.
    #[arg(long, default_value_t = 1_000)]
    commit_every: u64,
    /// Blocks between progress lines.
    #[arg(long, default_value_t = 1_000)]
    log_every: u64,
}

/// Rolls stored coverage back to the highest block the node still agrees with.
///
/// A backfill of sealed history should never need this, but a run interrupted
/// near the tip can resume across a reorg. Walking back one block at a time is
/// what makes that a rollback rather than a silently divergent journal.
async fn reconcile(
    zakura: &ZakuraClient,
    store: &mut EventStore,
    tip: u64,
) -> Result<(), BoxError> {
    loop {
        let Some(covered) = store.covered_through() else {
            return Ok(());
        };
        if covered > tip {
            store.rollback_to(if covered == store.start_height() {
                None
            } else {
                Some(tip)
            })?;
            continue;
        }
        let stored = store
            .block_at(covered)
            .ok_or("covered height has no stored block")?;
        if zakura.block_hash(covered).await? == stored.block_hash.to_display_hex() {
            return Ok(());
        }
        tracing::warn!(
            height = covered,
            "stored block is not on the node's chain; rewinding"
        );
        if covered == store.start_height() {
            store.rollback_to(None)?;
            return Ok(());
        }
        store.rollback_to(Some(covered - 1))?;
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli = Cli::parse();

    let zakura = ZakuraClient::from_cookie_file(&cli.zakura_rpc_url, &cli.zakura_cookie)?;
    // Chain identity comes from the node, once, and is then pinned in the
    // store: a journal built against one chain must never be read as another's.
    let genesis_hash = zakura.genesis_hash().await?;
    if genesis_hash != transparent_filter::MAINNET_GENESIS_DISPLAY {
        tracing::warn!(
            %genesis_hash,
            expected = transparent_filter::MAINNET_GENESIS_DISPLAY,
            "node is not on Zcash mainnet; the journal will be pinned to this chain"
        );
    }

    let mut store = EventStore::open(&cli.data_dir, &genesis_hash, cli.start_height)?;
    let node_tip = zakura.tip_height().await?;
    let stop = cli.stop_height.unwrap_or(node_tip);
    if stop > node_tip {
        return Err(format!("stop height {stop} is above the node's tip {node_tip}").into());
    }
    if stop < cli.start_height {
        return Err(format!(
            "stop height {stop} is below the start height {}",
            cli.start_height
        )
        .into());
    }

    reconcile(&zakura, &mut store, stop).await?;

    let mut cache = OutputCache::new(cli.cache_transactions);
    let started = Instant::now();
    let first = store.next_height();
    let mut rpc_lookups = 0u64;
    let mut cache_hits = 0u64;

    tracing::info!(
        data_dir = %cli.data_dir.display(),
        from = first,
        through = stop,
        remaining = stop.saturating_sub(first).saturating_add(1),
        events_stored = store.events_stored(),
        "starting event backfill"
    );

    let mut height = first;
    while height <= stop {
        let built = build_block_events(&zakura, &mut cache, height).await?;
        rpc_lookups += built.rpc_lookups;
        cache_hits += built.cache_hits;
        store.append_block(height, built.block_hash, &built.events)?;

        if height % cli.commit_every == 0 {
            store.commit()?;
        }
        if height % cli.log_every == 0 {
            let done = height - first + 1;
            let remaining = stop - height;
            let per_block = started.elapsed().as_secs_f64() / done as f64;
            tracing::info!(
                height,
                through = stop,
                events = store.events_stored(),
                blocks_per_second = format!("{:.1}", 1.0 / per_block.max(f64::EPSILON)),
                eta_minutes = format!("{:.1}", per_block * remaining as f64 / 60.0),
                rpc_lookups,
                cache_hits,
                "backfill progress"
            );
        }
        height += 1;
    }

    // The final commit is what makes the last partial batch durable. Without
    // it a run that finished cleanly would still resume from the last periodic
    // checkpoint.
    store.commit()?;
    tracing::info!(
        covered_through = ?store.covered_through(),
        blocks = store.blocks_covered(),
        events = store.events_stored(),
        elapsed_seconds = format!("{:.0}", started.elapsed().as_secs_f64()),
        rpc_lookups,
        cache_hits,
        "event backfill complete"
    );
    Ok(())
}
