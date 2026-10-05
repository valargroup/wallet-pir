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
use transparent_filter_server::ingest::{build_fetched_block_events, BoxError};
use transparent_filter_server::prevout::OutputCache;
use transparent_filter_server::state::StateReader;
use transparent_filter_server::zakura::ZakuraClient;

#[derive(Parser, Clone)]
#[command(
    name = "transparent-event-ingest",
    about = "Backfill the transparent event journal from a Zakura archive node"
)]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8232")]
    zakura_rpc_url: String,
    /// Required unless `--state-dir` is given.
    #[arg(long)]
    zakura_cookie: Option<PathBuf>,
    /// The node's `[state] cache_dir`. Reads the chain from the node's own
    /// database instead of its RPC, and needs no cookie.
    ///
    /// This is the fast path and the one to use. Resolving a previous output
    /// becomes two local RocksDB lookups instead of a `getrawtransaction`, so
    /// the output cache, its batching pre-pass and the pipelined fetch all stop
    /// being needed — and blocks stop depending on each other, which is what
    /// makes `--workers` possible.
    ///
    /// It opens a RocksDB *secondary* instance, so a running node is fine. The
    /// binary must be built against the revision that node was built from.
    #[arg(long)]
    state_dir: Option<PathBuf>,
    /// Blocks extracted at once. `--state-dir` only.
    ///
    /// Held well below the host's core count by default: the coordinator serves
    /// live PIR queries under `CPUWeight=20`, and a backfill that saturates it
    /// would be paid for by every wallet talking to the box.
    #[arg(long, default_value_t = 4)]
    workers: usize,
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
    /// Outputs retained in the previous-output cache.
    ///
    /// The dominant cost of a backfill is resolving previous outputs, and most
    /// of them were created within a few thousand blocks. A larger cache trades
    /// resident memory for RPC round trips.
    ///
    /// Counted in outputs, not transactions: an early-chain mining-pool
    /// coinbase carries thousands of outputs, so a transaction count does not
    /// bound what the cache holds.
    #[arg(long, default_value_t = transparent_filter_server::prevout::DEFAULT_CACHE_OUTPUTS)]
    cache_outputs: usize,
    /// Blocks between durable checkpoints.
    #[arg(long, default_value_t = 1_000)]
    commit_every: u64,
    /// Blocks between progress lines.
    #[arg(long, default_value_t = 1_000)]
    log_every: u64,
    /// Write complete txid display sidecars before each block checkpoint.
    #[arg(long)]
    txid_display: bool,
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

/// Rolls coverage back to the highest block the node's database still agrees
/// with. The state-backed twin of [`reconcile`].
fn reconcile_state(reader: &StateReader, store: &mut EventStore, tip: u64) -> Result<(), BoxError> {
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
        if reader.block_hash(covered)? == stored.block_hash.to_display_hex() {
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

/// The backfill, reading the node's database directly.
///
/// The shape differs from the RPC path in one way that matters: every height's
/// work is independent, because nothing carries an output cache from one block
/// to the next. So heights are extracted `--workers` at a time and appended in
/// order, and the journal is byte-identical to what a serial run would write.
async fn run_state_backfill(cli: Cli, state_dir: PathBuf) -> Result<(), BoxError> {
    let opened = state_dir.clone();
    let reader = tokio::task::spawn_blocking(move || {
        StateReader::open(&opened, zakura_chain::parameters::Network::Mainnet)
    })
    .await??;

    // Chain identity from the database itself, so this path needs no RPC at
    // all — not even to learn which chain it is reading.
    let genesis_hash = reader.block_hash(0)?;
    if genesis_hash != transparent_filter::MAINNET_GENESIS_DISPLAY {
        tracing::warn!(
            %genesis_hash,
            expected = transparent_filter::MAINNET_GENESIS_DISPLAY,
            "node is not on Zcash mainnet; the journal will be pinned to this chain"
        );
    }

    let mut store = EventStore::open(&cli.data_dir, &genesis_hash, cli.start_height)?;
    // The secondary sees only what the primary has flushed, so this tip trails
    // the node's by a little. That is the right bound to stop at: a height the
    // secondary cannot see yet is one this run cannot read.
    let node_tip = reader.tip_height()?;
    let stop = cli.stop_height.unwrap_or(node_tip);
    if stop > node_tip {
        return Err(
            format!("stop height {stop} is above the visible finalized tip {node_tip}").into(),
        );
    }
    if stop < cli.start_height {
        return Err(format!(
            "stop height {stop} is below the start height {}",
            cli.start_height
        )
        .into());
    }

    reconcile_state(&reader, &mut store, stop)?;

    let reader = std::sync::Arc::new(reader);
    let started = Instant::now();
    let first = store.next_height();
    let workers = cli.workers.max(1);
    let mut lookups = 0u64;

    tracing::info!(
        data_dir = %cli.data_dir.display(),
        state_dir = %state_dir.display(),
        from = first,
        through = stop,
        remaining = stop.saturating_sub(first).saturating_add(1),
        events_stored = store.events_stored(),
        workers,
        "starting event backfill from the node's state database"
    );

    let mut inflight: std::collections::VecDeque<
        tokio::task::JoinHandle<
            Result<(u64, transparent_filter_server::ingest::BuiltEvents), BoxError>,
        >,
    > = std::collections::VecDeque::new();
    let mut to_spawn = first;
    let mut height = first;
    while height <= stop {
        while inflight.len() < workers && to_spawn <= stop {
            let reader = reader.clone();
            let at = to_spawn;
            inflight.push_back(tokio::task::spawn_blocking(move || {
                reader.block_events(at).map(|events| (at, events))
            }));
            to_spawn += 1;
        }
        let (at, built) = inflight
            .pop_front()
            .expect("a task is in flight for every height in range")
            .await??;
        if at != height {
            return Err(format!("extracted height {at} out of order at {height}").into());
        }
        lookups += built.rpc_lookups;
        if cli.txid_display {
            store.append_block_with_display(
                height,
                built.block_hash,
                &built.events,
                &built.display,
            )?;
        } else {
            store.append_block(height, built.block_hash, &built.events)?;
        }

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
                state_lookups = lookups,
                "backfill progress"
            );
        }
        height += 1;
    }

    store.commit()?;
    tracing::info!(
        covered_through = ?store.covered_through(),
        blocks = store.blocks_covered(),
        events = store.events_stored(),
        elapsed_seconds = format!("{:.0}", started.elapsed().as_secs_f64()),
        state_lookups = lookups,
        "event backfill complete"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli = Cli::parse();

    if let Some(state_dir) = cli.state_dir.clone() {
        return run_state_backfill(cli, state_dir).await;
    }

    let Some(cookie) = cli.zakura_cookie.clone() else {
        return Err("--zakura-cookie is required unless --state-dir is given".into());
    };
    let zakura = ZakuraClient::from_cookie_file(&cli.zakura_rpc_url, &cookie)?;
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

    let mut cache = OutputCache::new(cli.cache_outputs);
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

    // The next block is fetched while this one is being resolved. Fetching is
    // the only part of a block's work that depends on no cache state, so it is
    // the only part that can run ahead; resolution and extraction both read a
    // cache that the block before them has just written.
    let mut height = first;
    let mut ahead = if height <= stop {
        Some(tokio::spawn({
            let zakura = zakura.clone();
            async move { zakura.block(height).await }
        }))
    } else {
        None
    };
    while height <= stop {
        let fetched = ahead
            .take()
            .expect("a fetch is in flight for every height in range")
            .await??;
        if height < stop {
            let next = height + 1;
            ahead = Some(tokio::spawn({
                let zakura = zakura.clone();
                async move { zakura.block(next).await }
            }));
        }

        let built = build_fetched_block_events(&zakura, &mut cache, height, fetched).await?;
        rpc_lookups += built.rpc_lookups;
        cache_hits += built.cache_hits;
        if cli.txid_display {
            store.append_block_with_display(
                height,
                built.block_hash,
                &built.events,
                &built.display,
            )?;
        } else {
            store.append_block(height, built.block_hash, &built.events)?;
        }

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
