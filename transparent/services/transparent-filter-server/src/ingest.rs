//! Following the best chain and building one filter per accepted block.
//!
//! The shape mirrors `enhance-pir-server`'s ingest loop: reconcile against the
//! node's current best chain first, then move forward. Reconciliation walks
//! back one block at a time until the stored hash agrees with the node, which
//! is what makes a reorg a rollback rather than a silently divergent history.

use crate::extract::{extract_block, IndexedEvent};
use crate::prevout::{prefetch_previous_outputs, OutputCache, ZakuraPreviousOutputs};
use crate::service::{Phase, ServiceState};
use crate::store::FilterStore;
use crate::zakura::ZakuraClient;
use transparent_filter::{build_filter, BlockHash, ScriptBytes};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Rolls stored coverage back to the highest block the node still agrees with.
///
/// Only the height map moves. Filter bytes for orphaned blocks stay in the
/// store as immutable cached content; they simply stop being reachable as
/// coverage, and become reachable again for free if their block returns.
pub async fn reconcile(
    zakura: &ZakuraClient,
    store: &mut FilterStore,
    tip: u64,
    state: &ServiceState,
) -> Result<(), BoxError> {
    let mut rolled_back = 0u64;
    loop {
        let Some(height) = store.covered_through() else {
            break;
        };
        let stored = store
            .block_hash_at(height)
            .expect("covered height has a hash");
        if height <= tip {
            let canonical = BlockHash::from_display_hex(&zakura.block_hash(height).await?)?;
            if canonical == stored {
                break;
            }
        }
        tracing::warn!(
            height,
            "rolling back filter coverage after a best-chain change"
        );
        store.rollback_to(height.checked_sub(1))?;
        rolled_back += 1;
    }
    if rolled_back > 0 {
        state.metrics().observe_rollback(rolled_back);
    }
    Ok(())
}

/// The filter for one block, and what resolving it cost.
pub struct BuiltFilter {
    pub block_hash: BlockHash,
    pub filter: transparent_filter::FilterBytes,
    pub rpc_lookups: u64,
    pub cache_hits: u64,
}

/// One block's indexed events, with the resolution work they cost.
pub struct BuiltEvents {
    pub block_hash: BlockHash,
    pub events: Vec<IndexedEvent>,
    pub display: Vec<transparent_shard::txid::TransparentDisplayRecord>,
    pub rpc_lookups: u64,
    pub cache_hits: u64,
}

/// Fetches a block and extracts its events, touching no shared state.
///
/// Deliberately takes no lock. Resolving previous outputs can mean many RPC
/// round trips, and holding a store's write lock across them would stop the
/// service answering requests from the coverage it already has. The lock is
/// taken only for the append that follows.
///
/// An unresolvable previous output propagates as an error and nothing is
/// appended. Publishing a partial result would be worse than publishing
/// nothing, because a consumer cannot tell the difference.
pub async fn build_block_events(
    zakura: &ZakuraClient,
    cache: &mut OutputCache,
    height: u64,
) -> Result<BuiltEvents, BoxError> {
    let fetched = zakura.block(height).await?;
    build_fetched_block_events(zakura, cache, height, fetched).await
}

/// [`build_block_events`] over a block already in hand.
///
/// Split out so a caller can fetch the next block while this one is still being
/// resolved and extracted. Fetching is the one part of the work that touches no
/// cache state: which outputs a block creates and which it spends is a property
/// of the block, and the cache only decides whether they have to be asked for.
/// So the fetch can run ahead, and everything after it cannot.
pub async fn build_fetched_block_events(
    zakura: &ZakuraClient,
    cache: &mut OutputCache,
    height: u64,
    fetched: (String, zakura_chain::block::Block),
) -> Result<BuiltEvents, BoxError> {
    let (hash_display, block) = fetched;
    let block_hash = BlockHash::from_display_hex(&hash_display)?;
    let event_height = u32::try_from(height)
        .map_err(|_| format!("height {height} does not fit the event encoding"))?;

    // Seed the cache with this block's own transactions before extraction, so a
    // later block spending them needs no lookup, and so a same-block spend is
    // never requested from the node.
    for transaction in &block.transactions {
        cache.insert_transaction(transaction);
    }
    // Then resolve everything still missing in batches, before extraction runs.
    let batched = prefetch_previous_outputs(zakura, cache, &block.transactions).await?;

    let runtime = tokio::runtime::Handle::current();
    let transactions = block.transactions.clone();
    let client = zakura.clone();
    // Extraction is synchronous and does blocking RPC through the runtime
    // handle, so it must not run on an async worker thread.
    let (events, rpc_lookups, cache_hits, cache_out) = tokio::task::spawn_blocking({
        let mut cache_owned = std::mem::replace(cache, OutputCache::new(1));
        move || {
            let mut previous = ZakuraPreviousOutputs::new(&client, runtime, &mut cache_owned);
            let result = extract_block(&transactions, &mut previous, event_height);
            let counts = (previous.rpc_lookups, previous.cache_hits);
            (result, counts.0, counts.1, cache_owned)
        }
    })
    .await?;
    *cache = cache_out;

    let extracted = events?;
    Ok(BuiltEvents {
        block_hash,
        events: extracted.events,
        display: extracted.display,
        // The pre-pass did the fetching; any straggler resolved during
        // extraction is counted with it.
        rpc_lookups: rpc_lookups + batched,
        cache_hits,
    })
}

/// Fetches a block and builds its filter.
///
/// The filter's element set is projected from the same extraction that produces
/// the events, rather than being derived by a second walk of the block. That is
/// what guarantees a filter covers exactly the scripts its shard's events are
/// keyed by: a script in the events but missing from the filter would be a
/// permanent, silent coverage hole, since a wallet told it had no activity
/// never looks again.
pub async fn build_block_filter(
    zakura: &ZakuraClient,
    cache: &mut OutputCache,
    height: u64,
) -> Result<BuiltFilter, BoxError> {
    let built = build_block_events(zakura, cache, height).await?;
    let mut elements: Vec<ScriptBytes> =
        built.events.into_iter().map(|(script, _)| script).collect();
    elements.sort();
    elements.dedup();
    let filter = build_filter(built.block_hash, &elements)?;
    Ok(BuiltFilter {
        block_hash: built.block_hash,
        filter,
        rpc_lookups: built.rpc_lookups,
        cache_hits: built.cache_hits,
    })
}

/// The ingest loop: reconcile, catch up, commit, repeat.
pub async fn run(
    zakura: ZakuraClient,
    state: ServiceState,
    cache_outputs: usize,
    poll_seconds: u64,
    commit_every: u64,
) -> Result<(), BoxError> {
    let mut cache = OutputCache::new(cache_outputs);
    loop {
        let tip = zakura.tip_height().await?;
        state.set_tip(tip).await;
        {
            let mut inner = state.inner().write().await;
            let store = &mut inner.store;
            reconcile(&zakura, store, tip, &state).await?;
        }

        let mut next = state.inner().read().await.store.next_height();
        if next > tip {
            state.set_phase(Phase::Serving).await;
            tokio::time::sleep(std::time::Duration::from_secs(poll_seconds)).await;
            continue;
        }
        state
            .set_phase(Phase::Syncing {
                current_height: next.checked_sub(1),
                target_height: tip,
            })
            .await;

        let mut since_commit = 0u64;
        let mut committed = false;
        while next <= tip {
            // Fetch and build outside the lock; take it only to append.
            let built = build_block_filter(&zakura, &mut cache, next).await?;
            {
                let mut inner = state.inner().write().await;
                let store = &mut inner.store;
                // Coverage cannot have moved: this task is the only writer.
                // Checking anyway keeps the append total rather than relying on
                // that staying true.
                if store.next_height() != next {
                    break;
                }
                store.append(next, built.block_hash, built.filter.as_slice())?;
                since_commit += 1;
                if since_commit >= commit_every {
                    store.commit()?;
                    since_commit = 0;
                    committed = true;
                }
            }
            // Republish progress on the same cadence as the commit, so the
            // health endpoint tracks the backfill instead of reporting the
            // height this pass started from for its whole duration. Doing it
            // per block would take the state lock once more per block for a
            // number an operator reads at human speed.
            if committed {
                state
                    .set_phase(Phase::Syncing {
                        current_height: Some(next),
                        target_height: tip,
                    })
                    .await;
                committed = false;
            }
            state
                .metrics()
                .observe_block(built.rpc_lookups, built.cache_hits);
            next += 1;
        }
        {
            let mut inner = state.inner().write().await;
            inner.store.commit()?;
        }
        state.set_phase(Phase::Serving).await;
        tokio::time::sleep(std::time::Duration::from_secs(poll_seconds)).await;
    }
}
