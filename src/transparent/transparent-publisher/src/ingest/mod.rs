//! Building one filter per accepted block for the root-scheduled filter store.
//!
//! The root publisher follows the chain; its executor reconciles each durable
//! checkpoint against the node, so a reorg is a rollback rather than a silently
//! divergent history.

use zakura_pir_publisher::transparent::filter as transparent_filter;

pub mod blocks;
pub mod extract;
pub mod journal;
pub mod prevout;
pub mod snapshot;
#[cfg(feature = "rocksdb-reader")]
pub mod state;
pub mod store;
pub mod zakura;

// The extractor's tests reach the journal through their grandparent module, so
// the standalone txid sizing exporter can compile the same file beside its
// own copies of these modules.
#[cfg(all(test, feature = "fixtures"))]
use transparent_shared::journal::{display_journal, events};

use crate::history::service::{Phase, ServiceState};
use crate::ingest::blocks::build_fetched_block_events;
use crate::ingest::prevout::OutputCache;
use crate::ingest::store::FilterStore;
use pir_pipeline::ingestion::{BlockId, ChainObservation, Ingestion};
use transparent_filter::{build_filter, BlockHash, ScriptBytes};
use zakura_server_chain::NodeClient;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The filter for one block, and what resolving it cost.
pub struct BuiltFilter {
    pub block_hash: BlockHash,
    pub filter: transparent_filter::FilterBytes,
    pub rpc_lookups: u64,
    pub cache_hits: u64,
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
    zakura: &NodeClient,
    cache: &mut OutputCache,
    height: u64,
) -> Result<BuiltFilter, BoxError> {
    let fetched = zakura.fetch_block(height).await?;
    build_fetched_block_filter(zakura, cache, height, fetched).await
}

/// [`build_block_filter`] over a block already in hand.
pub async fn build_fetched_block_filter(
    zakura: &NodeClient,
    cache: &mut OutputCache,
    height: u64,
    fetched: zakura_server_chain::transparent::FetchedBlock,
) -> Result<BuiltFilter, BoxError> {
    let built = build_fetched_block_events(zakura, cache, height, fetched, false).await?;
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

/// The filter store as a root-scheduled writer. Reconciliation, live extension
/// and backfill come from the root executor; this writer only reads committed
/// coverage, rolls it back and appends filters built by the same transparent
/// extraction History uses. It needs no historical event journal.
///
/// Rollback only moves the height map: filter bytes for orphaned blocks stay in
/// the store as immutable cached content and become reachable again for free if
/// their block returns.
pub struct FilterIngestion {
    zakura: NodeClient,
    state: ServiceState,
    cache: OutputCache,
    cache_outputs: usize,
    commit_every: u64,
    genesis: String,
    start: u64,
    end: Option<BlockId>,
}

impl FilterIngestion {
    pub async fn new(
        zakura: NodeClient,
        state: ServiceState,
        cache_outputs: usize,
        commit_every: u64,
    ) -> Self {
        let (genesis, start, end) = {
            let inner = state.inner().read().await;
            let store = &inner.store;
            (
                store.genesis_hash().to_owned(),
                store.start_height(),
                newest(store),
            )
        };
        Self {
            zakura,
            state,
            cache: OutputCache::new(cache_outputs),
            cache_outputs,
            commit_every,
            genesis,
            start,
            end,
        }
    }
}

/// The newest committed block of a filter store.
fn newest(store: &FilterStore) -> Option<BlockId> {
    let height = store.covered_through()?;
    Some(BlockId {
        height,
        hash: store.block_hash_at(height)?.to_display_hex(),
    })
}

impl Ingestion for FilterIngestion {
    type Error = BoxError;
    fn start_height(&self, _tip: u64) -> u64 {
        self.start
    }
    async fn observed(&mut self, observation: &ChainObservation) -> Result<(), BoxError> {
        self.state.set_tip(observation.tip.height).await;
        // A store built against one chain is never served as another.
        if observation.genesis != self.genesis {
            return Err("node genesis differs from the filter store's; coverage retained".into());
        }
        Ok(())
    }
    fn committed(&self) -> Option<BlockId> {
        self.end.clone()
    }
    async fn checkpoint_at_or_below(&self, height: u64) -> Option<BlockId> {
        let inner = self.state.inner().read().await;
        let store = &inner.store;
        let height = store.covered_through()?.min(height);
        Some(BlockId {
            height,
            hash: store.block_hash_at(height)?.to_display_hex(),
        })
    }
    async fn withdraw_above(&mut self, _keep: Option<&BlockId>) -> Result<(), BoxError> {
        // Filters carry no publication authority: coverage alone is withdrawn,
        // atomically with the rollback below.
        Ok(())
    }
    async fn rewind(&mut self, keep: Option<&BlockId>) -> Result<(), BoxError> {
        let end = self.end.as_ref().map(|end| end.height);
        let keep = keep.map(|keep| keep.height);
        tracing::warn!(
            ?keep,
            "rolling back filter coverage after a best-chain change"
        );
        {
            let mut inner = self.state.inner().write().await;
            inner.store.rollback_to(keep)?;
            self.end = newest(&inner.store);
        }
        let depth = match (end, keep) {
            (Some(end), Some(keep)) => end.saturating_sub(keep),
            (Some(end), None) => end + 1 - self.start,
            (None, _) => 0,
        };
        if depth > 0 {
            self.state.metrics().observe_rollback(depth);
        }
        self.cache = OutputCache::new(self.cache_outputs);
        Ok(())
    }
    async fn ingest(&mut self, from: u64, through: u64) -> Result<(), BoxError> {
        self.state
            .set_phase(Phase::Syncing {
                current_height: from.checked_sub(1),
                target_height: through,
            })
            .await;
        let mut since_commit = 0;
        for next in from..=through {
            // Fetch and build outside the lock; take it only to append.
            let fetched = self.zakura.fetch_block(next).await?;
            let parent = BlockHash::from_internal_bytes(fetched.parent()).to_display_hex();
            let expected = match &self.end {
                Some(end) if end.height + 1 == next => end.hash.clone(),
                None if next == self.start && next == 0 => {
                    BlockHash::from_internal_bytes([0; 32]).to_display_hex()
                }
                // The first block of an empty store has no committed parent.
                None if next == self.start => parent.clone(),
                _ => return Err("missing filter store parent".into()),
            };
            if parent != expected {
                return Err("node reorganized while fetching block; retry reconciliation".into());
            }
            let built =
                build_fetched_block_filter(&self.zakura, &mut self.cache, next, fetched).await?;
            if self.zakura.block_hash_display(next).await? != built.block_hash.to_display_hex() {
                return Err("block became noncanonical during extraction".into());
            }
            {
                let mut inner = self.state.inner().write().await;
                let store = &mut inner.store;
                store.append(next, built.block_hash, built.filter.as_slice())?;
                since_commit += 1;
                if since_commit >= self.commit_every {
                    store.commit()?;
                    since_commit = 0;
                }
            }
            self.end = Some(BlockId {
                height: next,
                hash: built.block_hash.to_display_hex(),
            });
            self.state
                .metrics()
                .observe_block(built.rpc_lookups, built.cache_hits);
        }
        self.state.inner().write().await.store.commit()?;
        self.state
            .set_phase(Phase::Syncing {
                current_height: Some(through),
                target_height: through,
            })
            .await;
        Ok(())
    }
    async fn caught_up(&mut self, _: &ChainObservation) -> Result<(), BoxError> {
        self.state.set_phase(Phase::Serving).await;
        Ok(())
    }
    fn failed(&mut self, error: &BoxError) -> pir_pipeline::FailurePolicy {
        tracing::error!(%error, "filter ingestion failed; retrying without advancing coverage");
        self.cache = OutputCache::new(self.cache_outputs);
        if error
            .downcast_ref::<crate::ingest::store::StoreError>()
            .is_some()
        {
            return pir_pipeline::FailurePolicy::Stop;
        }
        pir_pipeline::FailurePolicy::NextPoll
    }
}
