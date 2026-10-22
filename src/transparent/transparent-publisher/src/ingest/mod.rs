//! Following the best chain and building one filter per accepted block.
//!
//! The shape mirrors `enhance-pir-server`'s ingest loop: reconcile against the
//! node's current best chain first, then move forward. Reconciliation walks
//! back one block at a time until the stored hash agrees with the node, which
//! is what makes a reorg a rollback rather than a silently divergent history.

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
use crate::ingest::blocks::build_block_events;
use crate::ingest::prevout::OutputCache;
use crate::ingest::store::FilterStore;
use transparent_filter::{build_filter, BlockHash, ScriptBytes};
use zakura_server_chain::NodeClient;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Rolls stored coverage back to the highest block the node still agrees with.
///
/// Only the height map moves. Filter bytes for orphaned blocks stay in the
/// store as immutable cached content; they simply stop being reachable as
/// coverage, and become reachable again for free if their block returns.
pub async fn reconcile(
    zakura: &NodeClient,
    store: &mut FilterStore,
    tip: u64,
    state: &ServiceState,
) -> Result<(), BoxError> {
    let mut rolled_back = 0u64;
    while let Some(height) = store.covered_through() {
        let stored = store
            .block_hash_at(height)
            .expect("covered height has a hash");
        if height <= tip {
            let canonical = BlockHash::from_internal_bytes(zakura.block_hash(height).await?);
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
    let built = build_block_events(zakura, cache, height, false).await?;
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
    zakura: NodeClient,
    state: ServiceState,
    cache_outputs: usize,
    poll_seconds: u64,
    commit_every: u64,
    shutdown: pir_pipeline::service::Shutdown,
) -> Result<(), BoxError> {
    pir_pipeline::publisher::PublisherService::new(FilterIngestion {
        zakura,
        state,
        cache: OutputCache::new(cache_outputs),
        cache_outputs,
        poll_seconds,
        commit_every,
        retrying: false,
        shutdown: shutdown.clone(),
    })
    .run(shutdown.cancelled())
    .await
}

struct FilterIngestion {
    zakura: NodeClient,
    state: ServiceState,
    cache: OutputCache,
    cache_outputs: usize,
    poll_seconds: u64,
    commit_every: u64,
    retrying: bool,
    shutdown: pir_pipeline::service::Shutdown,
}
impl pir_pipeline::publisher::Workflow for FilterIngestion {
    type Error = BoxError;
    async fn step(&mut self) -> Result<pir_pipeline::publisher::Control, BoxError> {
        self.retrying = false;
        let tip = self.zakura.tip_height().await?;
        self.state.set_tip(tip).await;
        {
            let mut inner = self.state.inner().write().await;
            let store = &mut inner.store;
            reconcile(&self.zakura, store, tip, &self.state).await?;
        }

        let mut next = self.state.inner().read().await.store.next_height();
        if next > tip {
            self.state.set_phase(Phase::Serving).await;
            return Ok(pir_pipeline::publisher::Control::Continue);
        }
        self.state
            .set_phase(Phase::Syncing {
                current_height: next.checked_sub(1),
                target_height: tip,
            })
            .await;

        let mut since_commit = 0u64;
        let mut committed = false;
        while next <= tip && !self.shutdown.requested() {
            // Fetch and build outside the lock; take it only to append.
            let state = &self.state;
            let commit_every = self.commit_every;
            let rpc = &self.zakura;
            let cache = &mut self.cache;
            let built = pir_pipeline::publisher::PublicationPass::new(
                async { Ok::<_, BoxError>(pir_pipeline::Input::Work(next)) },
                |height| async move {
                    build_block_filter(rpc, cache, height)
                        .await
                        .map(pir_pipeline::Input::Work)
                },
                |built| async {
                    {
                        let mut inner = state.inner().write().await;
                        let store = &mut inner.store;
                        // Coverage cannot have moved: this task is the only writer.
                        // Checking anyway keeps the append total rather than relying on
                        // that staying true.
                        if store.next_height() != next {
                            return Ok(None);
                        }
                        store.append(next, built.block_hash, built.filter.as_slice())?;
                        since_commit += 1;
                        if since_commit >= commit_every {
                            store.commit()?;
                            since_commit = 0;
                            committed = true;
                        }
                    }
                    Ok(Some(built))
                },
            )
            .run(&mut ())
            .await?;
            let pir_pipeline::Input::Work(Some(built)) = built else {
                break;
            };
            // Republish progress on the same cadence as the commit, so the
            // health endpoint tracks the backfill instead of reporting the
            // height this pass started from for its whole duration. Doing it
            // per block would take the state lock once more per block for a
            // number an operator reads at human speed.
            if committed {
                self.state
                    .set_phase(Phase::Syncing {
                        current_height: Some(next),
                        target_height: tip,
                    })
                    .await;
                committed = false;
            }
            self.state
                .metrics()
                .observe_block(built.rpc_lookups, built.cache_hits);
            next += 1;
        }
        {
            let mut inner = self.state.inner().write().await;
            inner.store.commit()?;
        }
        self.state.set_phase(Phase::Serving).await;
        Ok(pir_pipeline::publisher::Control::Continue)
    }
    async fn finish(&mut self) -> Result<(), BoxError> {
        self.state.inner().write().await.store.commit()?;
        Ok(())
    }
    fn failed(&mut self, error: &BoxError) -> pir_pipeline::FailurePolicy {
        tracing::error!(%error, "filter ingestion stopped");
        self.retrying = true;
        self.cache = OutputCache::new(self.cache_outputs);
        pir_pipeline::FailurePolicy::NextPoll
    }
    async fn wait(&mut self) -> Result<(), BoxError> {
        let seconds = if self.retrying {
            self.state
                .set_phase(Phase::Failed {
                    reason: "ingestion failed; retrying; inspect local logs".into(),
                })
                .await;
            30
        } else {
            self.poll_seconds
        };
        tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
        Ok(())
    }
}
