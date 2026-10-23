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
/// When History runs beside the filters, blocks History's event journal has
/// committed are projected from it, including oversized-event sidecars, rather
/// than extracted a second time; heights below the journal's start still use
/// extraction, and heights above its coverage wait for History.
///
/// Rollback only moves the height map: filter bytes for orphaned blocks stay in
/// the store as immutable cached content and become reachable again for free if
/// their block returns.
pub struct FilterIngestion {
    history: Option<HistoryJournal>,
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
            history: None,
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

impl FilterIngestion {
    /// Project blocks History's committed journal at `dir` holds.
    pub fn sharing_history(mut self, dir: std::path::PathBuf) -> Self {
        self.history = Some(HistoryJournal { dir, reader: None });
        self
    }

    /// One block's hash, parent and filter, projected from History's journal
    /// when it holds the block, or extracted from the node otherwise.
    async fn build(&mut self, height: u64) -> Result<Built, BoxError> {
        if let Some(history) = &mut self.history {
            if let Some(built) = history.project(height)? {
                return Ok(built);
            }
        }
        let fetched = self.zakura.fetch_block(height).await?;
        let parent = BlockHash::from_internal_bytes(fetched.parent());
        let built =
            build_fetched_block_filter(&self.zakura, &mut self.cache, height, fetched).await?;
        if self.zakura.block_hash_display(height).await? != built.block_hash.to_display_hex() {
            return Err("block became noncanonical during extraction".into());
        }
        self.state
            .metrics()
            .observe_block(built.rpc_lookups, built.cache_hits);
        Ok(Built {
            block_hash: built.block_hash,
            parent: Some(parent),
            filter: built.filter,
        })
    }
}

struct Built {
    block_hash: BlockHash,
    /// The parent hash, when the source records it.
    parent: Option<BlockHash>,
    filter: transparent_filter::FilterBytes,
}

/// History's committed event journal, read in place.
struct HistoryJournal {
    dir: std::path::PathBuf,
    reader: Option<transparent_shared::journal::events::EventStore>,
}

impl HistoryJournal {
    /// The filter of `height` from committed History events, `None` below the
    /// journal's start. Heights above its committed coverage are an error the
    /// next observation retries, so the filter store never runs ahead of it.
    fn project(&mut self, height: u64) -> Result<Option<Built>, BoxError> {
        let reader = match &mut self.reader {
            Some(reader) => {
                reader.refresh()?;
                reader
            }
            None => self
                .reader
                .insert(transparent_shared::journal::events::EventStore::open_existing(&self.dir)?),
        };
        if height < reader.start_height() {
            return Ok(None);
        }
        let (Some(block), Some(events)) = (reader.block_at(height), reader.events_at(height)?)
        else {
            return Err(format!("History's journal has not committed block {height} yet").into());
        };
        let parent = height
            .checked_sub(1)
            .and_then(|parent| reader.block_at(parent))
            .map(|parent| parent.block_hash);
        let mut elements: Vec<ScriptBytes> = events.into_iter().map(|(script, _)| script).collect();
        elements.sort();
        elements.dedup();
        Ok(Some(Built {
            block_hash: block.block_hash,
            parent,
            filter: build_filter(block.block_hash, &elements)?,
        }))
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
            // Build outside the lock; take it only to append.
            let built = self.build(next).await?;
            let expected = match &self.end {
                Some(end) if end.height + 1 == next => Some(end.hash.clone()),
                None if next == self.start && next == 0 => {
                    Some(BlockHash::from_internal_bytes([0; 32]).to_display_hex())
                }
                // The first block of an empty store has no committed parent.
                None if next == self.start => None,
                _ => return Err("missing filter store parent".into()),
            };
            // A journal's first block records no parent; the scheduler's
            // canonical anchor check covers it.
            if let (Some(expected), Some(parent)) = (&expected, built.parent) {
                if parent.to_display_hex() != *expected {
                    return Err(
                        "node reorganized while fetching block; retry reconciliation".into(),
                    );
                }
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

#[cfg(test)]
mod history_projection_tests {
    use super::*;
    use transparent_shared::journal::events::EventStore;
    use zakura_pir_protocol::transparent::events::{
        ReceiveEvent, SpendEvent, TransparentEvent, Txid,
    };

    fn hash(height: u64) -> BlockHash {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&height.to_le_bytes());
        bytes[31] = 0xa5;
        BlockHash::from_internal_bytes(bytes)
    }

    fn receive(script: &[u8], height: u32, tag: u8) -> (ScriptBytes, TransparentEvent) {
        let event = TransparentEvent::Receive(ReceiveEvent {
            metadata: None,
            height,
            txid: Txid([tag; 32]),
            transaction_index: 0,
            output_index: u32::from(tag),
            value: 1_000,
            coinbase: false,
        });
        (ScriptBytes::new(script.to_vec()), event)
    }

    fn spend(script: &[u8], height: u32, tag: u8) -> (ScriptBytes, TransparentEvent) {
        let event = TransparentEvent::Spend(SpendEvent {
            metadata: None,
            height,
            spending_txid: Txid([tag + 1; 32]),
            transaction_index: 1,
            input_index: 0,
            spent_txid: Txid([tag; 32]),
            spent_output_index: 0,
        });
        (ScriptBytes::new(script.to_vec()), event)
    }

    /// Filters projected from History's committed events equal filters built
    /// over independently listed script sets: empty blocks, receives, spends,
    /// repeated and unusual scripts, and a script held only in a display
    /// sidecar. Heights below the journal's start fall
    /// back to extraction; heights above its coverage wait; a rollback the
    /// journal commits is visible to the next projection.
    #[test]
    fn projection_matches_independent_script_sets_and_follows_rollbacks() {
        let dir = tempfile::tempdir().unwrap();
        let p2pkh = [0x76, 0xa9, 0x14, 7, 0x88, 0xac];
        let bare = [0x51, 0x21, 3, 0x51, 0xae];
        let op_return = [0x6a, 0x04, 1, 2, 3, 4];
        let one_byte = [0x00];
        let blocks: Vec<(Vec<(ScriptBytes, TransparentEvent)>, Vec<&[u8]>)> = vec![
            (vec![], vec![]),
            (
                vec![
                    receive(&p2pkh, 11, 1),
                    receive(&bare, 11, 2),
                    spend(&p2pkh, 11, 3),
                ],
                vec![&p2pkh, &bare],
            ),
            (
                vec![receive(&op_return, 12, 4), spend(&one_byte, 12, 5)],
                vec![&one_byte, &op_return],
            ),
        ];
        let mut writer = EventStore::open(dir.path(), "00", 10).unwrap();
        for (offset, (events, _)) in blocks.iter().enumerate() {
            let height = 10 + offset as u64;
            writer.append_block(height, hash(height), events).unwrap();
        }
        // A script above the journal's limit lives only in the block's display
        // sidecar; a projection that skipped it would hide that activity.
        let oversized = vec![0x51; 20_000];
        let sidecar_height = 10 + blocks.len() as u64;
        transparent_shared::journal::display_journal::write_envelope(
            &transparent_shared::journal::display_journal::V1,
            dir.path(),
            hash(sidecar_height),
            std::iter::empty(),
            &[receive(&oversized, sidecar_height as u32, 6)],
        )
        .unwrap();
        writer
            .append_block(
                sidecar_height,
                hash(sidecar_height),
                &[receive(&p2pkh, sidecar_height as u32, 7)],
            )
            .unwrap();
        writer.commit().unwrap();
        let blocks: Vec<_> = blocks
            .into_iter()
            .chain([(vec![], vec![&oversized[..], &p2pkh[..]])])
            .collect();
        let mut journal = HistoryJournal {
            dir: dir.path().to_path_buf(),
            reader: None,
        };
        assert!(journal.project(9).unwrap().is_none(), "below the start");
        for (offset, (_, scripts)) in blocks.iter().enumerate() {
            let height = 10 + offset as u64;
            let built = journal.project(height).unwrap().unwrap();
            let mut expected: Vec<ScriptBytes> = scripts
                .iter()
                .map(|s| ScriptBytes::new(s.to_vec()))
                .collect();
            expected.sort();
            assert_eq!(built.block_hash, hash(height));
            assert_eq!(
                built.parent,
                height.checked_sub(1).filter(|h| *h >= 10).map(hash)
            );
            assert_eq!(
                built.filter.as_slice(),
                build_filter(hash(height), &expected).unwrap().as_slice(),
                "height {height}"
            );
        }
        assert!(journal.project(14).is_err(), "above committed coverage");
        // History rolls back and commits a replacement block.
        writer.rollback_to(Some(11)).unwrap();
        let mut replaced = hash(12).internal_bytes().to_owned();
        replaced[30] = 1;
        let replaced = BlockHash::from_internal_bytes(replaced);
        writer
            .append_block(12, replaced, &[receive(&p2pkh, 12, 9)])
            .unwrap();
        writer.commit().unwrap();
        let built = journal.project(12).unwrap().unwrap();
        assert_eq!(built.block_hash, replaced);
        assert_eq!(
            built.filter.as_slice(),
            build_filter(replaced, &[ScriptBytes::new(p2pkh.to_vec())])
                .unwrap()
                .as_slice()
        );
    }
}
