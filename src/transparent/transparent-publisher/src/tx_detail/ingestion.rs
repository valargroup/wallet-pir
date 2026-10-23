//! The display journal as a root-scheduled writer.
//!
//! The root follower observes the node; this writer extracts each block with
//! its display sidecars and commits it. A rollback is refused below the
//! sealed floor (the controller halts), and otherwise withdraws HTTP
//! authority and records the invalidation durably before the journal changes.
//! The controller only reads committed blocks ([`super::source::CommittedSource`]).
use super::controller::RollbackFence;
use super::now_ms;
use super::source::WriterView;
use crate::ingest::blocks::build_fetched_block_events;
use crate::ingest::prevout::{OutputCache, DEFAULT_CACHE_OUTPUTS};
use crate::ingest::zakura::NodeClient;
use pir_pipeline::ingestion::{BlockId, ChainObservation, Ingestion};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use transparent_shared::journal::events::{EventStore, EventStoreError};
use zakura_pir_publisher::transparent::filter::BlockHash;

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// The chain forked below the sealed floor; the journal is untouched.
    #[error("reorg ancestor {0} is below the sealed floor {1}")]
    SealedReorg(i128, u64),
    /// The journal could not be written; restart from its checkpoint.
    #[error(transparent)]
    Store(#[from] EventStoreError),
    /// Nothing advanced; retry on a later observation.
    #[error("{0}")]
    Transient(String),
}

fn transient(error: impl std::fmt::Display) -> IngestError {
    IngestError::Transient(error.to_string())
}

pub struct DetailIngestion {
    rpc: NodeClient,
    store: EventStore,
    outputs: OutputCache,
    fence: Arc<RollbackFence>,
    view: Arc<Mutex<WriterView>>,
    notify: watch::Sender<u64>,
}

impl DetailIngestion {
    /// The only writer of `store`; the controller sharing `fence` reads the
    /// committed journal and wakes on `notify`.
    pub fn new(
        rpc: NodeClient,
        store: EventStore,
        fence: Arc<RollbackFence>,
        view: Arc<Mutex<WriterView>>,
        notify: watch::Sender<u64>,
    ) -> Self {
        Self {
            rpc,
            store,
            outputs: OutputCache::new(DEFAULT_CACHE_OUTPUTS),
            fence,
            view,
            notify,
        }
    }

    fn block(&self, height: u64) -> Option<BlockId> {
        Some(BlockId {
            height,
            hash: self.store.block_at(height)?.block_hash.to_display_hex(),
        })
    }

    fn changed(&self) {
        self.notify.send_modify(|n| *n += 1);
    }

    /// Fetches, extracts and commits one block, as the History writer does.
    async fn ingest_one(&mut self, height: u64) -> Result<(), IngestError> {
        let fetched = self.rpc.fetch_block(height).await.map_err(transient)?;
        let parent = BlockHash::from_internal_bytes(fetched.parent()).to_display_hex();
        let expected = height
            .checked_sub(1)
            .and_then(|parent| self.block(parent))
            .ok_or_else(|| transient("missing journal parent"))?
            .hash;
        if parent != expected {
            return Err(transient("node reorganized while fetching block; retry"));
        }
        let built = build_fetched_block_events(&self.rpc, &mut self.outputs, height, fetched, true)
            .await
            .map_err(transient)?;
        if self
            .rpc
            .block_hash_display(height)
            .await
            .map_err(transient)?
            != built.block_hash.to_display_hex()
        {
            return Err(transient("block became noncanonical during extraction"));
        }
        self.store.append_block_with_display(
            height,
            built.block_hash,
            &built.events,
            &built.display,
        )?;
        self.store.commit()?;
        Ok(())
    }
}

impl Ingestion for DetailIngestion {
    type Error = IngestError;
    fn start_height(&self, _tip: u64) -> u64 {
        self.store.start_height()
    }
    /// Halt on another network, and note when each new height was first seen.
    async fn observed(&mut self, observation: &ChainObservation) -> Result<(), IngestError> {
        if observation.genesis != self.store.genesis_hash() {
            // Another network shares no block with the journal: its fork
            // ancestor lies below genesis, which the sealed floor always
            // refuses, so the controller halts and the journal is untouched.
            let (ancestor, floor) = self
                .fence
                .check(None)
                .expect_err("no rollback below genesis passes the sealed floor");
            return Err(IngestError::SealedReorg(ancestor, floor));
        }
        let tip = observation.tip.height;
        let next = self.store.covered_through().map_or(0, |h| h + 1);
        let now = now_ms();
        let mut view = self.view.lock().unwrap();
        view.node_tip = Some(tip);
        for height in next..=tip {
            view.first_seen.entry(height).or_insert(now);
        }
        Ok(())
    }
    fn committed(&self) -> Option<BlockId> {
        self.block(self.store.covered_through()?)
    }
    /// Checkpoints stop at the sealed floor: a fork below it is never searched
    /// past, and reconciliation ends in a refused rollback.
    async fn checkpoint_at_or_below(&self, height: u64) -> Option<BlockId> {
        if height < self.fence.floor() {
            return None;
        }
        self.block(self.store.covered_through()?.min(height))
    }
    async fn withdraw_above(&mut self, keep: Option<&BlockId>) -> Result<(), IngestError> {
        self.fence
            .check(keep.map(|keep| keep.height))
            .map_err(|(ancestor, floor)| IngestError::SealedReorg(ancestor, floor))?;
        let keep = keep.expect("a checked rollback keeps a block");
        let hash = self
            .store
            .block_at(keep.height)
            .ok_or_else(|| transient("missing journal ancestor"))?
            .block_hash;
        self.fence
            .before_rollback(keep.height, hash)
            .map_err(IngestError::Transient)
    }
    async fn rewind(&mut self, keep: Option<&BlockId>) -> Result<(), IngestError> {
        let keep = keep.map(|keep| keep.height);
        self.store.rollback_to(keep)?;
        self.outputs = OutputCache::new(DEFAULT_CACHE_OUTPUTS);
        // Replacement blocks are observed from the next pass, not when the
        // orphans were.
        let above = keep.map_or(0, |h| h + 1);
        self.view.lock().unwrap().first_seen.split_off(&above);
        self.changed();
        Ok(())
    }
    async fn ingest(&mut self, from: u64, through: u64) -> Result<(), IngestError> {
        self.view.lock().unwrap().current = false;
        for height in from..=through {
            self.ingest_one(height).await?;
            self.changed();
        }
        Ok(())
    }
    async fn caught_up(&mut self, _: &ChainObservation) -> Result<(), IngestError> {
        let mut view = self.view.lock().unwrap();
        if !view.current {
            view.current = true;
            drop(view);
            self.changed();
        }
        Ok(())
    }
    fn failed(&mut self, error: &IngestError) -> pir_pipeline::FailurePolicy {
        match error {
            IngestError::Transient(error) => {
                tracing::warn!(%error, "display journal ingestion retries next pass");
                self.view.lock().unwrap().error = Some(error.clone());
                pir_pipeline::FailurePolicy::NextPoll
            }
            // The controller halts on the refused rollback the fence holds,
            // or the service restarts from the journal's checkpoint.
            IngestError::SealedReorg(..) | IngestError::Store(_) => {
                self.changed();
                pir_pipeline::FailurePolicy::Stop
            }
        }
    }
}
