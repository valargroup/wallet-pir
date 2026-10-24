//! Where the controller's blocks come from.
//!
//! - [`CommittedSource`] reads what the root-scheduled journal writer
//!   ([`super::ingestion`]) committed; the root follows the node.
//! - [`ReplaySource`] advances a visible tip over an already ingested journal
//!   at a fixed pace, for accelerated runs across several seals.
//! - [`Script`] is driven by tests.
//!
//! Every source refuses a fork whose common ancestor lies below the sealed
//! floor (the end of the last published or queued seal, or the display
//! start's parent). A sealed range is never rewritten; the controller halts.
//!
//! A source that rolls the journal back calls the controller's
//! [`BeforeRollback`] first, so the invalidation the rollback calls for is
//! durable before any published block leaves the journal.

use zakura_pir_publisher::transparent as transparent_shard;
use zakura_pir_publisher::transparent::filter as transparent_filter;

use super::cache::DisplayCache;
use super::now_ms;
use crate::BoxError;
use std::collections::BTreeMap;
#[cfg(feature = "fixtures")]
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
#[cfg(feature = "fixtures")]
use std::time::{Duration, Instant};
use tokio::sync::watch;
use transparent_filter::BlockHash;
use transparent_shard::txid::DisplayRecord;
#[cfg(feature = "fixtures")]
use transparent_shard::txid_v2x::TransparentDisplayRecordV2x;
use transparent_shared::journal::events::{EventStore, EventStoreError};

/// Most blocks one poll returns, so a catch-up still publishes as it goes.
const BATCH: u64 = 64;

pub struct SourceBlock {
    pub height: u64,
    pub hash: BlockHash,
    pub records: Vec<DisplayRecord>,
    pub observed_ms: u64,
    pub ingested_ms: u64,
}

/// What one poll changed. A rollback applies before the blocks.
#[derive(Default)]
pub struct SourceUpdate {
    /// The common ancestor: every height above it was replaced.
    pub rollback_to: Option<u64>,
    pub blocks: Vec<SourceBlock>,
    /// A failure after the journal changed: the change still stands.
    pub error: Option<String>,
    /// Replay fell behind its schedule.
    pub lag: Option<serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    /// The chain forked below the sealed floor.
    #[error("reorg ancestor {ancestor} is below the sealed floor {floor}")]
    SealedReorg { ancestor: i128, floor: u64 },
    /// The journal could not be written; restart from its checkpoint.
    #[error(transparent)]
    Store(#[from] EventStoreError),
    /// Nothing changed; try again later.
    #[error("{0}")]
    Transient(String),
}

/// Called with the common ancestor and its hash before a source rolls the
/// journal back to it. An error leaves the journal untouched and fails the
/// poll.
pub type BeforeRollback<'a> = &'a mut (dyn FnMut(u64, BlockHash) -> Result<(), String> + Send);

fn transient(error: impl std::fmt::Display) -> SourceError {
    SourceError::Transient(error.to_string())
}

/// Journal blocks above the cache tip, through `through` and at most `limit`
/// of them, after rolling the cache back to where it agrees with the
/// journal. Shared by every source: a restarted or replaying controller
/// always catches up from the journal first.
fn from_journal(
    store: &EventStore,
    cache: &DisplayCache,
    floor: u64,
    (through, limit): (u64, u64),
    observed_ms: impl Fn(u64) -> u64,
) -> Result<SourceUpdate, SourceError> {
    let mut update = SourceUpdate::default();
    let mut agreed = cache.next_height().checked_sub(1);
    while let Some(height) = agreed.filter(|h| *h >= cache.start()) {
        let journal = store.block_at(height).map(|b| b.block_hash);
        if journal == cache.block(height).map(|b| b.hash) {
            break;
        }
        agreed = height.checked_sub(1);
    }
    if agreed != cache.next_height().checked_sub(1) {
        let ancestor = agreed.map_or(-1, i128::from);
        if ancestor < i128::from(floor) {
            return Err(SourceError::SealedReorg { ancestor, floor });
        }
        update.rollback_to = agreed;
    }
    let next = agreed.map_or(cache.start(), |h| h + 1);
    let through = through
        .min(store.covered_through().unwrap_or(0))
        .min(next.saturating_add(limit.max(1) - 1));
    let ingested_ms = now_ms();
    for height in next..=through {
        let hash = store
            .block_at(height)
            .ok_or_else(|| transient(format!("journal lost block {height}")))?
            .block_hash;
        update.blocks.push(SourceBlock {
            height,
            hash,
            records: store.display_at(height)?,
            observed_ms: observed_ms(height),
            ingested_ms,
        });
    }
    Ok(update)
}

/// What the root-scheduled journal writer reports beside committed blocks.
#[derive(Default)]
pub struct WriterView {
    /// The node's tip at the latest observation.
    pub(super) node_tip: Option<u64>,
    /// When each height above committed coverage was first observed.
    pub(super) first_seen: BTreeMap<u64, u64>,
    /// Whether committed coverage reached the latest observed tip.
    pub(super) current: bool,
    /// A failure after the journal changed; the change still stands.
    pub(super) error: Option<String>,
}

/// Committed display journal updates from the root-scheduled writer
/// ([`super::ingestion::DetailIngestion`]). The controller reads them through
/// its own read-only view of the journal; it never writes or follows the node.
pub struct CommittedSource {
    view: Arc<Mutex<WriterView>>,
    updates: watch::Receiver<u64>,
    /// Committed blocks remain beyond the last bounded batch.
    more: AtomicBool,
}

impl CommittedSource {
    pub fn new(view: Arc<Mutex<WriterView>>, updates: watch::Receiver<u64>) -> Self {
        Self {
            view,
            updates,
            more: AtomicBool::new(true),
        }
    }

    async fn wait(&self) {
        if self.more.load(Ordering::Acquire) {
            return;
        }
        let mut updates = self.updates.clone();
        if updates.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }

    fn poll(
        &mut self,
        store: &mut EventStore,
        cache: &DisplayCache,
        floor: u64,
        fence: &super::controller::RollbackFence,
    ) -> Result<SourceUpdate, SourceError> {
        self.updates.borrow_and_update();
        if let Some((ancestor, floor)) = fence.sealed() {
            return Err(SourceError::SealedReorg { ancestor, floor });
        }
        store.refresh()?;
        let journal_end = store
            .covered_through()
            .ok_or_else(|| transient("empty journal"))?;
        let mut view = self.view.lock().unwrap();
        let now = now_ms();
        let mut update = from_journal(store, cache, floor, (journal_end, BATCH), |h| {
            view.first_seen.get(&h).copied().unwrap_or(now)
        })?;
        let delivered = update
            .blocks
            .last()
            .map_or(cache.next_height(), |b| b.height + 1);
        self.more.store(delivered <= journal_end, Ordering::Release);
        view.first_seen = view.first_seen.split_off(&delivered);
        update.error = view.error.take();
        Ok(update)
    }
}

/// Advances a visible tip over an ingested journal at a fixed pace.
#[cfg(feature = "fixtures")]
pub struct ReplaySource {
    blocks_per_step: u64,
    interval: Duration,
    end: u64,
    next_tick: Instant,
    next_tick_ms: u64,
}

#[cfg(feature = "fixtures")]
impl ReplaySource {
    pub fn new(blocks_per_step: u64, interval: Duration, end: u64) -> Self {
        Self {
            blocks_per_step: blocks_per_step.max(1),
            interval,
            end,
            next_tick: Instant::now(),
            next_tick_ms: now_ms(),
        }
    }

    async fn wait(&self) {
        tokio::time::sleep_until(self.next_tick.into()).await;
    }

    fn exhausted(&self, cache: &DisplayCache) -> bool {
        cache.next_height() > self.end
    }

    /// One step, or several coalesced when the controller fell behind.
    fn poll(
        &mut self,
        store: &EventStore,
        cache: &DisplayCache,
        floor: u64,
    ) -> Result<SourceUpdate, SourceError> {
        let now = Instant::now();
        let behind = now.saturating_duration_since(self.next_tick);
        let steps = 1 + (behind.as_millis() / self.interval.as_millis().max(1)) as u64;
        let first_ms = self.next_tick_ms;
        let interval_ms = self.interval.as_millis() as u64;
        let from = cache.next_height();
        let through = (from - 1)
            .saturating_add(steps * self.blocks_per_step)
            .min(self.end);
        let per_step = self.blocks_per_step;
        // Coalesced steps arrive together; each block keeps its own tick.
        let mut update = from_journal(store, cache, floor, (through, u64::MAX), |height| {
            first_ms + height.saturating_sub(from) / per_step * interval_ms
        })?;
        if steps > 1 {
            update.lag = Some(serde_json::json!({
                "behind_steps": steps - 1, "lag_ms": behind.as_millis() as u64,
                "visible": update.blocks.last().map(|b| b.height),
            }));
        }
        self.next_tick += self.interval * steps as u32;
        self.next_tick_ms += interval_ms * steps;
        Ok(update)
    }
}

/// One step of a test script.
#[cfg(feature = "fixtures")]
pub enum ScriptStep {
    /// Make the journal visible through this height.
    Advance(u64),
    /// Replace the journal above `ancestor` with these blocks, as a node
    /// reorganization followed by ingest would.
    Reorg {
        ancestor: u64,
        blocks: Vec<(BlockHash, Vec<TransparentDisplayRecordV2x>)>,
    },
    /// Append these blocks to the journal and show them.
    Extend(Vec<(BlockHash, Vec<TransparentDisplayRecordV2x>)>),
    /// Roll the journal back to `ancestor`, then fail as a journal write
    /// would before ingesting the replacement: the controller exits.
    FailAfterRollback(u64),
}

#[cfg(feature = "fixtures")]
#[derive(Default)]
pub struct Script {
    steps: VecDeque<ScriptStep>,
}

#[cfg(feature = "fixtures")]
impl Script {
    pub fn new(steps: impl IntoIterator<Item = ScriptStep>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
        }
    }

    fn poll(
        &mut self,
        store: &mut EventStore,
        cache: &DisplayCache,
        floor: u64,
        before_rollback: BeforeRollback<'_>,
    ) -> Result<SourceUpdate, SourceError> {
        let mut roll_back = |store: &mut EventStore, ancestor: u64| {
            if ancestor < floor {
                return Err(SourceError::SealedReorg {
                    ancestor: i128::from(ancestor),
                    floor,
                });
            }
            let hash = store
                .block_at(ancestor)
                .ok_or_else(|| transient(format!("no journal block at {ancestor}")))?
                .block_hash;
            before_rollback(ancestor, hash).map_err(SourceError::Transient)?;
            Ok(store.rollback_to(Some(ancestor))?)
        };
        match self.steps.pop_front() {
            None => Ok(SourceUpdate::default()),
            Some(ScriptStep::Advance(through)) => {
                let update = from_journal(store, cache, floor, (through, BATCH), |_| now_ms())?;
                // A poll returns a bounded batch; the rest of the step follows.
                let visible = update.blocks.last().map(|b| b.height);
                if visible.is_some_and(|h| h < through) {
                    self.steps.push_front(ScriptStep::Advance(through));
                }
                Ok(update)
            }
            Some(ScriptStep::Reorg { ancestor, blocks }) => {
                roll_back(store, ancestor)?;
                append(store, blocks)?;
                // The cache learns of the rollback from the journal.
                from_journal(store, cache, floor, (u64::MAX, u64::MAX), |_| now_ms())
            }
            Some(ScriptStep::Extend(blocks)) => {
                append(store, blocks)?;
                from_journal(store, cache, floor, (u64::MAX, u64::MAX), |_| now_ms())
            }
            Some(ScriptStep::FailAfterRollback(ancestor)) => {
                roll_back(store, ancestor)?;
                Err(SourceError::Store(EventStoreError::Invariant(
                    "scripted journal failure".into(),
                )))
            }
        }
    }
}

/// Commits `blocks` after the journal's end, with the events their source
/// records imply.
#[cfg(feature = "fixtures")]
fn append(
    store: &mut EventStore,
    blocks: Vec<(BlockHash, Vec<TransparentDisplayRecordV2x>)>,
) -> Result<(), EventStoreError> {
    let next = store
        .covered_through()
        .map_or(store.start_height(), |h| h + 1);
    for (offset, (hash, sources)) in blocks.into_iter().enumerate() {
        let height = next + offset as u64;
        let events =
            transparent_shared::journal::display_journal::implied_events(height, &sources)?;
        store.append_block_with_display(height, hash, &events, &sources)?;
    }
    store.commit()
}

/// The source a controller runs with.
pub enum Source {
    Committed(CommittedSource),
    /// Replay, then optionally live once the replay end is visible.
    #[cfg(feature = "fixtures")]
    Replay(ReplaySource, Option<CommittedSource>),
    #[cfg(feature = "fixtures")]
    Script(Script),
}

impl Source {
    /// Resolves when the source has something to do. Cancel safe.
    pub async fn wait(&self) {
        match self {
            Self::Committed(committed) => committed.wait().await,
            #[cfg(feature = "fixtures")]
            Self::Replay(replay, _) => replay.wait().await,
            #[cfg(feature = "fixtures")]
            Self::Script(script) if !script.steps.is_empty() => {}
            #[cfg(feature = "fixtures")]
            Self::Script(_) => std::future::pending().await,
        }
    }

    /// Whether this source will never produce another block.
    #[cfg(feature = "fixtures")]
    pub fn exhausted(&self, cache: &DisplayCache) -> bool {
        match self {
            Self::Committed(_) => false,
            #[cfg(feature = "fixtures")]
            Self::Replay(replay, live) => live.is_none() && replay.exhausted(cache),
            #[cfg(feature = "fixtures")]
            Self::Script(script) => script.steps.is_empty(),
        }
    }

    /// The node's tip at the last live poll, if any.
    pub fn node_tip(&self) -> Option<u64> {
        match self {
            Self::Committed(committed) => committed.view.lock().unwrap().node_tip,
            #[cfg(feature = "fixtures")]
            Self::Replay(..) | Self::Script(_) => None,
        }
    }

    /// When the oldest block the node has shown but the journal does not
    /// hold yet was first seen, in Unix milliseconds.
    pub fn oldest_unfetched_ms(&self) -> Option<u64> {
        match self {
            Self::Committed(committed) => committed
                .view
                .lock()
                .unwrap()
                .first_seen
                .values()
                .min()
                .copied(),
            #[cfg(feature = "fixtures")]
            Self::Replay(..) | Self::Script(_) => None,
        }
    }

    pub fn mode(&self) -> &'static str {
        match self {
            Self::Committed(_) => "live",
            #[cfg(feature = "fixtures")]
            Self::Replay(_, None) => "replay",
            #[cfg(feature = "fixtures")]
            Self::Replay(_, Some(_)) => "replay-then-live",
            #[cfg(feature = "fixtures")]
            Self::Script(_) => "script",
        }
    }

    /// Whether the journal's writer has caught up with the observed tip;
    /// publication waits while it catches up.
    pub fn current(&self) -> bool {
        match self {
            Self::Committed(committed) => committed.view.lock().unwrap().current,
            #[cfg(feature = "fixtures")]
            Self::Replay(..) | Self::Script(_) => true,
        }
    }

    pub async fn poll(
        &mut self,
        store: &mut EventStore,
        cache: &DisplayCache,
        floor: u64,
        fence: &super::controller::RollbackFence,
        #[allow(unused_variables)] before_rollback: BeforeRollback<'_>,
    ) -> Result<SourceUpdate, SourceError> {
        #[cfg(feature = "fixtures")]
        if let Self::Replay(replay, live) = self {
            if replay.exhausted(cache) && live.is_some() {
                tracing::info!(
                    height = cache.next_height(),
                    "replay complete; following the node"
                );
                *self = Self::Committed(live.take().unwrap());
            }
        }
        match self {
            Self::Committed(committed) => committed.poll(store, cache, floor, fence),
            #[cfg(feature = "fixtures")]
            Self::Replay(replay, _) => replay.poll(store, cache, floor),
            #[cfg(feature = "fixtures")]
            Self::Script(script) => script.poll(store, cache, floor, before_rollback),
        }
    }
}

/// Opens the journal for writing at its recorded start. Opening with any
/// other start would move the journal aside (`EventStore::open`).
pub fn open_writer(dir: &std::path::Path) -> Result<EventStore, BoxError> {
    let existing = EventStore::open_existing(dir)?;
    let (genesis, start) = (existing.genesis_hash().to_string(), existing.start_height());
    drop(existing);
    Ok(EventStore::open(dir, &genesis, start)?)
}
