//! Where the controller's blocks come from.
//!
//! - [`LiveSource`] follows the node: it owns ingest for the display journal
//!   (`controller.rs`'s follow loop, without the history authority or fleet).
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

use super::cache::DisplayCache;
use super::now_ms;
use crate::events::{EventStore, EventStoreError};
use crate::ingest::build_fetched_block_events;
use crate::prevout::OutputCache;
use crate::publication::BoxError;
use crate::zakura::ZakuraClient;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};
use transparent_filter::BlockHash;
use transparent_shard::txid::TransparentDisplayRecord;

/// Most blocks one poll returns, so a catch-up still publishes as it goes.
const BATCH: u64 = 64;

pub struct SourceBlock {
    pub height: u64,
    pub hash: BlockHash,
    pub records: Vec<TransparentDisplayRecord>,
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

/// Follows the node and ingests into the journal.
pub struct LiveSource {
    rpc: ZakuraClient,
    poll: Duration,
    last_poll: Option<Instant>,
    outputs: OutputCache,
    /// When each height was first seen at or below the node's tip.
    first_seen: BTreeMap<u64, u64>,
}

impl LiveSource {
    pub fn new(rpc: ZakuraClient, poll: Duration) -> Self {
        Self {
            rpc,
            poll,
            last_poll: None,
            outputs: OutputCache::new(crate::prevout::DEFAULT_CACHE_OUTPUTS),
            first_seen: BTreeMap::new(),
        }
    }

    async fn wait(&self) {
        if let Some(last) = self.last_poll {
            tokio::time::sleep_until((last + self.poll).into()).await;
        }
    }

    fn observe(&mut self, from: u64, tip: u64) {
        let now = now_ms();
        for height in from..=tip {
            self.first_seen.entry(height).or_insert(now);
        }
    }

    async fn poll(
        &mut self,
        store: &mut EventStore,
        cache: &DisplayCache,
        floor: u64,
        before_rollback: BeforeRollback<'_>,
    ) -> Result<SourceUpdate, SourceError> {
        self.last_poll = Some(Instant::now());
        let journal_end = store
            .covered_through()
            .ok_or_else(|| transient("empty journal"))?;
        let diverged = cache
            .tip()
            .is_some_and(|(h, hash)| store.block_at(h).map(|b| b.block_hash) != Some(hash));
        if cache.next_height() <= journal_end || diverged {
            let now = now_ms();
            return from_journal(store, cache, floor, (journal_end, BATCH), |h| {
                self.first_seen.get(&h).copied().unwrap_or(now)
            });
        }
        let tip = self.rpc.tip_height().await.map_err(transient)?;
        if tip < floor {
            // A node that is behind is not a fork; nothing sealed is at stake.
            return Err(transient(format!(
                "node tip {tip} is below the sealed floor {floor}; waiting"
            )));
        }
        self.observe(cache.next_height(), tip);

        // Walk back to the highest journal block the node agrees with. The
        // walk never passes the floor: a fork there must not touch the journal.
        let mut ancestor = journal_end;
        let ours = loop {
            let ours = store
                .block_at(ancestor)
                .ok_or_else(|| transient("missing journal block"))?
                .block_hash;
            if ancestor <= tip
                && self.rpc.block_hash(ancestor).await.map_err(transient)? == ours.to_display_hex()
            {
                break ours;
            }
            if ancestor <= floor {
                return Err(SourceError::SealedReorg {
                    ancestor: i128::from(ancestor) - 1,
                    floor,
                });
            }
            ancestor -= 1;
        };
        let mut update = SourceUpdate::default();
        if ancestor != journal_end {
            before_rollback(ancestor, ours).map_err(SourceError::Transient)?;
            store.rollback_to(Some(ancestor))?;
            self.outputs = OutputCache::new(crate::prevout::DEFAULT_CACHE_OUTPUTS);
            // Replacement blocks are observed now, not when the orphans were.
            self.first_seen.split_off(&(ancestor + 1));
            self.observe(ancestor + 1, tip);
            update.rollback_to = Some(ancestor);
        }
        let mut next = ancestor + 1;
        while next <= tip.min(ancestor + BATCH) {
            match self.ingest(store, next).await {
                Ok((hash, records)) => update.blocks.push(SourceBlock {
                    height: next,
                    hash,
                    records,
                    observed_ms: self.first_seen.get(&next).copied().unwrap_or_else(now_ms),
                    ingested_ms: now_ms(),
                }),
                Err(SourceError::Transient(error)) => {
                    update.error = Some(error);
                    break;
                }
                Err(error) => return Err(error),
            }
            next += 1;
        }
        self.first_seen = self.first_seen.split_off(&next);
        if update.rollback_to.is_none() && update.blocks.is_empty() {
            if let Some(error) = update.error {
                return Err(SourceError::Transient(error));
            }
        }
        Ok(update)
    }

    /// Fetches, extracts and commits one block, as the history follow loop does.
    async fn ingest(
        &mut self,
        store: &mut EventStore,
        height: u64,
    ) -> Result<(BlockHash, Vec<TransparentDisplayRecord>), SourceError> {
        let fetched = self.rpc.block(height).await.map_err(transient)?;
        let parent = fetched.1.header.previous_block_hash.to_string();
        let expected = store
            .block_at(height - 1)
            .ok_or_else(|| transient("missing journal parent"))?
            .block_hash
            .to_display_hex();
        if parent != expected {
            return Err(transient("node reorganized while fetching block; retry"));
        }
        let built = build_fetched_block_events(&self.rpc, &mut self.outputs, height, fetched)
            .await
            .map_err(transient)?;
        if self.rpc.block_hash(height).await.map_err(transient)?
            != built.block_hash.to_display_hex()
        {
            return Err(transient("block became noncanonical during extraction"));
        }
        store.append_block_with_display(height, built.block_hash, &built.events, &built.display)?;
        store.commit()?;
        Ok((built.block_hash, built.display))
    }
}

/// Advances a visible tip over an ingested journal at a fixed pace.
pub struct ReplaySource {
    blocks_per_step: u64,
    interval: Duration,
    end: u64,
    next_tick: Instant,
    next_tick_ms: u64,
}

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
pub enum ScriptStep {
    /// Make the journal visible through this height.
    Advance(u64),
    /// Replace the journal above `ancestor` with these blocks, as a node
    /// reorganization followed by ingest would.
    Reorg {
        ancestor: u64,
        blocks: Vec<(BlockHash, Vec<TransparentDisplayRecord>)>,
    },
    /// Append these blocks to the journal and show them.
    Extend(Vec<(BlockHash, Vec<TransparentDisplayRecord>)>),
    /// Roll the journal back to `ancestor`, then fail as a journal write
    /// would before ingesting the replacement: the controller exits.
    FailAfterRollback(u64),
}

#[derive(Default)]
pub struct Script {
    steps: VecDeque<ScriptStep>,
}

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

/// Commits `blocks` after the journal's end.
fn append(
    store: &mut EventStore,
    blocks: Vec<(BlockHash, Vec<TransparentDisplayRecord>)>,
) -> Result<(), EventStoreError> {
    let next = store
        .covered_through()
        .map_or(store.start_height(), |h| h + 1);
    for (offset, (hash, records)) in blocks.into_iter().enumerate() {
        store.append_block_with_display(next + offset as u64, hash, &[], &records)?;
    }
    store.commit()
}

/// The source a controller runs with.
pub enum Source {
    Live(LiveSource),
    /// Replay, then optionally live once the replay end is visible.
    Replay(ReplaySource, Option<LiveSource>),
    Script(Script),
}

impl Source {
    /// Resolves when the source has something to do. Cancel safe.
    pub async fn wait(&self) {
        match self {
            Self::Live(live) => live.wait().await,
            Self::Replay(replay, _) => replay.wait().await,
            Self::Script(script) if !script.steps.is_empty() => {}
            Self::Script(_) => std::future::pending().await,
        }
    }

    /// Whether this source will never produce another block.
    pub fn exhausted(&self, cache: &DisplayCache) -> bool {
        match self {
            Self::Live(_) => false,
            Self::Replay(replay, live) => live.is_none() && replay.exhausted(cache),
            Self::Script(script) => script.steps.is_empty(),
        }
    }

    pub fn mode(&self) -> &'static str {
        match self {
            Self::Live(_) => "live",
            Self::Replay(_, None) => "replay",
            Self::Replay(_, Some(_)) => "replay-then-live",
            Self::Script(_) => "script",
        }
    }

    pub async fn poll(
        &mut self,
        store: &mut EventStore,
        cache: &DisplayCache,
        floor: u64,
        before_rollback: BeforeRollback<'_>,
    ) -> Result<SourceUpdate, SourceError> {
        if let Self::Replay(replay, live) = self {
            if replay.exhausted(cache) && live.is_some() {
                tracing::info!(
                    height = cache.next_height(),
                    "replay complete; following the node"
                );
                *self = Self::Live(live.take().unwrap());
            }
        }
        match self {
            Self::Live(live) => live.poll(store, cache, floor, before_rollback).await,
            Self::Replay(replay, _) => replay.poll(store, cache, floor),
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
