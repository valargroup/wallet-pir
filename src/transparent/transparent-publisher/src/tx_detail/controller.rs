//! The display controller loop.
//!
//! Each block from the source goes into the in-memory cache. The seal rule
//! runs over the cache and queues every seal it makes; queued seals are built
//! one at a time in the background (each chains its predecessor's digest).
//! A cycle then publishes: the built prefix of the queue becomes archives,
//! the recent shard is rebuilt over its window (every unsealed block,
//! reaching back over the newest archives until it holds `recent_window`
//! txids), a candidate is written with its prepared runtimes, and the HTTPS
//! distribution finalizes it for query servers to download.
//!
//! Sealed content never changes. A fork below the sealed floor, bytes that
//! differ under a published identity, or a map whose sealed entries are not
//! the previous ones (minus window drops) halts the controller: it writes
//! `halted.json`, logs `alert=txid_display_halt`, and stops publishing while
//! query servers keep serving the last finalized publication.
//!
//! A rollback withdraws the HTTPS publication and is recorded in
//! `invalidate.json` before the journal changes. A restart that finds the
//! journal off the activated chain without such a record rewrites everything
//! from the recent start.
mod preparation;
mod workflow;
use workflow::DetailPublication;

use zakura_pir_publisher::transparent as transparent_shard;
use zakura_pir_publisher::transparent::filter as transparent_filter;

use super::cache::DisplayCache;
use super::metrics::{Gauges, Metrics};
use super::now_ms;
use super::publisher::{
    collect, probe_hard_links, publish_parts, read_map, verify_dir, write_candidate, ActiveRecord,
    DisplayRoot, PublishError, PublishedShard, SealRecord, ShardSpec, HALTED_FILE, MANIFEST_FILE,
};
use super::source::{Source, SourceError, SourceUpdate};
use super::timeline::{HeightIndex, Timeline};
use crate::history::publication::{write_atomic, BoxError, Journal};
use pir_pipeline::Input;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use transparent_filter::BlockHash;
use transparent_shard::display::{
    height_counts, plan_seals, recent_start, window_start, DisplayManifest, DisplayMap,
    DisplayMapEntry, DisplaySealParams,
};
use transparent_shard::manifest::PublishedRevision;
use transparent_shared::journal::events::EventStore;
use transparent_shared::runtime::disk::ShippedRuntimes;
use transparent_shared::tx_detail::prebuild::Shipped;

const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Consecutive failed cycles after which publication counts as stalled.
const STALL_FAILURES: u32 = 5;
/// How often a standing recent-shard bound alert repeats.
const LAG_ALERT_EVERY: Duration = Duration::from_secs(60);
/// The lowest rollback since the last activation.
pub const INVALIDATE_FILE: &str = "invalidate.json";

/// Builds a revision's runtimes into a candidate directory; see
/// [`transparent_shared::tx_detail::prebuild::build_shipped`].
pub type Prebuild = fn(&Path, &Path) -> Result<Vec<Shipped>, String>;

pub struct Settings {
    /// Skip re-verifying sealed revisions at startup.
    pub trust_sealed: bool,
    /// The HTTPS publication query servers download. Every live publisher
    /// has one; controller tests without a node leave it out.
    pub distribution: Option<super::distribution::Distribution>,
    /// Builds the recent revision's runtimes into each candidate before it is
    /// finalized. A failed build is counted, and finalization builds them.
    pub prebuild: Prebuild,
    /// Unused candidates kept besides the active one.
    pub retain_candidates: usize,
    /// Above this many unsealed records in the cache, a `lag` alert repeats
    /// every minute: the next recent shard would hold them all.
    pub max_recent_records: Option<u64>,
    /// Return once a finite source is exhausted and fully published.
    pub exit_when_idle: bool,
    /// Keep every activated map in memory, for tests.
    pub record_maps: bool,
    pub status: Arc<RwLock<Value>>,
    /// What `/metrics` renders; see [`super::metrics`].
    pub metrics: Arc<Metrics>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            trust_sealed: false,
            distribution: None,
            prebuild: transparent_shared::tx_detail::prebuild::build_shipped,
            retain_candidates: 3,
            max_recent_records: None,
            exit_when_idle: false,
            record_maps: false,
            status: Arc::new(RwLock::new(json!({"phase": "starting"}))),
            metrics: Arc::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The source is exhausted and everything it produced is published.
    Idle,
    Halted(String),
}

/// Why sealing stopped. Each is a hard error by design.
#[derive(Clone, Debug, thiserror::Error)]
pub enum Halt {
    #[error("reorg ancestor {ancestor} is below the sealed floor {floor}")]
    SealedReorg { ancestor: i128, floor: u64 },
    #[error("{0}")]
    Immutable(String),
    #[error("sealed shard content changed: {0}")]
    SealedChanged(String),
    #[error("sealed map entries changed: {0}")]
    SealedEntryChanged(String),
}

// A prepared publication retains every ordered seal at the queue head. It
// borrows its controller through `advance`; no store or seal queue is copied.
struct DisplayMaterial {
    started: Instant,
    tip: u64,
    tip_hash: String,
    new_seals: Vec<SealRecord>,
    dropped: Vec<DisplayMapEntry>,
    floor: u64,
    unsealed: u64,
    recent_start: u64,
    recent_id: u64,
    recent: PublishedShard,
    map: DisplayMap,
}

struct DisplayCandidate {
    material: DisplayMaterial,
    candidate: PathBuf,
    sha: String,
    candidate_ms: u64,
    prebuild: Option<(u64, Vec<Shipped>)>,
}
/// Prepared HTTP material and the authority revision under which it was built.
struct DisplayActivation {
    distribution: super::distribution::Distribution,
    revision: Option<u64>,
    finalized: transparent_shared::tx_detail::distribution::Finalized,
}

#[derive(Debug, thiserror::Error)]
enum CycleError {
    #[error("{0}")]
    Halt(Halt),
    #[error("{0}")]
    Retry(String),
}

impl From<PublishError> for CycleError {
    fn from(error: PublishError) -> Self {
        match error {
            PublishError::Immutable(e) => Self::Halt(Halt::Immutable(e)),
            PublishError::Revision(e) => Self::Halt(Halt::SealedChanged(e.to_string())),
            PublishError::Other(e) => Self::Retry(e),
        }
    }
}

fn retry(error: impl std::fmt::Display) -> CycleError {
    CycleError::Retry(error.to_string())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JobState {
    Planned,
    Building,
    Built,
}

/// One seal from decision to publication.
struct SealJob {
    shard_id: u64,
    start: u64,
    end: u64,
    decided_tip: u64,
    bucket_counts: Vec<u64>,
    state: JobState,
    shard: Option<PublishedShard>,
    attempts: u32,
    retry_at: Option<Instant>,
}

impl SealJob {
    fn record(&self) -> SealRecord {
        let shard = self.shard.as_ref().expect("a publishable seal is built");
        SealRecord {
            shard_id: self.shard_id,
            start_height: self.start,
            end_height: self.end,
            digest: shard.digest.clone(),
            terminal_block_hash: shard.manifest.terminal_block_hash.clone(),
            decided_tip: self.decided_tip,
        }
    }

    fn ready(&self, now: Instant) -> bool {
        self.retry_at.is_none_or(|t| now >= t)
    }

    fn summary(&self) -> Value {
        json!({"shard_id": self.shard_id, "start": self.start, "end": self.end,
            "decided_tip": self.decided_tip, "state": format!("{:?}", self.state).to_lowercase(),
            "digest": self.shard.as_ref().map(|s| s.digest.clone())})
    }
}

struct Background {
    shard_id: u64,
    result: Result<Box<PublishedShard>, PublishError>,
}

fn backoff(attempts: u32) -> Duration {
    Duration::from_secs(1u64 << attempts.min(5)).min(MAX_BACKOFF)
}

fn ms(started: Instant) -> u64 {
    started.elapsed().as_millis() as u64
}

/// The lowest rollback since a candidate was last finalized, made while
/// `map_sha256` was active. Its ancestor was on that map's chain, so a
/// journal that still holds it agrees with everything published through it
/// below the ancestor.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct RollbackRecord {
    map_sha256: String,
    ancestor: u64,
    hash: String,
}

/// `invalidate.json`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
struct Invalidations {
    rollback: Option<RollbackRecord>,
}

impl Invalidations {
    fn load(root: &Path) -> Result<Self, BoxError> {
        match std::fs::read(root.join(INVALIDATE_FILE)) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error.into()),
        }
    }

    fn store(&self, root: &Path) -> Result<(), BoxError> {
        write_atomic(
            &root.join(INVALIDATE_FILE),
            &serde_json::to_vec_pretty(self)?,
        )
    }
}

/// Keeps the lowest rollback under the active map.
fn note_rollback(
    record: &mut Option<RollbackRecord>,
    map_sha256: &str,
    ancestor: u64,
    hash: BlockHash,
) {
    if record
        .as_ref()
        .is_some_and(|r| r.map_sha256 == map_sha256 && r.ancestor <= ancestor)
    {
        return;
    }
    *record = Some(RollbackRecord {
        map_sha256: map_sha256.to_string(),
        ancestor,
        hash: hash.to_display_hex(),
    });
}

/// Checks that `next` lists the sealed entries of `previous` unchanged, after
/// dropping the oldest `dropped` of them, before anything new.
pub fn check_sealed_continuity(
    previous: &DisplayMap,
    next: &DisplayMap,
    dropped: usize,
) -> Result<(), String> {
    let before: Vec<&DisplayMapEntry> = previous.shards.iter().filter(|s| s.sealed).collect();
    let after: Vec<&DisplayMapEntry> = next.shards.iter().filter(|s| s.sealed).collect();
    let kept = &before[dropped.min(before.len())..];
    if after.len() < kept.len() || after[..kept.len()] != *kept {
        return Err(format!(
            "{} archives kept from the previous map are not listed unchanged",
            kept.len()
        ));
    }
    Ok(())
}

/// The lowest height the cache must hold so the recent window can be filled
/// at any tip: the latest start at which the sealed blocks
/// `[start, unsealed - 1]` alone hold the window, or `floor`, the map's start,
/// when they never do. Read backwards from the journal.
pub fn cache_floor(
    store: &impl Journal,
    seal: &DisplaySealParams,
    floor: u64,
    unsealed: u64,
) -> Result<u64, BoxError> {
    let mut reversed = Vec::new();
    let mut sums = vec![0u64; seal.n_recent as usize];
    let mut height = unsealed;
    while height > floor && sums.iter().any(|s| *s < seal.recent_window) {
        height -= 1;
        let counts = height_counts(seal, &store.display_at(height)?);
        for (sum, count) in sums.iter_mut().zip(&counts.recent) {
            *sum += u64::from(*count);
        }
        reversed.push(counts);
    }
    reversed.reverse();
    Ok(window_start(seal, height, &reversed))
}

pub struct Controller {
    root: PathBuf,
    layout: DisplayRoot,
    store: EventStore,
    source: Source,
    cache: DisplayCache,
    index: HeightIndex,
    timeline: Timeline,
    settings: Settings,
    active: ActiveRecord,
    map: DisplayMap,
    /// The published recent revision and its `(shard_id, start)` lineage key.
    recent_previous: Option<(u64, u64, PublishedRevision)>,
    published_tip: (u64, String),
    queue: VecDeque<SealJob>,
    building: bool,
    sender: mpsc::UnboundedSender<Background>,
    receiver: mpsc::UnboundedReceiver<Background>,
    /// When each not yet published height was first observed.
    observed: BTreeMap<u64, u64>,
    halted: Option<String>,
    failures: u32,
    retry_at: Option<Instant>,
    rollback: Option<RollbackRecord>,
    /// What `invalidate.json` holds.
    saved_invalidations: Invalidations,
    lag_alerted: Option<Instant>,
    /// Cycles whose runtime prebuild failed since this process started.
    prebuild_failures: u64,
    /// The stop request of the running loop. Candidate preparation observes
    /// it between its blocking steps and ends without activating; once
    /// finalization begins, the cycle finishes its commit.
    shutdown: pir_pipeline::service::Shutdown,
    last_cycle: Value,
    /// Unix milliseconds of the last activation by a cycle, and of the last
    /// seal built; 0 before the first since this start.
    last_success_ms: u64,
    last_seal_ms: u64,
    maps: Vec<DisplayMap>,
    _lock: std::fs::File,
}

impl Controller {
    /// Recovers a bootstrapped root from `active.json`.
    ///
    /// Sealed terminals must still be the journal's blocks, and sealed
    /// revisions must verify from disk (unless trusted). The cache is reloaded
    /// from its window floor (see [`cache_floor`]) through the activated tip;
    /// queued seals are planned again from it and reproduce the same digests.
    /// A journal that no longer holds the activated tip is rewritten from the
    /// recorded rollback, or from the recent start without one.
    pub fn open(
        root: &Path,
        store: EventStore,
        source: Source,
        settings: Settings,
    ) -> Result<Self, BoxError> {
        // Candidate and active paths are recorded absolute.
        let root = &std::fs::canonicalize(root)?;
        let layout = DisplayRoot::load(root)?;
        layout.validate()?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("controller.lock"))?;
        lock.try_lock()
            .map_err(|e| format!("another display controller owns this root: {e}"))?;
        probe_hard_links(root)?;
        if store.genesis_hash() != layout.genesis_hash {
            return Err("journal genesis differs from the display root".into());
        }
        let timeline = Timeline::open(root)?;
        let halted = std::fs::read(root.join(HALTED_FILE)).ok().map(|bytes| {
            serde_json::from_slice::<Value>(&bytes)
                .ok()
                .and_then(|v| v["reason"].as_str().map(str::to_string))
                .unwrap_or_else(|| "halted.json is present".into())
        });
        let active = ActiveRecord::load(root)?;
        let map = read_map(&active.directory, &active.map_sha256)?;
        if map.seal != layout.seal || map.genesis_hash != layout.genesis_hash {
            return Err("the active map's parameters differ from the display root".into());
        }
        let recent = map
            .shards
            .last()
            .filter(|s| !s.sealed)
            .ok_or("the active map has no recent shard")?
            .clone();
        let sealed: Vec<&DisplayMapEntry> = map.shards.iter().filter(|s| s.sealed).collect();
        let history: Vec<&str> = active.seals.iter().map(|s| s.digest.as_str()).collect();
        if !history.ends_with(
            &sealed
                .iter()
                .map(|s| s.manifest_digest.as_str())
                .collect::<Vec<_>>(),
        ) {
            return Err("active.json seal history does not end with the map's archives".into());
        }
        let manifest: DisplayManifest = serde_json::from_slice(&std::fs::read(
            active
                .directory
                .join(&recent.manifest_digest)
                .join(MANIFEST_FILE),
        )?)?;
        let recent_previous = Some((
            recent.shard_id,
            recent.start_height,
            PublishedRevision {
                digest: recent.manifest_digest.clone(),
                revision: manifest.revision,
                supersedes: manifest.supersedes,
                sealed: false,
            },
        ));
        // A journal rolled back below the activated tip (a crash mid-reorg)
        // reloads what it still has; the next cycle publishes from there.
        let through = active.tip.min(store.covered_through().unwrap_or(0));
        let unsealed = active
            .seals
            .last()
            .map_or(layout.start_height, |s| s.end_height + 1);
        let floor =
            cache_floor(&store, &layout.seal, map.start_height, unsealed)?.min(recent.start_height);
        let cache = DisplayCache::load(&store, layout.seal, floor, through, now_ms())?;
        let index = HeightIndex::open(root)?;
        let saved_invalidations = Invalidations::load(root)?;
        let journal_hash = |height: u64| {
            store
                .block_at(height)
                .map(|b| b.block_hash.to_display_hex())
        };
        // Off the activated chain, the activated publication holds blocks the
        // journal no longer does. A recorded rollback whose ancestor the
        // journal still holds bounds where they differ. Otherwise it is
        // unknown, but sealed terminals are checked below, so everything from
        // the recent start is enough.
        let off_chain = journal_hash(active.tip).as_deref() != Some(active.tip_hash.as_str());
        let recorded = saved_invalidations
            .rollback
            .as_ref()
            .filter(|r| {
                r.map_sha256 == active.map_sha256
                    && journal_hash(r.ancestor).as_deref() == Some(r.hash.as_str())
            })
            .map(|r| r.ancestor + 1);
        let replaced_from = match (off_chain, recorded) {
            (false, _) => None,
            (true, Some(from)) => Some(from),
            (true, None) => Some(recent.start_height),
        };
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut controller = Self {
            root: root.to_path_buf(),
            published_tip: (active.tip, active.tip_hash.clone()),
            layout,
            store,
            source,
            cache,
            index,
            timeline,
            settings,
            active,
            map,
            recent_previous,
            queue: VecDeque::new(),
            building: false,
            sender,
            receiver,
            observed: BTreeMap::new(),
            halted,
            failures: 0,
            retry_at: None,
            rollback: saved_invalidations.rollback.clone(),
            saved_invalidations,
            lag_alerted: None,
            prebuild_failures: 0,
            shutdown: Default::default(),
            last_cycle: Value::Null,
            last_success_ms: 0,
            last_seal_ms: 0,
            maps: Vec::new(),
            _lock: lock,
        };
        if controller.halted.is_none() {
            match controller.check_sealed() {
                Ok(()) => controller.sync_index(replaced_from)?,
                Err(halt) => controller.halt(halt),
            }
        }
        if let Some(from) = replaced_from {
            controller.timeline.record(
                "reorg",
                json!({"from_height": from, "old_tip": controller.active.tip,
                    "journal_end": controller.store.covered_through(),
                    "recovered": if recorded.is_some() { "recorded" } else { "recent_start" }}),
            );
            controller.save_invalidations();
        }
        Ok(controller)
    }

    /// Sealed terminals against the journal, and sealed bytes against their
    /// digests.
    fn check_sealed(&self) -> Result<(), Halt> {
        let journal_hash = |height: u64| {
            self.store
                .block_at(height)
                .map(|b| b.block_hash.to_display_hex())
        };
        if journal_hash(self.layout.start_height - 1).as_deref() != Some(&self.layout.base_parent) {
            return Err(Halt::SealedReorg {
                ancestor: i128::from(self.layout.start_height) - 2,
                floor: self.layout.start_height - 1,
            });
        }
        for entry in self.map.shards.iter().filter(|s| s.sealed) {
            if journal_hash(entry.end_height).as_deref() != Some(&entry.terminal_block_hash) {
                return Err(Halt::SealedReorg {
                    ancestor: i128::from(entry.end_height) - 1,
                    floor: entry.end_height,
                });
            }
            if !self.settings.trust_sealed {
                let directory = self
                    .root
                    .join(super::publisher::SEALED_DIR)
                    .join(&entry.manifest_digest);
                let manifest = verify_dir(&directory, &entry.manifest_digest)
                    .map_err(|e| Halt::Immutable(format!("{}: {e}", directory.display())))?;
                if !entry.describes(&manifest) {
                    return Err(Halt::SealedEntryChanged(format!(
                        "shard {} manifest differs from its map entry",
                        entry.shard_id
                    )));
                }
            }
        }
        Ok(())
    }

    /// Brings the tooling index to the cache tip: drops anything above it,
    /// then rewrites the last indexed height (which may be torn) onwards, or
    /// from `rewrite_from` when that is lower, for heights a rollback the
    /// index never saw may have replaced.
    fn sync_index(&mut self, rewrite_from: Option<u64>) -> Result<(), BoxError> {
        let tip = self.cache.next_height() - 1;
        self.index.truncate_after(tip)?;
        let from = match self.index.last_height()? {
            Some(last) => {
                let from = rewrite_from.map_or(last, |h| h.min(last));
                self.index.truncate_after(from - 1)?;
                from
            }
            None => self.layout.start_height,
        };
        for height in from..=tip {
            match self.cache.block(height) {
                Some(block) => self.index.append(height, &block.records)?,
                None => self.index.append(height, &self.store.display_at(height)?)?,
            }
        }
        Ok(())
    }

    pub fn store(&self) -> &EventStore {
        &self.store
    }

    pub fn active(&self) -> &ActiveRecord {
        &self.active
    }

    pub fn map(&self) -> &DisplayMap {
        &self.map
    }

    /// Every map activated by this controller, oldest first, when recorded.
    pub fn maps(&self) -> &[DisplayMap] {
        &self.maps
    }

    fn halt(&mut self, halt: Halt) {
        let reason = halt.to_string();
        tracing::error!(
            alert = "txid_display_halt",
            %reason,
            "display controller halted; query servers keep serving the last publication"
        );
        let record = json!({"reason": reason, "unix_ms": now_ms(),
            "tip": self.cache.tip().map(|t| t.0), "map_sha256": self.active.map_sha256});
        if let Err(error) = write_atomic(
            &self.root.join(HALTED_FILE),
            &serde_json::to_vec_pretty(&record).unwrap(),
        ) {
            tracing::error!(%error, "could not write halted.json");
        }
        self.timeline.record("halt", record);
        self.halted = Some(reason);
        self.update_status();
    }

    /// The first height no published archive holds.
    fn unsealed_start(&self) -> u64 {
        self.active
            .seals
            .last()
            .map_or(self.layout.start_height, |s| s.end_height + 1)
    }

    /// The highest height a fork may not reach below: the last published or
    /// queued seal's end, or the display start's parent.
    fn sealed_floor(&self) -> u64 {
        self.queue
            .back()
            .map(|j| j.end)
            .or(self.active.seals.last().map(|s| s.end_height))
            .unwrap_or(self.layout.start_height - 1)
    }

    /// Built seals at the queue's head that the rule still makes at the
    /// cache tip. A rollback inside the margin can undo a decision: such a
    /// seal keeps its range and digest but waits, so a published archive
    /// always ends at least the reorg margin below the tip.
    fn publishable(&self) -> usize {
        let built = self
            .queue
            .iter()
            .take_while(|j| j.state == JobState::Built)
            .count();
        if built == 0 {
            return 0;
        }
        // The queue starts at the first unsealed height, so ranges line up.
        let start = self.unsealed_start();
        built.min(plan_seals(&self.layout.seal, start, self.cache.counts_from(start)).len())
    }

    fn needs_cycle(&self) -> bool {
        let moved = self
            .cache
            .tip()
            .is_some_and(|(h, hash)| (h, hash.to_display_hex()) != self.published_tip);
        moved || self.publishable() > 0
    }

    fn idle(&self) -> bool {
        self.source.exhausted(&self.cache)
            // A built seal the rule no longer makes waits for blocks.
            && self.queue.iter().all(|j| j.state == JobState::Built)
            && !self.building
            && !self.needs_cycle()
    }

    /// Runs until halted, or until idle when the settings ask for it.
    /// Journal write failures return an error: restart from its checkpoint.
    pub async fn run<E: pir_pipeline::publisher::PublisherExecutor>(
        &mut self,
        executor: E,
    ) -> Result<Outcome, BoxError> {
        self.run_until(executor, pir_pipeline::service::Shutdown::default())
            .await
    }
    pub async fn run_until<E: pir_pipeline::publisher::PublisherExecutor>(
        &mut self,
        executor: E,
        shutdown: pir_pipeline::service::Shutdown,
    ) -> Result<Outcome, BoxError> {
        self.shutdown = shutdown.clone();
        let result = self.run_loop(executor, shutdown).await;
        // Every return path waits for the seal builder before dropping the
        // controller and its exclusive root/journal ownership.
        while self.building {
            if let Some(event) = self.receiver.recv().await {
                self.background(event);
            }
        }
        result
    }
    async fn run_loop<E: pir_pipeline::publisher::PublisherExecutor>(
        &mut self,
        executor: E,
        shutdown: pir_pipeline::service::Shutdown,
    ) -> Result<Outcome, BoxError> {
        if let Some(reason) = &self.halted {
            tracing::error!(alert = "txid_display_halt", %reason, "display root is halted");
            self.update_status();
            return Ok(Outcome::Halted(reason.clone()));
        }
        if let Some(distribution) = &self.settings.distribution {
            // A reorg while stopped can orphan the saved tip. Keep HTTP
            // authority withdrawn while the source loop repairs that view.
            // A stop ends the restore before it commits; nothing was activated.
            match distribution
                .restore_until(
                    executor.clone(),
                    self.active.directory.clone(),
                    vec![self.active.directory.clone()],
                    shutdown.clone(),
                )
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    tracing::warn!("saved display publication is noncanonical; rebuilding")
                }
                Err(_) if shutdown.requested() => {
                    tracing::info!("display distribution restore stopped by shutdown");
                    return Ok(Outcome::Idle);
                }
                Err(error) => return Err(error),
            }
        }
        self.plan();
        executor
            .run(
                DetailPublication {
                    controller: self,
                    wake: None,
                },
                shutdown,
            )
            .await?;
        Ok(match &self.halted {
            Some(reason) => Outcome::Halted(reason.clone()),
            None => Outcome::Idle,
        })
    }

    async fn poll_source(&mut self) -> Result<(), BoxError> {
        let floor = self.sealed_floor();
        let distribution = self.settings.distribution.clone();
        let (rollback, saved, root, active) = (
            &mut self.rollback,
            &mut self.saved_invalidations,
            &self.root,
            &self.active.map_sha256,
        );
        let mut before_rollback = |ancestor: u64, hash: BlockHash| {
            if let Some(distribution) = &distribution {
                distribution
                    .withdraw(ancestor + 1)
                    .map_err(|e| e.to_string())?;
            }
            note_rollback(rollback, active, ancestor, hash);
            let next = Invalidations {
                rollback: rollback.clone(),
            };
            next.store(root)
                .map_err(|e| format!("recording the invalidation from {}: {e}", ancestor + 1))?;
            *saved = next;
            Ok(())
        };
        let polled = pir_pipeline::acquire(
            async {
                self.source
                    .poll(&mut self.store, &self.cache, floor, &mut before_rollback)
                    .await
                    .map(Input::Work)
            },
            &mut (),
        )
        .await;
        match polled {
            Ok(Input::Work(update)) => self.apply(update).await?,
            Ok(Input::Unchanged | Input::Deferred) => {}
            Err(SourceError::SealedReorg { ancestor, floor }) => {
                self.timeline.record(
                    "reorg",
                    json!({"ancestor": ancestor.to_string(), "floor": floor, "sealed": true}),
                );
                self.halt(Halt::SealedReorg { ancestor, floor });
            }
            Err(SourceError::Store(error)) => return Err(error.into()),
            Err(SourceError::Transient(error)) => {
                self.timeline
                    .record("error", json!({"stage": "source", "error": error}));
            }
        }
        Ok(())
    }

    async fn apply(&mut self, update: SourceUpdate) -> Result<(), BoxError> {
        if let Some(ancestor) = update.rollback_to {
            let old_tip = self.cache.tip().map(|t| t.0);
            self.timeline.record(
                "reorg",
                json!({"ancestor": ancestor, "from_height": ancestor + 1, "old_tip": old_tip,
                    "depth": old_tip.map(|t| t.saturating_sub(ancestor))}),
            );
            // Sources that roll the journal back have recorded this already.
            if let Some(block) = self.store.block_at(ancestor) {
                note_rollback(
                    &mut self.rollback,
                    &self.active.map_sha256,
                    ancestor,
                    block.block_hash,
                );
            }
            self.save_invalidations();
            self.cache.truncate_after(ancestor);
            if let Err(error) = self.index.truncate_after(ancestor) {
                tracing::error!(%error, "height index truncation failed");
            }
            self.observed.split_off(&(ancestor + 1));
        }
        for block in update.blocks {
            if let Err(error) = self.index.append(block.height, &block.records) {
                tracing::error!(%error, "height index append failed");
            }
            self.timeline.record(
                "block",
                json!({"height": block.height, "hash": block.hash.to_display_hex(),
                    "observed_ms": block.observed_ms, "ingested_ms": block.ingested_ms,
                    "records": block.records.len()}),
            );
            self.observed.insert(block.height, block.observed_ms);
            self.cache
                .push(block.height, block.hash, block.records, block.observed_ms)?;
        }
        if let Some(error) = update.error {
            self.timeline
                .record("error", json!({"stage": "source", "error": error}));
        }
        if let Some(lag) = update.lag {
            self.timeline.record("lag", lag);
        }
        self.plan();
        Ok(())
    }

    /// Alerts once cycles keep failing: query servers keep serving the last
    /// publication, so nothing else shows that the tip stopped advancing.
    fn stalled(&self, error: &str) {
        if self.failures < STALL_FAILURES {
            return;
        }
        tracing::error!(
            alert = "txid_display_publication_stalled",
            failures = self.failures,
            published_tip = self.published_tip.0,
            tip = self.cache.tip().map(|t| t.0),
            %error,
            "display publication keeps failing; query servers serve the last publication"
        );
        if self.failures == STALL_FAILURES {
            self.timeline.record(
                "stall",
                json!({"failures": self.failures, "published_tip": self.published_tip.0,
                    "tip": self.cache.tip().map(|t| t.0), "error": error,
                    "queued": self.queue.len()}),
            );
        }
    }

    /// Alerts while the unsealed range outgrows its bound, whether or not
    /// cycles succeed: the next recent shard would hold all of it.
    fn check_recent_bound(&mut self) {
        let Some(max) = self.settings.max_recent_records else {
            return;
        };
        let records = self.cache.record_count_from(self.unsealed_start());
        if records <= max {
            self.lag_alerted = None;
            return;
        }
        if self
            .lag_alerted
            .is_some_and(|t| t.elapsed() < LAG_ALERT_EVERY)
        {
            return;
        }
        self.lag_alerted = Some(Instant::now());
        tracing::error!(
            alert = "txid_display_recent_lag",
            records,
            max,
            queued = self.queue.len(),
            "unsealed records exceed the recent bound; sealing is behind"
        );
        self.timeline.record(
            "lag",
            json!({"reason": "recent_records", "records": records, "max": max,
                "queued": self.queue.len(), "published_tip": self.published_tip.0}),
        );
    }

    /// Queues every seal the rule makes over the unqueued part of the cache.
    fn plan(&mut self) {
        let Some((tip, _)) = self.cache.tip() else {
            return;
        };
        let from = self
            .queue
            .back()
            .map_or_else(|| self.unsealed_start(), |j| j.end + 1);
        let next_id = self.queue.back().map_or_else(
            || self.active.seals.last().map_or(0, |s| s.shard_id + 1),
            |j| j.shard_id + 1,
        );
        let seals = plan_seals(&self.layout.seal, from, self.cache.counts_from(from));
        for (shard_id, range) in (next_id..).zip(seals) {
            self.queue.push_back(SealJob {
                shard_id,
                start: *range.start(),
                end: *range.end(),
                decided_tip: tip,
                bucket_counts: self.cache.archive_counts(range),
                state: JobState::Planned,
                shard: None,
                attempts: 0,
                retry_at: None,
            });
        }
    }

    /// Starts the next seal build, one at a time.
    fn schedule(&mut self) {
        let now = Instant::now();
        if !self.building {
            if let Some(index) = self
                .queue
                .iter()
                .position(|j| j.state == JobState::Planned)
                .filter(|i| self.queue[*i].ready(now))
            {
                // Builds run in order, so every earlier job is built already.
                let parent = match index {
                    0 => self
                        .active
                        .seals
                        .last()
                        .map(|s| (s.digest.clone(), s.terminal_block_hash.clone())),
                    _ => self.queue[index - 1]
                        .shard
                        .as_ref()
                        .map(|s| (s.digest.clone(), s.manifest.terminal_block_hash.clone())),
                };
                let job = &self.queue[index];
                let Some(terminal) = self.cache.block(job.end).map(|b| b.hash.to_display_hex())
                else {
                    return;
                };
                let spec = ShardSpec {
                    shard_id: job.shard_id,
                    start: job.start,
                    end: job.end,
                    parent_block_hash: parent
                        .as_ref()
                        .map_or(self.layout.base_parent.clone(), |p| p.1.clone()),
                    terminal_block_hash: terminal,
                    parent_manifest_digest: parent.map_or(String::new(), |p| p.0),
                    sealed: true,
                    previous: None,
                };
                let parts = self.cache.records(job.start..=job.end);
                let (root, layout, sender) =
                    (self.root.clone(), self.layout.clone(), self.sender.clone());
                let shard_id = job.shard_id;
                self.queue[index].state = JobState::Building;
                self.building = true;
                tokio::spawn(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        publish_parts(&root, &layout, &spec, &parts).map(Box::new)
                    })
                    .await
                    .unwrap_or_else(|e| Err(PublishError::Other(e.to_string())));
                    let _ = sender.send(Background { shard_id, result });
                });
            }
        }
    }

    fn background(&mut self, Background { shard_id, result }: Background) {
        self.building = false;
        let Some(job) = self.queue.iter_mut().find(|j| j.shard_id == shard_id) else {
            return;
        };
        match result {
            Ok(shard) => {
                job.state = JobState::Built;
                job.shard = Some(*shard);
                job.attempts = 0;
                job.retry_at = None;
                self.timeline.record("seal", seal_event(job));
                self.last_seal_ms = now_ms();
            }
            Err(PublishError::Other(error)) => {
                job.state = JobState::Planned;
                job.retry_at = Some(Instant::now() + backoff(job.attempts));
                job.attempts += 1;
                self.timeline.record(
                    "error",
                    json!({"stage": "seal_build", "shard_id": shard_id, "error": error}),
                );
            }
            Err(error) => {
                if let CycleError::Halt(halt) = CycleError::from(error) {
                    self.halt(halt);
                }
            }
        }
    }

    /// Build the recent shard and map from the committed cache and ordered seals.
    async fn build_cycle(&mut self) -> Result<DisplayMaterial, CycleError> {
        let started = Instant::now();
        let (tip, tip_hash) = self.cache.tip().ok_or_else(|| retry("empty cache"))?;
        let tip_hash = tip_hash.to_display_hex();
        let count = self.publishable();
        let built: Vec<&SealJob> = self.queue.iter().take(count).collect();
        let new_seals: Vec<SealRecord> = built.iter().map(|j| j.record()).collect();
        let mut archives: Vec<DisplayMapEntry> = self
            .map
            .shards
            .iter()
            .filter(|s| s.sealed)
            .cloned()
            .chain(
                built
                    .iter()
                    .map(|j| j.shard.as_ref().unwrap().entry.clone()),
            )
            .collect();
        let excess = archives
            .len()
            .saturating_sub(self.layout.max_archive_shards as usize);
        let dropped: Vec<DisplayMapEntry> = archives.drain(..excess).collect();

        let last = new_seals.last().or(self.active.seals.last());
        let recent_id = last.map_or(0, |s| s.shard_id + 1);
        let unsealed = last.map_or(self.layout.start_height, |s| s.end_height + 1);
        if unsealed
            != self.unsealed_start()
                + new_seals
                    .iter()
                    .map(|s| s.end_height + 1 - s.start_height)
                    .sum::<u64>()
        {
            return Err(retry("queued seals are not contiguous with the archives"));
        }
        // The recent window reaches back over the newest archives, but never
        // below the map's start or the cache's.
        let floor = archives
            .first()
            .map_or(self.layout.start_height, |a| a.start_height)
            .max(self.cache.start());
        let recent_start = recent_start(
            &self.layout.seal,
            floor,
            unsealed,
            self.cache.counts_from(floor),
        );
        // The block before the window lies at or below the sealed floor, so
        // the journal's hash for it never changes.
        let parent_block_hash = match last {
            Some(seal) if seal.end_height + 1 == recent_start => seal.terminal_block_hash.clone(),
            _ if recent_start == self.layout.start_height => self.layout.base_parent.clone(),
            _ => self
                .store
                .block_at(recent_start - 1)
                .ok_or_else(|| retry(format!("journal has no block at {}", recent_start - 1)))?
                .block_hash
                .to_display_hex(),
        };
        let spec = ShardSpec {
            shard_id: recent_id,
            start: recent_start,
            end: tip,
            parent_block_hash,
            terminal_block_hash: tip_hash.clone(),
            parent_manifest_digest: last.map_or(String::new(), |s| s.digest.clone()),
            sealed: false,
            // The recent shard keeps one revision lineage per shard id while
            // its window slides.
            previous: self
                .recent_previous
                .as_ref()
                .filter(|(id, _, _)| *id == recent_id)
                .map(|(_, _, p)| p.clone()),
        };
        // A stop abandons the unactivated candidate as a normal outcome.
        // Each blocking step is awaited; publishing the recent shard and
        // prebuilding its runtimes each finish once started.
        let stopped = |controller: &Self| {
            let stopped = controller.shutdown.requested();
            if stopped {
                tracing::info!(tip, "display candidate preparation stopped by shutdown");
            }
            stopped
        };
        if stopped(self) {
            return Err(retry("display publication stopped"));
        }
        let parts = self.cache.records(recent_start..=tip);
        let (root, layout) = (self.root.clone(), self.layout.clone());
        let recent =
            tokio::task::spawn_blocking(move || publish_parts(&root, &layout, &spec, &parts))
                .await
                .map_err(retry)??;
        if stopped(self) {
            return Err(retry("display publication stopped"));
        }
        let map = self.layout.map(&archives, recent.entry.clone());
        map.check_shape()
            .map_err(|e| CycleError::Halt(Halt::SealedEntryChanged(e)))?;
        check_sealed_continuity(&self.map, &map, dropped.len())
            .map_err(|e| CycleError::Halt(Halt::SealedEntryChanged(e)))?;
        Ok(DisplayMaterial {
            started,
            tip,
            tip_hash,
            new_seals,
            dropped,
            floor,
            unsealed,
            recent_start,
            recent_id,
            recent,
            map,
        })
    }

    async fn activate_cycle(
        &mut self,
        candidate: DisplayCandidate,
        finalized: Option<DisplayActivation>,
    ) -> Result<(), CycleError> {
        let DisplayCandidate {
            material,
            candidate,
            sha,
            candidate_ms,
            prebuild,
        } = candidate;
        let DisplayMaterial {
            started,
            tip,
            tip_hash,
            new_seals,
            dropped,
            floor,
            unsealed,
            recent_start,
            recent_id,
            recent,
            map,
        } = material;
        // A stop requested during preparation abandons the unactivated
        // candidate. Once finalization begins, it and its commit finish.
        if self.shutdown.requested() {
            tracing::info!(tip, "display candidate activation stopped by shutdown");
            return Ok(());
        }
        // The rollback record vouches only for what was published when it was
        // made. This candidate may be published without being committed, so
        // a restart from here on rewrites the recent range.
        if self.rollback.is_some() {
            let next = Invalidations { rollback: None };
            next.store(&self.root).map_err(retry)?;
            self.saved_invalidations = next;
            self.rollback = None;
        }
        let finalize_started = Instant::now();
        if let Some(DisplayActivation {
            distribution,
            revision,
            finalized,
        }) = finalized
        {
            distribution
                .commit(finalized, revision, &self.shutdown)
                .map_err(retry)?;
        }
        let activate_ms = ms(finalize_started);
        let activated_ms = now_ms();
        self.last_success_ms = activated_ms;

        // Committed: query servers download the new map.
        self.queue.drain(..new_seals.len());
        self.active.seals.extend(new_seals.iter().cloned());
        // Keep enough sealed blocks to fill the window from the sealed floor,
        // so a rollback above it can always move the window back.
        let keep_from = window_start(
            &self.layout.seal,
            floor,
            &self.cache.counts_from(floor)[..(unsealed - floor) as usize],
        );
        self.cache.drop_through(keep_from.min(recent_start) - 1);
        self.recent_previous = Some((recent_id, recent_start, recent.revision()));
        self.active.directory = candidate;
        self.active.map_sha256 = sha.clone();
        self.active.tip = tip;
        self.active.tip_hash = tip_hash.clone();
        self.active.cycle += 1;
        if let Err(error) = self.active.store(&self.root) {
            tracing::error!(%error, "active.json write failed");
            self.timeline.record(
                "error",
                json!({"stage": "active", "error": error.to_string()}),
            );
        }
        self.map = map;
        if self.settings.record_maps {
            self.maps.push(self.map.clone());
        }
        self.published_tip = (tip, tip_hash.clone());
        let fresh: Vec<(u64, u64)> = self.observed.range(..=tip).map(|(h, o)| (*h, *o)).collect();
        self.observed = self.observed.split_off(&(tip + 1));
        for entry in &dropped {
            self.timeline.record(
                "drop",
                json!({"shard_id": entry.shard_id, "digest": entry.manifest_digest,
                    "start": entry.start_height, "end": entry.end_height,
                    "cycle": self.active.cycle}),
            );
        }
        let t = recent.timings;
        self.last_cycle = json!({
            "cycle": self.active.cycle, "tip": tip, "tip_hash": tip_hash,
            "first_observed_ms": fresh.iter().map(|f| f.1).min(),
            "build_s": t.build_s, "verify_s": t.verify_s, "digest_s": t.digest_s,
            "write_s": t.write_s, "candidate_ms": candidate_ms,
            "prebuild_ms": prebuild.as_ref().map(|p| p.0),
            "shipped_bytes": prebuild.as_ref().map(|p| p.1.iter().map(|f| f.bytes).sum::<u64>()),
            "shipped": prebuild.as_ref().map(|p| &p.1),
            "prebuild_failures": self.prebuild_failures,
            "activate_ms": activate_ms,
            "activated_ms": activated_ms, "cycle_ms": ms(started),
            "freshness_ms": fresh.iter().map(|f| activated_ms.saturating_sub(f.1)).collect::<Vec<_>>(),
            "recent": recent.summary(), "archives": self.map.shards.len() - 1,
            "first_shard_id": self.map.first_shard_id,
            "archive_chain_sha256": self.active.archive_chain_sha256(),
            "map_sha256": sha,
            "sealed_published": new_seals.iter().map(|s| s.shard_id).collect::<Vec<_>>(),
            "dropped": dropped.iter().map(|s| s.shard_id).collect::<Vec<_>>(),
        });
        self.timeline.record("cycle", self.last_cycle.clone());
        let stages = [
            ("build", t.build_s),
            ("verify", t.verify_s),
            ("digest", t.digest_s),
            ("write", t.write_s),
        ]
        .into_iter()
        .chain(prebuild.as_ref().map(|p| ("prebuild", p.0 as f64 / 1e3)))
        .chain([("activate", activate_ms as f64 / 1e3)]);
        self.settings.metrics.observe_cycle(
            started.elapsed().as_secs_f64(),
            fresh
                .iter()
                .map(|f| activated_ms.saturating_sub(f.1) as f64 / 1e3),
            stages,
        );
        self.collect();
        Ok(())
    }

    /// Builds the recent revision's runtimes into the candidate root; returns
    /// the milliseconds taken and the files written. A failure is logged and
    /// counted and its files are removed; finalization then builds them.
    async fn prebuild(&mut self, candidate: &Path, recent: &str) -> Option<(u64, Vec<Shipped>)> {
        let build = self.settings.prebuild;
        let started = Instant::now();
        let (revision, root) = (candidate.join(recent), candidate.to_path_buf());
        let built = tokio::task::spawn_blocking(move || build(&revision, &root))
            .await
            .unwrap_or_else(|error| Err(error.to_string()));
        match built {
            Ok(files) => Some((ms(started), files)),
            Err(error) => {
                self.prebuild_failures += 1;
                tracing::warn!(%error, failures = self.prebuild_failures,
                    "runtime prebuild failed; finalization builds the runtimes");
                self.timeline.record(
                    "error",
                    json!({"stage": "prebuild", "error": error, "failures": self.prebuild_failures}),
                );
                if let Err(error) = remove_shipped(candidate) {
                    tracing::error!(%error, "could not remove partial shipped runtimes");
                }
                None
            }
        }
    }

    /// Removes unused candidates and recent revisions; also after a failed
    /// cycle, whose candidate nothing serves.
    fn collect(&self) {
        let keep = std::collections::BTreeSet::from([self.active.directory.clone()]);
        if let Err(error) = collect(&self.root, &keep, self.settings.retain_candidates) {
            self.timeline.record(
                "error",
                json!({"stage": "collect", "error": error.to_string()}),
            );
        }
    }

    /// Mirrors the rollback record into `invalidate.json`. A failed write
    /// leaves it in memory, and the next change retries.
    fn save_invalidations(&mut self) {
        let next = Invalidations {
            rollback: self.rollback.clone(),
        };
        if next == self.saved_invalidations {
            return;
        }
        match next.store(&self.root) {
            Ok(()) => self.saved_invalidations = next,
            Err(error) => {
                tracing::error!(%error, "invalidate.json write failed");
                self.timeline.record(
                    "error",
                    json!({"stage": "invalidate_record", "error": error.to_string()}),
                );
            }
        }
    }

    fn update_status(&self) {
        let status = json!({
            "phase": if self.halted.is_some() { "halted" } else { "running" },
            "halted": self.halted,
            "mode": self.source.mode(),
            "tip": self.cache.tip().map(|t| t.0),
            "journal_end": self.store.covered_through(),
            "published": {"tip": self.published_tip.0, "tip_hash": self.published_tip.1,
                "map_sha256": self.active.map_sha256, "directory": self.active.directory,
                "archives": self.map.shards.len().saturating_sub(1),
                "first_shard_id": self.map.first_shard_id, "seals": self.active.seals.len()},
            "queue": self.queue.iter().map(SealJob::summary).collect::<Vec<_>>(),
            "last_cycle": self.last_cycle,
            "failures": self.failures,
            "stalled": self.failures >= STALL_FAILURES,
            "prebuild_failures": self.prebuild_failures,
            "updated_ms": now_ms(),
        });
        *self.settings.status.write().unwrap() = status;
        let published_height = self.published_tip.0;
        self.settings.metrics.set_gauges(Gauges {
            node_height: self
                .source
                .node_tip()
                .max(self.cache.tip().map(|t| t.0))
                .unwrap_or(published_height),
            published_height,
            oldest_unpublished_ms: self
                .observed
                .values()
                .copied()
                .chain(self.source.oldest_unfetched_ms())
                .min(),
            stalled: self.failures >= STALL_FAILURES,
            halted: self.halted.is_some(),
            consecutive_failures: self.failures,
            seal_queue: self.queue.len() as u64,
            last_success_ms: self.last_success_ms,
            last_seal_ms: self.last_seal_ms,
        });
    }
}

/// Removes every shipped runtime file, finished or partial, from a
/// candidate's root.
fn remove_shipped(candidate: &Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(candidate)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.file_type()?.is_file()
            && (ShippedRuntimes::is_entry(&name) || name.ends_with(".partial"))
        {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn seal_event(job: &SealJob) -> Value {
    let shard = job.shard.as_ref().expect("built");
    json!({
        "shard_id": job.shard_id, "start": job.start, "end": job.end,
        "decided_tip": job.decided_tip, "bucket_counts": job.bucket_counts,
        "placement_seeds": shard.entry.placement_seeds, "seed_tries": shard.seed_tries,
        "dir_segments": shard.entry.directory_segments,
        "build_s": shard.timings.build_s, "verify_s": shard.timings.verify_s,
        "digest_s": shard.timings.digest_s, "write_s": shard.timings.write_s,
        "digest": shard.digest,
    })
}

/// Unchanged controller status and metrics routes, mounted by the application host.
pub fn status_router(status: Arc<RwLock<Value>>, metrics: Arc<Metrics>) -> axum::Router {
    use axum::{routing::get, Json, Router};
    let current = move || {
        let status = status.clone();
        async move { Json(status.read().unwrap().clone()) }
    };
    let rendered = move || {
        let metrics = metrics.clone();
        async move {
            (
                [("content-type", "text/plain; version=0.0.4")],
                metrics.render(),
            )
        }
    };
    Router::new()
        .route("/", get(current.clone()))
        .route("/v1/status", get(current))
        .route("/v1/health", get(|| async { "ok" }))
        .route("/metrics", get(rendered))
}

#[cfg(all(test, feature = "fixtures"))]
#[path = "../../../../pir-publisher/tests/included/tx_detail_controller_tests.rs"]
mod tests;
