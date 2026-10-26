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
mod activation;
mod chain_input;
mod fence;
mod material;
mod preparation;
mod recovery;
mod reporting;
mod seal_queue;
mod workflow;
pub use fence::RollbackFence;
use workflow::DetailPublication;

use zakura_pir_publisher::transparent as transparent_shard;
use zakura_pir_publisher::transparent::filter as transparent_filter;

use super::cache::DisplayCache;
use super::metrics::{Gauges, Metrics};
use super::now_ms;
use super::publisher::{
    collect, publish_parts, read_map, validate_storage_layout, write_candidate, ActiveRecord,
    DisplayRoot, PublishError, PublishedShard, SealRecord, ShardSpec, HALTED_FILE, MANIFEST_FILE,
};
use super::source::{Source, SourceError, SourceUpdate};
use super::timeline::{HeightIndex, Timeline};
use crate::storage::records::write_atomic;
use crate::BoxError;
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
use transparent_shared::tx_detail::prebuild::Shipped;

const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Consecutive failed cycles after which publication counts as stalled.
const STALL_FAILURES: u32 = 5;
/// How often a standing recent-shard bound alert repeats.
const LAG_ALERT_EVERY: Duration = Duration::from_secs(60);
/// The lowest rollback since the last activation.
pub const INVALIDATE_FILE: &str = "invalidate.json";

pub struct Settings {
    /// The HTTPS publication query servers download. Every live publisher
    /// has one; controller tests without a node leave it out.
    pub distribution: Option<super::distribution::Distribution>,
    /// Required runtime construction, using source tables already verified by
    /// the preparation pass. Inventory checks are independent of this builder.
    pub runtime_preparation: Arc<dyn super::runtime_preparation::RuntimePreparation>,
    /// Unused candidates kept besides the active one.
    pub retain_candidates: usize,
    /// Above this many unsealed records in the cache, a `lag` alert repeats
    /// every minute: the next recent shard would hold them all.
    pub max_recent_records: Option<u64>,
    /// Return once a finite source is exhausted and fully published.
    #[cfg(feature = "fixtures")]
    pub exit_when_idle: bool,
    /// Keep every activated map in memory, for tests.
    #[cfg(feature = "fixtures")]
    pub record_maps: bool,
    pub status: Arc<RwLock<Value>>,
    /// What `/metrics` renders; see [`super::metrics`].
    pub metrics: Arc<Metrics>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            distribution: None,
            runtime_preparation: Arc::new(super::runtime_preparation::RequiredRuntimes),
            retain_candidates: 3,
            max_recent_records: None,
            #[cfg(feature = "fixtures")]
            exit_when_idle: false,
            #[cfg(feature = "fixtures")]
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
    #[error("display activation could not commit durably: {0}")]
    Activation(String),
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
    runtime_preparation: Option<(u64, Vec<Shipped>)>,
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
    store: &impl crate::ingest::journal::DisplayJournal,
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
    queue: VecDeque<SealJob>,
    building: bool,
    sender: mpsc::UnboundedSender<Background>,
    receiver: mpsc::UnboundedReceiver<Background>,
    /// When each not yet published height was first observed.
    observed: BTreeMap<u64, u64>,
    halted: Option<String>,
    failures: u32,
    retry_at: Option<Instant>,
    /// The sealed floor and rollback record, shared with the journal writer.
    fence: Arc<RollbackFence>,
    lag_alerted: Option<Instant>,
    /// The stop request of the running loop. Candidate preparation observes
    /// it between its blocking steps and ends without activating; once
    /// finalization begins, the cycle finishes its commit.
    shutdown: pir_pipeline::service::Shutdown,
    last_cycle: Value,
    /// Unix milliseconds of the last activation by a cycle, and of the last
    /// seal built; 0 before the first since this start.
    last_success_ms: u64,
    last_seal_ms: u64,
    #[cfg(feature = "fixtures")]
    maps: Vec<DisplayMap>,
    _lock: std::fs::File,
}

impl Controller {
    /// Recovers a bootstrapped root from `active.json`.
    ///
    /// Sealed terminals must still be the journal's blocks. Required runtime
    /// preparation verifies source bytes before serving. The cache is reloaded
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
        let lock = own_root(root)?;
        validate_storage_layout(root)?;
        super::activation::recover(root)?;
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
        let fence = Arc::new(RollbackFence::new(
            root,
            settings.distribution.clone(),
            saved_invalidations,
            &active.map_sha256,
            active
                .seals
                .last()
                .map_or(layout.start_height - 1, |s| s.end_height),
        ));
        let mut controller = Self {
            root: root.to_path_buf(),
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
            fence,
            lag_alerted: None,
            shutdown: Default::default(),
            last_cycle: Value::Null,
            last_success_ms: 0,
            last_seal_ms: 0,
            #[cfg(feature = "fixtures")]
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

    /// The rollback fence a root-scheduled journal writer shares.
    pub fn fence(&self) -> Arc<RollbackFence> {
        self.fence.clone()
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
    #[cfg(feature = "fixtures")]
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
        let moved = self.cache.tip().is_some_and(|(h, hash)| {
            (h, hash.to_display_hex()) != (self.active.tip, self.active.tip_hash.clone())
        });
        moved || self.publishable() > 0
    }

    #[cfg(feature = "fixtures")]
    fn idle(&self) -> bool {
        self.source.exhausted(&self.cache)
            // A built seal the rule no longer makes waits for blocks.
            && self.queue.iter().all(|j| j.state == JobState::Built)
            && !self.building
            && !self.needs_cycle()
    }

    fn finite_source_complete(&self) -> bool {
        #[cfg(feature = "fixtures")]
        {
            self.settings.exit_when_idle && self.retry_at.is_none() && self.idle()
        }
        #[cfg(not(feature = "fixtures"))]
        {
            false
        }
    }

    /// Runs until halted, or until idle when the settings ask for it.
    /// Journal write failures return an error: restart from its checkpoint.
    #[cfg(feature = "fixtures")]
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
        // The writer may have rolled back while HTTP restoration ran. Refresh
        // the journal under the shared fence before planning any archives.
        self.poll_source().await?;
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

    /// Alerts once cycles keep failing: query servers keep serving the last
    /// publication, so nothing else shows that the tip stopped advancing.
    fn stalled(&self, error: &str) {
        if self.failures < STALL_FAILURES {
            return;
        }
        tracing::error!(
            alert = "txid_display_publication_stalled",
            failures = self.failures,
            published_tip = self.active.tip,
            tip = self.cache.tip().map(|t| t.0),
            %error,
            "display publication keeps failing; query servers serve the last publication"
        );
        if self.failures == STALL_FAILURES {
            self.timeline.record(
                "stall",
                json!({"failures": self.failures, "published_tip": self.active.tip,
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
                "queued": self.queue.len(), "published_tip": self.active.tip}),
        );
    }
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

/// Takes the root's exclusive controller lock, refusing a root another process
/// owns. Whatever changes the root's records holds it for as long as it runs.
pub(crate) fn own_root(root: &Path) -> Result<std::fs::File, BoxError> {
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join("controller.lock"))?;
    lock.try_lock()
        .map_err(|e| format!("another display controller owns this root: {e}"))?;
    Ok(lock)
}

/// Unchanged controller status and metrics routes, mounted by the application host.
pub use reporting::status_router;

#[cfg(all(test, feature = "fixtures"))]
#[path = "../../../../pir-publisher/tests/included/tx_detail_controller_tests.rs"]
mod tests;
