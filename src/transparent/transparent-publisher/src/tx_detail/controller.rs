//! The display controller loop.
//!
//! Each block from the source goes into the in-memory cache. The seal rule
//! runs over the cache and queues every seal it makes; queued seals are built
//! one at a time in the background (each chains its predecessor's digest)
//! and staged on every archive owner. A cycle then publishes: the staged
//! prefix of the queue becomes archives, the recent shard is rebuilt over its
//! window (every unsealed block, reaching back over the newest archives until
//! it holds `recent_window` txids), a candidate is written, and workers are
//! activated archive owners first. A map therefore never names an archive its
//! owners are not already serving.
//!
//! Sealed content never changes. A fork below the sealed floor, bytes that
//! differ under a published identity, or a map whose sealed entries are not
//! the previous ones (minus window drops) halts the controller: it writes
//! `halted.json`, logs `alert=txid_display_halt`, and stops publishing while
//! workers keep serving the last activation.
//!
//! A rollback owes every recent replica an invalidation. It is recorded in
//! `invalidate.json` before the journal changes and kept until each replica
//! acknowledges it, and a replica is offered nothing newer until then. A
//! restart that finds the journal off the activated chain without such a
//! record withdraws the whole recent range.

use zakura_pir_publisher::transparent as transparent_shard;
use zakura_pir_publisher::transparent::filter as transparent_filter;

use super::cache::DisplayCache;
use super::metrics::{Gauges, Metrics};
use super::now_ms;
use super::publisher::{
    collect, probe_hard_links, publish_parts, read_map, verify_dir, write_candidate, ActiveRecord,
    DisplayRoot, PublishError, PublishedShard, SealRecord, ShardSpec, HALTED_FILE, MANIFEST_FILE,
};
use super::serving::{stage, Fleet, StageOutcome, WorkerConfig};
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
/// Command-adapter workers are asked to collect every this many cycles.
const WORKER_COLLECT_EVERY: u64 = 20;
/// Consecutive failed cycles after which publication counts as stalled.
const STALL_FAILURES: u32 = 5;
/// How often a standing recent-shard bound alert repeats.
const LAG_ALERT_EVERY: Duration = Duration::from_secs(60);
/// Invalidations recent replicas have not acknowledged, and the lowest
/// rollback since the last activation.
pub const INVALIDATE_FILE: &str = "invalidate.json";

/// Builds a revision's runtimes into a candidate directory; see
/// [`transparent_shared::tx_detail::prebuild::build_shipped`].
pub type Prebuild = fn(&Path, &Path) -> Result<Vec<Shipped>, String>;

pub struct Settings {
    /// Skip re-verifying sealed revisions at startup.
    pub trust_sealed: bool,
    /// Finalized HTTP publications; no externally controlled worker is required.
    pub distribution: Option<super::distribution::Distribution>,
    /// Build the recent revision's runtimes into each candidate, so recent
    /// replicas load them instead of building (`--ship-runtimes`). A failed
    /// build is counted and the candidate ships without them.
    pub ship_runtimes: Option<Prebuild>,
    /// Unused candidates kept besides the active and worker-held ones.
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
            ship_runtimes: None,
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

struct DisplayProgress;
impl pir_pipeline::Observer for DisplayProgress {
    fn finished(&mut self, phase: pir_pipeline::Phase, elapsed: Duration, succeeded: bool) {
        tracing::debug!(
            ?phase,
            seconds = elapsed.as_secs_f64(),
            succeeded,
            "display lifecycle phase"
        );
    }
}

// A prepared publication retains every ordered seal at the queue head. It
// borrows its controller through `advance`; no store or seal queue is copied.
struct DisplayCandidate {
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
    candidate: PathBuf,
    sha: String,
    candidate_ms: u64,
    prebuild: Option<(u64, Vec<Shipped>)>,
}

enum CycleError {
    Halt(Halt),
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
    Staging,
    Staged,
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
    stage_s: f64,
    attempts: u32,
    retry_at: Option<Instant>,
    /// Staged again after an archive owner lost it; its `seal` event stands.
    restaged: bool,
}

impl SealJob {
    fn record(&self) -> SealRecord {
        let shard = self.shard.as_ref().expect("a staged seal is built");
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

enum Background {
    Built {
        shard_id: u64,
        result: Result<Box<PublishedShard>, PublishError>,
    },
    Staged {
        shard_id: u64,
        seconds: f64,
        outcomes: Vec<StageOutcome>,
    },
}

fn backoff(attempts: u32) -> Duration {
    Duration::from_secs(1u64 << attempts.min(5)).min(MAX_BACKOFF)
}

fn ms(started: Instant) -> u64 {
    started.elapsed().as_millis() as u64
}

/// The lowest rollback since a candidate last went out to workers, made while
/// `map_sha256` was active. Its ancestor was on that map's chain, so a
/// journal that still holds it agrees with everything replicas can serve
/// through it, and the invalidations queued with it cover the rest.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct RollbackRecord {
    map_sha256: String,
    ancestor: u64,
    hash: String,
}

/// `invalidate.json`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
struct Invalidations {
    /// Each recent replica's unacknowledged `from_height`.
    pending: BTreeMap<String, u64>,
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
    fleet: Fleet,
    settings: Settings,
    active: ActiveRecord,
    map: DisplayMap,
    /// The published recent revision and its `(shard_id, start)` lineage key.
    recent_previous: Option<(u64, u64, PublishedRevision)>,
    published_tip: (u64, String),
    queue: VecDeque<SealJob>,
    building: bool,
    staging: bool,
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
    /// worker activation begins, the cycle finishes its commit.
    shutdown: pir_pipeline::service::Shutdown,
    last_cycle: Value,
    /// Unix milliseconds of the last activation by a cycle, and of the last
    /// seal staged on its owners; 0 before the first since this start.
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
    /// Undelivered invalidations are restored. A journal that no longer holds
    /// the activated tip, unless a recorded rollback accounts for it, owes
    /// every recent replica an invalidation from the recent start.
    pub fn open(
        root: &Path,
        store: EventStore,
        source: Source,
        workers: Vec<WorkerConfig>,
        settings: Settings,
    ) -> Result<Self, BoxError> {
        // Workers are handed absolute candidate paths.
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
        let mut fleet = Fleet::new(workers);
        let saved_invalidations = Invalidations::load(root)?;
        fleet.restore_invalidations(&saved_invalidations.pending);
        let journal_hash = |height: u64| {
            store
                .block_at(height)
                .map(|b| b.block_hash.to_display_hex())
        };
        // Off the activated chain, replicas may serve blocks the journal no
        // longer holds. A recorded rollback whose ancestor the journal still
        // holds queued every invalidation that calls for. Otherwise where they
        // differ is unknown, but sealed terminals are checked below, so
        // everything from the recent start is enough.
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
            (true, None) => {
                fleet.queue_invalidation(recent.start_height);
                Some(recent.start_height)
            }
        };
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut controller = Self {
            root: root.to_path_buf(),
            published_tip: (active.tip, active.tip_hash.clone()),
            fleet,
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
            staging: false,
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
            "display controller halted; workers keep serving the last activation"
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

    /// Staged seals at the queue's head that the rule still makes at the
    /// cache tip. A rollback inside the margin can undo a decision: such a
    /// seal keeps its range and digest but waits, so a published archive
    /// always ends at least the reorg margin below the tip.
    fn publishable(&self) -> usize {
        let staged = self
            .queue
            .iter()
            .take_while(|j| j.state == JobState::Staged)
            .count();
        if staged == 0 {
            return 0;
        }
        // The queue starts at the first unsealed height, so ranges line up.
        let start = self.unsealed_start();
        staged.min(plan_seals(&self.layout.seal, start, self.cache.counts_from(start)).len())
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
            // A staged seal the rule no longer makes waits for blocks.
            && self.queue.iter().all(|j| j.state == JobState::Staged)
            && !self.building
            && !self.staging
            && !self.needs_cycle()
            && !self.fleet.stale(&self.active.map_sha256, &self.map)
    }

    /// Runs until halted, or until idle when the settings ask for it.
    /// Journal write failures return an error: restart from its checkpoint.
    pub async fn run(&mut self) -> Result<Outcome, BoxError> {
        self.run_until(pir_pipeline::service::Shutdown::default())
            .await
    }
    pub async fn run_until(
        &mut self,
        shutdown: pir_pipeline::service::Shutdown,
    ) -> Result<Outcome, BoxError> {
        self.shutdown = shutdown.clone();
        let result = self.run_loop(shutdown).await;
        // Every return path waits for builders and worker transports before
        // dropping the controller and its exclusive root/journal ownership.
        while self.building || self.staging {
            if let Some(event) = self.receiver.recv().await {
                self.background(event);
            }
        }
        result
    }
    async fn run_loop(
        &mut self,
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
        let errors = self
            .fleet
            .refresh((&self.active.map_sha256, &self.map))
            .await;
        self.record_worker_errors("status", errors);
        self.plan();
        loop {
            if shutdown.requested() {
                // Builders and stage transports must finish before the root lock is released.
                while self.building || self.staging {
                    if let Some(event) = self.receiver.recv().await {
                        self.background(event);
                    }
                }
                return Ok(Outcome::Idle);
            }
            while let Ok(event) = self.receiver.try_recv() {
                self.background(event);
            }
            if let Some(reason) = &self.halted {
                return Ok(Outcome::Halted(reason.clone()));
            }
            self.schedule();
            if self.retry_at.is_none_or(|t| Instant::now() >= t) {
                let result = if self.needs_cycle() {
                    let result = self.cycle().await;
                    self.settings.metrics.cycle_finished(result.is_ok());
                    Some(result)
                } else if self.fleet.stale(&self.active.map_sha256, &self.map) {
                    Some(self.sync_workers().await)
                } else {
                    None
                };
                match result {
                    None | Some(Ok(())) => {
                        self.failures = 0;
                        self.retry_at = None;
                    }
                    Some(Err(CycleError::Halt(halt))) => self.halt(halt),
                    Some(Err(CycleError::Retry(error))) => {
                        tracing::warn!(%error, "display publication failed; retrying");
                        self.timeline
                            .record("error", json!({"stage": "cycle", "error": error}));
                        self.retry_at = Some(Instant::now() + backoff(self.failures));
                        self.failures += 1;
                        self.stalled(&error);
                        self.collect_local();
                    }
                }
            }
            self.check_recent_bound();
            self.update_status();
            if let Some(reason) = &self.halted {
                return Ok(Outcome::Halted(reason.clone()));
            }
            if self.settings.exit_when_idle && self.retry_at.is_none() && self.idle() {
                return Ok(Outcome::Idle);
            }
            let now = Instant::now();
            let wake_at = self
                .queue
                .iter()
                .filter_map(|j| j.retry_at)
                .chain(self.retry_at)
                .min()
                .unwrap_or(now + Duration::from_secs(3600));
            enum Wake {
                Background(Option<Background>),
                Source,
                Timer,
            }
            let mut wake = Wake::Timer;
            pir_pipeline::wait(
                wake_at.max(now).into(),
                async {
                    wake = tokio::select! {
                        event = self.receiver.recv() => Wake::Background(event),
                        () = self.source.wait() => Wake::Source,
                    };
                },
                shutdown.cancelled(),
            )
            .await;
            match wake {
                Wake::Background(Some(event)) => self.background(event),
                Wake::Background(None) => unreachable!("the controller holds a sender"),
                Wake::Source => self.poll_source().await?,
                Wake::Timer => {}
            }
        }
    }

    async fn poll_source(&mut self) -> Result<(), BoxError> {
        let floor = self.sealed_floor();
        let distribution = self.settings.distribution.clone();
        let (fleet, rollback, saved, root, active) = (
            &mut self.fleet,
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
            fleet.queue_invalidation(ancestor + 1);
            note_rollback(rollback, active, ancestor, hash);
            let next = Invalidations {
                pending: fleet.invalidations(),
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
            // A replica may serve more than the controller activated (an
            // activation whose reply was lost), so every rollback is withdrawn;
            // one above everything served withdraws nothing. Sources that roll
            // the journal back have recorded this already.
            self.fleet.queue_invalidation(ancestor + 1);
            if let Some(block) = self.store.block_at(ancestor) {
                note_rollback(
                    &mut self.rollback,
                    &self.active.map_sha256,
                    ancestor,
                    block.block_hash,
                );
            }
            self.save_invalidations();
            self.deliver_invalidations().await;
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

    /// Alerts once cycles keep failing: workers keep serving the last
    /// activation, so nothing else shows that the tip stopped advancing.
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
            "display publication keeps failing; workers serve the last activation"
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
                stage_s: 0.0,
                attempts: 0,
                retry_at: None,
                restaged: false,
            });
        }
    }

    /// Stages again any staged seal a known archive owner no longer holds:
    /// staging lives in the worker's memory, so a restart loses it. Until it
    /// is staged again, cycles publish without it and recent replicas keep
    /// advancing. Returns whether any was.
    fn restage_lost(&mut self) -> bool {
        let mut any = false;
        for job in self
            .queue
            .iter_mut()
            .filter(|j| j.state == JobState::Staged)
        {
            let digest = &job.shard.as_ref().expect("built").digest;
            let lost: Vec<String> = self
                .fleet
                .archive_owners()
                .filter(|w| {
                    w.known
                        && !w.staged.contains(digest)
                        && !w.sealed.as_ref().is_some_and(|s| s.contains(digest))
                })
                .map(|w| w.config.name.clone())
                .collect();
            if !lost.is_empty() {
                self.timeline.record(
                    "error",
                    json!({"stage": "seal_stage", "shard_id": job.shard_id,
                        "error": "archive owners lost the staged seal", "workers": lost}),
                );
                job.state = JobState::Built;
                job.attempts = 0;
                job.retry_at = None;
                job.restaged = true;
                any = true;
            }
        }
        any
    }

    /// Starts the next seal build and the next staging, one of each at a time.
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
                    let _ = sender.send(Background::Built { shard_id, result });
                });
            }
        }
        if !self.staging {
            let Some(job) = self
                .queue
                .iter_mut()
                .find(|j| j.state != JobState::Staged)
                .filter(|j| j.state == JobState::Built && j.ready(now))
            else {
                return;
            };
            let shard = job.shard.as_ref().expect("built");
            let targets: Vec<_> = self
                .fleet
                .archive_owners()
                .filter(|w| {
                    !w.staged.contains(&shard.digest)
                        && !w.sealed.as_ref().is_some_and(|s| s.contains(&shard.digest))
                })
                .map(|w| (w.config.name.clone(), w.config.transport.clone()))
                .collect();
            if targets.is_empty() {
                job.state = JobState::Staged;
                if !job.restaged {
                    self.timeline.record("seal", seal_event(job));
                    self.last_seal_ms = now_ms();
                }
                return;
            }
            job.state = JobState::Staging;
            self.staging = true;
            let (shard_id, digest, directory, sender) = (
                job.shard_id,
                shard.digest.clone(),
                shard.directory.clone(),
                self.sender.clone(),
            );
            tokio::spawn(async move {
                let started = Instant::now();
                let mut outcomes = Vec::new();
                for (name, transport) in targets {
                    outcomes.push(stage(name, transport, directory.clone(), digest.clone()).await);
                }
                let _ = sender.send(Background::Staged {
                    shard_id,
                    seconds: started.elapsed().as_secs_f64(),
                    outcomes,
                });
            });
        }
    }

    fn background(&mut self, event: Background) {
        match event {
            Background::Built { shard_id, result } => {
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
            Background::Staged {
                shard_id,
                seconds,
                outcomes,
            } => {
                self.staging = false;
                let Some(job) = self.queue.iter_mut().find(|j| j.shard_id == shard_id) else {
                    return;
                };
                let digest = job.shard.as_ref().expect("built").digest.clone();
                let mut failed = false;
                for outcome in outcomes {
                    let worker = self
                        .fleet
                        .workers
                        .iter_mut()
                        .find(|w| w.config.name == outcome.worker);
                    match (outcome.result, worker) {
                        (Ok(_), Some(worker)) => {
                            worker.staged.insert(digest.clone());
                        }
                        (Ok(_), None) => {}
                        (Err(error), worker) => {
                            failed = true;
                            if let Some(worker) = worker {
                                worker.known = false;
                            }
                            self.timeline.record(
                                "error",
                                json!({"stage": "seal_stage", "shard_id": shard_id,
                                    "worker": outcome.worker, "error": error}),
                            );
                        }
                    }
                }
                if failed {
                    job.state = JobState::Built;
                    job.retry_at = Some(Instant::now() + backoff(job.attempts));
                    job.attempts += 1;
                } else {
                    job.state = JobState::Staged;
                    job.stage_s = seconds;
                    if !job.restaged {
                        self.timeline.record("seal", seal_event(job));
                        self.last_seal_ms = now_ms();
                    }
                }
            }
        }
    }

    /// Publishes the current tip and every publishable seal at the queue's
    /// head.
    async fn cycle(&mut self) -> Result<(), CycleError> {
        pir_pipeline::advance(
            Input::Work(self),
            |controller| async move {
                match controller.prepare_cycle().await? {
                    Input::Work(candidate) => Ok(Input::Work((controller, candidate))),
                    Input::Unchanged => Ok(Input::Unchanged),
                    Input::Deferred => Ok(Input::Deferred),
                }
            },
            |(controller, candidate)| async move { controller.activate_cycle(candidate).await },
            &mut DisplayProgress,
        )
        .await?;
        Ok(())
    }

    async fn prepare_cycle(&mut self) -> Result<Input<DisplayCandidate>, CycleError> {
        let started = Instant::now();
        let (tip, tip_hash) = self.cache.tip().ok_or_else(|| retry("empty cache"))?;
        let tip_hash = tip_hash.to_display_hex();
        let errors = self
            .fleet
            .refresh((&self.active.map_sha256, &self.map))
            .await;
        self.record_worker_errors("status", errors);
        if self.restage_lost() {
            // Nothing else may wake the loop to start it.
            self.schedule();
        }
        let count = self.publishable();
        if count == 0 && (tip, &tip_hash) == (self.published_tip.0, &self.published_tip.1) {
            // Only a seal was due, and it is being staged again.
            return Ok(Input::Deferred);
        }
        let staged: Vec<&SealJob> = self.queue.iter().take(count).collect();
        let new_seals: Vec<SealRecord> = staged.iter().map(|j| j.record()).collect();
        let mut archives: Vec<DisplayMapEntry> = self
            .map
            .shards
            .iter()
            .filter(|s| s.sealed)
            .cloned()
            .chain(
                staged
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
            return Ok(Input::Unchanged);
        }
        let parts = self.cache.records(recent_start..=tip);
        let (root, layout) = (self.root.clone(), self.layout.clone());
        let recent =
            tokio::task::spawn_blocking(move || publish_parts(&root, &layout, &spec, &parts))
                .await
                .map_err(retry)??;
        if stopped(self) {
            return Ok(Input::Unchanged);
        }
        let map = self.layout.map(&archives, recent.entry.clone());
        map.check_shape()
            .map_err(|e| CycleError::Halt(Halt::SealedEntryChanged(e)))?;
        check_sealed_continuity(&self.map, &map, dropped.len())
            .map_err(|e| CycleError::Halt(Halt::SealedEntryChanged(e)))?;
        let candidate_started = Instant::now();
        let (candidate, sha) = write_candidate(&self.root, &map).map_err(retry)?;
        let candidate_ms = ms(candidate_started);
        let prebuild = self.prebuild(&candidate, &recent.digest).await;
        if stopped(self) {
            return Ok(Input::Unchanged);
        }

        Ok(Input::Work(DisplayCandidate {
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
            candidate,
            sha,
            candidate_ms,
            prebuild,
        }))
    }

    async fn activate_cycle(&mut self, candidate: DisplayCandidate) -> Result<(), CycleError> {
        let DisplayCandidate {
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
            candidate,
            sha,
            candidate_ms,
            prebuild,
        } = candidate;
        self.deliver_invalidations().await;
        // Delivery awaits workers; a stop requested meanwhile abandons the
        // unactivated candidate. Once worker activation begins, it and its
        // commit finish.
        if self.shutdown.requested() {
            tracing::info!(tip, "display candidate activation stopped by shutdown");
            return Ok(());
        }
        // The rollback record vouches only for what replicas held when it was
        // made. This candidate may reach some replicas without being
        // committed, so a restart from here on withdraws the recent range.
        if self.rollback.is_some() {
            let next = Invalidations {
                pending: self.fleet.invalidations(),
                rollback: None,
            };
            next.store(&self.root).map_err(retry)?;
            self.saved_invalidations = next;
            self.rollback = None;
        }
        let report = self
            .fleet
            .activate(&candidate, &sha, &map, true)
            .await
            .map_err(CycleError::Retry)?;
        if let Some(distribution) = &self.settings.distribution {
            distribution
                .finalize(candidate.clone(), vec![self.active.directory.clone()])
                .await
                .map_err(retry)?;
        }
        let activated_ms = now_ms();
        self.last_success_ms = activated_ms;

        // Committed: workers serve the new map.
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
            "ship_ms": report.ship_ms,
            "prepare_ms": report.prepare_ms, "activate_ms": report.activate_ms,
            "activated_ms": activated_ms, "cycle_ms": ms(started),
            "freshness_ms": fresh.iter().map(|f| activated_ms.saturating_sub(f.1)).collect::<Vec<_>>(),
            "recent": recent.summary(), "archives": self.map.shards.len() - 1,
            "first_shard_id": self.map.first_shard_id,
            "archive_chain_sha256": self.active.archive_chain_sha256(),
            "map_sha256": sha,
            "sealed_published": new_seals.iter().map(|s| s.shard_id).collect::<Vec<_>>(),
            "dropped": dropped.iter().map(|s| s.shard_id).collect::<Vec<_>>(),
            "workers": report.workers,
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
        .chain([
            ("ship", report.ship_ms as f64 / 1e3),
            ("prepare", report.prepare_ms as f64 / 1e3),
            ("activate", report.activate_ms as f64 / 1e3),
        ]);
        self.settings.metrics.observe_cycle(
            started.elapsed().as_secs_f64(),
            fresh
                .iter()
                .map(|f| activated_ms.saturating_sub(f.1) as f64 / 1e3),
            stages,
        );
        self.collect().await;
        Ok(())
    }

    /// With `--ship-runtimes`, builds the recent revision's runtimes into the
    /// candidate root; returns the milliseconds taken and the files written.
    /// A failure is logged and counted, its files are removed, and the
    /// candidate ships without runtimes: replicas then build their own.
    async fn prebuild(&mut self, candidate: &Path, recent: &str) -> Option<(u64, Vec<Shipped>)> {
        let build = self.settings.ship_runtimes?;
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
                    "runtime prebuild failed; the candidate ships without runtimes");
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

    /// Activates the current candidate on workers that are behind it, such as
    /// a worker that restarted, or every worker right after bootstrap, and
    /// delivers owed invalidations.
    ///
    /// Recent replicas are left alone while the journal no longer holds the
    /// activated tip: a replica that never held that map was not invalidated
    /// for it, and would serve its orphaned blocks.
    async fn sync_workers(&mut self) -> Result<(), CycleError> {
        let errors = self
            .fleet
            .refresh((&self.active.map_sha256, &self.map))
            .await;
        self.record_worker_errors("status", errors);
        self.deliver_invalidations().await;
        let on_chain = self
            .store
            .block_at(self.active.tip)
            .is_some_and(|b| b.block_hash.to_display_hex() == self.active.tip_hash);
        let report = self
            .fleet
            .activate(
                &self.active.directory,
                &self.active.map_sha256,
                &self.map,
                on_chain,
            )
            .await
            .map_err(CycleError::Retry)?;
        if !on_chain && self.fleet.stale(&self.active.map_sha256, &self.map) {
            return Err(retry(
                "the activated tip left the journal; recent replicas wait for the next cycle",
            ));
        }
        if !report.workers.is_empty() {
            self.timeline.record(
                "sync",
                json!({"map_sha256": self.active.map_sha256, "workers": report.workers}),
            );
        }
        Ok(())
    }

    /// Removes unused candidates and recent revisions; also after a failed
    /// cycle, whose candidate nothing serves.
    fn collect_local(&self) {
        let mut keep = self.fleet.held_directories();
        keep.insert(self.active.directory.clone());
        if let Err(error) = collect(&self.root, &keep, self.settings.retain_candidates) {
            self.timeline.record(
                "error",
                json!({"stage": "collect", "error": error.to_string()}),
            );
        }
    }

    async fn collect(&mut self) {
        self.collect_local();
        if self.active.cycle.is_multiple_of(WORKER_COLLECT_EVERY) {
            let errors = self.fleet.collect().await;
            self.record_worker_errors("worker_collect", errors);
        }
    }

    /// Mirrors owed invalidations and the rollback record into
    /// `invalidate.json`. A failed write leaves them in memory, and the next
    /// change retries.
    fn save_invalidations(&mut self) {
        let next = Invalidations {
            pending: self.fleet.invalidations(),
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

    /// Delivers owed invalidations ahead of any activation.
    async fn deliver_invalidations(&mut self) {
        if self.fleet.invalidations().is_empty() {
            return;
        }
        let errors = self
            .fleet
            .invalidate((&self.active.map_sha256, &self.map))
            .await;
        self.record_worker_errors("invalidate", errors);
        self.save_invalidations();
    }

    fn record_worker_errors(&self, stage: &str, errors: Vec<(String, String)>) {
        for (worker, error) in errors {
            tracing::warn!(stage, worker, %error, "display worker call failed");
            self.timeline.record(
                "error",
                json!({"stage": stage, "worker": worker, "error": error}),
            );
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
            "workers": self.fleet.workers.iter().map(|w| json!({
                "name": w.config.name, "role": w.config.role.as_str(), "known": w.known,
                "expected": w.expected, "staged": w.staged,
                "invalidate_from": w.invalidate_from})).collect::<Vec<_>>(),
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
        "stage_s": job.stage_s, "digest": shard.digest,
    })
}

/// Serves the controller's status as JSON, and its metrics in the
/// Prometheus text format on `/metrics`, on a private address.
pub async fn serve_status(
    listen: std::net::SocketAddr,
    status: Arc<RwLock<Value>>,
    metrics: Arc<Metrics>,
) -> Result<(), BoxError> {
    let app = status_router(status, metrics);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::error!(%error, "status service failed");
        }
    });
    Ok(())
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
