//! The display controller loop.
//!
//! Each block from the source goes into the in-memory cache. The seal rule
//! runs over the cache and queues every seal it makes; queued seals are built
//! one at a time in the background (each chains its predecessor's digest)
//! and staged on every archive owner. A cycle then publishes: the staged
//! prefix of the queue becomes archives, the recent shard is rebuilt over
//! what remains, a candidate is written, and workers are activated archive
//! owners first. A map therefore never names an archive its owners are not
//! already serving.
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

use super::cache::DisplayCache;
use super::now_ms;
use super::publisher::{
    collect, probe_hard_links, publish_parts, read_map, verify_dir, write_candidate, ActiveRecord,
    DisplayRoot, PublishError, PublishedShard, SealRecord, ShardSpec, HALTED_FILE, MANIFEST_FILE,
};
use super::serving::{stage, Fleet, StageOutcome, WorkerConfig};
use super::source::{Source, SourceError, SourceUpdate};
use super::timeline::{HeightIndex, Timeline};
use crate::events::EventStore;
use crate::publication::{write_atomic, BoxError, Journal};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use transparent_filter::BlockHash;
use transparent_shard::display::{plan_seals, DisplayManifest, DisplayMap, DisplayMapEntry};
use transparent_shard::manifest::PublishedRevision;

const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Command-adapter workers are asked to collect every this many cycles.
const WORKER_COLLECT_EVERY: u64 = 20;
/// Invalidations recent replicas have not acknowledged, and the lowest
/// rollback since the last activation.
pub const INVALIDATE_FILE: &str = "invalidate.json";

pub struct Settings {
    /// Skip re-verifying sealed revisions at startup.
    pub trust_sealed: bool,
    /// Unused candidates kept besides the active and worker-held ones.
    pub retain_candidates: usize,
    /// Above this many recent records, every cycle writes a `lag` alert.
    pub max_recent_records: Option<u64>,
    /// Return once a finite source is exhausted and fully published.
    pub exit_when_idle: bool,
    /// Keep every activated map in memory, for tests.
    pub record_maps: bool,
    pub status: Arc<RwLock<Value>>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            trust_sealed: false,
            retain_candidates: 3,
            max_recent_records: None,
            exit_when_idle: false,
            record_maps: false,
            status: Arc::new(RwLock::new(json!({"phase": "starting"}))),
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
    last_cycle: Value,
    maps: Vec<DisplayMap>,
    _lock: std::fs::File,
}

impl Controller {
    /// Recovers a bootstrapped root from `active.json`.
    ///
    /// Sealed terminals must still be the journal's blocks, and sealed
    /// revisions must verify from disk (unless trusted). The cache is reloaded
    /// from the recent shard's start through the activated tip; queued seals
    /// are planned again from it and reproduce the same digests. Undelivered
    /// invalidations are restored. A journal that no longer holds the
    /// activated tip, unless a recorded rollback accounts for it, owes every
    /// recent replica an invalidation from the recent start.
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
        let cache =
            DisplayCache::load(&store, layout.seal, recent.start_height, through, now_ms())?;
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
            last_cycle: Value::Null,
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
    /// seal keeps its range and digest but waits, so publishing it never
    /// leaves the recent shard below its floor.
    fn publishable(&self) -> usize {
        let staged = self
            .queue
            .iter()
            .take_while(|j| j.state == JobState::Staged)
            .count();
        if staged == 0 {
            return 0;
        }
        // The queue starts at the cache start, so ranges line up.
        let start = self.cache.start();
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
        if let Some(reason) = &self.halted {
            tracing::error!(alert = "txid_display_halt", %reason, "display root is halted");
            self.update_status();
            return Ok(Outcome::Halted(reason.clone()));
        }
        let errors = self
            .fleet
            .refresh((&self.active.map_sha256, &self.map))
            .await;
        self.record_worker_errors("status", errors);
        self.plan();
        loop {
            while let Ok(event) = self.receiver.try_recv() {
                self.background(event);
            }
            if let Some(reason) = &self.halted {
                return Ok(Outcome::Halted(reason.clone()));
            }
            self.schedule();
            if self.retry_at.is_none_or(|t| Instant::now() >= t) {
                let result = if self.needs_cycle() {
                    Some(self.cycle().await)
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
                        self.collect_local();
                    }
                }
            }
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
            let wake = tokio::select! {
                event = self.receiver.recv() => Wake::Background(event),
                () = self.source.wait() => Wake::Source,
                () = tokio::time::sleep_until(wake_at.max(now).into()) => Wake::Timer,
            };
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
        let (fleet, rollback, saved, root, active) = (
            &mut self.fleet,
            &mut self.rollback,
            &mut self.saved_invalidations,
            &self.root,
            &self.active.map_sha256,
        );
        let mut before_rollback = |ancestor: u64, hash: BlockHash| {
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
        let polled = self
            .source
            .poll(&mut self.store, &self.cache, floor, &mut before_rollback)
            .await;
        match polled {
            Ok(update) => self.apply(update).await?,
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

    /// Queues every seal the rule makes over the unqueued part of the cache.
    fn plan(&mut self) {
        let Some((tip, _)) = self.cache.tip() else {
            return;
        };
        let from = self.queue.back().map_or(self.cache.start(), |j| j.end + 1);
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
            });
        }
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
                let record = seal_event(job);
                self.timeline.record("seal", record);
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
                    let record = seal_event(job);
                    self.timeline.record("seal", record);
                }
            }
        }
    }

    /// Publishes the current tip and every publishable seal at the queue's
    /// head.
    async fn cycle(&mut self) -> Result<(), CycleError> {
        let started = Instant::now();
        let (tip, tip_hash) = self.cache.tip().ok_or_else(|| retry("empty cache"))?;
        let tip_hash = tip_hash.to_display_hex();
        let staged: Vec<&SealJob> = self.queue.iter().take(self.publishable()).collect();
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
        let recent_start = last.map_or(self.layout.start_height, |s| s.end_height + 1);
        if recent_start
            != self.cache.start()
                + new_seals
                    .iter()
                    .map(|s| s.end_height + 1 - s.start_height)
                    .sum::<u64>()
        {
            return Err(retry("queued seals are not contiguous with the cache"));
        }
        let spec = ShardSpec {
            shard_id: recent_id,
            start: recent_start,
            end: tip,
            parent_block_hash: last.map_or(self.layout.base_parent.clone(), |s| {
                s.terminal_block_hash.clone()
            }),
            terminal_block_hash: tip_hash.clone(),
            parent_manifest_digest: last.map_or(String::new(), |s| s.digest.clone()),
            sealed: false,
            previous: self
                .recent_previous
                .as_ref()
                .filter(|(id, start, _)| (*id, *start) == (recent_id, recent_start))
                .map(|(_, _, p)| p.clone()),
        };
        let parts = self.cache.records(recent_start..=tip);
        let (root, layout) = (self.root.clone(), self.layout.clone());
        let recent =
            tokio::task::spawn_blocking(move || publish_parts(&root, &layout, &spec, &parts))
                .await
                .map_err(retry)??;
        let map = self.layout.map(&archives, recent.entry.clone());
        map.check_shape()
            .map_err(|e| CycleError::Halt(Halt::SealedEntryChanged(e)))?;
        check_sealed_continuity(&self.map, &map, dropped.len())
            .map_err(|e| CycleError::Halt(Halt::SealedEntryChanged(e)))?;
        let candidate_started = Instant::now();
        let (candidate, sha) = write_candidate(&self.root, &map).map_err(retry)?;
        let candidate_ms = ms(candidate_started);

        let errors = self
            .fleet
            .refresh((&self.active.map_sha256, &self.map))
            .await;
        self.record_worker_errors("status", errors);
        self.deliver_invalidations().await;
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
        let activated_ms = now_ms();

        // Committed: workers serve the new map.
        self.queue.drain(..new_seals.len());
        self.active.seals.extend(new_seals.iter().cloned());
        self.cache.drop_through(recent_start - 1);
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
            "write_s": t.write_s, "candidate_ms": candidate_ms, "ship_ms": report.ship_ms,
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
        if let Some(max) = self
            .settings
            .max_recent_records
            .filter(|max| recent.manifest.records > *max)
        {
            tracing::error!(
                alert = "txid_display_recent_lag",
                records = recent.manifest.records,
                max,
                "recent shard exceeds its bound; sealing is behind"
            );
            self.timeline.record(
                "lag",
                json!({"reason": "recent_records", "records": recent.manifest.records,
                    "max": max, "queued": self.queue.len()}),
            );
        }
        self.collect().await;
        Ok(())
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
            "workers": self.fleet.workers.iter().map(|w| json!({
                "name": w.config.name, "role": w.config.role.as_str(), "known": w.known,
                "expected": w.expected, "staged": w.staged,
                "invalidate_from": w.invalidate_from})).collect::<Vec<_>>(),
            "updated_ms": now_ms(),
        });
        *self.settings.status.write().unwrap() = status;
    }
}

fn seal_event(job: &SealJob) -> Value {
    let shard = job.shard.as_ref().expect("built");
    json!({
        "shard_id": job.shard_id, "start": job.start, "end": job.end,
        "decided_tip": job.decided_tip, "bucket_counts": job.bucket_counts,
        "build_s": shard.timings.build_s, "verify_s": shard.timings.verify_s,
        "digest_s": shard.timings.digest_s, "write_s": shard.timings.write_s,
        "stage_s": job.stage_s, "digest": shard.digest,
    })
}

/// Serves the controller's status as JSON on a private address.
pub async fn serve_status(
    listen: std::net::SocketAddr,
    status: Arc<RwLock<Value>>,
) -> Result<(), BoxError> {
    use axum::{extract::State, routing::get, Json, Router};
    async fn current(State(status): State<Arc<RwLock<Value>>>) -> Json<Value> {
        Json(status.read().unwrap().clone())
    }
    let app = Router::new()
        .route("/", get(current))
        .route("/v1/status", get(current))
        .route("/v1/health", get(|| async { "ok" }))
        .with_state(status);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::error!(%error, "status service failed");
        }
    });
    Ok(())
}

/// Re-bootstraps into a scratch root at the journal end and checks every
/// recorded seal against it; verifies every revision of the active candidate.
///
/// A seal's range depends only on the chain below it, but whether the rule
/// has made it yet depends on blocks above it. Seals the journal makes beyond
/// the recorded ones are queued, not wrong. Recorded seals beyond the
/// reproduced ones were made on blocks a later reorg replaced: each is
/// rebuilt from its recorded parent and must match its range and digest.
pub fn verify(root: &Path, journal: &Path, scratch: Option<&Path>) -> Result<Value, BoxError> {
    let layout = DisplayRoot::load(root)?;
    let active = ActiveRecord::load(root)?;
    let map = read_map(&active.directory, &active.map_sha256)?;
    let mut problems = Vec::new();
    for entry in &map.shards {
        match verify_dir(
            &active.directory.join(&entry.manifest_digest),
            &entry.manifest_digest,
        ) {
            Ok(manifest) if entry.describes(&manifest) => {}
            Ok(_) => problems.push(format!(
                "shard {} manifest differs from the map",
                entry.shard_id
            )),
            Err(error) => problems.push(format!("shard {}: {error}", entry.shard_id)),
        }
    }
    let store = EventStore::open_existing(journal)?;
    let through = store.covered_through().ok_or("the journal is empty")?;
    let mut report = json!({"map_sha256": active.map_sha256, "revisions_verified": map.shards.len(),
        "seals": active.seals.len(), "archive_chain_sha256": active.archive_chain_sha256(),
        "through": through});
    if !active.seals.is_empty() {
        let scratch = tempfile::Builder::new()
            .prefix(".verify-")
            .tempdir_in(scratch.unwrap_or(root))?;
        let rebuilt = super::publisher::bootstrap(&store, scratch.path(), &layout, through)?.seals;
        let identity = |s: &SealRecord| {
            (
                s.shard_id,
                s.start_height,
                s.end_height,
                s.digest.clone(),
                s.terminal_block_hash.clone(),
            )
        };
        let differs = active
            .seals
            .iter()
            .zip(&rebuilt)
            .position(|(a, b)| identity(a) != identity(b));
        let mut unreproduced = 0;
        if let Some(index) = differs {
            problems.push(format!(
                "seal {index} differs from the journal's: recorded {:?}, reproduced {:?}",
                identity(&active.seals[index]),
                identity(&rebuilt[index])
            ));
        } else {
            for index in rebuilt.len()..active.seals.len() {
                let parent = index.checked_sub(1).map(|i| &active.seals[i]);
                match rebuild_seal(
                    &store,
                    scratch.path(),
                    &layout,
                    parent,
                    &active.seals[index],
                ) {
                    Ok(()) => unreproduced += 1,
                    Err(error) => problems.push(format!("seal {index}: {error}")),
                }
            }
        }
        report["reproduced_seals"] = rebuilt.len().into();
        report["queued"] = rebuilt.len().saturating_sub(active.seals.len()).into();
        report["unreproduced_verified"] = unreproduced.into();
    }
    report["ok"] = problems.is_empty().into();
    report["problems"] = problems.into();
    Ok(report)
}

/// Rebuilds one recorded seal from the journal, after its recorded parent,
/// without the gates that decide when the rule makes it.
fn rebuild_seal(
    store: &EventStore,
    scratch: &Path,
    layout: &DisplayRoot,
    parent: Option<&SealRecord>,
    seal: &SealRecord,
) -> Result<(), BoxError> {
    use transparent_shard::display::{archive_boundary, height_counts};
    let (id, start) = parent.map_or((0, layout.start_height), |p| {
        (p.shard_id + 1, p.end_height + 1)
    });
    let through = store.covered_through().unwrap_or(0).min(seal.end_height);
    let mut counts = Vec::new();
    let mut records = Vec::new();
    for height in start..=through {
        let block = store.display_at(height)?;
        counts.push(height_counts(&layout.seal, &block));
        records.extend(block);
    }
    let end = archive_boundary(&layout.seal, start, &counts);
    if (seal.shard_id, seal.start_height, Some(seal.end_height)) != (id, start, end) {
        return Err(format!(
            "recorded shard {} over {}..={}, the rule makes shard {id} over {start}..={end:?}",
            seal.shard_id, seal.start_height, seal.end_height
        )
        .into());
    }
    let terminal = store
        .block_at(seal.end_height)
        .ok_or("the journal lost a sealed terminal")?
        .block_hash
        .to_display_hex();
    let spec = ShardSpec {
        shard_id: id,
        start,
        end: seal.end_height,
        parent_block_hash: parent.map_or(layout.base_parent.clone(), |p| {
            p.terminal_block_hash.clone()
        }),
        terminal_block_hash: terminal,
        parent_manifest_digest: parent.map_or(String::new(), |p| p.digest.clone()),
        sealed: true,
        previous: None,
    };
    let shard = super::publisher::publish_shard(scratch, layout, &spec, &records)?;
    if (&shard.digest, &spec.terminal_block_hash) != (&seal.digest, &seal.terminal_block_hash) {
        return Err(format!(
            "rebuilt as {} ending at {}, recorded {} ending at {}",
            shard.digest, spec.terminal_block_hash, seal.digest, seal.terminal_block_hash
        )
        .into());
    }
    Ok(())
}

/// Picks a display start and bootstrap end from the journal's counts: the
/// start leaves `archives + replay_seals` seals decided by the journal end,
/// and the end is the last tip before seal `archives + 1` is decided, so the
/// recent shard starts near its largest and replay seals right away.
pub fn plan_start(
    store: &impl Journal,
    seal: &transparent_shard::display::DisplaySealParams,
    archives: u64,
    replay_seals: u64,
    max_archive_shards: u64,
) -> Result<Value, BoxError> {
    use transparent_shard::display::height_counts;
    let end = store.covered_through().ok_or("the journal is empty")?;
    let first = store.start_height() + 1;
    let wanted = (archives + replay_seals) as usize;
    // Read backwards until the rule makes enough seals over [start, end].
    let per_seal = seal.archive_target * u64::from(seal.n_archive);
    let mut reversed = Vec::new();
    let mut records = 0u64;
    let mut target = per_seal * (wanted as u64 + 1) + seal.recent_floor * u64::from(seal.n_recent);
    let (start, counts, seals) = loop {
        while records < target && end + 1 - reversed.len() as u64 > first {
            let height = end - reversed.len() as u64;
            let block = store.display_at(height)?;
            records += block.len() as u64;
            reversed.push(height_counts(seal, &block));
        }
        let start = end + 1 - reversed.len() as u64;
        let counts: Vec<_> = reversed.iter().rev().cloned().collect();
        let seals = plan_seals(seal, start, &counts);
        if seals.len() >= wanted || start <= first {
            break (start, counts, seals);
        }
        target += target / 4 + per_seal;
    };
    if seals.len() < wanted || archives == 0 {
        return Err(format!(
            "the journal holds {} seals from {start}; {wanted} wanted",
            seals.len()
        )
        .into());
    }
    // The first tip at which seal j (0-based) is decided.
    let decided = |j: usize| -> u64 {
        let h = *seals[j].end();
        let mut sums = vec![0u64; seal.n_recent as usize];
        let mut t = h;
        loop {
            if t + 1 > end {
                return end;
            }
            t += 1;
            for (sum, count) in sums.iter_mut().zip(&counts[(t - start) as usize].recent) {
                *sum += u64::from(*count);
            }
            if t >= h + seal.reorg_margin && sums.iter().all(|s| *s >= seal.recent_floor) {
                return t;
            }
        }
    };
    let through = if (archives as usize) < seals.len() {
        decided(archives as usize) - 1
    } else {
        end
    };
    let at_through = (0..seals.len()).filter(|j| decided(*j) <= through).count();
    let replayed = (0..seals.len()).filter(|j| decided(*j) > through).count();
    let total = at_through + replayed;
    Ok(json!({
        "start": start, "through": through, "journal_start": store.start_height(),
        "journal_end": end, "archives_at_through": at_through, "replay_seals": replayed,
        "expected_drops": (total as u64).saturating_sub(max_archive_shards),
        "records": records,
    }))
}

/// Builds recent shards of synthetic records at each size and reports their
/// table shape and timings, for choosing the archive target.
pub fn bench_recent(
    sizes: &[u64],
    geometry: &str,
    scratch: &Path,
    seed: u64,
) -> Result<Vec<Value>, BoxError> {
    let mut out = Vec::new();
    for &size in sizes {
        let records = synthetic_records(size, seed);
        let temp = tempfile::Builder::new()
            .prefix(".bench-")
            .tempdir_in(scratch)?;
        let layout = DisplayRoot {
            geometry: geometry.to_string(),
            seal: Default::default(),
            max_archive_shards: 1,
            network: transparent_filter::NETWORK.to_string(),
            genesis_hash: "00".repeat(32),
            start_height: 1,
            base_parent: "00".repeat(32),
        };
        let g = layout.validate()?;
        let spec = ShardSpec {
            shard_id: 0,
            start: 1,
            end: 1,
            parent_block_hash: "00".repeat(32),
            terminal_block_hash: "11".repeat(32),
            parent_manifest_digest: String::new(),
            sealed: false,
            previous: None,
        };
        let shard = super::publisher::publish_shard(temp.path(), &layout, &spec, &records)?;
        let segments = shard.entry.directory_segments.iter().sum::<u32>() as u64;
        let capacity = segments * g.directory_rows * transparent_shard::txid::ROW_BYTES as u64;
        let histogram = &shard.manifest.buckets[0].page_histogram;
        out.push(json!({
            "geometry": geometry, "records": size,
            "directory_segments": shard.entry.directory_segments,
            "page_segments": shard.entry.page_segments,
            "used_bytes": shard.used_bytes, "fill": shard.used_bytes as f64 / capacity as f64,
            "payload_bytes": shard.manifest.payload_bytes,
            "page_rows_used": shard.manifest.page_rows_used,
            "inline_records": shard.manifest.buckets[0].inline_records,
            "paged_records": size - shard.manifest.buckets[0].inline_records,
            "max_pages": histogram.keys().max(),
            "build_s": shard.timings.build_s, "verify_s": shard.timings.verify_s,
            "digest_s": shard.timings.digest_s, "write_s": shard.timings.write_s,
        }));
    }
    Ok(out)
}

/// Records with a recent-era size mix: about 90% inline payloads of 50-110
/// bytes, 9% one page, and 1% two to fifteen pages.
pub fn synthetic_records(
    count: u64,
    seed: u64,
) -> Vec<transparent_shard::txid::TransparentDisplayRecord> {
    use sha2::{Digest, Sha256};
    use transparent_events::{FeeState, TransactionMetadata, Txid};
    use transparent_shard::txid::{DisplayOutput, TransparentDisplayRecord};
    const FRAGMENT: u64 = 4_050;
    let mut x = seed ^ 0x9e37_79b9_7f4a_7c15;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    (0..count)
        .map(|i| {
            let class = next() % 1_000;
            let payload = if class < 900 {
                50 + next() % 61
            } else if class < 990 {
                129 + next() % (FRAGMENT - 128)
            } else {
                let pages = 2 + next() % 14;
                FRAGMENT * (pages - 1) + 1 + next() % FRAGMENT
            };
            let mut record = TransparentDisplayRecord {
                txid: Txid(Sha256::digest([seed.to_le_bytes(), i.to_le_bytes()].concat()).into()),
                coinbase: false,
                metadata: TransactionMetadata {
                    fee: FeeState::Exact(10_000),
                    transparent_input_count: 1,
                    has_shielded_components: false,
                },
                outputs: vec![DisplayOutput {
                    value: 1_000,
                    script: Vec::new(),
                }],
            };
            // The script length's varint grows with it; settle in two passes.
            for _ in 0..2 {
                let len = record.encode().map(|b| b.len() as u64).unwrap_or(0);
                let script = record.outputs[0].script.len() as u64;
                let wanted = (script + payload).saturating_sub(len);
                record.outputs[0].script = vec![0x51; wanted as usize];
            }
            record
        })
        .collect()
}

#[cfg(test)]
mod tests;
