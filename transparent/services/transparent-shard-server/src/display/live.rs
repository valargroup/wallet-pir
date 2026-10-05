//! Display publication control and request-scoped serving snapshots.
//!
//! Adapts the history worker's control (`crate::live`) to a publication that
//! grows by sealing: no assignment, a worker role instead, and two operations
//! of its own. `stage` verifies a sealed revision before any map names it and
//! builds and pins its runtimes, under its own lock so the per-block
//! prepare/activate cycle carries on meanwhile. The next prepare that names it
//! takes it over by inode and finds its runtimes resident, so a seal costs the
//! cycle that publishes it nothing.
//!
//! One JSON line each way over a root-only Unix socket. Replies are flat:
//! `{"ok":true,...}` or `{"ok":false,"error":...}`.
//!
//! A reorg invalidates only unsealed revisions, and only digest-addressed
//! requests to them are refused: the map, readiness and every archive keep
//! serving until the next activation replaces the recent shard. A reorg that
//! reaches a sealed revision is refused and changes nothing; sealed shards
//! are never rewritten.

use super::serves;
use super::service::{self, json, DisplayRuntime, DisplayState};
use super::set::{DisplayRevision, DisplaySet, MAP_FILE};
use crate::assignment::WorkerRole;
use crate::metrics::Snapshot;
use crate::runtime::RuntimeHandle;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Router;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tower::ServiceExt;
use transparent_shard::display::DisplayMap;

/// A publication directory and the digest of its map.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisplayPublication {
    pub directory: PathBuf,
    pub map_sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum DisplayCommand {
    Status,
    /// Verify a sealed revision directory, named by its digest, and pin its
    /// runtimes until a map naming it is activated.
    Stage {
        directory: PathBuf,
    },
    Unstage {
        digest: String,
    },
    /// `expected` is the active map digest, or empty before the first
    /// activation.
    Prepare {
        expected: String,
        publication: DisplayPublication,
    },
    Activate {
        expected: String,
        map_sha256: String,
    },
    Invalidate {
        expected: String,
        from_height: u64,
    },
    Discard {
        map_sha256: String,
    },
    Collect,
}

/// Fields of a successful reply, beside `"ok": true`.
pub type Reply = serde_json::Map<String, serde_json::Value>;

fn reply(value: serde_json::Value) -> Reply {
    match value {
        serde_json::Value::Object(map) => map,
        _ => Reply::new(),
    }
}

struct Active {
    state: DisplayState,
    publication: DisplayPublication,
}

struct Candidate {
    prepared_at: std::time::Instant,
    state: DisplayState,
    publication: DisplayPublication,
    epoch: u64,
    built: u64,
}

struct Preparing {
    map_sha256: String,
    started: std::time::Instant,
    phase: &'static str,
}

struct PreparingGuard<'a>(&'a RwLock<Option<Preparing>>);
impl Drop for PreparingGuard<'_> {
    fn drop(&mut self) {
        *self.0.write().unwrap() = None;
    }
}

/// Clears the record of a stage in progress however the stage ends.
struct InStage<'a>(&'a Mutex<Option<DisplayRevision>>);
impl Drop for InStage<'_> {
    fn drop(&mut self) {
        *self.0.lock().unwrap() = None;
    }
}

/// A sealed revision verified ahead of its map, with its runtimes pinned.
struct Staged {
    revision: DisplayRevision,
    _handles: Vec<RuntimeHandle>,
}

struct Inner {
    runtime: Arc<DisplayRuntime>,
    role: WorkerRole,
    retain: usize,
    record: PathBuf,
    /// Where collection may delete unused shipped directories: candidates
    /// and staged revisions may be shipped to separate roots. With none,
    /// nothing on disk is removed: a root shared with the controller or
    /// another worker is not this worker's to collect.
    collect_roots: Vec<PathBuf>,
    active: RwLock<Option<Active>>,
    retired: RwLock<Vec<Active>>,
    candidate: tokio::sync::Mutex<Option<Candidate>>,
    preparing: RwLock<Option<Preparing>>,
    staged: Mutex<BTreeMap<String, Staged>>,
    /// Serializes staging apart from `operations`, so a seal's builds never
    /// hold up the per-block cycle. Collection never waits for it either.
    staging: tokio::sync::Mutex<()>,
    /// The revision a stage in progress is verifying and building.
    /// Collection keeps its directory and runtimes without waiting for it.
    in_stage: Mutex<Option<DisplayRevision>>,
    /// Held by a collection from its keep set to its last deletion; a stage
    /// registers in `in_stage` under it, so a collection that missed the
    /// registration has pruned before that stage can persist a runtime.
    collection: Arc<tokio::sync::Mutex<()>>,
    invalid: RwLock<BTreeSet<String>>,
    operations: Arc<tokio::sync::Mutex<()>>,
    publication_gate: Mutex<()>,
    epoch: AtomicU64,
}

#[derive(Clone)]
pub struct DisplayLive(Arc<Inner>);

/// Never set: staging is not cancelled by reorgs, which cannot reach a
/// sealed revision without halting the controller.
static NOT_CANCELLED: AtomicBool = AtomicBool::new(false);

impl DisplayLive {
    pub fn new(
        runtime: Arc<DisplayRuntime>,
        role: WorkerRole,
        retain: usize,
        record: PathBuf,
        collect_roots: Vec<PathBuf>,
        initial: Option<(DisplayState, DisplayPublication)>,
    ) -> Result<Self, String> {
        let invalid: BTreeSet<String> = match std::fs::read(record.with_extension("invalid.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| format!("invalid revocation record: {e}"))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeSet::new(),
            Err(e) => return Err(e.to_string()),
        };
        if let Some((state, _)) = &initial {
            state.release_pins(Some(&invalid));
        }
        Ok(Self(Arc::new(Inner {
            runtime,
            role,
            retain,
            record,
            collect_roots,
            active: RwLock::new(initial.map(|(state, publication)| Active { state, publication })),
            retired: RwLock::new(Vec::new()),
            candidate: tokio::sync::Mutex::new(None),
            preparing: RwLock::new(None),
            staged: Mutex::new(BTreeMap::new()),
            staging: tokio::sync::Mutex::new(()),
            in_stage: Mutex::new(None),
            collection: Arc::new(tokio::sync::Mutex::new(())),
            invalid: RwLock::new(invalid),
            operations: Arc::new(tokio::sync::Mutex::new(())),
            publication_gate: Mutex::new(()),
            epoch: AtomicU64::new(0),
        })))
    }

    pub fn router(&self) -> Router {
        Router::new().fallback(dispatch).with_state(self.clone())
    }

    fn active_sha256(&self) -> String {
        self.0
            .active
            .read()
            .unwrap()
            .as_ref()
            .map(|active| active.publication.map_sha256.clone())
            .unwrap_or_default()
    }

    pub async fn command(&self, command: DisplayCommand) -> Result<Reply, String> {
        match command {
            DisplayCommand::Status => Ok(self.status()),
            DisplayCommand::Stage { directory } => self.stage(directory).await,
            DisplayCommand::Unstage { digest } => {
                let removed = self.0.staged.lock().unwrap().remove(&digest).is_some();
                Ok(reply(serde_json::json!({"unstaged": removed})))
            }
            DisplayCommand::Invalidate {
                expected,
                from_height,
            } => {
                let service = self.clone();
                tokio::task::spawn_blocking(move || service.invalidate(&expected, from_height))
                    .await
                    .map_err(|e| e.to_string())?
            }
            DisplayCommand::Prepare {
                expected,
                publication,
            } => self.prepare(expected, publication).await,
            DisplayCommand::Activate {
                expected,
                map_sha256,
            } => self.activate(expected, map_sha256).await,
            DisplayCommand::Discard { map_sha256 } => {
                let mut candidate = self.0.candidate.lock().await;
                let discarded = candidate
                    .as_ref()
                    .is_some_and(|c| c.publication.map_sha256 == map_sha256);
                if discarded {
                    *candidate = None;
                }
                Ok(reply(serde_json::json!({"discarded": discarded})))
            }
            DisplayCommand::Collect => self.collect().await,
        }
    }

    fn status(&self) -> Reply {
        let candidate = self.0.candidate.try_lock().ok().and_then(|c| {
            c.as_ref().map(|c| {
                serde_json::json!({
                    "map_sha256": c.publication.map_sha256,
                    "age_seconds": c.prepared_at.elapsed().as_secs_f64(),
                    "warm": c.state.is_warm() && c.epoch == self.0.epoch.load(Ordering::Acquire),
                })
            })
        });
        let preparing = self.0.preparing.read().unwrap().as_ref().map(|p| {
            serde_json::json!({
                "map_sha256": p.map_sha256,
                "age_seconds": p.started.elapsed().as_secs_f64(),
                "phase": p.phase,
            })
        });
        let active = self.0.active.read().unwrap();
        let staged: Vec<String> = self.0.staged.lock().unwrap().keys().cloned().collect();
        reply(serde_json::json!({
            "role": self.0.role.as_str(),
            "active": active.as_ref().map(|a| &a.publication),
            "staged": staged,
            "warm": active.as_ref().is_some_and(|a| a.state.is_warm()),
            "binary_sha256": pir_control::binary_sha256(),
            "candidate": candidate,
            "preparing": preparing,
            "retired_snapshots": self.0.retired.read().unwrap().len(),
            "revoked_revisions": self.0.invalid.read().unwrap().len(),
        }))
    }

    async fn stage(&self, directory: PathBuf) -> Result<Reply, String> {
        let _staging = self.0.staging.lock().await;
        let started = std::time::Instant::now();
        if !serves(self.0.role, true) {
            return Err(format!(
                "a {} worker does not hold sealed revisions",
                self.0.role.as_str()
            ));
        }
        if !directory.is_absolute() {
            return Err("a staged directory must be an absolute path".into());
        }
        let revision = tokio::task::spawn_blocking(move || DisplayRevision::read(&directory))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        if !revision.manifest.sealed {
            return Err(format!(
                "revision {} is not sealed; only archives are staged",
                revision.digest
            ));
        }
        let digest = revision.digest.clone();
        let done = |built: u64| {
            Ok::<_, String>(reply(serde_json::json!({
                "digest": digest,
                "built": built,
                "seconds": started.elapsed().as_secs_f64(),
            })))
        };
        let held = self.0.staged.lock().unwrap().contains_key(&digest)
            || self
                .0
                .active
                .read()
                .unwrap()
                .as_ref()
                .is_some_and(|a| a.state.set().held(&digest).is_some());
        if held {
            return done(0);
        }
        let _in_stage = {
            let _collection = self.0.collection.lock().await;
            *self.0.in_stage.lock().unwrap() = Some(revision.clone());
            InStage(&self.0.in_stage)
        };
        let revision = tokio::task::spawn_blocking(move || revision.verify_tables())
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        let mut handles = Vec::new();
        let mut built = 0;
        for (table, segment) in revision.targets() {
            let (handle, produced) = self
                .0
                .runtime
                .runtime(&revision, table, segment, &NOT_CANCELLED)
                .await
                .map_err(|e| format!("staging {} {segment}: {e}", table.label()))?;
            handles.push(handle);
            built += u64::from(produced);
        }
        tracing::info!(digest = %digest, built, seconds = started.elapsed().as_secs_f64(), "display revision staged");
        self.0.staged.lock().unwrap().insert(
            digest.clone(),
            Staged {
                revision,
                _handles: handles,
            },
        );
        done(built)
    }

    async fn prepare(
        &self,
        expected: String,
        publication: DisplayPublication,
    ) -> Result<Reply, String> {
        let _operation = self.0.operations.clone().lock_owned().await;
        let started = std::time::Instant::now();
        let epoch = self.0.epoch.load(Ordering::Acquire);
        let prepared = |state: &DisplayState, built: u64| {
            reply(serde_json::json!({
                "warm": true,
                "built": built,
                "reused": (state.warm_target() as u64).saturating_sub(built),
                "seconds": started.elapsed().as_secs_f64(),
            }))
        };
        let previous = {
            let active = self.0.active.read().unwrap();
            if let Some(active) = active.as_ref() {
                if active.publication.map_sha256 == publication.map_sha256 && active.state.is_warm()
                {
                    return Ok(prepared(&active.state, 0));
                }
            }
            let current = active
                .as_ref()
                .map(|a| a.publication.map_sha256.as_str())
                .unwrap_or_default();
            if current != expected {
                return Err("active predecessor changed".into());
            }
            active.as_ref().map(|a| a.state.clone())
        };
        if let Some(candidate) = self.0.candidate.lock().await.as_ref().filter(|c| {
            c.publication.map_sha256 == publication.map_sha256
                && c.state.is_warm()
                && c.epoch == epoch
        }) {
            return Ok(prepared(&candidate.state, candidate.built));
        }
        *self.0.preparing.write().unwrap() = Some(Preparing {
            map_sha256: publication.map_sha256.clone(),
            started,
            phase: "loading",
        });
        let _preparing = PreparingGuard(&self.0.preparing);
        // Release an obsolete candidate's pins before reserving anew.
        *self.0.candidate.lock().await = None;
        let staged: Vec<DisplayRevision> = self
            .0
            .staged
            .lock()
            .unwrap()
            .values()
            .map(|staged| staged.revision.clone())
            .collect();
        let runtime = self.0.runtime.clone();
        let (role, retain) = (self.0.role, self.0.retain);
        let directory = publication.directory.clone();
        let map_sha256 = publication.map_sha256.clone();
        let next = tokio::task::spawn_blocking(move || {
            // Idle retired runtimes give way to the candidate's builds;
            // active, staged and in-flight runtimes are pinned.
            runtime.cache.evict_unpinned();
            let staged: Vec<&DisplayRevision> = staged.iter().collect();
            let set = DisplaySet::open_reusing(
                &directory,
                retain,
                previous.as_ref().map(DisplayState::set),
                &staged,
                role,
            )
            .map_err(|e| e.to_string())?;
            if set.map_digest != map_sha256 {
                return Err("candidate map digest mismatch".to_string());
            }
            if let Some(old) = previous.as_ref().map(DisplayState::set) {
                if set.map.network != old.map.network
                    || set.map.genesis_hash != old.map.genesis_hash
                    || set.map.seal != old.map.seal
                {
                    return Err("candidate changes publication identity".to_string());
                }
            }
            DisplayState::build(set, &runtime)
        })
        .await
        .map_err(|e| e.to_string())??;
        if let Some(preparing) = self.0.preparing.write().unwrap().as_mut() {
            preparing.phase = "warming";
        }
        next.spawn_prewarm().await.map_err(|e| e.to_string())?;
        if self.0.epoch.load(Ordering::Acquire) != epoch {
            return Err("reorg invalidated preparation".into());
        }
        if !next.is_warm() {
            return Err("candidate did not become completely warm within the shared budget".into());
        }
        let built = next.built();
        let result = prepared(&next, built);
        tracing::info!(map = %publication.map_sha256, built, seconds = started.elapsed().as_secs_f64(), "display publication prepared");
        *self.0.candidate.lock().await = Some(Candidate {
            prepared_at: std::time::Instant::now(),
            state: next,
            publication,
            epoch,
            built,
        });
        Ok(result)
    }

    async fn activate(&self, expected: String, map_sha256: String) -> Result<Reply, String> {
        let operation = self.0.operations.clone().lock_owned().await;
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            // The durable record write can block on disk; serialization is
            // kept even if the control caller disconnects meanwhile.
            let _operation = operation;
            let inner = &service.0;
            {
                let active = inner.active.read().unwrap();
                if let Some(active) = active.as_ref() {
                    if active.publication.map_sha256 == map_sha256 && active.state.is_warm() {
                        return Ok(Reply::new());
                    }
                }
                let current = active
                    .as_ref()
                    .map(|a| a.publication.map_sha256.as_str())
                    .unwrap_or_default();
                if current != expected {
                    return Err("active predecessor changed".into());
                }
            }
            let mut candidate = inner.candidate.blocking_lock();
            // Invalidation must not land between the epoch check and the
            // durable pointer swap.
            let _gate = inner.publication_gate.lock().unwrap();
            let next = candidate.as_ref().ok_or("no prepared candidate")?;
            if next.epoch != inner.epoch.load(Ordering::Acquire) {
                return Err("reorg invalidated prepared candidate".into());
            }
            if next.publication.map_sha256 != map_sha256 || !next.state.is_warm() {
                return Err("candidate identity/readiness mismatch".into());
            }
            let named: BTreeSet<String> = next
                .state
                .set()
                .map
                .shards
                .iter()
                .map(|s| s.manifest_digest.clone())
                .collect();
            if named
                .iter()
                .any(|digest| inner.invalid.read().unwrap().contains(digest))
            {
                return Err("candidate contains invalidated history".into());
            }
            persist(&inner.record, &next.publication)?;
            let next = candidate.take().expect("checked above");
            let old = inner.active.write().unwrap().replace(Active {
                state: next.state,
                publication: next.publication,
            });
            // Predecessors stay answerable for stale-map clients from what is
            // resident, but are no longer residency obligations.
            if let Some(old) = old {
                old.state.retire();
                inner.retired.write().unwrap().push(old);
            }
            // The active snapshot pins what it names; staging has done its job.
            inner
                .staged
                .lock()
                .unwrap()
                .retain(|digest, _| !named.contains(digest));
            Ok(Reply::new())
        })
        .await
        .map_err(|e| e.to_string())?
    }

    fn invalidate(&self, expected: &str, from_height: u64) -> Result<Reply, String> {
        let inner = &self.0;
        let _gate = inner.publication_gate.lock().unwrap();
        let active = inner.active.read().unwrap();
        let current = active
            .as_ref()
            .map(|a| a.publication.map_sha256.as_str())
            .unwrap_or_default();
        if current != expected {
            return Err("active predecessor changed".into());
        }
        let retired = inner.retired.read().unwrap();
        let staged = inner.staged.lock().unwrap();
        let snapshots: Vec<&Active> = active.iter().chain(retired.iter()).collect();
        // Sealed shards are final. A reorg that reaches one is refused before
        // anything changes, and the controller halts.
        let sealed = snapshots
            .iter()
            .flat_map(|a| a.state.set().revisions())
            .map(|r| &r.manifest)
            .chain(staged.values().map(|s| &s.revision.manifest))
            .filter(|m| m.sealed && m.end_height >= from_height)
            .map(|m| m.shard_id)
            .min();
        if let Some(shard_id) = sealed {
            return Err(format!("reorg reaches sealed display shard {shard_id}"));
        }
        let orphaned: BTreeSet<String> = snapshots
            .iter()
            .flat_map(|a| a.state.set().revisions())
            .filter(|r| r.manifest.end_height >= from_height)
            .map(|r| r.digest.clone())
            .collect();
        inner.epoch.fetch_add(1, Ordering::AcqRel);
        let mut invalid = inner.invalid.write().unwrap();
        invalid.extend(orphaned.iter().cloned());
        for snapshot in &snapshots {
            snapshot.state.release_pins(Some(&invalid));
        }
        // Persisted before it is acknowledged: a restart must not resurrect
        // an orphaned revision.
        let revoked = invalid.clone();
        drop(invalid);
        drop(staged);
        drop(retired);
        drop(active);
        persist(&inner.record.with_extension("invalid.json"), &revoked)?;
        Ok(reply(serde_json::json!({
            "invalidated": orphaned,
            "from_height": from_height,
        })))
    }

    async fn collect(&self) -> Result<Reply, String> {
        let operation = self.0.operations.clone().lock_owned().await;
        // Never waits for a stage: the controller awaits this call within
        // its cycle, and a stage can run for minutes.
        let collection = self.0.collection.clone().lock_owned().await;
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            let _operation = operation;
            let _collection = collection;
            let inner = &service.0;
            let candidate = inner.candidate.blocking_lock();
            let mut retired = inner.retired.write().unwrap();
            // A long request may hold one old snapshot without preventing
            // collection of every other unused generation behind it.
            let recent_from = retired.len().saturating_sub(3);
            let mut dropped = Vec::new();
            for (index, old) in std::mem::take(&mut *retired).into_iter().enumerate() {
                if index >= recent_from || old.state.has_other_holders() {
                    retired.push(old);
                } else {
                    dropped.push(old);
                }
            }
            let active = inner.active.read().unwrap();
            // Held with `staged`: a finishing stage inserts there before it
            // clears this, so a stage is never in neither.
            let in_stage = inner.in_stage.lock().unwrap();
            let staged = inner.staged.lock().unwrap();
            let snapshots = active
                .iter()
                .chain(retired.iter())
                .map(|a| (&a.state, &a.publication))
                .chain(candidate.iter().map(|c| (&c.state, &c.publication)));
            let mut keep_dirs = BTreeSet::new();
            let mut keys = HashSet::new();
            for (state, publication) in snapshots {
                keep_dirs.insert(publication.directory.clone());
                keys.extend(state.set().runtime_key_strings());
            }
            for revision in staged.values().map(|s| &s.revision).chain(in_stage.iter()) {
                keep_dirs.insert(revision.dir.clone());
                keys.extend(revision.runtime_key_strings());
            }
            let held: BTreeSet<String> = staged
                .keys()
                .cloned()
                .chain(in_stage.iter().map(|r| r.digest.clone()))
                .collect();
            let unpublished_from = active
                .as_ref()
                .map(|a| first_unpublished_seal(&a.state.set().map));
            let kept_snapshots = retired.len();
            drop(staged);
            drop(in_stage);
            drop(active);
            drop(retired);
            drop(candidate);
            let collected_snapshots = dropped.len();
            drop(dropped);
            let disk = inner.runtime.cache.prune_disk(&keys)?;
            let mut removed = Vec::new();
            for root in &inner.collect_roots {
                removed.extend(collect_directories(
                    root,
                    &keep_dirs,
                    &held,
                    unpublished_from,
                )?);
            }
            Ok(reply(serde_json::json!({
                "removed": removed,
                "retired_snapshots": kept_snapshots,
                "collected_snapshots": collected_snapshots,
                "disk_freed_bytes": disk.unwrap_or(0),
                "disk_collection_deferred": disk.is_none(),
            })))
        })
        .await
        .map_err(|e| format!("collection task failed: {e}"))?
    }

    pub async fn listen(
        &self,
        path: &Path,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::os::unix::fs::PermissionsExt;
        if path.exists() {
            if tokio::net::UnixStream::connect(path).await.is_ok() {
                return Err("control socket already in use".into());
            }
            std::fs::remove_file(path)?;
        }
        let listener = tokio::net::UnixListener::bind(path)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        loop {
            let (stream, _) = listener.accept().await?;
            let service = self.clone();
            tokio::spawn(async move {
                let (read, mut write) = stream.into_split();
                let mut reader = BufReader::new(read);
                let mut bytes = Vec::new();
                use tokio::io::AsyncReadExt;
                let result = match (&mut reader)
                    .take(1024 * 1024)
                    .read_until(b'\n', &mut bytes)
                    .await
                {
                    Ok(_) if bytes.last() == Some(&b'\n') => match serde_json::from_slice(&bytes) {
                        Ok(command) => service.command(command).await,
                        Err(error) => Err(error.to_string()),
                    },
                    _ => Err("invalid or oversized control command".into()),
                };
                let line = reply_line(result);
                let _ = write.write_all(format!("{line}\n").as_bytes()).await;
            });
        }
    }
}

/// The control reply line for a command result.
pub fn reply_line(result: Result<Reply, String>) -> serde_json::Value {
    match result {
        Ok(fields) => {
            let mut line = Reply::new();
            line.insert("ok".into(), true.into());
            line.extend(fields);
            serde_json::Value::Object(line)
        }
        Err(error) => serde_json::json!({"ok": false, "error": error}),
    }
}

/// The lowest shard id whose sealed revision `map` has not published. Shard
/// ids are contiguous and only the last entry may be unsealed, and a seal
/// takes the recent shard's id, so every sealed revision below this one is
/// published or dropped from the window, and none of them is staged again.
fn first_unpublished_seal(map: &DisplayMap) -> u64 {
    let last = map.shards.last().expect("a checked map is nonempty");
    last.shard_id + u64::from(last.sealed)
}

/// Removes shipped directories under `root` that nothing uses: candidates
/// (holding a map) beyond the newest three unused, and revisions (named by
/// their digest, holding a manifest) that are not `held`.
///
/// A sealed revision at or above `unpublished_from` (every sealed revision,
/// before any activation) may have been shipped for a stage that has not
/// arrived yet, so it stays until a map publishes its shard.
fn collect_directories(
    root: &Path,
    keep: &BTreeSet<PathBuf>,
    held: &BTreeSet<String>,
    unpublished_from: Option<u64>,
) -> Result<Vec<String>, String> {
    let mut candidates = Vec::new();
    let mut removed = Vec::new();
    for entry in std::fs::read_dir(root).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();
        if !entry.file_type().is_ok_and(|t| t.is_dir()) || keep.contains(&path) {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.join(MAP_FILE).is_file() {
            let modified = entry.metadata().and_then(|m| m.modified()).ok();
            candidates.push((modified, path));
            continue;
        }
        if name.len() != 64 || !name.bytes().all(|c| c.is_ascii_hexdigit()) || held.contains(&name)
        {
            continue;
        }
        // Unreadable or foreign content is left for an operator.
        let Ok(revision) = DisplayRevision::read(&path) else {
            continue;
        };
        let manifest = &revision.manifest;
        let awaiting =
            manifest.sealed && unpublished_from.is_none_or(|from| manifest.shard_id >= from);
        if !awaiting {
            std::fs::remove_dir_all(&path).map_err(|e| e.to_string())?;
            removed.push(path.display().to_string());
        }
    }
    candidates.sort_by_key(|(modified, _)| std::cmp::Reverse(*modified));
    for (_, path) in candidates.into_iter().skip(3) {
        std::fs::remove_dir_all(&path).map_err(|e| e.to_string())?;
        removed.push(path.display().to_string());
    }
    Ok(removed)
}

/// Picks the snapshot holding a digest-addressed request's revision, newest
/// first, so a client on a superseded map is answered from what it fetched.
/// Tier and role are then checked inside that snapshot.
async fn dispatch(State(live): State<DisplayLive>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    let digest = path
        .split('/')
        .collect::<Vec<_>>()
        .windows(2)
        .find(|w| w[0] == "revisions")
        .map(|w| w[1].to_owned());
    let (state, map_sha256) = {
        let active = live.0.active.read().unwrap();
        let Some(active) = active.as_ref() else {
            drop(active);
            return live.unpublished(&path);
        };
        let mut state = active.state.clone();
        if let Some(digest) = &digest {
            if state.set().revision(digest).is_none() {
                if let Some(old) = live
                    .0
                    .retired
                    .read()
                    .unwrap()
                    .iter()
                    .rev()
                    .find(|a| a.state.set().revision(digest).is_some())
                {
                    state = old.state.clone();
                }
            }
        }
        (state, active.publication.map_sha256.clone())
    };
    let invalid = || {
        digest
            .as_ref()
            .is_some_and(|digest| live.0.invalid.read().unwrap().contains(digest))
    };
    let refused = || {
        json(
            StatusCode::CONFLICT,
            serde_json::json!({
                "error": "revision invalidated by a reorg",
                "retry": "refresh the txid map",
                "map_sha256": map_sha256,
            }),
        )
    };
    if invalid() {
        return refused();
    }
    let response = service::router(state.clone())
        .oneshot(request)
        .await
        .expect("infallible router");
    // A reorg during evaluation must not release an orphaned answer.
    if invalid() {
        return refused();
    }
    // A retired snapshot refuses what is no longer resident under its own,
    // superseded map; the client is sent to the active one.
    if response.status() == StatusCode::CONFLICT && state.is_retired() {
        return json(
            StatusCode::CONFLICT,
            serde_json::json!({
                "error": format!(
                    "revision {} is no longer served",
                    digest.as_deref().unwrap_or_default()
                ),
                "retry": "refresh the txid map",
                "map_sha256": live.active_sha256(),
            }),
        );
    }
    response
}

impl DisplayLive {
    /// Before the first activation the worker is alive but has nothing to
    /// serve.
    fn unpublished(&self, path: &str) -> Response {
        let role = self.0.role.as_str();
        match path {
            "/v1/health" => json(
                StatusCode::OK,
                serde_json::json!({
                    "phase": "waiting for a publication",
                    "role": role,
                    "staged": self.0.staged.lock().unwrap().len(),
                    "binary_sha256": pir_control::binary_sha256(),
                }),
            ),
            "/v1/ready" => json(
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::json!({
                    "ready": false,
                    "reason": "no display publication is active",
                    "role": role,
                    "map_sha256": null,
                    "warm": false,
                }),
            ),
            "/metrics" => (
                StatusCode::OK,
                [("content-type", "text/plain; version=0.0.4")],
                self.0.runtime.metrics.render(&Snapshot {
                    labels: vec![("role".into(), role.into())],
                    cache_budget_bytes: self.0.runtime.cache.budget(),
                    ..Snapshot::default()
                }),
            )
                .into_response(),
            _ => (
                StatusCode::SERVICE_UNAVAILABLE,
                [("content-type", "application/json"), ("retry-after", "1")],
                serde_json::json!({
                    "error": "no display publication is active",
                    "retry": "retry shortly",
                })
                .to_string(),
            )
                .into_response(),
        }
    }

    /// The active snapshot's map digest, or empty before the first
    /// activation; what a controller passes as `expected`.
    pub fn expected(&self) -> String {
        self.active_sha256()
    }
}

fn persist(path: &Path, value: &impl Serialize) -> Result<(), String> {
    use std::io::Write;
    let parent = path.parent().ok_or("record needs a parent directory")?;
    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let temp = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temp).map_err(|e| e.to_string())?;
    file.write_all(&serde_json::to_vec(value).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    std::fs::rename(temp, path).map_err(|e| e.to_string())?;
    std::fs::File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_and_replies_use_the_wire_shapes() {
        let command: DisplayCommand = serde_json::from_str(
            r#"{"operation":"prepare","expected":"","publication":{"directory":"/p","map_sha256":"ab"}}"#,
        )
        .unwrap();
        assert!(matches!(command, DisplayCommand::Prepare { expected, .. } if expected.is_empty()));
        for line in [
            r#"{"operation":"status"}"#,
            r#"{"operation":"stage","directory":"/s/aa"}"#,
            r#"{"operation":"unstage","digest":"aa"}"#,
            r#"{"operation":"activate","expected":"a","map_sha256":"b"}"#,
            r#"{"operation":"invalidate","expected":"a","from_height":7}"#,
            r#"{"operation":"discard","map_sha256":"b"}"#,
            r#"{"operation":"collect"}"#,
        ] {
            serde_json::from_str::<DisplayCommand>(line).unwrap();
        }
        let ok = reply_line(Ok(reply(serde_json::json!({"built": 2}))));
        assert_eq!(ok, serde_json::json!({"ok": true, "built": 2}));
        let refused = reply_line(Err("no".into()));
        assert_eq!(refused, serde_json::json!({"ok": false, "error": "no"}));
    }

    #[test]
    fn collection_removes_only_unused_shipped_directories() {
        use super::super::set::MANIFEST_FILE;
        use super::super::synth::{self, ShardSpec};
        let store = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let make = |name: &str, file: &str| {
            let dir = root.path().join(name);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(file), b"{}").unwrap();
            dir
        };
        // A revision of `shard_id` shipped into the collect root.
        let ship = |shard_id: u64, sealed: bool| {
            let spec = ShardSpec {
                shard_id,
                start_height: 10 * shard_id,
                end_height: 10 * shard_id + 9,
                sealed,
                revision: 0,
                supersedes: String::new(),
                parent_manifest_digest: String::new(),
                n_buckets: 1,
                archive_target: 1,
                geometry: &transparent_shard::display::TXID_2K,
            };
            let records = synth::records(3, shard_id);
            let published = synth::write_shard(store.path(), &spec, &records).unwrap();
            let dir = root.path().join(&published.digest);
            synth::link_revision(&published.dir, &dir).unwrap();
            (published.digest, dir)
        };
        let candidates: Vec<PathBuf> = (0..6)
            .map(|i| make(&format!("{:064x}", i), MAP_FILE))
            .collect();
        let (_, published) = ship(0, true);
        let (staged, staged_dir) = ship(1, true);
        let (_, awaiting) = ship(2, true);
        let (_, recent) = ship(2, false);
        let broken = make(&"cd".repeat(32), MANIFEST_FILE);
        let foreign = make("sealed", MANIFEST_FILE);
        let keep = BTreeSet::from([candidates[0].clone()]);
        let held = BTreeSet::from([staged]);

        // Before any activation a sealed revision may still await its stage.
        let removed = collect_directories(root.path(), &keep, &held, None).unwrap();
        assert!(published.exists() && staged_dir.exists() && awaiting.exists());
        assert!(!recent.exists());
        assert!(candidates[0].exists() && broken.exists() && foreign.exists());
        // Five unused candidates; the newest three survive. Two go, with the
        // unused recent revision.
        let left = candidates.iter().filter(|c| c.exists()).count();
        assert_eq!(left, 4);
        assert_eq!(removed.len(), 3);

        // Once a map publishes shards 0 and 1, shard 0's shipped copy goes.
        // Shard 1 stays while it is staged, shard 2 until a map names it.
        let removed = collect_directories(root.path(), &keep, &held, Some(2)).unwrap();
        assert_eq!(removed, vec![published.display().to_string()]);
        assert!(staged_dir.exists() && awaiting.exists());
        let removed = collect_directories(root.path(), &keep, &BTreeSet::new(), Some(3)).unwrap();
        assert_eq!(removed.len(), 2);
        assert!(!staged_dir.exists() && !awaiting.exists());
    }

    /// A stage holds its lock through verification and cold builds; the
    /// controller's collection, awaited within its cycle, must not wait.
    #[tokio::test]
    async fn collection_does_not_wait_for_a_stage() {
        use crate::service::ServiceConfig;
        let root = tempfile::tempdir().unwrap();
        let live = DisplayLive::new(
            DisplayRuntime::new(ServiceConfig::default(), None),
            WorkerRole::ArchiveOwner,
            3,
            root.path().join("active.json"),
            vec![root.path().to_path_buf()],
            None,
        )
        .unwrap();
        let _stage = live.0.staging.lock().await;
        let collected = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            live.command(DisplayCommand::Collect),
        )
        .await
        .expect("collection waited for the stage")
        .unwrap();
        assert_eq!(collected["removed"], serde_json::json!([]));
    }

    #[test]
    fn seals_are_published_below_the_recent_shard() {
        use super::super::synth;
        use transparent_shard::display::{DisplayMapEntry, DisplaySealParams};
        let entry = |shard_id: u64, sealed: bool| DisplayMapEntry {
            shard_id,
            start_height: shard_id,
            end_height: shard_id,
            parent_block_hash: String::new(),
            terminal_block_hash: String::new(),
            geometry: String::new(),
            n_buckets: 1,
            directory_segments: vec![1],
            page_segments: 1,
            records: 1,
            min_bucket_records: 1,
            manifest_digest: String::new(),
            revision: 0,
            sealed,
        };
        let mut map = synth::map(&DisplaySealParams::default(), &[]);
        map.shards = vec![entry(4, true), entry(5, true), entry(6, false)];
        assert_eq!(first_unpublished_seal(&map), 6);
        map.shards.pop();
        assert_eq!(first_unpublished_seal(&map), 6);
    }
}
