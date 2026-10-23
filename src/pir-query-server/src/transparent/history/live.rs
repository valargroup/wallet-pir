//! Private publication control and request-scoped, atomic serving snapshots.
use crate::transparent::durable::persist;
use crate::transparent::history::service::{self, ServiceConfig, ServiceState};
use crate::transparent::preparation::{PreparationOutcome, PreparingGuard, Refusal};
use axum::{
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Router,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use tower::ServiceExt;
use transparent_shared::history::{
    assignment::Assignment,
    shardset::{LoadOptions, LoadScope, ShardSet},
};

/// Refusal for a prepare that names a publication this worker already holds
/// under a different row of the assignment.
pub const ASSIGNMENT_CHANGED: &str =
    "this publication is already held under a different assignment for this worker";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Publication {
    pub directory: PathBuf,
    pub assignment: Option<PathBuf>,
    pub map_sha256: String,
    /// Where the runtimes its publisher prepared were staged, when they were:
    /// plain files beside the revision directories, tried before a restore
    /// or build. Absent, the worker produces every runtime itself. Recorded
    /// with the active publication, so a restart is offered them too.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtimes: Option<PathBuf>,
}

impl Publication {
    /// The prepared runtimes this publication offers, if any.
    pub fn shipped(&self) -> Option<transparent_shared::runtime::disk::ShippedRuntimes> {
        self.runtimes
            .clone()
            .map(transparent_shared::runtime::disk::ShippedRuntimes::new)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum Command {
    Status,
    Prepare {
        expected: String,
        publication: Publication,
    },
    Activate {
        expected: String,
        map_sha256: String,
    },
    Invalidate {
        expected: String,
        from_height: u64,
        /// Digests whose terminal hashes the controller just confirmed on the
        /// canonical chain (needed when retrying a partially activated reorg).
        #[serde(default)]
        keep_digests: BTreeSet<String>,
    },
    Discard {
        map_sha256: String,
    },
    Collect,
}

struct Active {
    state: ServiceState,
    publication: Publication,
}
struct Candidate {
    prepared_at: std::time::Instant,
    state: ServiceState,
    publication: Publication,
    epoch: u64,
}
struct Preparing {
    digest: String,
    started: std::time::Instant,
    phase: &'static str,
}
struct Inner {
    active: RwLock<Active>,
    invalid: RwLock<BTreeSet<String>>,
    candidate: tokio::sync::Mutex<Option<Candidate>>,
    preparing: RwLock<Option<Preparing>>,
    last_preparation: RwLock<Option<PreparationOutcome>>,
    config: ServiceConfig,
    options: LoadOptions,
    record: PathBuf,
    retired: RwLock<Vec<Active>>,
    operations: Arc<tokio::sync::Mutex<()>>,
    publication_gate: std::sync::Mutex<()>,
    epoch: std::sync::atomic::AtomicU64,
    required_runtimes: bool,
    _directory_owner: Option<Arc<std::fs::File>>,
}
#[derive(Clone)]
pub struct LiveService(Arc<Inner>);

struct HistoryActivation(Arc<Inner>);

impl crate::transparent::activation::Adapter for HistoryActivation {
    type Candidate = Candidate;
    type Validated = ();

    fn active(&self, requested: &str) -> (String, bool) {
        let inner = &self.0;
        let active = inner.active.read().unwrap();
        (
            active.publication.map_sha256.clone(),
            !inner.required_runtimes
                && active.publication.map_sha256 == requested
                && active.state.is_ready(),
        )
    }

    fn candidate(&self) -> &tokio::sync::Mutex<Option<Candidate>> {
        &self.0.candidate
    }

    fn publication_gate(&self) -> &std::sync::Mutex<()> {
        &self.0.publication_gate
    }

    fn epoch(&self) -> u64 {
        self.0.epoch.load(std::sync::atomic::Ordering::Acquire)
    }

    fn candidate_epoch(candidate: &Candidate) -> u64 {
        candidate.epoch
    }

    fn digest(candidate: &Candidate) -> &str {
        &candidate.publication.map_sha256
    }

    fn ready(candidate: &Candidate) -> bool {
        candidate.state.is_ready()
    }

    fn validate(&self, candidate: &Candidate) -> Result<(), String> {
        if candidate
            .state
            .set()
            .map
            .shards
            .iter()
            .any(|s| self.0.invalid.read().unwrap().contains(&s.manifest_digest))
        {
            return Err("candidate contains invalidated history".into());
        }
        Ok(())
    }

    fn persist(&self, candidate: &Candidate) -> Result<(), String> {
        persist(&self.0.record, &candidate.publication)
    }

    fn install(&self, candidate: Candidate, (): ()) {
        let inner = &self.0;
        let old = std::mem::replace(
            &mut *inner.active.write().unwrap(),
            Active {
                state: candidate.state,
                publication: candidate.publication,
            },
        );
        // Normal predecessors remain available to in-flight wallets, but
        // only current and preparing assignments are residency obligations.
        old.state.stop_warming();
        inner.retired.write().unwrap().push(old);
    }
}

impl LiveService {
    pub fn new(
        state: ServiceState,
        publication: Publication,
        config: ServiceConfig,
        options: LoadOptions,
        record: PathBuf,
    ) -> Result<Self, String> {
        Self::new_with_policy(state, publication, config, options, record, None)
    }
    pub(crate) fn new_required(
        state: ServiceState,
        publication: Publication,
        config: ServiceConfig,
        options: LoadOptions,
        record: PathBuf,
        directory_owner: Arc<std::fs::File>,
    ) -> Result<Self, String> {
        Self::new_with_policy(
            state,
            publication,
            config,
            options,
            record,
            Some(directory_owner),
        )
    }
    fn new_with_policy(
        state: ServiceState,
        publication: Publication,
        config: ServiceConfig,
        options: LoadOptions,
        record: PathBuf,
        directory_owner: Option<Arc<std::fs::File>>,
    ) -> Result<Self, String> {
        let required_runtimes = directory_owner.is_some();
        let invalid: BTreeSet<String> = match std::fs::read(record.with_extension("invalid.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| format!("invalid revocation record: {e}"))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeSet::new(),
            Err(e) => return Err(e.to_string()),
        };
        if !required_runtimes {
            state.release_invalidated(&invalid);
        }
        Ok(Self(Arc::new(Inner {
            active: RwLock::new(Active { state, publication }),
            invalid: RwLock::new(invalid),
            candidate: tokio::sync::Mutex::new(None),
            preparing: RwLock::new(None),
            last_preparation: RwLock::new(None),
            config,
            options,
            required_runtimes,
            _directory_owner: directory_owner,
            record,
            retired: RwLock::new(Vec::new()),
            operations: Arc::new(tokio::sync::Mutex::new(())),
            publication_gate: std::sync::Mutex::new(()),
            epoch: std::sync::atomic::AtomicU64::new(0),
        })))
    }
    /// Loads, builds and warms a candidate publication. Refusals are typed by
    /// what the controller must do about them; a refused command reports
    /// only their text.
    async fn prepare(
        &self,
        expected: String,
        publication: Publication,
    ) -> Result<serde_json::Value, Refusal> {
        let epoch = self.0.epoch.load(std::sync::atomic::Ordering::Acquire);
        // What this worker's own row of the requested assignment
        // digests to. A publication already held under another row
        // must be refused, not reported prepared: the durable active
        // record names the assignment file, and a restart would load
        // rows no prepare ever checked.
        let requested = match (&self.0.options.scope, &publication.assignment) {
            (LoadScope::Assigned { worker_id, .. }, Some(path)) => Some(
                Assignment::load(path)
                    .and_then(|assignment| assignment.worker_digest(worker_id))
                    .map_err(|e| Refusal::InvalidSource(e.to_string()))?,
            ),
            _ => None,
        };
        let held_as_requested = |set: &ShardSet| {
            set.scope().map(|scope| &scope.worker_assignment_sha256) == requested.as_ref()
        };
        let old = {
            let active = self.0.active.read().unwrap();
            if active.publication.map_sha256 == publication.map_sha256 && active.state.is_ready() {
                if !held_as_requested(active.state.set()) {
                    return Err(Refusal::Admission(ASSIGNMENT_CHANGED.into()));
                }
                if !self.0.required_runtimes
                    || active.publication.directory == publication.directory
                {
                    return Ok(serde_json::json!({"prepared":publication.map_sha256}));
                }
            }
            if active.publication.map_sha256 != expected {
                return Err(Refusal::Superseded("active predecessor changed".into()));
            }
            active.state.clone()
        };
        if let Some(candidate) = self.0.candidate.lock().await.as_ref().filter(|c| {
            c.publication.map_sha256 == publication.map_sha256
                && c.state.is_ready()
                && c.epoch == epoch
                && (!self.0.required_runtimes || c.publication.directory == publication.directory)
        }) {
            if !held_as_requested(candidate.state.set()) {
                return Err(Refusal::Admission(ASSIGNMENT_CHANGED.into()));
            }
            return Ok(serde_json::json!({"prepared":publication.map_sha256}));
        }
        let prepare_started = std::time::Instant::now();
        let _preparing = PreparingGuard::record(
            &self.0.preparing,
            Preparing {
                digest: publication.map_sha256.clone(),
                started: prepare_started,
                phase: "loading",
            },
        );
        // Release an obsolete candidate's pins before reserving anew.
        if let Some(candidate) = self.0.candidate.lock().await.take() {
            candidate.state.stop_warming();
        }
        let mut options = self.0.options.clone();
        match (&options.scope, &publication.assignment) {
            (LoadScope::Assigned { worker_id, .. }, Some(path)) => {
                options.scope = LoadScope::Assigned {
                    worker_id: worker_id.clone(),
                    assignment: Arc::new(
                        Assignment::load(path)
                            .map_err(|e| Refusal::InvalidSource(e.to_string()))?,
                    ),
                };
            }
            (LoadScope::Whole, None) => {}
            _ => {
                return Err(Refusal::InvalidSource(
                    "candidate must preserve assignment mode".into(),
                ))
            }
        }
        if self.0.required_runtimes {
            if let LoadScope::Assigned {
                assignment,
                worker_id,
            } = &options.scope
            {
                let next = assignment
                    .scope_for(worker_id)
                    .map_err(|e| Refusal::InvalidSource(e.to_string()))?;
                if old
                    .set()
                    .scope()
                    .is_some_and(|held| !held.assigned.is_subset(&next.assigned))
                    && old.archive().is_none()
                {
                    return Err(Refusal::Admission("local retirement requires a qualified archive route; recent-worker mode has none".into()));
                }
            }
        }
        let directory = publication.directory.clone();
        let digest = publication.map_sha256.clone();
        let shipped = if self.0.required_runtimes {
            Some(
                transparent_shared::runtime::disk::ShippedRuntimes::required(
                    publication.runtimes.clone().ok_or_else(|| {
                        Refusal::InvalidSource("provider requires prepared runtimes".into())
                    })?,
                ),
            )
        } else {
            publication.shipped()
        };
        let config = self.0.config;
        let loading_started = std::time::Instant::now();
        let retiring = old.clone();
        let (next, verification, material) = tokio::task::spawn_blocking(move || {
            tracing::info!(map = %digest, queue_seconds = loading_started.elapsed().as_secs_f64(), "publication loading task started");
            let eviction_started = std::time::Instant::now();
            old.evict_unpinned_except_current();
            tracing::info!(map = %digest, seconds = eviction_started.elapsed().as_secs_f64(), "publication runtime eviction finished");
            let open_started = std::time::Instant::now();
            let set = ShardSet::open_for_serving(&directory, &options)
                .map_err(|e| Refusal::InvalidSource(e.to_string()))?;
            let verification = set.verification().clone();
            tracing::info!(
                map = %digest,
                seconds = open_started.elapsed().as_secs_f64(),
                segments_hashed = verification.segments_hashed,
                segments_reused = verification.segments_reused,
                bytes_hashed = verification.bytes_hashed,
                page_bytes_hashed = verification.page_bytes_hashed,
                bytes_reused = verification.bytes_reused,
                verify_cpu_micros = verification.cpu_micros,
                "publication shard loading finished"
            );
            if set.map_digest != digest {
                return Err(Refusal::InvalidSource("candidate map digest mismatch".into()));
            }
            if set.map.genesis_hash != old.set().map.genesis_hash
                || set.map.start_height != old.set().map.start_height
                || set.map.seal != old.set().map.seal
            {
                return Err(Refusal::InvalidSource(
                    "candidate changes publication identity".into(),
                ));
            }
            let build_started = std::time::Instant::now();
            let state = ServiceState::build_reusing(set, config, None, Some(&old), shipped);
            tracing::info!(map = %digest, seconds = build_started.elapsed().as_secs_f64(), succeeded = state.is_ok(), "publication service construction finished");
            let state = state.and_then(|state| {
                let material = if state.required_material() {
                    Some(state.check_required_material()?)
                } else {
                    None
                };
                Ok((state, material))
            });
            state
                .map(|(state, material)| (state, verification, material))
                .map_err(Refusal::Runtime)
        })
        .await
        .map_err(|e| Refusal::Runtime(e.to_string()))??;
        if self.0.required_runtimes || retiring.archive().is_some() {
            next.qualify_retirement(&retiring)
                .await
                .map_err(Refusal::Admission)?;
        }
        let loading_seconds = loading_started.elapsed().as_secs_f64();
        let warming_started = std::time::Instant::now();
        self.0.preparing.write().unwrap().as_mut().unwrap().phase = "warming";
        let _prewarm = next.spawn_prewarm();
        let warming_seconds = warming_started.elapsed().as_secs_f64();
        tracing::info!(map = %publication.map_sha256, loading_seconds, warming_seconds, "publication preparation stages");
        if self.0.epoch.load(std::sync::atomic::Ordering::Acquire) != epoch {
            return Err(Refusal::Superseded("reorg invalidated preparation".into()));
        }
        if !next.is_ready() {
            return Err(Refusal::Admission("candidate is not ready".into()));
        }
        let [loading, warming, total] = &next.metrics().prepare_seconds;
        loading.observe(std::time::Duration::from_secs_f64(loading_seconds));
        warming.observe(std::time::Duration::from_secs_f64(warming_seconds));
        total.observe(prepare_started.elapsed());
        let preparation = next.preparation();
        tracing::info!(map = %publication.map_sha256, %preparation, "publication runtimes produced");
        *self.0.candidate.lock().await = Some(Candidate {
            prepared_at: std::time::Instant::now(),
            state: next,
            publication: publication.clone(),
            epoch,
        });
        Ok(
            serde_json::json!({"prepared":publication.map_sha256,"loading_seconds":loading_seconds,"warming_seconds":warming_seconds,"runtimes":preparation,"verification":verification,"required_material":material}),
        )
    }

    pub(crate) fn provider_observations(&self) -> serde_json::Value {
        let active = self.0.active.read().unwrap();
        let state = &active.state;
        let invalid = self.0.invalid.read().unwrap();
        let coverage: Vec<_> = state
            .set()
            .map
            .shards
            .iter()
            .map(|s| {
                serde_json::json!({"shard_id":s.shard_id,"start_height":s.start_height,
                "end_height":s.end_height,"manifest_digest":s.manifest_digest,
                "local":state.set().is_assigned(s.shard_id),
                "revoked":invalid.contains(&s.manifest_digest)})
            })
            .collect();
        serde_json::json!({
            "binary_sha256":pir_control::binary_sha256(),
            "incarnation":pir_control::Identity::process().incarnation,
            "verified_publication":{"map_sha256":active.publication.map_sha256},
            "artifact_validation":state.artifact_validation(),
            "runtime_readiness":{"warm":state.is_warm(),"invalidated":state.is_invalidated(),
                "required":state.required_material(),"preparation":state.preparation()},
            "active_coverage":coverage,
            "archive_reachability":state.archive().map(|a| a.observation()),
        })
    }

    /// Whether revocation permanently cancelled the current snapshot.
    pub(crate) fn is_invalidated(&self) -> bool {
        self.0.active.read().unwrap().state.is_invalidated()
    }

    /// Whether `/v1/ready` would report this provider ready: the active
    /// publication is not invalidated, holds shards and none of its shards'
    /// revisions is withdrawn. Reads only in-memory state.
    pub fn ready(&self) -> bool {
        let active = self.0.active.read().unwrap();
        let set = active.state.set();
        let invalid = self.0.invalid.read().unwrap();
        active.state.is_ready()
            && !set.is_empty()
            && !set
                .map
                .shards
                .iter()
                .any(|s| invalid.contains(&s.manifest_digest))
    }

    pub fn router(&self) -> Router {
        let telemetry = self.0.active.read().unwrap().state.metrics().http.clone();
        Router::new()
            .fallback(dispatch)
            .layer(axum::middleware::from_fn_with_state(
                telemetry,
                transparent_shared::http::observe,
            ))
            .with_state(self.clone())
    }

    pub async fn command(&self, command: Command) -> Result<serde_json::Value, String> {
        // Preparation is serialized with activation; invalidation is allowed to
        // interrupt it and is checked again before activation.
        if let Command::Invalidate {
            expected,
            from_height,
            keep_digests,
        } = &command
        {
            let service = self.clone();
            let expected = expected.clone();
            let from_height = *from_height;
            let keep_digests = keep_digests.clone();
            return tokio::task::spawn_blocking(move || {
                let _gate = service.0.publication_gate.lock().unwrap();
                let active = service.0.active.read().unwrap();
                if active.publication.map_sha256 != expected {
                    return Err("active predecessor changed".into());
                }
                let retired = service.0.retired.read().unwrap();
                let digests: Vec<_> = std::iter::once(&*active)
                    .chain(retired.iter())
                    .flat_map(|a| a.state.set().revisions().iter())
                    .filter(|s| {
                        s.manifest.end_height >= from_height && !keep_digests.contains(&s.digest)
                    })
                    .map(|s| s.digest.clone())
                    .collect();
                service
                    .0
                    .epoch
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                let mut invalid = service.0.invalid.write().unwrap();
                invalid.retain(|digest| !keep_digests.contains(digest));
                invalid.extend(digests);
                active.state.release_invalidated(&invalid);
                for old in retired.iter() {
                    old.state.release_invalidated(&invalid);
                }
                // Persist invalidation before acknowledging it; restart must not
                // resurrect an orphaned publication.
                let revoked = invalid.clone();
                drop(invalid);
                drop(retired);
                drop(active);
                persist(&service.0.record.with_extension("invalid.json"), &revoked)?;
                Ok(serde_json::json!({"invalidated": true, "from_height":from_height}))
            })
            .await
            .map_err(|e| e.to_string())?;
        }
        if matches!(command, Command::Status) {
            let candidate = self.0.candidate.try_lock().ok().and_then(|c| c.as_ref().map(|c| serde_json::json!({"map_sha256":c.publication.map_sha256,"age_seconds":c.prepared_at.elapsed().as_secs_f64(),"epoch":c.epoch,"warm":c.state.is_warm() && c.epoch == self.0.epoch.load(std::sync::atomic::Ordering::Acquire),"runtimes":c.state.preparation()})));
            let preparing = self.0.preparing.read().unwrap().as_ref().map(|p| serde_json::json!({"map_sha256":p.digest,"age_seconds":p.started.elapsed().as_secs_f64(),"phase":p.phase}));
            let active = self.0.active.read().unwrap();
            let retired = self.0.retired.read().unwrap();
            let revisions:std::collections::BTreeMap<_,_>=std::iter::once(&*active).chain(retired.iter()).flat_map(|a|a.state.set().revisions().iter())
                .map(|s|(s.digest.clone(),serde_json::json!({"digest":s.digest,"end_height":s.manifest.end_height,"terminal_block_hash":s.manifest.terminal_block_hash}))).collect();
            return Ok(
                serde_json::json!({"artifact_validation":active.state.artifact_validation(),"active":active.publication,"runtimes":active.state.preparation(),"warm":active.state.is_warm(),"invalidated":active.state.is_invalidated(),"revoked_revisions":self.0.invalid.read().unwrap().len(),"revoked_digests":self.0.invalid.read().unwrap().clone(),"preparing":preparing,"last_preparation":self.0.last_preparation.read().unwrap().clone(),"retired_snapshots":retired.len(),"candidate":candidate,"revisions":revisions.into_values().collect::<Vec<_>>()}),
            );
        }
        let _operation = self.0.operations.clone().lock_owned().await;
        match command {
            Command::Prepare {
                expected,
                publication,
            } => {
                let digest = publication.map_sha256.clone();
                let outcome = self.prepare(expected, publication).await;
                let verification = outcome
                    .as_ref()
                    .ok()
                    .and_then(|reply| reply.get("verification"))
                    .and_then(|report| serde_json::from_value(report.clone()).ok());
                *self.0.last_preparation.write().unwrap() =
                    Some(PreparationOutcome::new(digest, &outcome, verification));
                outcome.map_err(String::from)
            }
            Command::Activate {
                expected,
                map_sha256,
            } => self.activate(expected, map_sha256, _operation).await,

            Command::Collect => {
                let collection_started = std::time::Instant::now();
                let service = self.clone();
                tokio::task::spawn_blocking(move || {
                    // The job owns serialization even if its control caller is
                    // cancelled. A later prepare must not race disk pruning.
                    let _operation = _operation;
                    let candidate = service.0.candidate.blocking_lock();
                    let candidate_dir = candidate.as_ref().map(|c| c.publication.directory.clone());
                    let mut runtime_digests: std::collections::HashSet<String> = candidate
                        .as_ref()
                        .into_iter()
                        .flat_map(|c| c.state.set().revisions())
                        .flat_map(|s| s.runtime_identities())
                        .collect();
                    let mut retired = service.0.retired.write().unwrap();
                    let removed = retain_snapshots(&mut retired);
                    let active = service.0.active.read().unwrap();
                    let root = active
                        .publication
                        .directory
                        .parent()
                        .ok_or("publication lacks parent")?
                        .to_path_buf();
                    let mut keep: BTreeSet<PathBuf> = retired
                        .iter()
                        .map(|a| a.publication.directory.clone())
                        .collect();
                    keep.insert(active.publication.directory.clone());
                    if let Some(directory) = candidate_dir {
                        keep.insert(directory);
                    }
                    runtime_digests.extend(
                        std::iter::once(&*active)
                            .chain(retired.iter())
                            .flat_map(|a| a.state.set().revisions())
                            .flat_map(|s| s.runtime_identities()),
                    );
                    let state = active.state.clone();
                    // Status and request dispatch must never wait on disk writes,
                    // directory traversal, or deletion while holding serving locks.
                    drop(active);
                    drop(retired);
                    drop(candidate);
                    drop(removed);
                    let snapshot_seconds = collection_started.elapsed().as_secs_f64();
                    let disk_started = std::time::Instant::now();
                    let disk_pruned = state.prune_disk(&runtime_digests)?;
                    let disk_collection_deferred = disk_pruned.is_none();
                    let disk_freed_bytes = disk_pruned.unwrap_or(0);
                    let disk_seconds = disk_started.elapsed().as_secs_f64();
                    let directories_started = std::time::Instant::now();
                    let source_collection_deferred = !collect_publications(&root, &keep)
                        .map_err(|e| e.to_string())?;
                    let directory_seconds = directories_started.elapsed().as_secs_f64();
                    tracing::info!(snapshot_seconds, disk_seconds, directory_seconds, disk_freed_bytes, disk_collection_deferred, source_collection_deferred, "publication collection stages");
                    Ok(serde_json::json!({"collected":true,"source_collection_deferred":source_collection_deferred,"disk_freed_bytes":disk_freed_bytes,"disk_collection_deferred":disk_collection_deferred,"snapshot_seconds":snapshot_seconds,"disk_seconds":disk_seconds,"directory_seconds":directory_seconds}))
                })
                .await
                .map_err(|e| format!("collection task failed: {e}"))?
            }
            Command::Discard { map_sha256 } => {
                let mut candidate = self.0.candidate.lock().await;
                if candidate
                    .as_ref()
                    .is_some_and(|c| c.publication.map_sha256 == map_sha256)
                {
                    if let Some(discarded) = candidate.take() {
                        discarded.state.stop_warming();
                    }
                }
                Ok(serde_json::json!({"discarded":map_sha256}))
            }
            _ => unreachable!(),
        }
    }

    /// Apply the existing snapshot/handle retention policy without touching
    /// source directories or immutable objects. Also run on unchanged polls so
    /// a completed long request does not retain an old view until another update.
    pub(crate) async fn collect_memory(&self) -> Result<(), String> {
        let operation = self.0.operations.clone().lock_owned().await;
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            let _operation = operation;
            let removed = retain_snapshots(&mut service.0.retired.write().unwrap());
            drop(removed);
        })
        .await
        .map_err(|e| format!("snapshot collection task failed: {e}"))
    }

    /// A negative exact-anchor attestation revokes that revision, not a newer
    /// canonical replacement whose range overlaps it. Use the same owner fence,
    /// cancellation and durable digest set as range invalidation.
    pub(crate) async fn revoke_exact(&self, digests: BTreeSet<String>) -> Result<(), String> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            let _gate = service.0.publication_gate.lock().unwrap();
            let active = service.0.active.read().unwrap();
            let retired = service.0.retired.read().unwrap();
            let mut invalid = service.0.invalid.write().unwrap();
            if digests.is_subset(&invalid) {
                return Ok(());
            }
            invalid.extend(digests);
            service
                .0
                .epoch
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            // Remain fenced in memory even on disk failure. Manager intent was
            // persisted first and will replay this union before restart serving.
            active.state.release_invalidated(&invalid);
            for old in retired.iter() {
                old.state.release_invalidated(&invalid);
            }
            persist(&service.0.record.with_extension("invalid.json"), &*invalid)
        })
        .await
        .map_err(|e| e.to_string())?
    }
    /// Replay manager-owned intent into an unexposed startup owner. Do not
    /// cancel the newly verified view: a newer checked recovery may clear the
    /// fence before its initial activation. Dispatch still consults this set.
    pub(crate) fn fence_recovered(&self, digests: &BTreeSet<String>) -> Result<(), String> {
        let _gate = self.0.publication_gate.lock().unwrap();
        let mut invalid = self.0.invalid.write().unwrap();
        invalid.extend(digests.iter().cloned());
        persist(&self.0.record.with_extension("invalid.json"), &*invalid)
    }
    /// Called only inside the manager's fresh checked activation. Clearing a
    /// durable digest fence never revives an invalidated in-memory snapshot;
    /// recovery prepares a new view with its own pins and cancellation state.
    pub(crate) fn restore_verified(&self, keep: &BTreeSet<String>) -> Result<(), String> {
        let _gate = self.0.publication_gate.lock().unwrap();
        let mut invalid = self.0.invalid.write().unwrap();
        let next: BTreeSet<_> = invalid.difference(keep).cloned().collect();
        persist(&self.0.record.with_extension("invalid.json"), &next)?;
        *invalid = next;
        Ok(())
    }
    /// The publication owner holds command serialization while obtaining a
    /// fresh authority decision, immediately before the durable predecessor CAS.
    /// The manager's check also applies newly observed revocations; persisted
    /// files or a saved proof cannot call this boundary on restart.
    pub(crate) async fn activate_checked<F, Fut>(
        &self,
        expected: String,
        map_sha256: String,
        initial: bool,
        check: F,
    ) -> Result<(), String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let operation = self.0.operations.clone().lock_owned().await;
        if !initial {
            self.qualify_candidate_retirement().await?;
        }
        check().await?;
        if initial {
            let _gate = self.0.publication_gate.lock().unwrap();
            let active = self.0.active.read().unwrap();
            if active.publication.map_sha256 != map_sha256
                || !active.state.is_ready()
                || active.state.is_invalidated()
                || active
                    .state
                    .set()
                    .map
                    .shards
                    .iter()
                    .any(|s| self.0.invalid.read().unwrap().contains(&s.manifest_digest))
            {
                return Err("initial publication is not eligible/warm".into());
            }
            persist(&self.0.record, &active.publication)?;
            Ok(())
        } else {
            self.commit_activation(expected, map_sha256, operation)
                .await
                .map(|_| ())
        }
    }
    async fn activate(
        &self,
        expected: String,
        map_sha256: String,
        _operation: tokio::sync::OwnedMutexGuard<()>,
    ) -> Result<serde_json::Value, String> {
        self.qualify_candidate_retirement().await?;
        self.commit_activation(expected, map_sha256, _operation)
            .await
    }
    async fn qualify_candidate_retirement(&self) -> Result<(), String> {
        let old = self.0.active.read().unwrap().state.clone();
        let next = self
            .0
            .candidate
            .lock()
            .await
            .as_ref()
            .map(|c| c.state.clone());
        if self.0.required_runtimes || old.archive().is_some() {
            if let Some(next) = next {
                next.qualify_retirement(&old).await?;
            }
        }
        Ok(())
    }
    async fn commit_activation(
        &self,
        expected: String,
        map_sha256: String,
        _operation: tokio::sync::OwnedMutexGuard<()>,
    ) -> Result<serde_json::Value, String> {
        crate::transparent::activation::activate(
            expected,
            map_sha256.clone(),
            HistoryActivation(self.0.clone()),
            _operation,
        )
        .await?;
        Ok(serde_json::json!({"active":map_sha256}))
    }
}

// One policy for explicit disk collection and manager-only memory collection.
// Return removed views so their destruction happens outside the serving lock.
fn retain_snapshots(retired: &mut Vec<Active>) -> Vec<Active> {
    let recent_from = retired.len().saturating_sub(3);
    let mut removed = Vec::new();
    for (index, old) in std::mem::take(retired).into_iter().enumerate() {
        if index >= recent_from || old.state.has_other_holders() {
            retired.push(old);
        } else {
            removed.push(old);
        }
    }
    removed
}

async fn dispatch(State(live): State<LiveService>, request: Request) -> Response {
    let mut state = live.0.active.read().unwrap().state.clone();
    let path = request.uri().path().to_owned();
    let digest = path
        .split('/')
        .collect::<Vec<_>>()
        .windows(2)
        .find(|w| w[0] == "revisions")
        .map(|w| w[1].to_owned());
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
    let invalid = || {
        if state.is_invalidated() {
            return true;
        }
        let invalid = live.0.invalid.read().unwrap();
        match &digest {
            Some(digest) => invalid.contains(digest),
            None => {
                state
                    .set()
                    .map
                    .shards
                    .iter()
                    .any(|s| invalid.contains(&s.manifest_digest))
                    && path != "/metrics"
            }
        }
    };
    let refused = || {
        (if digest.is_some() {StatusCode::CONFLICT} else {StatusCode::SERVICE_UNAVAILABLE}, axum::Json(serde_json::json!({"error":"publication invalidated by reorg", "retry":"refresh the shard map", "map_sha256":live.0.active.read().unwrap().publication.map_sha256}))).into_response()
    };
    if invalid() {
        return refused();
    }
    let response = service::router(state.clone())
        .oneshot(request)
        .await
        .unwrap();
    // A reorg during evaluation must not release an orphaned answer.
    if invalid() {
        return refused();
    }
    response
}

/// Collect unused publications and then revisions no remaining publication
/// references, including private HTTPS staging trees. Staging uses the same
/// file lock while linking catalog sources. Defer
/// rather than blocking control commands behind a slow transfer.
fn collect_publications(root: &Path, keep: &BTreeSet<PathBuf>) -> std::io::Result<bool> {
    use std::fs::{self, OpenOptions, TryLockError};
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join(".sources.lock"))?;
    match lock.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Ok(false),
        Err(TryLockError::Error(error)) => return Err(error),
    }
    let digest = |name: &std::ffi::OsStr| {
        name.to_str().is_some_and(|s| {
            s.len() == 64
                && s.bytes()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        })
    };
    // A recovered record may name its tree through another spelling of the
    // same directory; compare resolved paths so it is never mistaken as unused.
    let keep: BTreeSet<PathBuf> = keep
        .iter()
        .flat_map(|path| std::iter::once(path.clone()).chain(fs::canonicalize(path).ok()))
        .collect();
    let kept = |path: &Path| {
        keep.contains(path) || fs::canonicalize(path).is_ok_and(|path| keep.contains(&path))
    };
    let mut publications = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let staged = name
            .to_str()
            .and_then(|name| name.strip_prefix("staging-"))
            .and_then(|name| name.split_once('-'))
            .is_some_and(|(revision, time)| {
                revision.parse::<u64>().is_ok() && time.parse::<u128>().is_ok()
            });
        if (digest(&name) || staged) && entry.file_type()?.is_dir() {
            publications.push(entry);
        }
    }
    publications.sort_by_key(|e| std::cmp::Reverse(e.metadata().and_then(|m| m.modified()).ok()));
    let mut unused = 0;
    let mut referenced = BTreeSet::new();
    for publication in publications {
        if !kept(&publication.path()) {
            unused += 1;
            if unused > 3 {
                fs::remove_dir_all(publication.path())?;
                continue;
            }
        }
        for revision in fs::read_dir(publication.path())? {
            let revision = revision?;
            // A malformed or partially staged link cannot make a referenced
            // revision disappear. Loading it still requires full validation.
            if digest(&revision.file_name()) {
                referenced.insert(revision.file_name());
            }
        }
    }
    let catalog = root.join(".sources");
    if fs::symlink_metadata(&catalog).is_ok_and(|m| m.is_dir()) {
        for entry in fs::read_dir(catalog)? {
            let entry = entry?;
            if digest(&entry.file_name())
                && entry.file_type()?.is_dir()
                && !referenced.contains(&entry.file_name())
            {
                fs::remove_dir_all(entry.path())?;
            }
        }
    }
    Ok(true)
}

#[cfg(test)]
mod collection_tests {
    use super::*;
    #[test]
    fn collection_keeps_https_staging_sources() {
        let root = tempfile::tempdir().unwrap();
        let catalog = root.path().join(".sources");
        std::fs::create_dir(&catalog).unwrap();
        let mut keep = BTreeSet::new();
        // Active, candidate and retained publications all use HTTPS names.
        for n in 0..8 {
            let revision = format!("{n:064x}");
            std::fs::create_dir(catalog.join(&revision)).unwrap();
            std::fs::write(catalog.join(&revision).join("rows.bin"), b"checked rows").unwrap();
            let publication = root.path().join(format!("staging-{n}-100"));
            std::fs::create_dir(&publication).unwrap();
            std::os::unix::fs::symlink(
                format!("../.sources/{revision}"),
                publication.join(&revision),
            )
            .unwrap();
            if n < 3 {
                keep.insert(publication);
            }
        }
        let orphan = catalog.join("f".repeat(64));
        std::fs::create_dir(&orphan).unwrap();
        collect_publications(root.path(), &keep).unwrap();
        for (n, publication) in keep.iter().enumerate() {
            assert_eq!(
                std::fs::read(publication.join(format!("{n:064x}")).join("rows.bin")).unwrap(),
                b"checked rows"
            );
        }
        assert!(!orphan.exists());
        assert_eq!(
            std::fs::read_dir(root.path())
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().starts_with("staging-"))
                .count(),
            keep.len() + 3
        );
    }

    #[test]
    fn collection_keeps_a_tree_named_through_another_path() {
        let real = tempfile::tempdir().unwrap();
        let alias = tempfile::tempdir().unwrap();
        let spelled = alias.path().join("provider");
        std::os::unix::fs::symlink(real.path(), &spelled).unwrap();
        let epoch = std::time::SystemTime::UNIX_EPOCH;
        for n in 0..5u64 {
            let publication = real.path().join(format!("staging-{n}-100"));
            std::fs::create_dir(&publication).unwrap();
            std::fs::File::open(&publication)
                .unwrap()
                .set_modified(epoch + std::time::Duration::from_secs(1_000 + n))
                .unwrap();
        }
        // The oldest tree would be the first unused one removed.
        let keep = BTreeSet::from([spelled.join("staging-0-100")]);
        assert!(collect_publications(real.path(), &keep).unwrap());
        assert!(real.path().join("staging-0-100").is_dir());
        assert!(!real.path().join("staging-1-100").exists());
        for n in 2..5 {
            assert!(real.path().join(format!("staging-{n}-100")).is_dir());
        }
    }

    #[test]
    fn collection_keeps_referenced_sources_and_defers_during_staging() {
        let root = tempfile::tempdir().unwrap();
        let catalog = root.path().join(".sources");
        std::fs::create_dir(&catalog).unwrap();
        let revision = "a".repeat(64);
        let orphan = "b".repeat(64);
        std::fs::create_dir(catalog.join(&revision)).unwrap();
        std::fs::create_dir(catalog.join(&orphan)).unwrap();
        let mut keep = BTreeSet::new();
        for n in 0..8 {
            let publication = root.path().join(format!("{n:064x}"));
            std::fs::create_dir(&publication).unwrap();
            std::os::unix::fs::symlink(
                format!("../.sources/{revision}"),
                publication.join(&revision),
            )
            .unwrap();
            if n == 0 {
                keep.insert(publication);
            }
        }
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.path().join(".sources.lock"))
            .unwrap();
        lock.lock().unwrap();
        assert!(!collect_publications(root.path(), &keep).unwrap());
        assert!(catalog.join(&orphan).exists());
        drop(lock);
        assert!(collect_publications(root.path(), &keep).unwrap());
        assert!(catalog.join(&revision).exists());
        assert!(!catalog.join(&orphan).exists());
        assert!(keep.iter().next().unwrap().exists());
        assert_eq!(
            std::fs::read_dir(root.path())
                .unwrap()
                .filter_map(Result::ok)
                .filter(|e| e.file_name().to_str().is_some_and(|s| s.len() == 64))
                .count(),
            4
        );
        for entry in std::fs::read_dir(root.path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name().to_str().is_some_and(|s| s.len() == 64) {
                std::fs::remove_dir_all(entry.path()).unwrap();
            }
        }
        collect_publications(root.path(), &BTreeSet::new()).unwrap();
        assert!(!catalog.join(&revision).exists());
    }
}
