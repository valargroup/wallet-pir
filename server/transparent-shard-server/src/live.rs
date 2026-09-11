//! Private publication control and request-scoped, atomic serving snapshots.
//! The Unix socket is an operator interface, never routed through the public edge.
use crate::{
    assignment::Assignment,
    service::{self, ServiceConfig, ServiceState},
    shardset::{LoadOptions, LoadScope, ShardSet},
};
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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tower::ServiceExt;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Publication {
    pub directory: PathBuf,
    pub assignment: Option<PathBuf>,
    pub map_sha256: String,
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
struct PreparingGuard<'a>(&'a RwLock<Option<Preparing>>);
impl Drop for PreparingGuard<'_> {
    fn drop(&mut self) {
        *self.0.write().unwrap() = None;
    }
}
struct Inner {
    active: RwLock<Active>,
    invalid: RwLock<BTreeSet<String>>,
    candidate: tokio::sync::Mutex<Option<Candidate>>,
    preparing: RwLock<Option<Preparing>>,
    config: ServiceConfig,
    options: LoadOptions,
    record: PathBuf,
    retired: RwLock<Vec<Active>>,
    operations: Arc<tokio::sync::Mutex<()>>,
    publication_gate: std::sync::Mutex<()>,
    epoch: std::sync::atomic::AtomicU64,
}
#[derive(Clone)]
pub struct LiveService(Arc<Inner>);
impl LiveService {
    pub fn new(
        state: ServiceState,
        publication: Publication,
        config: ServiceConfig,
        options: LoadOptions,
        record: PathBuf,
    ) -> Result<Self, String> {
        let invalid: BTreeSet<String> = match std::fs::read(record.with_extension("invalid.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| format!("invalid revocation record: {e}"))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeSet::new(),
            Err(e) => return Err(e.to_string()),
        };
        state.release_invalidated(&invalid);
        Ok(Self(Arc::new(Inner {
            active: RwLock::new(Active { state, publication }),
            invalid: RwLock::new(invalid),
            candidate: tokio::sync::Mutex::new(None),
            preparing: RwLock::new(None),
            config,
            options,
            record,
            retired: RwLock::new(Vec::new()),
            operations: Arc::new(tokio::sync::Mutex::new(())),
            publication_gate: std::sync::Mutex::new(()),
            epoch: std::sync::atomic::AtomicU64::new(0),
        })))
    }
    pub fn router(&self) -> Router {
        Router::new().fallback(dispatch).with_state(self.clone())
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
            let candidate = self.0.candidate.try_lock().ok().and_then(|c| c.as_ref().map(|c| serde_json::json!({"map_sha256":c.publication.map_sha256,"age_seconds":c.prepared_at.elapsed().as_secs_f64(),"epoch":c.epoch,"warm":c.state.is_warm() && c.epoch == self.0.epoch.load(std::sync::atomic::Ordering::Acquire)})));
            let preparing = self.0.preparing.read().unwrap().as_ref().map(|p| serde_json::json!({"map_sha256":p.digest,"age_seconds":p.started.elapsed().as_secs_f64(),"phase":p.phase}));
            let active = self.0.active.read().unwrap();
            let retired = self.0.retired.read().unwrap();
            let revisions:std::collections::BTreeMap<_,_>=std::iter::once(&*active).chain(retired.iter()).flat_map(|a|a.state.set().revisions().iter())
                .map(|s|(s.digest.clone(),serde_json::json!({"digest":s.digest,"end_height":s.manifest.end_height,"terminal_block_hash":s.manifest.terminal_block_hash}))).collect();
            return Ok(
                serde_json::json!({"active":active.publication,"warm":active.state.is_warm(),"invalidated":active.state.is_invalidated(),"revoked_revisions":self.0.invalid.read().unwrap().len(),"preparing":preparing,"retired_snapshots":retired.len(),"candidate":candidate,"revisions":revisions.into_values().collect::<Vec<_>>()}),
            );
        }
        let _operation = self.0.operations.clone().lock_owned().await;
        match command {
            Command::Prepare {
                expected,
                publication,
            } => {
                let epoch = self.0.epoch.load(std::sync::atomic::Ordering::Acquire);
                let old = {
                    let active = self.0.active.read().unwrap();
                    if active.publication.map_sha256 == publication.map_sha256
                        && active.state.is_warm()
                    {
                        return Ok(serde_json::json!({"prepared":publication.map_sha256}));
                    }
                    if active.publication.map_sha256 != expected {
                        return Err("active predecessor changed".into());
                    }
                    active.state.clone()
                };
                if self.0.candidate.lock().await.as_ref().is_some_and(|c| {
                    c.publication.map_sha256 == publication.map_sha256
                        && c.state.is_warm()
                        && c.epoch == epoch
                }) {
                    return Ok(serde_json::json!({"prepared":publication.map_sha256}));
                }
                *self.0.preparing.write().unwrap() = Some(Preparing {
                    digest: publication.map_sha256.clone(),
                    started: std::time::Instant::now(),
                    phase: "loading",
                });
                let _preparing = PreparingGuard(&self.0.preparing);
                // Release an obsolete candidate's pins before reserving anew.
                *self.0.candidate.lock().await = None;
                let mut options = self.0.options.clone();
                match (&options.scope, &publication.assignment) {
                    (LoadScope::Assigned { worker_id, .. }, Some(path)) => {
                        options.scope = LoadScope::Assigned {
                            worker_id: worker_id.clone(),
                            assignment: Arc::new(
                                Assignment::load(path).map_err(|e| e.to_string())?,
                            ),
                        };
                    }
                    (LoadScope::Whole, None) => {}
                    _ => return Err("candidate must preserve assignment mode".into()),
                }
                let directory = publication.directory.clone();
                let digest = publication.map_sha256.clone();
                let config = self.0.config;
                let loading_started = std::time::Instant::now();
                let next = tokio::task::spawn_blocking(move || {
                    tracing::info!(map = %digest, queue_seconds = loading_started.elapsed().as_secs_f64(), "publication loading task started");
                    let eviction_started = std::time::Instant::now();
                    old.evict_unpinned();
                    tracing::info!(map = %digest, seconds = eviction_started.elapsed().as_secs_f64(), "publication runtime eviction finished");
                    let open_started = std::time::Instant::now();
                    let set = ShardSet::open_reusing(&directory, &options, Some(old.set()))
                        .map_err(|e| e.to_string())?;
                    tracing::info!(map = %digest, seconds = open_started.elapsed().as_secs_f64(), "publication shard loading finished");
                    if set.map_digest != digest {
                        return Err("candidate map digest mismatch".into());
                    }
                    if set.map.genesis_hash != old.set().map.genesis_hash
                        || set.map.start_height != old.set().map.start_height
                        || set.map.seal != old.set().map.seal
                    {
                        return Err("candidate changes publication identity".into());
                    }
                    let build_started = std::time::Instant::now();
                    let state = ServiceState::build_reusing(set, config, None, Some(&old));
                    tracing::info!(map = %digest, seconds = build_started.elapsed().as_secs_f64(), succeeded = state.is_ok(), "publication service construction finished");
                    state
                })
                .await
                .map_err(|e| e.to_string())??;
                let loading_seconds = loading_started.elapsed().as_secs_f64();
                let warming_started = std::time::Instant::now();
                self.0.preparing.write().unwrap().as_mut().unwrap().phase = "warming";
                next.spawn_prewarm().await.map_err(|e| e.to_string())?;
                let warming_seconds = warming_started.elapsed().as_secs_f64();
                tracing::info!(map = %publication.map_sha256, loading_seconds, warming_seconds, "publication preparation stages");
                if self.0.epoch.load(std::sync::atomic::Ordering::Acquire) != epoch {
                    return Err("reorg invalidated preparation".into());
                }
                if !next.is_warm() {
                    return Err(
                        "candidate did not become completely warm within the shared budget".into(),
                    );
                }
                *self.0.candidate.lock().await = Some(Candidate {
                    prepared_at: std::time::Instant::now(),
                    state: next,
                    publication: publication.clone(),
                    epoch,
                });
                Ok(
                    serde_json::json!({"prepared":publication.map_sha256,"loading_seconds":loading_seconds,"warming_seconds":warming_seconds}),
                )
            }
            Command::Activate {
                expected,
                map_sha256,
            } => {
                let service = self.clone();
                tokio::task::spawn_blocking(move || {
                    // Durable publication writes can block on disk. Keep them
                    // off the async executor and retain serialization even if
                    // the control caller disconnects while the job is running.
                    let _operation = _operation;
                    {
                        let active = service.0.active.read().unwrap();
                        if active.publication.map_sha256 == map_sha256 && active.state.is_warm() {
                            return Ok(serde_json::json!({"active":map_sha256}));
                        }
                        if active.publication.map_sha256 != expected {
                            return Err("active predecessor changed".into());
                        }
                    }
                    let mut candidate = service.0.candidate.blocking_lock();
                    // Invalidation must not land between the epoch check and the
                    // durable active pointer swap, including after a restart.
                    let _gate = service.0.publication_gate.lock().unwrap();
                    let next = candidate.as_ref().ok_or("no prepared candidate")?;
                    if next.epoch != service.0.epoch.load(std::sync::atomic::Ordering::Acquire) {
                        return Err("reorg invalidated prepared candidate".into());
                    }
                    if next.publication.map_sha256 != map_sha256 || !next.state.is_warm() {
                        return Err("candidate identity/readiness mismatch".into());
                    }
                    if next.state.set().map.shards.iter().any(|s| {
                        service
                            .0
                            .invalid
                            .read()
                            .unwrap()
                            .contains(&s.manifest_digest)
                    }) {
                        return Err("candidate contains invalidated history".into());
                    }
                    persist(&service.0.record, &next.publication)?;
                    let next = candidate.take().unwrap();
                    let old = std::mem::replace(
                        &mut *service.0.active.write().unwrap(),
                        Active {
                            state: next.state,
                            publication: next.publication,
                        },
                    );
                    // Normal predecessors remain available to in-flight wallets.
                    // Release their residency pins: only current and preparing
                    // assignments are residency obligations.
                    old.state.release_pins();
                    service.0.retired.write().unwrap().push(old);
                    Ok(serde_json::json!({"active":map_sha256}))
                })
                .await
                .map_err(|e| e.to_string())?
            }
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
                        .map(|s| s.digest.clone())
                        .collect();
                    let mut retired = service.0.retired.write().unwrap();
                    // A long request may hold one old snapshot without preventing
                    // collection of every other unused generation behind it.
                    let recent_from = retired.len().saturating_sub(3);
                    let mut removed = Vec::new();
                    for (index, old) in std::mem::take(&mut *retired).into_iter().enumerate() {
                        if index >= recent_from || old.state.has_other_holders() {
                            retired.push(old);
                        } else {
                            removed.push(old);
                        }
                    }
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
                            .map(|s| s.digest.clone()),
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
                    // Only controller-created, digest-named generations are ours.
                    // Keep the newest three unused directories across restarts too.
                    let mut unused: Vec<_> = std::fs::read_dir(root)
                        .map_err(|e| e.to_string())?
                        .filter_map(Result::ok)
                        .filter(|e| {
                            e.file_name().to_str().is_some_and(|s| {
                                s.len() == 64 && s.bytes().all(|c| c.is_ascii_hexdigit())
                            }) && e.file_type().is_ok_and(|t| t.is_dir())
                                && !keep.contains(&e.path())
                        })
                        .collect();
                    unused.sort_by_key(|e| {
                        std::cmp::Reverse(e.metadata().and_then(|m| m.modified()).ok())
                    });
                    for entry in unused.into_iter().skip(3) {
                        std::fs::remove_dir_all(entry.path()).map_err(|e| e.to_string())?;
                    }
                    let directory_seconds = directories_started.elapsed().as_secs_f64();
                    tracing::info!(snapshot_seconds, disk_seconds, directory_seconds, disk_freed_bytes, disk_collection_deferred, "publication collection stages");
                    Ok(serde_json::json!({"collected":true,"disk_freed_bytes":disk_freed_bytes,"disk_collection_deferred":disk_collection_deferred,"snapshot_seconds":snapshot_seconds,"disk_seconds":disk_seconds,"directory_seconds":directory_seconds}))
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
                    *candidate = None;
                }
                Ok(serde_json::json!({"discarded":map_sha256}))
            }
            _ => unreachable!(),
        }
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
                let body = match result {
                    Ok(value) => serde_json::json!({"ok":true,"result":value}),
                    Err(error) => serde_json::json!({"ok":false,"error":error}),
                };
                let _ = write.write_all(format!("{body}\n").as_bytes()).await;
            });
        }
    }
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
