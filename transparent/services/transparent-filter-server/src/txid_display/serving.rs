//! Driving display workers.
//!
//! Each worker speaks the display control protocol: one JSON line each way
//! over its control socket (`status`, `stage`, `prepare`, `activate`,
//! `invalidate`, `discard`, ...). A worker is reached either directly over
//! its socket, when it shares the controller's filesystem, or through a
//! command adapter that ships directories to its host and relays control:
//! one JSON request on the adapter's stdin, one reply on its stdout.
//!
//! Archive owners hold the sealed shards; recent replicas hold the recent
//! shard and serve the map. A map may only name archives its archive owners
//! already serve, so a publication activates archive owners first, and only
//! when the sealed set changed, and recent replicas after them.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

const CONTROL_TIMEOUT: Duration = Duration::from_secs(90);
const STAGE_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerRole {
    RecentReplica,
    ArchiveOwner,
}

impl WorkerRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RecentReplica => "recent-replica",
            Self::ArchiveOwner => "archive-owner",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Transport {
    /// The worker reads the controller's directories directly.
    Socket { socket: PathBuf },
    /// `<command> <config>` ships directories and relays control.
    Command { command: PathBuf, config: PathBuf },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    pub name: String,
    pub role: WorkerRole,
    pub transport: Transport,
}

/// `--workers FILE`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkersFile {
    pub workers: Vec<WorkerConfig>,
}

impl WorkersFile {
    pub fn load(path: &Path) -> Result<Self, super::BoxError> {
        let file: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        let mut names = BTreeSet::new();
        for worker in &file.workers {
            if !names.insert(&worker.name) {
                return Err(format!("worker {:?} is listed twice", worker.name).into());
            }
        }
        Ok(file)
    }
}

fn reply_ok(reply: Value) -> Result<Value, String> {
    if reply["ok"] == true {
        Ok(reply)
    } else {
        Err(reply["error"]
            .as_str()
            .map_or_else(|| format!("refused: {reply}"), str::to_string))
    }
}

async fn socket_call(socket: &Path, command: &Value) -> Result<Value, String> {
    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .map_err(|e| format!("connect {}: {e}", socket.display()))?;
    let (read, mut write) = stream.into_split();
    let mut line = serde_json::to_vec(command).map_err(|e| e.to_string())?;
    line.push(b'\n');
    write.write_all(&line).await.map_err(|e| e.to_string())?;
    let mut reply = String::new();
    tokio::io::BufReader::new(read)
        .read_line(&mut reply)
        .await
        .map_err(|e| e.to_string())?;
    serde_json::from_str(&reply).map_err(|e| format!("unreadable reply {reply:?}: {e}"))
}

/// One request to a command adapter, as `controller::fleet` does it.
async fn command_call(
    command: &Path,
    config: &Path,
    request: &Value,
    timeout: Duration,
) -> Result<Value, String> {
    use std::process::Stdio;
    let mut child = tokio::process::Command::new(command)
        .arg(config)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("spawn {}: {e}", command.display()))?;
    let mut stdin = child.stdin.take().ok_or("missing adapter stdin")?;
    stdin
        .write_all(&serde_json::to_vec(request).map_err(|e| e.to_string())?)
        .await
        .map_err(|e| e.to_string())?;
    drop(stdin);
    let output = tokio::time::timeout(timeout, child.wait_with_output())
        .await
        .map_err(|_| format!("adapter timed out after {timeout:?}"))?
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!("adapter failed: {}", output.status));
    }
    let body: Value = serde_json::from_slice(&output.stdout)
        .map_err(|e| format!("unreadable adapter reply: {e}"))?;
    reply_ok(body)
}

impl Transport {
    /// Sends one control command and returns the worker's successful reply.
    pub async fn control(
        &self,
        worker: &str,
        command: Value,
        timeout: Duration,
    ) -> Result<Value, String> {
        match self {
            Self::Socket { socket } => {
                let reply = tokio::time::timeout(timeout, socket_call(socket, &command))
                    .await
                    .map_err(|_| format!("control timed out after {timeout:?}"))??;
                reply_ok(reply)
            }
            Self::Command {
                command: exe,
                config,
            } => {
                let body = command_call(
                    exe,
                    config,
                    &json!({"operation": "control", "worker": worker, "command": command}),
                    timeout,
                )
                .await?;
                reply_ok(body["reply"].clone())
            }
        }
    }

    /// Makes `source` available to the worker; returns the worker-side path.
    pub async fn ship(
        &self,
        worker: &str,
        kind: &str,
        source: &Path,
        name: &str,
        link_dest: Option<&str>,
        timeout: Duration,
    ) -> Result<String, String> {
        match self {
            // The worker reads the controller's own paths.
            Self::Socket { .. } => Ok(source.to_string_lossy().into_owned()),
            Self::Command { command, config } => {
                let body = command_call(
                    command,
                    config,
                    &json!({"operation": "ship", "worker": worker, "kind": kind,
                        "source": source, "name": name, "link_dest": link_dest}),
                    timeout,
                )
                .await?;
                body["directory"]
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| format!("ship reply names no directory: {body}"))
            }
        }
    }

    pub fn is_socket(&self) -> bool {
        matches!(self, Self::Socket { .. })
    }
}

/// What the controller knows of one worker.
pub struct Worker {
    pub config: WorkerConfig,
    /// Whether the fields below reflect the worker; refreshed by `status`
    /// after any failed call.
    pub known: bool,
    /// The worker's active map, the `expected` of its next prepare.
    pub expected: String,
    /// The worker-side directory of that map.
    pub active_dir: Option<String>,
    /// Sealed digests the active map names, when the controller knows them.
    pub sealed: Option<Vec<String>>,
    pub staged: BTreeSet<String>,
    /// A recent replica's undelivered invalidation: the lowest height a
    /// rollback replaced since it last acknowledged one. It is delivered
    /// before the replica is offered anything newer, so it never withdraws a
    /// revision built after the rollback.
    pub invalidate_from: Option<u64>,
}

/// Per-stage milliseconds of one activation.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ActivationReport {
    pub ship_ms: u64,
    pub prepare_ms: u64,
    pub activate_ms: u64,
    pub workers: Vec<Value>,
}

/// One staged seal on one worker.
pub struct StageOutcome {
    pub worker: String,
    pub result: Result<Value, String>,
}

pub struct Fleet {
    pub workers: Vec<Worker>,
}

fn sealed_digests(map: &transparent_shard::display::DisplayMap) -> Vec<String> {
    map.shards
        .iter()
        .filter(|s| s.sealed)
        .map(|s| s.manifest_digest.clone())
        .collect()
}

fn ms(started: Instant) -> u64 {
    started.elapsed().as_millis() as u64
}

impl Fleet {
    pub fn new(configs: Vec<WorkerConfig>) -> Self {
        Self {
            workers: configs
                .into_iter()
                .map(|config| Worker {
                    config,
                    known: false,
                    expected: String::new(),
                    active_dir: None,
                    sealed: None,
                    staged: BTreeSet::new(),
                    invalidate_from: None,
                })
                .collect(),
        }
    }

    pub fn archive_owners(&self) -> impl Iterator<Item = &Worker> {
        self.workers
            .iter()
            .filter(|w| w.config.role == WorkerRole::ArchiveOwner)
    }

    /// Reads each unknown worker's state. `current` is the controller's active
    /// map, whose sealed set a worker serving it is known to hold.
    pub async fn refresh(
        &mut self,
        current: (&str, &transparent_shard::display::DisplayMap),
    ) -> Vec<(String, String)> {
        let mut errors = Vec::new();
        for worker in self.workers.iter_mut().filter(|w| !w.known) {
            if let Err(error) = refresh_worker(worker, current).await {
                errors.push((worker.config.name.clone(), error));
            }
        }
        errors
    }

    /// Whether any worker is behind `map`: a recent replica not serving it or
    /// owed an invalidation, or an archive owner not serving its sealed set.
    pub fn stale(&self, sha: &str, map: &transparent_shard::display::DisplayMap) -> bool {
        let sealed = sealed_digests(map);
        self.workers.iter().any(|w| match w.config.role {
            WorkerRole::RecentReplica => w.expected != sha || w.invalidate_from.is_some(),
            WorkerRole::ArchiveOwner => w.sealed.as_ref() != Some(&sealed),
        })
    }

    /// Publishes a candidate: archive owners whose sealed set differs first,
    /// then, when `recent`, every recent replica not already serving it. An
    /// archive failure leaves recent replicas untouched, and so does an
    /// invalidation a recent replica has not acknowledged yet
    /// ([`Fleet::invalidate`] goes first).
    pub async fn activate(
        &mut self,
        candidate: &Path,
        sha: &str,
        map: &transparent_shard::display::DisplayMap,
        recent: bool,
    ) -> Result<ActivationReport, String> {
        let sealed = sealed_digests(map);
        let mut report = ActivationReport::default();
        let roles: &[WorkerRole] = if recent {
            &[WorkerRole::ArchiveOwner, WorkerRole::RecentReplica]
        } else {
            &[WorkerRole::ArchiveOwner]
        };
        for &role in roles {
            for worker in self.workers.iter_mut().filter(|w| w.config.role == role) {
                if let Some(from) = worker.invalidate_from {
                    return Err(format!(
                        "{} {}: invalidation from {from} is not acknowledged",
                        role.as_str(),
                        worker.config.name
                    ));
                }
                let current = match role {
                    WorkerRole::ArchiveOwner => worker.sealed.as_ref() == Some(&sealed),
                    WorkerRole::RecentReplica => worker.expected == sha,
                };
                if current {
                    continue;
                }
                let result = activate_worker(worker, candidate, sha, &mut report).await;
                if let Err(error) = result {
                    worker.known = false;
                    return Err(format!("{} {}: {error}", role.as_str(), worker.config.name));
                }
                worker.sealed = Some(sealed.clone());
                worker.staged.retain(|d| !sealed.contains(d));
            }
        }
        Ok(report)
    }

    /// Owes every recent replica an invalidation from `from_height`, merged
    /// with any it still owes. Archive owners keep serving: nothing sealed can
    /// be affected.
    pub fn queue_invalidation(&mut self, from_height: u64) {
        for worker in self
            .workers
            .iter_mut()
            .filter(|w| w.config.role == WorkerRole::RecentReplica)
        {
            worker.invalidate_from = Some(
                worker
                    .invalidate_from
                    .map_or(from_height, |h| h.min(from_height)),
            );
        }
    }

    /// Undelivered invalidations by worker name, as `invalidate.json` keeps them.
    pub fn invalidations(&self) -> BTreeMap<String, u64> {
        self.workers
            .iter()
            .filter_map(|w| w.invalidate_from.map(|h| (w.config.name.clone(), h)))
            .collect()
    }

    /// Restores invalidations a previous run had not delivered.
    pub fn restore_invalidations(&mut self, pending: &BTreeMap<String, u64>) {
        for worker in self
            .workers
            .iter_mut()
            .filter(|w| w.config.role == WorkerRole::RecentReplica)
        {
            if let Some(from) = pending.get(&worker.config.name) {
                worker.invalidate_from = Some(*from);
            }
        }
    }

    /// Delivers every owed invalidation. A replica whose state is unknown is
    /// read first, so the call names the map it actually serves, whichever
    /// map that is; a failure keeps the invalidation owed.
    pub async fn invalidate(
        &mut self,
        current: (&str, &transparent_shard::display::DisplayMap),
    ) -> Vec<(String, String)> {
        let mut errors = Vec::new();
        for worker in self
            .workers
            .iter_mut()
            .filter(|w| w.invalidate_from.is_some())
        {
            let result = async {
                if !worker.known {
                    refresh_worker(worker, current).await?;
                }
                let from_height = worker.invalidate_from.expect("filtered");
                let command = json!({"operation": "invalidate", "expected": worker.expected,
                    "from_height": from_height});
                worker
                    .config
                    .transport
                    .control(&worker.config.name, command, CONTROL_TIMEOUT)
                    .await
            }
            .await;
            match result {
                Ok(_) => worker.invalidate_from = None,
                Err(error) => {
                    worker.known = false;
                    errors.push((worker.config.name.clone(), error));
                }
            }
        }
        errors
    }

    /// Worker-side directories the controller must keep: with a socket
    /// transport, a worker serves straight out of the controller's root.
    pub fn held_directories(&self) -> BTreeSet<PathBuf> {
        self.workers
            .iter()
            .filter(|w| w.config.transport.is_socket())
            .filter_map(|w| w.active_dir.as_ref().map(PathBuf::from))
            .collect()
    }

    /// Asks command-adapter workers to collect what they no longer serve.
    /// Socket workers share the controller's root, which it collects itself.
    pub async fn collect(&self) -> Vec<(String, String)> {
        let mut errors = Vec::new();
        for worker in self
            .workers
            .iter()
            .filter(|w| !w.config.transport.is_socket() && w.known)
        {
            if let Err(error) = worker
                .config
                .transport
                .control(
                    &worker.config.name,
                    json!({"operation": "collect"}),
                    CONTROL_TIMEOUT,
                )
                .await
            {
                errors.push((worker.config.name.clone(), error));
            }
        }
        errors
    }
}

async fn refresh_worker(
    worker: &mut Worker,
    (current_sha, current_map): (&str, &transparent_shard::display::DisplayMap),
) -> Result<(), String> {
    let reply = worker
        .config
        .transport
        .control(
            &worker.config.name,
            json!({"operation": "status"}),
            CONTROL_TIMEOUT,
        )
        .await?;
    if reply["role"] != worker.config.role.as_str() {
        return Err(format!(
            "worker reports role {}, configured {}",
            reply["role"],
            worker.config.role.as_str()
        ));
    }
    let active = &reply["active"];
    worker.expected = active["map_sha256"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    worker.active_dir = active["directory"].as_str().map(str::to_string);
    worker.sealed = (!worker.expected.is_empty() && worker.expected == current_sha)
        .then(|| sealed_digests(current_map));
    worker.staged = reply["staged"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|d| d.as_str().map(str::to_string))
        .collect();
    worker.known = true;
    Ok(())
}

async fn activate_worker(
    worker: &mut Worker,
    candidate: &Path,
    sha: &str,
    report: &mut ActivationReport,
) -> Result<(), String> {
    let name = worker.config.name.clone();
    let transport = worker.config.transport.clone();
    let started = Instant::now();
    let directory = transport
        .ship(
            &name,
            "candidate",
            candidate,
            sha,
            worker.active_dir.as_deref(),
            CONTROL_TIMEOUT,
        )
        .await?;
    let ship_ms = ms(started);
    let started = Instant::now();
    let prepared = transport
        .control(
            &name,
            json!({"operation": "prepare", "expected": worker.expected,
                "publication": {"directory": directory, "map_sha256": sha}}),
            CONTROL_TIMEOUT,
        )
        .await?;
    let prepare_ms = ms(started);
    let started = Instant::now();
    if let Err(error) = transport
        .control(
            &name,
            json!({"operation": "activate", "expected": worker.expected, "map_sha256": sha}),
            CONTROL_TIMEOUT,
        )
        .await
    {
        // Release the prepared candidate; the next cycle prepares afresh.
        let _ = transport
            .control(
                &name,
                json!({"operation": "discard", "map_sha256": sha}),
                CONTROL_TIMEOUT,
            )
            .await;
        return Err(error);
    }
    let activate_ms = ms(started);
    worker.expected = sha.to_string();
    worker.active_dir = Some(directory);
    report.ship_ms += ship_ms;
    report.prepare_ms += prepare_ms;
    report.activate_ms += activate_ms;
    report.workers.push(json!({
        "name": name, "role": worker.config.role.as_str(), "ship_ms": ship_ms,
        "prepare_ms": prepare_ms, "activate_ms": activate_ms,
        "built": prepared["built"], "reused": prepared["reused"],
        "seconds": prepared["seconds"],
    }));
    Ok(())
}

/// Ships a sealed revision to one archive owner and has it build and pin the
/// revision's runtimes before any map names it.
pub async fn stage(
    name: String,
    transport: Transport,
    revision: PathBuf,
    digest: String,
) -> StageOutcome {
    let result = async {
        let started = Instant::now();
        let directory = transport
            .ship(&name, "staged", &revision, &digest, None, STAGE_TIMEOUT)
            .await?;
        let reply = transport
            .control(
                &name,
                json!({"operation": "stage", "directory": directory}),
                STAGE_TIMEOUT,
            )
            .await?;
        if reply["digest"] != digest {
            return Err(format!("worker staged {} for {digest}", reply["digest"]));
        }
        Ok(json!({"directory": directory, "built": reply["built"],
            "seconds": reply["seconds"], "stage_ms": ms(started)}))
    }
    .await;
    StageOutcome {
        worker: name,
        result,
    }
}
