//! Publisher identities are checked once, after activation, without queueing
//! query work behind the checker. A failure belongs to the dependent shards.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::File,
    io::{self, Read},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc, Condvar, Mutex, OnceLock, Weak,
    },
    thread::JoinHandle,
    time::Duration,
};
use transparent_shared::{
    publication::{self, ReceivedArtifact},
    runtime::{RuntimeCache, RuntimeKey},
};

const PENDING: u8 = 0;
const PASSED: u8 = 1;
const FAILED: u8 = 2;
const REPAIRING: u8 = 3;
const CHUNK: usize = 1 << 20;
// Serializes catalog links with replacement installation, never with hashing.
// Query admission and query execution never take this lock.
pub(crate) static CATALOG: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Stamp {
    device: u64,
    inode: u64,
    bytes: u64,
    modified: (i64, i64),
}
impl Stamp {
    fn of(file: &File) -> io::Result<Self> {
        let m = file.metadata()?;
        if !m.is_file() {
            return Err(io::Error::other("artifact is not a regular file"));
        }
        Ok(Self {
            device: m.dev(),
            inode: m.ino(),
            bytes: m.len(),
            modified: (m.mtime(), m.mtime_nsec()),
        })
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    sha256: String,
    bytes: u64,
    stamp: Stamp,
    passed: bool,
}
fn record_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap().to_os_string();
    name.push(".validation.json");
    path.with_file_name(name)
}

/// Called once after the consumer's exclusive directory lock is acquired,
/// before recovery creates any validation jobs. Never follow directory links.
pub(crate) fn cleanup_repairs(root: &Path) -> io::Result<()> {
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if kind.is_dir() {
                directories.push(entry.path());
            } else if kind.is_file()
                && (name.contains(".validation.repair-") || name.starts_with(".validation-link-"))
            {
                std::fs::remove_file(entry.path())?;
            }
        }
    }
    Ok(())
}
fn open(path: &Path) -> io::Result<File> {
    if !std::fs::symlink_metadata(path)?.is_file() {
        return Err(io::Error::other("artifact is not a regular file"));
    }
    File::open(path)
}

struct Job {
    expected: ReceivedArtifact,
    primary: PathBuf,
    stamp: Mutex<Stamp>,
    aliases: Mutex<BTreeSet<PathBuf>>,
    caches: Mutex<Vec<(Weak<RuntimeCache>, RuntimeKey)>>,
    phase: AtomicU8,
    enabled: AtomicBool,
}
impl Job {
    fn withdrawn(&self) -> bool {
        self.phase.load(Ordering::Acquire) >= FAILED
    }
    fn save(&self, passed: bool) -> io::Result<()> {
        let record = Record {
            version: 1,
            sha256: self.expected.sha256.clone(),
            bytes: self.expected.bytes,
            stamp: self.stamp.lock().unwrap().clone(),
            passed,
        };
        crate::transparent::durable::persist(&record_path(&self.primary), &record)
            .map_err(io::Error::other)
    }
    fn fail(&self, error: &io::Error) {
        // Withdraw in memory even if persisting the failure also fails.
        self.phase.store(REPAIRING, Ordering::Release);
        if let Err(persist) = self.save(false) {
            tracing::error!(%persist, path = %self.primary.display(), "cannot persist artifact withdrawal");
        }
        self.phase.store(FAILED, Ordering::Release);
        tracing::error!(%error, sha256 = %self.expected.sha256, path = %self.primary.display(),
            "artifact validation failed; dependent shards withdrawn");
    }
}
struct Work {
    job: Weak<Job>,
    replacement: Option<PathBuf>,
}
#[derive(Default)]
struct State {
    queries: usize,
    stopped: bool,
    jobs: BTreeMap<String, Weak<Job>>,
    queue: VecDeque<Work>,
}
#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
}
struct Core {
    shared: Arc<Shared>,
    thread: Mutex<Option<JoinHandle<()>>>,
}
impl Drop for Core {
    fn drop(&mut self) {
        {
            let mut state = self.shared.state.lock().unwrap();
            state.stopped = true;
            for work in state.queue.drain(..) {
                if let Some(path) = work.replacement {
                    let _ = std::fs::remove_file(path);
                }
            }
        }
        self.shared.wake.notify_all();
        if let Some(thread) = self.thread.lock().unwrap().take() {
            let _ = thread.join();
        }
    }
}
fn registry() -> &'static Mutex<Weak<Core>> {
    static CORE: OnceLock<Mutex<Weak<Core>>> = OnceLock::new();
    CORE.get_or_init(Default::default)
}
fn core() -> Arc<Core> {
    let mut saved = registry().lock().unwrap();
    if let Some(core) = saved.upgrade() {
        return core;
    }
    let core = new_core();
    *saved = Arc::downgrade(&core);
    core
}

fn new_core() -> Arc<Core> {
    let shared = Arc::new(Shared::default());
    let worker = shared.clone();
    let thread = std::thread::Builder::new()
        .name("artifact-validation".into())
        .spawn(move || run(worker))
        .expect("start artifact validation thread");
    Arc::new(Core {
        shared,
        thread: Mutex::new(Some(thread)),
    })
}

/// Called under CATALOG while staging immutable links. Keep repairs from
/// missing an alias created by another publication before its scope is built.
pub(crate) fn track_link(object: &Path, alias: &Path) -> io::Result<()> {
    let Some(core) = registry().lock().unwrap().upgrade() else {
        return Ok(());
    };
    let original = Stamp::of(&open(object)?)?;
    let linked = Stamp::of(&open(alias)?)?;
    if (original.device, original.inode) != (linked.device, linked.inode) {
        return Ok(());
    }
    let state = core.shared.state.lock().unwrap();
    for job in state
        .jobs
        .values()
        .filter_map(Weak::upgrade)
        .filter(|j| j.primary == object)
    {
        job.aliases.lock().unwrap().insert(alias.to_path_buf());
    }
    Ok(())
}

/// Kept through request admission, waiting and evaluation, including cancellation
/// of an HTTP waiter while its admitted blocking operation is still running.
pub(crate) struct QueryActivity(Arc<Core>);
pub(crate) fn query_activity() -> QueryActivity {
    activity(core())
}
fn activity(core: Arc<Core>) -> QueryActivity {
    core.shared.state.lock().unwrap().queries += 1;
    QueryActivity(core)
}
impl Drop for QueryActivity {
    fn drop(&mut self) {
        let mut state = self.0.shared.state.lock().unwrap();
        state.queries -= 1;
        if state.queries == 0 {
            self.0.shared.wake.notify_all();
        }
    }
}
fn idle(shared: &Shared) -> io::Result<()> {
    let mut state = shared.state.lock().unwrap();
    while state.queries != 0 && !state.stopped {
        state = shared.wake.wait(state).unwrap();
    }
    if state.stopped {
        return Err(io::Error::other("validation stopped"));
    }
    Ok(())
}

/// Owned by a serving snapshot: dropping it removes unneeded queued checks.
pub(crate) struct Scope {
    core: Arc<Core>,
    activated: OnceLock<()>,
    current: BTreeMap<u64, String>,
    revisions: BTreeMap<String, (u64, Vec<Arc<Job>>)>,
    errors: BTreeMap<String, String>,
}
impl Default for Scope {
    fn default() -> Self {
        Self {
            core: core(),
            activated: OnceLock::new(),
            current: BTreeMap::new(),
            revisions: BTreeMap::new(),
            errors: BTreeMap::new(),
        }
    }
}
impl Scope {
    pub(crate) fn current(&mut self, shard: u64, revision: &str) {
        self.current.insert(shard, revision.to_string());
    }
    pub(crate) fn add(
        &mut self,
        revision: &str,
        shard: u64,
        path: &Path,
        cache: &Arc<RuntimeCache>,
        key: &RuntimeKey,
    ) {
        self.revisions
            .entry(revision.into())
            .or_insert((shard, Vec::new()));
        match self.register(path, cache, key) {
            Ok(Some(job)) => self.revisions.get_mut(revision).unwrap().1.push(job),
            Ok(None) => {} // Locally prepared inputs keep their existing policy.
            Err(error) => {
                self.errors.insert(revision.into(), error.to_string());
            }
        }
    }
    fn register(
        &self,
        path: &Path,
        cache: &Arc<RuntimeCache>,
        runtime: &RuntimeKey,
    ) -> io::Result<Option<Arc<Job>>> {
        let _catalog = CATALOG.lock().unwrap();
        let Some(expected) = publication::received_artifact(path)? else {
            return Ok(None);
        };
        let file = open(path)?;
        let stamp = Stamp::of(&file)?;
        if stamp.bytes != expected.bytes {
            return Err(io::Error::other("received artifact length mismatch"));
        }
        // Copies cannot borrow the original inode's successful validation.
        let primary = match open(&expected.object).and_then(|f| Stamp::of(&f)) {
            Ok(other) if (other.device, other.inode) == (stamp.device, stamp.inode) => {
                expected.object.clone()
            }
            _ => path.to_path_buf(),
        };
        let identity = format!(
            "{}:{}:{}:{}",
            stamp.device, stamp.inode, expected.sha256, expected.bytes
        );
        let mut state = self.core.shared.state.lock().unwrap();
        state.jobs.retain(|_, weak| weak.strong_count() != 0);
        let job = match state.jobs.get(&identity).and_then(Weak::upgrade) {
            Some(job) if *job.stamp.lock().unwrap() == stamp => job,
            _ => {
                // A copy cannot escape the original object's persisted
                // withdrawal, even though successful checks remain inode-local.
                let original_failed = primary != expected.object
                    && File::open(record_path(&expected.object))
                        .ok()
                        .and_then(|input| {
                            serde_json::from_reader::<_, Record>(input.take(8193)).ok()
                        })
                        .is_some_and(|r| {
                            r.version == 1
                                && r.sha256 == expected.sha256
                                && r.bytes == expected.bytes
                                && !r.passed
                        });
                let phase = if original_failed {
                    FAILED
                } else {
                    match File::open(record_path(&primary)) {
                        Ok(input) => {
                            let record: Record = serde_json::from_reader(input.take(8193))?;
                            if record.version == 1
                                && record.sha256 == expected.sha256
                                && record.bytes == expected.bytes
                            {
                                if !record.passed {
                                    FAILED
                                } else if record.stamp == stamp {
                                    PASSED
                                } else {
                                    PENDING
                                }
                            } else {
                                PENDING
                            }
                        }
                        Err(error) if error.kind() == io::ErrorKind::NotFound => PENDING,
                        Err(error) => return Err(error),
                    }
                };
                let job = Arc::new(Job {
                    expected,
                    primary,
                    stamp: Mutex::new(stamp),
                    aliases: Mutex::new(BTreeSet::new()),
                    caches: Mutex::new(Vec::new()),
                    phase: AtomicU8::new(phase),
                    enabled: AtomicBool::new(false),
                });
                state.jobs.insert(identity, Arc::downgrade(&job));
                job
            }
        };
        job.aliases.lock().unwrap().insert(path.to_path_buf());
        let mut caches = job.caches.lock().unwrap();
        if !caches
            .iter()
            .any(|(c, k)| c.ptr_eq(&Arc::downgrade(cache)) && k == runtime)
        {
            caches.push((Arc::downgrade(cache), runtime.clone()));
        }
        drop(caches);
        Ok(Some(job))
    }
    fn activate(&self) {
        self.activated.get_or_init(|| {
            let mut state = self.core.shared.state.lock().unwrap();
            for (_, jobs) in self.revisions.values() {
                for job in jobs {
                    if !job.enabled.swap(true, Ordering::AcqRel)
                        && job.phase.load(Ordering::Acquire) == PENDING
                    {
                        state.queue.push_back(Work {
                            job: Arc::downgrade(job),
                            replacement: None,
                        });
                    }
                }
            }
            self.core.shared.wake.notify_all();
        });
    }
    pub(crate) fn unavailable(&self, revision: &str) -> bool {
        self.errors.contains_key(revision)
            || self
                .revisions
                .get(revision)
                .is_some_and(|(_, jobs)| jobs.iter().any(|j| j.withdrawn()))
    }
    /// Whether some current revision still serves: false only when this
    /// snapshot has current revisions and background validation withdrew
    /// every one. Retained predecessors do not count. Provider `/v1/ready`
    /// and the host's typed readiness both use this.
    pub(crate) fn serves_current(&self) -> bool {
        self.current.is_empty() || !self.current.values().all(|r| self.unavailable(r))
    }
    pub(crate) fn status(&self) -> serde_json::Value {
        let mut jobs = BTreeMap::new();
        let mut failed = Vec::new();
        for (revision, (shard, refs)) in &self.revisions {
            if self.unavailable(revision) {
                failed.push(serde_json::json!({"shard_id":shard,"revision":revision}));
            }
            for job in refs {
                jobs.insert(Arc::as_ptr(job) as usize, job.phase.load(Ordering::Acquire));
            }
        }
        serde_json::json!({"pending":jobs.values().filter(|&&p| p == PENDING).count(),
            "validated":jobs.values().filter(|&&p| p == PASSED).count(),
            "failed":jobs.values().filter(|&&p| p >= FAILED).count(), "withdrawn_shards":failed})
    }
    pub(crate) fn metrics(&self) -> String {
        let status = self.status();
        ["pending", "validated", "failed"]
            .iter()
            .map(|name| {
                format!(
                    "transparent_artifact_validation_{} {}\n",
                    name, status[name]
                )
            })
            .collect()
    }
    pub(crate) async fn serve(
        &self,
        request: axum::extract::Request,
        next: axum::middleware::Next,
    ) -> axum::response::Response {
        let parts: Vec<_> = request.uri().path().split('/').collect();
        let target = parts
            .windows(4)
            .find(|p| p[0] == "shards" && p[2] == "revisions")
            .map(|p| p[3].to_string());
        let shard = parts
            .windows(2)
            .find(|p| p[0] == "shards")
            .and_then(|p| p[1].parse::<u64>().ok());
        let affected = || {
            target.as_ref().map_or_else(
                || {
                    shard.is_some_and(|id| {
                        self.current.get(&id).is_some_and(|r| self.unavailable(r))
                    })
                },
                |r| self.unavailable(r),
            )
        };
        let ready = request.uri().path() == "/v1/ready";
        let _activity = shard.map(|_| activity(self.core.clone()));
        // The router is now handling a request: never start validation from
        // construction/preparation, which can precede listener activation.
        self.activate();
        if affected() || (ready && !self.serves_current()) {
            return unavailable();
        }
        let response = next.run(request).await;
        if affected() {
            unavailable()
        } else {
            response
        }
    }
}
fn unavailable() -> axum::response::Response {
    crate::transparent::request::RequestError::Busy(
        "shard artifact validation failed; repair pending",
    )
    .into_response("")
}

fn run(shared: Arc<Shared>) {
    loop {
        let work = {
            let mut state = shared.state.lock().unwrap();
            while state.queue.is_empty() && !state.stopped {
                state = shared.wake.wait(state).unwrap();
            }
            if state.stopped {
                return;
            }
            state.queue.pop_front().unwrap()
        };
        let Some(job) = work.job.upgrade() else {
            if let Some(path) = work.replacement {
                let _ = std::fs::remove_file(path);
            }
            continue;
        };
        let result = check(&shared, &job, work.replacement.as_deref());
        if let Err(error) = result {
            if !shared.state.lock().unwrap().stopped && error.kind() != io::ErrorKind::Interrupted {
                job.fail(&error);
            }
        }
        if let Some(path) = work.replacement {
            let _ = std::fs::remove_file(path);
        }
    }
}
fn check(shared: &Shared, job: &Arc<Job>, replacement: Option<&Path>) -> io::Result<()> {
    idle(shared)?;
    let path = replacement.unwrap_or(&job.primary);
    let mut file = open(path)?;
    let before = Stamp::of(&file)?;
    if replacement.is_none() && before != *job.stamp.lock().unwrap() {
        return Err(io::Error::other("artifact replaced before validation"));
    }
    let mut hash = Sha256::new();
    let mut buffer = vec![0; CHUNK];
    let mut bytes = 0u64;
    loop {
        idle(shared)?;
        if Arc::strong_count(job) == 1 {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "publication no longer needs artifact",
            ));
        }
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        bytes = bytes
            .checked_add(n as u64)
            .ok_or_else(|| io::Error::other("artifact length overflow"))?;
        if bytes > job.expected.bytes {
            return Err(io::Error::other("artifact exceeds declared length"));
        }
        hash.update(&buffer[..n]);
    }
    let actual = hex::encode(hash.finalize());
    if bytes != job.expected.bytes || actual != job.expected.sha256 {
        return Err(io::Error::other(format!(
            "artifact digest or length mismatch: got {actual}, {bytes} bytes"
        )));
    }
    if before != Stamp::of(&file)? || before != Stamp::of(&open(path)?)? {
        return Err(io::Error::other("artifact changed during validation"));
    }
    if replacement.is_some() {
        let _catalog = loop {
            idle(shared)?;
            // Do not hold the catalog while waiting for query priority or a
            // pinned cache entry. Unrelated publications must keep progressing.
            let catalog = match CATALOG.try_lock() {
                Ok(catalog) => Some(catalog),
                Err(std::sync::TryLockError::WouldBlock) => None,
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(io::Error::other("artifact catalog lock poisoned"));
                }
            };
            let mut cleared = true;
            if catalog.is_some() {
                for (cache, key) in job.caches.lock().unwrap().iter() {
                    if let Some(cache) = cache.upgrade() {
                        cleared &= cache.evict_keys(std::slice::from_ref(key))?;
                    }
                }
            }
            if cleared {
                if let Some(catalog) = catalog {
                    break catalog;
                }
            }
            drop(catalog);
            let state = shared.state.lock().unwrap();
            let _ = shared
                .wake
                .wait_timeout(state, Duration::from_millis(50))
                .unwrap();
        };
        let mut aliases = job.aliases.lock().unwrap().clone();
        aliases.insert(job.primary.clone());
        for alias in aliases {
            // Retired publication paths can disappear while their immutable
            // descriptor remains held; never recreate collected directories.
            if !alias.exists() && alias != job.primary {
                continue;
            }
            let temp = alias.with_file_name(format!(".validation-link-{}", std::process::id()));
            match std::fs::hard_link(path, &temp) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::Unsupported => {
                    std::fs::copy(path, &temp)?;
                    open(&temp)?.sync_all()?;
                }
                Err(e) => return Err(e),
            }
            std::fs::rename(&temp, &alias)?;
            let artifact = publication::Artifact {
                path: String::new(),
                sha256: job.expected.sha256.clone(),
                bytes: job.expected.bytes,
                kind: publication::Kind::RecentRuntime,
            };
            publication::record_received_file(&alias, &artifact, &job.expected.object)
                .map_err(io::Error::other)?;
            File::open(alias.parent().unwrap())?.sync_all()?;
        }
        *job.stamp.lock().unwrap() = Stamp::of(&open(&job.primary)?)?;
    }
    job.save(true)?;
    job.phase.store(PASSED, Ordering::Release);
    tracing::info!(sha256 = %job.expected.sha256, bytes, "artifact background validation passed");
    Ok(())
}

/// Run only after the synchronization pass has refreshed publisher authority.
/// Repairs use the same source, TLS policy and transfer budget as normal receipt.
pub(crate) async fn repair_failed(source: &mut crate::transparent::source::Source, root: &Path) {
    repair(&core(), source, root).await;
}

// A cancelled synchronization must leave the artifact retryable and remove its
// unfinished replacement. Ownership transfers to the checker only after enqueue.
struct IncomingRepair {
    job: Arc<Job>,
    path: Option<PathBuf>,
}
impl Drop for IncomingRepair {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(path);
            self.job.phase.store(FAILED, Ordering::Release);
        }
    }
}

async fn repair(core: &Arc<Core>, source: &mut crate::transparent::source::Source, root: &Path) {
    use tokio::io::AsyncWriteExt;
    let jobs: Vec<_> = {
        let state = core.shared.state.lock().unwrap();
        if state.queries != 0 {
            return;
        }
        state
            .jobs
            .values()
            .filter_map(Weak::upgrade)
            .filter(|job| {
                job.enabled.load(Ordering::Acquire)
                    && job.expected.object.parent() == Some(root.join("objects").as_path())
                    && job.phase.load(Ordering::Acquire) == FAILED
            })
            .collect()
    };
    for job in jobs {
        if job
            .phase
            .compare_exchange(FAILED, REPAIRING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            continue;
        }
        let partial = root.join("objects").join(format!(
            "{}.validation.repair-{}",
            job.expected.sha256,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut incoming = IncomingRepair {
            job: job.clone(),
            path: Some(partial.clone()),
        };
        let result = async {
            // Create synchronously so cancellation cannot race a pending open
            // that creates the file after the cleanup guard has removed it.
            let output = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&partial)?;
            let mut output = tokio::fs::File::from_std(output);
            let mut response = source
                .get(&format!("artifacts/{}", job.expected.sha256))
                .await?;
            let mut remaining = job.expected.bytes;
            while let Some(chunk) = response.chunk().await? {
                if core.shared.state.lock().unwrap().queries != 0 {
                    return Err::<(), publication::Error>("repair yielded to queries".into());
                }
                remaining = remaining
                    .checked_sub(chunk.len() as u64)
                    .ok_or("repair exceeds declared length")?;
                output.write_all(&chunk).await?;
            }
            if remaining != 0 {
                return Err("repair incomplete".into());
            }
            let mut permissions = output.metadata().await?.permissions();
            permissions.set_readonly(true);
            output.set_permissions(permissions).await?;
            output.sync_all().await?;
            Ok(())
        }
        .await;
        match result {
            Ok(()) => {
                core.shared.state.lock().unwrap().queue.push_back(Work {
                    job: Arc::downgrade(&job),
                    replacement: incoming.path.take(),
                });
                core.shared.wake.notify_all();
            }
            Err(error) => {
                tracing::warn!(%error, sha256 = %job.expected.sha256, "withdrawn artifact repair deferred");
            }
        }
    }
}

#[cfg(test)]
mod tests;
