//! Autonomous recent-worker synchronization. The private root has one owner;
//! completed bytes are reusable inputs, never saved serving authority.
use pir_publication::records::persist;
use zakura_pir_protocol::transparent::filter as transparent_filter;

use crate::transparent::{
    history::{
        live::{Command, LiveService, Publication},
        service::{ServiceConfig, ServiceState},
    },
    routing::bootstrap::{self, Config},
    source::Source,
};
use pir_pipeline::{
    consumer::{Consumer, ConsumerEngine, ConsumerPass, PeerHandover, PeerReservation},
    Input,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};
use transparent_shared::publication::{self, Current, Error, Inventory};
use transparent_shared::{
    history::{
        assignment::Assignment,
        shardset::{LoadOptions, LoadScope, ShardSet},
    },
    runtime::disk::ShippedRuntimes,
};

/// Transparent publication policy hosted by the shared synchronization loop.
pub struct Manager {
    sync: pir_pipeline::consumer::polling::SyncManager<ConsumerEngine<HistorySync>>,
}
impl Manager {
    /// Check publisher discovery, then open the provider root. An unreachable or
    /// incompatible publisher opens no state.
    pub async fn new(config: Config, service: ServiceConfig) -> Result<Self, Error> {
        crate::transparent::source::discover(&config).await?;
        Ok(Self {
            sync: pir_pipeline::consumer::polling::SyncManager::new(ConsumerEngine::new(
                HistorySync::new(config, service)?,
            )),
        })
    }
    /// Hold and verify every shard locally, including archive runtimes.
    pub fn with_full_holdings(mut self) -> Self {
        self.sync.task_mut().context_mut().full = true;
        self
    }
    pub fn with_archive(
        mut self,
        archive: std::sync::Arc<crate::transparent::routing::archive::Archive>,
    ) -> Self {
        self.sync.task_mut().context_mut().archive = Some(archive);
        self
    }
    pub fn live(&self) -> Option<LiveService> {
        self.sync.task().context().live()
    }
    pub fn router(&self) -> Result<axum::Router, Error> {
        self.sync.task().context().router()
    }
    pub async fn poll_once(&mut self) -> Result<Progress, Error> {
        self.sync.poll_once().await.map(ConsumerPass::into_report)
    }
    /// Synchronize until a publisher contract version mismatch, which is
    /// returned unchanged. Every other failure is retried.
    pub async fn run(self) -> Error {
        self.sync.run().await
    }
    /// [`Self::run`] until `stop`; a pass in progress finishes first.
    pub async fn run_until(self, stop: impl std::future::Future<Output = ()>) -> Option<Error> {
        self.sync.run_until(stop).await
    }
}

struct HistorySync {
    config: Config,
    full: bool,
    service: ServiceConfig,
    archive: Option<std::sync::Arc<crate::transparent::routing::archive::Archive>>,
    source: Source,
    _lock: std::sync::Arc<std::fs::File>,
    live: Option<LiveService>,
    /// Checked bootstrap data kept privately until fresh source checks pass.
    pending: Option<LiveService>,
    /// Only a successfully activated snapshot may supply a conditional token.
    current: Option<Current>,
    /// Exact-anchor negatives need their own revision floor: eligible Current
    /// alone does not encode them. Retain it before any fallible intent I/O.
    revocations: Revocations,
    recovery_attempted: bool,
    continuation: bool,
    observation_unchanged: bool,
    synchronization: std::sync::Arc<std::sync::RwLock<serde_json::Value>>,
}
#[derive(Debug, serde::Serialize)]
pub struct Progress {
    pub unchanged: bool,
    pub revision: u64,
    pub downloaded_bytes: u64,
    pub downloaded_files: usize,
    /// Wall seconds of each stage of a changed pass; absent when unchanged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stages: Option<Stages>,
}
/// Observed durations only: diagnostics for campaigns, never authority.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct Stages {
    /// Inventory request and downloaded artifact bodies.
    pub transfer_seconds: f64,
    /// Local copies, verification and syncing of the staging tree.
    pub staging_seconds: f64,
    /// Strict load of the staged set before its runtimes warm.
    pub loading_seconds: f64,
    pub prewarm_seconds: f64,
    /// Final source/anchor checks, the predecessor CAS and collection.
    pub activation_seconds: f64,
}
impl HistorySync {
    pub fn new(mut config: Config, service: ServiceConfig) -> Result<Self, Error> {
        let source = Source::new(&config)?;
        std::fs::create_dir_all(&config.directory)?;
        if !std::fs::symlink_metadata(&config.directory)?.is_dir() {
            return Err("provider root must be a real directory".into());
        }
        config.directory = std::fs::canonicalize(&config.directory)?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(config.directory.join("bootstrap.lock"))?;
        lock.try_lock()
            .map_err(|_| "another publication consumer owns this directory")?;
        crate::transparent::validation::cleanup_repairs(&config.directory)?;
        let invalid: BTreeSet<String> =
            match std::fs::read(config.directory.join("active.invalid.json")) {
                Ok(bytes) => serde_json::from_slice(&bytes)?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeSet::new(),
                Err(e) => return Err(e.into()),
            };
        let mut intent = floor(&config.directory)?;
        if !invalid.is_subset(&intent.digests) {
            // If one of the two durability writes failed, conservatively bind
            // otherwise unrecorded owner revocations to the last observed source
            // revision before polling again. Equal-revision recovery stays shut.
            intent.revision = intent
                .revision
                .max(source.observed_revision(&config.directory)?);
            intent.digests.extend(invalid);
            persist_floor(&config.directory, &intent)?;
        }
        // No disk-cache restore/build path in this mode, including after restart.
        Ok(Self {
            config,
            full: false,
            service,
            archive: None,
            source,
            _lock: std::sync::Arc::new(lock),
            live: None,
            pending: None,
            current: None,
            revocations: intent,
            recovery_attempted: false,
            continuation: false,
            observation_unchanged: false,
            synchronization: std::sync::Arc::new(std::sync::RwLock::new(
                serde_json::json!({"state":"not-polled"}),
            )),
        })
    }
    pub fn live(&self) -> Option<LiveService> {
        self.live.clone()
    }
    /// Private operational route; the operator TLS proxy must expose wallet paths only.
    /// It reads actual owner/runtime state and the last completed source/archive
    /// observations. No timestamp is serving authority or a freshness lease.
    pub fn router(&self) -> Result<axum::Router, Error> {
        let live = self.live().ok_or("provider did not activate")?;
        let sync = self.synchronization.clone();
        Ok(live.router().route(
            "/v1/provider-status",
            axum::routing::get(move || {
                let live = live.clone();
                let sync = sync.clone();
                async move {
                    let mut status = live.provider_observations();
                    status["synchronization"] = sync.read().unwrap().clone();
                    axum::Json(status)
                }
            }),
        ))
    }
    /// Collection is maintenance, not serving authority. Run it even while
    /// activation is refused, and refuse another allocation if it cannot finish.
    async fn collect(&self) -> Result<(), Error> {
        if let Some(live) = self.live.as_ref().or(self.pending.as_ref()) {
            let reply = live.command(Command::Collect).await?;
            if reply["source_collection_deferred"] != false {
                return Err("publication collection deferred; staging paused".into());
            }
            let status = live.command(Command::Status).await?;
            let active: Publication = serde_json::from_value(status["active"].clone())?;
            let root = self.config.directory.clone();
            tokio::task::spawn_blocking(move || {
                crate::transparent::objects::collect_objects(
                    &root,
                    &active.directory.join("shards.json"),
                )
                .map(|_| ())
                .map_err(|e| e.to_string())
            })
            .await??;
            return Ok(());
        }
        let root = self.config.directory.clone();
        tokio::task::spawn_blocking(move || collect_saved(&root).map_err(|e| e.to_string()))
            .await??;
        Ok(())
    }
    async fn load_saved(&mut self, current: Current) -> Result<PreparedHistory, Error> {
        let record = self.config.directory.join("active.json");
        let publication: Publication = serde_json::from_slice(&std::fs::read(&record)?)?;
        let directory = std::fs::canonicalize(&publication.directory)?;
        if !directory.starts_with(&self.config.directory) {
            return Err("saved publication escapes provider root".into());
        }
        let options = if self.full {
            if publication.assignment.is_some() {
                return Err("saved provider holdings differ from full coverage".into());
            }
            LoadOptions::whole(0)
        } else {
            let assignment = publication
                .assignment
                .as_ref()
                .ok_or("saved provider lacks assignment")?;
            LoadOptions {
                scope: LoadScope::Assigned {
                    worker_id: crate::transparent::routing::holdings::PROVIDER.into(),
                    assignment: std::sync::Arc::new(Assignment::load(assignment)?),
                },
                ..LoadOptions::whole(0)
            }
        };
        let load_options = options.clone();
        let set = tokio::task::spawn_blocking(move || {
            ShardSet::open_for_serving(&directory, &load_options)
        })
        .await??;
        if set.map_digest != publication.map_sha256
            || set.map.network != self.config.network
            || set.map.genesis_hash != self.config.genesis_hash
        {
            return Err("saved publication chain/map identity mismatch".into());
        }
        let state = ServiceState::build_with_runtimes(
            set,
            self.service,
            None,
            Some(ShippedRuntimes::required(
                publication
                    .runtimes
                    .clone()
                    .ok_or("saved provider lacks prepared runtimes")?,
            )),
        )?;
        let state = match &self.archive {
            Some(archive) => state.with_archive(archive.clone()),
            None => state,
        };
        state.check_required_material()?;
        let _prewarm = state.spawn_prewarm();
        let live = LiveService::new_required(
            state,
            publication.clone(),
            self.service,
            options,
            record,
            self._lock.clone(),
        )?;
        live.fence_recovered(
            &combined_floor(&self.config.directory, &mut self.revocations)?.digests,
        )?;
        // Warming is reusable work, not serving authority. If the publisher
        // advances during recovery, keep these checked inputs private so the
        // next fresh update does not warm the saved publication all over again.
        self.pending = Some(live.clone());
        let operation = live.reserve_activation(true).await?;
        Ok(PreparedHistory::Recovery {
            current: Box::new(current),
            live,
            publication,
            operation,
        })
    }
    /// Whether the recovered predecessor is exactly the fresh `current`: its
    /// persisted verified inventory, map, holdings and runtime configuration
    /// must all equal what a new pass would stage. A matching map alone is not
    /// enough, and an absent or damaged record only means another update.
    /// Saved files never grant authority: recovery already rechecked the fresh
    /// snapshot and every retained anchor before this runs.
    async fn recovered_is_current(&mut self, current: &Current) -> Result<bool, Error> {
        let publication: Publication =
            serde_json::from_slice(&std::fs::read(self.config.directory.join("active.json"))?)?;
        let directory = std::fs::canonicalize(&publication.directory)?;
        let runtimes = publication
            .runtimes
            .as_deref()
            .map(std::fs::canonicalize)
            .transpose()?;
        if runtimes.as_ref() != Some(&directory) {
            return Ok(false);
        }
        let Some(saved) = std::fs::read(directory.join("verified-inventory.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Inventory>(&bytes).ok())
        else {
            return Ok(false);
        };
        if saved.map_sha256 != publication.map_sha256 {
            return Ok(false);
        }
        let raw = self
            .source
            .small(
                &format!("inventories/{}", current.inventory_sha256),
                16 << 20,
            )
            .await?;
        if publication::digest(&raw) != current.inventory_sha256 {
            return Err("inventory digest mismatch".into());
        }
        let fresh: Inventory = serde_json::from_slice(&raw)?;
        fresh.validate(&self.config.network, &self.config.genesis_hash)?;
        if fresh != saved {
            return Ok(false);
        }
        let map: transparent_filter::ShardMap =
            serde_json::from_slice(&std::fs::read(directory.join("shards.json"))?)?;
        fresh.validate_map(&map)?;
        let end = map.shards.last().ok_or("empty publication")?;
        if current.anchor_height != end.end_height || current.anchor_hash != end.terminal_block_hash
        {
            return Ok(false);
        }
        // A revoked exact digest needs the full update's anchor restoration.
        let floor = combined_floor(&self.config.directory, &mut self.revocations)?;
        if map
            .shards
            .iter()
            .any(|shard| floor.digests.contains(&shard.manifest_digest))
        {
            return Ok(false);
        }
        let expected = bootstrap::holdings(&self.config, &map, &fresh, self.full)?;
        Ok(match (&expected.scope, &publication.assignment) {
            (LoadScope::Whole, None) => true,
            (LoadScope::Assigned { assignment, .. }, Some(path)) => {
                Assignment::load(path)? == **assignment
            }
            _ => false,
        })
    }
}

/// No in-memory owner exists on this path. Keep the saved predecessor and all
/// of its local inputs without treating that record as authority to serve it.
fn collect_saved(root: &Path) -> Result<(), Error> {
    let canonical = std::fs::canonicalize(root)?;
    let root = canonical.as_path();
    let mut keep = BTreeSet::new();
    let saved = match std::fs::read(root.join("active.json")) {
        Ok(bytes) => Some(serde_json::from_slice::<Publication>(&bytes)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if let Some(saved) = &saved {
        for path in std::iter::once(&saved.directory)
            .chain(saved.runtimes.iter())
            .chain(saved.assignment.iter())
        {
            let path = std::fs::canonicalize(path)?;
            let relative = path.strip_prefix(root)?;
            let first = relative
                .components()
                .next()
                .ok_or("saved input names provider root")?;
            keep.insert(root.join(first));
        }
    }
    if !crate::transparent::source_collection::collect_publications(root, &keep)? {
        return Err("publication collection deferred; staging paused".into());
    }
    // A retained map demonstrates the cache's hard-link mode. With no trees
    // left there are no publication references, so only unlinked objects remain.
    let linked = saved.map(|p| p.directory.join("shards.json")).or_else(|| {
        std::fs::read_dir(root)
            .ok()?
            .filter_map(Result::ok)
            .find_map(|entry| {
                let map = entry.path().join("shards.json");
                (entry.file_type().ok()?.is_dir() && map.is_file()).then_some(map)
            })
    });
    if let Some(linked) = linked {
        crate::transparent::objects::collect_objects(root, &linked)?;
    } else {
        crate::transparent::objects::collect_orphan_objects(root)?;
    }
    Ok(())
}
enum HistoryCandidate {
    Recovery(Current),
    Update(Current),
}
enum HistoryArtifacts {
    Recovery,
    Staged(Box<bootstrap::Staged>),
}
struct PreparedUpdate {
    current: Current,
    staged: bootstrap::Staged,
    publication: Publication,
    live: LiveService,
    expected: String,
    initial: bool,
    stages: Stages,
    operation: tokio::sync::OwnedMutexGuard<()>,
    activating: std::time::Instant,
}
enum PreparedHistory {
    Recovery {
        current: Box<Current>,
        live: LiveService,
        publication: Publication,
        operation: tokio::sync::OwnedMutexGuard<()>,
    },
    Update(Box<PreparedUpdate>),
}
struct HistoryValidation {
    current: Current,
    live: LiveService,
    map_path: Option<std::path::PathBuf>,
}
impl Consumer for HistorySync {
    type Observation = Current;
    type Candidate = HistoryCandidate;
    type Artifacts = HistoryArtifacts;
    type Loaded = PreparedHistory;
    type Validation = HistoryValidation;
    type Authority = ();
    type Report = Progress;
    async fn settle(&mut self) -> Result<(), Error> {
        self.source.discover().await?;
        if let Some(live) = &self.live {
            live.collect_memory().await?;
        }
        Ok(())
    }
    async fn observe(&mut self) -> Result<Current, Error> {
        let current = poll_snapshot(
            &mut self.source,
            &self.config,
            self.live.as_ref().or(self.pending.as_ref()),
            self.current.as_ref(),
            &mut self.revocations,
        )
        .await?;
        self.observation_unchanged = current.is_none();
        let current = current
            .or_else(|| self.current.clone())
            .ok_or("missing current publication")?;
        if !current.eligible() {
            return Err("publisher current publication is withdrawn or noncanonical".into());
        }
        Ok(current)
    }
    fn eligible(&self, current: &Current) -> bool {
        current.eligible()
    }
    fn keep_loaded(&self, _: &PreparedHistory) -> bool {
        false
    }
    async fn check_serving(&mut self, _: &Current) -> Result<(), Error> {
        Ok(())
    }
    async fn select(&mut self, current: &Current) -> Result<Input<HistoryCandidate>, Error> {
        self.continuation = false;
        crate::transparent::validation::repair_failed(&mut self.source, &self.config.directory)
            .await;
        if self.observation_unchanged {
            return Ok(Input::Unchanged);
        }
        if !self.recovery_attempted
            && self.live.is_none()
            && self.pending.is_none()
            && self.config.directory.join("active.json").exists()
        {
            self.recovery_attempted = true;
            self.continuation = true;
            return Ok(Input::Work(HistoryCandidate::Recovery(current.clone())));
        }
        self.collect().await?;
        Ok(Input::Work(HistoryCandidate::Update(current.clone())))
    }
    async fn reserve(&mut self, _: &mut HistoryCandidate) -> Result<PeerReservation, Error> {
        Ok(PeerReservation::Reserved)
    }
    async fn obtain(&mut self, candidate: &HistoryCandidate) -> Result<HistoryArtifacts, Error> {
        match candidate {
            HistoryCandidate::Recovery(_) => Ok(HistoryArtifacts::Recovery),
            HistoryCandidate::Update(current) => Ok(HistoryArtifacts::Staged(Box::new(
                bootstrap::stage_holdings(&self.config, &mut self.source, current, true, self.full)
                    .await?,
            ))),
        }
    }
    async fn load(
        &mut self,
        candidate: HistoryCandidate,
        artifacts: HistoryArtifacts,
    ) -> Result<PreparedHistory, Error> {
        match (candidate, artifacts) {
            (HistoryCandidate::Recovery(current), HistoryArtifacts::Recovery) => {
                self.load_saved(current).await
            }
            (HistoryCandidate::Update(current), HistoryArtifacts::Staged(staged)) => {
                let staged = *staged;
                let mut stages = Stages {
                    transfer_seconds: staged.transfer_seconds,
                    staging_seconds: staged.stage_seconds - staged.transfer_seconds,
                    ..Stages::default()
                };
                let publication = Publication {
                    directory: staged.directory.clone(),
                    assignment: (!self.full).then(|| staged.directory.join("assignment.json")),
                    map_sha256: staged.inventory.map_sha256.clone(),
                    runtimes: Some(staged.directory.clone()),
                };
                let cold = self.live.is_none();
                // Revocation permanently cancels a snapshot, even when the publisher
                // later restores its map. Rebuild private data before that recovery.
                if self
                    .pending
                    .as_ref()
                    .is_some_and(LiveService::is_invalidated)
                {
                    self.pending = None;
                }
                let live = if let Some(live) = self.live.as_ref().or(self.pending.as_ref()) {
                    live.clone()
                } else {
                    let loading = std::time::Instant::now();
                    let root = staged.directory.clone();
                    let options = staged.options.clone();
                    let set = tokio::task::spawn_blocking(move || {
                        ShardSet::open_for_serving(&root, &options)
                    })
                    .await??;
                    let state = ServiceState::build_with_runtimes(
                        set,
                        self.service,
                        None,
                        Some(ShippedRuntimes::required(staged.directory.clone())),
                    )?;
                    let state = match &self.archive {
                        Some(archive) => state.with_archive(archive.clone()),
                        None => state,
                    };
                    stages.loading_seconds = loading.elapsed().as_secs_f64();
                    let warming = std::time::Instant::now();
                    let _prewarm = state.spawn_prewarm();
                    state.check_required_material()?;
                    stages.prewarm_seconds = warming.elapsed().as_secs_f64();
                    LiveService::new_required(
                        state,
                        publication.clone(),
                        self.service,
                        staged.options.clone(),
                        self.config.directory.join("active.json"),
                        self._lock.clone(),
                    )?
                };
                if cold {
                    self.pending = Some(live.clone());
                    live.fence_recovered(
                        &combined_floor(&self.config.directory, &mut self.revocations)?.digests,
                    )?;
                }
                let status = live.command(Command::Status).await?;
                let expected = status["active"]["map_sha256"]
                    .as_str()
                    .ok_or("owner has no predecessor")?
                    .to_string();
                // An unchanged private bootstrap view still needs its first durable
                // activation. A changed map uses the normal prepared-candidate path.
                let initial = cold && expected == publication.map_sha256;
                if !initial {
                    let reply = live
                        .command(Command::Prepare {
                            expected: expected.clone(),
                            publication: publication.clone(),
                        })
                        .await;
                    let reply = match reply {
                        Ok(reply) => reply,
                        Err(error) => {
                            if cold {
                                self.pending = None;
                            }
                            return Err(error.into());
                        }
                    };
                    stages.loading_seconds = reply["loading_seconds"].as_f64().unwrap_or_default();
                    stages.prewarm_seconds = reply["warming_seconds"].as_f64().unwrap_or_default();
                }
                let activating = std::time::Instant::now();
                let operation = live.reserve_activation(initial).await?;
                Ok(PreparedHistory::Update(Box::new(PreparedUpdate {
                    current,
                    staged,
                    publication,
                    live,
                    expected,
                    initial,
                    stages,
                    operation,
                    activating,
                })))
            }
            _ => Err("history preparation does not match its artifact selection".into()),
        }
    }
    async fn rebind(
        &mut self,
        _: &Current,
        loaded: PreparedHistory,
    ) -> Result<PreparedHistory, Error> {
        Ok(loaded)
    }
    fn validate(
        &self,
        latest: &Current,
        loaded: &PreparedHistory,
    ) -> Result<HistoryValidation, Error> {
        let (current, live, map_path) = match loaded {
            PreparedHistory::Recovery { current, live, .. } => (current.as_ref(), live, None),
            PreparedHistory::Update(next) => (
                &next.current,
                &next.live,
                Some(next.staged.directory.join("shards.json")),
            ),
        };
        if latest != current {
            return Err("publication superseded before activation".into());
        }
        Ok(HistoryValidation {
            current: current.clone(),
            live: live.clone(),
            map_path,
        })
    }
    async fn authorize(&mut self, _: &Current, checked: &HistoryValidation) -> Result<(), Error> {
        let current = &checked.current;
        let config = &self.config;
        let root = &config.directory;
        let source = &mut self.source;
        let revocations = &mut self.revocations;
        let checked_live = checked.live.clone();
        let latest = current.clone();
        if let Some(map_path) = &checked.map_path {
            // A missed invalidation is not encoded in the latest range.
            // Revalidate exact candidate anchors, including formerly revoked
            // revisions, before permitting a newer recovery to clear them.
            let map: transparent_filter::ShardMap =
                serde_json::from_slice(&std::fs::read(map_path)?)?;
            let anchors = map.shards.iter().map(|shard| {
                (
                    shard.end_height,
                    shard.terminal_block_hash.clone(),
                    shard.manifest_digest.clone(),
                )
            });
            if !fresh_anchors(
                source,
                root,
                &checked_live,
                &latest,
                anchors,
                true,
                revocations,
            )
            .await?
            {
                return Err("candidate anchor is noncanonical".into());
            }
            let keep: BTreeSet<String> = map
                .shards
                .iter()
                .map(|shard| shard.manifest_digest.clone())
                .collect();
            if latest.revision > combined_floor(root, revocations)?.revision {
                checked_live.restore_verified(&keep)?;
                let mut intent = combined_floor(root, revocations)?;
                intent.digests.retain(|digest| !keep.contains(digest));
                persist_floor(root, &intent)?;
                *revocations = intent;
            }
            // Anchor checks cannot authorize a candidate across a source
            // pointer/recovery change. Never use 304 for this final read.
            let final_current =
                poll_snapshot(source, config, Some(&checked_live), None, revocations)
                    .await?
                    .ok_or("missing final current")?;
            if final_current != latest {
                return Err("publication superseded during anchor checks".into());
            }
        } else {
            let final_current =
                poll_snapshot(source, config, Some(&checked_live), None, revocations)
                    .await?
                    .ok_or("missing recovery recheck")?;
            if &final_current != current {
                return Err("recovery snapshot superseded during anchor checks".into());
            }
        }
        Ok(())
    }
    async fn confirm(&mut self, _: &mut PreparedHistory) -> Result<PeerReservation, Error> {
        Ok(PeerReservation::Reserved)
    }
    async fn commit(
        &mut self,
        loaded: PreparedHistory,
        _: HistoryValidation,
        _: (),
    ) -> Result<(Progress, PeerHandover), Error> {
        match loaded {
            PreparedHistory::Recovery {
                current,
                live,
                publication,
                operation,
            } => {
                let current = *current;
                live.commit_checked(
                    publication.map_sha256.clone(),
                    publication.map_sha256,
                    true,
                    operation,
                )
                .await?;
                self.pending = None;
                self.live = Some(live);
                let exact = match self.recovered_is_current(&current).await {
                    Ok(exact) => exact,
                    Err(error) => {
                        tracing::warn!(%error, "recovered publication not confirmed current; updating");
                        false
                    }
                };
                self.continuation = !exact;
                if exact {
                    self.current = Some(current);
                }
                Ok((self.serving_report().await, PeerHandover::Complete))
            }
            PreparedHistory::Update(next) => {
                let PreparedUpdate {
                    current,
                    staged,
                    publication,
                    live,
                    expected,
                    initial,
                    mut stages,
                    operation,
                    activating,
                } = *next;
                live.commit_checked(expected, publication.map_sha256, initial, operation)
                    .await?;
                persist(
                    &staged.directory.join("verified-inventory.json"),
                    &staged.inventory,
                )?;
                self.pending = None;
                self.live = Some(live.clone());
                self.current = Some(current.clone());
                collect_disk(&live, &self.config.directory, &staged.directory).await;
                stages.activation_seconds = activating.elapsed().as_secs_f64();
                Ok((
                    Progress {
                        unchanged: false,
                        revision: current.revision,
                        downloaded_bytes: self.source.downloaded(),
                        downloaded_files: staged.downloaded_files,
                        stages: Some(stages),
                    },
                    PeerHandover::Complete,
                ))
            }
        }
    }
    async fn serving_report(&mut self) -> Progress {
        Progress {
            unchanged: true,
            revision: self.current.as_ref().map_or(0, |current| current.revision),
            downloaded_bytes: self.source.downloaded(),
            downloaded_files: 0,
            stages: None,
        }
    }
    fn continue_after(&mut self, result: &Result<ConsumerPass<Progress>, Error>) -> bool {
        let again = std::mem::take(&mut self.continuation);
        if again {
            if let Err(error) = result {
                tracing::warn!(%error, "saved publication not eligible for recovery");
            }
        }
        again
            && !result
                .as_ref()
                .err()
                .is_some_and(crate::transparent::source::is_incompatible)
    }
    async fn pass_started(&mut self) {
        self.source.reset_budget();
        self.recovery_attempted = false;
        self.continuation = false;
        self.synchronization.write().unwrap()["in_progress"] = true.into();
    }
    async fn pass_finished(&mut self, result: &Result<ConsumerPass<Progress>, Error>) {
        if result.is_err()
            || result.as_ref().is_ok_and(|pass| match pass {
                ConsumerPass::Committed { report, .. } | ConsumerPass::Deferred { report } => {
                    report.unchanged
                }
            })
        {
            if let Err(error) = self.collect().await {
                tracing::warn!(%error, "failed-publication collection refused; staging remains paused");
            }
        }
        let completed = std::time::SystemTime::now();
        let at = completed
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let mut sync = self.synchronization.write().unwrap();
        // The last activated change survives later unchanged or refused polls.
        let last_change = match result {
            Ok(
                ConsumerPass::Committed {
                    report: progress, ..
                }
                | ConsumerPass::Deferred { report: progress },
            ) if !progress.unchanged => {
                serde_json::json!({"completed_unix":at,"progress":progress})
            }
            _ => sync["last_change"].clone(),
        };
        *sync = match result {
            Ok(
                ConsumerPass::Committed {
                    report: progress, ..
                }
                | ConsumerPass::Deferred { report: progress },
            ) => {
                serde_json::json!({"in_progress":false,"completed_unix":at,"state":"synchronized","progress":progress})
            }
            Err(error) => {
                serde_json::json!({"in_progress":false,"completed_unix":at,"state":"refused","error":error.to_string()})
            }
        };
        sync["last_change"] = last_change;
        match result {
            Ok(ConsumerPass::Committed { report, .. } | ConsumerPass::Deferred { report }) => {
                tracing::info!(?report, "publication synchronized")
            }
            Err(error) => tracing::warn!(%error, "publication synchronization refused"),
        }
    }
    fn terminal(&self, error: &Error) -> bool {
        crate::transparent::source::is_incompatible(error)
    }
}
/// Disk retention after an activation follows the owner's snapshot policy:
/// `Collect` keeps the active, retained and candidate trees, three more unused
/// ones and the catalog revisions they reference; objects that no remaining
/// tree links are then unlinked. Activation already succeeded, so a failed or
/// deferred collection is logged and retried after the next activation.
async fn collect_disk(live: &LiveService, root: &Path, active: &Path) {
    match live.command(Command::Collect).await {
        Ok(reply) if reply["source_collection_deferred"] == false => {
            let (root, linked) = (root.to_path_buf(), active.join("shards.json"));
            match tokio::task::spawn_blocking(move || {
                crate::transparent::objects::collect_objects(&root, &linked)
                    .map_err(|e| e.to_string())
            })
            .await
            {
                Ok(Ok(Some((files, bytes)))) => {
                    tracing::info!(files, bytes, "collected unreferenced objects")
                }
                Ok(Ok(None)) => tracing::info!("object collection deferred"),
                Ok(Err(error)) => tracing::warn!(%error, "object collection failed"),
                Err(error) => tracing::warn!(%error, "object collection task failed"),
            }
        }
        Ok(_) => tracing::info!("publication source collection deferred"),
        Err(error) => tracing::warn!(%error, "publication disk collection failed"),
    }
}
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Revocations {
    revision: u64,
    digests: BTreeSet<String>,
}
fn read_floor(path: &Path) -> Result<Revocations, Error> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Revocations::default()),
        Err(e) => Err(e.into()),
    }
}
fn floor(root: &Path) -> Result<Revocations, Error> {
    let mut primary = read_floor(&root.join("revocation-floor.json"))?;
    let fallback = read_floor(&root.join("revocation-fallback.json"))?;
    primary.revision = primary.revision.max(fallback.revision);
    primary.digests.extend(fallback.digests);
    Ok(primary)
}
fn combined_floor(root: &Path, memory: &mut Revocations) -> Result<Revocations, Error> {
    let mut storage_error = None;
    for name in ["revocation-floor.json", "revocation-fallback.json"] {
        match read_floor(&root.join(name)) {
            Ok(durable) => {
                memory.revision = memory.revision.max(durable.revision);
                memory.digests.extend(durable.digests);
            }
            Err(error) => {
                storage_error.get_or_insert(error);
            }
        }
    }
    if let Some(error) = storage_error {
        return Err(error);
    }
    Ok(memory.clone())
}
fn persist_floor(root: &Path, intent: &Revocations) -> Result<(), Error> {
    // A failed primary write is still an error. Retain the SAME exact source
    // revision/digests through an independent atomic path before owner fencing;
    // a stale high-water cannot reconstruct a missed revision after restart.
    let primary = persist(&root.join("revocation-floor.json"), intent);
    let fallback = if primary.is_err() || root.join("revocation-fallback.json").exists() {
        persist(&root.join("revocation-fallback.json"), intent)
    } else {
        Ok(())
    };
    primary?;
    fallback?;
    Ok(())
}
async fn revoke(
    root: &Path,
    live: &LiveService,
    current: &Current,
    from: u64,
    memory: &mut Revocations,
) -> Result<(), Error> {
    let status = live.command(Command::Status).await?;
    let mut digests = BTreeSet::new();
    for revision in status["revisions"]
        .as_array()
        .ok_or("owner missing revisions")?
    {
        if revision["end_height"].as_u64().is_some_and(|h| h >= from) {
            digests.insert(
                revision["digest"]
                    .as_str()
                    .ok_or("owner missing digest")?
                    .into(),
            );
        }
    }
    revoke_digests(root, live, current, digests, memory, Watermark::Advance).await
}

/// Whether a revocation moves the recovery watermark up to `current`.
enum Watermark {
    /// A withdrawal or an invalidation range. Replay protection needs the
    /// watermark at this source revision even when every digest is already fenced.
    Advance,
    /// An exact-anchor recheck. Digests already fenced and recorded stay at the
    /// revision that first revoked them, so a later canonical publication can
    /// clear them. A new digest, or one missing from either record, still advances.
    Recheck,
}

/// Whether an exact-anchor recheck of `digests` is new evidence. `covered` means
/// the live fence already contains every digest; `recorded` is the durable floor.
/// The watermark advances when either record is missing a digest.
fn recheck_raises_watermark(
    covered: bool,
    digests: &BTreeSet<String>,
    recorded: &BTreeSet<String>,
) -> bool {
    !covered || !digests.is_subset(recorded)
}

async fn revoke_digests(
    root: &Path,
    live: &LiveService,
    current: &Current,
    digests: BTreeSet<String>,
    memory: &mut Revocations,
    watermark: Watermark,
) -> Result<(), Error> {
    let raise = match watermark {
        Watermark::Advance => true,
        Watermark::Recheck => {
            recheck_raises_watermark(live.revocations_cover(&digests), &digests, &memory.digests)
        }
    };
    if raise {
        memory.revision = memory.revision.max(current.revision);
    }
    memory.digests.extend(digests.iter().cloned());
    let durable = (|| -> Result<(), Error> {
        let saved = combined_floor(root, memory);
        if let Err(error) = saved {
            // Preserve evidence in the existing fallback if readable; never
            // discard the in-process bound when any read or write also fails.
            if let Ok(fallback) = read_floor(&root.join("revocation-fallback.json")) {
                memory.revision = memory.revision.max(fallback.revision);
                memory.digests.extend(fallback.digests);
                let _ = persist(&root.join("revocation-fallback.json"), memory);
            }
            return Err(error);
        }
        let intent = memory.clone();
        persist_floor(root, &intent)
    })();
    // Storage failure cannot leave known orphaned material serving. The owner
    // still fences in memory and attempts its own durable write before we fail.
    let fenced = live.revoke_exact(digests).await;
    durable?;
    fenced?;
    Ok(())
}

async fn apply_snapshot(
    source: &mut Source,
    root: &Path,
    live: Option<&LiveService>,
    current: &Current,
    memory: &mut Revocations,
) -> Result<(), Error> {
    let Some(live) = live else {
        if !current.eligible() {
            memory.revision = memory.revision.max(current.revision);
            let intent = combined_floor(root, memory)?;
            persist_floor(root, &intent)?;
        }
        return Ok(());
    };
    let status = live.command(Command::Status).await?;
    let revisions = status["revisions"]
        .as_array()
        .ok_or("owner missing revisions")?;
    if let Some(from) = current.invalidated_from {
        if revisions
            .iter()
            .any(|s| s["end_height"].as_u64().is_some_and(|h| h >= from))
        {
            revoke(root, live, current, from, memory).await?;
        }
    }
    if !current.eligible() {
        let digests = revisions
            .iter()
            .filter(|s| {
                s["end_height"] == current.anchor_height
                    && s["terminal_block_hash"] == current.anchor_hash
            })
            .filter_map(|s| s["digest"].as_str().map(str::to_string))
            .collect();
        revoke_digests(root, live, current, digests, memory, Watermark::Advance).await?;
    }
    // Revalidate all retained anchors after missed revisions, rather than infer
    // their validity from the publisher's most recent invalidation range.
    let mut anchors = Vec::with_capacity(revisions.len());
    for revision in revisions {
        anchors.push((
            revision["end_height"]
                .as_u64()
                .ok_or("invalid owner anchor")?,
            revision["terminal_block_hash"]
                .as_str()
                .ok_or("invalid owner hash")?
                .to_string(),
            revision["digest"]
                .as_str()
                .ok_or("owner missing digest")?
                .to_string(),
        ));
    }
    fresh_anchors(source, root, live, current, anchors, false, memory).await?;
    Ok(())
}

/// Fresh canonicality of exact `(height, hash, digest)` anchors in bounded
/// batches, each pair asked once. Authenticated negatives from each completed
/// batch are revoked before any later batch is requested, so neither a later
/// failure nor a persistence error can discard them. A transport, outage or
/// identity failure answers for no anchor of its batch. Returns whether every
/// checked anchor was canonical; `stop` ends after the first negative batch.
async fn fresh_anchors(
    source: &mut Source,
    root: &Path,
    live: &LiveService,
    current: &Current,
    anchors: impl IntoIterator<Item = (u64, String, String)>,
    stop: bool,
    memory: &mut Revocations,
) -> Result<bool, Error> {
    let mut digests: BTreeMap<publication::AnchorPair, BTreeSet<String>> = BTreeMap::new();
    for (height, hash, digest) in anchors {
        digests
            .entry(publication::AnchorPair { height, hash })
            .or_default()
            .insert(digest);
    }
    let pairs: Vec<_> = digests.keys().cloned().collect();
    let mut canonical = true;
    for batch in pairs.chunks(publication::MAX_ANCHOR_BATCH) {
        let answers = source.anchors(current, batch).await?;
        let negative: BTreeSet<String> = batch
            .iter()
            .filter(|pair| !answers[*pair])
            .flat_map(|pair| digests[pair].iter().cloned())
            .collect();
        if !negative.is_empty() {
            canonical = false;
            revoke_digests(root, live, current, negative, memory, Watermark::Recheck).await?;
            if stop {
                break;
            }
        }
    }
    Ok(canonical)
}

/// Every live poll boundary applies authenticated positive evidence even if a
/// control-record write fails. Transport, identity and replay failures never
/// become invalidation evidence; a 304 never grants new authority.
async fn poll_snapshot(
    source: &mut Source,
    config: &Config,
    live: Option<&LiveService>,
    previous: Option<&Current>,
    memory: &mut Revocations,
) -> Result<Option<Current>, Error> {
    let root = &config.directory;
    let Some((current, read_error)) = source
        .poll(root, previous, &config.network, &config.genesis_hash)
        .await?
    else {
        return Ok(None);
    };
    let recorded = combined_floor(root, memory);
    if current.revision < memory.revision {
        return Err("publisher replay below observed revocation revision".into());
    }
    let mut source_storage = Ok(());
    if current.revision == memory.revision && current.eligible() {
        match source.observed_revision(root) {
            Ok(revision) if revision < memory.revision => {
                return Err("equal revision lacks a durable exact source identity".into());
            }
            Err(error) => source_storage = Err(error),
            _ => {}
        }
    }
    let durable = source.observe(root, &current);
    let applied = apply_snapshot(source, root, live, &current, memory).await;
    if let Some(error) = read_error {
        return Err(error);
    }
    source_storage?;
    durable?;
    applied?;
    recorded?;
    Ok(Some(current))
}

#[cfg(test)]
mod revocation_tests {
    use super::*;

    #[test]
    fn damaged_floor_preserves_memory_and_every_readable_companion() {
        for failed in ["revocation-floor.json", "revocation-fallback.json"] {
            for unreadable in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let companion = if failed.contains("fallback") {
                    "revocation-floor.json"
                } else {
                    "revocation-fallback.json"
                };
                persist(
                    &root.path().join(companion),
                    &Revocations {
                        revision: 100,
                        digests: BTreeSet::from(["durable".into()]),
                    },
                )
                .unwrap();
                if unreadable {
                    std::fs::create_dir(root.path().join(failed)).unwrap();
                } else {
                    std::fs::write(root.path().join(failed), b"{").unwrap();
                }
                assert!(
                    floor(root.path()).is_err(),
                    "startup must refuse a damaged floor"
                );
                let mut memory = Revocations {
                    revision: 90,
                    digests: BTreeSet::from(["in-memory".into()]),
                };
                assert!(combined_floor(root.path(), &mut memory).is_err());
                assert_eq!(memory.revision, 100);
                assert_eq!(
                    memory.digests,
                    BTreeSet::from(["durable".into(), "in-memory".into()])
                );
            }
        }
    }

    #[test]
    fn a_repeated_anchor_recheck_leaves_room_for_a_later_recovery() {
        let digest = BTreeSet::from(["d".to_string()]);
        let mut floor = Revocations::default();
        assert!(recheck_raises_watermark(false, &digest, &floor.digests));
        floor.revision = floor.revision.max(7);
        floor.digests.extend(digest.iter().cloned());

        // The same bytes are still noncanonical on a newer publication, and
        // both the live fence and the floor already record them. Recovery
        // requires a strictly newer source revision than this watermark.
        assert!(!recheck_raises_watermark(true, &digest, &floor.digests));
        assert!(9 > floor.revision);

        // Fenced in the live owner but missing from the floor: still new.
        assert!(recheck_raises_watermark(
            true,
            &digest,
            &Revocations::default().digests
        ));
        // Recovery clears the live fence. The same bytes failing again advance.
        assert!(recheck_raises_watermark(false, &digest, &floor.digests));
        // One new sibling digest makes the whole recheck new evidence.
        let both = BTreeSet::from(["d".to_string(), "e".to_string()]);
        assert!(recheck_raises_watermark(true, &both, &floor.digests));
    }
}

#[cfg(test)]
mod collection_tests {
    use super::*;
    use std::os::unix::fs::symlink;

    fn stage(root: &Path, revision: u64) -> std::path::PathBuf {
        let digest = format!("{revision:064x}");
        let object = root.join("objects").join(&digest);
        std::fs::create_dir_all(root.join("objects")).unwrap();
        std::fs::write(&object, vec![revision as u8; 4096]).unwrap();
        let source = root.join(".sources").join(&digest);
        std::fs::create_dir_all(&source).unwrap();
        std::fs::hard_link(&object, source.join("rows")).unwrap();
        let directory = root.join(format!("staging-{revision}-100"));
        std::fs::create_dir(&directory).unwrap();
        std::fs::hard_link(&object, directory.join("shards.json")).unwrap();
        symlink(source, directory.join(&digest)).unwrap();
        std::fs::File::open(&directory)
            .unwrap()
            .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(revision + 1))
            .unwrap();
        directory
    }

    #[test]
    fn failed_publications_bound_unique_objects_and_preserve_the_saved_predecessor() {
        let root = tempfile::tempdir().unwrap();
        let active = stage(root.path(), 0);
        persist(
            &root.path().join("active.json"),
            &Publication {
                directory: active.clone(),
                assignment: None,
                runtimes: Some(active.clone()),
                map_sha256: "0".repeat(64),
            },
        )
        .unwrap();
        for revision in 1..20 {
            stage(root.path(), revision);
            collect_saved(root.path()).unwrap();
            let objects = std::fs::read_dir(root.path().join("objects"))
                .unwrap()
                .count();
            assert!(
                objects <= 4,
                "active plus three unused publications: {objects}"
            );
            assert_eq!(
                std::fs::read(active.join("shards.json")).unwrap().len(),
                4096
            );
        }
    }

    #[test]
    fn a_provider_without_an_active_publication_still_collects_failed_stages() {
        let root = tempfile::tempdir().unwrap();
        for revision in 0..12 {
            stage(root.path(), revision);
            collect_saved(root.path()).unwrap();
            assert!(
                std::fs::read_dir(root.path().join("objects"))
                    .unwrap()
                    .count()
                    <= 3
            );
        }
    }

    #[test]
    fn downloads_from_an_interrupted_first_stage_are_collected_without_a_map() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("objects")).unwrap();
        let object = root.path().join("objects").join("a".repeat(64));
        std::fs::write(&object, b"downloaded before staging failed").unwrap();
        collect_saved(root.path()).unwrap();
        assert!(!object.exists());
    }

    #[test]
    fn unreadable_or_escaping_saved_inputs_refuse_collection() {
        let root = tempfile::tempdir().unwrap();
        for revision in 0..6 {
            stage(root.path(), revision);
        }
        for bytes in [
            b"{".to_vec(),
            serde_json::to_vec(&Publication {
                directory: std::env::temp_dir(),
                assignment: None,
                runtimes: None,
                map_sha256: "0".repeat(64),
            })
            .unwrap(),
        ] {
            std::fs::write(root.path().join("active.json"), bytes).unwrap();
            assert!(collect_saved(root.path()).is_err());
            assert_eq!(
                std::fs::read_dir(root.path().join("objects"))
                    .unwrap()
                    .count(),
                6
            );
        }
    }

    #[test]
    fn staging_lock_contention_pauses_collection_without_deleting_inputs() {
        let root = tempfile::tempdir().unwrap();
        for revision in 0..6 {
            stage(root.path(), revision);
        }
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.path().join(".sources.lock"))
            .unwrap();
        lock.try_lock().unwrap();
        assert!(collect_saved(root.path())
            .unwrap_err()
            .to_string()
            .contains("staging paused"));
        assert_eq!(
            std::fs::read_dir(root.path().join("objects"))
                .unwrap()
                .count(),
            6
        );
        drop(lock);
        collect_saved(root.path()).unwrap();
        assert_eq!(
            std::fs::read_dir(root.path().join("objects"))
                .unwrap()
                .count(),
            3
        );
    }
}
