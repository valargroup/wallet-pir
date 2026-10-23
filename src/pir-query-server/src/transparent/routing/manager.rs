//! Autonomous recent-worker synchronization. The private root has one owner;
//! completed bytes are reusable inputs, never saved serving authority.
use zakura_pir_protocol::transparent::filter as transparent_filter;

use crate::transparent::{
    durable::persist,
    history::{
        live::{Command, LiveService, Publication},
        service::{ServiceConfig, ServiceState},
    },
    routing::bootstrap::{self, Config},
    source::Source,
};
use pir_pipeline::Input;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Duration,
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
    sync: crate::synchronization::SyncManager<HistorySync>,
}
impl Manager {
    /// Check publisher discovery, then open the provider root. An unreachable or
    /// incompatible publisher opens no state.
    pub async fn new(config: Config, service: ServiceConfig) -> Result<Self, Error> {
        crate::transparent::source::discover(&config).await?;
        Ok(Self {
            sync: crate::synchronization::SyncManager::new(HistorySync::new(config, service)?),
        })
    }
    /// Hold and verify every shard locally, including archive runtimes.
    pub fn with_full_holdings(mut self) -> Self {
        self.sync.task_mut().full = true;
        self
    }
    pub fn with_archive(
        mut self,
        archive: std::sync::Arc<crate::transparent::routing::archive::Archive>,
    ) -> Self {
        self.sync.task_mut().archive = Some(archive);
        self
    }
    pub fn live(&self) -> Option<LiveService> {
        self.sync.task().live()
    }
    pub fn router(&self) -> Result<axum::Router, Error> {
        self.sync.task().router()
    }
    pub async fn poll_once(&mut self) -> Result<Progress, Error> {
        self.sync.poll_once().await
    }
    /// Synchronize until a publisher contract version mismatch, which is
    /// returned unchanged. Every other failure is retried.
    pub async fn run(self) -> Error {
        self.sync.run().await
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
    async fn reconcile(&mut self) -> Result<Progress, Error> {
        let input = pir_pipeline::acquire(self.acquire(), &mut ()).await?;
        match pir_pipeline::advance(input, Acquired::prepare, Prepared::publish, &mut ()).await? {
            Input::Work(progress) => Ok(progress),
            Input::Unchanged => Ok(Progress {
                unchanged: true,
                revision: self
                    .current
                    .as_ref()
                    .expect("conditional poll has an active snapshot")
                    .revision,
                downloaded_bytes: self.source.downloaded(),
                downloaded_files: 0,
                stages: None,
            }),
            // HTTPS acquisition has no deferred queue or admission outcome.
            Input::Deferred => unreachable!("HTTPS acquisition never defers work"),
        }
    }
    async fn acquire(&mut self) -> Result<Input<Acquired<'_>>, Error> {
        self.source.reset_budget();
        if let Some(live) = &self.live {
            live.collect_memory().await?;
        }
        let current = poll_snapshot(
            &mut self.source,
            &self.config,
            self.live.as_ref(),
            self.current.as_ref(),
            &mut self.revocations,
        )
        .await?;
        // Repair after refreshing authority, before staging can refuse a
        // withdrawn object shared with the next publication.
        crate::transparent::validation::repair_failed(&mut self.source, &self.config.directory)
            .await;
        let Some(current) = current else {
            return Ok(Input::Unchanged);
        };
        let mut recovered = false;
        if self.live.is_none()
            && self.pending.is_none()
            && self.config.directory.join("active.json").exists()
        {
            // A failed update does not discard the complete predecessor across
            // restart. Recovery still requires a fresh source snapshot and exact
            // retained anchors; loading this record alone never opens a listener.
            match self.recover(&current).await {
                Ok(live) => {
                    self.pending = None;
                    self.live = Some(live);
                    recovered = true;
                }
                Err(error) => tracing::warn!(%error, "saved publication not eligible for recovery"),
            }
        }
        // A recovered owner was not present for the first observation.
        if self.current.is_none() {
            apply_snapshot(
                &mut self.source,
                &self.config.directory,
                self.live.as_ref(),
                &current,
                &mut self.revocations,
            )
            .await?;
        }
        if !current.eligible() {
            return Err("publisher current publication is withdrawn or noncanonical".into());
        }
        if recovered {
            // The freshly checked predecessor already serves this exact snapshot:
            // restaging would only load and warm the same files a second time.
            match self.recovered_is_current(&current).await {
                Ok(true) => {
                    self.current = Some(current);
                    return Ok(Input::Unchanged);
                }
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(%error, "recovered publication not confirmed current; updating")
                }
            }
        }
        Ok(Input::Work(Acquired {
            owner: self,
            current,
        }))
    }
    async fn recover(&mut self, current: &Current) -> Result<LiveService, Error> {
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
        let checked = live.clone();
        let source = &mut self.source;
        let config = &self.config;
        let revocations = &mut self.revocations;
        live.activate_checked(
            publication.map_sha256.clone(),
            publication.map_sha256,
            true,
            || async {
                let result: Result<(), Error> = async {
                    let latest = poll_snapshot(source, config, Some(&checked), None, revocations)
                        .await?
                        .ok_or("missing recovery current")?;
                    if &latest != current {
                        return Err("recovery snapshot superseded".into());
                    }
                    let final_current =
                        poll_snapshot(source, config, Some(&checked), None, revocations)
                            .await?
                            .ok_or("missing recovery recheck")?;
                    if latest != final_current {
                        return Err("recovery snapshot superseded during anchor checks".into());
                    }
                    Ok(())
                }
                .await;
                result.map_err(|e| e.to_string())
            },
        )
        .await?;
        Ok(live)
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
impl crate::synchronization::SyncTask for HistorySync {
    type Report = Progress;
    type Error = Error;
    fn started(&mut self) {
        self.synchronization.write().unwrap()["in_progress"] = true.into();
    }
    async fn synchronize(&mut self) -> Result<Progress, Error> {
        // Every pass, unchanged ones included, rechecks the publisher's contract
        // version before any product request.
        self.source.discover().await?;
        self.reconcile().await
    }
    fn terminal(&self, error: &Error) -> bool {
        crate::transparent::source::is_incompatible(error)
    }
    fn finished(
        &mut self,
        result: &Result<Progress, Error>,
        completed: std::time::SystemTime,
        _elapsed: Duration,
    ) {
        let at = completed
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let mut sync = self.synchronization.write().unwrap();
        // The last activated change survives later unchanged or refused polls.
        let last_change = match result {
            Ok(progress) if !progress.unchanged => {
                serde_json::json!({"completed_unix":at,"progress":progress})
            }
            _ => sync["last_change"].clone(),
        };
        *sync = match result {
            Ok(progress) => {
                serde_json::json!({"in_progress":false,"completed_unix":at,"state":"synchronized","progress":progress})
            }
            Err(error) => {
                serde_json::json!({"in_progress":false,"completed_unix":at,"state":"refused","error":error.to_string()})
            }
        };
        sync["last_change"] = last_change;
    }
    fn report(&self, result: &Result<Progress, Error>) {
        match result {
            Ok(progress) => tracing::info!(?progress, "publication synchronized"),
            Err(error) => tracing::warn!(%error, "publication synchronization refused"),
        }
    }
}

/// Authenticated, replay-ordered source work. Safety evidence is applied before
/// returning it, including predecessor recovery. Neither this token nor staged
/// files grant authority. The borrow keeps one source budget and one owner/pass.
struct Acquired<'a> {
    owner: &'a mut HistorySync,
    current: Current,
}

/// Strictly loaded candidate; only the existing owner may publish it after its
/// final source/anchor checks under activation serialization. No dataset copies
/// or persisted phase token are needed.
struct Prepared<'a> {
    work: Acquired<'a>,
    staged: bootstrap::Staged,
    publication: Publication,
    live: LiveService,
    expected: String,
    initial: bool,
    stages: Stages,
}

impl<'a> Acquired<'a> {
    async fn prepare(self) -> Result<Input<Prepared<'a>>, Error> {
        let Acquired { owner, current } = self;
        let staged =
            bootstrap::stage_holdings(&owner.config, &mut owner.source, &current, true, owner.full)
                .await?;
        let mut stages = Stages {
            transfer_seconds: staged.transfer_seconds,
            staging_seconds: staged.stage_seconds - staged.transfer_seconds,
            ..Stages::default()
        };
        let publication = Publication {
            directory: staged.directory.clone(),
            assignment: (!owner.full).then(|| staged.directory.join("assignment.json")),
            map_sha256: staged.inventory.map_sha256.clone(),
            runtimes: Some(staged.directory.clone()),
        };
        let cold = owner.live.is_none();
        // Revocation permanently cancels a snapshot, even when the publisher
        // later restores its map. Rebuild private data before that recovery.
        if owner
            .pending
            .as_ref()
            .is_some_and(LiveService::is_invalidated)
        {
            owner.pending = None;
        }
        let live = if let Some(live) = owner.live.as_ref().or(owner.pending.as_ref()) {
            live.clone()
        } else {
            let loading = std::time::Instant::now();
            let root = staged.directory.clone();
            let options = staged.options.clone();
            let set =
                tokio::task::spawn_blocking(move || ShardSet::open_for_serving(&root, &options))
                    .await??;
            let state = ServiceState::build_with_runtimes(
                set,
                owner.service,
                None,
                Some(ShippedRuntimes::required(staged.directory.clone())),
            )?;
            let state = match &owner.archive {
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
                owner.service,
                staged.options.clone(),
                owner.config.directory.join("active.json"),
                owner._lock.clone(),
            )?
        };
        if cold {
            owner.pending = Some(live.clone());
            live.fence_recovered(
                &combined_floor(&owner.config.directory, &mut owner.revocations)?.digests,
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
                        owner.pending = None;
                    }
                    return Err(error.into());
                }
            };
            stages.loading_seconds = reply["loading_seconds"].as_f64().unwrap_or_default();
            stages.prewarm_seconds = reply["warming_seconds"].as_f64().unwrap_or_default();
        }
        Ok(Input::Work(Prepared {
            work: Acquired { owner, current },
            staged,
            publication,
            live,
            expected,
            initial,
            stages,
        }))
    }
}

impl Prepared<'_> {
    async fn publish(self) -> Result<Progress, Error> {
        let Prepared {
            work: Acquired { owner, current },
            staged,
            publication,
            live,
            expected,
            initial,
            mut stages,
        } = self;
        let activating = std::time::Instant::now();
        let config = &owner.config;
        let root = &config.directory;
        let revocations = &mut owner.revocations;
        let source = &mut owner.source;
        let checked_live = live.clone();
        let map_path = staged.directory.join("shards.json");
        // The owner obtains this check while it holds activation serialization.
        live.activate_checked(
            expected,
            publication.map_sha256.clone(),
            initial,
            || async {
                let result: Result<(), Error> = async {
                    let latest =
                        poll_snapshot(source, config, Some(&checked_live), None, revocations)
                            .await?
                            .ok_or("missing fresh current")?;
                    if latest != current || !latest.eligible() {
                        return Err("publication superseded before activation".into());
                    }
                    // A missed invalidation is not encoded in the latest range.
                    // Revalidate exact candidate anchors, including formerly revoked
                    // revisions, before permitting a newer recovery to clear them.
                    let map: transparent_filter::ShardMap =
                        serde_json::from_slice(&std::fs::read(&map_path)?)?;
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
                    Ok(())
                }
                .await;
                result.map_err(|e| e.to_string())
            },
        )
        .await?;
        persist(
            &staged.directory.join("verified-inventory.json"),
            &staged.inventory,
        )?;
        owner.pending = None;
        owner.live = Some(live.clone());
        owner.current = Some(current.clone());
        collect_disk(&live, &owner.config.directory, &staged.directory).await;
        stages.activation_seconds = activating.elapsed().as_secs_f64();
        Ok(Progress {
            unchanged: false,
            revision: current.revision,
            downloaded_bytes: owner.source.downloaded(),
            downloaded_files: staged.downloaded_files,
            stages: Some(stages),
        })
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
    revoke_digests(root, live, current, digests, memory).await
}
async fn revoke_digests(
    root: &Path,
    live: &LiveService,
    current: &Current,
    digests: BTreeSet<String>,
    memory: &mut Revocations,
) -> Result<(), Error> {
    memory.revision = memory.revision.max(current.revision);
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
        revoke_digests(root, live, current, digests, memory).await?;
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
            revoke_digests(root, live, current, negative, memory).await?;
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
}
