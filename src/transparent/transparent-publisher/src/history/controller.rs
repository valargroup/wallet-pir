//! Continuous, chain-checked publication. Ingest owns the journal; builders use
//! copied suffix snapshots and activation is delegated to the private fleet tool.
mod workflow;
use workflow::HistoryPublication;
use zakura_pir_publisher::transparent::filter as transparent_filter;
use zakura_pir_publisher::transparent::layout::Geometry;
use zakura_pir_publisher::transparent::seal::DecoupledPolicy;

pub(crate) use crate::history::distribution::advance_distribution_revision;
#[cfg(all(test, feature = "fixtures"))]
pub(crate) use crate::history::distribution::advance_distribution_revision_with_sync;
use crate::history::publication::{self, BoxError, Journal, PublishOptions};
use crate::history::shard_filters::ShardFilters;
use crate::ingest::blocks::build_fetched_block_events;
use crate::ingest::prevout::OutputCache;
use crate::ingest::snapshot::Snapshot;
use crate::ingest::zakura::NodeClient;
use axum::{
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Router,
};
use pir_pipeline::Input;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
};
use tokio::io::AsyncWriteExt;
use transparent_filter::BlockHash;
use transparent_shared::journal::events::{EventStore, EventStoreError, Refreshed};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub data_dir: PathBuf,
    pub publication_root: PathBuf,
    pub initial_publication: PathBuf,
    pub recent_from: u64,
    pub recent_geometry: String,
    pub archive_geometry: String,
    pub rpc_url: String,
    pub rpc_cookie: PathBuf,
    /// Executable implementing prepare/activate/invalidate with JSON on stdin.
    #[serde(default)]
    pub fleet_command: PathBuf,
    #[serde(default)]
    pub fleet_config: PathBuf,
    pub listen: std::net::SocketAddr,
    pub source_sha: String,
    #[serde(default)]
    pub shadow: bool,
    /// Which newly built shards publish a directory choice table. Absent means
    /// `off`, which is what every configuration before this field publishes.
    #[serde(default)]
    pub directory_choice: publication::DirectoryChoice,
    /// Range-filter profile to publish under. Absent means the original
    /// profile, which is what every configuration before this field publishes.
    #[serde(default = "default_range_profile")]
    pub range_profile: String,
    /// Follow a journal another process writes instead of ingesting into it.
    ///
    /// The journal at `data_dir` is then opened read-only: its writer keeps
    /// the lock, this controller re-reads its checkpoints, and a rollback the
    /// writer commits invalidates publication exactly as one seen by ingest
    /// would. Nothing is written beside the journal.
    #[serde(default)]
    pub follow_journal: bool,
    /// Where journal snapshots are spooled. Absent means the publication
    /// root; never the journal's directory when following.
    #[serde(default)]
    pub snapshot_dir: Option<PathBuf>,
    /// Prepare every runtime of each candidate beside its revisions, and
    /// offer them to the fleet, so workers load them instead of building.
    /// Off unless set: workers then build every runtime themselves.
    #[serde(default)]
    pub ship_runtimes: bool,
    /// Offer the opt-in prepared-publication distribution API. Requires shipped
    /// runtimes; existing deployments keep their existing preparation policy.
    #[serde(default)]
    pub distribute_prepared: bool,
    /// Finalize full prepared publications without managing query servers.
    #[serde(default)]
    pub http_only: bool,
}

impl Config {
    pub fn validate(&self) -> Result<(), BoxError> {
        if self.http_only && (!self.distribute_prepared || !self.ship_runtimes || self.shadow) {
            return Err("HTTP-only publishing requires prepared distribution, shipped runtimes and no shadow mode".into());
        }
        if !self.http_only
            && (self.fleet_command.as_os_str().is_empty()
                || self.fleet_config.as_os_str().is_empty())
        {
            return Err("fleet publishing requires fleet_command and fleet_config".into());
        }
        Ok(())
    }
    fn snapshot_dir(&self) -> &Path {
        self.snapshot_dir
            .as_deref()
            .unwrap_or(&self.publication_root)
    }
}

fn default_range_profile() -> String {
    transparent_filter::RANGE_PROFILE.to_string()
}
#[derive(Clone, Serialize, Deserialize)]
pub struct ActivePublication {
    pub directory: PathBuf,
    pub map_sha256: String,
    pub height: u64,
    pub hash: String,
    #[serde(default)]
    pub upstreams: Vec<String>,
}
struct Public {
    publication: ActivePublication,
    filters: ShardFilters,
    distribution: Option<transparent_shared::publication::Finalized>,
}
struct Inner {
    cycle_duration: pir_observability::Distribution,
    publication_freshness: pir_observability::Distribution,
    telemetry: pir_observability::HttpMetrics,
    publication_attempts: AtomicU64,
    publication_failures: AtomicU64,
    last_publication: AtomicU64,
    active: RwLock<Arc<Public>>,
    withdrawn: AtomicBool,
    epoch: AtomicU64,
    distribution_revision: AtomicU64,
    distribution_slots: Arc<tokio::sync::Semaphore>,
    /// The existing producer canonicality source, never provider credentials.
    canonicality: Option<NodeClient>,
    invalidated_from: AtomicU64,
    publication_gate: std::sync::Mutex<()>,
    /// The last `/v2/shards/init` answer assembled from the workers, with the
    /// map it describes. Activation switches the workers to the next map a
    /// moment before the public pointer moves; in that window no worker
    /// answers for the active map, and this answer, built for that same map,
    /// is served instead of a 503.
    last_init: std::sync::Mutex<Option<(String, serde_json::Value)>>,
    status: RwLock<serde_json::Value>,
    http: reqwest::Client,
    observed: RwLock<BTreeMap<u64, std::time::Instant>>,
    /// The fleet reconciler's membership record, when the fleet config names
    /// a state directory.
    membership: Option<PathBuf>,
    /// The last candidate whose runtimes were prepared, as verified then: the
    /// next one's unchanged tables are its hard links and are not read again.
    prepared_set: std::sync::Mutex<Option<transparent_shared::history::shardset::ShardSet>>,
    /// The service's stop request. Candidate preparation observes it between
    /// files and segments and ends without activating; activation completes.
    shutdown: pir_pipeline::service::Shutdown,
    /// Distributed runtimes this process already loaded and self-checked; the
    /// next inventory rehashes them but checks only changed bytes again.
    checked_runtimes: std::sync::Mutex<transparent_shared::publication::CheckedRuntimes>,
    /// Runtime preparation outcomes since this process started.
    runtime_preparations: AtomicU64,
    runtime_preparation_failures: AtomicU64,
    /// Background body validation of restored publications.
    validation: super::validator::Queue,
    /// The active publication lists bytes that failed integrity validation.
    integrity_failed: AtomicBool,
    /// Integrity withdrawal the fleet has not yet confirmed. Withdrawing this
    /// process's routes leaves the router proxying setup and query requests
    /// to workers that still serve the publication; the publication loop,
    /// which also owns activation, sends the fleet `withdraw` until it succeeds.
    fleet_withdrawal: AtomicBool,
    /// Candidate preparation and activation in progress; background
    /// validation waits while any runs.
    foreground: AtomicU64,
}
#[derive(Clone)]
pub struct Authority(Arc<Inner>);

/// The fleet's membership record: `<state_dir>/membership.json` from the
/// fleet adapter's config, if it names one.
fn membership_path(fleet_config: &Path) -> Option<PathBuf> {
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fleet_config).ok()?).ok()?;
    Some(PathBuf::from(config.get("state_dir")?.as_str()?).join("membership.json"))
}

/// Recent replicas routed right now, from a membership record written in the
/// last ten seconds. The count reported at activation goes stale as soon as
/// the reconciler routes a replica that finished preparing after quorum.
fn routed_recent_replicas(membership: Option<&Path>) -> Option<u64> {
    let record: serde_json::Value =
        serde_json::from_slice(&std::fs::read(membership?).ok()?).ok()?;
    let age = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs_f64()
        - record.get("updated_unix")?.as_f64()?;
    if !(0.0..=10.0).contains(&age) {
        return None;
    }
    record.get("routed_recent")?.as_u64()
}
impl Authority {
    pub fn router(&self) -> Router {
        self.0
            .telemetry
            .initialize(&["init", "map", "filter", "manifest"]);
        Router::new()
            .fallback(public_request)
            .layer(axum::middleware::from_fn_with_state(
                self.0.telemetry.clone(),
                transparent_shared::http::observe,
            ))
            .with_state(self.clone())
    }
}

pub async fn fleet(
    config: &Config,
    request: serde_json::Value,
) -> Result<serde_json::Value, BoxError> {
    use std::process::Stdio;
    let mut child = tokio::process::Command::new(&config.fleet_command)
        .arg(&config.fleet_config)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or("missing fleet stdin")?
        .write_all(&serde_json::to_vec(&request)?)
        .await?;
    let output = tokio::time::timeout(std::time::Duration::from_secs(90), child.wait_with_output())
        .await??;
    if !output.status.success() {
        return Err(format!("fleet operation failed: {}", output.status).into());
    }
    let body: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    if body["ok"] != true {
        return Err(format!("fleet refused operation: {body}").into());
    }
    Ok(body)
}

fn read_public(directory: PathBuf, upstreams: Vec<String>) -> Result<Public, BoxError> {
    let filters = ShardFilters::open(&directory)?;
    let end = filters.map().shards.last().ok_or("empty publication")?;
    let publication = ActivePublication {
        directory,
        map_sha256: filters.map_digest(),
        height: end.end_height,
        hash: end.terminal_block_hash.clone(),
        upstreams,
    };
    Ok(Public {
        publication,
        filters,
        distribution: None,
    })
}

pub async fn run(config: Config) -> Result<(), BoxError> {
    run_managed(
        config,
        pir_pipeline::service::Http::new(|addr, app| async move {
            axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await
        }),
        pir_pipeline::service::Shutdown::default(),
    )
    .await
}

/// Owns ingestion and authority through shutdown; transactions finish before stopping.
pub async fn run_managed(
    config: Config,
    http: pir_pipeline::service::Http<Router>,
    shutdown: pir_pipeline::service::Shutdown,
) -> Result<(), BoxError> {
    config.validate()?;
    if config.distribute_prepared && !config.ship_runtimes {
        return Err("prepared distribution requires ship_runtimes".into());
    }
    std::fs::create_dir_all(&config.publication_root)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(config.publication_root.join("controller.lock"))?;
    lock.try_lock()
        .map_err(|e| format!("another publication controller owns this root: {e}"))?;
    // Reserve a revision before exposing any state. A restarted controller
    // cannot replay a former current pointer, even if it reuses its map.
    let distribution_revision = if config.distribute_prepared {
        advance_distribution_revision(&config.publication_root)?
    } else {
        0
    };
    let rpc = crate::ingest::zakura::connect(&config.rpc_url, &config.rpc_cookie)?;
    let existing = EventStore::open_existing(&config.data_dir)?;
    if existing.start_height() != 0 || existing.genesis_hash() != rpc.block_hash_display(0).await? {
        return Err("journal identity does not match genesis RPC history".into());
    }
    std::fs::create_dir_all(config.snapshot_dir())?;
    // A followed journal stays its writer's: read-only, unlocked, untruncated.
    let store = if config.follow_journal {
        existing
    } else {
        let genesis = existing.genesis_hash().to_string();
        let start = existing.start_height();
        drop(existing);
        EventStore::open(&config.data_dir, &genesis, start)?
    };
    let store = Arc::new(tokio::sync::Mutex::new(store));
    let active_path = config.publication_root.join("active.json");
    let public = if active_path.exists() {
        let saved: ActivePublication = serde_json::from_slice(&std::fs::read(&active_path)?)?;
        let loaded = read_public(saved.directory, saved.upstreams)?;
        if loaded.publication.map_sha256 != saved.map_sha256 {
            return Err("active record disagrees with publication".into());
        }
        loaded
    } else {
        read_public(config.initial_publication.clone(), Vec::new())?
    };
    if public.filters.map().genesis_hash != store.lock().await.genesis_hash() {
        return Err("publication genesis does not match the journal".into());
    }
    let started = std::time::Instant::now();
    let mut public = public;
    let mut restored = None;
    let mut failed = None;
    if config.distribute_prepared && active_path.exists() {
        match restore_public(&config, &public) {
            Ok(Restore::Restored(found)) => {
                public.distribution = Some(found.finalized);
                restored = Some(found.validation);
            }
            // Only an installation without completion evidence (a fresh one,
            // or one written before this record existed) is prepared in full.
            Ok(Restore::Absent) => tracing::warn!(
                "completed publication has no restore record; it is prepared again in full before serving"
            ),
            // Recorded corrupt bytes stay withdrawn in this process too; only
            // a candidate with verified replacement bytes serves again.
            Ok(Restore::Failed(failure)) => {
                tracing::error!(path = %failure.path, reason = %failure.reason,
                    "the saved publication lists bytes that failed integrity validation; it stays withdrawn");
                failed = Some(failure);
            }
            // Present but unusable evidence, unsafe or incomplete files, an
            // unreadable integrity record or unresolved intent refuse to
            // start rather than hiding a full scan after a deploy stop.
            Err(error) => {
                return Err(format!(
                    "completed publication {} cannot be restored: {error}; deleting its completion record does not clear recorded integrity failures or publication intent, so deploy verified replacement bytes",
                    public.publication.directory.display()
                )
                .into())
            }
        }
    }
    let restore_seconds = started.elapsed().as_secs_f64();
    let authority = Authority(Arc::new(Inner {
        cycle_duration: Default::default(),
        publication_freshness: Default::default(),
        telemetry: Default::default(),
        publication_attempts: AtomicU64::new(0),
        publication_failures: AtomicU64::new(0),
        last_publication: AtomicU64::new(0),
        active: RwLock::new(Arc::new(public)),
        withdrawn: AtomicBool::new(true),
        epoch: AtomicU64::new(0),
        distribution_revision: AtomicU64::new(distribution_revision),
        distribution_slots: Arc::new(tokio::sync::Semaphore::new(DISTRIBUTION_SLOTS)),
        canonicality: Some(rpc.clone()),
        invalidated_from: AtomicU64::new(u64::MAX),
        publication_gate: std::sync::Mutex::new(()),
        last_init: std::sync::Mutex::new(None),
        status: RwLock::new(serde_json::json!({"phase":"starting",
            "integrity_validation":{"state":"pending"},
            "correctness_validation":{"state":"pending"}})),
        observed: RwLock::new(BTreeMap::new()),
        http: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()?,
        membership: membership_path(&config.fleet_config),
        prepared_set: std::sync::Mutex::new(None),
        shutdown: shutdown.clone(),
        checked_runtimes: Default::default(),
        runtime_preparations: AtomicU64::new(0),
        runtime_preparation_failures: AtomicU64::new(0),
        validation: Default::default(),
        integrity_failed: AtomicBool::new(failed.is_some()),
        // Workers may still serve what the previous process activated.
        fleet_withdrawal: AtomicBool::new(failed.is_some()),
        foreground: AtomicU64::new(0),
    }));
    if let Some(failure) = failed {
        let mut status = authority.0.status.write().unwrap();
        status["phase"] = "integrity_failed".into();
        status["integrity_validation"] = serde_json::json!({"state":"failed",
            "publication":{"height":authority.0.active.read().unwrap().publication.height,
                "hash":authority.0.active.read().unwrap().publication.hash},
            "map_sha256":failure.map_sha256,"inventory_sha256":failure.inventory_sha256,
            "reason":format!("{}: {}", failure.path, failure.reason),
            "artifact":serde_json::to_value(&failure).unwrap_or_default()});
    }
    struct Fence(Authority);
    impl Drop for Fence {
        fn drop(&mut self) {
            self.0 .0.withdrawn.store(true, Ordering::SeqCst);
        }
    }
    let _fence = Fence(authority.clone());
    // Joined when this function returns, before the controller lock is released.
    let _validator = super::validator::Validator::spawn(
        authority.0.validation.clone(),
        Arc::new(transparent_shared::runtime::memory::WorkMemory::default()),
        {
            let authority = authority.clone();
            let root = config.publication_root.clone();
            Box::new(move || {
                authority.0.distribution_slots.available_permits() < DISTRIBUTION_SLOTS
                    || authority.0.foreground.load(Ordering::Acquire) > 0
                    || super::validator::held_by_test(&root)
            })
        },
        {
            let authority = authority.clone();
            let root = config.publication_root.clone();
            Box::new(move |identity, event| validation_event(&root, &authority, identity, event))
        },
    )?;
    let mut tasks = pir_pipeline::service::Tasks::default();
    let server = tokio::spawn(http.serve(config.listen, authority.router()));
    tasks.own(&server);
    let (notify, updates) = tokio::sync::watch::channel(0u64);
    let ingest_config = config.clone();
    let ingest_authority = authority.clone();
    let ingest_store = store.clone();
    let ingest_rpc = rpc.clone();
    let input_stop = shutdown.clone();
    let failed_authority = authority.clone();
    let ingest = tokio::spawn(async move {
        let mut task = HistoryInputTask {
            config: ingest_config,
            rpc: ingest_rpc,
            store: ingest_store,
            authority: ingest_authority,
            notify,
            cache: OutputCache::new(crate::ingest::prevout::DEFAULT_CACHE_OUTPUTS),
            pending_invalidation: None,
        };
        let result = pir_pipeline::supervise(
            &mut task,
            std::time::Duration::from_secs(1),
            input_stop.cancelled(),
        )
        .await;
        if result.is_err() {
            let _gate = failed_authority.0.publication_gate.lock().unwrap();
            failed_authority.0.withdrawn.store(true, Ordering::Release);
            failed_authority.0.epoch.fetch_add(1, Ordering::AcqRel);
            input_stop.request();
        }
        result
    });
    tasks.own(&ingest);
    if let Some(validation) = restored {
        // Serve the restored publication as soon as authority allows; its
        // bodies are verified again in the background.
        match serve_restored(&config, &rpc, &store, &authority, validation).await {
            Ok(()) => {
                let mut status = authority.0.status.write().unwrap();
                status["restore"] = serde_json::json!({
                    "metadata_seconds": restore_seconds,
                    "serving_seconds": started.elapsed().as_secs_f64(),
                });
                tracing::info!(
                    metadata_seconds = restore_seconds,
                    serving_seconds = started.elapsed().as_secs_f64(),
                    "restored completed publication is serving"
                );
            }
            Err(error) => tracing::warn!(
                %error,
                "restored publication is not served; preparing it again in full"
            ),
        }
    }
    pir_pipeline::publisher::PublisherService::new(HistoryPublication {
        config,
        rpc,
        store,
        authority,
        shutdown: shutdown.clone(),
        server,
        ingest,
        updates,
    })
    .run(shutdown.cancelled())
    .await
}

// Independent from publication: backpressure or slow fleet preparation cannot
// delay canonical observation, revocation or journal commits.
struct HistoryInputTask {
    config: Config,
    rpc: NodeClient,
    store: Arc<tokio::sync::Mutex<EventStore>>,
    authority: Authority,
    notify: tokio::sync::watch::Sender<u64>,
    cache: OutputCache,
    pending_invalidation: Option<u64>,
}
impl pir_pipeline::Task for HistoryInputTask {
    type Error = BoxError;
    async fn run(&mut self) -> Result<(), BoxError> {
        pir_pipeline::acquire(
            async {
                if self.config.follow_journal {
                    follow_once(
                        &self.config,
                        &self.rpc,
                        &self.store,
                        &self.authority,
                        &mut self.pending_invalidation,
                        &self.notify,
                    )
                    .await?;
                } else {
                    ingest_once(
                        &self.config,
                        &self.rpc,
                        &self.store,
                        &self.authority,
                        &mut self.cache,
                        &self.notify,
                    )
                    .await?;
                }
                Ok::<_, BoxError>(Input::Work(()))
            },
            &mut (),
        )
        .await?;
        Ok(())
    }
    fn failed(&mut self, error: &BoxError) -> pir_pipeline::FailurePolicy {
        if error.downcast_ref::<EventStoreError>().is_some() {
            return pir_pipeline::FailurePolicy::Stop;
        }
        tracing::error!(%error,"chain/journal follow failed; retrying without advancing coverage");
        self.authority.0.status.write().unwrap()["ingest_error"] = error.to_string().into();
        pir_pipeline::FailurePolicy::NextPoll
    }
}

async fn invalidate(config: &Config, authority: &Authority, from: u64) -> Result<(), BoxError> {
    let retain = {
        let _gate = authority.0.publication_gate.lock().unwrap();
        let active = authority.0.active.read().unwrap();
        // A journal-only fork cancels candidates, but does not orphan this
        // older endpoint. The fleet independently checks this exact identity
        // under its routing lock before agreeing to preserve public service.
        let retain = (from > active.publication.height
            && !authority.0.withdrawn.load(Ordering::Acquire))
        .then(|| {
            serde_json::json!({
                "map_sha256":active.publication.map_sha256,
                "height":active.publication.height,
                "hash":active.publication.hash,
            })
        });
        if retain.is_none() {
            authority.0.withdrawn.store(true, Ordering::Release);
            authority.0.validation.cancel();
        }
        let epoch = authority.0.epoch.fetch_add(1, Ordering::AcqRel) + 1;
        if retain.is_some() {
            authority.0.validation.retain(epoch);
        }
        authority.0.invalidated_from.store(from, Ordering::Release);
        if config.distribute_prepared {
            authority.0.distribution_revision.store(
                advance_distribution_revision(&config.publication_root)?,
                Ordering::Release,
            );
        }
        publication::write_atomic(
            &config.publication_root.join("withdrawn.json"),
            &serde_json::to_vec(&serde_json::json!({"from_height":from,"acknowledged":false}))?,
        )?;
        retain
    };
    if !config.shadow && !config.http_only {
        let result = fleet(
            config,
            serde_json::json!({"operation":"invalidate","from_height":from,
                "retain_publication":retain}),
        )
        .await;
        // An unavailable/older fleet adapter cannot implicitly acknowledge
        // retention. A retry must revalidate and revoke before publication.
        if !matches!(&result, Ok(value) if value["retained_publication"] == true) {
            authority.0.withdrawn.store(true, Ordering::Release);
        }
        result?;
    }
    publication::write_atomic(
        &config.publication_root.join("withdrawn.json"),
        &serde_json::to_vec(&serde_json::json!({"from_height":from,"acknowledged":true}))?,
    )?;
    Ok(())
}

/// Send a pending integrity withdrawal to the fleet. Shadow and HTTP-only
/// controllers own no worker routing and only clear the request.
async fn withdraw_fleet(config: &Config, authority: &Authority) -> Result<(), BoxError> {
    if !authority.0.fleet_withdrawal.swap(false, Ordering::AcqRel)
        || config.shadow
        || config.http_only
    {
        return Ok(());
    }
    if let Err(error) = fleet(config, serde_json::json!({"operation":"withdraw"})).await {
        authority.0.fleet_withdrawal.store(true, Ordering::Release);
        return Err(error);
    }
    tracing::warn!("fleet routing withdrawn for a publication listing corrupt bytes");
    if let Some(status) = authority.0.status.write().unwrap().as_object_mut() {
        status.remove("fleet_withdrawal_error");
    }
    Ok(())
}

/// One pass of following a journal another process writes: observe the
/// node's tip for freshness, re-read the journal's checkpoint, and treat a
/// rollback its writer committed exactly as ingest treats one it performs.
///
/// A rollback whose invalidation fails is retried on the next pass, from the
/// lowest height seen, until the fleet acknowledges it.
async fn follow_once(
    config: &Config,
    rpc: &NodeClient,
    store: &Arc<tokio::sync::Mutex<EventStore>>,
    authority: &Authority,
    pending_invalidation: &mut Option<u64>,
    notify: &tokio::sync::watch::Sender<u64>,
) -> Result<(), BoxError> {
    let tip = rpc.tip_height().await?;
    let tip_hash = rpc.block_hash_display(tip).await?;
    {
        let public = authority.0.active.read().unwrap();
        if tip > public.publication.height
            || (tip == public.publication.height && tip_hash != public.publication.hash)
        {
            authority
                .0
                .observed
                .write()
                .unwrap()
                .entry(tip)
                .or_insert_with(std::time::Instant::now);
        }
    }
    authority.0.status.write().unwrap()["node_height"] = tip.into();
    authority.0.status.write().unwrap()["node_hash"] = tip_hash.into();
    let (refreshed, end) = {
        let mut journal = store.lock().await;
        let end = journal.covered_through();
        (journal.refresh()?, end)
    };
    match refreshed {
        Refreshed::RolledBack { from } => {
            *pending_invalidation = Some(pending_invalidation.map_or(from, |at| at.min(from)));
            authority.0.status.write().unwrap()["reorg_depth"] =
                end.map_or(0, |end| (end + 1).saturating_sub(from)).into();
            notify.send_modify(|n| *n += 1);
        }
        Refreshed::Extended => notify.send_modify(|n| *n += 1),
        Refreshed::Unchanged => {}
    }
    if let Some(from) = *pending_invalidation {
        invalidate(config, authority, from).await?;
        *pending_invalidation = None;
    }
    if let Some(height) = store.lock().await.covered_through() {
        authority.0.status.write().unwrap()["journal_height"] = height.into();
    }
    Ok(())
}

async fn ingest_once(
    config: &Config,
    rpc: &NodeClient,
    store: &Arc<tokio::sync::Mutex<EventStore>>,
    authority: &Authority,
    cache: &mut OutputCache,
    notify: &tokio::sync::watch::Sender<u64>,
) -> Result<(), BoxError> {
    let tip = rpc.tip_height().await?;
    let tip_hash = rpc.block_hash_display(tip).await?;
    {
        let public = authority.0.active.read().unwrap();
        if tip > public.publication.height
            || (tip == public.publication.height && tip_hash != public.publication.hash)
        {
            authority
                .0
                .observed
                .write()
                .unwrap()
                .entry(tip)
                .or_insert_with(std::time::Instant::now);
        }
    }
    authority.0.status.write().unwrap()["node_height"] = tip.into();
    authority.0.status.write().unwrap()["node_hash"] = tip_hash.clone().into();
    let public = authority.0.active.read().unwrap().clone();
    let public_changed = public.publication.height > tip
        || rpc.block_hash_display(public.publication.height).await? != public.publication.hash;
    let acknowledged = || {
        std::fs::read(config.publication_root.join("withdrawn.json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
            .is_some_and(|v| v["acknowledged"] == true)
    };
    // A deep ancestor search can take many RPC calls. Once the advertised
    // endpoint is known to be orphaned, withdraw before doing that search.
    // The fleet independently keeps canonical revisions when given height 0.
    let withdrew_early =
        public_changed && (!authority.0.withdrawn.load(Ordering::Acquire) || !acknowledged());
    if withdrew_early {
        invalidate(config, authority, 0).await?;
    }
    if public_changed && rpc.block_hash_display(0).await? != store.lock().await.genesis_hash() {
        return Err("node genesis changed; publication withdrawn and journal retained".into());
    }
    let mut ancestor = store.lock().await.covered_through();
    while let Some(height) = ancestor {
        let hash = store
            .lock()
            .await
            .block_at(height)
            .ok_or("missing journal endpoint")?
            .block_hash;
        if height <= tip && rpc.block_hash_display(height).await? == hash.to_display_hex() {
            break;
        }
        ancestor = height.checked_sub(1);
    }
    let end = store.lock().await.covered_through();
    let invalid_from = if ancestor != end {
        Some(ancestor.map_or(0, |h| h + 1))
    } else {
        None
    };
    if let Some(from) = invalid_from {
        // Repeated calls are necessary if a previous remote invalidation failed.
        // Avoid changing the epoch again after the same fork was reconciled.
        if !withdrew_early
            && (ancestor != end
                || !authority.0.withdrawn.load(Ordering::Acquire)
                || !acknowledged())
        {
            invalidate(config, authority, from).await?;
        }
    }
    if ancestor != end {
        store.lock().await.rollback_to(ancestor)?;
        *cache = OutputCache::new(crate::ingest::prevout::DEFAULT_CACHE_OUTPUTS);
        authority.0.status.write().unwrap()["reorg_depth"] = end
            .unwrap_or(0)
            .saturating_sub(ancestor.unwrap_or(0))
            .into();
        notify.send_modify(|n| *n += 1);
    }
    let mut next = store.lock().await.next_height();
    while next <= tip {
        let fetched = rpc.fetch_block(next).await?;
        let parent = BlockHash::from_internal_bytes(fetched.parent()).to_display_hex();
        let expected = if next == 0 {
            BlockHash::from_internal_bytes([0; 32]).to_display_hex()
        } else {
            store
                .lock()
                .await
                .block_at(next - 1)
                .ok_or("missing journal parent")?
                .block_hash
                .to_display_hex()
        };
        if parent != expected {
            return Err("node reorganized while fetching block; retry reconciliation".into());
        }
        let built = build_fetched_block_events(rpc, cache, next, fetched, false).await?;
        if rpc.block_hash_display(next).await? != built.block_hash.to_display_hex() {
            return Err("block became noncanonical during extraction".into());
        }
        {
            // History publications carry no display tables; the txid display
            // controller keeps its own journal with display sidecars.
            let mut journal = store.lock().await;
            journal.append_block(next, built.block_hash, &built.events)?;
            journal.commit()?;
        }
        authority.0.status.write().unwrap()["journal_height"] = next.into();
        notify.send_modify(|n| *n += 1);
        next += 1;
    }
    Ok(())
}

struct HistoryProgress;
impl pir_pipeline::Observer for HistoryProgress {
    fn finished(
        &mut self,
        phase: pir_pipeline::Phase,
        elapsed: std::time::Duration,
        succeeded: bool,
    ) {
        tracing::debug!(
            ?phase,
            seconds = elapsed.as_secs_f64(),
            succeeded,
            "history lifecycle phase"
        );
    }
}

// Product-owned tokens retain the immutable source snapshot and activation
// evidence. The shared engine owns neither stores nor publication authority.
struct HistoryWork {
    old: Arc<Public>,
    epoch: u64,
    snapshot: Snapshot,
    height: u64,
    hash: String,
}
struct HistoryCandidate {
    candidate: Public,
    epoch: u64,
    height: u64,
    hash: String,
    started: std::time::Instant,
    published_unix: f64,
    prepared_unix: f64,
    runtimes: Option<serde_json::Value>,
    prepared: serde_json::Value,
}

type HistorySeal = fn(&Geometry) -> DecoupledPolicy;

async fn publish_once(
    config: &Config,
    rpc: &NodeClient,
    store: &Arc<tokio::sync::Mutex<EventStore>>,
    authority: &Authority,
) -> Result<(), BoxError> {
    publish_candidate(
        config,
        rpc,
        store,
        authority,
        DecoupledPolicy::for_geometry,
        Some(config.archive_geometry.clone()),
        Some(config.recent_from),
    )
    .await
}

async fn publish_candidate(
    config: &Config,
    rpc: &NodeClient,
    store: &Arc<tokio::sync::Mutex<EventStore>>,
    authority: &Authority,
    seal: HistorySeal,
    archive_geometry: Option<String>,
    recent_from: Option<u64>,
) -> Result<(), BoxError> {
    let input = pir_pipeline::acquire(
        acquire_history(config, rpc, store, authority),
        &mut HistoryProgress,
    )
    .await?;
    struct Foreground<'a>(&'a AtomicU64);
    impl Drop for Foreground<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::AcqRel);
        }
    }
    let _foreground = matches!(input, Input::Work(_)).then(|| {
        authority.0.foreground.fetch_add(1, Ordering::AcqRel);
        Foreground(&authority.0.foreground)
    });
    pir_pipeline::publisher::PublicationPass::new(
        async { Ok::<_, BoxError>(input) },
        |work| {
            prepare_candidate(
                config,
                rpc,
                authority,
                work,
                seal,
                archive_geometry.clone(),
                recent_from,
            )
        },
        |candidate| activate_history(config, rpc, authority, candidate),
    )
    .run(&mut HistoryProgress)
    .await?;
    Ok(())
}

async fn acquire_history(
    config: &Config,
    rpc: &NodeClient,
    store: &Arc<tokio::sync::Mutex<EventStore>>,
    authority: &Authority,
) -> Result<Input<HistoryWork>, BoxError> {
    let old = authority.0.active.read().unwrap().clone();
    let epoch = authority.0.epoch.load(Ordering::Acquire);
    {
        let journal = store.lock().await;
        if journal.covered_through() == Some(old.publication.height)
            && journal
                .block_at(old.publication.height)
                .is_some_and(|b| b.block_hash.to_display_hex() == old.publication.hash)
            && !authority.0.withdrawn.load(Ordering::Acquire)
        {
            return Ok(Input::Unchanged);
        }
    }
    authority
        .0
        .publication_attempts
        .fetch_add(1, Ordering::Relaxed);
    let snapshot = {
        let journal = store.lock().await;
        Snapshot::capture(&journal, old.filters.map(), config.snapshot_dir())?
    };
    let height = snapshot.end();
    let hash = snapshot
        .block_at(height)
        .ok_or("snapshot has no endpoint")?
        .block_hash
        .to_display_hex();
    if rpc.block_hash_display(height).await? != hash {
        return Err("journal endpoint not canonical".into());
    }
    if height == old.publication.height
        && hash == old.publication.hash
        && (!authority.0.withdrawn.load(Ordering::Acquire)
            || authority.0.integrity_failed.load(Ordering::Acquire))
    {
        // An integrity failure needs replacement bytes, not another full
        // preparation of the same ones.
        return Ok(Input::Unchanged);
    }
    if config.shadow
        && authority
            .0
            .status
            .read()
            .unwrap()
            .get("shadow_hash")
            .is_some_and(|h| h == &hash)
    {
        return Ok(Input::Unchanged);
    }
    Ok(Input::Work(HistoryWork {
        old,
        epoch,
        snapshot,
        height,
        hash,
    }))
}

async fn prepare_candidate(
    config: &Config,
    rpc: &NodeClient,
    authority: &Authority,
    work: HistoryWork,
    seal: HistorySeal,
    archive_geometry: Option<String>,
    recent_from: Option<u64>,
) -> Result<Input<HistoryCandidate>, BoxError> {
    let HistoryWork {
        old,
        epoch,
        snapshot,
        height,
        hash,
    } = work;
    let started = std::time::Instant::now();
    // A stop ends preparation as a normal outcome: the active publication is
    // untouched and activation never begins. Blocking work is always awaited.
    let stopped = || {
        tracing::info!(height, "candidate preparation stopped by shutdown");
        Ok(Input::Unchanged)
    };
    if authority.0.shutdown.requested() {
        return stopped();
    }
    // Asked by every interruptible blocking step below, between its units.
    let stop = {
        let authority = authority.clone();
        move || {
            authority.0.shutdown.requested() || authority.0.epoch.load(Ordering::Acquire) != epoch
        }
    };
    let same = height == old.publication.height && hash == old.publication.hash;
    let directory = if same {
        old.publication.directory.clone()
    } else {
        config.publication_root.join(format!(
            "candidate-{height}-{hash}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ))
    };
    // Prefix equality, not publication height alone, determines reuse after forks.
    if !same {
        std::fs::create_dir(&directory)?;
        let options = PublishOptions {
            data_dir: config.data_dir.clone(),
            output: directory.clone(),
            previous: Some(old.publication.directory.clone()),
            recent_geometry: config.recent_geometry.clone(),
            archive_geometry,
            recent_from,
            zakura_rpc_url: config.rpc_url.clone(),
            zakura_cookie: config.rpc_cookie.clone(),
            through: Some(height),
            record: Some(directory.join("publication.json")),
            source_sha: Some(config.source_sha.clone()),
            directory_choice: config.directory_choice,
            range_profile: config.range_profile.clone(),
        };
        let stop = stop.clone();
        // HTTP-only publications store each table segment once beside them;
        // fleet publications keep plain files for the workers that read them.
        let stored = config.http_only;
        let output = directory.clone();
        let published = tokio::task::spawn_blocking(move || {
            publication::publish_until(
                &options,
                &snapshot,
                BlockHash::from_internal_bytes([0; 32]),
                &seal,
                &stop,
            )?;
            if stored {
                let adopted = crate::history::store::adopt_segments(&output, &stop)?;
                tracing::info!(?adopted, "stored candidate table segments");
            }
            Ok::<_, BoxError>(())
        })
        .await?;
        match published {
            Err(_) if authority.0.shutdown.requested() => return stopped(),
            Err(error) if error.is::<transparent_shared::publication::Stopped>() => {
                return Err("candidate invalidated while preparing".into())
            }
            result => drop(result?),
        }
    }
    if authority.0.shutdown.requested() {
        return stopped();
    }
    let mut candidate = read_public(directory.clone(), Vec::new())?;
    authority.0.status.write().unwrap()["candidate_height"] = height.into();
    let published_unix = unix_now();
    let runtimes = if config.ship_runtimes {
        match prepare_runtimes(config, authority, &directory, epoch).await {
            Err(_) if authority.0.shutdown.requested() => return stopped(),
            result => Some(result?),
        }
    } else {
        None
    };
    if config.distribute_prepared {
        // Reuse the exact verified set just used to prepare the candidate.
        // This does not turn optional fleet fallback into a distribution promise.
        let set = authority
            .0
            .prepared_set
            .lock()
            .unwrap()
            .take()
            .ok_or("prepared distribution has no verified candidate")?;
        let root = directory.clone();
        let cutoff = config.recent_from;
        let full = config.http_only;
        let mut checked = std::mem::take(&mut *authority.0.checked_runtimes.lock().unwrap());
        let started = std::time::Instant::now();
        let stop = stop.clone();
        let (set, checked, distribution) = tokio::task::spawn_blocking(move || {
            let result = transparent_shared::publication::Finalized::prepare_reusing_until(
                &root,
                &set,
                cutoff,
                full,
                &mut checked,
                &stop,
            );
            (set, checked, result)
        })
        .await?;
        tracing::info!(
            loaded = checked.loaded,
            reused = checked.reused,
            bytes_hashed = checked.bytes_hashed,
            bytes_reused = checked.bytes_reused,
            seconds = started.elapsed().as_secs_f64(),
            "prepared distribution inventory"
        );
        *authority.0.checked_runtimes.lock().unwrap() = checked;
        *authority.0.prepared_set.lock().unwrap() = Some(set);
        candidate.distribution = Some(match distribution {
            Err(_) if authority.0.shutdown.requested() => return stopped(),
            Err(error) if error.is::<transparent_shared::publication::Stopped>() => {
                return Err("candidate invalidated while preparing".into())
            }
            result => result?,
        });
    }
    let mut request = serde_json::json!({"operation":"prepare","directory":directory,"map_sha256":candidate.publication.map_sha256,"recent_from":config.recent_from,"source_sha":config.source_sha});
    if runtimes.is_some() {
        // The runtimes are plain files of the candidate directory itself.
        request["runtimes"] = serde_json::json!(directory);
    }
    // Ingestion may invalidate this candidate while the fleet is still waiting
    // for a preparation quorum. Stop that wait promptly: finishing an orphan's
    // preparation only delays the replacement. Dropping fleet() kills its local
    // child; independently owned worker preparation remains fenced by its epoch.
    // Activation is deliberately outside this cancellation path because it can
    // change routing and must complete the existing post-activation checks.
    let invalidated = async {
        while authority.0.epoch.load(Ordering::Acquire) == epoch {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    };
    let prepared = if config.http_only {
        serde_json::json!({"workers": []})
    } else {
        tokio::select! {
            biased;
            _ = authority.0.shutdown.cancelled() => return stopped(),
            _ = invalidated => return Err("candidate invalidated while preparing".into()),
            result = fleet(config, request) => result?,
        }
    };
    if authority.0.shutdown.requested() {
        return stopped();
    }
    if authority.0.epoch.load(Ordering::Acquire) != epoch
        || rpc.block_hash_display(height).await? != hash
    {
        return Err("candidate invalidated while preparing".into());
    }
    // Shutdown can arrive while the canonicality request is awaiting its reply.
    if authority.0.shutdown.requested() {
        return stopped();
    }
    let prepared_unix = unix_now();
    if config.shadow {
        authority.0.status.write().unwrap()["phase"] = "shadow_verified".into();
        authority.0.status.write().unwrap()["shadow_hash"] = hash.into();
        authority.0.status.write().unwrap()["build_seconds"] =
            started.elapsed().as_secs_f64().into();
        return Ok(Input::Unchanged);
    }
    Ok(Input::Work(HistoryCandidate {
        candidate,
        epoch,
        height,
        hash,
        started,
        published_unix,
        prepared_unix,
        runtimes,
        prepared,
    }))
}

async fn activate_history(
    config: &Config,
    rpc: &NodeClient,
    authority: &Authority,
    ready: HistoryCandidate,
) -> Result<(), BoxError> {
    let HistoryCandidate {
        mut candidate,
        epoch,
        height,
        hash,
        started,
        published_unix,
        prepared_unix,
        runtimes,
        prepared,
    } = ready;
    // The candidate is only evidence. Recheck at the owner boundary before
    // writing activation intent or changing any fleet authority.
    if authority.0.epoch.load(Ordering::Acquire) != epoch
        || rpc.block_hash_display(height).await? != hash
        // Ingest can invalidate while the canonicality RPC is awaiting its reply.
        || authority.0.epoch.load(Ordering::Acquire) != epoch
    {
        return Err("candidate invalidated before activation".into());
    }
    // The node request can outlive a stop request. Until activation intent is
    // written, abandoning the candidate still leaves the active publication alone.
    if authority.0.shutdown.requested() {
        tracing::info!(height, "candidate activation stopped by shutdown");
        return Ok(());
    }
    let directory = &candidate.publication.directory;
    if let Some(distribution) = &candidate.distribution {
        let failures = integrity_failures(&config.publication_root)?;
        if distribution.inventory().artifacts.iter().any(|a| {
            failures
                .iter()
                .any(|f| f.found_sha256.as_deref() == Some(a.sha256.as_str()))
        }) {
            return Err("candidate lists bytes that failed integrity validation".into());
        }
        // Completion evidence for a cheap restart, written only now that the
        // required preparation passed and before the active pointer moves.
        let record =
            distribution.restore_record(config.http_only, config.recent_from, height, &hash)?;
        publication::write_durable(
            &directory.join(transparent_shared::publication::RESTORE_FILE),
            &serde_json::to_vec(&record)?,
        )?;
    }
    publication::write_atomic(
        &config.publication_root.join("activation.json"),
        &serde_json::to_vec(&candidate.publication)?,
    )?;
    let activated = if config.http_only {
        serde_json::json!({"upstreams": [], "recent_replicas": []})
    } else {
        fleet(config,serde_json::json!({"operation":"activate","directory":directory,"map_sha256":candidate.publication.map_sha256,"prepared":prepared})).await?
    };
    if authority.0.epoch.load(Ordering::Acquire) != epoch
        || rpc.block_hash_display(height).await? != hash
    {
        authority.0.withdrawn.store(true, Ordering::Release);
        // The ingest loop invalidates the exact changed suffix; until then the
        // fleet hook withdraws routing entirely.
        if !config.http_only {
            fleet(config, serde_json::json!({"operation":"withdraw"})).await?;
        }
        return Err("candidate invalidated during fleet activation".into());
    }
    candidate.publication.upstreams = serde_json::from_value(activated["upstreams"].clone())?;
    let publication = candidate.publication.clone();
    let freshness = authority
        .0
        .observed
        .read()
        .unwrap()
        .range(..=height)
        .map(|(_, seen)| seen.elapsed().as_secs_f64())
        .fold(0f64, f64::max);
    {
        let _gate = authority.0.publication_gate.lock().unwrap();
        if authority.0.epoch.load(Ordering::Acquire) != epoch {
            return Err("reorg raced the public pointer commit".into());
        }
        publication::write_atomic(
            &config.publication_root.join("active.json"),
            &serde_json::to_vec(&candidate.publication)?,
        )?;
        if config.distribute_prepared {
            authority.0.distribution_revision.store(
                advance_distribution_revision(&config.publication_root)?,
                Ordering::Release,
            );
        }
        *authority.0.active.write().unwrap() = Arc::new(candidate);
        authority
            .0
            .invalidated_from
            .store(u64::MAX, Ordering::Release);
        authority.0.withdrawn.store(false, Ordering::Release);
        authority.0.integrity_failed.store(false, Ordering::Release);
        // The fleet activated these verified bytes; a withdrawal still
        // pending for the replaced publication is obsolete.
        authority.0.fleet_withdrawal.store(false, Ordering::Release);
        // Preparation verified these bytes; a restored predecessor's
        // validation is obsolete and releases its directory.
        authority.0.validation.cancel();
        // Freshly prepared bytes listed as a failed artifact was are verified
        // replacements; other recorded failures remain.
        if let Some(distribution) = &authority.0.active.read().unwrap().distribution {
            let failures = integrity_failures(&config.publication_root)?;
            let remaining: Vec<_> = failures
                .iter()
                .filter(|f| !lists_failed(std::slice::from_ref(*f), distribution.inventory()))
                .cloned()
                .collect();
            if remaining.len() != failures.len() {
                publication::write_durable(
                    &config.publication_root.join(INTEGRITY_FAILURES),
                    &serde_json::to_vec(&remaining)?,
                )?;
            }
        }
        for name in ["activation.json", "withdrawn.json"] {
            let p = config.publication_root.join(name);
            if p.exists() {
                std::fs::remove_file(p)?;
            }
        }
    }
    {
        let mut status = authority.0.status.write().unwrap();
        status["phase"] = "serving".into();
        status["public_height"] = height.into();
        status["public_hash"] = hash.clone().into();
        status["map_sha256"] = publication.map_sha256.clone().into();
        status["cycle_seconds"] = started.elapsed().as_secs_f64().into();
        authority
            .0
            .cycle_duration
            .observe(started.elapsed().as_secs_f64());
        authority.0.publication_freshness.observe(freshness);
        authority.0.last_publication.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
        status["freshness_seconds"] = freshness.into();
        status["ready_replicas"] = activated["recent_replicas"].clone();
        status.as_object_mut().unwrap().remove("publication_error");
        status.as_object_mut().unwrap().remove("restored");
        // Preparation ran both phases over these exact bytes before activation.
        for phase in ["integrity_validation", "correctness_validation"] {
            status[phase] = serde_json::json!({"state":"passed","source":"preparation",
                "publication":{"height":height,"hash":hash},
                "map_sha256":publication.map_sha256,
                "inventory_sha256":authority.0.active.read().unwrap().distribution.as_ref().map(|d| d.sha256().to_string())});
        }
        // What this cycle did, for operators and measurements: when its
        // candidate was published, accepted by a preparation quorum and
        // activated, what its runtimes cost here, and what each worker
        // that prepared it reported.
        let cycle = serde_json::json!({
            "height": height,
            "hash": hash,
            "map_sha256": publication.map_sha256,
            "first_observed_seconds_ago": freshness,
            "published_unix": published_unix,
            "prepared_unix": prepared_unix,
            "activated_unix": unix_now(),
            "cycle_seconds": started.elapsed().as_secs_f64(),
            "runtime_preparation": runtimes,
            "workers": prepared["workers"],
        });
        tracing::info!(%cycle, "publication cycle");
        status["last_cycle"] = cycle;
    }
    authority
        .0
        .observed
        .write()
        .unwrap()
        .retain(|h, _| *h > height);
    tracing::info!(
        height,
        hash,
        freshness_seconds = freshness,
        seconds = started.elapsed().as_secs_f64(),
        "activated warm publication"
    );
    Ok(())
}

fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Concurrent distribution responses; background validation waits while any runs.
const DISTRIBUTION_SLOTS: usize = 4;
/// Artifacts whose bytes failed integrity validation, kept across restarts.
const INTEGRITY_FAILURES: &str = "integrity-failures.json";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct IntegrityFailure {
    path: String,
    expected_sha256: String,
    found_sha256: Option<String>,
    inventory_sha256: String,
    map_sha256: String,
    reason: String,
}

fn integrity_failures(root: &Path) -> Result<Vec<IntegrityFailure>, BoxError> {
    match std::fs::read(root.join(INTEGRITY_FAILURES)) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

/// Whether `inventory` lists a failed artifact as it was, or bytes found corrupt.
fn lists_failed(
    failures: &[IntegrityFailure],
    inventory: &transparent_shared::publication::Inventory,
) -> bool {
    inventory.artifacts.iter().any(|a| {
        failures.iter().any(|f| {
            (f.path == a.path && f.expected_sha256 == a.sha256)
                || f.found_sha256.as_deref() == Some(a.sha256.as_str())
        })
    })
}

enum Restore {
    /// No completion evidence: the ordinary path prepares it in full.
    Absent,
    /// The saved inventory lists bytes recorded as failing integrity.
    Failed(IntegrityFailure),
    Restored(Box<transparent_shared::publication::Restored>),
}

/// The saved publication's completed distribution, without reading its bodies.
fn restore_public(config: &Config, public: &Public) -> Result<Restore, BoxError> {
    use transparent_shared::publication::{Finalized, RestoreExpectation, RESTORE_FILE};
    // Recorded withdrawal or unfinished activation intent is resolved by the
    // ordinary path, which starts withdrawn; it is never bypassed.
    for intent in ["withdrawn.json", "activation.json"] {
        if config.publication_root.join(intent).exists() {
            tracing::warn!(
                intent,
                "unresolved publication intent; not restoring cheaply"
            );
            return Ok(Restore::Absent);
        }
    }
    let directory = &public.publication.directory;
    let record = match std::fs::read(directory.join(RESTORE_FILE)) {
        Ok(record) => record,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Restore::Absent),
        Err(e) => return Err(format!("unreadable completed-publication record: {e}").into()),
    };
    let map = public.filters.map();
    let restored = Finalized::restore_completed(
        directory,
        &record,
        &RestoreExpectation {
            network: map.network.clone(),
            genesis_hash: map.genesis_hash.clone(),
            map_sha256: public.publication.map_sha256.clone(),
            full: config.http_only,
            recent_from_height: config.recent_from,
            anchor_height: public.publication.height,
            anchor_hash: public.publication.hash.clone(),
        },
    )?;
    let failures = integrity_failures(&config.publication_root)?;
    if let Some(failure) = failures
        .iter()
        .find(|f| lists_failed(std::slice::from_ref(*f), restored.finalized.inventory()))
    {
        return Ok(Restore::Failed(failure.clone()));
    }
    Ok(Restore::Restored(Box::new(restored)))
}

/// Make a restored publication public after fresh authority checks: its exact
/// anchor is still the journal's and the node's, no invalidation intervened,
/// and the fleet (unless HTTP-only) prepared and activated it.
async fn serve_restored(
    config: &Config,
    rpc: &NodeClient,
    store: &Arc<tokio::sync::Mutex<EventStore>>,
    authority: &Authority,
    validation: transparent_shared::publication::Validation,
) -> Result<(), BoxError> {
    let epoch = authority.0.epoch.load(Ordering::Acquire);
    let current = authority.0.active.read().unwrap().clone();
    let publication = current.publication.clone();
    let distribution = current
        .distribution
        .clone()
        .ok_or("restored publication has no distribution")?;
    let anchored = || async {
        let journal = store.lock().await;
        journal
            .block_at(publication.height)
            .is_some_and(|b| b.block_hash.to_display_hex() == publication.hash)
    };
    if !anchored().await || rpc.block_hash_display(publication.height).await? != publication.hash {
        return Err("restored anchor is not canonical".into());
    }
    let mut upstreams = publication.upstreams.clone();
    if !config.http_only {
        let request = serde_json::json!({"operation":"prepare","directory":publication.directory,
            "map_sha256":publication.map_sha256,"recent_from":config.recent_from,
            "source_sha":config.source_sha,"runtimes":publication.directory});
        let prepared = fleet(config, request).await?;
        if authority.0.epoch.load(Ordering::Acquire) != epoch {
            return Err("restored publication invalidated while preparing".into());
        }
        publication::write_atomic(
            &config.publication_root.join("activation.json"),
            &serde_json::to_vec(&publication)?,
        )?;
        let activated = fleet(
            config,
            serde_json::json!({"operation":"activate",
            "directory":publication.directory,"map_sha256":publication.map_sha256,
            "prepared":prepared}),
        )
        .await?;
        upstreams = serde_json::from_value(activated["upstreams"].clone())?;
    }
    if !anchored().await || rpc.block_hash_display(publication.height).await? != publication.hash {
        authority.0.withdrawn.store(true, Ordering::Release);
        if !config.http_only {
            fleet(config, serde_json::json!({"operation":"withdraw"})).await?;
        }
        return Err("restored anchor is not canonical".into());
    }
    {
        let _gate = authority.0.publication_gate.lock().unwrap();
        if authority.0.epoch.load(Ordering::Acquire) != epoch
            || config.publication_root.join("withdrawn.json").exists()
            || authority.0.integrity_failed.load(Ordering::Acquire)
        {
            return Err("restored publication invalidated before serving".into());
        }
        if upstreams != publication.upstreams {
            let mut next = read_public(publication.directory.clone(), upstreams)?;
            next.distribution = Some(distribution.clone());
            publication::write_atomic(
                &config.publication_root.join("active.json"),
                &serde_json::to_vec(&next.publication)?,
            )?;
            *authority.0.active.write().unwrap() = Arc::new(next);
        }
        authority
            .0
            .invalidated_from
            .store(u64::MAX, Ordering::Release);
        authority.0.withdrawn.store(false, Ordering::Release);
        let pending = config.publication_root.join("activation.json");
        if pending.exists() {
            std::fs::remove_file(pending)?;
        }
    }
    {
        let mut status = authority.0.status.write().unwrap();
        status["phase"] = "serving".into();
        status["restored"] = true.into();
        status["public_height"] = publication.height.into();
        status["public_hash"] = publication.hash.clone().into();
        status["map_sha256"] = publication.map_sha256.clone().into();
    }
    authority.0.validation.submit(super::validator::Job {
        identity: super::validator::Identity {
            directory: publication.directory.clone(),
            map_sha256: publication.map_sha256.clone(),
            inventory_sha256: distribution.sha256().to_string(),
            height: publication.height,
            hash: publication.hash.clone(),
            epoch,
        },
        inventory: distribution.inventory().clone(),
        validation,
    });
    Ok(())
}

/// A validation status object for `identity`.
fn validation_status(identity: &super::validator::Identity, state: &str) -> serde_json::Value {
    serde_json::json!({
        "state": state,
        "publication": {"height": identity.height, "hash": identity.hash},
        "map_sha256": identity.map_sha256,
        "inventory_sha256": identity.inventory_sha256,
    })
}

/// Apply one validator result. A result changes status only while it is about
/// the active inventory; a confirmed mismatch is always recorded, and
/// withdraws the active publication whenever it lists the same bytes.
fn validation_event(
    root: &Path,
    authority: &Authority,
    identity: &super::validator::Identity,
    event: super::validator::Event,
) {
    use super::validator::Event;
    use transparent_shared::publication::{CorrectnessError, IntegrityError};
    // Callers hold the publication gate: an activation, reorg or withdrawal
    // advances the epoch or replaces the active publication, after which a
    // result about the old one changes nothing.
    let current = || {
        let active = authority.0.active.read().unwrap();
        authority.0.epoch.load(Ordering::Acquire) == identity.epoch
            && !authority.0.withdrawn.load(Ordering::Acquire)
            && active.publication.directory == identity.directory
            && active.publication.map_sha256 == identity.map_sha256
            && active.publication.height == identity.height
            && active.publication.hash == identity.hash
            && active
                .distribution
                .as_ref()
                .is_some_and(|d| d.sha256() == identity.inventory_sha256)
    };
    let set = |key: &str, value: serde_json::Value| {
        if current() {
            authority.0.status.write().unwrap()[key] = value;
        }
    };
    let key = |phase: &str| format!("{phase}_validation");
    let mismatch = matches!(
        event,
        Event::Integrity(Err(IntegrityError::Mismatch { .. }))
    );
    // The mismatch arm takes the gate itself.
    let _gate = (!mismatch).then(|| authority.0.publication_gate.lock().unwrap());
    let update = |phase: &str, f: &dyn Fn(&mut serde_json::Value)| {
        if current() {
            let mut status = authority.0.status.write().unwrap();
            f(&mut status[key(phase)]);
        }
    };
    match event {
        Event::Started(phase) => {
            let mut value = validation_status(identity, "pending");
            value["started_unix"] = unix_now().into();
            set(&key(phase), value);
        }
        Event::Progress(phase, bytes) => update(phase, &|v| v["bytes_hashed"] = bytes.into()),
        Event::Integrity(Ok(bytes)) => update("integrity", &|v| {
            let started = v["started_unix"].as_f64().unwrap_or_else(unix_now);
            v["state"] = "passed".into();
            v["bytes_hashed"] = bytes.into();
            v["seconds"] = (unix_now() - started).into();
            v.as_object_mut().map(|o| o.remove("reason"));
        }),
        Event::Integrity(Err(IntegrityError::Mismatch {
            path,
            expected_sha256,
            found_sha256,
            reason,
        })) => {
            tracing::error!(%path, %reason, inventory = %identity.inventory_sha256,
                "publication artifact failed integrity validation");
            let failure = IntegrityFailure {
                path,
                expected_sha256,
                found_sha256,
                inventory_sha256: identity.inventory_sha256.clone(),
                map_sha256: identity.map_sha256.clone(),
                reason: reason.clone(),
            };
            let _gate = authority.0.publication_gate.lock().unwrap();
            // An unreadable record is never replaced by one holding only this
            // failure; the publication is withdrawn below either way.
            let recorded = match integrity_failures(root) {
                Ok(mut failures) => {
                    if !failures.contains(&failure) {
                        failures.push(failure.clone());
                    }
                    match serde_json::to_vec(&failures)
                        .map_err(BoxError::from)
                        .and_then(|bytes| {
                            publication::write_durable(&root.join(INTEGRITY_FAILURES), &bytes)
                        }) {
                        Ok(()) => true,
                        Err(error) => {
                            tracing::error!(%error, "could not record the integrity failure");
                            false
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(
                        %error,
                        "integrity failure record is unreadable; kept as is and the publication withdrawn"
                    );
                    false
                }
            };
            let active = authority.0.active.read().unwrap().clone();
            if let Some(distribution) = &active.distribution {
                if lists_failed(std::slice::from_ref(&failure), distribution.inventory()) {
                    authority.0.integrity_failed.store(true, Ordering::Release);
                    authority.0.withdrawn.store(true, Ordering::Release);
                    authority.0.fleet_withdrawal.store(true, Ordering::Release);
                    let mut status = authority.0.status.write().unwrap();
                    status["phase"] = "integrity_failed".into();
                    let mut value = validation_status(identity, "failed");
                    value["reason"] = format!("{}: {reason}", failure.path).into();
                    value["artifact"] = serde_json::to_value(&failure).unwrap_or_default();
                    // False when the failure is not durably recorded, so a
                    // restart would not by itself keep these bytes withdrawn.
                    value["recorded"] = recorded.into();
                    status["integrity_validation"] = value;
                }
            }
        }
        Event::Integrity(Err(
            error @ (IntegrityError::Unavailable(_) | IntegrityError::Stopped),
        )) => update("integrity", &|v| {
            v["state"] = "pending".into();
            v["reason"] = error.to_string().into();
        }),
        Event::Correctness(Ok(checked)) => {
            update("correctness", &|v| {
                let started = v["started_unix"].as_f64().unwrap_or_else(unix_now);
                v["state"] = "passed".into();
                v["runtimes_checked"] = checked.loaded.into();
                v["seconds"] = (unix_now() - started).into();
                v.as_object_mut().map(|o| o.remove("reason"));
            });
            if current() {
                authority.0.checked_runtimes.lock().unwrap().adopt(checked);
            }
        }
        Event::Correctness(Err(CorrectnessError::Failed(reason))) => {
            tracing::error!(%reason, inventory = %identity.inventory_sha256,
                "publication failed correctness validation; still serving");
            update("correctness", &|v| {
                v["state"] = "failed".into();
                v["reason"] = reason.clone().into();
            })
        }
        Event::Correctness(Err(error)) => update("correctness", &|v| {
            v["state"] = "pending".into();
            v["reason"] = error.to_string().into();
        }),
    }
}

/// Prepares the runtimes of the candidate in `directory` beside its
/// revisions; returns its report for the cycle record.
///
/// The candidate is read as a worker reads it, verifying every table the
/// previous prepared candidate did not already hold as the same file. A
/// table that fails verification fails the publication. Each runtime is
/// linked from the active publication or a retained candidate that holds a
/// completed file of its identity, and built otherwise; a runtime that
/// cannot be built or written is reported and left to the workers. An
/// invalidation stops the preparation between segments, and the candidate
/// is then never offered.
async fn prepare_runtimes(
    config: &Config,
    authority: &Authority,
    directory: &Path,
    epoch: u64,
) -> Result<serde_json::Value, BoxError> {
    use transparent_shared::history::shardset::{LoadOptions, ShardSet};
    let mut retained = vec![authority
        .0
        .active
        .read()
        .unwrap()
        .publication
        .directory
        .clone()];
    let mut candidates: Vec<_> = std::fs::read_dir(&config.publication_root)?
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_ok_and(|t| t.is_dir())
                && e.file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with("candidate-"))
                && e.path() != directory
        })
        .collect();
    candidates.sort_by_key(|e| std::cmp::Reverse(e.metadata().and_then(|m| m.modified()).ok()));
    retained.extend(candidates.into_iter().map(|e| e.path()));
    let previous = authority.0.prepared_set.lock().unwrap().take();
    let stop = {
        let authority = authority.clone();
        move || {
            authority.0.shutdown.requested() || authority.0.epoch.load(Ordering::Acquire) != epoch
        }
    };
    let directory = directory.to_path_buf();
    let stored = config.http_only;
    let (previous, prepared) = tokio::task::spawn_blocking(move || {
        // Every revision the publisher wrote; it retains them all, and so
        // may a worker.
        let options = LoadOptions {
            prune_excess: true,
            ..LoadOptions::whole(transparent_shared::history::shardset::DEFAULT_RETAIN_REVISIONS)
        };
        match ShardSet::open_reusing_until(&directory, &options, previous.as_ref(), &stop) {
            Ok(set) => {
                let prepared = transparent_shared::runtime::prebuild::prepare_publication(
                    &set, &directory, &retained, &stop,
                )
                .map_err(|e| e.to_string())
                .and_then(|prepared| {
                    if stored {
                        let adopted = crate::history::store::adopt_runtimes(&directory)
                            .map_err(|e| format!("storing prepared runtimes: {e}"))?;
                        tracing::info!(?adopted, "stored candidate runtimes");
                    }
                    Ok(prepared)
                });
                (Some(set), prepared)
            }
            // A stopped scan proved nothing; the earlier set keeps its evidence.
            Err(transparent_shared::history::shardset::LoadError::Stopped) => (
                previous,
                Err("candidate invalidated while preparing".to_string()),
            ),
            Err(error) => (
                previous,
                Err(format!("the candidate did not verify: {error}")),
            ),
        }
    })
    .await?;
    *authority.0.prepared_set.lock().unwrap() = previous;
    let prepared = prepared?;
    authority
        .0
        .runtime_preparations
        .fetch_add(1, Ordering::Relaxed);
    authority
        .0
        .runtime_preparation_failures
        .fetch_add(prepared.failures.len() as u64, Ordering::Relaxed);
    let report = prepared.report();
    authority.0.status.write().unwrap()["runtime_preparation"] = report.clone();
    Ok(report)
}

/// A public request the authority cannot answer consistently right now. The
/// `retry-after` makes wallets retry it shortly instead of failing the sync.
fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [("retry-after", "1"), ("cache-control", "no-store")],
    )
        .into_response()
}

/// The last assembled `/v2/shards/init` answer, if it describes `map`.
fn init_for_map(
    last: &Option<(String, serde_json::Value)>,
    map: &str,
) -> Option<serde_json::Value> {
    last.as_ref()
        .filter(|(last_map, _)| last_map == map)
        .map(|(_, value)| value.clone())
}

/// Use the existing owner canonicality input, not prepared-file parsing. A
/// source outage/identity failure proves nothing; a same-network hash mismatch
/// is positive noncanonical evidence. Snapshot fencing brackets both RPC reads.
async fn check_distribution_anchor(
    authority: &Authority,
    current: &transparent_shared::publication::Current,
    height: u64,
    hash: &str,
) -> Result<bool, BoxError> {
    let _permit = authority
        .0
        .distribution_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| "distribution canonicality admission full")?;
    let rpc = authority
        .0
        .canonicality
        .as_ref()
        .ok_or("canonicality source unavailable")?;
    if rpc.block_hash_display(0).await? != current.genesis_hash {
        return Err("canonicality source changed network".into());
    }
    let canonical = rpc.block_hash_display(height).await? == hash;
    // As in the existing canonical guard, a repointed/wrong-network node
    // cannot supply positive reorg evidence.
    if !canonical && rpc.block_hash_display(0).await? != current.genesis_hash {
        return Err("canonicality source changed network".into());
    }
    let _gate = authority.0.publication_gate.lock().unwrap();
    if authority.0.epoch.load(Ordering::Acquire) != current.recovery_epoch
        || authority.0.distribution_revision.load(Ordering::Acquire) != current.revision
    {
        return Err("canonicality snapshot superseded".into());
    }
    Ok(canonical)
}

/// [`check_distribution_anchor`] for a whole batch: one admission and genesis
/// check before and after every hash read, then one fence under the
/// publication gate against any epoch, revision, map, inventory or withdrawal
/// change. Any failure answers for no anchor.
async fn check_distribution_anchors(
    authority: &Authority,
    current: &transparent_shared::publication::Current,
    map_sha256: &str,
    anchors: &[transparent_shared::publication::AnchorPair],
) -> Result<Vec<bool>, BoxError> {
    let _permit = authority
        .0
        .distribution_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| "distribution canonicality admission full")?;
    let rpc = authority
        .0
        .canonicality
        .as_ref()
        .ok_or("canonicality source unavailable")?;
    if rpc.block_hash_display(0).await? != current.genesis_hash {
        return Err("canonicality source changed network".into());
    }
    let mut canonical = Vec::with_capacity(anchors.len());
    for anchor in anchors {
        canonical.push(rpc.block_hash_display(anchor.height).await? == anchor.hash);
    }
    // A repointed/wrong-network node cannot supply positive reorg evidence.
    if rpc.block_hash_display(0).await? != current.genesis_hash {
        return Err("canonicality source changed network".into());
    }
    let _gate = authority.0.publication_gate.lock().unwrap();
    let active = authority.0.active.read().unwrap();
    if authority.0.epoch.load(Ordering::Acquire) != current.recovery_epoch
        || authority.0.distribution_revision.load(Ordering::Acquire) != current.revision
        || authority.0.withdrawn.load(Ordering::Acquire) != !current.eligible()
        || active.publication.map_sha256 != map_sha256
        || active.distribution.as_ref().map(|d| d.sha256())
            != Some(current.inventory_sha256.as_str())
    {
        return Err("canonicality snapshot superseded".into());
    }
    Ok(canonical)
}

async fn anchor_batch(
    authority: &Authority,
    current: &transparent_shared::publication::Current,
    map_sha256: &str,
    request: Request,
) -> Response {
    use transparent_shared::publication::{self, AnchorBatch, AnchorBatchRequest, AnchorResult};
    let Ok(body) =
        axum::body::to_bytes(request.into_body(), publication::MAX_ANCHOR_BATCH_BYTES).await
    else {
        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
    };
    let Ok(batch) = serde_json::from_slice::<AnchorBatchRequest>(&body) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if batch.validate().is_err() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    // Never answer for this snapshot what the consumer asked about another.
    if !batch.names(current) {
        return (StatusCode::CONFLICT, [("cache-control", "no-store")]).into_response();
    }
    let canonical =
        match check_distribution_anchors(authority, current, map_sha256, &batch.anchors).await {
            Ok(canonical) => canonical,
            Err(_) => return unavailable(),
        };
    (
        [("cache-control", "no-store")],
        axum::Json(AnchorBatch {
            schema: publication::SCHEMA.into(),
            revision: current.revision,
            recovery_epoch: current.recovery_epoch,
            inventory_sha256: current.inventory_sha256.clone(),
            genesis_hash: current.genesis_hash.clone(),
            results: batch
                .anchors
                .into_iter()
                .zip(canonical)
                .map(|(anchor, canonical)| AnchorResult {
                    height: anchor.height,
                    hash: anchor.hash,
                    canonical,
                })
                .collect(),
        }),
    )
        .into_response()
}

async fn distribution_request(authority: Authority, request: Request) -> Response {
    use transparent_shared::publication::{self, Current, Eligibility};
    let batch = request.uri().path().strip_prefix(publication::PREFIX)
        == Some(publication::ANCHOR_BATCH_PATH);
    let method = if batch {
        axum::http::Method::POST
    } else {
        axum::http::Method::GET
    };
    if request.method() != method || request.uri().query().is_some() {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    // Capture current, withdrawal and epoch under the same gate that commits
    // the public pointer and withdraws orphaned coverage.
    let (active, mut current) = {
        let _gate = authority.0.publication_gate.lock().unwrap();
        let active = authority.0.active.read().unwrap().clone();
        let Some(finalized) = &active.distribution else {
            return unavailable();
        };
        let current = Current {
            schema: publication::SCHEMA.into(),
            revision: authority.0.distribution_revision.load(Ordering::Acquire),
            recovery_epoch: authority.0.epoch.load(Ordering::Acquire),
            inventory_sha256: finalized.sha256().to_string(),
            network: active.filters.map().network.clone(),
            genesis_hash: active.filters.map().genesis_hash.clone(),
            anchor_height: active.publication.height,
            anchor_hash: active.publication.hash.clone(),
            eligibility: if authority.0.withdrawn.load(Ordering::Acquire) {
                Eligibility::Withdrawn
            } else {
                Eligibility::Canonical
            },
            invalidated_from: match authority.0.invalidated_from.load(Ordering::Acquire) {
                u64::MAX => None,
                h => Some(h),
            },
        };
        (active, current)
    };
    let suffix = request
        .uri()
        .path()
        .strip_prefix(publication::PREFIX)
        .unwrap();
    if batch {
        let map_sha256 = active.publication.map_sha256.clone();
        return anchor_batch(&authority, &current, &map_sha256, request).await;
    }
    if let Some(anchor) = suffix.strip_prefix("anchors/") {
        let Some((height, hash)) = anchor.split_once('/') else {
            return StatusCode::NOT_FOUND.into_response();
        };
        let Ok(height) = height.parse::<u64>() else {
            return StatusCode::NOT_FOUND.into_response();
        };
        if !publication::is_digest(hash) {
            return StatusCode::NOT_FOUND.into_response();
        }
        let canonical = match check_distribution_anchor(&authority, &current, height, hash).await {
            Ok(canonical) => canonical,
            Err(_) => return unavailable(),
        };
        return (
            [("cache-control", "no-store")],
            axum::Json(publication::AnchorCheck {
                schema: publication::SCHEMA.into(),
                revision: current.revision,
                recovery_epoch: current.recovery_epoch,
                genesis_hash: current.genesis_hash,
                height,
                hash: hash.into(),
                canonical,
            }),
        )
            .into_response();
    }
    if suffix == "current" {
        if current.eligible() {
            match check_distribution_anchor(
                &authority,
                &current,
                current.anchor_height,
                &current.anchor_hash,
            )
            .await
            {
                Ok(true) => {}
                Ok(false) => current.eligibility = Eligibility::Noncanonical,
                Err(_) => return unavailable(),
            }
            // Never attest a candidate across withdrawal, recovery or a pointer
            // change that happened while the canonicality request was pending.
            let _gate = authority.0.publication_gate.lock().unwrap();
            if authority.0.epoch.load(Ordering::Acquire) != current.recovery_epoch
                || authority.0.distribution_revision.load(Ordering::Acquire) != current.revision
                || authority.0.active.read().unwrap().publication.map_sha256
                    != active.publication.map_sha256
            {
                return unavailable();
            }
            if authority.0.withdrawn.load(Ordering::Acquire) {
                current.eligibility = Eligibility::Withdrawn;
            }
        }
        let etag = format!(
            "\"{}\"",
            publication::digest(&serde_json::to_vec(&current).expect("serializable current"))
        );
        if request
            .headers()
            .get("if-none-match")
            .and_then(|h| h.to_str().ok())
            == Some(etag.as_str())
        {
            return (
                StatusCode::NOT_MODIFIED,
                [("cache-control", "no-store"), ("etag", etag.as_str())],
            )
                .into_response();
        }
        return (
            [("cache-control", "no-store"), ("etag", etag.as_str())],
            axum::Json(current),
        )
            .into_response();
    }
    if !current.eligible() {
        return unavailable();
    }
    let finalized = active.distribution.as_ref().unwrap();
    if suffix == format!("inventories/{}", finalized.sha256()) {
        return (
            [
                ("content-type", "application/json"),
                ("cache-control", "public, max-age=31536000, immutable"),
            ],
            finalized.bytes().to_vec(),
        )
            .into_response();
    }
    let Some(sha256) = suffix.strip_prefix("artifacts/") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(path) = finalized.file(sha256) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // Immutable file handles survive ordinary pointer retirement; they never
    // authorize activation. The consumer rechecks current after verification.
    let permit = match authority.0.distribution_slots.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return unavailable(),
    };
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(_) => return unavailable(),
    };
    (
        [
            ("content-type", "application/octet-stream"),
            ("cache-control", "public, max-age=31536000, immutable"),
        ],
        pir_control::http::retain(
            axum::body::Body::from_stream(tokio_util::io::ReaderStream::new(file)),
            (permit, active),
        ),
    )
        .into_response()
}

async fn public_request(State(authority): State<Authority>, request: Request) -> Response {
    let path = request.uri().path();
    if path.starts_with(transparent_shared::publication::PREFIX) {
        return distribution_request(authority, request).await;
    }
    let epoch = authority.0.epoch.load(Ordering::Acquire);
    let routed = routed_recent_replicas(authority.0.membership.as_deref());
    if path == "/v1/status" {
        let mut status = authority.0.status.read().unwrap().clone();
        if let Some(routed) = routed {
            status["ready_replicas"] = routed.into();
        }
        return axum::Json(status).into_response();
    }
    if path == "/metrics" {
        let mut status = authority.0.status.read().unwrap().clone();
        if let Some(routed) = routed {
            status["ready_replicas"] = routed.into();
        }
        let public = authority.0.active.read().unwrap().publication.height;
        let node = status["node_height"].as_u64().unwrap_or(public);
        let oldest = authority
            .0
            .observed
            .read()
            .unwrap()
            .range((public.saturating_add(1))..)
            .map(|(_, t)| t.elapsed().as_secs_f64())
            .fold(0f64, f64::max);
        let mut text = authority.0.telemetry.render();
        text.push_str(
            &authority
                .0
                .cycle_duration
                .render("transparent_publication_cycle_seconds"),
        );
        text.push_str(
            &authority
                .0
                .publication_freshness
                .render("transparent_publication_freshness_seconds"),
        );
        for (name, value) in [
            (
                "attempts_total",
                authority.0.publication_attempts.load(Ordering::Relaxed),
            ),
            (
                "failures_total",
                authority.0.publication_failures.load(Ordering::Relaxed),
            ),
            (
                "last_success_timestamp_seconds",
                authority.0.last_publication.load(Ordering::Relaxed),
            ),
            (
                "runtime_preparations_total",
                authority.0.runtime_preparations.load(Ordering::Relaxed),
            ),
            (
                "runtime_preparation_failures_total",
                authority
                    .0
                    .runtime_preparation_failures
                    .load(Ordering::Relaxed),
            ),
        ] {
            text.push_str(&format!("transparent_publication_{name} {value}\n"));
        }
        for (name, value) in [
            ("node_height", node as f64),
            (
                "journal_height",
                status["journal_height"].as_f64().unwrap_or(0.),
            ),
            ("public_height", public as f64),
            ("lag_blocks", node.saturating_sub(public) as f64),
            ("oldest_unpublished_seconds", oldest),
            (
                "withdrawn",
                if authority.0.withdrawn.load(Ordering::Acquire) {
                    1.
                } else {
                    0.
                },
            ),
            (
                "ready_recent_replicas",
                status["ready_replicas"].as_f64().unwrap_or(0.),
            ),
            (
                "last_cycle_seconds",
                status["cycle_seconds"].as_f64().unwrap_or(0.),
            ),
            (
                "last_freshness_seconds",
                status["freshness_seconds"].as_f64().unwrap_or(0.),
            ),
            ("reorg_depth", status["reorg_depth"].as_f64().unwrap_or(0.)),
            (
                "integrity_validation_state",
                validation_state(&status["integrity_validation"]),
            ),
            (
                "integrity_validation_seconds",
                status["integrity_validation"]["seconds"]
                    .as_f64()
                    .unwrap_or(0.),
            ),
            (
                "correctness_validation_state",
                validation_state(&status["correctness_validation"]),
            ),
            (
                "correctness_validation_seconds",
                status["correctness_validation"]["seconds"]
                    .as_f64()
                    .unwrap_or(0.),
            ),
        ] {
            text.push_str(&format!("# TYPE transparent_publication_{name} gauge\ntransparent_publication_{name} {value}\n"));
        }
        return ([("content-type", "text/plain; version=0.0.4")], text).into_response();
    }
    if request.method() != axum::http::Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    if authority.0.withdrawn.load(Ordering::Acquire) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "1"), ("cache-control", "no-store")],
            "transparent publication is being reconciled",
        )
            .into_response();
    }
    let active = authority.0.active.read().unwrap().clone();
    let body = if path == "/v2/shards" || path == "/v2/filters/shards" {
        Some(("application/json", active.filters.map_json().to_vec()))
    } else if path == "/v2/shards/init" {
        let expected: std::collections::BTreeSet<_> = active
            .filters
            .map()
            .shards
            .iter()
            .map(|e| e.geometry.clone())
            .collect();
        let mut geometries = std::collections::BTreeMap::new();
        let mut init = None;
        for upstream in &active.publication.upstreams {
            if let Ok(response) = authority
                .0
                .http
                .get(format!("http://{upstream}/v2/shards/init"))
                .send()
                .await
            {
                if let Ok(value) = response.json::<serde_json::Value>().await {
                    if value["map_sha256"] != active.publication.map_sha256 {
                        continue;
                    }
                    if let Some(entries) = value["geometries"].as_array() {
                        for geometry in entries {
                            if let Some(name) = geometry["name"].as_str() {
                                if geometries.get(name).is_some_and(|old| old != geometry) {
                                    return unavailable();
                                }
                                geometries.insert(name.to_string(), geometry.clone());
                            }
                        }
                    }
                    init = Some(value);
                    if expected.iter().all(|name| geometries.contains_key(name)) {
                        break;
                    }
                }
            }
        }
        if authority.0.withdrawn.load(Ordering::Acquire)
            || authority.0.epoch.load(Ordering::Acquire) != epoch
        {
            return unavailable();
        }
        let map = active.publication.map_sha256.as_str();
        let value = match init {
            Some(mut value) if expected.iter().all(|name| geometries.contains_key(name)) => {
                value["geometries"] = serde_json::Value::Array(geometries.into_values().collect());
                *authority.0.last_init.lock().unwrap() = Some((map.to_owned(), value.clone()));
                value
            }
            _ => match init_for_map(&authority.0.last_init.lock().unwrap(), map) {
                Some(value) => value,
                None => return unavailable(),
            },
        };
        return ([("cache-control", "no-cache")], axum::Json(value)).into_response();
    } else {
        let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
        match parts.as_slice() {
            ["v2", "filters", "shards", id, "filter"] => id
                .parse()
                .ok()
                .and_then(|id| active.filters.filter(id))
                .map(|b| ("application/octet-stream", b.to_vec())),
            ["v2", "shards", id, "revisions", digest, "manifest"]
                if digest.len() == 64 && digest.bytes().all(|c| c.is_ascii_hexdigit()) =>
            {
                if active
                    .filters
                    .map()
                    .shards
                    .iter()
                    .any(|e| e.shard_id.to_string() == *id && e.manifest_digest == *digest)
                {
                    std::fs::read(
                        active
                            .publication
                            .directory
                            .join(digest)
                            .join("manifest.json"),
                    )
                    .ok()
                    .map(|b| ("application/json", b))
                } else {
                    return StatusCode::CONFLICT.into_response();
                }
            }
            _ => None,
        }
    };
    if authority.0.withdrawn.load(Ordering::Acquire)
        || authority.0.epoch.load(Ordering::Acquire) != epoch
    {
        return unavailable();
    }
    match body {
        Some((content_type, body)) => (
            [
                ("content-type", content_type),
                ("cache-control", "no-cache"),
                ("x-shard-map-sha256", active.publication.map_sha256.as_str()),
            ],
            body,
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// 0 pending, 1 passed, -1 failed.
fn validation_state(value: &serde_json::Value) -> f64 {
    match value["state"].as_str() {
        Some("passed") => 1.,
        Some("failed") => -1.,
        _ => 0.,
    }
}

fn collect_candidates(config: &Config, authority: &Authority) -> Result<(), BoxError> {
    let active = authority
        .0
        .active
        .read()
        .unwrap()
        .publication
        .directory
        .clone();
    let pending = std::fs::read(config.publication_root.join("activation.json"))
        .ok()
        .and_then(|b| serde_json::from_slice::<ActivePublication>(&b).ok())
        .map(|p| p.directory);
    let validating = authority.0.validation.held();
    let mut unused: Vec<_> = std::fs::read_dir(&config.publication_root)?
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_ok_and(|t| t.is_dir())
                && e.file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with("candidate-"))
                && e.path() != active
                && pending.as_ref() != Some(&e.path())
                && !validating.contains(&e.path())
        })
        .collect();
    unused.sort_by_key(|e| std::cmp::Reverse(e.metadata().and_then(|m| m.modified()).ok()));
    for entry in unused.into_iter().skip(3) {
        std::fs::remove_dir_all(entry.path())?;
    }
    // Stored segments and runtimes no remaining publication links.
    let collected = crate::history::store::collect(&config.publication_root)?;
    if collected != Default::default() {
        tracing::info!(?collected, "collected unreferenced stored objects");
    }
    Ok(())
}

#[cfg(all(test, feature = "fixtures"))]
mod tests {

    #[test]
    fn init_is_served_from_the_last_answer_only_for_the_same_map() {
        let last = Some((
            "old-map".to_owned(),
            serde_json::json!({"map_sha256":"old-map"}),
        ));
        // Mid-activation the public pointer still names the old map, so the
        // answer built for it stands in for the workers that moved on.
        assert_eq!(
            super::init_for_map(&last, "old-map"),
            Some(serde_json::json!({"map_sha256":"old-map"}))
        );
        // Once the pointer moves, an answer for another map is never served.
        assert_eq!(super::init_for_map(&last, "new-map"), None);
        assert_eq!(super::init_for_map(&None, "old-map"), None);
    }

    #[test]
    fn unavailable_asks_wallets_to_retry() {
        let response = super::unavailable();
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(response.headers()["retry-after"], "1");
    }

    #[test]
    fn ready_replicas_follow_a_fresh_membership_record_only() {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join("state");
        std::fs::create_dir(&state).unwrap();
        let fleet = root.path().join("fleet.json");
        std::fs::write(
            &fleet,
            serde_json::to_vec(&serde_json::json!({"state_dir": state})).unwrap(),
        )
        .unwrap();
        let membership = membership_path(&fleet).unwrap();
        assert_eq!(membership, state.join("membership.json"));
        assert_eq!(routed_recent_replicas(Some(&membership)), None);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let write = |updated: f64| {
            std::fs::write(
                &membership,
                serde_json::to_vec(
                    &serde_json::json!({"updated_unix": updated, "routed_recent": 4}),
                )
                .unwrap(),
            )
            .unwrap()
        };
        write(now);
        assert_eq!(routed_recent_replicas(Some(&membership)), Some(4));
        write(now - 60.0);
        assert_eq!(
            routed_recent_replicas(Some(&membership)),
            None,
            "a stale record falls back to the activation count"
        );
        assert_eq!(membership_path(&root.path().join("absent.json")), None);
    }

    use super::*;
    use axum::{routing::post, Json};

    /// A few empty blocks close a shard. The service follow path does not use this.
    #[cfg(not(feature = "qualification-tests"))]
    fn two_script_seal(geometry: &Geometry) -> DecoupledPolicy {
        let mut policy = DecoupledPolicy::for_geometry(geometry);
        policy.scripts.target = 2;
        policy
    }

    async fn publish_follow(
        config: &Config,
        rpc: &NodeClient,
        store: &Arc<tokio::sync::Mutex<EventStore>>,
        authority: &Authority,
    ) -> Result<(), BoxError> {
        #[cfg(not(feature = "qualification-tests"))]
        {
            return publish_candidate(config, rpc, store, authority, two_script_seal, None, None)
                .await;
        }
        #[cfg(feature = "qualification-tests")]
        {
            publish_once(config, rpc, store, authority).await
        }
    }

    async fn prepare_follow(
        config: &Config,
        rpc: &NodeClient,
        authority: &Authority,
        work: HistoryWork,
    ) -> Result<Input<HistoryCandidate>, BoxError> {
        #[cfg(not(feature = "qualification-tests"))]
        {
            return prepare_candidate(config, rpc, authority, work, two_script_seal, None, None)
                .await;
        }
        #[cfg(feature = "qualification-tests")]
        {
            prepare_candidate(
                config,
                rpc,
                authority,
                work,
                DecoupledPolicy::for_geometry,
                Some(config.archive_geometry.clone()),
                Some(config.recent_from),
            )
            .await
        }
    }
    type Chain = Arc<RwLock<Vec<Vec<u8>>>>;
    struct MockNode {
        chain: Chain,
        pause_ancestor: AtomicBool,
        ancestor_started: tokio::sync::Notify,
        resume: tokio::sync::Notify,
    }
    fn raw_block(parent: [u8; 32], height: u32, nonce: u8) -> Vec<u8> {
        // Syntactically valid headers and coinbase-only blocks with no outputs
        // are sufficient here: the mock node represents consensus acceptance,
        // which is not this controller's job. No proof-of-work is asserted by
        // this fixture. The chain parser rejects blocks without transactions.
        let mut raw = 4u32.to_le_bytes().to_vec();
        raw.extend(parent);
        raw.extend([0; 64]);
        raw.extend(1_700_000_000u32.to_le_bytes());
        raw.extend(0x1f07ffffu32.to_le_bytes());
        raw.extend([nonce; 32]);
        raw.extend([0xfd, 0x40, 0x05]);
        raw.extend([0; 1344]);
        raw.push(1);
        // A height-zero coinbase must carry the real genesis script, and nothing
        // here reads coinbase heights, so genesis encodes height one.
        raw.extend(zakura_server_chain::testing::coinbase(height.max(1), &[]).bytes());
        raw
    }
    fn hash(raw: &[u8]) -> BlockHash {
        let block = zakura_server_chain::transparent::FetchedBlock::from_bytes(raw).unwrap();
        BlockHash::from_internal_bytes(block.hash())
    }
    fn extend(chain: &mut Vec<Vec<u8>>, count: usize, tag: u8) {
        for i in 0..count {
            let parent = chain
                .last()
                .map(|b| *hash(b).internal_bytes())
                .unwrap_or([0; 32]);
            chain.push(raw_block(parent, chain.len() as u32, tag + i as u8));
        }
    }
    async fn rpc(
        State(node): State<Arc<MockNode>>,
        Json(request): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        let height = || {
            request["params"][0]
                .as_u64()
                .or_else(|| request["params"][0].as_str().and_then(|s| s.parse().ok()))
                .unwrap() as usize
        };
        if request["method"] == "getblockhash"
            && height() == 3
            && node.pause_ancestor.swap(false, Ordering::AcqRel)
        {
            node.ancestor_started.notify_one();
            node.resume.notified().await;
        }
        let chain = node.chain.read().unwrap();
        let result = match request["method"].as_str().unwrap() {
            "getblockcount" => serde_json::json!(chain.len() - 1),
            "getblockhash" => serde_json::json!(hash(&chain[height()]).to_display_hex()),
            "getblock" => serde_json::json!(hex::encode(&chain[height()])),
            method => panic!("unexpected RPC {method}"),
        };
        Json(serde_json::json!({"id":request["id"],"result":result,"error":null}))
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn rpc_follow_publishes_empty_blocks_and_recovers_a_sealed_reorg() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let chain: Chain = Arc::new(RwLock::new(Vec::new()));
        extend(&mut chain.write().unwrap(), 5, 1);
        let node = Arc::new(MockNode {
            chain: chain.clone(),
            pause_ancestor: AtomicBool::new(false),
            ancestor_started: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route("/", post(rpc)).with_state(node.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let cookie = root.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let client = crate::ingest::zakura::connect(format!("http://{address}"), &cookie).unwrap();
        let journal_dir = root.path().join("journal");
        let mut journal = EventStore::open(
            &journal_dir,
            &hash(&chain.read().unwrap()[0]).to_display_hex(),
            0,
        )
        .unwrap();
        for h in 0..3 {
            journal
                .append_block(h, hash(&chain.read().unwrap()[h as usize]), &[])
                .unwrap();
        }
        journal.commit().unwrap();
        let initial = root.path().join("initial");
        let options = PublishOptions {
            data_dir: journal_dir.clone(),
            output: initial.clone(),
            previous: None,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: if cfg!(feature = "qualification-tests") {
                Some("decoupled-32k-32k".into())
            } else {
                None
            },
            recent_from: if cfg!(feature = "qualification-tests") {
                Some(2)
            } else {
                None
            },
            zakura_rpc_url: String::new(),
            zakura_cookie: cookie.clone(),
            through: None,
            record: None,
            source_sha: None,
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
        };
        #[cfg(feature = "qualification-tests")]
        publication::publish(&options, &journal, BlockHash::from_internal_bytes([0; 32])).unwrap();
        #[cfg(not(feature = "qualification-tests"))]
        publication::publish_with(
            &options,
            &journal,
            BlockHash::from_internal_bytes([0; 32]),
            &two_script_seal,
        )
        .unwrap();
        let hook = root.path().join("fleet.py");
        std::fs::write(&hook, r#"#!/usr/bin/env python3
import json,sys,time
from pathlib import Path
r=json.load(sys.stdin)
with Path(__file__).with_suffix('.requests').open('a') as log: log.write(json.dumps(r)+'\n')
if r['operation']=='prepare' and Path(__file__).with_suffix('.fail').exists():
    print(json.dumps({'ok':False,'error':'fixture preparation failed'})); sys.exit(0)
pause=Path(__file__).with_suffix('.pause')
if r['operation']=='prepare' and pause.exists():
    pause.with_suffix('.entered').touch()
    deadline=time.monotonic()+10
    while pause.exists():
        if time.monotonic()>deadline: raise RuntimeError('fixture preparation pause expired')
        time.sleep(.01)
print(json.dumps({'ok':True,'upstreams':[],'retained_publication':bool(r.get('retain_publication'))}))
"#).unwrap();
        std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o700)).unwrap();
        let publications = root.path().join("publications");
        std::fs::create_dir(&publications).unwrap();
        let config = Config {
            data_dir: journal_dir,
            publication_root: publications,
            initial_publication: initial.clone(),
            recent_from: 2,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: "decoupled-32k-32k".into(),
            rpc_url: format!("http://{address}"),
            rpc_cookie: cookie,
            fleet_command: hook,
            fleet_config: root.path().join("unused"),
            listen: address,
            source_sha: "fixture".into(),
            shadow: false,
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
            follow_journal: false,
            snapshot_dir: None,
            ship_runtimes: false,
            distribute_prepared: false,
            http_only: false,
        };
        let authority = Authority(Arc::new(Inner {
            cycle_duration: Default::default(),
            publication_freshness: Default::default(),
            telemetry: Default::default(),
            publication_attempts: AtomicU64::new(0),
            publication_failures: AtomicU64::new(0),
            last_publication: AtomicU64::new(0),
            active: RwLock::new(Arc::new(read_public(initial, Vec::new()).unwrap())),
            withdrawn: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            distribution_revision: AtomicU64::new(1),
            distribution_slots: Arc::new(tokio::sync::Semaphore::new(4)),
            canonicality: None,
            invalidated_from: AtomicU64::new(u64::MAX),
            publication_gate: std::sync::Mutex::new(()),
            last_init: std::sync::Mutex::new(None),
            status: RwLock::new(serde_json::json!({})),
            observed: RwLock::new(BTreeMap::new()),
            http: reqwest::Client::new(),
            membership: None,
            prepared_set: std::sync::Mutex::new(None),
            shutdown: Default::default(),
            checked_runtimes: Default::default(),
            runtime_preparations: AtomicU64::new(0),
            runtime_preparation_failures: AtomicU64::new(0),
            validation: Default::default(),
            integrity_failed: AtomicBool::new(false),
            fleet_withdrawal: AtomicBool::new(false),
            foreground: AtomicU64::new(0),
        }));
        let store = Arc::new(tokio::sync::Mutex::new(journal));
        let (notify, _) = tokio::sync::watch::channel(0);
        let mut input = HistoryInputTask {
            config: config.clone(),
            rpc: client.clone(),
            store: store.clone(),
            authority: authority.clone(),
            notify: notify.clone(),
            cache: OutputCache::new(100),
            pending_invalidation: None,
        };
        pir_pipeline::Task::run(&mut input).await.unwrap();
        let mut cache = input.cache;
        assert_eq!(store.lock().await.covered_through(), Some(4));
        let fail = config.fleet_command.with_extension("fail");
        std::fs::write(&fail, []).unwrap();
        assert!(publish_follow(&config, &client, &store, &authority)
            .await
            .is_err());
        assert_eq!(authority.0.active.read().unwrap().publication.height, 2);
        assert!(!config.publication_root.join("activation.json").exists());
        // Preparation failure cannot undo the independent source commit; a
        // restarted reader sees it before any successful publication.
        assert_eq!(
            EventStore::open_existing(&config.data_dir)
                .unwrap()
                .covered_through(),
            Some(4)
        );
        std::fs::remove_file(fail).unwrap();
        // A candidate observed by shared progress has no activation authority.
        let Input::Work(work) = acquire_history(&config, &client, &store, &authority)
            .await
            .unwrap()
        else {
            panic!("new durable work expected");
        };
        let Input::Work(ready) = prepare_follow(&config, &client, &authority, work)
            .await
            .unwrap()
        else {
            panic!("prepared candidate expected");
        };
        authority.0.epoch.fetch_add(1, Ordering::AcqRel);
        assert!(activate_history(&config, &client, &authority, ready)
            .await
            .unwrap_err()
            .to_string()
            .contains("invalidated before activation"));
        assert!(!config.publication_root.join("activation.json").exists());
        let requests =
            std::fs::read_to_string(config.fleet_command.with_extension("requests")).unwrap();
        assert!(requests
            .lines()
            .all(
                |line| serde_json::from_str::<serde_json::Value>(line).unwrap()["operation"]
                    != "activate"
            ));
        // Replacing journal-only blocks must cancel preparation without
        // withdrawing the still-canonical publication through height two.
        {
            let mut blocks = chain.write().unwrap();
            blocks.truncate(3);
            extend(&mut blocks, 2, 17);
        }
        ingest_once(&config, &client, &store, &authority, &mut cache, &notify)
            .await
            .unwrap();
        assert!(authority.0.epoch.load(Ordering::Acquire) > 0);
        assert!(
            !authority.0.withdrawn.load(Ordering::Acquire),
            "a journal-only fork must preserve canonical public coverage"
        );
        assert_eq!(authority.0.active.read().unwrap().publication.height, 2);
        // Hold the fleet preparation reply while ingestion discovers another
        // journal-only fork. Old public coverage stays available; the orphaned
        // candidate must fail its epoch check before any activation request.
        let pause = config.fleet_command.with_extension("pause");
        let entered = config.fleet_command.with_extension("entered");
        std::fs::write(&pause, []).unwrap();
        {
            let publication = publish_follow(&config, &client, &store, &authority);
            tokio::pin!(publication);
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    tokio::select! {
                        result = &mut publication => panic!("preparation did not pause: {result:?}"),
                        _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                            if entered.exists() { break; }
                        }
                    }
                }
            }).await.unwrap();
            {
                let mut blocks = chain.write().unwrap();
                blocks.truncate(3);
                extend(&mut blocks, 2, 18);
            }
            ingest_once(&config, &client, &store, &authority, &mut cache, &notify)
                .await
                .unwrap();
            let response = public_request(
                State(authority.clone()),
                Request::builder()
                    .uri("/v2/shards")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            // The orphaned preparation never replies. Cancellation must finish
            // before releasing the hook, while canonical old coverage survives.
            assert!(
                tokio::time::timeout(std::time::Duration::from_secs(2), &mut publication)
                    .await
                    .expect("invalidated preparation must not wait for the fleet reply")
                    .unwrap_err()
                    .to_string()
                    .contains("candidate invalidated while preparing")
            );
            std::fs::remove_file(&pause).unwrap();
            assert_eq!(authority.0.active.read().unwrap().publication.height, 2);
        }
        publish_follow(&config, &client, &store, &authority)
            .await
            .unwrap();
        assert_eq!(authority.0.active.read().unwrap().publication.height, 4);
        let old = authority
            .0
            .active
            .read()
            .unwrap()
            .publication
            .map_sha256
            .clone();
        // Same-height fork of the published tip.
        {
            let mut blocks = chain.write().unwrap();
            blocks.truncate(1);
            extend(&mut blocks, 4, 20);
        }
        {
            node.pause_ancestor.store(true, Ordering::Release);
            let ingest = ingest_once(&config, &client, &store, &authority, &mut cache, &notify);
            tokio::pin!(ingest);
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                tokio::select! {
                    result = &mut ingest => panic!("ancestor lookup was not paused: {result:?}"),
                    _ = node.ancestor_started.notified() => {}
                }
            })
            .await
            .unwrap();
            let response = public_request(
                State(authority.clone()),
                Request::builder()
                    .uri("/v2/shards")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "orphaned coverage must be withdrawn before a slow ancestor search"
            );
            node.resume.notify_one();
            ingest.await.unwrap();
        }
        assert!(authority.0.withdrawn.load(Ordering::Acquire));
        publish_follow(&config, &client, &store, &authority)
            .await
            .unwrap();
        assert!(!authority.0.withdrawn.load(Ordering::Acquire));
        {
            let active = authority.0.active.read().unwrap();
            assert_eq!(active.publication.height, 4);
            assert_ne!(active.publication.map_sha256, old);
            assert_eq!(
                active.publication.hash,
                hash(&chain.read().unwrap()[4]).to_display_hex()
            );
        }
        let genesis = store.lock().await.block_at(0).unwrap().block_hash;
        {
            let mut blocks = chain.write().unwrap();
            blocks.clear();
            extend(&mut blocks, 5, 40);
        }
        let error = ingest_once(&config, &client, &store, &authority, &mut cache, &notify)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("node genesis changed"));
        assert!(authority.0.withdrawn.load(Ordering::Acquire));
        assert_eq!(store.lock().await.block_at(0).unwrap().block_hash, genesis);
        server.abort();
    }

    /// With `ship_runtimes`, the real publication path prepares every
    /// runtime of each candidate beside its revisions and offers them in the
    /// fleet's prepare request; the next candidate links the unchanged ones
    /// from the active publication and builds only what changed.
    #[cfg(feature = "qualification-tests")]
    #[tokio::test(flavor = "multi_thread")]
    async fn shipped_runtimes_are_prepared_linked_and_offered_with_the_candidate() {
        shipped_runtime_scenario(false).await;
    }

    /// Source-only candidates still exercise invalidation, verification against
    /// previously verified files, and a counted runtime write failure.
    #[cfg(feature = "qualification-tests")]
    #[tokio::test(flavor = "multi_thread")]
    async fn shipped_runtime_preparation_refuses_invalid_candidates_and_reports_write_failure() {
        shipped_runtime_scenario(true).await;
    }

    // Success exercises cold preparation and handover with both supported
    // geometries. Refusals need the same verified source files, but no prior
    // cryptographic build: keep that independent work in its own bounded case.
    #[cfg(feature = "qualification-tests")]
    async fn shipped_runtime_scenario(failures_only: bool) {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let root = tempfile::tempdir().unwrap();
        let chain: Chain = Arc::new(RwLock::new(Vec::new()));
        extend(&mut chain.write().unwrap(), 5, 1);
        let node = Arc::new(MockNode {
            chain: chain.clone(),
            pause_ancestor: AtomicBool::new(false),
            ancestor_started: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route("/", post(rpc)).with_state(node.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let cookie = root.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let client = crate::ingest::zakura::connect(format!("http://{address}"), &cookie).unwrap();
        let journal_dir = root.path().join("journal");
        let mut journal = EventStore::open(
            &journal_dir,
            &hash(&chain.read().unwrap()[0]).to_display_hex(),
            0,
        )
        .unwrap();
        for h in 0..3 {
            journal
                .append_block(h, hash(&chain.read().unwrap()[h as usize]), &[])
                .unwrap();
        }
        journal.commit().unwrap();
        let initial = root.path().join("initial");
        let options = PublishOptions {
            data_dir: journal_dir.clone(),
            output: initial.clone(),
            previous: None,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: Some("decoupled-32k-32k".into()),
            recent_from: Some(2),
            zakura_rpc_url: String::new(),
            zakura_cookie: cookie.clone(),
            through: None,
            record: None,
            source_sha: None,
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
        };
        publication::publish(&options, &journal, BlockHash::from_internal_bytes([0; 32])).unwrap();
        // The fleet records every request it is sent.
        let hook = root.path().join("fleet.py");
        std::fs::write(&hook, r#"#!/usr/bin/env python3
import json,sys
from pathlib import Path
r=json.load(sys.stdin)
with Path(__file__).with_suffix('.requests').open('a') as log: log.write(json.dumps(r)+'\n')
print(json.dumps({'ok':True,'upstreams':[],'workers':{},'retained_publication':bool(r.get('retain_publication'))}))
"#).unwrap();
        std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o700)).unwrap();
        let requests = || -> Vec<serde_json::Value> {
            std::fs::read_to_string(hook.with_extension("requests"))
                .unwrap_or_default()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect()
        };
        let publications = root.path().join("publications");
        std::fs::create_dir(&publications).unwrap();
        let config = Config {
            data_dir: journal_dir,
            publication_root: publications,
            initial_publication: initial.clone(),
            recent_from: 2,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: "decoupled-32k-32k".into(),
            rpc_url: format!("http://{address}"),
            rpc_cookie: cookie,
            fleet_command: hook.clone(),
            fleet_config: root.path().join("unused"),
            listen: address,
            source_sha: "fixture".into(),
            shadow: false,
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
            follow_journal: false,
            snapshot_dir: None,
            ship_runtimes: true,
            distribute_prepared: false,
            http_only: false,
        };
        let authority = Authority(Arc::new(Inner {
            cycle_duration: Default::default(),
            publication_freshness: Default::default(),
            telemetry: Default::default(),
            publication_attempts: AtomicU64::new(0),
            publication_failures: AtomicU64::new(0),
            last_publication: AtomicU64::new(0),
            active: RwLock::new(Arc::new(read_public(initial, Vec::new()).unwrap())),
            withdrawn: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            distribution_revision: AtomicU64::new(1),
            distribution_slots: Arc::new(tokio::sync::Semaphore::new(4)),
            canonicality: None,
            invalidated_from: AtomicU64::new(u64::MAX),
            publication_gate: std::sync::Mutex::new(()),
            last_init: std::sync::Mutex::new(None),
            status: RwLock::new(serde_json::json!({})),
            observed: RwLock::new(BTreeMap::new()),
            http: reqwest::Client::new(),
            membership: None,
            prepared_set: std::sync::Mutex::new(None),
            shutdown: Default::default(),
            checked_runtimes: Default::default(),
            runtime_preparations: AtomicU64::new(0),
            runtime_preparation_failures: AtomicU64::new(0),
            validation: Default::default(),
            integrity_failed: AtomicBool::new(false),
            fleet_withdrawal: AtomicBool::new(false),
            foreground: AtomicU64::new(0),
        }));
        let store = Arc::new(tokio::sync::Mutex::new(journal));
        let (notify, _) = tokio::sync::watch::channel(0);
        let mut cache = OutputCache::new(100);
        let runtime_files = |dir: &Path| -> BTreeMap<String, (u64, u64)> {
            std::fs::read_dir(dir)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|e| e.file_name().to_string_lossy().ends_with(".runtime"))
                .map(|e| {
                    let meta = e.metadata().unwrap();
                    (
                        e.file_name().to_string_lossy().into_owned(),
                        (meta.dev(), meta.ino()),
                    )
                })
                .collect()
        };

        let source = if failures_only {
            authority.0.active.read().unwrap().publication.clone()
        } else {
            ingest_once(&config, &client, &store, &authority, &mut cache, &notify)
                .await
                .unwrap();
            publish_once(&config, &client, &store, &authority)
                .await
                .unwrap();
            let first = authority.0.active.read().unwrap().publication.clone();
            assert_eq!(first.height, 4);
            let offered = requests();
            let prepare = offered
                .iter()
                .find(|r| r["operation"] == "prepare")
                .expect("a prepare request");
            assert_eq!(prepare["runtimes"], serde_json::json!(first.directory));
            let set =
                transparent_shared::history::shardset::ShardSet::open(&first.directory, 3).unwrap();
            let files = runtime_files(&first.directory);
            assert_eq!(
                files.len(),
                set.warm_targets().len(),
                "one file per runtime"
            );
            let cycle = authority.0.status.read().unwrap()["last_cycle"].clone();
            let report = &cycle["runtime_preparation"];
            assert_eq!(report["built"], files.len(), "{cycle}");
            assert_eq!(report["failures"], serde_json::json!([]));
            assert!(cycle["activated_unix"].as_f64() >= cycle["prepared_unix"].as_f64());
            assert!(cycle["prepared_unix"].as_f64() >= cycle["published_unix"].as_f64());

            // The next block: the sealed archive shard's runtime is the same
            // file, linked; only the changed tail is built.
            extend(&mut chain.write().unwrap(), 1, 60);
            ingest_once(&config, &client, &store, &authority, &mut cache, &notify)
                .await
                .unwrap();
            publish_once(&config, &client, &store, &authority)
                .await
                .unwrap();
            let second = authority.0.active.read().unwrap().publication.clone();
            assert_eq!(second.height, 5);
            let report = authority.0.status.read().unwrap()["runtime_preparation"].clone();
            assert!(report["linked"].as_u64().unwrap() >= 1, "{report}");
            assert!(report["built"].as_u64().unwrap() >= 1, "{report}");
            let (before, after) = (files, runtime_files(&second.directory));
            let shared: Vec<_> = before
                .iter()
                .filter(|(name, _)| after.contains_key(*name))
                .collect();
            assert!(!shared.is_empty());
            for (name, identity) in shared {
                assert_eq!(&after[name], identity, "{name} is the same file");
            }
            assert_eq!(authority.0.runtime_preparations.load(Ordering::Relaxed), 2);

            server.abort();
            return;
        };

        // A preparation for an epoch an invalidation has since moved past
        // stops before its first segment, and nothing is offered.
        // Candidate inputs are immutable. Link them into a private tree and
        // omit runtime caches, rather than copying large files just to delete
        // them. Fault injection below unlinks a table before changing it.
        fn candidate_inputs(source: &Path, target: &Path) {
            std::fs::create_dir(target).unwrap();
            for entry in std::fs::read_dir(source).unwrap() {
                let entry = entry.unwrap();
                let kind = entry.file_type().unwrap();
                if kind.is_dir() {
                    candidate_inputs(&entry.path(), &target.join(entry.file_name()));
                } else {
                    assert!(kind.is_file());
                    if entry.path().extension().is_none_or(|ext| ext != "runtime") {
                        std::fs::hard_link(entry.path(), target.join(entry.file_name())).unwrap();
                    }
                }
            }
        }
        let candidate = config.publication_root.join("candidate-superseded");
        candidate_inputs(&source.directory, &candidate);
        for name in runtime_files(&candidate).keys() {
            std::fs::remove_file(candidate.join(name)).unwrap();
        }
        let sent = requests().len();
        let stale = authority.0.epoch.load(Ordering::Acquire) + 1;
        let error = prepare_runtimes(&config, &authority, &candidate, stale)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("invalidated"), "{error}");
        assert!(runtime_files(&candidate).is_empty());
        assert_eq!(requests().len(), sent);
        assert!(authority.0.prepared_set.lock().unwrap().is_some());

        // A candidate whose table does not verify is not published.
        let epoch = authority.0.epoch.load(Ordering::Acquire);
        // Replacing a linked directory table makes a new file that the last
        // verified set does not vouch for.
        let candidate = config.publication_root.join("candidate-corrupt");
        candidate_inputs(&source.directory, &candidate);
        let mut corrupted = 0;
        for entry in std::fs::read_dir(&candidate)
            .unwrap()
            .filter_map(Result::ok)
        {
            let table = entry.path().join("directory.0.bin");
            if !table.is_file() {
                continue;
            }
            let mut bytes = std::fs::read(&table).unwrap();
            bytes[0] ^= 1;
            std::fs::remove_file(&table).unwrap();
            std::fs::write(&table, bytes).unwrap();
            let original = source
                .directory
                .join(entry.file_name())
                .join("directory.0.bin");
            assert_ne!(
                std::fs::metadata(&table).unwrap().ino(),
                std::fs::metadata(original).unwrap().ino()
            );
            corrupted += 1;
        }
        assert!(corrupted > 0);
        let error = prepare_runtimes(&config, &authority, &candidate, epoch)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("did not verify"), "{error}");

        // A runtime that cannot be written is reported, not fatal: the
        // candidate is still offered and workers build it.
        let unwritable = config.publication_root.join("candidate-unwritable");
        // Permission failure is independent of archive size. Publish a real
        // recent-only candidate so this negative case does not rebuild the
        // large archive runtimes already checked above.
        let mut recent_only = options;
        recent_only.output = unwritable.clone();
        recent_only.archive_geometry = None;
        recent_only.recent_from = None;
        publication::publish(
            &recent_only,
            &*store.lock().await,
            BlockHash::from_internal_bytes([0; 32]),
        )
        .unwrap();
        for name in runtime_files(&unwritable).keys() {
            std::fs::remove_file(unwritable.join(name)).unwrap();
        }
        // The refusal fixture has never built or retained any runtime.
        for retained in [&config.initial_publication, &source.directory] {
            for name in runtime_files(retained).keys() {
                std::fs::remove_file(retained.join(name)).unwrap();
            }
        }
        std::fs::set_permissions(&unwritable, std::fs::Permissions::from_mode(0o555)).unwrap();
        let report = prepare_runtimes(&config, &authority, &unwritable, epoch)
            .await
            .unwrap();
        std::fs::set_permissions(&unwritable, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            !report["failures"].as_array().unwrap().is_empty(),
            "{report}"
        );
        assert_eq!(report["runtimes"], 0);
        assert_eq!(
            authority
                .0
                .runtime_preparation_failures
                .load(Ordering::Relaxed),
            report["failures"].as_array().unwrap().len() as u64
        );
        server.abort();
    }

    /// A five-block node whose journal and publication end at height two, and
    /// a fleet hook that logs every request and, while `fleet.pause` exists,
    /// holds an activation after touching `fleet.entered`.
    struct Follow {
        _root: tempfile::TempDir,
        server: tokio::task::JoinHandle<()>,
        client: NodeClient,
        config: Config,
        store: Arc<tokio::sync::Mutex<EventStore>>,
    }
    async fn follow_fixture() -> Follow {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let chain: Chain = Arc::new(RwLock::new(Vec::new()));
        extend(&mut chain.write().unwrap(), 5, 1);
        let node = Arc::new(MockNode {
            chain: chain.clone(),
            pause_ancestor: AtomicBool::new(false),
            ancestor_started: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().route("/", post(rpc)).with_state(node);
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let cookie = root.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let client = crate::ingest::zakura::connect(format!("http://{address}"), &cookie).unwrap();
        let journal_dir = root.path().join("journal");
        let mut journal = EventStore::open(
            &journal_dir,
            &hash(&chain.read().unwrap()[0]).to_display_hex(),
            0,
        )
        .unwrap();
        for h in 0..3 {
            journal
                .append_block(h, hash(&chain.read().unwrap()[h as usize]), &[])
                .unwrap();
        }
        journal.commit().unwrap();
        let initial = root.path().join("initial");
        let qualification = cfg!(feature = "qualification-tests");
        let options = PublishOptions {
            data_dir: journal_dir.clone(),
            output: initial.clone(),
            previous: None,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: qualification.then(|| "decoupled-32k-32k".into()),
            recent_from: qualification.then_some(2),
            zakura_rpc_url: String::new(),
            zakura_cookie: cookie.clone(),
            through: None,
            record: None,
            source_sha: None,
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
        };
        publication::publish_with(
            &options,
            &journal,
            BlockHash::from_internal_bytes([0; 32]),
            &follow_seal,
        )
        .unwrap();
        let hook = root.path().join("fleet.py");
        std::fs::write(&hook, r#"#!/usr/bin/env python3
import json,sys,time
from pathlib import Path
r=json.load(sys.stdin)
with Path(__file__).with_suffix('.requests').open('a') as log: log.write(json.dumps(r)+'\n')
pause=Path(__file__).with_suffix('.pause')
if r['operation']=='activate' and pause.exists():
    pause.with_suffix('.entered').touch()
    deadline=time.monotonic()+60
    while pause.exists():
        if time.monotonic()>deadline: raise RuntimeError('fixture activation pause expired')
        time.sleep(.01)
print(json.dumps({'ok':True,'upstreams':[],'retained_publication':bool(r.get('retain_publication'))}))
"#).unwrap();
        std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o700)).unwrap();
        let publications = root.path().join("publications");
        std::fs::create_dir(&publications).unwrap();
        let config = Config {
            data_dir: journal_dir,
            publication_root: publications,
            initial_publication: initial,
            recent_from: 2,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: "decoupled-32k-32k".into(),
            rpc_url: format!("http://{address}"),
            rpc_cookie: cookie,
            fleet_command: hook,
            fleet_config: root.path().join("unused"),
            listen: address,
            source_sha: "fixture".into(),
            shadow: false,
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
            follow_journal: false,
            snapshot_dir: None,
            ship_runtimes: false,
            distribute_prepared: false,
            http_only: false,
        };
        let store = Arc::new(tokio::sync::Mutex::new(journal));
        Follow {
            _root: root,
            server,
            client,
            config,
            store,
        }
    }
    /// The seal policy [`publish_follow`] publishes under.
    fn follow_seal(geometry: &Geometry) -> DecoupledPolicy {
        #[cfg(not(feature = "qualification-tests"))]
        {
            two_script_seal(geometry)
        }
        #[cfg(feature = "qualification-tests")]
        {
            DecoupledPolicy::for_geometry(geometry)
        }
    }
    impl Follow {
        /// The authority a publisher starting now would hold: the publication
        /// `active.json` names, checked against its map as at startup, or the
        /// initial one.
        fn authority(&self, shutdown: pir_pipeline::service::Shutdown) -> Authority {
            let saved = self.config.publication_root.join("active.json");
            let public = if saved.exists() {
                let saved: ActivePublication =
                    serde_json::from_slice(&std::fs::read(saved).unwrap()).unwrap();
                let loaded = read_public(saved.directory, saved.upstreams).unwrap();
                assert_eq!(loaded.publication.map_sha256, saved.map_sha256);
                loaded
            } else {
                read_public(self.config.initial_publication.clone(), Vec::new()).unwrap()
            };
            Authority(Arc::new(Inner {
                cycle_duration: Default::default(),
                publication_freshness: Default::default(),
                telemetry: Default::default(),
                publication_attempts: AtomicU64::new(0),
                publication_failures: AtomicU64::new(0),
                last_publication: AtomicU64::new(0),
                active: RwLock::new(Arc::new(public)),
                withdrawn: AtomicBool::new(false),
                epoch: AtomicU64::new(0),
                distribution_revision: AtomicU64::new(1),
                distribution_slots: Arc::new(tokio::sync::Semaphore::new(4)),
                canonicality: None,
                invalidated_from: AtomicU64::new(u64::MAX),
                publication_gate: std::sync::Mutex::new(()),
                last_init: std::sync::Mutex::new(None),
                status: RwLock::new(serde_json::json!({})),
                observed: RwLock::new(BTreeMap::new()),
                http: reqwest::Client::new(),
                membership: None,
                prepared_set: std::sync::Mutex::new(None),
                shutdown,
                checked_runtimes: Default::default(),
                runtime_preparations: AtomicU64::new(0),
                runtime_preparation_failures: AtomicU64::new(0),
                validation: Default::default(),
                integrity_failed: AtomicBool::new(false),
                fleet_withdrawal: AtomicBool::new(false),
                foreground: AtomicU64::new(0),
            }))
        }
        /// Brings the journal to the node's tip, height four.
        async fn ingest(&self, authority: &Authority) {
            let (notify, _) = tokio::sync::watch::channel(0);
            ingest_once(
                &self.config,
                &self.client,
                &self.store,
                authority,
                &mut OutputCache::new(100),
                &notify,
            )
            .await
            .unwrap();
            assert_eq!(self.store.lock().await.covered_through(), Some(4));
        }
        /// The operations of every fleet request so far.
        fn operations(&self) -> Vec<String> {
            std::fs::read_to_string(self.config.fleet_command.with_extension("requests"))
                .unwrap_or_default()
                .lines()
                .map(|line| {
                    let request: serde_json::Value = serde_json::from_str(line).unwrap();
                    request["operation"].as_str().unwrap().to_string()
                })
                .collect()
        }
    }

    /// A stop requested while the candidate is being published ends its
    /// preparation as a normal outcome: the build writes no map, the fleet is
    /// not asked, nothing is activated and no failure is counted. A
    /// publisher started afterwards prepares and activates the same height.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stop_during_the_candidate_build_is_unchanged_and_a_restart_resumes() {
        static STOPPING: std::sync::LazyLock<pir_pipeline::service::Shutdown> =
            std::sync::LazyLock::new(Default::default);
        // Asked as the build starts, after preparation checked for a stop.
        fn stopping_seal(geometry: &Geometry) -> DecoupledPolicy {
            STOPPING.request();
            follow_seal(geometry)
        }
        let follow = follow_fixture().await;
        let config = &follow.config;
        let authority = follow.authority(STOPPING.clone());
        follow.ingest(&authority).await;
        let Input::Work(work) = acquire_history(config, &follow.client, &follow.store, &authority)
            .await
            .unwrap()
        else {
            panic!("new durable work expected");
        };
        let qualification = cfg!(feature = "qualification-tests");
        let prepared = prepare_candidate(
            config,
            &follow.client,
            &authority,
            work,
            stopping_seal,
            qualification.then(|| config.archive_geometry.clone()),
            qualification.then_some(config.recent_from),
        )
        .await;
        assert!(STOPPING.requested());
        assert!(
            matches!(prepared, Ok(Input::Unchanged)),
            "stopped as a normal outcome: {:?}",
            prepared.err()
        );
        assert_eq!(authority.0.active.read().unwrap().publication.height, 2);
        assert_eq!(authority.0.publication_failures.load(Ordering::Relaxed), 0);
        assert!(follow.operations().is_empty(), "the fleet was not asked");
        for name in ["active.json", "activation.json"] {
            assert!(!config.publication_root.join(name).exists(), "{name}");
        }
        let candidates: Vec<_> = std::fs::read_dir(&config.publication_root)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.is_dir())
            .collect();
        assert_eq!(candidates.len(), 1, "{candidates:?}");
        assert!(
            !candidates[0].join("shards.json").exists(),
            "no map written"
        );

        let restarted = follow.authority(Default::default());
        publish_follow(config, &follow.client, &follow.store, &restarted)
            .await
            .unwrap();
        assert_eq!(restarted.0.active.read().unwrap().publication.height, 4);
        assert_eq!(follow.operations(), ["prepare", "activate"]);
        follow.server.abort();
    }

    /// A stop requested once fleet activation has begun does not abandon the
    /// cycle: it completes the post-activation canonicality check and the
    /// pointer commit, counts no failure, sends no further fleet request,
    /// and a publisher started afterwards loads the same active record.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_stop_during_fleet_activation_still_commits_the_publication() {
        let follow = follow_fixture().await;
        let config = &follow.config;
        let shutdown = pir_pipeline::service::Shutdown::default();
        let authority = follow.authority(shutdown.clone());
        follow.ingest(&authority).await;
        let pause = config.fleet_command.with_extension("pause");
        let entered = config.fleet_command.with_extension("entered");
        std::fs::write(&pause, []).unwrap();
        let publication = publish_follow(config, &follow.client, &follow.store, &authority);
        tokio::pin!(publication);
        loop {
            tokio::select! {
                result = &mut publication => panic!("activation did not pause: {result:?}"),
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                    if entered.exists() { break; }
                }
            }
        }
        shutdown.request();
        std::fs::remove_file(&pause).unwrap();
        publication.await.unwrap();

        let active = authority.0.active.read().unwrap().publication.clone();
        assert_eq!(active.height, 4);
        let saved: ActivePublication = serde_json::from_slice(
            &std::fs::read(config.publication_root.join("active.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            (&saved.directory, &saved.map_sha256, saved.height),
            (&active.directory, &active.map_sha256, active.height)
        );
        assert!(!config.publication_root.join("activation.json").exists());
        assert!(!authority.0.withdrawn.load(Ordering::Acquire));
        assert_eq!(authority.0.publication_failures.load(Ordering::Relaxed), 0);
        {
            let status = authority.0.status.read().unwrap();
            assert_eq!(status["phase"], "serving");
            assert_eq!(status["public_height"], 4);
            assert!(status.get("publication_error").is_none());
        }
        // Nothing is left to retry.
        publish_follow(config, &follow.client, &follow.store, &authority)
            .await
            .unwrap();
        assert_eq!(follow.operations(), ["prepare", "activate"]);

        let restarted = follow.authority(Default::default());
        assert_eq!(
            restarted.0.active.read().unwrap().publication.map_sha256,
            active.map_sha256
        );
        follow.server.abort();
    }
}

#[cfg(all(test, feature = "fixtures"))]
#[path = "../../../../pir-publisher/tests/included/controller_distribution_tests.rs"]
pub(crate) mod distribution_tests;
