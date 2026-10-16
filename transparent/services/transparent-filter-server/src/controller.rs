//! Continuous, chain-checked publication. Ingest owns the journal; builders use
//! copied suffix snapshots and activation is delegated to the private fleet tool.
use crate::{
    events::{BlockEntry, EventStore, EventStoreError, Refreshed},
    ingest::build_fetched_block_events,
    prevout::OutputCache,
    publication::{self, BoxError, Journal, PublishOptions},
    shard_filters::ShardFilters,
    zakura::NodeClient,
};
use axum::{
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Router,
};
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
use zakura_transparent_private_shared::events::TransparentEvent;
use zakura_transparent_private_shared::filter::{BlockHash, ScriptBytes, ShardMap};

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
    pub fleet_command: PathBuf,
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
}

impl Config {
    fn snapshot_dir(&self) -> &Path {
        self.snapshot_dir
            .as_deref()
            .unwrap_or(&self.publication_root)
    }
}

fn default_range_profile() -> String {
    zakura_transparent_private_shared::filter::RANGE_PROFILE.to_string()
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
    prepared_set: std::sync::Mutex<Option<transparent_server_backend::shardset::ShardSet>>,
    /// Runtime preparation outcomes since this process started.
    runtime_preparations: AtomicU64,
    runtime_preparation_failures: AtomicU64,
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
                pir_observability::observe,
            ))
            .with_state(self.clone())
    }
}

/// Snapshot only rebuilt event rows plus the endpoint hashes used to establish
/// reuse. It stays valid even if ingest truncates and rewrites its journal.
pub struct Snapshot {
    journal_version: u16,
    genesis: String,
    first: u64,
    end: u64,
    count: u64,
    blocks: BTreeMap<u64, BlockEntry>,
    events: std::sync::Mutex<std::fs::File>,
    events_offset: u64,
    events_first: u64,
    display_dir: PathBuf,
}
impl Snapshot {
    /// Copies the journal suffix `previous` does not cover as sealed into an
    /// anonymous file under `temp_dir`.
    pub fn capture(
        store: &EventStore,
        previous: &ShardMap,
        temp_dir: &Path,
    ) -> Result<Self, BoxError> {
        let end = store.covered_through().ok_or("empty journal")?;
        let mut first = store.start_height();
        let mut blocks = BTreeMap::new();
        for entry in &previous.shards {
            if let Some(block) = store.block_at(entry.end_height) {
                blocks.insert(entry.end_height, block);
            }
        }
        for entry in &previous.shards {
            if !entry.sealed
                || entry.end_height > end
                || blocks
                    .get(&entry.end_height)
                    .is_none_or(|b| b.block_hash.to_display_hex() != entry.terminal_block_hash)
            {
                break;
            }
            first = entry.end_height + 1;
        }
        blocks.insert(end, store.block_at(end).ok_or("snapshot endpoint missing")?);
        let (events, events_offset) = store.snapshot_suffix(first, temp_dir)?;
        for height in first..=end {
            blocks.insert(
                height,
                store.block_at(height).ok_or("snapshot block missing")?,
            );
        }
        // A followed journal's writer may have rolled back and rewritten the
        // suffix while it was copied. An unchanged endpoint means it did not.
        if !store.still_committed(end, blocks[&end])? {
            return Err("the journal changed while its suffix was copied".into());
        }
        Ok(Self {
            journal_version: store.version(),
            genesis: store.genesis_hash().into(),
            first: store.start_height(),
            end,
            count: store.events_stored(),
            blocks,
            events: std::sync::Mutex::new(events),
            events_offset,
            events_first: first,
            display_dir: store.directory().to_path_buf(),
        })
    }
}
impl Journal for Snapshot {
    fn journal_version(&self) -> u16 {
        self.journal_version
    }
    fn display_at(
        &self,
        height: u64,
    ) -> Result<Vec<zakura_transparent_private_server::shard::txid::DisplayRecord>, EventStoreError>
    {
        let block = self
            .blocks
            .get(&height)
            .ok_or_else(|| EventStoreError::Invariant("snapshot display block missing".into()))?;
        let sources = crate::display_journal::read_sources(&self.display_dir, block.block_hash)?;
        crate::display_journal::validate_events(
            &sources,
            &self.events_at(height)?.ok_or_else(|| {
                EventStoreError::Invariant("snapshot display events missing".into())
            })?,
        )?;
        sources
            .iter()
            .map(|source| {
                source
                    .display_record()
                    .map_err(|e| EventStoreError::Invariant(e.to_string()))
            })
            .collect()
    }
    fn genesis_hash(&self) -> &str {
        &self.genesis
    }
    fn start_height(&self) -> u64 {
        self.first
    }
    fn covered_through(&self) -> Option<u64> {
        Some(self.end)
    }
    fn events_stored(&self) -> u64 {
        self.count
    }
    fn block_at(&self, h: u64) -> Option<BlockEntry> {
        self.blocks.get(&h).copied()
    }
    fn events_at(
        &self,
        h: u64,
    ) -> Result<Option<Vec<(ScriptBytes, TransparentEvent)>>, EventStoreError> {
        if h < self.events_first {
            return Ok(None);
        }
        let Some(mut entry) = self.blocks.get(&h).copied() else {
            return Ok(None);
        };
        entry.offset = entry
            .offset
            .checked_sub(self.events_offset)
            .ok_or_else(|| EventStoreError::Invariant("snapshot offset precedes suffix".into()))?;
        let mut file = self.events.lock().unwrap();
        let end = self
            .blocks
            .get(&(h + 1))
            .map_or(file.metadata()?.len(), |next| {
                next.offset - self.events_offset
            });
        let mut events = crate::events::read_event_record(&mut file, entry, end)?;
        if let Some(events) = &mut events {
            events.extend(crate::display_journal::oversized_events(
                &self.display_dir,
                entry.block_hash,
            )?);
        }
        Ok(events)
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
    })
}

pub async fn run(config: Config) -> Result<(), BoxError> {
    std::fs::create_dir_all(&config.publication_root)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(config.publication_root.join("controller.lock"))?;
    lock.try_lock()
        .map_err(|e| format!("another publication controller owns this root: {e}"))?;
    let rpc = crate::zakura::connect(&config.rpc_url, &config.rpc_cookie)?;
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
        publication_gate: std::sync::Mutex::new(()),
        last_init: std::sync::Mutex::new(None),
        status: RwLock::new(serde_json::json!({"phase":"starting"})),
        observed: RwLock::new(BTreeMap::new()),
        http: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()?,
        membership: membership_path(&config.fleet_config),
        prepared_set: std::sync::Mutex::new(None),
        runtime_preparations: AtomicU64::new(0),
        runtime_preparation_failures: AtomicU64::new(0),
    }));
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    let app = authority.router();
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::error!(%error,"publication HTTP service failed");
            std::process::exit(1);
        }
    });
    let (notify, mut updates) = tokio::sync::watch::channel(0u64);
    let ingest_config = config.clone();
    let ingest_authority = authority.clone();
    let ingest_store = store.clone();
    let ingest_rpc = rpc.clone();
    tokio::spawn(async move {
        let mut cache = OutputCache::new(crate::prevout::DEFAULT_CACHE_OUTPUTS);
        let mut pending_invalidation = None;
        loop {
            if ingest_config.follow_journal {
                if let Err(error) = follow_once(
                    &ingest_config,
                    &ingest_rpc,
                    &ingest_store,
                    &ingest_authority,
                    &mut pending_invalidation,
                    &notify,
                )
                .await
                {
                    if error.downcast_ref::<EventStoreError>().is_some() {
                        tracing::error!(%error,"journal read failed; restart from its checkpoint");
                        std::process::exit(1);
                    }
                    tracing::error!(%error,"journal follow failed; retrying without advancing coverage");
                    ingest_authority.0.status.write().unwrap()["ingest_error"] =
                        error.to_string().into();
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
            if let Err(error) = ingest_once(
                &ingest_config,
                &ingest_rpc,
                &ingest_store,
                &ingest_authority,
                &mut cache,
                &notify,
            )
            .await
            {
                if error.downcast_ref::<EventStoreError>().is_some() {
                    tracing::error!(%error,"journal write failed; restart from durable checkpoint");
                    std::process::exit(1);
                }
                tracing::error!(%error,"chain follow failed; retrying without advancing coverage");
                ingest_authority.0.status.write().unwrap()["ingest_error"] =
                    error.to_string().into();
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });
    loop {
        if let Err(error) = publish_once(&config, &rpc, &store, &authority).await {
            authority
                .0
                .publication_failures
                .fetch_add(1, Ordering::Relaxed);
            tracing::error!(%error,"publication failed; keeping valid coverage and retrying");
            authority.0.status.write().unwrap()["publication_error"] = error.to_string().into();
        }
        let public_height = authority.0.active.read().unwrap().publication.height;
        let oldest = authority
            .0
            .observed
            .read()
            .unwrap()
            .range((public_height.saturating_add(1))..)
            .map(|(_, t)| t.elapsed().as_secs())
            .max()
            .unwrap_or(0);
        if oldest > 30 {
            tracing::error!(
                alert = "transparent_publication_stale",
                oldest_unpublished_seconds = oldest,
                "publication freshness target exceeded"
            );
        }
        if let Err(error) = collect_candidates(&config, &authority) {
            tracing::warn!(%error,"candidate collection failed");
        }
        tokio::select! { _=updates.changed()=>{}, _=tokio::time::sleep(std::time::Duration::from_secs(1))=>{} }
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
        }
        authority.0.epoch.fetch_add(1, Ordering::AcqRel);
        publication::write_atomic(
            &config.publication_root.join("withdrawn.json"),
            &serde_json::to_vec(&serde_json::json!({"from_height":from,"acknowledged":false}))?,
        )?;
        retain
    };
    if !config.shadow {
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
        *cache = OutputCache::new(crate::prevout::DEFAULT_CACHE_OUTPUTS);
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

async fn publish_once(
    config: &Config,
    rpc: &NodeClient,
    store: &Arc<tokio::sync::Mutex<EventStore>>,
    authority: &Authority,
) -> Result<(), BoxError> {
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
            return Ok(());
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
    let height = snapshot.end;
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
        && !authority.0.withdrawn.load(Ordering::Acquire)
    {
        return Ok(());
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
        return Ok(());
    }
    let started = std::time::Instant::now();
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
            archive_geometry: Some(config.archive_geometry.clone()),
            recent_from: Some(config.recent_from),
            zakura_rpc_url: config.rpc_url.clone(),
            zakura_cookie: config.rpc_cookie.clone(),
            through: Some(height),
            record: Some(directory.join("publication.json")),
            source_sha: Some(config.source_sha.clone()),
            directory_choice: config.directory_choice,
            range_profile: config.range_profile.clone(),
        };
        tokio::task::spawn_blocking(move || {
            publication::publish(&options, &snapshot, BlockHash::from_internal_bytes([0; 32]))
        })
        .await??;
    }
    let mut candidate = read_public(directory.clone(), Vec::new())?;
    authority.0.status.write().unwrap()["candidate_height"] = height.into();
    let published_unix = unix_now();
    let runtimes = if config.ship_runtimes {
        Some(prepare_runtimes(config, authority, &directory, epoch).await?)
    } else {
        None
    };
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
    let prepared = tokio::select! {
        biased;
        _ = invalidated => return Err("candidate invalidated while preparing".into()),
        result = fleet(config, request) => result?,
    };
    if authority.0.epoch.load(Ordering::Acquire) != epoch
        || rpc.block_hash_display(height).await? != hash
    {
        return Err("candidate invalidated while preparing".into());
    }
    let prepared_unix = unix_now();
    if config.shadow {
        authority.0.status.write().unwrap()["phase"] = "shadow_verified".into();
        authority.0.status.write().unwrap()["shadow_hash"] = hash.into();
        authority.0.status.write().unwrap()["build_seconds"] =
            started.elapsed().as_secs_f64().into();
        return Ok(());
    }
    publication::write_atomic(
        &config.publication_root.join("activation.json"),
        &serde_json::to_vec(&candidate.publication)?,
    )?;
    let activated=fleet(config,serde_json::json!({"operation":"activate","directory":directory,"map_sha256":candidate.publication.map_sha256,"prepared":prepared})).await?;
    if authority.0.epoch.load(Ordering::Acquire) != epoch
        || rpc.block_hash_display(height).await? != hash
    {
        authority.0.withdrawn.store(true, Ordering::Release);
        // The ingest loop invalidates the exact changed suffix; until then the
        // fleet hook withdraws routing entirely.
        fleet(config, serde_json::json!({"operation":"withdraw"})).await?;
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
        *authority.0.active.write().unwrap() = Arc::new(candidate);
        authority.0.withdrawn.store(false, Ordering::Release);
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
    use transparent_server_backend::shardset::{LoadOptions, ShardSet};
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
        move || authority.0.epoch.load(Ordering::Acquire) != epoch
    };
    let directory = directory.to_path_buf();
    let (previous, prepared) = tokio::task::spawn_blocking(move || {
        // Every revision the publisher wrote; it retains them all, and so
        // may a worker.
        let options = LoadOptions {
            prune_excess: true,
            ..LoadOptions::whole(transparent_server_backend::shardset::DEFAULT_RETAIN_REVISIONS)
        };
        match ShardSet::open_reusing(&directory, &options, previous.as_ref()) {
            Ok(set) => {
                let prepared = transparent_server_backend::prebuild::prepare_publication(
                    &set, &directory, &retained, &stop,
                );
                (Some(set), prepared.map_err(|e| e.to_string()))
            }
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

async fn public_request(State(authority): State<Authority>, request: Request) -> Response {
    let path = request.uri().path();
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
    let mut unused: Vec<_> = std::fs::read_dir(&config.publication_root)?
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_ok_and(|t| t.is_dir())
                && e.file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with("candidate-"))
                && e.path() != active
                && pending.as_ref() != Some(&e.path())
        })
        .collect();
    unused.sort_by_key(|e| std::cmp::Reverse(e.metadata().and_then(|m| m.modified()).ok()));
    for entry in unused.into_iter().skip(3) {
        std::fs::remove_dir_all(entry.path())?;
    }
    Ok(())
}

#[cfg(test)]
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
    type Chain = Arc<RwLock<Vec<Vec<u8>>>>;
    struct MockNode {
        chain: Chain,
        pause_ancestor: AtomicBool,
        ancestor_started: tokio::sync::Notify,
        resume: tokio::sync::Notify,
    }
    fn raw_block(parent: [u8; 32], nonce: u8) -> Vec<u8> {
        // Syntactically valid headers and empty blocks are sufficient here:
        // the mock node represents consensus acceptance, which is not this
        // controller's job. No proof-of-work is asserted by this fixture.
        let mut raw = 4u32.to_le_bytes().to_vec();
        raw.extend(parent);
        raw.extend([0; 64]);
        raw.extend(1_700_000_000u32.to_le_bytes());
        raw.extend(0x1f07ffffu32.to_le_bytes());
        raw.extend([nonce; 32]);
        raw.extend([0xfd, 0x40, 0x05]);
        raw.extend([0; 1344]);
        raw.push(0);
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
            chain.push(raw_block(parent, tag + i as u8));
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
        let client = crate::zakura::connect(format!("http://{address}"), &cookie).unwrap();
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
            range_profile: zakura_transparent_private_shared::filter::RANGE_PROFILE.to_string(),
        };
        publication::publish(&options, &journal, BlockHash::from_internal_bytes([0; 32])).unwrap();
        let hook = root.path().join("fleet.py");
        std::fs::write(&hook, r#"#!/usr/bin/env python3
import json,sys,time
from pathlib import Path
r=json.load(sys.stdin)
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
            range_profile: zakura_transparent_private_shared::filter::RANGE_PROFILE.to_string(),
            follow_journal: false,
            snapshot_dir: None,
            ship_runtimes: false,
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
            publication_gate: std::sync::Mutex::new(()),
            last_init: std::sync::Mutex::new(None),
            status: RwLock::new(serde_json::json!({})),
            observed: RwLock::new(BTreeMap::new()),
            http: reqwest::Client::new(),
            membership: None,
            prepared_set: std::sync::Mutex::new(None),
            runtime_preparations: AtomicU64::new(0),
            runtime_preparation_failures: AtomicU64::new(0),
        }));
        let store = Arc::new(tokio::sync::Mutex::new(journal));
        let (notify, _) = tokio::sync::watch::channel(0);
        let mut cache = OutputCache::new(100);
        ingest_once(&config, &client, &store, &authority, &mut cache, &notify)
            .await
            .unwrap();
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
            let publication = publish_once(&config, &client, &store, &authority);
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
        publish_once(&config, &client, &store, &authority)
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
        // Same-height fork beginning inside the sealed first shard.
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
        publish_once(&config, &client, &store, &authority)
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
    /// from the active publication and builds only what changed. A
    /// preparation whose candidate was invalidated is never offered, a
    /// candidate that fails verification is not published, and a runtime
    /// that cannot be written is reported and left to the workers.
    #[tokio::test(flavor = "multi_thread")]
    async fn shipped_runtimes_are_prepared_linked_and_offered_with_the_candidate() {
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
        let client = crate::zakura::connect(format!("http://{address}"), &cookie).unwrap();
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
            range_profile: zakura_transparent_private_shared::filter::RANGE_PROFILE.to_string(),
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
            range_profile: zakura_transparent_private_shared::filter::RANGE_PROFILE.to_string(),
            follow_journal: false,
            snapshot_dir: None,
            ship_runtimes: true,
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
            publication_gate: std::sync::Mutex::new(()),
            last_init: std::sync::Mutex::new(None),
            status: RwLock::new(serde_json::json!({})),
            observed: RwLock::new(BTreeMap::new()),
            http: reqwest::Client::new(),
            membership: None,
            prepared_set: std::sync::Mutex::new(None),
            runtime_preparations: AtomicU64::new(0),
            runtime_preparation_failures: AtomicU64::new(0),
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
            transparent_server_backend::shardset::ShardSet::open(&first.directory, 3).unwrap();
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

        // A preparation for an epoch an invalidation has since moved past
        // stops before its first segment, and nothing is offered.
        let candidate = config.publication_root.join("candidate-superseded");
        std::process::Command::new("cp")
            .arg("-a")
            .arg(&second.directory)
            .arg(&candidate)
            .status()
            .unwrap();
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

        // A candidate whose table does not verify is not published.
        let epoch = authority.0.epoch.load(Ordering::Acquire);
        // A new candidate whose tables are new files the last verified set
        // does not vouch for.
        let candidate = config.publication_root.join("candidate-corrupt");
        std::process::Command::new("cp")
            .arg("-a")
            .arg(&second.directory)
            .arg(&candidate)
            .status()
            .unwrap();
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
        std::process::Command::new("cp")
            .arg("-a")
            .arg(&second.directory)
            .arg(&unwritable)
            .status()
            .unwrap();
        for name in runtime_files(&unwritable).keys() {
            std::fs::remove_file(unwritable.join(name)).unwrap();
        }
        // Nothing retained holds this copy's tail either.
        for retained in [&first.directory, &second.directory] {
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
}
