//! Continuous, chain-checked publication. Ingest owns the journal; builders use
//! copied suffix snapshots and activation is delegated to the private fleet tool.
use crate::{
    events::{BlockEntry, EventStore, EventStoreError},
    ingest::build_fetched_block_events,
    prevout::OutputCache,
    publication::{self, BoxError, Journal, PublishOptions},
    shard_filters::ShardFilters,
    zakura::ZakuraClient,
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
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
};
use tokio::io::AsyncWriteExt;
use transparent_events::TransparentEvent;
use transparent_filter::{BlockHash, ScriptBytes, ShardMap};

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
    active: RwLock<Arc<Public>>,
    withdrawn: AtomicBool,
    epoch: AtomicU64,
    publication_gate: std::sync::Mutex<()>,
    status: RwLock<serde_json::Value>,
    http: reqwest::Client,
    observed: RwLock<BTreeMap<u64, std::time::Instant>>,
}
#[derive(Clone)]
pub struct Authority(Arc<Inner>);
impl Authority {
    pub fn router(&self) -> Router {
        Router::new()
            .fallback(public_request)
            .with_state(self.clone())
    }
}

/// Snapshot only rebuilt event rows plus the endpoint hashes used to establish
/// reuse. It stays valid even if ingest truncates and rewrites its journal.
pub struct Snapshot {
    genesis: String,
    first: u64,
    end: u64,
    count: u64,
    blocks: BTreeMap<u64, BlockEntry>,
    events: std::sync::Mutex<std::fs::File>,
    events_offset: u64,
    events_first: u64,
}
impl Snapshot {
    pub fn capture(store: &EventStore, previous: &ShardMap) -> Result<Self, BoxError> {
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
        let (events, events_offset) = store.snapshot_suffix(first)?;
        for height in first..=end {
            blocks.insert(
                height,
                store.block_at(height).ok_or("snapshot block missing")?,
            );
        }
        Ok(Self {
            genesis: store.genesis_hash().into(),
            first: store.start_height(),
            end,
            count: store.events_stored(),
            blocks,
            events: std::sync::Mutex::new(events),
            events_offset,
            events_first: first,
        })
    }
}
impl Journal for Snapshot {
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
        crate::events::read_event_record(&mut self.events.lock().unwrap(), entry)
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
    let rpc = ZakuraClient::from_cookie_file(&config.rpc_url, &config.rpc_cookie)?;
    let existing = EventStore::open_existing(&config.data_dir)?;
    if existing.start_height() != 0 || existing.genesis_hash() != rpc.genesis_hash().await? {
        return Err("journal identity does not match genesis RPC history".into());
    }
    let store = EventStore::open(
        &config.data_dir,
        existing.genesis_hash(),
        existing.start_height(),
    )?;
    drop(existing);
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
        active: RwLock::new(Arc::new(public)),
        withdrawn: AtomicBool::new(true),
        epoch: AtomicU64::new(0),
        publication_gate: std::sync::Mutex::new(()),
        status: RwLock::new(serde_json::json!({"phase":"starting"})),
        observed: RwLock::new(BTreeMap::new()),
        http: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()?,
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
        loop {
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

async fn ingest_once(
    config: &Config,
    rpc: &ZakuraClient,
    store: &Arc<tokio::sync::Mutex<EventStore>>,
    authority: &Authority,
    cache: &mut OutputCache,
    notify: &tokio::sync::watch::Sender<u64>,
) -> Result<(), BoxError> {
    let tip = rpc.tip_height().await?;
    let tip_hash = rpc.block_hash(tip).await?;
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
        || rpc.block_hash(public.publication.height).await? != public.publication.hash;
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
    if public_changed && rpc.genesis_hash().await? != store.lock().await.genesis_hash() {
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
        if height <= tip && rpc.block_hash(height).await? == hash.to_display_hex() {
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
        let fetched = rpc.block(next).await?;
        let parent = fetched.1.header.previous_block_hash.to_string();
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
        let built = build_fetched_block_events(rpc, cache, next, fetched).await?;
        if rpc.block_hash(next).await? != built.block_hash.to_display_hex() {
            return Err("block became noncanonical during extraction".into());
        }
        {
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
    rpc: &ZakuraClient,
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
    let snapshot = {
        let journal = store.lock().await;
        Snapshot::capture(&journal, old.filters.map())?
    };
    let height = snapshot.end;
    let hash = snapshot
        .block_at(height)
        .ok_or("snapshot has no endpoint")?
        .block_hash
        .to_display_hex();
    if rpc.block_hash(height).await? != hash {
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
        };
        tokio::task::spawn_blocking(move || {
            publication::publish(&options, &snapshot, BlockHash::from_internal_bytes([0; 32]))
        })
        .await??;
    }
    let mut candidate = read_public(directory.clone(), Vec::new())?;
    authority.0.status.write().unwrap()["candidate_height"] = height.into();
    let request = serde_json::json!({"operation":"prepare","directory":directory,"map_sha256":candidate.publication.map_sha256,"recent_from":config.recent_from,"source_sha":config.source_sha});
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
    if authority.0.epoch.load(Ordering::Acquire) != epoch || rpc.block_hash(height).await? != hash {
        return Err("candidate invalidated while preparing".into());
    }
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
    if authority.0.epoch.load(Ordering::Acquire) != epoch || rpc.block_hash(height).await? != hash {
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
        status["freshness_seconds"] = freshness.into();
        status["ready_replicas"] = activated["recent_replicas"].clone();
        status.as_object_mut().unwrap().remove("publication_error");
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

async fn public_request(State(authority): State<Authority>, request: Request) -> Response {
    let path = request.uri().path();
    let epoch = authority.0.epoch.load(Ordering::Acquire);
    if path == "/v1/status" {
        return axum::Json(authority.0.status.read().unwrap().clone()).into_response();
    }
    if path == "/metrics" {
        let status = authority.0.status.read().unwrap();
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
        let mut text = String::new();
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
    let body = if path == "/v1/shards" || path == "/v1/filters/shards" {
        Some(("application/json", active.filters.map_json().to_vec()))
    } else if path == "/v1/shards/init" {
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
                .get(format!("http://{upstream}/v1/shards/init"))
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
                                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
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
        if expected.iter().all(|name| geometries.contains_key(name))
            && !authority.0.withdrawn.load(Ordering::Acquire)
            && authority.0.epoch.load(Ordering::Acquire) == epoch
        {
            if let Some(mut value) = init {
                value["geometries"] = serde_json::Value::Array(geometries.into_values().collect());
                return ([("cache-control", "no-cache")], axum::Json(value)).into_response();
            }
        }
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    } else {
        let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
        match parts.as_slice() {
            ["v1", "filters", "shards", id, "filter"] => id
                .parse()
                .ok()
                .and_then(|id| active.filters.filter(id))
                .map(|b| ("application/octet-stream", b.to_vec())),
            ["v1", "shards", id, "revisions", digest, "manifest"]
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
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
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
    use super::*;
    use axum::{routing::post, Json};
    use zakura_chain::serialization::ZcashDeserialize;
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
        let block = zakura_chain::block::Block::zcash_deserialize(raw).unwrap();
        BlockHash::from_display_hex(&block.hash().to_string()).unwrap()
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
        let client = ZakuraClient::from_cookie_file(format!("http://{address}"), &cookie).unwrap();
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
            recent_geometry: "recent-4k".into(),
            archive_geometry: Some("recent-8k".into()),
            recent_from: Some(2),
            zakura_rpc_url: String::new(),
            zakura_cookie: cookie.clone(),
            through: None,
            record: None,
            source_sha: None,
            directory_choice: publication::DirectoryChoice::Off,
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
            recent_geometry: "recent-4k".into(),
            archive_geometry: "recent-8k".into(),
            rpc_url: format!("http://{address}"),
            rpc_cookie: cookie,
            fleet_command: hook,
            fleet_config: root.path().join("unused"),
            listen: address,
            source_sha: "fixture".into(),
            shadow: false,
            directory_choice: publication::DirectoryChoice::Off,
        };
        let authority = Authority(Arc::new(Inner {
            active: RwLock::new(Arc::new(read_public(initial, Vec::new()).unwrap())),
            withdrawn: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            publication_gate: std::sync::Mutex::new(()),
            status: RwLock::new(serde_json::json!({})),
            observed: RwLock::new(BTreeMap::new()),
            http: reqwest::Client::new(),
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
                    .uri("/v1/shards")
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
                    .uri("/v1/shards")
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
}
