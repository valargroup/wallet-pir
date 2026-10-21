//! Consumer-owned packing activation and exact worker handover.
use crate::{
    admission::{read_body, BodyError, Queue, Refusal},
    protocol::{self, digest, Manifest, QueryBinding},
    publication::{Object, METADATA_LIMIT},
    runtime::Packing,
    synchronization::{Authority, Client, Config as SourceConfig, Error, Guard, Lease, Safety},
    worker::Evaluate,
    worker_sync::{Readiness, ReadinessRequest, Release, PIN_LIFETIME},
};
use axum::{
    extract::{Path as UrlPath, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use pir_sync_manager::SyncTask;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

const MAINTENANCE: Duration = Duration::from_secs(5);
const BODY_LIMIT: usize = 512 * 1024;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(flatten)]
    pub source: SourceConfig,
    /// Domain responsibilities and private worker replicas are local configuration.
    #[serde(deserialize_with = "domains")]
    pub workers: BTreeMap<u64, Vec<String>>,
    #[serde(default, deserialize_with = "domains")]
    pub preferred: BTreeMap<u64, BTreeSet<String>>,
}
/// JSON object keys are strings. Inside the host's tagged role and this
/// flattened source they are not converted to integers, so parse them here.
fn domains<'de, D, V>(deserializer: D) -> Result<BTreeMap<u64, V>, D::Error>
where
    D: serde::Deserializer<'de>,
    V: Deserialize<'de>,
{
    BTreeMap::<String, V>::deserialize(deserializer)?
        .into_iter()
        .map(|(domain, value)| Ok((domain.parse().map_err(serde::de::Error::custom)?, value)))
        .collect()
}
impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        self.source.validate()?;
        if self.workers.is_empty() || self.workers.len() > 6 {
            return Err("router needs one to six domain responsibilities".into());
        }
        for (&shard, urls) in &self.workers {
            if urls.is_empty()
                || urls.len() > 2
                || urls.iter().collect::<BTreeSet<_>>().len() != urls.len()
            {
                return Err("router needs one or two distinct replicas per domain".into());
            }
            for url in urls {
                crate::packing_router::valid_origin(url)?;
            }
            if self
                .preferred
                .get(&shard)
                .is_some_and(|set| set.iter().any(|url| !urls.contains(url)))
            {
                return Err("preferred workers must be assigned replicas".into());
            }
        }
        if self
            .preferred
            .keys()
            .any(|key| !self.workers.contains_key(key))
        {
            return Err("preferred domain is not assigned".into());
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Peer {
    url: String,
    incarnation: String,
    deadline: Instant,
}
struct Loaded {
    manifest: Manifest,
    packing: BTreeMap<u64, Arc<Packing>>,
    objects: BTreeMap<u64, Object>,
    peers: Mutex<BTreeMap<u64, Vec<Peer>>>,
}
struct Retired {
    view: Weak<Loaded>,
    manifest: Manifest,
    peers: BTreeMap<u64, Vec<Peer>>,
}
#[derive(Default)]
struct Inner {
    active: Option<Arc<Loaded>>,
    retired: Vec<Retired>,
    material: BTreeMap<String, Weak<Packing>>,
    outstanding: BTreeMap<String, usize>,
    cursor: usize,
}

#[derive(Clone)]
pub struct PackingRouter {
    config: Config,
    root: PathBuf,
    authority: Authority,
    inner: Arc<Mutex<Inner>>,
    http: reqwest::Client,
    admission: Arc<Semaphore>,
    waiters: Arc<Semaphore>,
    synchronization: Arc<Semaphore>,
    budget: crate::PackingBudget,
    loader: std::sync::mpsc::Sender<Box<dyn FnOnce() + Send>>,
    max_objects: usize,
    timing: crate::query_timing::QueryTiming,
    pub incarnation: String,
    _ownership: Arc<File>,
}
type ApiError = (StatusCode, String);
fn unavailable(error: impl ToString) -> ApiError {
    (StatusCode::SERVICE_UNAVAILABLE, error.to_string())
}

impl PackingRouter {
    pub fn open(
        root: &Path,
        config: Config,
        max_objects: usize,
        requests: usize,
    ) -> Result<Self, Error> {
        config.validate()?;
        if !(1..=6).contains(&max_objects)
            || !(1..=4).contains(&requests)
            || config.workers.len() > max_objects
        {
            return Err("router limits exceed qualification target".into());
        }
        fs::create_dir_all(root)?;
        let ownership = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("router.lock"))?;
        ownership
            .try_lock()
            .map_err(|e| format!("router state already owned: {e}"))?;
        if root.join("fence.json").exists() {
            return Err("autonomous router requires a separate state directory".into());
        }
        let safety = Safety::open(root, &config.source.publisher_origin)?;
        let authority = Authority::new(root, &config.source, safety)?;
        let (loader, jobs) = std::sync::mpsc::channel::<Box<dyn FnOnce() + Send>>();
        std::thread::Builder::new()
            .name("packing-loader".into())
            .spawn(move || {
                while let Ok(job) = jobs.recv() {
                    job();
                }
            })?;
        Ok(Self {
            root: root.into(),
            config,
            authority,
            inner: Default::default(),
            http: crate::internal_auth::client_builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            admission: Arc::new(Semaphore::new(requests)),
            waiters: Arc::new(Semaphore::new(16)),
            synchronization: Arc::new(Semaphore::new(1)),
            budget: crate::PackingBudget::router(),
            loader,
            max_objects,
            timing: Default::default(),
            incarnation: hex::encode(rand::random::<[u8; 16]>()),
            _ownership: Arc::new(ownership),
        })
    }
    pub fn synchronizer(&self) -> Result<Synchronizer, Error> {
        Ok(Synchronizer {
            router: self.clone(),
            client: Client::new(
                &self.root,
                &self.config.source,
                self.authority.safety.clone(),
            )?,
            _ownership: self
                .synchronization
                .clone()
                .try_acquire_owned()
                .map_err(|_| "router synchronization already owned")?,
        })
    }
    pub fn public_router(&self) -> Router {
        Router::new()
            .route("/v1/enhance/query", post(query))
            .route("/v1/enhance/session/:session_id", get(session))
            .with_state(self.clone())
    }
    pub fn control_router(&self) -> Router {
        Router::new()
            .route("/internal/health", get(health))
            .route("/internal/readiness", post(readiness))
            .route("/internal/metrics", get(metrics))
            .layer(axum::extract::DefaultBodyLimit::max(
                METADATA_LIMIT as usize,
            ))
            .layer(crate::internal_auth::Token::from_env().layer())
            .with_state(self.clone())
    }

    async fn peers(&self, manifest: &Manifest) -> Result<Option<BTreeMap<u64, Vec<Peer>>>, Error> {
        let mut requests = tokio::task::JoinSet::new();
        for (&shard, urls) in &self.config.workers {
            if !manifest
                .coverage
                .shards
                .iter()
                .any(|domain| domain.id == shard)
            {
                continue;
            }
            for url in urls {
                let http = self.http.clone();
                let url = url.clone();
                let manifest = manifest.clone();
                let holder = self.incarnation.clone();
                requests.spawn(async move {
                    let started = Instant::now();
                    let response = http
                        .post(format!("{url}/internal/readiness"))
                        .timeout(Duration::from_secs(1))
                        .json(&ReadinessRequest {
                            manifest_sha256: digest(&manifest),
                            holder: Some(holder),
                        })
                        .send()
                        .await;
                    let result = async {
                        let response = response?.error_for_status()?;
                        let bytes =
                            crate::query_serving::bounded(response, METADATA_LIMIT as usize)
                                .await?;
                        let ready: Readiness = serde_json::from_slice(&bytes)?;
                        if ready.manifest != manifest
                            || ready.manifest_sha256 != digest(&manifest)
                            || !ready.shards.contains(&shard)
                            || ready.incarnation.len() != 32
                            || hex::decode(&ready.incarnation).is_err()
                            || started.elapsed() >= PIN_LIFETIME
                        {
                            return Err::<Peer, Error>(
                                "worker readiness does not bind requested material".into(),
                            );
                        }
                        Ok(Peer {
                            url,
                            incarnation: ready.incarnation,
                            deadline: started + PIN_LIFETIME,
                        })
                    }
                    .await;
                    (shard, result)
                });
            }
        }
        let mut peers: BTreeMap<u64, Vec<Peer>> = BTreeMap::new();
        while let Some(result) = requests.join_next().await {
            let (shard, result) = result?;
            if let Ok(peer) = result {
                peers.entry(shard).or_default().push(peer);
            }
        }
        Ok(self
            .config
            .workers
            .keys()
            .filter(|id| {
                manifest
                    .coverage
                    .shards
                    .iter()
                    .any(|domain| domain.id == **id)
            })
            .all(|id| peers.get(id).is_some_and(|p| !p.is_empty()))
            .then_some(peers))
    }

    async fn release(&self, manifest: &Manifest, peers: &BTreeMap<u64, Vec<Peer>>) {
        let mut requests = tokio::task::JoinSet::new();
        let unique = peers
            .values()
            .flatten()
            .map(|peer| (peer.url.clone(), peer.incarnation.clone()))
            .collect::<BTreeSet<_>>();
        for (url, incarnation) in unique {
            let http = self.http.clone();
            let request = Release {
                manifest_sha256: digest(manifest),
                holder: self.incarnation.clone(),
                worker_incarnation: incarnation,
            };
            requests.spawn(async move {
                let _ = http
                    .post(format!("{url}/internal/release"))
                    .timeout(Duration::from_secs(1))
                    .json(&request)
                    .send()
                    .await;
            });
        }
        while requests.join_next().await.is_some() {}
    }

    async fn maintain(&self) {
        let (views, retired) = {
            let mut inner = self.inner.lock().unwrap();
            let mut views = inner.active.iter().cloned().collect::<Vec<_>>();
            let mut retired = Vec::new();
            inner.retired.retain(|old| match old.view.upgrade() {
                Some(view) => {
                    views.push(view);
                    true
                }
                None => {
                    retired.push((old.manifest.clone(), old.peers.clone()));
                    false
                }
            });
            (views, retired)
        };
        for (manifest, peers) in retired {
            self.release(&manifest, &peers).await;
        }
        for view in views {
            if self.authority.fresh(&view.manifest).await.is_ok() {
                if let Ok(Some(peers)) = self.peers(&view.manifest).await {
                    *view.peers.lock().unwrap() = peers;
                }
            }
        }
    }

    async fn query_view(&self, binding: QueryBinding) -> Result<(Arc<Loaded>, Guard), ApiError> {
        let loaded = self
            .inner
            .lock()
            .unwrap()
            .active
            .clone()
            .ok_or_else(|| unavailable("router is not ready"))?;
        crate::query_serving::validate_binding(&loaded.manifest, binding)?;
        let guard = self
            .authority
            .fresh(&loaded.manifest)
            .await
            .map_err(unavailable)?;
        let needs_peers = loaded
            .peers
            .lock()
            .unwrap()
            .get(&binding.shard_id)
            .is_none_or(|peers| peers.iter().all(|peer| Instant::now() >= peer.deadline));
        if needs_peers {
            let peers = self
                .peers(&loaded.manifest)
                .await
                .map_err(unavailable)?
                .ok_or_else(|| unavailable("compatible workers are not ready"))?;
            *loaded.peers.lock().unwrap() = peers;
        }
        Ok((loaded, guard))
    }

    fn select(
        &self,
        loaded: &Loaded,
        shard: u64,
        excluded: Option<&str>,
    ) -> Result<(String, Option<String>, Outstanding), ApiError> {
        let mut inner = self.inner.lock().unwrap();
        let peers = loaded.peers.lock().unwrap();
        let peers = peers
            .get(&shard)
            .ok_or_else(|| unavailable("unassigned worker domain"))?;
        let eligible = peers
            .iter()
            .filter(|p| Some(p.url.as_str()) != excluded && Instant::now() < p.deadline)
            .collect::<Vec<_>>();
        let preferred = self.config.preferred.get(&shard);
        let idle = eligible.iter().find(|p| {
            preferred.is_some_and(|set| set.contains(&p.url))
                && inner.outstanding.get(&p.url).copied().unwrap_or(0) == 0
        });
        let chosen = if let Some(peer) = idle {
            *peer
        } else {
            let ordinary = eligible
                .iter()
                .filter(|p| !preferred.is_some_and(|set| set.contains(&p.url)))
                .copied()
                .collect::<Vec<_>>();
            let choices = if ordinary.is_empty() {
                eligible
            } else {
                ordinary
            };
            let minimum = choices
                .iter()
                .map(|p| inner.outstanding.get(&p.url).copied().unwrap_or(0))
                .min()
                .ok_or_else(|| unavailable("no compatible worker"))?;
            let tied = choices
                .into_iter()
                .filter(|p| inner.outstanding.get(&p.url).copied().unwrap_or(0) == minimum)
                .collect::<Vec<_>>();
            tied[inner.cursor % tied.len()]
        };
        inner.cursor = inner.cursor.wrapping_add(1);
        *inner.outstanding.entry(chosen.url.clone()).or_default() += 1;
        Ok((
            chosen.url.clone(),
            Some(chosen.incarnation.clone()),
            Outstanding {
                inner: self.inner.clone(),
                url: chosen.url.clone(),
            },
        ))
    }
}

struct Outstanding {
    inner: Arc<Mutex<Inner>>,
    url: String,
}
impl Drop for Outstanding {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        let count = inner.outstanding.get_mut(&self.url).unwrap();
        *count -= 1;
    }
}

pub struct Synchronizer {
    router: PackingRouter,
    client: Client,
    _ownership: tokio::sync::OwnedSemaphorePermit,
}
impl Synchronizer {
    async fn advance(&mut self) -> Result<(), Error> {
        self.client.begin().await?;
        let response = self.client.current().await?;
        // An invalidated active view no longer promises service. In-flight
        // requests keep their Arcs and byte charges until their fenced bodies end.
        {
            let mut inner = self.router.inner.lock().unwrap();
            if inner.active.as_ref().is_some_and(|view| {
                self.router
                    .authority
                    .safety
                    .permits(&view.manifest)
                    .is_err()
            }) {
                if let Some(old) = inner.active.take() {
                    let peers = old.peers.lock().unwrap().clone();
                    inner.retired.push(Retired {
                        view: Arc::downgrade(&old),
                        manifest: old.manifest.clone(),
                        peers,
                    });
                }
            }
        }
        let manifest = response
            .current
            .manifest
            .clone()
            .ok_or("no complete Enhance publication")?;
        let observed =
            Lease::from_response(response.clone(), &manifest, &self.router.authority.safety)?;
        if self
            .router
            .inner
            .lock()
            .unwrap()
            .active
            .as_ref()
            .is_some_and(|view| view.manifest == manifest)
        {
            self.router.authority.install(manifest, observed)?;
            return Ok(());
        }
        let inventory = self
            .client
            .inventory(
                response
                    .current
                    .inventory
                    .as_ref()
                    .ok_or("missing inventory")?,
                &manifest,
            )
            .await?;
        let domains = inventory
            .domains
            .into_iter()
            .filter(|domain| {
                self.router
                    .config
                    .workers
                    .contains_key(&domain.plan.shard.id)
            })
            .collect::<Vec<_>>();
        let keys = domains
            .iter()
            .map(|domain| {
                Ok(digest(&(
                    &domain.packing.sha256,
                    domain.plan.shard.logical_rows,
                    domain.reference()?,
                )))
            })
            .collect::<Result<BTreeSet<_>, String>>()?;
        if keys.len() > self.router.max_objects {
            return Err("router resident assignment limit exceeded".into());
        }
        let missing = {
            let mut inner = self.router.inner.lock().unwrap();
            inner
                .material
                .retain(|_, material| material.strong_count() > 0);
            keys.iter()
                .filter(|key| !inner.material.contains_key(*key))
                .count()
        };
        if let Err(error) = self.router.budget.admit_mappings(missing) {
            tracing::warn!(%error, "router replacement deferred by capacity; retaining valid active material");
            return Ok(());
        }
        let mut keep = domains
            .iter()
            .map(|domain| domain.packing.sha256.clone())
            .collect::<BTreeSet<_>>();
        {
            let inner = self.router.inner.lock().unwrap();
            for view in inner
                .active
                .iter()
                .cloned()
                .chain(inner.retired.iter().filter_map(|old| old.view.upgrade()))
            {
                keep.extend(view.objects.values().map(|object| object.sha256.clone()));
            }
        }
        self.client.collect(&keep)?;
        let mut packing = BTreeMap::new();
        let mut objects = BTreeMap::new();
        for domain in domains {
            let object = domain.packing_object();
            let key = digest(&(
                &domain.packing.sha256,
                domain.plan.shard.logical_rows,
                domain.reference()?,
            ));
            let cached = self
                .router
                .inner
                .lock()
                .unwrap()
                .material
                .get(&key)
                .and_then(Weak::upgrade);
            let material = if let Some(material) = cached {
                material
            } else {
                {
                    let mut inner = self.router.inner.lock().unwrap();
                    inner
                        .material
                        .retain(|_, material| material.strong_count() > 0);
                }
                let path = self.client.object(&object).await?;
                let (sender, receiver) = tokio::sync::oneshot::channel();
                let budget = self.router.budget.clone();
                let descriptor = domain.packing.clone();
                let rows = domain.plan.shard.logical_rows;
                let reference = domain.reference()?;
                self.router
                    .loader
                    .send(Box::new(move || {
                        let result =
                            crate::prepared_packing::load(&path, &descriptor, rows, &budget)
                                .and_then(|packing| {
                                    if packing.reference(reference.shard_id)? != reference {
                                        return Err(
                                            "published packing does not match session".into()
                                        );
                                    }
                                    Ok(Arc::new(packing))
                                });
                        let _ = sender.send(result);
                    }))
                    .map_err(|_| "packing loader stopped")?;
                let material = receiver.await??;
                self.router
                    .inner
                    .lock()
                    .unwrap()
                    .material
                    .insert(key, Arc::downgrade(&material));
                material
            };
            packing.insert(domain.plan.shard.id, material);
            objects.insert(domain.plan.shard.id, object);
        }
        let Some(peers) = self.router.peers(&manifest).await? else {
            return Ok(());
        };
        let lease = self.client.authority(&manifest).await?;
        if lease.current() != &response.current {
            return Err("router candidate superseded while loading".into());
        }
        let guard = self.router.authority.install(manifest.clone(), lease)?;
        guard.check()?;
        let loaded = Arc::new(Loaded {
            manifest,
            packing,
            objects,
            peers: Mutex::new(peers),
        });
        let mut inner = self.router.inner.lock().unwrap();
        if let Some(old) = inner.active.replace(loaded) {
            let peers = old.peers.lock().unwrap().clone();
            inner.retired.push(Retired {
                view: Arc::downgrade(&old),
                manifest: old.manifest.clone(),
                peers,
            });
        }
        Ok(())
    }
    pub async fn poll_once(&mut self) -> Result<(), Error> {
        let router = self.router.clone();
        router.maintain().await;
        let advance = self.advance();
        tokio::pin!(advance);
        let mut timer = tokio::time::interval(MAINTENANCE);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        timer.tick().await;
        loop {
            tokio::select! {
                result = &mut advance => return result,
                _ = timer.tick() => router.maintain().await,
            }
        }
    }
}
impl SyncTask for Synchronizer {
    type Report = ();
    type Error = Error;
    async fn synchronize(&mut self) -> Result<(), Error> {
        self.poll_once().await
    }
    async fn wait_until(&mut self, deadline: tokio::time::Instant) {
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep_until(deadline.min(tokio::time::Instant::now() + MAINTENANCE)).await;
            self.router.maintain().await;
        }
    }
    fn report(&self, result: &Result<(), Error>) {
        if let Err(error) = result {
            tracing::warn!(%error, "Enhance router synchronization deferred");
        }
    }
    /// Only a confirmed publisher contract version mismatch stops the host.
    fn terminal(&self, error: &Error) -> bool {
        pir_sync_manager::is_incompatible(error.as_ref())
    }
}

async fn health(State(router): State<PackingRouter>) -> Json<serde_json::Value> {
    let inner = router.inner.lock().unwrap();
    Json(
        serde_json::json!({"incarnation": router.incarnation, "active": inner.active.as_ref().map(|view| digest(&view.manifest)),
        "packing_charged_bytes": router.budget.charged_bytes(),
        "ready": inner.active.as_ref().is_some_and(|view| router.authority.check(&view.manifest).is_ok())}),
    )
}
async fn metrics(State(router): State<PackingRouter>) -> impl IntoResponse {
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        format!(
            "{}enhance_packing_router_charged_bytes {}\n{}",
            router.timing.render(),
            router.budget.charged_bytes(),
            crate::memory_observation::observe().render("enhance_packing_router_")
        ),
    )
}
async fn readiness(
    State(router): State<PackingRouter>,
    Json(request): Json<ReadinessRequest>,
) -> Result<Json<Readiness>, ApiError> {
    let loaded = router
        .inner
        .lock()
        .unwrap()
        .active
        .clone()
        .filter(|view| digest(&view.manifest) == request.manifest_sha256)
        .ok_or_else(|| unavailable("requested router material is not active"))?;
    let guard = router
        .authority
        .fresh(&loaded.manifest)
        .await
        .map_err(unavailable)?;
    guard.check().map_err(unavailable)?;
    let peers = router
        .peers(&loaded.manifest)
        .await
        .map_err(unavailable)?
        .ok_or_else(|| unavailable("compatible workers are not ready"))?;
    *loaded.peers.lock().unwrap() = peers;
    guard.check().map_err(unavailable)?;
    Ok(Json(Readiness {
        incarnation: router.incarnation,
        manifest_sha256: request.manifest_sha256,
        manifest: loaded.manifest.clone(),
        shards: loaded.packing.keys().copied().collect(),
    }))
}
async fn session(State(router): State<PackingRouter>, UrlPath(id): UrlPath<String>) -> Response {
    let result = async {
        if !protocol::canonical_hash(&id) {
            return Err((StatusCode::BAD_REQUEST, "invalid session id".into()));
        }
        let loaded = router
            .inner
            .lock()
            .unwrap()
            .active
            .clone()
            .ok_or_else(|| unavailable("router is not ready"))?;
        let shard = loaded
            .manifest
            .coverage
            .shards
            .iter()
            .find(|shard| {
                loaded
                    .manifest
                    .session_id(shard.id)
                    .is_ok_and(|hash| hex::encode(hash) == id)
            })
            .ok_or((StatusCode::GONE, "session_unavailable".into()))?
            .id;
        let packing = loaded
            .packing
            .get(&shard)
            .cloned()
            .ok_or((StatusCode::GONE, "session_unavailable".into()))?;
        let guard = router
            .authority
            .fresh(&loaded.manifest)
            .await
            .map_err(unavailable)?;
        let bytes =
            serde_json::to_vec(&packing.session(&loaded.manifest, shard)).map_err(unavailable)?;
        Ok::<_, ApiError>(
            (
                [("content-type", "application/json")],
                crate::response_body::guarded(bytes, (loaded, packing), move || guard.check()),
            )
                .into_response(),
        )
    }
    .await;
    result.unwrap_or_else(crate::query_serving::public_error)
}
async fn query(State(router): State<PackingRouter>, request: Request) -> Response {
    let started = Instant::now();
    match serve(router.clone(), request).await {
        Ok(mut response) => {
            if let Some(stages) = response
                .extensions_mut()
                .remove::<crate::query_timing::CompletedStages>()
            {
                router.timing.observe(stages, started.elapsed());
            }
            response
        }
        Err(error) => crate::query_serving::public_error(error),
    }
}
async fn serve(router: PackingRouter, request: Request) -> Result<Response, ApiError> {
    let began = Instant::now();
    let bytes = read_body(request.into_body(), BODY_LIMIT, Duration::from_secs(30))
        .await
        .map_err(|e| match e {
            BodyError::Timeout => (StatusCode::REQUEST_TIMEOUT, e.to_string()),
            BodyError::Read(_) => (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()),
        })?;
    let body_read = began.elapsed();
    let permit = Queue::with_permits(
        router.admission.clone(),
        router.waiters.clone(),
        Duration::from_secs(2),
        false,
    )
    .acquire(|| ())
    .await
    .map_err(|refusal| match refusal {
        Refusal::Full => (StatusCode::TOO_MANY_REQUESTS, "query queue full".into()),
        Refusal::Deadline => (StatusCode::TOO_MANY_REQUESTS, "admission deadline".into()),
        Refusal::Closed => unavailable("admission closed"),
    })?;
    let binding = QueryBinding::decode(&bytes).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let (loaded, guard) = router.query_view(binding).await?;
    let packing = loaded
        .packing
        .get(&binding.shard_id)
        .cloned()
        .ok_or((StatusCode::GONE, "session_unavailable".into()))?;
    let query_rows = crate::query_serving::validate_binding(&loaded.manifest, binding)?;
    crate::query_serving::admitted(async move {
        guard.check().map_err(unavailable)?;
        let coefficients = packing
            .query_coefficients(&bytes, binding, query_rows)
            .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
        let request = Evaluate {
            binding: Some(binding.encode()),
            generation: binding.generation,
            shard_id: binding.shard_id,
            epoch: hex::encode(binding.epoch),
            session_id: hex::encode(binding.session_id),
            coefficients,
        };
        let count = loaded
            .peers
            .lock()
            .unwrap()
            .get(&binding.shard_id)
            .map_or(0, Vec::len);
        let answer =
            crate::query_serving::evaluate_exact(&router.http, &request, count, |excluded| {
                router.select(&loaded, binding.shard_id, excluded)
            })
            .await?;
        guard.check().map_err(unavailable)?;
        let worker_time = answer.worker_time;
        let (bytes, packing, packing_time) = tokio::task::spawn_blocking(move || {
            let began = Instant::now();
            let response = packing
                .pack(&bytes, &answer.coefficients, query_rows)
                .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
            Ok::<_, ApiError>((response, packing, began.elapsed()))
        })
        .await
        .map_err(unavailable)??;
        guard.check().map_err(unavailable)?;
        let mut response =
            crate::response_body::guarded(bytes, (permit, loaded, packing), move || guard.check())
                .into_response();
        response
            .extensions_mut()
            .insert(crate::query_timing::CompletedStages {
                body_read,
                worker: worker_time,
                packing: packing_time,
            });
        Ok(response)
    })
    .await?
}
