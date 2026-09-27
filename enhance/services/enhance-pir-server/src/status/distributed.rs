//! Private Status role protocol. Transport authentication is supplied by restricted
//! SSH forwarding; every listener and configured peer must be loopback-only.
use super::*;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
};
use tokio::sync::Mutex;

pub type Failure = Box<dyn std::error::Error + Send + Sync>;
const WATCHDOG: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Worker,
    Router,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub epoch: u64,
    pub incarnation: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prepare {
    pub binding: Binding,
    pub manifest: Manifest,
    pub artifact_digest: Hash,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ready {
    pub binding: Binding,
    pub manifest: Manifest,
    pub artifact_digest: Hash,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Activate {
    pub binding: Binding,
    pub manifest: Manifest,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Health {
    pub role: Role,
    pub binding: Binding,
    pub active: Option<Hash>,
    pub cached_rows: Option<Hash>,
    pub ready: bool,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fence {
    version: u32,
    network: Hash,
    epoch: u64,
    generation: u64,
    manifest: Option<Hash>,
}
struct StateData {
    fence: Fence,
    candidate: Option<(Generation, Hash)>,
    candidate_artifact: Option<Arc<Vec<u8>>>,
    preparation_cache: Option<Arc<Generation>>,
    controller: Option<Controller>,
    contact: Option<Instant>,
    poisoned: bool,
}
#[derive(Clone)]
pub struct Service {
    role: Role,
    network: Hash,
    incarnation: String,
    dir: PathBuf,
    _lock: Arc<File>,
    state: Arc<Mutex<StateData>>,
    preparation: Arc<Mutex<()>>,
    // Query packing stays on the global pool; preparation cannot enqueue there.
    preparation_pool: Option<Arc<rayon::ThreadPool>>,
    http: reqwest::Client,
    serving_http: reqwest::Client,
    artifact_origin: String,
    worker_origin: String,
    cuda: bool,
    permits: Arc<super::admission::Admission>,
}

fn preparation_pool() -> Result<rayon::ThreadPool, Failure> {
    let threads = std::env::var("STATUS_PREPARATION_THREADS")
        .ok()
        .map(|v| v.parse::<usize>())
        .transpose()?
        .unwrap_or(1);
    build_preparation_pool(threads)
}
/// Preparation runs below the query path: on a shared CPU host the worker's
/// evaluations and the router's packing must preempt candidate preparation.
const PREPARATION_NICE: i32 = 10;
fn build_preparation_pool(threads: usize) -> Result<rayon::ThreadPool, Failure> {
    // Independent of RAYON_NUM_THREADS, the online query CPU budget. Explicitly
    // bounded to prevent accidental CPU oversubscription from deployment config.
    if !(1..=4).contains(&threads) {
        return Err("STATUS_PREPARATION_THREADS must be between 1 and 4".into());
    }
    Ok(rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|i| format!("status-prepare-{i}"))
        .start_handler(|_| lower_thread_priority(PREPARATION_NICE))
        .build()?)
}
/// Lower only the calling thread's scheduling priority (Linux per-thread nice).
fn lower_thread_priority(nice: i32) {
    #[cfg(target_os = "linux")]
    // SAFETY: setpriority on the calling thread's own tid touches no memory.
    unsafe {
        let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
        if libc::setpriority(libc::PRIO_PROCESS, tid, nice) != 0 {
            tracing::warn!("could not lower Status preparation thread priority");
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = nice;
}

pub fn loopback_origin(origin: &str) -> Result<(), Failure> {
    let url = reqwest::Url::parse(origin)?;
    let ip: std::net::IpAddr = url.host_str().ok_or("missing peer")?.parse()?;
    if url.scheme() != "http"
        || !ip.is_loopback()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "Status peer must be a loopback HTTP origin behind authenticated forwarding".into(),
        );
    }
    Ok(())
}
fn persist(dir: &FsPath, fence: &Fence) -> Result<(), Failure> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(dir.join("fence.tmp"))?;
    file.write_all(&serde_json::to_vec(fence)?)?;
    file.sync_all()?;
    fs::rename(dir.join("fence.tmp"), dir.join("fence.json"))?;
    File::open(dir)?.sync_all()?;
    Ok(())
}
impl Service {
    pub fn open(
        role: Role,
        network: Hash,
        dir: PathBuf,
        artifact_origin: String,
        worker_origin: String,
        cuda: bool,
    ) -> Result<Self, Failure> {
        loopback_origin(&artifact_origin)?;
        loopback_origin(&worker_origin)?;
        if network == [0; 32] || (role == Role::Router && cuda) {
            return Err("invalid Status role configuration".into());
        }
        fs::create_dir_all(&dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("role.lock"))?;
        lock.try_lock().map_err(|e| e.to_string())?;
        let fence = if dir.join("fence.json").exists() {
            let f: Fence = serde_json::from_slice(&fs::read(dir.join("fence.json"))?)?;
            if f.version != 1 || f.network != network {
                return Err("incompatible Status fence".into());
            }
            f
        } else {
            Fence {
                version: 1,
                network,
                epoch: 0,
                generation: 0,
                manifest: None,
            }
        };
        let incarnation = hex::encode(rand::random::<[u8; 32]>());
        Ok(Self {
            role,
            network,
            incarnation,
            dir,
            _lock: Arc::new(lock),
            state: Arc::new(Mutex::new(StateData {
                fence,
                candidate: None,
                candidate_artifact: None,
                preparation_cache: None,
                controller: None,
                contact: None,
                poisoned: false,
            })),
            preparation: Arc::new(Mutex::new(())),
            preparation_pool: Some(Arc::new(preparation_pool()?)),
            serving_http: serving_http_client()?,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            artifact_origin,
            worker_origin,
            cuda,
            permits: Arc::new(super::admission::Admission::production()),
        })
    }
    fn binding(&self, state: &StateData) -> Binding {
        Binding {
            epoch: state.fence.epoch,
            incarnation: self.incarnation.clone(),
        }
    }
    fn check(&self, state: &StateData, binding: &Binding) -> Result<(), StatusCode> {
        if state.poisoned || binding != &self.binding(state) || binding.epoch == 0 {
            Err(StatusCode::CONFLICT)
        } else {
            Ok(())
        }
    }
    async fn serving(&self) -> Result<Controller, StatusCode> {
        let state = self.state.lock().await;
        let role = match self.role {
            Role::Worker => "worker",
            Role::Router => "router",
        };
        if state.poisoned {
            telemetry::admission_rejected(role, "poisoned");
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        if state.contact.is_none_or(|t| t.elapsed() > WATCHDOG) {
            telemetry::admission_rejected(role, "watchdog_expired");
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        state.controller.clone().ok_or_else(|| {
            telemetry::admission_rejected(role, "no_controller");
            StatusCode::SERVICE_UNAVAILABLE
        })
    }
    /// Control, artifact, public material, worker evaluation and telemetry.
    /// Never contains the public query route.
    pub fn control_routes(&self) -> Router {
        Router::new()
            .route("/control/health", get(health))
            .route("/control/fence", post(fence))
            .route("/control/prepare", post(prepare))
            .route("/control/prepare-refresh", post(prepare_refresh))
            .route("/control/activate", post(activate))
            .route("/control/heartbeat", post(heartbeat))
            .route("/artifact/:id", get(artifact))
            .route("/public/:id", get(public))
            .route("/evaluate", post(worker_query))
            .layer(axum::middleware::from_fn(|request, next| {
                telemetry::observe("worker", request, next)
            }))
            .layer(DefaultBodyLimit::max(512 * 1024))
            .with_state(self.clone())
            .merge(telemetry::routes())
    }
    /// Only the router's forwarded query route, with the same body limit and
    /// admission as the merged listener.
    pub fn query_routes(&self) -> Router {
        Router::new()
            .route("/v1/status/query", post(router_query))
            .layer(axum::middleware::from_fn(|request, next| {
                telemetry::observe("router", request, next)
            }))
            .layer(DefaultBodyLimit::max(512 * 1024))
            .with_state(self.clone())
    }
    /// Merged single-listener layout.
    pub fn routes(&self) -> Router {
        self.control_routes().merge(self.query_routes())
    }
    /// With `query_listen`, a router serves only `/v1/status/query` there and
    /// everything else on `listen`. Without it, both share `listen`.
    pub async fn serve(
        self,
        listen: SocketAddr,
        query_listen: Option<SocketAddr>,
    ) -> Result<(), Failure> {
        if !listen.ip().is_loopback() || query_listen.is_some_and(|q| !q.ip().is_loopback()) {
            return Err("Status role listener must be loopback".into());
        }
        let Some(query_listen) = query_listen else {
            axum::serve(tokio::net::TcpListener::bind(listen).await?, self.routes()).await?;
            return Ok(());
        };
        if self.role != Role::Router {
            return Err("--query-listen applies only to the router role".into());
        }
        if query_listen == listen {
            return Err("Status query listener must differ from the control listener".into());
        }
        let control = tokio::net::TcpListener::bind(listen).await?;
        let queries = tokio::net::TcpListener::bind(query_listen).await?;
        let control = axum::serve(control, self.control_routes());
        let queries = axum::serve(queries, self.query_routes());
        tokio::select! {
            result = control => result?,
            result = queries => result?,
        }
        Ok(())
    }
}
async fn health(State(s): State<Service>) -> Json<Health> {
    let state = s.state.lock().await;
    Json(Health {
        role: s.role,
        binding: s.binding(&state),
        cached_rows: state
            .preparation_cache
            .as_ref()
            .map(|g| g.manifest.rows_digest),
        active: state.controller.as_ref().map(|c| c.current().manifest.id()),
        ready: !state.poisoned
            && state.contact.is_some_and(|t| t.elapsed() <= WATCHDOG)
            && state
                .controller
                .as_ref()
                .is_some_and(|c| c.current().manifest.fresh(now_ms()).is_ok()),
    })
}
async fn fence(
    State(s): State<Service>,
    Json(b): Json<Binding>,
) -> Result<Json<Binding>, StatusCode> {
    let mut state = s.state.lock().await;
    if state.poisoned
        || b.incarnation != s.incarnation
        || b.epoch < state.fence.epoch
        || b.epoch == 0
    {
        return Err(StatusCode::CONFLICT);
    }
    if b.epoch > state.fence.epoch || state.controller.is_none() {
        if let Some(c) = &state.controller {
            c.revoke();
        }
        state.controller = None;
        state.candidate = None;
        state.candidate_artifact = None;
        state.contact = None;
        state.fence.epoch = b.epoch;
        state.fence.manifest = None;
        if persist(&s.dir, &state.fence).is_err() {
            state.poisoned = true;
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }
    Ok(Json(s.binding(&state)))
}
async fn prepare(
    State(s): State<Service>,
    Json(p): Json<Prepare>,
) -> Result<Json<Ready>, StatusCode> {
    let began = Instant::now();
    let _permit = s
        .preparation
        .try_lock()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    p.manifest
        .fresh(now_ms())
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let previous = {
        let state = s.state.lock().await;
        s.check(&state, &p.binding)?;
        if p.manifest.network != s.network
            || p.manifest.recovery_epoch != p.binding.epoch
            || p.manifest.generation <= state.fence.generation
        {
            return Err(StatusCode::CONFLICT);
        }
        if let Some((g, digest)) = &state.candidate {
            if g.manifest == p.manifest && digest == &p.artifact_digest {
                return Ok(Json(Ready {
                    binding: p.binding,
                    manifest: g.manifest.clone(),
                    artifact_digest: *digest,
                }));
            }
        }
        if state.controller.as_ref().is_some_and(|c| !c.can_prepare()) {
            return Err(StatusCode::TOO_MANY_REQUESTS);
        }
        state
            .preparation_cache
            .clone()
            .or_else(|| state.controller.as_ref().map(Controller::current))
    };
    let limit = match s.role {
        Role::Worker => ROWS * ROW_BYTES,
        Role::Router => Generation::hint_len(),
    };
    let response = s
        .http
        .get(format!(
            "{}/artifact/{}",
            s.artifact_origin,
            hex::encode(p.artifact_digest)
        ))
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let bytes = bounded(response, limit).await?;
    if (s.role == Role::Router && bytes.len() != limit)
        || <Hash>::from(Sha256::digest(&bytes)) != p.artifact_digest
    {
        return Err(StatusCode::BAD_GATEWAY);
    }
    let fetch_ms = began.elapsed().as_millis();
    let manifest = p.manifest.clone();
    let role = s.role;
    let cuda = s.cuda;
    let preparation_pool = s.preparation_pool.clone();
    let g = tokio::task::spawn_blocking(move || match role {
        Role::Router => preparation_pool
            .as_ref()
            .ok_or(Error::Unavailable)?
            .install(|| {
                Generation::prepare_router_with_previous(manifest, &bytes, previous.as_deref())
            }),
        Role::Worker => {
            let base = if bytes.len() < ROWS * ROW_BYTES {
                previous.as_ref().map(|g| {
                    (
                        g.manifest.rows_digest,
                        g.units
                            .iter()
                            .flat_map(|u| u.rows.iter().copied())
                            .collect(),
                    )
                })
            } else {
                None
            };
            let bytes = super::artifact::decode(bytes, manifest.rows_digest, base)?;
            let snapshot = index::Snapshot {
                network: manifest.network,
                salt: manifest.salt,
                start: manifest.coverage_start,
                height: manifest.anchor_height,
                anchor: manifest.anchor_hash,
                entries: manifest.entries,
                rows: bytes,
                digest: manifest.rows_digest,
                evicted_blocks: 0,
            };
            let (mut g, _) = preparation_pool
                .as_ref()
                .ok_or(Error::Unavailable)?
                .install(|| {
                    Generation::prepare_worker(
                        &snapshot,
                        manifest.generation,
                        manifest.recovery_epoch,
                        manifest.observed_ms,
                        previous.as_deref(),
                        if cuda {
                            MatvecBackend::Cuda { device: 0 }
                        } else {
                            MatvecBackend::Cpu
                        },
                    )
                })?;
            g.manifest = manifest;
            Ok(g)
        }
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;
    let prepare_ms = began.elapsed().as_millis();
    let (g, artifact, digest) = tokio::task::spawn_blocking(move || -> Result<_, Error> {
        let artifact = if role == Role::Worker {
            Some(Arc::new(g.hint_bytes()?))
        } else {
            None
        };
        let digest = artifact.as_ref().map_or(g.manifest.public_digest, |a| {
            Sha256::digest(a.as_slice()).into()
        });
        Ok((g, artifact, digest))
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    tracing::info!(
        ?role,
        fetch_ms,
        prepare_ms,
        total_ms = began.elapsed().as_millis(),
        source_age_ms = now_ms().saturating_sub(g.manifest.observed_ms),
        "Status role preparation"
    );
    let mut state = s.state.lock().await;
    // Cached calculations are never authority. They may survive a fence, but
    // all serving requires a new matching prepare/activation decision.
    state.preparation_cache = Some(Arc::new(g.clone()));
    s.check(&state, &p.binding)?;
    g.manifest
        .fresh(now_ms())
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let ready = Ready {
        binding: p.binding,
        manifest: g.manifest.clone(),
        artifact_digest: digest,
    };
    state.candidate = Some((g, digest));
    state.candidate_artifact = artifact;
    Ok(Json(ready))
}
/// Reaffirm only identical content and coverage; the coordinator must have
/// completed a fresh identical source observation. This never rebuilds material.
async fn prepare_refresh(
    State(s): State<Service>,
    Json(a): Json<Activate>,
) -> Result<Json<Ready>, StatusCode> {
    let _permit = s
        .preparation
        .try_lock()
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
    let mut state = s.state.lock().await;
    s.check(&state, &a.binding)?;
    a.manifest
        .fresh(now_ms())
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let c = state.controller.as_ref().ok_or(StatusCode::CONFLICT)?;
    if a.manifest.generation <= state.fence.generation {
        return Err(StatusCode::CONFLICT);
    }
    let previous = c.current();
    let mut expected = previous.manifest.clone();
    if a.manifest.observed_ms < expected.observed_ms {
        return Err(StatusCode::CONFLICT);
    }
    expected.generation = a.manifest.generation;
    expected.observed_ms = a.manifest.observed_ms;
    if expected != a.manifest {
        return Err(StatusCode::CONFLICT);
    }
    let mut g = (*previous).clone();
    g.manifest = a.manifest.clone();
    state.candidate = Some((g, a.manifest.public_digest));
    state.candidate_artifact = None;
    Ok(Json(Ready {
        binding: a.binding,
        manifest: a.manifest.clone(),
        artifact_digest: a.manifest.public_digest,
    }))
}

async fn activate(
    State(s): State<Service>,
    Json(a): Json<Activate>,
) -> Result<Json<Ready>, StatusCode> {
    let mut state = s.state.lock().await;
    s.check(&state, &a.binding)?;
    a.manifest
        .fresh(now_ms())
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    if let Some(c) = &state.controller {
        if c.current().manifest == a.manifest {
            return Ok(Json(Ready {
                binding: a.binding,
                manifest: a.manifest.clone(),
                artifact_digest: a.manifest.public_digest,
            }));
        }
    }
    let (candidate, _) = state.candidate.as_ref().ok_or(StatusCode::CONFLICT)?;
    let mut expected = candidate.manifest.clone();
    // The worker binds the router's public digest only at the durable activation decision.
    if s.role == Role::Worker {
        expected.public_digest = a.manifest.public_digest;
    }
    if expected != a.manifest || a.manifest.generation <= state.fence.generation {
        return Err(StatusCode::CONFLICT);
    }
    let mut next = state.fence.clone();
    next.generation = a.manifest.generation;
    next.manifest = Some(a.manifest.id());
    if persist(&s.dir, &next).is_err() {
        if let Some(c) = &state.controller {
            c.revoke();
        }
        state.poisoned = true;
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    state.poisoned = true;
    let (mut candidate, _) = state.candidate.take().unwrap();
    state.candidate_artifact = None;
    candidate.manifest = a.manifest.clone();
    if let Some(c) = &state.controller {
        c.activate(candidate).map_err(|_| StatusCode::CONFLICT)?;
    } else {
        state.controller = Some(Controller::new(candidate));
    }
    state.fence = next;
    state.poisoned = false;
    state.contact = Some(Instant::now());
    Ok(Json(Ready {
        binding: a.binding,
        manifest: a.manifest.clone(),
        artifact_digest: a.manifest.public_digest,
    }))
}
async fn heartbeat(
    State(s): State<Service>,
    Json(a): Json<Activate>,
) -> Result<Json<Binding>, StatusCode> {
    let mut state = s.state.lock().await;
    s.check(&state, &a.binding)?;
    let c = state.controller.as_ref().ok_or(StatusCode::CONFLICT)?;
    // Contact is bound to authority and process incarnation, not to the
    // retention lifetime of a session. Fast successive publications can evict
    // a heartbeat's old session while the same authority is still valid.
    if a.manifest.network != s.network
        || a.manifest.recovery_epoch != state.fence.epoch
        || a.manifest.generation == 0
        || a.manifest.generation > state.fence.generation
        || c.revoked.load(Ordering::SeqCst)
    {
        return Err(StatusCode::CONFLICT);
    }
    state.contact = Some(Instant::now());
    Ok(Json(a.binding))
}
async fn artifact(State(s): State<Service>, Path(id): Path<String>) -> Result<Vec<u8>, StatusCode> {
    if s.role != Role::Worker {
        return Err(StatusCode::NOT_FOUND);
    }
    let state = s.state.lock().await;
    let (_, digest) = state.candidate.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    if hex::encode(digest) != id || state.poisoned {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(state
        .candidate_artifact
        .as_ref()
        .ok_or(StatusCode::NOT_FOUND)?
        .as_ref()
        .clone())
}
async fn public(State(s): State<Service>, Path(id): Path<String>) -> Result<Vec<u8>, StatusCode> {
    let state = s.state.lock().await;
    let (g, _) = state.candidate.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    if s.role != Role::Router || hex::encode(g.manifest.public_digest) != id || state.poisoned {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(g.public.as_ref().clone())
}
async fn worker_query(State(s): State<Service>, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    if s.role != Role::Worker {
        return Err(StatusCode::NOT_FOUND);
    }
    let c = s.serving().await?;
    let http_state = HttpState {
        controller: c,
        origin: String::new(),
        http: s.serving_http.clone(),
        permits: s.permits.clone(),
    };
    let result = evaluate(State(http_state), body).await?;
    s.serving().await?;
    Ok(result)
}
async fn router_query(State(s): State<Service>, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    if s.role != Role::Router {
        return Err(StatusCode::NOT_FOUND);
    }
    let c = s.serving().await?;
    let http_state = HttpState {
        controller: c,
        origin: s.worker_origin.clone(),
        http: s.serving_http.clone(),
        permits: s.permits.clone(),
    };
    let result = query(State(http_state), body).await?;
    s.serving().await?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn open(role: Role, dir: &FsPath) -> Service {
        Service::open(
            role,
            [1; 32],
            dir.to_path_buf(),
            "http://127.0.0.1:1".into(),
            "http://127.0.0.1:2".into(),
            false,
        )
        .unwrap()
    }
    fn material(epoch: u64, generation: u64) -> Generation {
        Generation {
            manifest: Manifest {
                protocol: PROTOCOL.into(),
                network: [1; 32],
                salt: [2; 32],
                generation,
                recovery_epoch: epoch,
                coverage_start: 1,
                anchor_height: 1,
                anchor_hash: [3; 32],
                observed_ms: now_ms(),
                entries: 0,
                rows_digest: [4; 32],
                public_digest: [5; 32],
            },
            public: Arc::new(Vec::new()),
            units: Vec::new(),
            packing: Arc::new(Vec::new()),
            top: None,
            packing_digests: Vec::new(),
        }
    }
    async fn establish(s: &Service, epoch: u64) -> Binding {
        let binding = Binding {
            epoch,
            incarnation: s.incarnation.clone(),
        };
        assert_eq!(
            fence(State(s.clone()), Json(binding.clone()))
                .await
                .unwrap()
                .0,
            binding
        );
        binding
    }
    #[tokio::test]
    async fn distributed_http_telemetry_counts_rejected_queries_on_both_listener_layouts() {
        use axum::{
            body::{to_bytes, Body},
            http::Request,
        };
        use tower::ServiceExt;
        for role in [Role::Router, Role::Worker] {
            let dir = tempfile::tempdir().unwrap();
            let service = open(role, dir.path());
            let (path, operation) = if role == Role::Router {
                ("/v1/status/query", "router_query")
            } else {
                ("/evaluate", "worker_evaluate")
            };
            for split in [false, true] {
                let routes = if !split {
                    service.routes()
                } else if role == Role::Router {
                    service.query_routes()
                } else {
                    service.control_routes()
                };
                let response = routes
                    .oneshot(Request::post(path).body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
                let response = service
                    .control_routes()
                    .oneshot(
                        Request::get("/internal/status-apm")
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let value: serde_json::Value =
                    serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap())
                        .unwrap();
                assert_eq!(value["http_observation_version"], 1);
                assert!(!value["http_instance"].as_str().unwrap().is_empty());
                assert!(
                    value["operations"][operation]["server_errors"]
                        .as_u64()
                        .unwrap()
                        >= 1
                );
                assert!(value["operations"][operation]["failures"].as_u64().unwrap() >= 1);
                assert_eq!(
                    value["operations"][operation]["buckets"]
                        .as_array()
                        .unwrap()
                        .len(),
                    12
                );
            }
        }
    }
    #[test]
    fn private_peers_reject_external_or_credentialed_origins() {
        for peer in [
            "https://127.0.0.1",
            "http://example.org",
            "http://10.0.0.1",
            "http://u:p@127.0.0.1",
            "http://127.0.0.1/a",
            "http://127.0.0.1/?q=1",
        ] {
            assert!(loopback_origin(peer).is_err(), "{peer}");
        }
        assert!(loopback_origin("http://127.0.0.1:8381").is_ok());
    }
    #[tokio::test]
    async fn restart_changes_incarnation_and_never_restores_readiness() {
        let dir = tempfile::tempdir().unwrap();
        let s = open(Role::Worker, dir.path());
        let binding = establish(&s, 7).await;
        let g = material(7, 3);
        let manifest = g.manifest.clone();
        s.state.lock().await.candidate = Some((g, [6; 32]));
        let _ = activate(
            State(s.clone()),
            Json(Activate {
                binding: binding.clone(),
                manifest: manifest.clone(),
            }),
        )
        .await
        .unwrap();
        assert!(s.serving().await.is_ok());
        drop(s);
        let recovered = open(Role::Worker, dir.path());
        assert!(recovered.serving().await.is_err());
        assert_ne!(binding.incarnation, recovered.incarnation);
        assert!(heartbeat(
            State(recovered.clone()),
            Json(Activate { binding, manifest })
        )
        .await
        .is_err());
        assert_eq!(recovered.state.lock().await.fence.generation, 3);
    }
    #[tokio::test]
    async fn fence_revokes_already_pinned_controller_and_rejects_old_activation() {
        let dir = tempfile::tempdir().unwrap();
        let s = open(Role::Router, dir.path());
        let b = establish(&s, 1).await;
        let g = material(1, 1);
        let manifest = g.manifest.clone();
        s.state.lock().await.candidate = Some((g, [6; 32]));
        let a = Activate {
            binding: b,
            manifest,
        };
        let _ = activate(State(s.clone()), Json(a.clone())).await.unwrap();
        let pinned = s.serving().await.unwrap();
        let generation = pinned.current();
        establish(&s, 2).await;
        assert_eq!(pinned.check(&generation), Err(StatusCode::GONE));
        assert!(activate(State(s.clone()), Json(a)).await.is_err());
        assert!(s.serving().await.is_err());
    }
    #[tokio::test]
    async fn wrong_digest_activation_is_rejected_and_repeat_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let s = open(Role::Router, dir.path());
        let b = establish(&s, 1).await;
        let g = material(1, 1);
        let manifest = g.manifest.clone();
        s.state.lock().await.candidate = Some((g, [6; 32]));
        let mut a = Activate {
            binding: b,
            manifest,
        };
        a.manifest.rows_digest = [9; 32];
        assert!(activate(State(s.clone()), Json(a.clone())).await.is_err());
        a.manifest.rows_digest = [4; 32];
        let _ = activate(State(s.clone()), Json(a.clone())).await.unwrap();
        let _ = activate(State(s.clone()), Json(a)).await.unwrap();
        assert_eq!(s.state.lock().await.fence.generation, 1);
    }
    #[tokio::test]
    async fn refresh_cannot_launder_changed_source_or_coverage() {
        let dir = tempfile::tempdir().unwrap();
        let s = open(Role::Router, dir.path());
        let b = establish(&s, 1).await;
        let g = material(1, 1);
        let mut a = Activate {
            binding: b,
            manifest: g.manifest.clone(),
        };
        s.state.lock().await.candidate = Some((g, [6; 32]));
        let _ = activate(State(s.clone()), Json(a.clone())).await.unwrap();
        a.manifest.generation = 2;
        a.manifest.rows_digest = [9; 32];
        assert!(prepare_refresh(State(s.clone()), Json(a.clone()))
            .await
            .is_err());
        a.manifest.rows_digest = [4; 32];
        a.manifest.anchor_height = 2;
        assert!(prepare_refresh(State(s.clone()), Json(a.clone()))
            .await
            .is_err());
        a.manifest.anchor_height = 1;
        let _ = prepare_refresh(State(s.clone()), Json(a.clone()))
            .await
            .unwrap();
        let _ = activate(State(s.clone()), Json(a.clone())).await.unwrap();
        assert_eq!(s.serving().await.unwrap().current().manifest, a.manifest);
    }

    #[tokio::test]
    async fn stale_source_does_not_revoke_valid_control_authority() {
        let dir = tempfile::tempdir().unwrap();
        let s = open(Role::Router, dir.path());
        let b = establish(&s, 1).await;
        let mut g = material(1, 1);
        g.manifest.observed_ms = now_ms() - MAX_AGE_MS - 1;
        let a = Activate {
            binding: b,
            manifest: g.manifest.clone(),
        };
        s.state.lock().await.controller = Some(Controller::new(g));
        s.state.lock().await.fence.generation = 1;
        let _ = heartbeat(State(s.clone()), Json(a.clone())).await.unwrap();
        let c = s.serving().await.unwrap();
        assert_eq!(c.check(&c.current()), Err(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!health(State(s.clone())).await.0.ready);
        assert_eq!(s.state.lock().await.fence.epoch, 1);
    }

    #[tokio::test]
    async fn watchdog_does_not_renew_source_freshness() {
        let dir = tempfile::tempdir().unwrap();
        let s = open(Role::Router, dir.path());
        let b = establish(&s, 1).await;
        let g = material(1, 1);
        let a = Activate {
            binding: b,
            manifest: g.manifest.clone(),
        };
        s.state.lock().await.candidate = Some((g, [6; 32]));
        let _ = activate(State(s.clone()), Json(a.clone())).await.unwrap();
        s.state.lock().await.contact = Some(Instant::now() - WATCHDOG - Duration::from_secs(1));
        assert!(s.serving().await.is_err());
        let _ = heartbeat(State(s.clone()), Json(a.clone())).await.unwrap();
        assert_eq!(
            s.serving().await.unwrap().current().manifest.observed_ms,
            a.manifest.observed_ms
        );
    }
    #[tokio::test]
    async fn split_listeners_keep_query_route_off_the_control_router() {
        use axum::{body::Body, http::Request};
        use tower::ServiceExt;
        let dir = tempfile::tempdir().unwrap();
        let s = open(Role::Router, dir.path());
        async fn status(router: Router, method: &str, path: &str) -> StatusCode {
            router
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
        }
        // Control router: no query route; control routes present.
        assert_eq!(
            status(s.control_routes(), "POST", "/v1/status/query").await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status(s.control_routes(), "GET", "/control/health").await,
            StatusCode::OK
        );
        // Query router: only the query route (unready role answers 503, not 404).
        assert_eq!(
            status(s.query_routes(), "POST", "/v1/status/query").await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        for path in [
            "/control/health",
            "/control/fence",
            "/control/prepare",
            "/control/activate",
            "/control/heartbeat",
            "/artifact/00",
            "/public/00",
            "/evaluate",
            "/internal/metrics",
            "/internal/health",
        ] {
            for method in ["GET", "POST"] {
                assert_eq!(
                    status(s.query_routes(), method, path).await,
                    StatusCode::NOT_FOUND,
                    "{method} {path}"
                );
            }
        }
        // Merged layout still serves both.
        assert_eq!(
            status(s.routes(), "POST", "/v1/status/query").await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status(s.routes(), "GET", "/control/health").await,
            StatusCode::OK
        );
        // A split worker or a non-loopback query listener is refused.
        let worker_dir = tempfile::tempdir().unwrap();
        let worker = open(Role::Worker, worker_dir.path());
        assert!(worker
            .serve(
                "127.0.0.1:0".parse().unwrap(),
                Some("127.0.0.1:0".parse().unwrap())
            )
            .await
            .is_err());
        assert!(s
            .clone()
            .serve(
                "127.0.0.1:0".parse().unwrap(),
                Some("0.0.0.0:0".parse().unwrap())
            )
            .await
            .is_err());
    }
    #[test]
    fn preparation_rejects_unbounded_threads() {
        assert!(build_preparation_pool(0).is_err());
        assert!(build_preparation_pool(5).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn preparation_threads_run_below_query_priority() {
        let pool = build_preparation_pool(2).unwrap();
        let nice = pool.install(|| {
            // SAFETY: getpriority on the calling thread's own tid touches no memory.
            unsafe {
                let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
                libc::getpriority(libc::PRIO_PROCESS, tid)
            }
        });
        assert_eq!(nice, PREPARATION_NICE);
    }

    #[test]
    fn preparation_uses_its_own_bounded_pool() {
        let pool = build_preparation_pool(2).unwrap();
        pool.install(|| {
            assert_eq!(rayon::current_num_threads(), 2);
            (0..16).into_par_iter().for_each(|_| {
                assert!(std::thread::current()
                    .name()
                    .unwrap()
                    .starts_with("status-prepare-"));
            });
        });
        assert!(!std::thread::current()
            .name()
            .unwrap_or("")
            .starts_with("status-prepare-"));
    }
}
