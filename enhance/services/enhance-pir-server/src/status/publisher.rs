//! Independently scheduled Status controller hosted by the Enhance coordinator.
use super::{distributed::*, *};
use crate::zakura::ZakuraClient;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::Write,
    net::SocketAddr,
    path::PathBuf,
};
use tokio::sync::watch;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub state_dir: PathBuf,
    pub salt_hex: String,
    pub window_blocks: u32,
    pub worker_origin: String,
    pub router_origin: String,
    pub private_listen: SocketAddr,
    #[serde(default)]
    pub public_enabled: bool,
}
type SharedArtifact = Arc<RwLock<Option<(Hash, Arc<Vec<u8>>)>>>;

#[derive(Clone)]
pub struct Publisher {
    config: Config,
    controller: Arc<RwLock<Option<Controller>>>,
    artifact: SharedArtifact,
    last_rows: SharedArtifact,
    permits: Arc<Semaphore>,
    healthy: Arc<AtomicBool>,
    http: reqwest::Client,
}
impl Publisher {
    pub fn new(config: Config) -> Result<Self, Failure> {
        loopback_origin(&config.worker_origin)?;
        loopback_origin(&config.router_origin)?;
        if !config.private_listen.ip().is_loopback() || config.window_blocks == 0 {
            return Err("invalid private Status configuration".into());
        }
        let salt: Hash = hex::decode(&config.salt_hex)?
            .try_into()
            .map_err(|_| "salt must be 32 bytes")?;
        if salt == [0; 32] {
            return Err("zero Status salt".into());
        }
        Ok(Self {
            config,
            controller: Arc::new(RwLock::new(None)),
            artifact: Arc::new(RwLock::new(None)),
            last_rows: Arc::new(RwLock::new(None)),
            permits: Arc::new(Semaphore::new(16)),
            healthy: Arc::new(AtomicBool::new(false)),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }
    pub fn routes(&self) -> Router {
        Router::new()
            .route("/v1/status/init", get(init))
            .route("/v1/status/session/:id", get(session))
            .route("/v1/status/query", post(forward))
            .layer(DefaultBodyLimit::max(512 * 1024))
            .with_state(self.clone())
    }
    fn http_state(&self) -> Result<HttpState, StatusCode> {
        if !self.healthy.load(Ordering::SeqCst) {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        let c = self
            .controller
            .read()
            .unwrap()
            .clone()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
        let mut state = HttpState::new(c, self.config.router_origin.clone());
        state.permits = self.permits.clone();
        Ok(state)
    }
    fn stop(&self) {
        self.healthy.store(false, Ordering::SeqCst);
        if let Some(c) = self.controller.write().unwrap().take() {
            c.revoke();
        }
    }
    /// Dedicated runtime bounds Status scheduling independently of Enhance's loop.
    pub fn start(&self, rpc: ZakuraClient) -> Result<(), Failure> {
        let this = self.clone();
        std::thread::Builder::new().name("status-controller".into()).spawn(move || {
            let result = tokio::runtime::Builder::new_multi_thread().worker_threads(2).max_blocking_threads(2)
                .enable_all().build().map_err(|e| -> Failure { Box::new(e) })
                .and_then(|runtime| runtime.block_on(this.run(rpc)));
            this.stop();
            if let Err(error) = result { tracing::error!(%error, "Status controller stopped; Enhance remains independent"); }
        })?;
        Ok(())
    }
    async fn post<T: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        origin: &str,
        path: &str,
        body: &T,
    ) -> Result<R, Failure> {
        let response = self
            .http
            .post(format!("{origin}{path}"))
            .json(body)
            .send()
            .await?;
        let bytes = bounded(response, 16 * 1024)
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::from_slice(&bytes)?)
    }
    async fn fence_roles(&self, epoch: u64) -> Result<[Binding; 2], Failure> {
        let mut bindings = Vec::new();
        for (origin, role) in [
            (&self.config.worker_origin, Role::Worker),
            (&self.config.router_origin, Role::Router),
        ] {
            let bytes = bounded(
                self.http
                    .get(format!("{origin}/control/health"))
                    .send()
                    .await?,
                16 * 1024,
            )
            .await
            .map_err(|e| e.to_string())?;
            let health: Health = serde_json::from_slice(&bytes)?;
            if health.role != role || health.binding.epoch > epoch {
                return Err("Status authority behind role fence".into());
            }
            let binding = Binding {
                epoch,
                incarnation: health.binding.incarnation,
            };
            let ack: Binding = self.post(origin, "/control/fence", &binding).await?;
            if ack != binding {
                return Err("incorrect Status fence acknowledgment".into());
            }
            bindings.push(binding);
        }
        Ok(bindings.try_into().unwrap())
    }
    async fn publish(
        &self,
        observed: &source::Observation,
        authority: &mut authority::Authority,
        bindings: &[Binding; 2],
        rpc: &ZakuraClient,
    ) -> Result<Manifest, Failure> {
        let (generation, recovery_epoch) = authority.next_identity()?;
        let snapshot = &observed.snapshot;
        let manifest = Manifest {
            protocol: PROTOCOL.into(),
            network: snapshot.network,
            salt: snapshot.salt,
            generation,
            recovery_epoch,
            coverage_start: snapshot.start,
            anchor_height: snapshot.height,
            anchor_hash: snapshot.anchor,
            observed_ms: observed.observed_ms,
            entries: snapshot.entries,
            rows_digest: snapshot.digest,
            public_digest: [0; 32],
        };
        manifest.fresh(now_ms())?;
        let previous = self
            .controller
            .read()
            .unwrap()
            .as_ref()
            .map(Controller::current);
        if let Some(previous) = previous.filter(|g| same_source(snapshot, &g.manifest)) {
            let mut refreshed = previous.manifest.clone();
            refreshed.generation = generation;
            refreshed.observed_ms = observed.observed_ms;
            for (origin, binding) in [
                (&self.config.worker_origin, &bindings[0]),
                (&self.config.router_origin, &bindings[1]),
            ] {
                let ack: Ready = self
                    .post(
                        origin,
                        "/control/prepare-refresh",
                        &Activate {
                            binding: binding.clone(),
                            manifest: refreshed.clone(),
                        },
                    )
                    .await?;
                if ack.binding != *binding || ack.manifest != refreshed {
                    return Err("wrong refresh acknowledgment".into());
                }
            }
            return self
                .commit_activate(authority, bindings, refreshed, previous.public.clone(), rpc)
                .await;
        }
        // Immutable candidate bytes are durable before remote preparation begins.
        let dir = self.config.state_dir.join("candidate");
        fs::create_dir_all(&dir)?;
        let mut file = File::create(dir.join("rows.tmp"))?;
        file.write_all(&snapshot.rows)?;
        file.sync_all()?;
        fs::rename(dir.join("rows.tmp"), dir.join("rows.bin"))?;
        let mut metadata = File::create(dir.join("manifest.tmp"))?;
        metadata.write_all(&serde_json::to_vec(&manifest)?)?;
        metadata.sync_all()?;
        fs::rename(dir.join("manifest.tmp"), dir.join("manifest.json"))?;
        File::open(&dir)?.sync_all()?;
        let health_bytes = bounded(
            self.http
                .get(format!("{}/control/health", self.config.worker_origin))
                .send()
                .await?,
            16 * 1024,
        )
        .await
        .map_err(|e| e.to_string())?;
        let health: Health = serde_json::from_slice(&health_bytes)?;
        if health.binding != bindings[0] {
            return Err("worker incarnation changed before transfer".into());
        }
        let previous = self.last_rows.read().unwrap().clone();
        let base = previous
            .as_ref()
            .filter(|(digest, _)| health.cached_rows == Some(*digest));
        let encoded = super::artifact::encode(
            &snapshot.rows,
            base.map(|(digest, rows)| (*digest, rows.as_slice())),
        )?;
        let transfer_digest = Sha256::digest(&encoded).into();
        tracing::info!(
            bytes = encoded.len(),
            full_bytes = snapshot.rows.len(),
            "Status snapshot transfer"
        );
        *self.artifact.write().unwrap() = Some((transfer_digest, Arc::new(encoded)));
        *self.last_rows.write().unwrap() = Some((snapshot.digest, Arc::new(snapshot.rows.clone())));
        let worker: Ready = self
            .post(
                &self.config.worker_origin,
                "/control/prepare",
                &Prepare {
                    binding: bindings[0].clone(),
                    manifest: manifest.clone(),
                    artifact_digest: transfer_digest,
                },
            )
            .await?;
        if worker.binding != bindings[0] || worker.manifest != manifest {
            return Err("worker prepared another candidate".into());
        }
        let router: Ready = self
            .post(
                &self.config.router_origin,
                "/control/prepare",
                &Prepare {
                    binding: bindings[1].clone(),
                    manifest: manifest.clone(),
                    artifact_digest: worker.artifact_digest,
                },
            )
            .await?;
        let mut expected = manifest.clone();
        expected.public_digest = router.manifest.public_digest;
        if router.binding != bindings[1]
            || router.manifest != expected
            || router.artifact_digest != expected.public_digest
        {
            return Err("router prepared another candidate".into());
        }
        let public = bounded(
            self.http
                .get(format!(
                    "{}/public/{}",
                    self.config.router_origin,
                    hex::encode(expected.public_digest)
                ))
                .send()
                .await?,
            2 * 1024 * 1024,
        )
        .await
        .map_err(|e| e.to_string())?;
        if <Hash>::from(Sha256::digest(&public)) != expected.public_digest {
            return Err("public material digest mismatch".into());
        }
        self.commit_activate(authority, bindings, expected, Arc::new(public), rpc)
            .await
    }
    async fn commit_activate(
        &self,
        authority: &mut authority::Authority,
        bindings: &[Binding; 2],
        expected: Manifest,
        public: Arc<Vec<u8>>,
        rpc: &ZakuraClient,
    ) -> Result<Manifest, Failure> {
        if canonical_hash(rpc, expected.anchor_height).await? != expected.anchor_hash {
            return Err("candidate anchor disconnected".into());
        }
        expected.fresh(now_ms())?;
        authority.commit(&expected)?;
        for (origin, binding) in [
            (&self.config.worker_origin, &bindings[0]),
            (&self.config.router_origin, &bindings[1]),
        ] {
            let ack: Ready = self
                .post(
                    origin,
                    "/control/activate",
                    &Activate {
                        binding: binding.clone(),
                        manifest: expected.clone(),
                    },
                )
                .await?;
            if ack.binding != *binding || ack.manifest != expected {
                return Err("wrong activation acknowledgment".into());
            }
        }
        let g = Generation {
            manifest: expected.clone(),
            public,
            units: Vec::new(),
            packing: Arc::new(Vec::new()),
            top: None,
            packing_digests: Vec::new(),
        };
        {
            let mut controller = self.controller.write().unwrap();
            if let Some(c) = controller.as_ref() {
                c.activate(g)?;
            } else {
                *controller = Some(Controller::new(g));
            }
        }
        self.healthy.store(true, Ordering::SeqCst);
        tracing::info!(
            generation = expected.generation,
            recovery_epoch = expected.recovery_epoch,
            source_observed_ms = expected.observed_ms,
            queryable_ms = now_ms(),
            "Status live publication"
        );
        Ok(expected)
    }
    async fn run(&self, rpc: ZakuraClient) -> Result<(), Failure> {
        let private = self.routes().merge(
            Router::new()
                .route("/artifact/:id", get(artifact))
                .with_state(self.clone()),
        );
        let listener = tokio::net::TcpListener::bind(self.config.private_listen).await?;
        let server = tokio::spawn(async move { axum::serve(listener, private).await });
        let salt: Hash = hex::decode(&self.config.salt_hex)?
            .try_into()
            .map_err(|_| "invalid salt")?;
        let mut source = source::RollingWindow::open(
            self.config.state_dir.join("source"),
            salt,
            self.config.window_blocks,
        )?;
        let (tx, mut rx) = watch::channel(None::<Arc<source::Observation>>);
        let source_rpc = rpc.clone();
        let observer = tokio::spawn(async move {
            loop {
                match source.observe(&source_rpc).await {
                    Ok(value) => {
                        if tx.send(Some(Arc::new(value))).is_err() {
                            break;
                        }
                    }
                    Err(error) => tracing::warn!(%error, "Status source observation failed"),
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        let mut last_observed = 0;
        loop {
            self.stop();
            if server.is_finished() || observer.is_finished() {
                return Err("Status background service stopped".into());
            }
            if rx.borrow().is_none() {
                rx.changed().await?;
            }
            let first = rx.borrow().clone().ok_or("Status source unavailable")?;
            let mut authority = authority::Authority::open(
                self.config.state_dir.join("authority"),
                first.snapshot.network,
                salt,
            )?;
            let (_, epoch) = authority.next_identity()?;
            let bindings = match self.fence_roles(epoch).await {
                Ok(b) => b,
                Err(error) => {
                    tracing::warn!(%error, "Status role fencing deferred");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };
            // Heartbeats never change observation freshness or authorize a recovery.
            let (active_tx, active_rx) = watch::channel(None::<Manifest>);
            let heartbeat_owner = self.clone();
            let heartbeat_bindings = bindings.clone();
            let heartbeat_rpc = rpc.clone();
            let heartbeat = tokio::spawn(async move {
                struct StopOnExit(Publisher);
                impl Drop for StopOnExit {
                    fn drop(&mut self) {
                        self.0.stop();
                    }
                }
                let _guard = StopOnExit(heartbeat_owner.clone());
                loop {
                    let active = active_rx.borrow().clone();
                    if let Some(manifest) = active {
                        if canonical_hash(&heartbeat_rpc, manifest.anchor_height).await?
                            != manifest.anchor_hash
                        {
                            heartbeat_owner.stop();
                            return Err::<(), Failure>("active Status anchor disconnected".into());
                        }
                        for (origin, binding) in [
                            (
                                &heartbeat_owner.config.worker_origin,
                                &heartbeat_bindings[0],
                            ),
                            (
                                &heartbeat_owner.config.router_origin,
                                &heartbeat_bindings[1],
                            ),
                        ] {
                            let ack: Binding = heartbeat_owner
                                .post(
                                    origin,
                                    "/control/heartbeat",
                                    &Activate {
                                        binding: binding.clone(),
                                        manifest: manifest.clone(),
                                    },
                                )
                                .await?;
                            if ack != *binding {
                                return Err("wrong Status heartbeat acknowledgment".into());
                            }
                        }
                    } else {
                        for (origin, binding) in [
                            (
                                &heartbeat_owner.config.worker_origin,
                                &heartbeat_bindings[0],
                            ),
                            (
                                &heartbeat_owner.config.router_origin,
                                &heartbeat_bindings[1],
                            ),
                        ] {
                            let bytes = bounded(
                                heartbeat_owner
                                    .http
                                    .get(format!("{origin}/control/health"))
                                    .send()
                                    .await?,
                                16 * 1024,
                            )
                            .await
                            .map_err(|e| e.to_string())?;
                            let health: Health = serde_json::from_slice(&bytes)?;
                            if health.binding != *binding {
                                return Err(
                                    "Status role restarted before initial publication".into()
                                );
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
            loop {
                let observed = rx
                    .borrow_and_update()
                    .clone()
                    .ok_or("Status source unavailable")?;
                if observed.observed_ms <= last_observed {
                    tokio::select! { result = rx.changed() => { result?; }, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                    if heartbeat.is_finished() {
                        break;
                    }
                    continue;
                }
                last_observed = observed.observed_ms;
                if let Some(current) = active_tx.borrow().as_ref() {
                    if same_source(&observed.snapshot, current)
                        && observed.observed_ms.saturating_sub(current.observed_ms) < 5_000
                    {
                        continue;
                    }
                }
                if heartbeat.is_finished() {
                    break;
                }
                let identity_before = authority.next_identity()?;
                match self
                    .publish(&observed, &mut authority, &bindings, &rpc)
                    .await
                {
                    Ok(manifest) => {
                        if heartbeat.is_finished() {
                            self.stop();
                            break;
                        }
                        active_tx.send(Some(manifest))?;
                    }
                    Err(error) => {
                        if authority.next_identity().ok() == Some(identity_before)
                            && !heartbeat.is_finished()
                        {
                            // Preparation/backpressure cannot grant serving authority.
                            // Preserve the committed view and try the next observation.
                            tracing::warn!(%error, "Status preparation deferred; committed view retained");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                        tracing::warn!(%error, "Status activation uncertain; fencing before recovery");
                        break;
                    }
                }
            }
            self.stop();
            heartbeat.abort();
            let _ = heartbeat.await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
fn same_source(s: &index::Snapshot, m: &Manifest) -> bool {
    s.network == m.network
        && s.salt == m.salt
        && s.digest == m.rows_digest
        && s.start == m.coverage_start
        && s.height == m.anchor_height
        && s.anchor == m.anchor_hash
        && s.entries == m.entries
}
async fn init(State(s): State<Publisher>) -> Result<Json<Manifest>, StatusCode> {
    super::init(State(s.http_state()?)).await
}
async fn session(
    State(s): State<Publisher>,
    Path(id): Path<String>,
) -> Result<Vec<u8>, StatusCode> {
    super::session(State(s.http_state()?), Path(id)).await
}
async fn forward(State(s): State<Publisher>, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    let result = super::forward(State(s.http_state()?), body).await?;
    s.http_state()?;
    Ok(result)
}
async fn artifact(
    State(s): State<Publisher>,
    Path(id): Path<String>,
) -> Result<Vec<u8>, StatusCode> {
    let artifact = s.artifact.read().unwrap();
    let (digest, bytes) = artifact.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    if hex::encode(digest) != id {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(bytes.as_ref().clone())
}

async fn canonical_hash(rpc: &ZakuraClient, height: u32) -> Result<Hash, Failure> {
    let hash: zakura_chain::block::Hash = rpc.block_hash(u64::from(height)).await?.parse()?;
    Ok(hash.0)
}
