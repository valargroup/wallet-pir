//! Independently scheduled Status controller hosted by the Enhance coordinator.
use super::{distributed::*, http::*, *};
use crate::admission::ClientSlots;
use crate::zakura::ZakuraClient;
use axum::http::HeaderMap;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use pir_pipeline::Input;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use status_query_server::admission;
use status_query_server::protocol::*;
use std::{
    fs::{self, File},
    io::Write,
    net::SocketAddr,
    path::PathBuf,
};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::sync::watch;

/// Concurrent forwarded queries admitted per client address.
pub const PER_CLIENT_CONCURRENCY: usize = 2;

/// First `X-Forwarded-For` address, else `X-Real-IP`, else `unknown`. The
/// ingress proxy in front of this loopback listener sets these headers; the
/// loopback peer address carries no client identity.
fn client_key(headers: &HeaderMap) -> String {
    crate::admission::client_key(headers, None)
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub state_dir: PathBuf,
    pub salt_hex: String,
    pub window_blocks: u32,
    pub worker_origin: String,
    pub router_origin: String,
    /// Optional independently tunneled query path; control remains on router_origin.
    #[serde(default)]
    pub query_router_origin: Option<String>,
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
    permits: Arc<admission::Admission>,
    clients: Arc<ClientSlots>,
    healthy: Arc<AtomicBool>,
    http: reqwest::Client,
    serving_http: reqwest::Client,
}
impl Publisher {
    pub fn new(config: Config) -> Result<Self, Failure> {
        loopback_origin(&config.worker_origin)?;
        loopback_origin(&config.router_origin)?;
        if let Some(origin) = &config.query_router_origin {
            loopback_origin(origin)?;
        }
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
            permits: Arc::new(admission::Admission::coordinator()),
            clients: Arc::new(ClientSlots::new(PER_CLIENT_CONCURRENCY)),
            healthy: Arc::new(AtomicBool::new(false)),
            serving_http: serving_http_client()?,
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
            .layer(axum::middleware::from_fn(|request, next| {
                telemetry::observe("coordinator", request, next)
            }))
            .layer(DefaultBodyLimit::max(512 * 1024))
            .with_state(self.clone())
    }
    fn http_state(&self) -> Result<HttpState, StatusCode> {
        if !self.healthy.load(Ordering::SeqCst) {
            telemetry::admission_rejected("coordinator", "publisher_unhealthy");
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        let c = self.controller.read().unwrap().clone().ok_or_else(|| {
            telemetry::admission_rejected("coordinator", "no_controller");
            StatusCode::SERVICE_UNAVAILABLE
        })?;
        Ok(HttpState {
            controller: c,
            origin: self
                .config
                .query_router_origin
                .as_ref()
                .unwrap_or(&self.config.router_origin)
                .clone(),
            http: self.serving_http.clone(),
            permits: self.permits.clone(),
        })
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
        // Identity is selected by the product authority; shared progress never
        // grants authority. A failed activation still requires outer fencing.
        let identity = authority.next_identity()?;
        match pir_pipeline::advance(
            Input::Work(observed),
            |observed| async move {
                self.prepare(observed, identity, bindings)
                    .await
                    .map(Input::Work)
            },
            |(manifest, public)| self.commit_activate(authority, bindings, manifest, public, rpc),
            &mut StatusProgress,
        )
        .await?
        {
            Input::Work(manifest) => Ok(manifest),
            Input::Unchanged | Input::Deferred => {
                unreachable!("Status preparation returns a candidate")
            }
        }
    }
    async fn prepare(
        &self,
        observed: &source::Observation,
        (generation, recovery_epoch): (u64, u64),
        bindings: &[Binding; 2],
    ) -> Result<(Manifest, Arc<Vec<u8>>), Failure> {
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
        if let Some(previous) = previous.filter(|g| same_source(snapshot, g.manifest())) {
            let mut refreshed = previous.manifest().clone();
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
            return Ok((refreshed, previous.public().clone()));
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
        Ok((expected, Arc::new(public)))
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
        let g = Generation::public_only(expected.clone(), public);
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
        let private = self.routes().merge(telemetry::routes()).merge(
            Router::new()
                .route("/artifact/:id", get(artifact))
                .with_state(self.clone()),
        );
        let listener = tokio::net::TcpListener::bind(self.config.private_listen).await?;
        let server = tokio::spawn(async move { axum::serve(listener, private).await });
        let salt: Hash = hex::decode(&self.config.salt_hex)?
            .try_into()
            .map_err(|_| "invalid salt")?;
        let source = source::RollingWindow::open(
            self.config.state_dir.join("source"),
            salt,
            self.config.window_blocks,
        )?;
        let (tx, mut rx) = watch::channel(None::<Arc<source::Observation>>);
        let source_rpc = rpc.clone();
        let observer = tokio::spawn(async move {
            let mut task = ObservationTask {
                source,
                rpc: source_rpc,
                tx,
            };
            pir_pipeline::supervise(&mut task, Duration::from_secs(1), std::future::pending()).await
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
                let mut task = HeartbeatTask {
                    owner: heartbeat_owner,
                    bindings: heartbeat_bindings,
                    rpc: heartbeat_rpc,
                    active: active_rx,
                };
                pir_pipeline::supervise(&mut task, Duration::from_secs(1), std::future::pending())
                    .await
            });
            loop {
                let observed = rx
                    .borrow_and_update()
                    .clone()
                    .ok_or("Status source unavailable")?;
                if observed.observed_ms <= last_observed {
                    let mut changed = Ok(());
                    pir_pipeline::wait(
                        tokio::time::Instant::now() + Duration::from_secs(1),
                        async {
                            changed = rx.changed().await;
                        },
                        std::future::pending(),
                    )
                    .await;
                    changed?;
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
// Observation only: these timings do not renew source freshness or a role lease.
struct StatusProgress;
impl pir_pipeline::Observer for StatusProgress {
    fn finished(&mut self, phase: pir_pipeline::Phase, elapsed: Duration, succeeded: bool) {
        tracing::debug!(
            ?phase,
            seconds = elapsed.as_secs_f64(),
            succeeded,
            "Status lifecycle phase"
        );
    }
}

/// Observation owns the checkpoint cache; it continues while publication waits
/// on remote preparation. Sending progress cannot refresh a served manifest.
struct ObservationTask {
    source: source::RollingWindow,
    rpc: ZakuraClient,
    tx: watch::Sender<Option<Arc<source::Observation>>>,
}
enum ObservationError {
    Source(source::SourceError),
    Closed,
}
impl pir_pipeline::Task for ObservationTask {
    type Error = ObservationError;
    async fn run(&mut self) -> Result<(), Self::Error> {
        let input = pir_pipeline::acquire(
            async { self.source.observe(&self.rpc).await.map(Input::Work) },
            &mut (),
        )
        .await
        .map_err(ObservationError::Source)?;
        if let Input::Work(value) = input {
            self.tx
                .send(Some(Arc::new(value)))
                .map_err(|_| ObservationError::Closed)?;
        }
        Ok(())
    }
    fn failed(&mut self, error: &Self::Error) -> pir_pipeline::FailurePolicy {
        match error {
            ObservationError::Source(error) => {
                tracing::warn!(%error, "Status source observation failed");
                pir_pipeline::FailurePolicy::NextPoll
            }
            ObservationError::Closed => pir_pipeline::FailurePolicy::Stop,
        }
    }
}

struct HeartbeatTask {
    owner: Publisher,
    bindings: [Binding; 2],
    rpc: ZakuraClient,
    active: watch::Receiver<Option<Manifest>>,
}
impl pir_pipeline::Task for HeartbeatTask {
    type Error = Failure;
    async fn run(&mut self) -> Result<(), Failure> {
        let active = self.active.borrow().clone();
        if let Some(manifest) = active {
            if canonical_hash(&self.rpc, manifest.anchor_height).await? != manifest.anchor_hash {
                self.owner.stop();
                return Err::<(), Failure>("active Status anchor disconnected".into());
            }
            for (origin, binding) in [
                (&self.owner.config.worker_origin, &self.bindings[0]),
                (&self.owner.config.router_origin, &self.bindings[1]),
            ] {
                let ack: Binding = self
                    .owner
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
                (&self.owner.config.worker_origin, &self.bindings[0]),
                (&self.owner.config.router_origin, &self.bindings[1]),
            ] {
                let bytes = bounded(
                    self.owner
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
                    return Err("Status role restarted before initial publication".into());
                }
            }
        }
        Ok(())
    }
    fn failed(&mut self, _: &Failure) -> pir_pipeline::FailurePolicy {
        // The outer owner revokes and fences before attempting recovery.
        pir_pipeline::FailurePolicy::Stop
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
    http::init(State(s.http_state()?)).await
}
async fn session(
    State(s): State<Publisher>,
    Path(id): Path<String>,
) -> Result<axum::body::Bytes, StatusCode> {
    http::session(State(s.http_state()?), Path(id)).await
}
async fn forward(
    State(s): State<Publisher>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    let http_state = s.http_state()?;
    let Some(_slot) = s.clients.try_acquire(&client_key(&headers)) else {
        telemetry::admission_rejected("coordinator", "client_concurrency");
        return Err(StatusCode::TOO_MANY_REQUESTS);
    };
    let result = http::forward(State(http_state), body).await?;
    s.http_state()?;
    Ok(result)
}
async fn artifact(
    State(s): State<Publisher>,
    Path(id): Path<String>,
) -> Result<axum::body::Bytes, StatusCode> {
    let bytes = {
        let artifact = s.artifact.read().unwrap();
        let (digest, bytes) = artifact.as_ref().ok_or(StatusCode::NOT_FOUND)?;
        if hex::encode(digest) != id {
            return Err(StatusCode::NOT_FOUND);
        }
        bytes.clone()
    };
    Ok(super::shared_bytes(bytes))
}

async fn canonical_hash(rpc: &ZakuraClient, height: u32) -> Result<Hash, Failure> {
    let hash = rpc.block_hash(u64::from(height)).await?;
    Ok(hash)
}

#[cfg(test)]
mod query_transport_tests {
    use super::*;
    fn config() -> Config {
        serde_json::from_value(serde_json::json!({
            "state_dir":"/unused", "salt_hex":hex::encode([1u8;32]), "window_blocks":1,
            "worker_origin":"http://127.0.0.1:8481", "router_origin":"http://127.0.0.1:8482",
            "private_listen":"127.0.0.1:8480"
        }))
        .unwrap()
    }
    #[tokio::test]
    async fn publisher_init_rejections_are_observed_as_http_server_errors() {
        use axum::{
            body::{to_bytes, Body},
            http::Request,
        };
        use tower::ServiceExt;
        let publisher = Publisher::new(config()).unwrap();
        let response = publisher
            .routes()
            .oneshot(Request::get("/v1/status/init").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let response = telemetry::routes()
            .oneshot(
                Request::get("/internal/status-apm")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let data: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
        assert!(
            data["operations"]["init"]["server_errors"]
                .as_u64()
                .unwrap()
                >= 1
        );
        assert!(data["operations"]["init"]["arrivals"].as_u64().unwrap() >= 1);
    }
    /// Exercise the existing publisher API at its HTTP control/source boundaries:
    /// neither failed refresh preparation nor failed anchor revalidation may advance
    /// authority or replace the previously committed public observation.
    #[tokio::test]
    async fn refresh_preparation_and_source_failures_keep_committed_observation_and_retry() {
        use axum::response::IntoResponse;
        use serde_json::{json, Value};
        let root = tempfile::tempdir().unwrap();
        let (source_rpc, source_service) = source::tests::fixture(false, false).await;
        let checkpoint = root.path().join("source/source.bin");
        let mut rolling =
            source::RollingWindow::open(root.path().join("source"), [1; 32], 1).unwrap();
        let observed = rolling.observe(&source_rpc).await.unwrap();
        let snapshot = &observed.snapshot;
        let public = Arc::new(vec![1, 2, 3]);
        let manifest = Manifest {
            protocol: PROTOCOL.into(),
            network: snapshot.network,
            salt: snapshot.salt,
            generation: 1,
            recovery_epoch: 1,
            coverage_start: snapshot.start,
            anchor_height: snapshot.height,
            anchor_hash: snapshot.anchor,
            observed_ms: observed.observed_ms,
            entries: snapshot.entries,
            rows_digest: snapshot.digest,
            public_digest: Sha256::digest(public.as_slice()).into(),
        };
        let refuse_prepare = Arc::new(AtomicBool::new(true));
        let pause_prepare = Arc::new(AtomicBool::new(false));
        let preparing = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let heartbeats = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let role = Router::new()
            .route(
                "/control/prepare-refresh",
                post({
                    let refuse = refuse_prepare.clone();
                    let pause = pause_prepare.clone();
                    let preparing = preparing.clone();
                    let release = release.clone();
                    move |Json(input): Json<Activate>| {
                        let (refuse, pause, preparing, release) = (
                            refuse.clone(),
                            pause.clone(),
                            preparing.clone(),
                            release.clone(),
                        );
                        async move {
                            if pause.load(Ordering::SeqCst) {
                                preparing.notify_one();
                                release.notified().await;
                            }
                            if refuse.load(Ordering::SeqCst) {
                                return StatusCode::SERVICE_UNAVAILABLE.into_response();
                            }
                            Json(Ready {
                                binding: input.binding,
                                artifact_digest: input.manifest.public_digest,
                                manifest: input.manifest,
                            })
                            .into_response()
                        }
                    }
                }),
            )
            .route(
                "/control/activate",
                post(|Json(input): Json<Activate>| async move {
                    Json(Ready {
                        binding: input.binding,
                        artifact_digest: input.manifest.public_digest,
                        manifest: input.manifest,
                    })
                }),
            );
        let role = role.route(
            "/control/heartbeat",
            post({
                let heartbeats = heartbeats.clone();
                move |Json(input): Json<Activate>| {
                    heartbeats.fetch_add(1, Ordering::SeqCst);
                    async move { Json(input.binding) }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let role_task = tokio::spawn(async move { axum::serve(listener, role).await.unwrap() });
        let source_down = Arc::new(AtomicBool::new(true));
        let rpc_routes = Router::new().route(
            "/",
            post({
                let source_down = source_down.clone();
                let anchor = snapshot.anchor;
                move |Json(request): Json<Value>| {
                    let source_down = source_down.clone();
                    async move {
                        assert_eq!(request["method"], "getblockhash");
                        if source_down.load(Ordering::SeqCst) {
                            return StatusCode::SERVICE_UNAVAILABLE.into_response();
                        }
                        Json(
                            json!({"result": zakura_server_chain::hash::display(&anchor),
                        "error": null, "id": request["id"]}),
                        )
                        .into_response()
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rpc_origin = format!("http://{}", listener.local_addr().unwrap());
        let rpc_task =
            tokio::spawn(async move { axum::serve(listener, rpc_routes).await.unwrap() });
        std::fs::write(root.path().join("cookie"), "test:test").unwrap();
        let rpc = ZakuraClient::from_cookie_file(&rpc_origin, root.path().join("cookie")).unwrap();
        let mut config = config();
        config.state_dir = root.path().join("publisher");
        config.worker_origin = origin.clone();
        config.router_origin = origin;
        config.salt_hex = hex::encode(snapshot.salt);
        let publisher = Publisher::new(config).unwrap();
        *publisher.controller.write().unwrap() = Some(Controller::new(Generation::public_only(
            manifest.clone(),
            public,
        )));
        publisher.healthy.store(true, Ordering::SeqCst);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let init = format!("http://{}/v1/status/init", listener.local_addr().unwrap());
        let routes = publisher.routes();
        let public_task = tokio::spawn(async move { axum::serve(listener, routes).await.unwrap() });
        let mut authority = authority::Authority::open(
            root.path().join("authority"),
            snapshot.network,
            snapshot.salt,
        )
        .unwrap();
        authority.commit(&manifest).unwrap();
        let identity = authority.next_identity().unwrap();
        let bindings = [
            Binding {
                epoch: 1,
                incarnation: "worker".into(),
            },
            Binding {
                epoch: 1,
                incarnation: "router".into(),
            },
        ];
        for preparation_fails in [true, false] {
            refuse_prepare.store(preparation_fails, Ordering::SeqCst);
            assert!(publisher
                .publish(&observed, &mut authority, &bindings, &rpc)
                .await
                .is_err());
            assert_eq!(authority.next_identity().unwrap(), identity);
            let served: Manifest = reqwest::get(&init).await.unwrap().json().await.unwrap();
            assert_eq!(served, manifest);
        }
        source_down.store(false, Ordering::SeqCst);
        // Run the production heartbeat task while actual shared preparation is
        // held at a real HTTP reply. Its renewals retain the old observation.
        pause_prepare.store(true, Ordering::SeqCst);
        let (_active_tx, active) = watch::channel(Some(manifest.clone()));
        let mut heartbeat = HeartbeatTask {
            owner: publisher.clone(),
            bindings: bindings.clone(),
            rpc: rpc.clone(),
            active,
        };
        let checkpoint_before = std::fs::read(&checkpoint).unwrap();
        let authority_before = std::fs::read(root.path().join("authority/authority.json")).unwrap();
        let (observations, source_progress) = watch::channel(None);
        let mut input = ObservationTask {
            source: rolling,
            rpc: source_rpc,
            tx: observations,
        };
        {
            let publication = publisher.publish(&observed, &mut authority, &bindings, &rpc);
            tokio::pin!(publication);
            tokio::select! {
                result = &mut publication => panic!("preparation did not pause: {result:?}"),
                _ = preparing.notified() => {},
            }
            let (source_shutdown, source_stopped) = tokio::sync::oneshot::channel();
            let source_task = tokio::spawn(async move {
                pir_pipeline::supervise(&mut input, Duration::from_secs(1), async {
                    let _ = source_stopped.await;
                })
                .await
            });
            let (shutdown, stopped) = tokio::sync::oneshot::channel();
            let heartbeat_task = tokio::spawn(async move {
                pir_pipeline::supervise(&mut heartbeat, Duration::from_secs(1), async {
                    let _ = stopped.await;
                })
                .await
            });
            tokio::time::timeout(Duration::from_secs(4), async {
                while heartbeats.load(Ordering::SeqCst) < 6 {
                    assert!(
                        !heartbeat_task.is_finished(),
                        "heartbeat stopped during preparation"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            shutdown.send(()).unwrap();
            heartbeat_task.await.unwrap().unwrap();
            assert_eq!(heartbeats.load(Ordering::SeqCst), 6);
            let progressed = source_progress
                .borrow()
                .clone()
                .expect("observation continues during prepare");
            assert!(progressed.observed_ms > manifest.observed_ms);
            assert_eq!(progressed.snapshot.digest, observed.snapshot.digest);
            assert_eq!(
                std::fs::read(&checkpoint).unwrap(),
                checkpoint_before,
                "unchanged source tip preserves the committed checkpoint"
            );
            assert!(!std::fs::read(&checkpoint).unwrap().is_empty());
            assert_eq!(
                std::fs::read(root.path().join("authority/authority.json")).unwrap(),
                authority_before
            );
            source_shutdown.send(()).unwrap();
            source_task
                .await
                .unwrap()
                .map_err(|_| "observation supervision failed")
                .unwrap();
            let served: Manifest = reqwest::get(&init).await.unwrap().json().await.unwrap();
            assert_eq!(served, manifest, "heartbeats cannot refresh source time");
            pause_prepare.store(false, Ordering::SeqCst);
            release.notify_one();
            // Both role preparations use the same HTTP fixture; after the
            // first is released the second no longer pauses.
            publication.await.unwrap();
        }
        let activated = publisher
            .publish(&observed, &mut authority, &bindings, &rpc)
            .await
            .unwrap();
        assert_eq!(activated.generation, identity.0 + 1);
        assert_eq!(activated.observed_ms, observed.observed_ms);
        let served: Manifest = reqwest::get(init).await.unwrap().json().await.unwrap();
        assert_eq!(served, activated);
        role_task.abort();
        rpc_task.abort();
        public_task.abort();
        source_service.abort();
        let _ = source_service.await;
    }

    #[test]
    fn per_client_slots_cap_concurrency_and_evict_idle_keys() {
        let slots = ClientSlots::new(PER_CLIENT_CONCURRENCY);
        let a1 = slots.try_acquire("1.2.3.4").expect("first slot");
        let a2 = slots.try_acquire("1.2.3.4").expect("second slot");
        assert!(slots.try_acquire("1.2.3.4").is_none(), "third is refused");
        // Another client is independent.
        let b1 = slots.try_acquire("5.6.7.8").expect("other client");
        assert_eq!(slots.tracked(), 2);
        drop(a1);
        let a3 = slots.try_acquire("1.2.3.4").expect("slot freed by drop");
        assert!(slots.try_acquire("1.2.3.4").is_none());
        drop(a2);
        drop(a3);
        assert_eq!(slots.tracked(), 1, "zero entries are evicted");
        drop(b1);
        assert_eq!(slots.tracked(), 0);
        // A refused acquisition never leaves a zero entry behind.
        let c1 = slots.try_acquire("unknown").unwrap();
        let c2 = slots.try_acquire("unknown").unwrap();
        assert!(slots.try_acquire("unknown").is_none());
        drop((c1, c2));
        assert_eq!(slots.tracked(), 0);
    }
    #[test]
    fn client_key_prefers_first_forwarded_address() {
        let mut h = HeaderMap::new();
        assert_eq!(client_key(&h), "unknown");
        h.insert("x-real-ip", "9.9.9.9".parse().unwrap());
        assert_eq!(client_key(&h), "9.9.9.9");
        h.insert(
            "x-forwarded-for",
            " 1.2.3.4 , 10.0.0.1, 127.0.0.1".parse().unwrap(),
        );
        assert_eq!(client_key(&h), "1.2.3.4");
        h.insert("x-forwarded-for", " ".parse().unwrap());
        assert_eq!(client_key(&h), "9.9.9.9");
    }
    #[test]
    fn optional_query_transport_is_loopback_only() {
        let mut c = config();
        assert!(c.query_router_origin.is_none());
        assert!(Publisher::new(c.clone()).is_ok());
        c.query_router_origin = Some("http://127.0.0.1:8483".into());
        assert!(Publisher::new(c.clone()).is_ok());
        for bad in [
            "http://example.com",
            "http://user:pass@127.0.0.1:8483",
            "http://127.0.0.1:8483/path",
        ] {
            c.query_router_origin = Some(bad.into());
            assert!(Publisher::new(c.clone()).is_err());
        }
    }
}

#[cfg(all(test, feature = "native-reinspiring"))]
mod lifecycle_wallet_tests {
    use super::*;
    #[allow(dead_code)]
    mod transport {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../status-query-server/tests/support/status_http.rs"
        ));
    }
    use transport::Loopback;
    use zakura_pir_status as wallet;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_status_lifecycle_collects_publishes_and_advances_over_real_roles() {
        // Exercise full production geometry with a bounded four-CPU preparation
        // lane, as the Transparent unit runner does. A child confines the existing
        // role constructor's environment setting; concurrent tests never see it.
        // The outer case budget includes child startup, work and teardown.
        const CHILD: &str = "WALLET_PIR_STATUS_LIFECYCLE_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = tokio::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "publisher::lifecycle_wallet_tests::shared_status_lifecycle_collects_publishes_and_advances_over_real_roles"])
                .env(CHILD, "1").env("STATUS_PREPARATION_THREADS", "4")
                .kill_on_drop(true).output().await.unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (rpc, source_task) = source::tests::fixture(false, false).await;
        let (tx, rx) = watch::channel(None);
        let mut input = ObservationTask {
            source: source::RollingWindow::open(root.path().join("source"), [1; 32], 1).unwrap(),
            rpc: rpc.clone(),
            tx,
        };
        pir_pipeline::Task::run(&mut input)
            .await
            .map_err(|_| "source collection")
            .unwrap();
        let observed = rx.borrow().clone().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let publisher_origin = format!("http://{}", listener.local_addr().unwrap());
        let worker_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let worker_origin = format!("http://{}", worker_listener.local_addr().unwrap());
        let router_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let router_origin = format!("http://{}", router_listener.local_addr().unwrap());
        let publisher = Publisher::new(Config {
            state_dir: root.path().join("publisher"),
            salt_hex: hex::encode([1; 32]),
            window_blocks: 1,
            worker_origin: worker_origin.clone(),
            router_origin: router_origin.clone(),
            query_router_origin: None,
            private_listen: listener.local_addr().unwrap(),
            public_enabled: true,
        })
        .unwrap();
        let app = publisher.routes().merge(
            Router::new()
                .route("/artifact/:id", get(artifact))
                .with_state(publisher.clone()),
        );
        let publisher_task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let worker = Service::open(
            Role::Worker,
            observed.snapshot.network,
            root.path().join("worker"),
            publisher_origin.clone(),
            worker_origin.clone(),
            false,
        )
        .unwrap();
        let router = Service::open(
            Role::Router,
            observed.snapshot.network,
            root.path().join("router"),
            worker_origin.clone(),
            worker_origin,
            false,
        )
        .unwrap();
        let worker_routes = worker.routes();
        let router_routes = router.routes();
        let worker_task =
            tokio::spawn(async move { axum::serve(worker_listener, worker_routes).await.unwrap() });
        let router_task =
            tokio::spawn(async move { axum::serve(router_listener, router_routes).await.unwrap() });
        // Reuse RAII listener cleanup from the existing independent-client test.
        let _services = [
            transport::Listening {
                origin: publisher_origin.clone(),
                task: publisher_task,
            },
            transport::Listening {
                origin: String::new(),
                task: worker_task,
            },
            transport::Listening {
                origin: String::new(),
                task: router_task,
            },
            transport::Listening {
                origin: String::new(),
                task: source_task,
            },
        ];
        let mut authority = authority::Authority::open(
            root.path().join("authority"),
            observed.snapshot.network,
            [1; 32],
        )
        .unwrap();
        let bindings = publisher
            .fence_roles(authority.next_identity().unwrap().1)
            .await
            .unwrap();
        let first = publisher
            .publish(&observed, &mut authority, &bindings, &rpc)
            .await
            .unwrap();
        assert!(root.path().join("source/source.bin").exists());
        let transport = Loopback {
            origin: publisher_origin,
            http: reqwest::Client::new(),
            corrupt_answer: AtomicBool::new(false),
        };
        let anchor = wallet::AcceptedAnchor {
            network: first.network,
            height: first.anchor_height,
            hash: first.anchor_hash,
        };
        let coverage = wallet::LocalCoverageContext {
            earliest_possible_inclusion: Some(1),
            required_through: Some(1),
        };
        let txid = zakura_server_chain::hash::parse_display(
            &zakura_server_chain::testing::status::regtest_genesis().2,
        )
        .unwrap();
        let client = wallet::transport::PendingClient::fetch(
            &transport,
            "https://status-wallet.invalid",
            now_ms,
        )
        .await
        .unwrap()
        .accept(&transport, &anchor)
        .await
        .unwrap();
        assert_eq!(
            client.observe(&transport, &txid, coverage).await.unwrap(),
            wallet::Observation::Mined(1)
        );
        pir_pipeline::Task::run(&mut input)
            .await
            .map_err(|_| "next source collection")
            .unwrap();
        let next_observed = rx.borrow().clone().unwrap();
        let next = publisher
            .publish(&next_observed, &mut authority, &bindings, &rpc)
            .await
            .unwrap();
        assert_eq!(next.generation, first.generation + 1);
        assert_eq!(next.rows_digest, first.rows_digest);
        assert_eq!(next.observed_ms, next_observed.observed_ms);
        let current = wallet::transport::PendingClient::fetch(
            &transport,
            "https://status-wallet.invalid",
            now_ms,
        )
        .await
        .unwrap()
        .accept(&transport, &anchor)
        .await
        .unwrap();
        assert_eq!(
            current.observe(&transport, &txid, coverage).await.unwrap(),
            wallet::Observation::Mined(1)
        );
        // Keep the original real HTTP wallet regression's malformed and
        // independent synthetic observation cases unchanged in its own target.
    }
}
