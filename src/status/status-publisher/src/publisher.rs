//! Status source capture and prepared HTTPS publications. Consumers never
//! participate in publication completion or renew its source observation time.
mod workflow;
use crate::{authority, source, zakura::ZakuraClient};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use ipir_sp::server::MatvecBackend;
use pir_pipeline::Input;
use serde::Deserialize;
use status_query_server::{now_ms, protocol::*, publication::*, telemetry, Generation};
use std::{
    fs,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::sync::watch;
use workflow::Publication;

type Failure = Box<dyn std::error::Error + Send + Sync>;
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub state_dir: PathBuf,
    pub salt_hex: String,
    pub window_blocks: u32,
    pub private_listen: SocketAddr,
}
#[derive(Default)]
struct Published {
    current: Option<Current>,
    manifest: Option<Manifest>,
    // An in-flight preparation holds its token, preventing pointer reuse even
    // when several withdrawals leave the public eligibility flag unchanged.
    fence: Arc<()>,
}
impl Published {
    fn check_fence(&self, expected: &Arc<()>) -> Result<(), Failure> {
        if !Arc::ptr_eq(&self.fence, expected) {
            return Err("Status authority changed during preparation".into());
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct Publisher {
    config: Config,
    published: Arc<RwLock<Published>>,
    rpc: Arc<RwLock<Option<ZakuraClient>>>,
    exported: Arc<std::sync::Mutex<Option<Inventory>>>,
}
impl Publisher {
    pub fn new(config: Config) -> Result<Self, Failure> {
        let salt: Hash = hex::decode(&config.salt_hex)?
            .try_into()
            .map_err(|_| "salt must be 32 bytes")?;
        if !config.private_listen.ip().is_loopback() || config.window_blocks == 0 || salt == [0; 32]
        {
            return Err("invalid private Status configuration".into());
        }
        Ok(Self {
            config,
            published: Arc::new(RwLock::new(Published::default())),
            rpc: Arc::new(RwLock::new(None)),
            exported: Arc::new(std::sync::Mutex::new(None)),
        })
    }
    pub fn private_routes(&self) -> Router {
        Router::new()
            .route("/control/active", get(active))
            .route(&format!("{PREFIX}current"), get(current))
            .route(&format!("{PREFIX}objects/:id"), get(object))
            .route(&format!("{PREFIX}anchors/check"), post(anchors))
            .layer(axum::extract::DefaultBodyLimit::max(METADATA_LIMIT))
            .with_state(self.clone())
            .merge(telemetry_routes())
    }
    fn root(&self) -> PathBuf {
        self.config.state_dir.join("distribution")
    }
    fn revision(&self) -> Result<u64, Failure> {
        let root = self.root();
        fs::create_dir_all(&root)?;
        let path = root.join("revision.json");
        let previous = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<u64>(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e.into()),
        };
        let next = previous.checked_add(1).ok_or("Status revision exhausted")?;
        durable(&root, "revision.json", &serde_json::to_vec(&next)?)?;
        Ok(next)
    }
    fn withdraw(&self) {
        let mut state = self.published.write().unwrap();
        state.fence = Arc::new(());
        if let Some(current) = &mut state.current {
            current.eligible = false;
        }
        state.manifest = None;
    }
    fn store(&self, bytes: &[u8]) -> Result<Object, Failure> {
        let object = Object::new(bytes);
        let root = self.root().join(pir_artifact::namespace("objects"));
        fs::create_dir_all(&root)?;
        let path = root.join(&object.checksum);
        if path.exists() {
            object.verify(&fs::read(path)?)?;
        } else {
            durable(&root, &object.checksum, bytes)?;
        }
        Ok(object)
    }
    /// Keep the current material even after expiry, plus all still-fresh
    /// predecessors. Query transfers pin open inodes; old observations cannot
    /// activate after their original freshness deadline.
    fn collect(&self, inventory: Inventory) -> Result<(), Failure> {
        let root = self.root();
        let mut retained: Vec<Inventory> = match fs::read(root.join("retained.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        retained.retain(|old| {
            old.manifest.fresh(now_ms()).is_ok() && old.manifest.id() != inventory.manifest.id()
        });
        retained.push(inventory);
        let mut keep = std::collections::BTreeSet::new();
        for inventory in &retained {
            keep.insert(digest(&serde_json::to_vec(inventory)?));
            for object in inventory
                .units
                .iter()
                .chain(&inventory.packing)
                .chain(std::iter::once(&inventory.public))
            {
                keep.insert(object.checksum.clone());
            }
        }
        durable(&root, "retained.json", &serde_json::to_vec(&retained)?)?;
        for entry in fs::read_dir(root.join(pir_artifact::namespace("objects")))? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if valid_digest(&name) && entry.file_type()?.is_file() && !keep.contains(&name) {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }
    async fn publish(
        &self,
        observed: Arc<source::Observation>,
        authority: &mut authority::Authority,
        cache: &mut Option<Generation>,
        rpc: &ZakuraClient,
    ) -> Result<Generation, Failure> {
        let expected = self.published.read().unwrap().fence.clone();
        let (generation, epoch) = authority.next_identity()?;
        let previous = cache.clone();
        let result = pir_pipeline::publisher::PublicationPass::new(
            async { Ok::<_, Failure>(Input::Work((expected, generation, epoch, previous))) },
            |(expected, generation, epoch, previous)| async move {
                let prepared =
                    tokio::task::spawn_blocking(move || -> Result<Generation, Failure> {
                        let (g, _) = Generation::prepare(
                            &observed.snapshot,
                            generation,
                            epoch,
                            observed.observed_ms,
                            previous.as_ref(),
                            MatvecBackend::Cpu,
                        )?;
                        Ok(g)
                    })
                    .await??;
                *cache = Some(prepared.clone());
                let publisher = self.clone();
                let candidate = prepared.clone();
                let inventory =
                    tokio::task::spawn_blocking(move || -> Result<Inventory, Failure> {
                        let mut cache = publisher.exported.lock().unwrap();
                        let inventory = if let Some(old) = cache.as_ref().filter(|i| {
                            i.manifest.network == candidate.manifest().network
                                && i.manifest.salt == candidate.manifest().salt
                                && i.manifest.rows_digest == candidate.manifest().rows_digest
                        }) {
                            let mut inventory = old.clone();
                            inventory.manifest = candidate.manifest().clone();
                            inventory
                        } else {
                            candidate.export_prepared(|bytes| publisher.store(bytes))?
                        };
                        *cache = Some(inventory.clone());
                        Ok(inventory)
                    })
                    .await??;
                let manifest = inventory.manifest.clone();
                let object = self.store(&serde_json::to_vec(&inventory)?)?;
                Ok(Input::Work((
                    expected, generation, epoch, prepared, inventory, manifest, object,
                )))
            },
            |(expected, generation, epoch, prepared, inventory, manifest, object)| async move {
                if rpc.block_hash(u64::from(manifest.anchor_height)).await? != manifest.anchor_hash
                {
                    self.withdraw();
                    return Err("Status candidate anchor disconnected".into());
                }
                // A withdrawal while preparation was running must survive this pass.
                // Hold the same lock through the durable commit and pointer switch so
                // a concurrent withdrawal cannot be replaced by an eligible view.
                {
                    let mut published = self.published.write().unwrap();
                    published.check_fence(&expected)?;
                    manifest.fresh(now_ms())?;
                    authority.commit(&manifest)?;
                    let current = Current {
                        schema: SCHEMA.into(),
                        revision: self.revision()?,
                        recovery_epoch: epoch,
                        network: manifest.network,
                        salt: manifest.salt,
                        generation: manifest.generation,
                        inventory: Some(object),
                        eligible: true,
                    };
                    // The current pointer is durable only after all referenced objects and
                    // authority. A restart allocates a new revision and recovery epoch.
                    durable(&self.root(), "current.json", &serde_json::to_vec(&current)?)?;
                    *published = Published {
                        current: Some(current),
                        manifest: Some(manifest.clone()),
                        fence: expected,
                    };
                }
                tracing::info!(
                    generation,
                    recovery_epoch = epoch,
                    source_observed_ms = manifest.observed_ms,
                    published_ms = now_ms(),
                    "Status prepared publication"
                );
                let publisher = self.clone();
                if let Err(error) =
                    tokio::task::spawn_blocking(move || publisher.collect(inventory)).await?
                {
                    tracing::warn!(%error, "Status artifact collection deferred");
                }
                Ok(prepared)
            },
        )
        .run(&mut ())
        .await?;
        match result {
            Input::Work(generation) => Ok(generation),
            Input::Unchanged | Input::Deferred => {
                unreachable!("Status preparation produces a candidate")
            }
        }
    }
    pub async fn run_managed(
        &self,
        rpc: ZakuraClient,
        http: pir_pipeline::service::Http<Router>,
        shutdown: pir_pipeline::service::Shutdown,
    ) -> Result<(), Failure> {
        let _lock = pir_pipeline::service::lock(&self.config.state_dir)?;
        *self.rpc.write().unwrap() = Some(rpc.clone());
        self.revision()?;
        struct Withdraw(Publisher);
        impl Drop for Withdraw {
            fn drop(&mut self) {
                self.0.withdraw();
            }
        }
        let _withdraw = Withdraw(self.clone());
        let mut tasks = pir_pipeline::service::Tasks::default();
        let server = tokio::spawn(http.serve(self.config.private_listen, self.private_routes()));
        tasks.own(&server);
        let salt = hex::decode(&self.config.salt_hex)?
            .try_into()
            .map_err(|_| "invalid salt")?;
        let source = source::RollingWindow::open(
            self.config.state_dir.join("source"),
            salt,
            self.config.window_blocks,
        )?;
        let (tx, rx) = watch::channel(None::<Arc<source::Observation>>);
        let source_rpc = rpc.clone();
        let observer = tokio::spawn(async move {
            let mut task = ObservationTask {
                source,
                rpc: source_rpc,
                tx,
            };
            pir_pipeline::supervise(&mut task, Duration::from_secs(1), std::future::pending()).await
        });
        tasks.own(&observer);
        pir_pipeline::publisher::PublisherService::new(Publication {
            publisher: self,
            rpc,
            rx,
            salt,
            authority: None,
            previous: None,
            last_observed: 0,
            server,
            observer,
        })
        .run(shutdown.cancelled())
        .await
    }
}

fn durable(root: &std::path::Path, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    pir_control::durable::write_atomic(root, name, |f| f.write_all(bytes))?;
    fs::set_permissions(root.join(name), {
        use std::os::unix::fs::PermissionsExt;
        fs::Permissions::from_mode(0o444)
    })?;
    pir_control::durable::sync_dir(root)
}
async fn active(State(s): State<Publisher>) -> Result<Json<Manifest>, StatusCode> {
    let manifest = s
        .published
        .read()
        .unwrap()
        .manifest
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    manifest
        .fresh(now_ms())
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(manifest))
}
async fn current(State(s): State<Publisher>, headers: HeaderMap) -> Result<Response, StatusCode> {
    let current = s
        .published
        .read()
        .unwrap()
        .current
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let token = format!("\"{}-{}\"", current.revision, current.eligible);
    let mut response = if headers.get("if-none-match").and_then(|s| s.to_str().ok()) == Some(&token)
    {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        Json(current).into_response()
    };
    response.headers_mut().insert(
        "etag",
        token
            .parse()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    );
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    Ok(response)
}
async fn object(
    State(s): State<Publisher>,
    Path(id): Path<String>,
) -> Result<Response, StatusCode> {
    if !valid_digest(&id) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let file = tokio::fs::File::open(s.root().join(pir_artifact::namespace("objects")).join(id))
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let stream = tokio_util::io::ReaderStream::new(file);
    Ok(Body::from_stream(stream).into_response())
}
async fn anchors(
    State(s): State<Publisher>,
    Json(request): Json<AnchorRequest>,
) -> Result<Json<AnchorResponse>, StatusCode> {
    if request.anchors.is_empty() || request.anchors.len() > MAX_ANCHORS {
        return Err(StatusCode::BAD_REQUEST);
    }
    let before = s
        .published
        .read()
        .unwrap()
        .current
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    if request.current != before || !before.eligible {
        return Err(StatusCode::CONFLICT);
    }
    let rpc = s
        .rpc
        .read()
        .unwrap()
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let mut canonical = Vec::new();
    for (height, hash) in request.anchors {
        canonical.push(
            rpc.block_hash(u64::from(height))
                .await
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
                == hash,
        );
    }
    if s.published.read().unwrap().current.as_ref() != Some(&before) {
        return Err(StatusCode::CONFLICT);
    }
    Ok(Json(AnchorResponse {
        current: before,
        canonical,
    }))
}
fn telemetry_routes() -> Router {
    Router::new()
        .route(
            "/internal/status-apm",
            get(|| async {
                Json(
                    tokio::task::spawn_blocking(telemetry::snapshot)
                        .await
                        .unwrap_or_default(),
                )
            }),
        )
        .route(
            "/internal/metrics",
            get(|| async {
                (
                    [(
                        axum::http::header::CONTENT_TYPE,
                        "text/plain; version=0.0.4",
                    )],
                    telemetry::prometheus(),
                )
                    .into_response()
            }),
        )
        .route("/internal/health", get(|| async { "ok\n" }))
}
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

#[cfg(all(test, feature = "fixtures", feature = "native-reinspiring"))]
#[path = "../../../pir-publisher/tests/included/status_lifecycle_wallet.rs"]
mod lifecycle_wallet_tests;

#[cfg(test)]
mod withdrawal_tests {
    use super::*;

    #[test]
    fn repeated_withdrawals_refuse_preparations_even_without_an_eligible_publication() {
        let root = tempfile::tempdir().unwrap();
        let publisher = Publisher::new(Config {
            state_dir: root.path().into(),
            salt_hex: "01".repeat(32),
            window_blocks: 1,
            private_listen: "127.0.0.1:8480".parse().unwrap(),
        })
        .unwrap();
        let before = publisher.published.read().unwrap().fence.clone();
        publisher.withdraw();
        let recovery = publisher.published.read().unwrap().fence.clone();
        publisher.withdraw();
        let published = publisher.published.read().unwrap();
        assert!(published.current.is_none());
        assert!(published.check_fence(&before).is_err());
        assert!(published.check_fence(&recovery).is_err());
        assert!(published.check_fence(&published.fence).is_ok());
    }
}
