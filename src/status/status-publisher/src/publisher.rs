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
use pir_pipeline::Input;
use pir_publication::{
    conditional_token, AuthorityCheckRefusal, AuthorityCheckResponse, AuthorityTransition,
    PublicationAuthority, PublicationReference, PublishedObjectStore, AUTHORITY_CHECK_PATH,
    CURRENT_PATH, OBJECTS_PATH,
};
use serde::Deserialize;
use status_shared::{now_ms, protocol::*, publication::*, telemetry, Generation};
use std::{
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
    /// The shared publication authority, opened once the chain is known.
    authority: Option<PublicationAuthority<StatusDescriptor>>,
    manifest: Option<Manifest>,
}
impl Published {
    /// The authority sequence a preparation starts under. Every withdrawal
    /// and recovery advances it, so a preparation that began earlier cannot
    /// publish.
    fn preparation_fence(&self) -> Result<(u64, u64), Failure> {
        let current = self
            .authority
            .as_ref()
            .ok_or("Status publication authority is not open")?
            .current();
        Ok((current.sequence, current.fence.recovery_epoch))
    }
    fn check_fence(&self, expected_sequence: u64) -> Result<(), Failure> {
        if self.preparation_fence()?.0 != expected_sequence {
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
    /// Distributed objects; only retained publications are served.
    objects: Arc<PublishedObjectStore>,
}
impl Config {
    /// Check the configuration without opening state.
    pub fn validate(&self) -> Result<(), Failure> {
        let salt: Hash = hex::decode(&self.salt_hex)?
            .try_into()
            .map_err(|_| "salt must be 32 bytes")?;
        if !self.private_listen.ip().is_loopback() || self.window_blocks == 0 || salt == [0; 32] {
            return Err("invalid private Status configuration".into());
        }
        Ok(())
    }
}
impl Publisher {
    /// Validate `config` and open its distributed-object store.
    pub fn new(config: Config) -> Result<Self, Failure> {
        config.validate()?;
        let objects = PublishedObjectStore::open(config.state_dir.join("distribution/objects"))?;
        Ok(Self {
            config,
            published: Arc::new(RwLock::new(Published::default())),
            rpc: Arc::new(RwLock::new(None)),
            exported: Arc::new(std::sync::Mutex::new(None)),
            objects: Arc::new(objects),
        })
    }
    pub fn private_routes(&self) -> Router {
        Router::new()
            .route("/control/active", get(active))
            .route(&format!("{PREFIX}{CURRENT_PATH}"), get(current))
            .route(&format!("{PREFIX}{OBJECTS_PATH}:id"), get(object))
            .route(
                &format!("{PREFIX}{AUTHORITY_CHECK_PATH}"),
                post(authority_check),
            )
            .layer(axum::extract::DefaultBodyLimit::max(METADATA_LIMIT))
            .with_state(self.clone())
            .merge(telemetry_routes())
    }
    /// Open the publication authority for `network` unless it is open. Opening
    /// starts a new recovery epoch.
    fn open_authority(&self, network: Hash, salt: Hash) -> Result<(), Failure> {
        let mut published = self.published.write().unwrap();
        if published.authority.is_none() {
            published.authority = Some(PublicationAuthority::open(
                self.config.state_dir.join("distribution/publication.json"),
                scope(network, salt),
            )?);
        }
        Ok(())
    }
    /// Stop serving the current publication and refuse in-flight preparations.
    fn withdraw(&self) {
        self.transition(AuthorityTransition::Withdraw);
    }
    /// Whether the latest authority transition was a withdrawal.
    fn last_transition_withdrew(&self) -> bool {
        self.published
            .read()
            .unwrap()
            .authority
            .as_ref()
            .is_some_and(|authority| {
                let current = authority.current();
                !current.eligible && current.fence.latest_withdrawal == current.sequence
            })
    }
    /// Start a new recovery epoch after a withdrawal or an uncertain write.
    fn recover(&self) {
        self.transition(AuthorityTransition::Recover);
    }
    fn transition(&self, transition: AuthorityTransition<StatusDescriptor>) {
        let mut published = self.published.write().unwrap();
        published.manifest = None;
        if let Some(authority) = &mut published.authority {
            if let Err(error) = authority.apply(transition) {
                tracing::warn!(%error, "Status authority transition was not durable");
            }
        }
    }
    async fn publish(
        &self,
        observed: Arc<source::Observation>,
        authority: &mut authority::Authority,
        cache: &mut Option<Generation>,
        rpc: &ZakuraClient,
    ) -> Result<Generation, Failure> {
        let (expected, epoch) = self.published.read().unwrap().preparation_fence()?;
        let generation = authority.next_generation()?;
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
                            candidate
                                .export_prepared(|bytes| Ok(publisher.objects.store(bytes)?))?
                        };
                        *cache = Some(inventory.clone());
                        Ok(inventory)
                    })
                    .await??;
                let manifest = inventory.manifest.clone();
                let object = self.objects.store(&serde_json::to_vec(&inventory)?)?;
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
                    published.check_fence(expected)?;
                    manifest.fresh(now_ms())?;
                    // Authorize the referenced objects before any consumer can
                    // see the pointer. Still-activatable predecessors stay
                    // served so peers can finish handover; queries pin inodes.
                    self.objects.retain_publication(
                        std::iter::once(object.clone())
                            .chain(inventory.referenced_objects().cloned()),
                        manifest.observed_ms.saturating_add(MAX_AGE_MS),
                        now_ms(),
                    )?;
                    authority.commit(&manifest)?;
                    // The envelope is published only after all referenced objects and
                    // the generation are durable. A restart starts a new recovery epoch.
                    let publication = PublicationReference::new(
                        inventory.material_identity(),
                        object,
                        StatusDescriptor {
                            generation: manifest.generation,
                        },
                    );
                    published
                        .authority
                        .as_mut()
                        .ok_or("Status publication authority is not open")?
                        .apply(AuthorityTransition::Publish(publication))?;
                    published.manifest = Some(manifest.clone());
                }
                tracing::info!(
                    generation,
                    recovery_epoch = epoch,
                    source_observed_ms = manifest.observed_ms,
                    published_ms = now_ms(),
                    "Status prepared publication"
                );
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
        .authority
        .as_ref()
        .map(|authority| authority.current().clone())
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let token = conditional_token(&current);
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
    let file = s
        .objects
        .open_retained(&id)
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
            _ => StatusCode::NOT_FOUND,
        })?;
    let stream = tokio_util::io::ReaderStream::new(tokio::fs::File::from_std(file));
    Ok(Body::from_stream(stream).into_response())
}
/// Answer whether anchors are still canonical, only while the request's
/// authority is current and eligible before and after the node lookups.
async fn authority_check(
    State(s): State<Publisher>,
    Json(request): Json<AnchorCheckRequest>,
) -> Result<Json<AuthorityCheckResponse>, StatusCode> {
    let admit = || {
        let published = s.published.read().unwrap();
        let current = published
            .authority
            .as_ref()
            .ok_or(StatusCode::CONFLICT)?
            .current();
        request.admit(current).map_err(|refusal| match refusal {
            AuthorityCheckRefusal::MalformedRequest => StatusCode::BAD_REQUEST,
            AuthorityCheckRefusal::AuthorityAdvanced => StatusCode::CONFLICT,
        })
    };
    admit()?;
    let rpc = s
        .rpc
        .read()
        .unwrap()
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let mut still_authoritative = Vec::new();
    for (height, hash) in &request.subjects {
        still_authoritative.push(
            rpc.block_hash(u64::from(*height))
                .await
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
                == *hash,
        );
    }
    admit()?;
    Ok(Json(AuthorityCheckResponse {
        authority: request.authority,
        still_authoritative,
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
    fn withdrawals_and_recoveries_refuse_earlier_preparations() {
        let root = tempfile::tempdir().unwrap();
        let publisher = Publisher::new(Config {
            state_dir: root.path().into(),
            salt_hex: "01".repeat(32),
            window_blocks: 1,
            private_listen: "127.0.0.1:8480".parse().unwrap(),
        })
        .unwrap();
        assert!(publisher
            .published
            .read()
            .unwrap()
            .preparation_fence()
            .is_err());
        publisher.open_authority([1; 32], [1; 32]).unwrap();
        let (before, _) = publisher
            .published
            .read()
            .unwrap()
            .preparation_fence()
            .unwrap();
        publisher.withdraw();
        let (withdrawn, _) = publisher
            .published
            .read()
            .unwrap()
            .preparation_fence()
            .unwrap();
        publisher.withdraw();
        publisher.recover();
        let published = publisher.published.read().unwrap();
        let current = published.authority.as_ref().unwrap().current();
        assert!(!current.eligible);
        assert_eq!(current.fence.recovery_epoch, 2);
        assert!(published.check_fence(before).is_err());
        assert!(published.check_fence(withdrawn).is_err());
        assert!(published.check_fence(current.sequence).is_ok());
    }

    /// A failed newer publication followed by a failed withdrawal leaves the
    /// old publication withdrawn: neither `current` nor an authority check
    /// grants it, and a restart cannot restore it.
    #[tokio::test]
    async fn failed_authority_writes_revoke_live_authority_checks() {
        let root = tempfile::tempdir().unwrap();
        let config = Config {
            state_dir: root.path().into(),
            salt_hex: "01".repeat(32),
            window_blocks: 1,
            private_listen: "127.0.0.1:8480".parse().unwrap(),
        };
        let publisher = Publisher::new(config.clone()).unwrap();
        publisher.open_authority([1; 32], [1; 32]).unwrap();
        let publication = |generation| {
            AuthorityTransition::Publish(PublicationReference::new(
                [generation as u8; 32],
                pir_publication::ObjectReference::for_bytes(b"inventory"),
                StatusDescriptor { generation },
            ))
        };
        let published = {
            let mut state = publisher.published.write().unwrap();
            let authority = state.authority.as_mut().unwrap();
            authority.apply(publication(1)).unwrap().clone()
        };
        let request = AnchorCheckRequest {
            authority: published.authority().unwrap(),
            subjects: vec![(1, [3; 32])],
        };

        let distribution = root.path().join("distribution");
        let saved = std::fs::read(distribution.join("publication.json")).unwrap();
        let objects = root.path().join("objects-moved");
        std::fs::rename(&distribution, &objects).unwrap();
        std::fs::write(&distribution, b"not a directory").unwrap();
        {
            let mut state = publisher.published.write().unwrap();
            let authority = state.authority.as_mut().unwrap();
            assert!(authority.apply(publication(2)).is_err());
        }
        publisher.withdraw();

        let current = publisher
            .published
            .read()
            .unwrap()
            .authority
            .as_ref()
            .unwrap()
            .current()
            .clone();
        assert!(!current.eligible && current.sequence > published.sequence);
        assert_eq!(
            authority_check(State(publisher.clone()), Json(request.clone()))
                .await
                .err(),
            Some(StatusCode::CONFLICT)
        );

        drop(publisher);
        std::fs::remove_file(&distribution).unwrap();
        std::fs::rename(&objects, &distribution).unwrap();
        std::fs::write(distribution.join("publication.json"), saved).unwrap();
        let restarted = Publisher::new(config).unwrap();
        restarted.open_authority([1; 32], [1; 32]).unwrap();
        let recovered = restarted
            .published
            .read()
            .unwrap()
            .authority
            .as_ref()
            .unwrap()
            .current()
            .clone();
        assert!(!recovered.eligible && recovered.sequence > current.sequence);
        assert_eq!(
            authority_check(State(restarted), Json(request)).await.err(),
            Some(StatusCode::CONFLICT)
        );
    }
}
