//! Status source capture and prepared HTTPS publications. Consumers never
//! participate in publication completion or renew its source observation time.
mod ingestion;
mod preparation;
mod restore;
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
use pir_pipeline::publisher::PublicationInput;
use pir_publication::{
    conditional_token, AuthorityCheckRefusal, AuthorityTransition, PublicationAuthority,
    PublicationReference, PublishedObjectStore, SubjectAnswer, AUTHORITY_CHECK_PATH, CURRENT_PATH,
    OBJECTS_PATH,
};
use serde::Deserialize;
use status_shared::{now_ms, protocol::*, publication::*, telemetry, Generation};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, RwLock},
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
    /// Publish `inventory`, stored as `object`, unless authority advanced past
    /// `expected` or its manifest is no longer fresh: retain its objects, commit
    /// its generation, then make it current.
    fn commit_inventory(
        &self,
        authority: &mut authority::Authority,
        inventory: &Inventory,
        object: pir_publication::ObjectReference,
        expected: u64,
        shutdown: &pir_pipeline::service::Shutdown,
    ) -> Result<(), Failure> {
        let manifest = &inventory.manifest;
        // The same guard fences withdrawal and durable advancement after RPC.
        let mut published = self.published.write().unwrap();
        if shutdown.requested() {
            return Err("Status publication stopped".into());
        }
        published.check_fence(expected)?;
        manifest.fresh(now_ms())?;
        self.objects.retain_publication(
            std::iter::once(object.clone()).chain(inventory.referenced_objects().cloned()),
            manifest.observed_ms.saturating_add(MAX_AGE_MS),
            now_ms(),
        )?;
        authority.commit(manifest)?;
        published
            .authority
            .as_mut()
            .ok_or("Status publication authority is not open")?
            .apply(AuthorityTransition::Publish(PublicationReference::new(
                inventory.material_identity(),
                object,
                StatusDescriptor {
                    generation: manifest.generation,
                },
            )))?;
        published.manifest = Some(manifest.clone());
        *self.exported.lock().unwrap() = Some(inventory.clone());
        Ok(())
    }
    /// Supervise the private listener, root-scheduled window ingestion and
    /// publication until shutdown. Ingestion finishes its current operation
    /// before the publication loop releases the state lock.
    pub async fn run_managed<E>(
        &self,
        rpc: ZakuraClient,
        feed: pir_pipeline::ingestion::ChainFeed,
        http: pir_pipeline::service::Http<Router>,
        shutdown: pir_pipeline::service::Shutdown,
        executor: E,
    ) -> Result<(), Failure>
    where
        E: pir_pipeline::publisher::PublisherExecutor + pir_pipeline::ingestion::IngestionExecutor,
    {
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
        let window = ingestion::StatusIngestion {
            source,
            rpc: rpc.clone(),
            publisher: self.clone(),
            tx,
        };
        // Canonical lookups use this runtime's client, not the host's.
        let feed = pir_pipeline::ingestion::ChainFeed {
            chain: Arc::new(ingestion::NodeChain(rpc.clone())),
            observations: feed.observations,
        };
        let scheduler = executor.clone();
        let stop = shutdown.clone();
        let observer = tokio::spawn(async move { scheduler.ingest(window, feed, stop).await });
        let result = executor
            .run(
                Publication {
                    publisher: self,
                    rpc,
                    rx,
                    salt,
                    authority: None,
                    previous: None,
                    restorable: true,
                    last_observed: 0,
                    server,
                    observer: &observer,
                    shutdown: shutdown.clone(),
                },
                shutdown.clone(),
            )
            .await;
        // Ingestion completes its current operation before ownership is released.
        shutdown.request();
        let _ = observer.await;
        result
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
/// Answer whether each requested anchor is still canonical, naming the anchor
/// it answers, only while the request's authority is current and eligible
/// before and after the node lookups. An answer renews no manifest freshness.
async fn authority_check(
    State(s): State<Publisher>,
    Json(request): Json<AnchorCheckRequest>,
) -> Result<Json<AnchorCheckResponse>, StatusCode> {
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
    let mut answers = Vec::new();
    let mut canonical = std::collections::BTreeMap::new();
    for &(height, hash) in &request.subjects {
        // Several serving identities usually name the same anchor. Check
        // each height once per request, rather than stretching this live
        // authority round across another publication with duplicate RPCs.
        let canonical_hash = if let Some(hash) = canonical.get(&height) {
            *hash
        } else {
            let hash = rpc
                .block_hash(u64::from(height))
                .await
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
            canonical.insert(height, hash);
            hash
        };
        let still_authoritative = canonical_hash == hash;
        answers.push(SubjectAnswer {
            subject: (height, hash),
            still_authoritative,
        });
    }
    admit()?;
    Ok(Json(AnchorCheckResponse {
        authority: request.authority,
        answers,
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
#[cfg(all(test, feature = "fixtures", feature = "native-reinspiring"))]
#[path = "../../../pir-publisher/tests/included/status_lifecycle_wallet.rs"]
mod lifecycle_wallet_tests;

#[cfg(test)]
mod withdrawal_tests {
    use super::*;

    #[cfg(feature = "fixtures")]
    #[tokio::test]
    async fn repeated_anchor_checks_share_one_live_lookup_but_not_across_requests() {
        use std::sync::atomic::Ordering;
        let root = tempfile::tempdir().unwrap();
        let publisher = Publisher::new(Config {
            state_dir: root.path().into(),
            salt_hex: "01".repeat(32),
            window_blocks: 1,
            private_listen: "127.0.0.1:8480".parse().unwrap(),
        })
        .unwrap();
        let (rpc, server, mock) = source::tests::controlled_chain_fixture(false, false).await;
        let anchor = rpc.block_hash(1).await.unwrap();
        mock.hash_reads.store(0, Ordering::SeqCst);
        *publisher.rpc.write().unwrap() = Some(rpc);
        publisher.open_authority([1; 32], [1; 32]).unwrap();
        let current = publisher
            .published
            .write()
            .unwrap()
            .authority
            .as_mut()
            .unwrap()
            .apply(AuthorityTransition::Publish(PublicationReference::new(
                [7; 32],
                pir_publication::ObjectReference::for_bytes(b"inventory"),
                StatusDescriptor { generation: 1 },
            )))
            .unwrap()
            .clone();
        let request = AnchorCheckRequest {
            authority: current.authority().unwrap(),
            subjects: vec![(1, anchor), (1, [0xff; 32]), (1, anchor)],
        };
        for reads in [1, 2] {
            let answer = authority_check(State(publisher.clone()), Json(request.clone()))
                .await
                .unwrap()
                .0;
            assert_eq!(
                answer.answers.iter().map(|a| a.subject).collect::<Vec<_>>(),
                request.subjects
            );
            assert_eq!(request.verdicts(answer).unwrap(), [true, false, true]);
            assert_eq!(mock.hash_reads.load(Ordering::SeqCst), reads);
        }
        publisher.withdraw();
        assert_eq!(
            authority_check(State(publisher), Json(request)).await.err(),
            Some(StatusCode::CONFLICT)
        );
        assert_eq!(mock.hash_reads.load(Ordering::SeqCst), 2);
        server.abort();
    }

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
