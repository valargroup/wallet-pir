//! Filter ingestion and serving run independently of PIR preparation and rotation.
use crate::{
    application::{self, Config, Result},
    canonical::{freshest, is_canonical},
};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use receiver_directory::{seen::SeenSet, Hash};
use receiver_pir_server::Publications;
use receiver_store::{archive::ArchiveStore, Config as StoreConfig, ProviderStore, Store};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{info, warn};
use zakura_server_chain::NodeClient;

/// Continuity state lives separately from the rebuildable block index.
pub const FILE: &str = "archive.sqlite";

/// Exact archive database and network boundary, shared by HTTP reads and the guard.
#[derive(Clone)]
pub struct Location {
    pub path: PathBuf,
    pub config: StoreConfig,
}

/// Collects on a dedicated owner thread so witness construction cannot delay filter ingestion.
/// The returned completion resolves only after the thread has exited.
pub fn start(
    mut config: Config,
    nodes: Vec<NodeClient>,
    genesis: Hash,
    publications: Publications,
    poll: Duration,
    shutdown: pir_pipeline::service::Shutdown,
) -> Result<tokio::sync::oneshot::Receiver<Result<()>>> {
    config.depth = config.filter_depth.unwrap_or(config.depth);
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("receiver-filters".into())
        .spawn(move || {
            let result = (|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                runtime.block_on(async {
                    loop {
                        if let Err(error) = ingest(&config, &nodes, genesis, &publications).await {
                            warn!(%error,"filter ingestion deferred");
                        }
                        tokio::select! {
                            _ = shutdown.cancelled() => return Ok(()),
                            _ = tokio::time::sleep(poll) => {}
                        }
                    }
                })
            })();
            let _ = sender.send(result);
        })?;
    Ok(receiver)
}

/// Commits the chain and publishes filters without entering the PIR preparation phase.
async fn ingest(
    config: &Config,
    nodes: &[NodeClient],
    genesis: Hash,
    publications: &Publications,
) -> Result<()> {
    let started = std::time::Instant::now();
    let (tip, rpc) = freshest(nodes, genesis).await?;
    let boundary = crate::blocks::boundary(&rpc, config.start_height - 1).await?;
    let mut store = Store::open(
        config.data_dir.join("directory.sqlite"),
        StoreConfig {
            genesis,
            start_height: config.start_height,
            start_parent: boundary.hash,
            start_position: boundary.position,
        },
    )?;
    let mut collected = application::collect(
        config,
        &rpc,
        tip,
        &mut store,
        Some(publications),
        genesis,
        started,
    )
    .await?;
    collected.publish_filters().await?;
    drop(collected);
    let mut archive = ArchiveStore::open(config.data_dir.join(FILE), store.config().clone())?;
    publications.with_protected_sessions(|ids| archive.prune(application::unix_now(), ids))?;
    if let Some(head) = archive.head()? {
        info!(
            node_tip = tip,
            filter_height = head.frontier.height,
            depth = config.depth,
            ingestion_us = started.elapsed().as_micros() as u64,
            "receiver filter ready"
        );
    }
    Ok(())
}

/// Provider unavailability suppresses only seen issuance evidence, never paid block filters.
pub async fn publish(config: &Config, rpc: &NodeClient, store: &Store) -> Result<()> {
    let started = std::time::Instant::now();
    let mut archive = ArchiveStore::open(config.data_dir.join(FILE), store.config().clone())?;
    let seen = if config.providers_enabled {
        let capture = (|| -> Result<_> {
            let providers = ProviderStore::open(config.data_dir.join(application::PROVIDERS))?;
            let sets = crate::near::provider_sets(&providers)?;
            let Some(set) = sets.into_iter().find(|s| s.label == "near-intents/seen") else {
                return Ok(None);
            };
            let mut seen =
                SeenSet::new(store.config().genesis, set.since_unix).union(&set.receivers);
            seen.until_unix = set.until_unix;
            Ok(Some(archive.record_seen(&seen)?))
        })();
        match capture {
            Ok(seen) => seen,
            Err(error) => {
                warn!(%error,"seen unavailable; continuing paid filter publication");
                None
            }
        }
    } else {
        None
    };
    let head = store.stage_archive(&mut archive, seen, application::unix_now())?;
    if !is_canonical(rpc, head.frontier.height, head.frontier.hash).await? {
        archive.revoke(application::unix_now())?;
        return Err("chain changed during filter preparation".into());
    }
    rpc.check_network(head.genesis).await?;
    archive.activate(&head, application::unix_now())?;
    info!(
        height = head.frontier.height,
        epoch = head.epoch,
        build_us = started.elapsed().as_micros() as u64,
        "receiver filter publication"
    );
    Ok(())
}

/// Checks archive anchors independently of indexing and PIR work. RPC failure proves no fork.
pub async fn guard(location: Location, nodes: Vec<NodeClient>, poll: Duration) {
    loop {
        let checked = async {
            let (tip, rpc) = freshest(&nodes, location.config.genesis).await?;
            let read = location.clone();
            let head = tokio::task::spawn_blocking(move || {
                ArchiveStore::read(read.path, read.config)?.head()
            })
            .await??;
            if let Some(head) = head {
                if u64::from(head.frontier.height) <= tip
                    && !is_canonical(&rpc, head.frontier.height, head.frontier.hash).await?
                {
                    rpc.check_network(head.genesis).await?;
                    let write = location.clone();
                    tokio::task::spawn_blocking(move || {
                        ArchiveStore::open(write.path, write.config)?
                            .revoke_if_head(&head, application::unix_now())
                    })
                    .await??;
                }
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        };
        if let Err(error) = checked.await {
            warn!(%error,"filter canonical check deferred");
        }
        tokio::time::sleep(poll).await;
    }
}

#[derive(Clone)]
struct Service {
    location: Location,
    slots: Arc<Semaphore>,
}

/// Keeps the response allocation's slot until the transport drops its bytes, including cancellation.
struct Output {
    bytes: Vec<u8>,
    _slot: OwnedSemaphorePermit,
}
impl AsRef<[u8]> for Output {
    /// Lets `Bytes` share the allocation without releasing its resource lease.
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// Serves bounded immutable archive objects. Unknown revisions return 410, never an empty filter.
pub fn router(location: Location) -> Router {
    Router::new()
        .route(
            "/v1/receiver/archive/head",
            get(|State(s): State<Service>| async move {
                read(s, |store| {
                    store
                        .head()?
                        .ok_or(receiver_store::Error::Coverage)?
                        .encode()
                        .map_err(Into::into)
                })
                .await
            }),
        )
        .route(
            "/v1/receiver/archive/range/:head/:page",
            get(
                |State(s): State<Service>, Path((head, page)): Path<(String, u32)>| async move {
                    let Ok(head) = decode_id(&head) else {
                        return StatusCode::BAD_REQUEST.into_response();
                    };
                    read(s, move |store| store.range(head, page)).await
                },
            ),
        )
        .route(
            "/v1/receiver/archive/seen/:digest",
            get(
                |State(s): State<Service>, Path(digest): Path<String>| async move {
                    let Ok(digest) = decode_id(&digest) else {
                        return StatusCode::BAD_REQUEST.into_response();
                    };
                    read(s, move |store| store.seen(digest)).await
                },
            ),
        )
        .route(
            "/v1/receiver/archive/coverage/:archive/:epoch/:session",
            get(
                |State(s): State<Service>,
                 Path((archive, epoch, session)): Path<(String, u64, String)>| async move {
                    let (Ok(archive), Ok(session)) = (decode_id(&archive), decode_id(&session))
                    else {
                        return StatusCode::BAD_REQUEST.into_response();
                    };
                    read(s, move |store| store.coverage(archive, epoch, session)).await
                },
            ),
        )
        .with_state(Service {
            location,
            slots: Arc::new(Semaphore::new(2)),
        })
}

/// Refuses malformed path identities before scheduling database work.
fn decode_id(value: &str) -> Result<Hash> {
    if value.len() != 64 {
        return Err("invalid archive identity".into());
    }
    Ok(hex::decode(value)?
        .try_into()
        .map_err(|_| "invalid archive identity")?)
}

/// Admits a bounded read before spawning; its slot follows work and bytes even if the request ends.
async fn read(
    service: Service,
    load: impl FnOnce(&mut ArchiveStore) -> std::result::Result<Vec<u8>, receiver_store::Error>
        + Send
        + 'static,
) -> Response {
    let Ok(slot) = service.slots.try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let result = tokio::task::spawn_blocking(move || {
        let mut store = ArchiveStore::read(service.location.path, service.location.config)?;
        Ok::<_, receiver_store::Error>(Output {
            bytes: load(&mut store)?,
            _slot: slot,
        })
    })
    .await;
    match result {
        Ok(Ok(output)) => (
            [
                (header::CACHE_CONTROL, "no-store"),
                (header::CONTENT_TYPE, "application/octet-stream"),
            ],
            Bytes::from_owner(output),
        )
            .into_response(),
        Ok(Err(receiver_store::Error::Coverage)) => StatusCode::GONE.into_response(),
        _ => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use receiver_directory::{
        archive::{Head, Proof},
        blocks::BlockId,
    };

    /// A tiny durable archive and its real HTTP service, without PIR preparation.
    #[tokio::test]
    async fn http_serves_pinned_pages_and_unavailable_is_not_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = StoreConfig {
            genesis: [1; 32],
            start_height: 1,
            start_parent: [2; 32],
            start_position: 0,
        };
        let location = Location {
            path: dir.path().join(FILE),
            config: config.clone(),
        };
        let mut archive = ArchiveStore::open(&location.path, config).unwrap();
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!(
            "http://{}/v1/receiver/archive",
            socket.local_addr().unwrap()
        );
        let app = router(location.clone());
        let task = tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        assert_eq!(
            reqwest::get(format!("{origin}/head"))
                .await
                .unwrap()
                .status(),
            StatusCode::GONE
        );
        let seen = SeenSet::new([1; 32], 0).union([&[7; 43]]);
        let reference = archive.record_seen(&seen).unwrap();
        let head = archive
            .stage(
                [Ok(receiver_store::archive::Block {
                    id: BlockId {
                        height: 1,
                        hash: [3; 32],
                    },
                    parent: [2; 32],
                    records: [4; 32],
                    receivers: vec![[7; 43]],
                })],
                Some(reference.clone()),
                0,
            )
            .unwrap();
        archive.activate(&head, 0).unwrap();
        let bytes = reqwest::get(format!("{origin}/head"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(Head::decode(&bytes).unwrap(), head);
        let bytes = reqwest::get(format!(
            "{origin}/range/{}/0",
            hex::encode(head.digest().unwrap())
        ))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
        let page = Proof::decode(&head, 0, &bytes).unwrap();
        assert_eq!(page.filters()[0].matches(&[[7; 43]]), vec![true]);
        let bytes = reqwest::get(format!("{origin}/seen/{}", hex::encode(reference.digest)))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(SeenSet::decode(&bytes).unwrap(), seen);
        let missing = reqwest::get(format!("{origin}/range/{}/0", "00".repeat(32)))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::GONE);
        assert_eq!(missing.bytes().await.unwrap().len(), 0);
        archive.revoke(1).unwrap();
        assert_eq!(
            reqwest::get(format!("{origin}/head"))
                .await
                .unwrap()
                .status(),
            StatusCode::GONE
        );
        // A reader that already pinned a head can finish its immutable range during grace.
        assert!(reqwest::get(format!(
            "{origin}/range/{}/0",
            hex::encode(head.digest().unwrap())
        ))
        .await
        .unwrap()
        .status()
        .is_success());
        task.abort();
    }

    /// Output buffers, not just handlers, own admission slots until the last byte owner is dropped.
    #[test]
    fn response_bytes_hold_memory_lease() {
        let slots = Arc::new(Semaphore::new(1));
        let bytes = Bytes::from_owner(Output {
            bytes: vec![0; 16],
            _slot: slots.clone().try_acquire_owned().unwrap(),
        });
        let copy = bytes.clone();
        drop(bytes);
        assert!(slots.clone().try_acquire_owned().is_err());
        drop(copy);
        assert!(slots.try_acquire_owned().is_ok());
    }
}
