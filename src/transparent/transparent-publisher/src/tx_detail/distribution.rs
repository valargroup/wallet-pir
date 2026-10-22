//! Publisher-owned HTTP authority for independently synchronized display servers.
mod restoration;
use crate::ingest::zakura::NodeClient;
use crate::storage::records::write_durable;
use crate::BoxError;
use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Router,
};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use transparent_shared::publication::{
    digest, AnchorCheck, CheckedRuntimes, Current, Eligibility, Stopped,
};
use transparent_shared::tx_detail::distribution::{Finalized, PREFIX, SCHEMA};
use transparent_shared::tx_detail::{set::DisplaySet, DisplayRole};

#[derive(Clone)]
pub struct Distribution(Arc<Inner>);
struct Inner {
    root: PathBuf,
    rpc: NodeClient,
    active: RwLock<Option<(Current, Arc<RetainedPublication>)>>,
    slots: Arc<tokio::sync::Semaphore>,
    /// Source verification evidence for unchanged objects in the next set.
    /// Reuse requires the same open-file identity and metadata.
    prepared_set: std::sync::Mutex<Option<DisplaySet>>,
    /// Shipped runtimes this process already loaded and self-checked; the
    /// next inventory reuses evidence only while their file stamps agree.
    checked_runtimes: std::sync::Mutex<CheckedRuntimes>,
}
struct RetainedPublication {
    finalized: Finalized,
    _lease: crate::storage::PublicationLease,
}
impl std::ops::Deref for RetainedPublication {
    type Target = Finalized;
    fn deref(&self) -> &Finalized {
        &self.finalized
    }
}
impl Distribution {
    pub fn new(root: &Path, rpc: NodeClient) -> Result<Self, BoxError> {
        // Reserve a durable process boundary before exposing any publication.
        crate::history::distribution::advance_distribution_revision(root)?;
        Ok(Self(Arc::new(Inner {
            root: root.to_path_buf(),
            rpc,
            active: RwLock::new(None),
            slots: Arc::new(tokio::sync::Semaphore::new(4)),
            prepared_set: std::sync::Mutex::new(None),
            checked_runtimes: Default::default(),
        })))
    }
    pub fn router(&self) -> Router {
        Router::new().fallback(request).with_state(self.clone())
    }
    pub(super) fn commit(
        &self,
        finalized: Finalized,
        expected: Option<u64>,
        shutdown: &pir_pipeline::service::Shutdown,
    ) -> Result<(), BoxError> {
        let publication = super::publisher::ActiveRecord::load(&self.0.root)?;
        self.commit_publication(finalized, expected, shutdown, &publication)
    }
    pub(super) fn commit_publication(
        &self,
        finalized: Finalized,
        expected: Option<u64>,
        shutdown: &pir_pipeline::service::Shutdown,
        publication: &super::publisher::ActiveRecord,
    ) -> Result<(), BoxError> {
        let lease = crate::storage::PublicationLease::acquire(&publication.directory)?;
        let mut active = self.0.active.write().unwrap();
        if shutdown.requested() {
            return Err(Stopped.into());
        }
        if active.as_ref().map(|(current, _)| current.revision) != expected {
            return Err("display authority changed before commit".into());
        }
        let current = Current {
            schema: SCHEMA.into(),
            revision: crate::history::distribution::advance_distribution_revision(&self.0.root)?,
            recovery_epoch: 0,
            inventory_sha256: finalized.sha256.clone(),
            network: finalized.inventory.network.clone(),
            genesis_hash: finalized.inventory.genesis_hash.clone(),
            anchor_height: finalized.anchor_height,
            anchor_hash: finalized.anchor_hash.clone(),
            eligibility: Eligibility::Canonical,
            invalidated_from: None,
        };
        if let Err(error) = super::activation::commit(&self.0.root, publication, &current) {
            // Never expose a successor whose restart state did not commit.
            if let Some((current, _)) = active.as_mut() {
                current.eligibility = Eligibility::Withdrawn;
            }
            return Err(error);
        }
        *active = Some((
            current,
            Arc::new(RetainedPublication {
                finalized,
                _lease: lease,
            }),
        ));
        Ok(())
    }
    /// Withdraw before any journal rollback; a failed write still fences this process.
    pub fn withdraw(&self, from: u64) -> Result<(), BoxError> {
        let mut active = self.0.active.write().unwrap();
        if let Some((current, _)) = active.as_mut() {
            current.eligibility = Eligibility::Withdrawn;
            current.invalidated_from =
                Some(current.invalidated_from.map_or(from, |old| old.min(from)));
            // Keep the same revision negative if reserving the next revision fails.
            current.revision =
                crate::history::distribution::advance_distribution_revision(&self.0.root)?;
            write_durable(
                &self.0.root.join("http-current.json"),
                &serde_json::to_vec(current)?,
            )?;
        }
        Ok(())
    }
    /// Prepare verified display material without changing HTTP authority.
    pub(super) async fn prepare(
        &self,
        directory: PathBuf,
        retained: Vec<PathBuf>,
        shutdown: pir_pipeline::service::Shutdown,
        stop: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Result<Finalized, BoxError> {
        if shutdown.requested() {
            return Err(Stopped.into());
        }
        self.prepare_with(
            directory,
            retained,
            shutdown,
            stop,
            Arc::new(super::runtime_preparation::RequiredRuntimes),
        )
        .await
        .map(|(finalized, _)| finalized)
    }
    pub(super) async fn prepare_with(
        &self,
        directory: PathBuf,
        retained: Vec<PathBuf>,
        shutdown: pir_pipeline::service::Shutdown,
        stop: impl Fn() -> bool + Send + Sync + 'static,
        preparation: Arc<dyn super::runtime_preparation::RuntimePreparation>,
    ) -> Result<
        (
            Finalized,
            Vec<transparent_shared::tx_detail::prebuild::Shipped>,
        ),
        BoxError,
    > {
        let work = directory;
        let previous = self.0.prepared_set.lock().unwrap().take();
        let mut checked = std::mem::take(&mut *self.0.checked_runtimes.lock().unwrap());
        let started = std::time::Instant::now();
        let (kept, checked, finalized) = tokio::task::spawn_blocking(move || {
            let result = (|| -> Result<(DisplaySet, Finalized, Vec<transparent_shared::tx_detail::prebuild::Shipped>), BoxError> {
                crate::storage::adopt_segments(&work, &stop)?;
                crate::storage::adopt_indexes(&work, &stop)?;
                let set = DisplaySet::open_reusing_until(
                    &work,
                    0,
                    previous.as_ref(),
                    &[],
                    DisplayRole::Combined,
                    &stop,
                )?;
                let mut prepared = Vec::new();
                for revision in set.current() {
                    prepared.extend(preparation.prepare(revision, &work, &retained, &stop)?);
                }
                crate::storage::adopt_runtimes(&work, &stop)?;
                let finalized = Finalized::prepare_reusing_until(&work, &set, &mut checked, &stop)?;
                Ok((set, finalized, prepared))
            })();
            match result {
                Ok((set, finalized, prepared)) => (Some(set), checked, Ok((finalized, prepared))),
                // Earlier verification stays valid evidence for its own files.
                Err(error) => (previous, checked, Err(error)),
            }
        })
        .await?;
        tracing::info!(
            loaded = checked.loaded,
            reused = checked.reused,
            seconds = started.elapsed().as_secs_f64(),
            "prepared display distribution inventory"
        );
        *self.0.checked_runtimes.lock().unwrap() = checked;
        *self.0.prepared_set.lock().unwrap() = kept;
        let finalized = match finalized {
            Err(_) if shutdown.requested() => return Err(Stopped.into()),
            result => result?,
        };
        if shutdown.requested() {
            return Err(Stopped.into());
        }
        Ok(finalized)
    }
    pub(super) fn revision(&self) -> Option<u64> {
        self.0
            .active
            .read()
            .unwrap()
            .as_ref()
            .map(|(current, _)| current.revision)
    }
    /// Recheck the exact source before the caller enters its commit guard.
    pub(super) async fn canonical(
        &self,
        finalized: &Finalized,
        shutdown: &pir_pipeline::service::Shutdown,
    ) -> Result<bool, BoxError> {
        if shutdown.requested() {
            return Err(Stopped.into());
        }
        if self.0.rpc.block_hash_display(0).await? != finalized.inventory.genesis_hash {
            self.withdraw(0)?;
            return Err("display publication genesis differs from the node".into());
        }
        if self
            .0
            .rpc
            .block_hash_display(finalized.anchor_height)
            .await?
            != finalized.anchor_hash
        {
            self.withdraw(0)?;
            return Ok(false);
        }
        // Shutdown can arrive while the anchor check awaits the node.
        if shutdown.requested() {
            return Err(Stopped.into());
        }
        Ok(true)
    }
    #[cfg(all(test, feature = "fixtures"))]
    pub(crate) async fn restore<E: pir_pipeline::publisher::PublisherExecutor>(
        &self,
        executor: E,
        directory: PathBuf,
        retained: Vec<PathBuf>,
    ) -> Result<bool, BoxError> {
        self.restore_until(executor, directory, retained, Default::default())
            .await
    }
    pub(crate) async fn restore_until<E: pir_pipeline::publisher::PublisherExecutor>(
        &self,
        executor: E,
        directory: PathBuf,
        retained: Vec<PathBuf>,
        shutdown: pir_pipeline::service::Shutdown,
    ) -> Result<bool, BoxError> {
        let stopping = shutdown.clone();
        self.restore_asking(executor, directory, retained, shutdown, move || {
            stopping.requested()
        })
        .await
    }
    pub(super) async fn restore_asking<E: pir_pipeline::publisher::PublisherExecutor>(
        &self,
        executor: E,
        directory: PathBuf,
        retained: Vec<PathBuf>,
        shutdown: pir_pipeline::service::Shutdown,
        stop: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Result<bool, BoxError> {
        let revision = self.revision();
        executor
            .publish(restoration::DisplayRestoration {
                distribution: self,
                directory,
                retained,
                shutdown,
                stop: Some(stop),
                revision,
                canonical: false,
            })
            .await
    }
    /// The map digest of the kept verified set, and how many runtimes the
    /// last inventory loaded and self-checked and how many it reused.
    #[cfg(all(test, feature = "qualification-tests", feature = "fixtures"))]
    pub(super) fn evidence(&self) -> (Option<String>, usize, usize) {
        let set = self.0.prepared_set.lock().unwrap();
        let checked = self.0.checked_runtimes.lock().unwrap();
        (
            set.as_ref().map(|s| s.map_digest.clone()),
            checked.loaded,
            checked.reused,
        )
    }
}
async fn request(State(authority): State<Distribution>, request: Request) -> Response {
    match serve(authority, request).await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(%error, "display distribution refused");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::CACHE_CONTROL, "no-store")],
            )
                .into_response()
        }
    }
}
async fn serve(authority: Distribution, request: Request) -> Result<Response, BoxError> {
    if request.method() != axum::http::Method::GET {
        return Ok(StatusCode::METHOD_NOT_ALLOWED.into_response());
    }
    let Some(suffix) = request.uri().path().strip_prefix(PREFIX) else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    let (mut current, finalized) = authority
        .0
        .active
        .read()
        .unwrap()
        .clone()
        .ok_or("display publication not ready")?;
    if suffix == "current" || suffix.starts_with("anchors/") {
        let (height, hash) = if suffix == "current" {
            (current.anchor_height, current.anchor_hash.clone())
        } else {
            let Some((height, hash)) = suffix
                .strip_prefix("anchors/")
                .and_then(|s| s.split_once('/'))
            else {
                return Ok(StatusCode::NOT_FOUND.into_response());
            };
            if !transparent_shared::publication::is_digest(hash) {
                return Ok(StatusCode::BAD_REQUEST.into_response());
            }
            (height.parse::<u64>()?, hash.to_string())
        };
        let canonical = authority.0.rpc.block_hash_display(0).await? == current.genesis_hash
            && authority.0.rpc.block_hash_display(height).await? == hash;
        let snapshot = authority.0.active.read().unwrap();
        if snapshot.as_ref().map(|s| &s.0) != Some(&current) {
            return Err("display authority changed during anchor check".into());
        }
        drop(snapshot);
        if suffix != "current" {
            return Ok(axum::Json(AnchorCheck {
                schema: SCHEMA.into(),
                revision: current.revision,
                recovery_epoch: current.recovery_epoch,
                genesis_hash: current.genesis_hash,
                height,
                hash,
                canonical,
            })
            .into_response());
        }
        if !canonical && current.eligible() {
            current.eligibility = Eligibility::Noncanonical;
        }
        let etag = format!("\"{}\"", digest(&serde_json::to_vec(&current)?));
        let headers = [
            (header::ETAG, etag.clone()),
            (header::CACHE_CONTROL, "no-cache".into()),
        ];
        if request
            .headers()
            .get(header::IF_NONE_MATCH)
            .and_then(|s| s.to_str().ok())
            == Some(etag.as_str())
        {
            return Ok((StatusCode::NOT_MODIFIED, headers).into_response());
        }
        return Ok((headers, axum::Json(current)).into_response());
    }
    if !current.eligible() {
        return Ok(StatusCode::NOT_FOUND.into_response());
    }
    if suffix == format!("inventories/{}", finalized.sha256) {
        return Ok((
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, "public, immutable"),
            ],
            finalized.bytes.clone(),
        )
            .into_response());
    }
    let Some(hash) = suffix.strip_prefix("artifacts/") else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    let Some((path, length)) = finalized.files.get(hash) else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    let permit = match authority.0.slots.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::RETRY_AFTER, "1")],
            )
                .into_response())
        }
    };
    let file = tokio::fs::File::from_std(pir_publication::open_object_file(path, Some(*length))?);
    let body = Body::from_stream(tokio_util::io::ReaderStream::new(file));
    Ok(Response::builder()
        .header(header::CONTENT_LENGTH, *length)
        .header(header::CACHE_CONTROL, "public, immutable")
        .body(pir_control::http::retain(body, (permit, finalized)))?)
}
