//! Publisher-owned HTTP authority for independently synchronized display servers.
use crate::history::publication::{write_atomic, BoxError};
use crate::ingest::zakura::NodeClient;
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
    active: RwLock<Option<(Current, Arc<Finalized>)>>,
    slots: Arc<tokio::sync::Semaphore>,
    /// The last set an inventory was prepared from, as verified then: the
    /// next one's unchanged revisions are its hard links and are not
    /// verified again before the inventory rehashes them.
    prepared_set: std::sync::Mutex<Option<DisplaySet>>,
    /// Shipped runtimes this process already loaded and self-checked; the
    /// next inventory rehashes them but checks only changed bytes again.
    checked_runtimes: std::sync::Mutex<CheckedRuntimes>,
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
    pub fn commit(&self, finalized: Finalized) -> Result<(), BoxError> {
        let mut active = self.0.active.write().unwrap();
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
        write_atomic(
            &self.0.root.join("http-current.json"),
            &serde_json::to_vec(&current)?,
        )?;
        *active = Some((current, Arc::new(finalized)));
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
            write_atomic(
                &self.0.root.join("http-current.json"),
                &serde_json::to_vec(current)?,
            )?;
        }
        Ok(())
    }
    pub async fn finalize(
        &self,
        directory: PathBuf,
        retained: Vec<PathBuf>,
    ) -> Result<(), BoxError> {
        if !self.restore(directory, retained).await? {
            return Err("display publication anchor is not canonical".into());
        }
        Ok(())
    }
    /// Reuse a saved publication only while its anchor is canonical. A stale
    /// tip returns false with authority withdrawn so startup can rebuild it;
    /// invalid material, chain identity and RPC/storage failures remain errors.
    pub(crate) async fn restore(
        &self,
        directory: PathBuf,
        retained: Vec<PathBuf>,
    ) -> Result<bool, BoxError> {
        self.restore_until(directory, retained, Default::default())
            .await
    }
    /// [`Self::restore`], ending with [`Stopped`] once `shutdown` is requested:
    /// between the files, bounded reads and runtime segments of loading,
    /// runtime preparation and finalization, and before authority is
    /// withdrawn or committed. A stop commits nothing and keeps the earlier
    /// verified set; the blocking work it interrupts has ended on return.
    pub(crate) async fn restore_until(
        &self,
        directory: PathBuf,
        retained: Vec<PathBuf>,
        shutdown: pir_pipeline::service::Shutdown,
    ) -> Result<bool, BoxError> {
        if shutdown.requested() {
            return Err(Stopped.into());
        }
        let work = directory;
        let previous = self.0.prepared_set.lock().unwrap().take();
        let mut checked = std::mem::take(&mut *self.0.checked_runtimes.lock().unwrap());
        let started = std::time::Instant::now();
        let stopping = shutdown.clone();
        let (kept, checked, finalized) = tokio::task::spawn_blocking(move || {
            let stop = || stopping.requested();
            let result = (|| -> Result<(DisplaySet, Finalized), BoxError> {
                let set = DisplaySet::open_reusing_until(
                    &work,
                    0,
                    previous.as_ref(),
                    &[],
                    DisplayRole::Combined,
                    &stop,
                )?;
                for revision in set.current() {
                    transparent_shared::tx_detail::prebuild::build_shipped_verified_until(
                        revision, &work, &retained, &stop,
                    )?;
                }
                let finalized = Finalized::prepare_reusing_until(&work, &set, &mut checked, &stop)?;
                Ok((set, finalized))
            })();
            match result {
                Ok((set, finalized)) => (Some(set), checked, Ok(finalized)),
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
        self.commit(finalized)?;
        Ok(true)
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
    let Some(path) = finalized.files.get(hash) else {
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
    let file = tokio::fs::File::open(path).await?;
    let length = file.metadata().await?.len();
    let body = Body::from_stream(tokio_util::io::ReaderStream::new(file));
    Ok(Response::builder()
        .header(header::CONTENT_LENGTH, length)
        .header(header::CACHE_CONTROL, "public, immutable")
        .body(pir_control::http::retain(body, (permit, finalized)))?)
}
