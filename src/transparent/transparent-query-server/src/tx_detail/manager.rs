//! Autonomous full-coverage display synchronization. Disk bytes never confer authority.
use super::{
    live::{DisplayCommand, DisplayLive, DisplayPublication},
    service::DisplayRuntime,
};
use crate::{
    control::persist,
    routing::bootstrap::{Config, Source},
};
use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, RwLock},
};
use tokio::io::AsyncWriteExt;
use transparent_shared::{
    publication::{self, Current, Error},
    tx_detail::{
        distribution::{Inventory, PREFIX, SCHEMA},
        set::{DisplaySet, MAP_FILE},
        DisplayRole,
    },
};

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct Floor {
    revision: u64,
    digests: BTreeSet<String>,
}

/// Display-specific validation and activation using shared HTTPS synchronization.
pub struct Manager {
    sync: pir_sync_manager::SyncManager<DisplaySync>,
}
impl Manager {
    pub fn new(config: Config, service: crate::config::ServiceConfig) -> Result<Self, Error> {
        Ok(Self {
            sync: pir_sync_manager::SyncManager::new(DisplaySync::new(config, service)?),
        })
    }
    pub fn router(&self) -> axum::Router {
        self.sync.task().router()
    }
    pub async fn ready(&self) -> bool {
        self.sync.task().ready().await
    }
    pub async fn poll_once(&mut self) -> Result<serde_json::Value, Error> {
        self.sync.poll_once().await
    }
    pub async fn run(self) {
        self.sync.run().await;
    }
}

struct DisplaySync {
    config: Config,
    source: Source,
    live: DisplayLive,
    current: Option<Current>,
    floor: Floor,
    _lock: Arc<std::fs::File>,
    synchronization: Arc<RwLock<serde_json::Value>>,
}
impl DisplaySync {
    pub fn new(mut config: Config, service: crate::config::ServiceConfig) -> Result<Self, Error> {
        std::fs::create_dir_all(&config.directory)?;
        if !std::fs::symlink_metadata(&config.directory)?.is_dir() {
            return Err("display provider root is not a real directory".into());
        }
        config.directory = std::fs::canonicalize(&config.directory)?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(config.directory.join("bootstrap.lock"))?;
        lock.try_lock()
            .map_err(|_| "another display consumer owns this directory")?;
        let source = Source::for_product(&config, PREFIX, SCHEMA)?;
        let mut floor: Floor = match std::fs::read(config.directory.join("revocation-floor.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Floor::default(),
            Err(e) => return Err(e.into()),
        };
        let revoked: BTreeSet<String> =
            match std::fs::read(config.directory.join("active.invalid.json")) {
                Ok(bytes) => serde_json::from_slice(&bytes)?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeSet::new(),
                Err(e) => return Err(e.into()),
            };
        if !revoked.is_subset(&floor.digests) {
            floor.revision = floor
                .revision
                .max(source.observed_revision(&config.directory)?);
            floor.digests.extend(revoked);
            persist(&config.directory.join("revocation-floor.json"), &floor)?;
        }
        let lock = Arc::new(lock);
        let live = DisplayLive::new(
            DisplayRuntime::new_required(service),
            DisplayRole::Combined,
            3,
            config.directory.join("active.json"),
            Vec::new(),
            None,
        )?
        .retain_directory_owner(lock.clone());
        Ok(Self {
            config,
            source,
            live,
            current: None,
            floor,
            _lock: lock,
            synchronization: Arc::new(RwLock::new(serde_json::json!({"state":"not-polled"}))),
        })
    }
    pub fn router(&self) -> axum::Router {
        let sync = self.synchronization.clone();
        let live = self.live.clone();
        self.live.router().route("/v1/provider-status", axum::routing::get(move || {
            let sync=sync.clone(); let live=live.clone(); async move {
                let status=live.command(DisplayCommand::Status).await.unwrap_or_default();
                axum::Json(serde_json::json!({"synchronization":sync.read().unwrap().clone(),
                    "verified_publication":status["active"],"runtime_readiness":{"warm":status["warm"]},
                    "revoked_revisions":status["revoked_revisions"]}))
            }
        }))
    }

    async fn observe(&mut self) -> Result<Current, Error> {
        let polled = self
            .source
            .poll(
                &self.config.directory,
                self.current.as_ref(),
                &self.config.network,
                &self.config.genesis_hash,
            )
            .await?;
        let (current, storage_error) = match polled {
            Some(value) => value,
            None => (
                self.current
                    .clone()
                    .ok_or("unchanged without an active display publication")?,
                None,
            ),
        };
        let reconciled = reconcile_serving(
            &mut self.source,
            &self.config.directory,
            &self.live,
            &mut self.floor,
            &current,
        )
        .await;
        if let Some(error) = storage_error {
            return Err(error);
        }
        reconciled?;
        if !current.eligible() {
            return Err("display source is withdrawn or noncanonical".into());
        }
        Ok(current)
    }
    async fn update(&mut self) -> Result<serde_json::Value, Error> {
        let current = self.observe().await?;
        if self.current.as_ref() == Some(&current) {
            return Ok(serde_json::json!({"unchanged":true,"revision":current.revision}));
        }
        let result = self.download(&current).await;
        let publication = match result {
            Ok(publication) => publication,
            Err(error) => {
                if self.current.is_none() {
                    if let Ok(bytes) = std::fs::read(self.config.directory.join("active.json")) {
                        if let Ok(saved) = serde_json::from_slice::<DisplayPublication>(&bytes) {
                            if let Err(recovery) = self.activate(&current, saved).await {
                                tracing::warn!(%recovery,"saved display publication refused");
                            }
                        }
                    }
                }
                return Err(error);
            }
        };
        self.activate(&current, publication).await?;
        self.live.command(DisplayCommand::Collect).await?;
        self.current = Some(current.clone());
        Ok(
            serde_json::json!({"unchanged":false,"revision":current.revision,"downloaded_bytes":self.source.downloaded()}),
        )
    }
    /// Whether a checked publication is actually active; saved pointers alone never count.
    pub async fn ready(&self) -> bool {
        self.live
            .command(DisplayCommand::Status)
            .await
            .is_ok_and(|s| s["active"].is_object() && s["warm"] == true)
    }
    async fn download(&mut self, current: &Current) -> Result<DisplayPublication, Error> {
        let raw = self
            .source
            .small(
                &format!("inventories/{}", current.inventory_sha256),
                16 << 20,
            )
            .await?;
        if publication::digest(&raw) != current.inventory_sha256 {
            return Err("display inventory digest mismatch".into());
        }
        let inventory: Inventory = serde_json::from_slice(&raw)?;
        inventory.validate(&self.config.network, &self.config.genesis_hash)?;
        let stage = self.config.directory.join(format!(
            "staging-{}-{}",
            current.revision,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        std::fs::create_dir(&stage)?;
        let result = async {
            for artifact in &inventory.artifacts {
                let target = stage.join(&artifact.path);
                std::fs::create_dir_all(target.parent().ok_or("display artifact has no parent")?)?;
                let cached = self.config.directory.join("objects").join(&artifact.sha256);
                let reused = std::fs::symlink_metadata(&cached).is_ok_and(|m| m.is_file())
                    && publication::verify_file(&cached, artifact).is_ok();
                if reused {
                    tokio::fs::copy(&cached, &target).await?;
                } else {
                    let mut output = tokio::fs::OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(&target)
                        .await?;
                    let mut response = self
                        .source
                        .get(&format!("artifacts/{}", artifact.sha256))
                        .await?;
                    let mut remaining = artifact.bytes;
                    while let Some(chunk) = response.chunk().await? {
                        if chunk.len() as u64 > remaining {
                            return Err("display artifact exceeds declared length".into());
                        }
                        output.write_all(&chunk).await?;
                        remaining -= chunk.len() as u64;
                    }
                    if remaining != 0 {
                        return Err("incomplete display artifact".into());
                    }
                    output.sync_all().await?;
                }
                publication::verify_file(&target, artifact)?;
                if !reused {
                    std::fs::create_dir_all(cached.parent().unwrap())?;
                    let partial = cached.with_extension("partial");
                    tokio::fs::copy(&target, &partial).await?;
                    std::fs::File::open(&partial)?.sync_all()?;
                    std::fs::rename(&partial, &cached)?;
                    std::fs::File::open(cached.parent().unwrap())?.sync_all()?;
                }
            }
            let map: zakura_pir_protocol::transparent::shard::display::DisplayMap =
                serde_json::from_slice(&std::fs::read(stage.join(MAP_FILE))?)?;
            if map.sha256() != inventory.map_sha256
                || map.network != self.config.network
                || map.genesis_hash != self.config.genesis_hash
            {
                return Err("display map/inventory identity mismatch".into());
            }
            let tip = map.shards.last().ok_or("empty display map")?;
            if tip.end_height != current.anchor_height
                || tip.terminal_block_hash != current.anchor_hash
            {
                return Err("display anchor names another map".into());
            }
            persist(&stage.join("verified-display-inventory.json"), &inventory)?;
            crate::routing::bootstrap::sync_tree(&stage)?;
            Ok(DisplayPublication {
                directory: stage.clone(),
                map_sha256: inventory.map_sha256,
            })
        }
        .await;
        if result.is_err() {
            let _ = std::fs::remove_dir_all(&stage);
        }
        result
    }
    async fn activate(
        &mut self,
        current: &Current,
        publication: DisplayPublication,
    ) -> Result<(), Error> {
        let directory = std::fs::canonicalize(&publication.directory)?;
        if !directory.starts_with(&self.config.directory) {
            return Err("saved display publication escapes provider root".into());
        }
        let set = DisplaySet::open(&directory, 0, DisplayRole::Combined)?;
        if set.map_digest != publication.map_sha256
            || set.map.network != self.config.network
            || set.map.genesis_hash != self.config.genesis_hash
        {
            return Err("saved display publication identity mismatch".into());
        }
        let anchors: Vec<_> = set
            .current()
            .iter()
            .map(|r| {
                (
                    r.digest.clone(),
                    r.manifest.end_height,
                    r.manifest.terminal_block_hash.clone(),
                )
            })
            .collect();
        let status = self.live.command(DisplayCommand::Status).await?;
        let expected = status["active"]["map_sha256"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        self.live
            .command(DisplayCommand::Prepare {
                expected: expected.clone(),
                publication: publication.clone(),
            })
            .await?;
        let live = self.live.clone();
        let root = self.config.directory.clone();
        let floor = &mut self.floor;
        let source = &mut self.source;
        live.activate_checked(expected, publication.map_sha256, || async {
            let latest = refresh_serving(source, &root, &live, floor, current)
                .await
                .map_err(|e| e.to_string())?;
            if latest != *current || !latest.eligible() {
                return Err("display candidate superseded or invalidated".into());
            }
            let mut keep = BTreeSet::new();
            for (digest, height, hash) in &anchors {
                if !source
                    .anchor(&latest, *height, hash)
                    .await
                    .map_err(|e| e.to_string())?
                {
                    let revoked = revoke(
                        &root,
                        &live,
                        floor,
                        &latest,
                        BTreeSet::from([digest.clone()]),
                    );
                    // The chain can change after the preceding pointer check.
                    // Refresh serving authority before leaving the rejected candidate.
                    let reconciled = refresh_serving(source, &root, &live, floor, &latest).await;
                    revoked.map_err(|e| e.to_string())?;
                    reconciled.map_err(|e| e.to_string())?;
                    return Err("display candidate has noncanonical history".into());
                }
                if floor.digests.contains(digest) && latest.revision <= floor.revision {
                    return Err("display recovery requires a higher source revision".into());
                }
                keep.insert(digest.clone());
            }
            let final_current = refresh_serving(source, &root, &live, floor, current)
                .await
                .map_err(|e| e.to_string())?;
            if final_current != latest {
                return Err("display current changed during anchor checks".into());
            }
            live.restore_verified(&keep)?;
            Ok(())
        })
        .await?;
        Ok(())
    }
}

impl pir_sync_manager::SyncTask for DisplaySync {
    type Report = serde_json::Value;
    type Error = Error;
    async fn synchronize(&mut self) -> Result<Self::Report, Error> {
        self.source.reset_budget();
        *self.synchronization.write().unwrap() = serde_json::json!({"state":"synchronizing"});
        self.live.command(DisplayCommand::Collect).await?;
        let result = self.update().await;
        match &result {
            Ok(report) => {
                *self.synchronization.write().unwrap() =
                    serde_json::json!({"state":"synchronized","last_change":report})
            }
            Err(error) => {
                *self.synchronization.write().unwrap() =
                    serde_json::json!({"state":"refused","error":error.to_string()})
            }
        }
        result
    }
    fn report(&self, result: &Result<Self::Report, Error>) {
        if let Err(error) = result {
            tracing::warn!(%error,"display synchronization refused");
        }
    }
}

/// Fence memory first; a failed durable write must never release an orphaned answer.
fn revoke(
    root: &Path,
    live: &DisplayLive,
    floor: &mut Floor,
    current: &Current,
    digests: BTreeSet<String>,
) -> Result<(), Error> {
    floor.revision = floor.revision.max(current.revision);
    floor.digests.extend(digests.iter().cloned());
    let owner = live.revoke(&digests);
    let saved = persist(&root.join("revocation-floor.json"), floor);
    owner?;
    saved?;
    Ok(())
}

/// Apply every accepted source snapshot to active and retained serving views.
/// A recovered canonical pointer may omit a withdrawal the consumer missed, so
/// exact anchor checks must precede even a superseded-candidate return.
async fn reconcile_serving(
    source: &mut Source,
    root: &Path,
    live: &DisplayLive,
    floor: &mut Floor,
    current: &Current,
) -> Result<(), Error> {
    let anchors: BTreeSet<_> = live.anchors().into_iter().collect();
    if let Some(from) = current.invalidated_from {
        let digests = anchors
            .iter()
            .filter(|(_, height, _)| *height >= from)
            .map(|(digest, _, _)| digest.clone())
            .collect();
        revoke(root, live, floor, current, digests)?;
    }
    if !current.eligible() {
        let digests = anchors
            .iter()
            .filter(|(_, height, hash)| {
                *height == current.anchor_height && *hash == current.anchor_hash
            })
            .map(|(digest, _, _)| digest.clone())
            .collect();
        revoke(root, live, floor, current, digests)?;
    }
    let recorded = source.observe(root, current);
    // A storage failure cannot hide exact negative evidence from the live owner.
    let checked = async {
        for (digest, height, hash) in anchors {
            if !source.anchor(current, height, &hash).await? {
                revoke(root, live, floor, current, BTreeSet::from([digest]))?;
            }
        }
        Ok(())
    }
    .await;
    recorded?;
    checked
}

/// Read a fresh pointer and reconcile serving authority before any caller can
/// reject a superseded candidate or publish a replacement.
async fn refresh_serving(
    source: &mut Source,
    root: &Path,
    live: &DisplayLive,
    floor: &mut Floor,
    identity: &Current,
) -> Result<Current, Error> {
    let (current, storage) = source
        .poll(root, None, &identity.network, &identity.genesis_hash)
        .await?
        .ok_or("missing display current")?;
    let reconciled = reconcile_serving(source, root, live, floor, &current).await;
    if let Some(error) = storage {
        return Err(error);
    }
    reconciled?;
    Ok(current)
}
