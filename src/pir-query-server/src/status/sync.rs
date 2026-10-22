//! Status publication policy hosted by the product-neutral HTTPS loop.
use super::*;
use pir_pipeline::Input;
use pir_sync_manager::{HttpsSource, SyncTask};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fs, io::Read};

#[derive(Debug)]
struct PublicationAdvanced;
impl std::fmt::Display for PublicationAdvanced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Status publication advanced during coordination")
    }
}
impl std::error::Error for PublicationAdvanced {}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Floor {
    current: Option<Current>,
    invalid: BTreeSet<Hash>,
}
pub(super) struct StatusSync {
    service: Service,
    source: HttpsSource,
    backend: MatvecBackend,
    floor: Floor,
    conditional: Option<Current>,
    retained: Vec<Object>,
    loaded: Option<Inventory>,
    deferred: bool,
}
impl StatusSync {
    pub fn new(service: Service, config: &RoleConfig) -> Result<Self, Failure> {
        let source = super::source(config)?;
        let floor = match fs::read(service.dir.join("publisher-high-water.json")) {
            Ok(bytes) => serde_json::from_slice::<Floor>(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Floor::default(),
            Err(error) => return Err(error.into()),
        };
        if let Some(current) = &floor.current {
            current.validate(service.network)?;
        }
        fs::create_dir_all(service.dir.join("objects"))?;
        // The exclusive owner can remove interrupted transfer files. Completed
        // objects and every authority/revocation record are preserved.
        for entry in fs::read_dir(service.dir.join("objects"))? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".download-")
            {
                fs::remove_file(entry.path())?;
            }
        }
        let backend = if config.cuda {
            MatvecBackend::Cuda { device: 0 }
        } else {
            MatvecBackend::Cpu
        };
        Ok(Self {
            service,
            source,
            backend,
            floor,
            conditional: None,
            retained: Vec::new(),
            loaded: None,
            deferred: false,
        })
    }
    async fn save_floor(&self) -> Result<(), Failure> {
        let root = self.service.dir.clone();
        let bytes = serde_json::to_vec(&self.floor)?;
        let result = tokio::task::spawn_blocking(move || {
            persist(&root, "publisher-high-water.json", &bytes)
        })
        .await?;
        if let Err(error) = result {
            self.revoke().await;
            self.service.state.lock().await.poisoned = true;
            return Err(error.into());
        }
        Ok(())
    }
    async fn revoke(&self) {
        let mut state = self.service.state.lock().await;
        if let Some(c) = &state.controller {
            c.revoke();
        }
        state.router_holds.clear();
    }
    async fn observe(&mut self, current: Current) -> Result<(), Failure> {
        current.validate(self.service.network)?;
        let mut invalidate = !current.eligible;
        if let Some(old) = &self.floor.current {
            if !current.succeeds(old) {
                return Err("Status publisher replay or identity change".into());
            }
            invalidate |= current.recovery_epoch > old.recovery_epoch;
        }
        if invalidate {
            let manifests = self
                .service
                .state
                .lock()
                .await
                .controller
                .as_ref()
                .map(Controller::serving_manifests)
                .unwrap_or_default();
            self.floor
                .invalid
                .extend(manifests.iter().map(Manifest::id));
            self.revoke().await;
        }
        self.floor.current = Some(current);
        self.save_floor().await
    }
    async fn current(&mut self) -> Result<Current, Failure> {
        let token = self
            .conditional
            .as_ref()
            .map(|c| format!("\"{}-{}\"", c.revision, c.eligible));
        let mut response = self.source.request("current", token.as_deref()).await?;
        let current = if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            response.discard().await?;
            self.conditional
                .clone()
                .ok_or("Status unexpected unchanged response")?
        } else if response.status().is_success() {
            serde_json::from_slice::<Current>(&response.read_limited(METADATA_LIMIT).await?)?
        } else {
            response.discard().await?;
            return Err("Status publisher unavailable".into());
        };
        drop(response);
        self.observe(current.clone()).await?;
        self.conditional = Some(current.clone());
        Ok(current)
    }
    async fn object(&mut self, object: &Object) -> Result<PathBuf, Failure> {
        object.validate(attempt_limit())?;
        let root = self.service.dir.join("objects");
        let target = root.join(&object.sha256);
        if target.exists() {
            let path = target.clone();
            let expected = object.clone();
            tokio::task::spawn_blocking(move || verify_file(&path, &expected)).await??;
            return Ok(target);
        }
        let temporary = root.join(format!(
            ".download-{}",
            hex::encode(rand::random::<[u8; 16]>())
        ));
        let result = async {
            let mut response = self
                .source
                .request(&format!("objects/{}", object.sha256), None)
                .await?;
            if !response.status().is_success() {
                response.discard().await?;
                return Err::<(), Failure>("Status object unavailable".into());
            }
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .await?;
            response
                .copy_to(
                    &mut file,
                    object.bytes,
                    pir_sync_manager::Verification::Sha256(&object.sha256),
                )
                .await?;
            file.sync_all().await?;
            drop(file);
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&temporary, fs::Permissions::from_mode(0o444)).await?;
            tokio::fs::rename(&temporary, &target).await?;
            let directory = root.clone();
            tokio::task::spawn_blocking(move || fs::File::open(directory)?.sync_all()).await??;
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        result?;
        Ok(target)
    }
    async fn worker_inventory(&self) -> Result<Option<Object>, Failure> {
        let response = self
            .service
            .serving_http
            .get(format!("{}/v1/provider-status", self.service.worker_origin))
            .send()
            .await?;
        let bytes = serving::bounded(response, METADATA_LIMIT)
            .await
            .map_err(|_| "Status worker status unavailable")?;
        let status: ProviderStatus = serde_json::from_slice(&bytes)?;
        // An available worker that is still loading is ordinary coordination,
        // so keep the shared success cadence instead of transport backoff.
        if status
            .manifest
            .as_ref()
            .is_none_or(|m| m.fresh(now_ms()).is_err())
        {
            return Ok(None);
        }
        Ok(status.inventory)
    }
    async fn pin_worker(&self, manifest: &Manifest) -> Result<(), Failure> {
        if self.service.role != Role::Router {
            return Ok(());
        }
        let response = self
            .service
            .serving_http
            .post(format!(
                "{}/v1/provider-pin/{}",
                self.service.worker_origin,
                hex::encode(manifest.id())
            ))
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::CONFLICT {
            return Err(Box::new(PublicationAdvanced));
        }
        let bytes = serving::bounded(response, 16)
            .await
            .map_err(|_| "Status worker cannot evaluate candidate generation")?;
        if !serde_json::from_slice::<bool>(&bytes)? {
            return Err("Status worker generation not ready".into());
        }
        Ok(())
    }
    async fn authority(&mut self, current: Current, candidate: &Manifest) -> Result<(), Failure> {
        let active = self
            .service
            .state
            .lock()
            .await
            .controller
            .as_ref()
            .map(Controller::serving_manifests)
            .unwrap_or_default();
        let mut manifests = vec![candidate.clone()];
        manifests.extend(
            active
                .into_iter()
                .filter(|m| m.id() != candidate.id() && m.fresh(now_ms()).is_ok()),
        );
        let request = AnchorRequest {
            current: current.clone(),
            anchors: manifests
                .iter()
                .map(|m| (m.anchor_height, m.anchor_hash))
                .collect(),
        };
        let mut response = self
            .source
            .post_json("anchors/check", serde_json::to_vec(&request)?)
            .await?;
        if response.status() == reqwest::StatusCode::CONFLICT {
            response.discard().await?;
            // Observe withdrawal or epoch changes before retaining any old view.
            drop(response);
            self.current().await?;
            return Err(Box::new(PublicationAdvanced));
        }
        if !response.status().is_success() {
            response.discard().await?;
            return Err("Status authority changed during synchronization".into());
        }
        let attestation: AnchorResponse =
            serde_json::from_slice(&response.read_limited(METADATA_LIMIT).await?)?;
        drop(response);
        if attestation.current != current || attestation.canonical.len() != manifests.len() {
            return Err("Status authority response differs".into());
        }
        let mut changed = false;
        for (manifest, canonical) in manifests.iter().zip(&attestation.canonical) {
            if !canonical {
                self.floor.invalid.insert(manifest.id());
                self.revoke().await;
                changed = true;
            }
        }
        if changed {
            self.save_floor().await?;
        }
        if self.floor.invalid.contains(&candidate.id())
            || candidate.recovery_epoch != current.recovery_epoch
        {
            return Err("Status candidate revoked".into());
        }
        Ok(())
    }
    async fn update(&mut self) -> Result<u64, Failure> {
        let input = pir_pipeline::acquire(self.acquire(), &mut ()).await?;
        match pir_pipeline::advance(input, Acquired::prepare, Prepared::publish, &mut ()).await? {
            Input::Work(generation) => Ok(generation),
            Input::Deferred => Ok(self
                .service
                .state
                .lock()
                .await
                .controller
                .as_ref()
                .map_or(0, |c| c.current().manifest().generation)),
            Input::Unchanged => {
                unreachable!("Status rechecks authority even for unchanged material")
            }
        }
    }
    async fn acquire(&mut self) -> Result<Input<Acquired<'_>>, Failure> {
        self.source.discover().await?;
        self.source.reset_budget();
        let current = self.current().await?;
        // A withdrawal is the publisher's announced recovery, not a transport
        // failure. Service is already fenced, so keep the normal cadence and
        // load the replacement soon after it is published.
        if !current.eligible {
            self.deferred = true;
            return Ok(Input::Deferred);
        }
        self.service
            .state
            .lock()
            .await
            .router_holds
            .retain(|g| g.manifest().fresh(now_ms()).is_ok());
        let active = self
            .service
            .state
            .lock()
            .await
            .controller
            .as_ref()
            .filter(|c| !c.revoked())
            .map(|c| c.current().manifest().clone());
        if let Some(active) = active
            .filter(|m| m.recovery_epoch == current.recovery_epoch && m.fresh(now_ms()).is_ok())
        {
            self.authority(current.clone(), &active).await?;
        }
        let inventory_object = if self.service.role == Role::Router {
            let Some(inventory) = self.worker_inventory().await? else {
                self.deferred = true;
                return Ok(Input::Deferred);
            };
            inventory
        } else {
            current
                .inventory
                .clone()
                .ok_or("Status current has no inventory")?
        };
        inventory_object.validate(METADATA_LIMIT as u64)?;
        let inventory_path = self.object(&inventory_object).await?;
        let inventory: Inventory = serde_json::from_slice(&fs::read(inventory_path)?)?;
        inventory.validate(&current)?;
        if self.floor.invalid.contains(&inventory.manifest.id()) {
            return Err("Status candidate was invalidated".into());
        }
        inventory.manifest.fresh(now_ms())?;
        // Even an unchanged active publication must revalidate live authority.
        let previous = self
            .service
            .state
            .lock()
            .await
            .controller
            .as_ref()
            .map(Controller::current);
        let unchanged = previous
            .as_ref()
            .is_some_and(|g| g.manifest().id() == inventory.manifest.id());
        if !unchanged
            && self
                .service
                .state
                .lock()
                .await
                .controller
                .as_ref()
                .is_some_and(|c| !c.can_prepare())
        {
            self.deferred = true;
            return Ok(Input::Deferred);
        }
        // Reuse only the material already verified and loaded by this process.
        // The new identity still passes live authority and freshness checks in
        // publish; no bytes are read from disk on this metadata-only path.
        let refreshed = previous
            .as_ref()
            .zip(self.loaded.as_ref())
            .and_then(|(g, old)| {
                if old.manifest != *g.manifest() || !old.same_prepared_material(&inventory) {
                    return None;
                }
                let next = g.reidentified(
                    inventory.manifest.generation,
                    inventory.manifest.recovery_epoch,
                    inventory.manifest.observed_ms,
                );
                (next.manifest() == &inventory.manifest).then_some(next)
            });
        if !unchanged && refreshed.is_none() {
            for object in
                std::iter::once(&inventory.public).chain(if self.service.role == Role::Worker {
                    inventory.units.iter()
                } else {
                    inventory.packing.iter()
                })
            {
                self.object(object).await?;
            }
        }
        Ok(Input::Work(Acquired {
            sync: self,
            inventory,
            inventory_object,
            previous,
            unchanged,
            refreshed,
        }))
    }
}

// Tokens borrow the sole synchronization owner. No phase can run twice or
// overlap another pass; only publication may persist and switch the active view.
struct Acquired<'a> {
    sync: &'a mut StatusSync,
    inventory: Inventory,
    inventory_object: Object,
    previous: Option<Arc<Generation>>,
    unchanged: bool,
    refreshed: Option<Generation>,
}
struct Prepared<'a> {
    sync: &'a mut StatusSync,
    inventory: Inventory,
    inventory_object: Object,
    candidate: Option<Generation>,
}
impl<'a> Acquired<'a> {
    async fn prepare(self) -> Result<Input<Prepared<'a>>, Failure> {
        let Self {
            sync,
            inventory,
            inventory_object,
            previous,
            unchanged,
            refreshed,
        } = self;
        let candidate = if unchanged {
            previous.as_ref().map(|g| g.as_ref().clone())
        } else if refreshed.is_some() {
            refreshed
        } else {
            let root = sync.service.dir.join("objects");
            let i = inventory.clone();
            let worker = sync.service.role == Role::Worker;
            let backend = sync.backend;
            Some(
                tokio::task::spawn_blocking(move || {
                    Generation::load_prepared(
                        &i,
                        worker,
                        |o| root.join(&o.sha256),
                        backend,
                        previous.as_deref(),
                    )
                })
                .await??,
            )
        };
        Ok(Input::Work(Prepared {
            sync,
            inventory,
            inventory_object,
            candidate,
        }))
    }
}
impl Prepared<'_> {
    async fn publish(self) -> Result<u64, Failure> {
        let Self {
            sync,
            inventory,
            inventory_object,
            candidate,
        } = self;
        let latest = sync.current().await?;
        if !latest.eligible {
            return Err("Status publication withdrawn".into());
        }
        inventory.validate(&latest)?;
        sync.authority(latest, &inventory.manifest).await?;
        sync.pin_worker(&inventory.manifest).await?;
        inventory.manifest.fresh(now_ms())?;
        if let Some(candidate) = candidate {
            let root = sync.service.dir.clone();
            let bytes = serde_json::to_vec(&inventory_object)?;
            if let Err(error) =
                tokio::task::spawn_blocking(move || persist(&root, "active.json", &bytes)).await?
            {
                sync.revoke().await;
                sync.service.state.lock().await.poisoned = true;
                return Err(error.into());
            }
            let mut state = sync.service.state.lock().await;
            let mut retired_owner = None;
            let retired = match state.controller.as_ref() {
                Some(controller) if !controller.revoked() => {
                    if controller.current().manifest().id() != candidate.manifest().id() {
                        controller.activate_deferring_release(candidate)?
                    } else {
                        Vec::new()
                    }
                }
                _ => {
                    retired_owner = state.controller.replace(Controller::new(candidate));
                    Vec::new()
                }
            };
            state.inventory = Some(inventory_object.clone());
            state.poisoned = false;
            drop(state);
            if !retired.is_empty() || retired_owner.is_some() {
                tokio::task::spawn_blocking(move || drop((retired, retired_owner)));
            }
            sync.loaded = Some(inventory.clone());
            sync.retained.push(inventory_object.clone());
            if sync.retained.len() > 2 {
                sync.retained.remove(0);
            }
            let root = sync.service.dir.join("objects");
            let retained = sync.retained.clone();
            tokio::task::spawn_blocking(move || collect(&root, &retained)).await??;
        }
        if sync.service.role == Role::Router {
            let release = sync
                .service
                .serving_http
                .post(format!(
                    "{}/v1/provider-commit/{}",
                    sync.service.worker_origin,
                    hex::encode(inventory.manifest.id())
                ))
                .send()
                .await;
            match release {
                Ok(response) if response.status().is_success() => {}
                _ => {
                    sync.deferred = true;
                    tracing::warn!("Status router handover acknowledgment deferred");
                }
            }
        }
        Ok(inventory.manifest.generation)
    }
}

impl SyncTask for StatusSync {
    type Report = u64;
    type Error = Failure;
    async fn synchronize(&mut self) -> Result<u64, Failure> {
        self.deferred = false;
        self.service.state.lock().await.synchronization = "synchronizing".into();
        let result = match self.update().await {
            // A newer pointer won the authority handshake. Nothing was activated;
            // retry at the normal cadence rather than spending half the serving
            // freshness window in transport-failure backoff.
            Err(error) if error.is::<PublicationAdvanced>() => {
                self.deferred = true;
                Ok(self
                    .service
                    .state
                    .lock()
                    .await
                    .controller
                    .as_ref()
                    .map_or(0, |c| c.current().manifest().generation))
            }
            result => result,
        };
        self.service.state.lock().await.synchronization = if result.is_ok() && self.deferred {
            "waiting_for_peer"
        } else if result.is_ok() {
            "synchronized"
        } else {
            "refused"
        }
        .into();
        result
    }
    fn terminal(&self, error: &Failure) -> bool {
        pir_sync_manager::is_incompatible(error.as_ref())
    }
    fn report(&self, result: &Result<u64, Failure>) {
        if let Err(error) = result {
            tracing::warn!(%error,"Status synchronization refused");
        }
    }
}
fn persist(root: &FsPath, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    pir_control::durable::write_atomic(root, name, |f| f.write_all(bytes))?;
    pir_control::durable::sync_dir(root)
}
fn verify_file(path: &FsPath, object: &Object) -> Result<(), Failure> {
    let file = fs::File::open(path)?;
    if file.metadata()?.len() != object.bytes {
        return Err("Status cached object length differs".into());
    }
    let mut reader = std::io::BufReader::new(file);
    let mut buffer = [0u8; 65536];
    let mut hash = Sha256::new();
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    if hex::encode(hash.finalize()) != object.sha256 {
        return Err("Status cached object digest differs".into());
    }
    Ok(())
}

fn collect(root: &FsPath, inventories: &[Object]) -> Result<(), Failure> {
    let mut keep = BTreeSet::new();
    for object in inventories {
        keep.insert(object.sha256.clone());
        let bytes = fs::read(root.join(&object.sha256))?;
        object.verify(&bytes)?;
        let inventory: Inventory = serde_json::from_slice(&bytes)?;
        inventory.validate_material()?;
        for item in inventory
            .units
            .iter()
            .chain(&inventory.packing)
            .chain(std::iter::once(&inventory.public))
        {
            keep.insert(item.sha256.clone());
        }
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if valid_digest(&name) && entry.file_type()?.is_file() && !keep.contains(&name) {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
