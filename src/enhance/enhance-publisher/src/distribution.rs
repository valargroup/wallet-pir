//! Read-only HTTPS distribution backed by immutable material and fresh node checks.
//! Publication and revocation changes are durable before any HTTP observer can
//! see them. Restart never restores authority from cached metadata alone.
mod authority;
mod http;
mod retention;

use crate::{
    chain_source::ZakuraClient,
    durable_state::{DistributionState, RetainedPublication},
    publication::{read_metadata, PreparedPublication},
};
use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use enhance_shared::{
    artifact,
    protocol::{digest, Manifest, RETAINED_GENERATIONS},
    publication::{Current, Inventory, Object, Revocation, METADATA_LIMIT, PREFIX, VERSION},
};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

/// Durable publication selection and read-only artifact distribution.
/// Stored eligibility is checked against the chain before granting authority.
#[derive(Clone)]
pub struct PublicationDistribution {
    root: PathBuf,
    rpc: ZakuraClient,
    disk: Arc<Mutex<DistributionState>>,
    poisoned: Arc<AtomicBool>,
    _owner: Arc<File>,
}

impl PublicationDistribution {
    /// Lock distribution state for this owner's lifetime and validate retained
    /// descriptors. Persist a withdrawal on every reopen; cached metadata alone
    /// never restores fresh chain authority.
    pub fn open(root: &Path, rpc: ZakuraClient) -> Result<Self, String> {
        fs::create_dir_all(root).map_err(|e| e.to_string())?;
        let owner = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join("distribution.lock"))
            .map_err(|e| e.to_string())?;
        owner
            .try_lock()
            .map_err(|_| "Enhance distribution already owned")?;
        let mut disk: DistributionState = if root.join("current.json").exists() {
            read_metadata(&root.join("current.json"))?
        } else {
            DistributionState {
                current: Current {
                    version: VERSION,
                    revision: 1,
                    withdrawal_revision: 1,
                    recovery_epoch: 0,
                    revoked: BTreeSet::new(),
                    eligible: false,
                    manifest: None,
                    inventory: None,
                },
                retained: Vec::new(),
            }
        };
        disk.current.validate()?;
        if disk.retained.len() > RETAINED_GENERATIONS {
            return Err("too many retained Enhance publications".into());
        }
        for retained in &disk.retained {
            retained.manifest.validate()?;
            retained.inventory.validate()?;
            if retained.inventory.bytes > METADATA_LIMIT {
                return Err("retained inventory exceeds metadata limit".into());
            }
        }
        // A successor must revalidate material and chain authority before the
        // old current can become eligible again. Keep every durable fence.
        disk.current.revision = disk
            .current
            .revision
            .checked_add(1)
            .ok_or("distribution revision exhausted")?;
        disk.current.withdrawal_revision = disk.current.revision;
        disk.current.eligible = false;
        persist(root, &disk)?;
        Ok(Self {
            root: root.into(),
            rpc,
            disk: Arc::new(Mutex::new(disk)),
            poisoned: Arc::new(AtomicBool::new(false)),
            _owner: Arc::new(owner),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn durable_current(&self) -> Current {
        self.disk.lock().unwrap().current.clone()
    }
    /// Last durably installed manifest, for operator tooling. This does not
    /// authorize serving; consumers must obtain fresh source authority.
    pub fn published_manifest(&self) -> Option<Manifest> {
        self.disk.lock().unwrap().current.manifest.clone()
    }
    pub(crate) fn retained_manifests(&self) -> Vec<Manifest> {
        self.disk
            .lock()
            .unwrap()
            .retained
            .iter()
            .map(|r| r.manifest.clone())
            .collect()
    }
    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    /// Install a completed publication after hashing every object it names.
    /// No consumer address or acknowledgment participates; the only remote
    /// dependency is the canonical chain source.
    /// Import checked product material through the host's publication engine.
    pub async fn publish<E: pir_pipeline::publisher::PublisherExecutor>(
        &self,
        manifest: Manifest,
        inventory: Inventory,
        executor: E,
    ) -> Result<(), String> {
        executor
            .publish(
                crate::publisher::preparation::EnhancePublication::inventory(
                    self, manifest, inventory,
                )?,
            )
            .await
    }
    pub async fn publish_prepared<E: pir_pipeline::publisher::PublisherExecutor>(
        &self,
        manifest: Manifest,
        prepared: PreparedPublication,
        executor: E,
    ) -> Result<(), String> {
        executor
            .publish(crate::publisher::preparation::EnhancePublication::prepared(
                self, manifest, prepared,
            )?)
            .await
    }

    pub(crate) fn commit_candidate(
        &self,
        predecessor: &Current,
        manifest: Manifest,
        object: Object,
    ) -> Result<(), String> {
        let mut disk = self.disk.lock().unwrap();
        if disk.current != *predecessor {
            return Err("Enhance publication authority changed during preparation".into());
        }
        for shard in &manifest.coverage.shards {
            if disk
                .current
                .revoked
                .contains(&hex::encode(manifest.session_id(shard.id)?))
            {
                return Err("cannot publish invalidated Enhance material".into());
            }
        }
        let mut next = disk.clone();
        next.current.revision = next
            .current
            .revision
            .checked_add(1)
            .ok_or("distribution revision exhausted")?;
        next.current.recovery_epoch = next.current.recovery_epoch.max(manifest.recovery_epoch);
        next.current.eligible = true;
        next.current.manifest = Some(manifest.clone());
        next.current.inventory = Some(object.clone());
        next.retained
            .retain(|r| digest(&r.manifest) != digest(&manifest));
        next.retained.insert(
            0,
            RetainedPublication {
                manifest,
                inventory: object,
            },
        );
        next.retained.truncate(RETAINED_GENERATIONS);
        self.commit(&mut disk, next)?;
        drop(disk);
        self.collect_retired()?;
        Ok(())
    }

    /// Withdraw before journal rewind, even when the rewind needs no new domain
    /// epoch. RetainedPublication material is never deleted by an authority change.
    pub fn withdraw(&self, revocation: &Revocation) -> Result<(), String> {
        let mut disk = self.disk.lock().unwrap();
        let mut next = disk.clone();
        if revocation.recovery_epoch < next.current.recovery_epoch
            || !next.current.revoked.is_subset(&revocation.sessions)
        {
            return Err("nonmonotonic Enhance distribution fence".into());
        }
        next.current.recovery_epoch = revocation.recovery_epoch;
        next.current.revoked = revocation.sessions.clone();
        next.current.eligible = false;
        next.current.revision = next
            .current
            .revision
            .checked_add(1)
            .ok_or("distribution revision exhausted")?;
        next.current.withdrawal_revision = next.current.revision;
        self.commit(&mut disk, next)
    }

    fn commit(&self, disk: &mut DistributionState, next: DistributionState) -> Result<(), String> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err("Enhance distribution persistence failed; reopen required".into());
        }
        next.current.validate()?;
        if !next.current.succeeds(&disk.current) {
            return Err("Enhance distribution would replay authority".into());
        }
        if let Err(error) = persist(&self.root, &next) {
            self.poisoned.store(true, Ordering::Release);
            return Err(error);
        }
        *disk = next;
        Ok(())
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route(&format!("{PREFIX}current"), get(http::current))
            .route(&format!("{PREFIX}authority"), post(http::authority))
            .route(
                &format!("{PREFIX}inventories/:digest"),
                get(http::inventory),
            )
            .route(&format!("{PREFIX}objects/:digest"), get(http::object))
            .layer(DefaultBodyLimit::max(1024))
            .with_state(self.clone())
    }
}

fn persist(root: &Path, disk: &DistributionState) -> Result<(), String> {
    let bytes = serde_json::to_vec(disk).map_err(|e| e.to_string())?;
    if bytes.len() as u64 > METADATA_LIMIT {
        return Err("distribution state exceeds metadata limit".into());
    }
    artifact::write_atomic(root, "current.json", |f| f.write_all(&bytes))
        .map_err(|e| e.to_string())?;
    File::open(root)
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests;
