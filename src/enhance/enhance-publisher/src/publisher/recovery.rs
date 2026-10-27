//! Open recovery floors and persist successors before exposing them in memory.
use super::{EnhancePublisher, PublisherRuntimeState};
use crate::{
    chain_source::ZakuraClient, distribution::PublicationDistribution,
    durable_state::PublisherState,
};
use enhance_shared::artifact;
use enhance_shared::{
    protocol::{self, MAX_QUERY_SHARDS},
    publication::{Inventory, Revocation},
};
use std::fs::{self, OpenOptions};
use std::{
    fs::File,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

impl EnhancePublisher {
    pub(super) fn open_recovered(root: &Path, rpc: ZakuraClient) -> Result<Self, String> {
        fs::create_dir_all(root).map_err(|e| e.to_string())?;
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("publisher.lock"))
            .map_err(|e| e.to_string())?;
        owner
            .try_lock()
            .map_err(|_| "Enhance publisher state already owned")?;
        let path = root.join("publisher.json");
        if !path.exists() && root.join("https/current.json").exists() {
            return Err(
                "publisher recovery state is missing; refusing to reconstruct lower floors".into(),
            );
        }
        let mut durable: PublisherState = if path.exists() {
            crate::publication::read_metadata(&path)?
        } else {
            PublisherState::default()
        };
        if durable.version != 1
            || durable.next_generation == 0
            || durable.lifecycle.next_id > MAX_QUERY_SHARDS
            || durable.recovery.domain_epochs.iter().any(|(id, value)| {
                *id >= MAX_QUERY_SHARDS
                    || value.parse::<u64>().map_or(true, |epoch| {
                        epoch.to_string() != *value || epoch > durable.recovery.epoch
                    })
            })
            || durable
                .recovery
                .revoked
                .iter()
                .any(|id| !protocol::canonical_hash(id))
            || durable
                .recovery
                .sealed
                .iter()
                .any(|(id, (_, hash))| *id >= MAX_QUERY_SHARDS || !protocol::canonical_hash(hash))
        {
            return Err("invalid Enhance publisher recovery state".into());
        }
        if durable.next_generation > 1 && !root.join("https/current.json").exists() {
            return Err(
                "publisher distribution state is missing; refusing lower publication floors".into(),
            );
        }
        persist_publisher_state(root, &durable)?;
        let distribution = PublicationDistribution::open(&root.join("https"), rpc)?;
        let floor = distribution.durable_current();
        durable.recovery.epoch = durable.recovery.epoch.max(floor.recovery_epoch);
        durable.recovery.revoked.extend(floor.revoked);
        for manifest in distribution.retained_manifests() {
            durable.next_generation = durable.next_generation.max(
                manifest
                    .generation
                    .checked_add(1)
                    .ok_or("generation exhausted")?,
            );
            for (id, value) in manifest.domain_recovery_epochs {
                let previous = durable
                    .recovery
                    .domain_epochs
                    .entry(id)
                    .or_insert_with(|| "0".into());
                let epoch: u64 = value.parse().map_err(|_| "invalid retained domain epoch")?;
                if epoch
                    > previous
                        .parse::<u64>()
                        .map_err(|_| "invalid domain epoch")?
                {
                    *previous = value;
                }
            }
        }
        persist_publisher_state(root, &durable)?;
        distribution.withdraw(&Revocation {
            recovery_epoch: durable.recovery.epoch,
            sessions: durable.recovery.revoked.clone(),
        })?;
        Ok(Self {
            root: root.into(),
            state: Arc::new(Mutex::new(PublisherRuntimeState {
                durable,
                poisoned: false,
                preparing: None,
                error: None,
                last_advancement: None,
                consecutive_failures: 0,
                restorable: true,
            })),
            distribution,
            preparation: Arc::new(Semaphore::new(1)),
            incarnation: format!("{:032x}", publisher_incarnation_id()),
            _owner: Arc::new(owner),
        })
    }
    /// Serve again the publication that was current when this publisher last stopped,
    /// once it is checked as a fresh publication would be. Opening withdrew it, so
    /// leases from the previous process are already void. It returns to service only
    /// when no recovery fence was raised and none of its sessions was revoked since it
    /// was published, its inventory still matches it, every object it names hashes to
    /// its recorded digest and its anchor block is still canonical; the commit then
    /// fails if anything changed authority meanwhile. Only the first call may restore,
    /// and none after [`Self::forgo_restore`]. Returns whether it was restored.
    pub async fn restore<E: pir_pipeline::publisher::PublisherExecutor>(
        &self,
        executor: E,
        shutdown: pir_pipeline::service::Shutdown,
    ) -> Result<bool, String> {
        if !std::mem::replace(&mut self.state.lock().unwrap().restorable, false) {
            return Ok(false);
        }
        let current = self.distribution.durable_current();
        let (Some(manifest), Some(object)) = (current.manifest.clone(), current.inventory.clone())
        else {
            return Ok(false);
        };
        if current.eligible {
            return Ok(false);
        }
        if manifest.recovery_epoch != current.recovery_epoch {
            return Err("a recovery fence followed the last Enhance publication".into());
        }
        let domains = self
            .state
            .lock()
            .unwrap()
            .durable
            .recovery
            .domain_epochs
            .clone();
        for shard in &manifest.coverage.shards {
            if current
                .revoked
                .contains(&hex::encode(manifest.session_id(shard.id)?))
            {
                return Err("the last Enhance publication's sessions were revoked".into());
            }
            if domains.get(&shard.id) != manifest.domain_recovery_epochs.get(&shard.id) {
                return Err("a domain fence followed the last Enhance publication".into());
            }
        }
        let inventory: Inventory = crate::publication::read_metadata(
            &self
                .distribution
                .root()
                .join("inventories")
                .join(format!("{}.json", object.sha256)),
        )?;
        if protocol::digest(&inventory) != object.sha256 {
            return Err("the last Enhance inventory differs from its digest".into());
        }
        executor
            .publish(
                super::preparation::EnhancePublication::inventory(
                    &self.distribution,
                    manifest,
                    inventory,
                )?
                .stopping(shutdown),
            )
            .await?;
        // A restored publication is a durable successor, as the first one this process
        // built would be; publication monitoring needs this time to read its metrics.
        self.state.lock().unwrap().last_advancement = Some(std::time::SystemTime::now());
        Ok(true)
    }
    pub(super) fn save_state(
        &self,
        state: &mut PublisherRuntimeState,
        next: PublisherState,
    ) -> Result<(), String> {
        if state.poisoned {
            return Err("Enhance publisher persistence failed; reopen required".into());
        }
        if let Err(error) = persist_publisher_state(&self.root, &next) {
            state.poisoned = true;
            self.distribution.poison();
            return Err(error);
        }
        state.durable = next;
        Ok(())
    }
}
fn persist_publisher_state(root: &Path, state: &PublisherState) -> Result<(), String> {
    let bytes = serde_json::to_vec(state).map_err(|e| e.to_string())?;
    if bytes.len() as u64 > enhance_shared::publication::METADATA_LIMIT {
        return Err("publisher state exceeds metadata limit".into());
    }
    artifact::write_atomic(root, "publisher.json", |file| file.write_all(&bytes))
        .and_then(|_| File::open(root)?.sync_all())
        .map_err(|e| e.to_string())
}
fn publisher_incarnation_id() -> u128 {
    // The incarnation scopes in-process, nonserializable work tokens. The
    // operating system supplies the same entropy used for request identities.
    use std::io::Read;
    let mut bytes = [0; 16];
    File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .expect("OS entropy");
    u128::from_le_bytes(bytes)
}
