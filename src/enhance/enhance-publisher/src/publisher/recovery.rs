//! Open recovery floors and persist successors before exposing them in memory.
use super::{EnhancePublisher, PublisherRuntimeState};
use crate::{
    chain_source::ZakuraClient, distribution::PublicationDistribution,
    durable_state::PublisherState,
};
use enhance_material::artifact;
use enhance_material::{
    protocol::{self, MAX_QUERY_SHARDS},
    publication::Revocation,
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
            })),
            distribution,
            preparation: Arc::new(Semaphore::new(1)),
            incarnation: format!("{:032x}", publisher_incarnation_id()),
            _owner: Arc::new(owner),
        })
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
    if bytes.len() as u64 > enhance_material::publication::METADATA_LIMIT {
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
