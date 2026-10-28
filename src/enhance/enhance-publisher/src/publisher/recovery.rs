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

/// Names the inventory of a publication [`EnhancePublisher::forgo_restore`] refused.
const FORGONE_RESTORE: &str = "restore-forgone";
/// Armed before a staged journal swap. Committed only once that swap replaces
/// the live journal; a rejected swap deletes it.
const FORGONE_PENDING: &str = "restore-forgone.pending";

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
    /// Remember that adopting `receipt_id` would replace the journal this
    /// publication was built from. The refusal is not in force until that swap
    /// finishes: [`Self::settle_armed_forgo`] commits it, and
    /// [`Self::disarm_forgo`] drops it when adoption keeps the live journal.
    pub fn arm_forgo(&self, receipt_id: &str) -> Result<(), String> {
        if !safe_receipt_id(receipt_id) {
            return Err("rebuild receipt id is not a safe directory suffix".into());
        }
        let Some(object) = self.distribution.durable_current().inventory else {
            return Ok(());
        };
        let record = format!("{}\n{receipt_id}\n", object.sha256);
        self.write_refused(record.as_bytes(), FORGONE_PENDING)
    }
    /// The staged swap did not replace the live journal. Drop the armed
    /// refusal; a refusal already committed by a finished swap stays.
    pub fn disarm_forgo(&self) -> Result<(), String> {
        self.remove_refused(FORGONE_PENDING)
    }
    /// Commit an armed refusal whose journal swap finished, including one whose
    /// process died after the swap and before [`Self::forgo_restore`].
    ///
    /// The backup directory is the predecessor journal `adopt_staged` leaves
    /// beside a live replacement. An interrupted swap has that backup and no
    /// live journal yet, so it stays armed until adoption finishes or rejects.
    /// A pending record whose swap neither finished nor is still staged was
    /// rejected and is removed.
    pub fn settle_armed_forgo(&self, data_dir: &Path) -> Result<(), String> {
        let Some((hash, receipt_id)) = self.read_armed_forgo()? else {
            return Ok(());
        };
        let replaced = data_dir
            .join(format!("enhance.before-rebuild-{receipt_id}"))
            .is_dir()
            && data_dir.join("enhance").is_dir();
        let still_staged = data_dir
            .join(crate::rebuild::STAGED_DIR)
            .join(crate::rebuild::RECEIPT_FILE)
            .is_file();
        if replaced {
            self.write_refused(hash.as_bytes(), FORGONE_RESTORE)?;
            self.remove_refused(FORGONE_PENDING)?;
        } else if !still_staged {
            self.remove_refused(FORGONE_PENDING)?;
        }
        Ok(())
    }
    /// Never serve the current publication again, in this process or after a restart.
    /// Call only after a rebuilt journal has replaced the live one: its records may
    /// differ from the ones this publication was built from. A later publication may
    /// be restored again. A swap that has not finished is [`Self::arm_forgo`].
    pub fn forgo_restore(&self) -> Result<(), String> {
        self.state.lock().unwrap().restorable = false;
        let Some(object) = self.distribution.durable_current().inventory else {
            return Ok(());
        };
        self.write_refused(object.sha256.as_bytes(), FORGONE_RESTORE)?;
        self.remove_refused(FORGONE_PENDING)
    }
    fn write_refused(&self, bytes: &[u8], name: &str) -> Result<(), String> {
        artifact::write_atomic(&self.root, name, |file| file.write_all(bytes))
            .and_then(|_| File::open(&self.root)?.sync_all())
            .map_err(|e| e.to_string())
    }
    fn remove_refused(&self, name: &str) -> Result<(), String> {
        let path = self.root.join(name);
        if path.exists() {
            fs::remove_file(&path).map_err(|e| e.to_string())?;
            File::open(&self.root)
                .map_err(|e| e.to_string())?
                .sync_all()
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    /// The armed inventory hash and receipt id, when the pending record is present
    /// and well formed.
    fn read_armed_forgo(&self) -> Result<Option<(String, String)>, String> {
        let path = self.root.join(FORGONE_PENDING);
        if !path.exists() {
            return Ok(None);
        }
        let text = fs::read_to_string(&path).map_err(|e| e.to_string())?;
        let mut lines = text.lines();
        let hash = lines.next().unwrap_or("");
        let receipt_id = lines.next().unwrap_or("");
        if hash.len() == 64
            && hash
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            && safe_receipt_id(receipt_id)
            && lines.next().is_none()
        {
            return Ok(Some((hash.to_string(), receipt_id.to_string())));
        }
        Err("armed Enhance restore refusal is corrupt".into())
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
        let forgone = fs::read(self.root.join(FORGONE_RESTORE)).ok();
        if forgone.as_deref() == Some(object.sha256.as_bytes()) {
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
fn safe_receipt_id(receipt_id: &str) -> bool {
    !receipt_id.is_empty()
        && receipt_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
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
