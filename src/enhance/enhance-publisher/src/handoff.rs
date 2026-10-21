//! Offline recovery-state handoff for the direct HTTPS cutover and rollback.
//!
//! The deployment transaction stops the source first. These additional file
//! locks refuse a live source even when invoked outside that transaction. No
//! runtime loads the previous controller or treats copied bytes as authority.
use enhance_query_server::{
    artifact,
    protocol::{self, Lifecycle, Manifest, MAX_QUERY_SHARDS},
    publication::{Current, VERSION},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    path::Path,
};

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Recovery {
    epoch: u64,
    domain_epochs: BTreeMap<u64, String>,
    revoked: BTreeSet<String>,
    sealed: BTreeMap<u64, (u64, String)>,
}
impl Recovery {
    fn validate(&self) -> Result<(), String> {
        for (id, epoch) in &self.domain_epochs {
            let number = decimal(epoch)?;
            if *id >= MAX_QUERY_SHARDS || number > self.epoch {
                return Err("invalid domain recovery floor".into());
            }
        }
        if self.revoked.iter().any(|id| !protocol::canonical_hash(id))
            || self
                .sealed
                .iter()
                .any(|(id, (_, hash))| *id >= MAX_QUERY_SHARDS || !protocol::canonical_hash(hash))
        {
            return Err("invalid recovery identities".into());
        }
        Ok(())
    }
    fn merge(&mut self, other: &Self) {
        self.epoch = self.epoch.max(other.epoch);
        self.revoked.extend(other.revoked.iter().cloned());
        for (id, epoch) in &other.domain_epochs {
            let previous = self.domain_epochs.entry(*id).or_insert_with(|| "0".into());
            if decimal(epoch).unwrap() > decimal(previous).unwrap() {
                *previous = epoch.clone();
            }
        }
    }
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PublisherState {
    version: u16,
    next_generation: u64,
    lifecycle: Lifecycle,
    recovery: Recovery,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Retained {
    manifest: Manifest,
    inventory: enhance_query_server::publication::Object,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DistributionState {
    current: Current,
    retained: Vec<Retained>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    version: u16,
    previous_sha256: String,
    rollback_epoch: Option<u64>,
}

fn decimal(text: &str) -> Result<u64, String> {
    let value: u64 = text.parse().map_err(|_| "invalid recovery integer")?;
    if value.to_string() != text {
        return Err("noncanonical recovery integer".into());
    }
    Ok(value)
}
fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    crate::publication::read_metadata(path)
}
fn save<T: Serialize>(root: &Path, name: &str, value: &T) -> Result<(), String> {
    artifact::write_atomic(root, name, |file| {
        serde_json::to_writer(file, value).map_err(std::io::Error::other)
    })
    .map(drop)
    .map_err(|e| e.to_string())
}
fn lock(root: &Path, name: &str) -> Result<File, String> {
    fs::create_dir_all(root).map_err(|e| e.to_string())?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join(name))
        .map_err(|e| e.to_string())?;
    file.try_lock()
        .map_err(|_| format!("state is still owned: {name}"))?;
    Ok(file)
}

/// Carry old floors forward, or fence every retained session before restoring
/// the previous runtime. A rollback reserves one durable epoch before writing
/// either state format; interrupted retries use that same reservation.
pub fn transfer(data: &Path, rollback: bool) -> Result<Value, String> {
    let previous_path = data.join("control/controller.json");
    if !data.is_absolute() || !previous_path.is_file() {
        return Err(
            "handoff needs the stopped canonical source and its previous controller state".into(),
        );
    }
    let root = data.join("publication");
    let https = root.join("https");
    let _owners = [
        lock(data, "publisher.lock")?,
        lock(&data.join("control"), "controller.lock")?,
        lock(&root, "publisher.lock")?,
        lock(&https, "distribution.lock")?,
    ];
    let mut previous: Value = read(&previous_path)?;
    if !matches!(previous["version"].as_u64(), Some(7 | 8))
        || !previous["operation"].is_null()
        || !previous["recovery"]["rollback_to"].is_null()
        || ["draining", "pending_commits", "pending_aborts"]
            .iter()
            .any(|name| {
                previous[*name]
                    .as_array()
                    .is_none_or(|items| !items.is_empty())
            })
    {
        return Err("previous controller is unsupported or has unfinished decisions".into());
    }
    let previous_bytes = serde_json::to_vec(&previous).map_err(|e| e.to_string())?;
    let previous_sha256 = hex::encode(Sha256::digest(previous_bytes));
    let mut recovery = previous["recovery"].clone();
    recovery
        .as_object_mut()
        .ok_or("missing previous recovery state")?
        .remove("rollback_to");
    recovery["epoch"] = json!(decimal(
        recovery["epoch"]
            .as_str()
            .ok_or("missing previous recovery epoch")?
    )?);
    let old_recovery: Recovery = serde_json::from_value(recovery).map_err(|e| e.to_string())?;
    old_recovery.validate()?;
    let old_next = previous["next_generation"]
        .as_u64()
        .filter(|n| *n > 0)
        .ok_or("missing previous generation reservation")?;
    let old_lifecycle: Lifecycle =
        serde_json::from_value(previous["lifecycle"].clone()).map_err(|e| e.to_string())?;
    if old_lifecycle.next_id > MAX_QUERY_SHARDS {
        return Err("previous lifecycle exceeds the protocol ceiling".into());
    }
    let mut manifests: Vec<Manifest> =
        serde_json::from_value(previous["published"].clone()).map_err(|e| e.to_string())?;
    let receipt_path = root.join("handoff.json");
    let mut receipt = if receipt_path.exists() {
        let receipt: Receipt = read(&receipt_path)?;
        if receipt.version != 1 || (!rollback && receipt.rollback_epoch.is_some()) {
            return Err("unsupported or already rolled-back handoff".into());
        }
        receipt
    } else {
        if rollback || root.join("publisher.json").exists() || https.join("current.json").exists() {
            return Err("state exists without a handoff receipt; refusing to overwrite it".into());
        }
        let receipt = Receipt {
            version: 1,
            previous_sha256: previous_sha256.clone(),
            rollback_epoch: None,
        };
        save(&root, "handoff.json", &receipt)?;
        receipt
    };
    if !rollback && receipt.previous_sha256 != previous_sha256 {
        return Err("previous state changed since handoff began".into());
    }
    let mut state: PublisherState = if root.join("publisher.json").exists() {
        read(&root.join("publisher.json"))?
    } else {
        PublisherState {
            version: 1,
            next_generation: old_next,
            lifecycle: old_lifecycle,
            recovery: old_recovery.clone(),
        }
    };
    state.recovery.validate()?;
    if state.version != 1
        || state.next_generation == 0
        || state.lifecycle.next_id > MAX_QUERY_SHARDS
    {
        return Err("invalid publisher state".into());
    }
    let mut distribution: DistributionState = if https.join("current.json").exists() {
        read(&https.join("current.json"))?
    } else {
        DistributionState {
            current: Current {
                version: VERSION,
                revision: 1,
                withdrawal_revision: 1,
                recovery_epoch: state.recovery.epoch,
                revoked: state.recovery.revoked.clone(),
                eligible: false,
                manifest: None,
                inventory: None,
            },
            retained: Vec::new(),
        }
    };
    distribution.current.validate()?;
    state.recovery.merge(&old_recovery);
    state.recovery.epoch = state
        .recovery
        .epoch
        .max(distribution.current.recovery_epoch);
    state
        .recovery
        .revoked
        .extend(distribution.current.revoked.iter().cloned());
    manifests.extend(distribution.retained.iter().map(|r| r.manifest.clone()));
    if let Some(manifest) = &distribution.current.manifest {
        manifests.push(manifest.clone());
    }
    state.next_generation = state.next_generation.max(old_next);
    for manifest in &manifests {
        manifest.validate()?;
        state.next_generation = state.next_generation.max(
            manifest
                .generation
                .checked_add(1)
                .ok_or("generation exhausted")?,
        );
        let floor = Recovery {
            epoch: manifest.recovery_epoch,
            domain_epochs: manifest.domain_recovery_epochs.clone(),
            ..Default::default()
        };
        floor.validate()?;
        state.recovery.merge(&floor);
    }
    if rollback {
        let epoch = match receipt.rollback_epoch {
            Some(epoch) => epoch,
            None => {
                let epoch = state
                    .recovery
                    .epoch
                    .checked_add(1)
                    .ok_or("recovery epoch exhausted")?;
                receipt.rollback_epoch = Some(epoch);
                save(&root, "handoff.json", &receipt)?;
                epoch
            }
        };
        if state.recovery.epoch > epoch {
            return Err("source advanced after rollback was reserved".into());
        }
        state.recovery.epoch = epoch;
        for id in 0..MAX_QUERY_SHARDS {
            state.recovery.domain_epochs.insert(id, epoch.to_string());
        }
        for manifest in &manifests {
            for shard in &manifest.coverage.shards {
                state
                    .recovery
                    .revoked
                    .insert(hex::encode(manifest.session_id(shard.id)?));
            }
        }
    }
    state.recovery.validate()?;
    distribution.current.recovery_epoch = state.recovery.epoch;
    distribution.current.revoked = state.recovery.revoked.clone();
    distribution.current.revision = distribution
        .current
        .revision
        .checked_add(1)
        .ok_or("publication revision exhausted")?;
    distribution.current.withdrawal_revision = distribution.current.revision;
    distribution.current.eligible = false;
    save(&root, "publisher.json", &state)?;
    save(&https, "current.json", &distribution)?;
    if rollback {
        previous["next_generation"] = json!(state.next_generation);
        previous["lifecycle"] =
            serde_json::to_value(&state.lifecycle).map_err(|e| e.to_string())?;
        previous["recovery"] = serde_json::to_value(&state.recovery).map_err(|e| e.to_string())?;
        previous["recovery"]["epoch"] = json!(state.recovery.epoch.to_string());
        previous["recovery"]["rollback_to"] = Value::Null;
        previous["published"] = json!([]);
        save(&data.join("control"), "controller.json", &previous)?;
    }
    Ok(
        json!({"rollback":rollback, "recovery_epoch":state.recovery.epoch,
        "next_generation":state.next_generation, "withdrawal_revision":distribution.current.withdrawal_revision}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn previous(root: &Path) {
        fs::create_dir_all(root.join("control")).unwrap();
        save(&root.join("control"), "controller.json", &json!({
            "version":8, "next_generation":23, "lifecycle":Lifecycle::default(),
            "recovery":{"epoch":"4", "domain_epochs":{"0":"4"},
                "revoked":["ab".repeat(32)], "sealed":{"0":[3_428_143,"12".repeat(32)]}, "rollback_to":null},
            "operation":null, "draining":[], "pending_commits":[], "pending_aborts":[],
            "published":[], "epoch":91, "revision":17, "groups":[], "assignments":{},
            "unrelated_previous_field":{"kept":true}
        })).unwrap();
    }

    #[test]
    fn import_preserves_every_floor_without_granting_serving_authority() {
        let root = tempfile::tempdir().unwrap();
        previous(root.path());
        let old = fs::read(root.path().join("control/controller.json")).unwrap();
        let report = transfer(root.path(), false).unwrap();
        assert_eq!(report["recovery_epoch"], 4);
        assert_eq!(report["next_generation"], 23);
        let state: PublisherState = read(&root.path().join("publication/publisher.json")).unwrap();
        assert!(state.recovery.revoked.contains(&"ab".repeat(32)));
        assert_eq!(state.recovery.domain_epochs[&0], "4");
        assert_eq!(state.recovery.sealed[&0].0, 3_428_143);
        let distribution: DistributionState =
            read(&root.path().join("publication/https/current.json")).unwrap();
        assert!(!distribution.current.eligible);
        assert!(distribution.current.manifest.is_none());
        transfer(root.path(), false).unwrap();
        assert_eq!(
            fs::read(root.path().join("control/controller.json")).unwrap(),
            old
        );
    }

    #[test]
    fn rollback_merges_new_floors_and_retries_one_reserved_epoch_after_interruption() {
        let root = tempfile::tempdir().unwrap();
        previous(root.path());
        transfer(root.path(), false).unwrap();
        let publication = root.path().join("publication");
        let mut state: PublisherState = read(&publication.join("publisher.json")).unwrap();
        state.next_generation = 29;
        state.recovery.epoch = 5;
        state.recovery.domain_epochs.insert(0, "5".into());
        state.recovery.revoked.insert("cd".repeat(32));
        save(&publication, "publisher.json", &state).unwrap();
        // Simulate interruption after reserving rollback and persisting the new
        // source fence, before updating distribution or the previous format.
        let mut receipt: Receipt = read(&publication.join("handoff.json")).unwrap();
        receipt.rollback_epoch = Some(6);
        save(&publication, "handoff.json", &receipt).unwrap();
        state.recovery.epoch = 6;
        save(&publication, "publisher.json", &state).unwrap();
        for _ in 0..2 {
            let report = transfer(root.path(), true).unwrap();
            assert_eq!(report["recovery_epoch"], 6);
            assert_eq!(report["next_generation"], 29);
            let restored: Value = read(&root.path().join("control/controller.json")).unwrap();
            assert_eq!(restored["recovery"]["epoch"], "6");
            assert_eq!(
                restored["recovery"]["domain_epochs"]
                    .as_object()
                    .unwrap()
                    .len(),
                MAX_QUERY_SHARDS as usize
            );
            assert_eq!(restored["recovery"]["revoked"].as_array().unwrap().len(), 2);
            assert_eq!(
                restored["epoch"], 91,
                "the previous controller lease is not a recovery epoch"
            );
            assert_eq!(restored["unrelated_previous_field"]["kept"], true);
            assert_eq!(restored["published"], json!([]));
        }
        assert!(
            transfer(root.path(), false).is_err(),
            "a completed rollback needs a fresh reviewed cutover"
        );
    }

    #[test]
    fn running_owner_and_unfinished_recovery_refuse_without_overwriting_state() {
        let root = tempfile::tempdir().unwrap();
        previous(root.path());
        let owner = lock(root.path(), "publisher.lock").unwrap();
        assert!(transfer(root.path(), false).unwrap_err().contains("owned"));
        assert!(!root.path().join("publication/publisher.json").exists());
        drop(owner);
        let mut old: Value = read(&root.path().join("control/controller.json")).unwrap();
        old["recovery"]["rollback_to"] = json!(0);
        save(&root.path().join("control"), "controller.json", &old).unwrap();
        assert!(transfer(root.path(), false)
            .unwrap_err()
            .contains("unfinished"));
        assert!(!root.path().join("publication/publisher.json").exists());
    }
}
