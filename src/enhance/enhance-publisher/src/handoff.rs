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
use std::io::Read;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    path::Path,
};

// The predecessor retains completed operation receipts. Its offline file can
// exceed the current HTTPS metadata bound; that network bound stays unchanged.
const PREVIOUS_STATE_LIMIT: u64 = 16 * 1024 * 1024;

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
    transaction: String,
    completed: bool,
    initialized: bool,
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
fn previous_state(path: &Path) -> Result<Value, String> {
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|e| e.to_string())?
        .take(PREVIOUS_STATE_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 > PREVIOUS_STATE_LIMIT {
        return Err("previous controller state exceeds the offline handoff bound".into());
    }
    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
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

/// Refuse startup until every handoff write is durable. A reserved receipt is
/// not permission to initialize empty publication or recovery state.
pub fn verify_startup(data: &Path) -> Result<(), String> {
    if !data.join("control/controller.json").exists() {
        return Ok(());
    }
    let root = data.join("publication");
    let receipt: Receipt = read(&root.join("handoff.json"))
        .map_err(|_| "existing Enhance control state requires the offline HTTPS handoff")?;
    if receipt.version != 1
        || !receipt.completed
        || !receipt.initialized
        || receipt.rollback_epoch.is_some()
        || !root.join("publisher.json").is_file()
        || !root.join("https/current.json").is_file()
    {
        return Err("Enhance HTTPS handoff is incomplete or has rolled back".into());
    }
    let previous = previous_state(&data.join("control/controller.json"))?;
    let hash = hex::encode(Sha256::digest(
        serde_json::to_vec(&previous).map_err(|e| e.to_string())?,
    ));
    if receipt.previous_sha256 != hash {
        return Err("previous controller changed after the HTTPS handoff".into());
    }
    Ok(())
}

/// Carry old floors forward, or fence every retained session before restoring
/// the previous runtime. A rollback reserves one durable epoch before writing
/// either state format; interrupted retries use that same reservation.
pub fn transfer(data: &Path, rollback: bool, transaction: &str) -> Result<Value, String> {
    if !transaction.starts_with("publisher-")
        || transaction.len() > 160
        || !transaction
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err("handoff requires the owning publisher transaction".into());
    }
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
    let mut previous = previous_state(&previous_path)?;
    if rollback
        && !root.join("handoff.json").exists()
        && !root.join("publisher.json").exists()
        && !https.join("current.json").exists()
    {
        // No imported runtime could start without a completed receipt. A failed
        // forward validation can leave the predecessor's own pending recovery;
        // return it unchanged so its original runtime can reconcile that work.
        if !matches!(previous["version"].as_u64(), Some(7 | 8)) {
            return Err("unsupported previous controller".into());
        }
        let epoch = decimal(
            previous["recovery"]["epoch"]
                .as_str()
                .ok_or("missing previous epoch")?,
        )?;
        let generation = previous["next_generation"]
            .as_u64()
            .filter(|n| *n > 0)
            .ok_or("missing previous generation")?;
        return Ok(
            json!({"transaction":transaction,"rollback":true,"unchanged":true,"recovery_epoch":epoch,
            "next_generation":generation,"withdrawal_revision":0}),
        );
    }
    if !matches!(previous["version"].as_u64(), Some(7 | 8))
        || !previous["operation"].is_null()
        || !previous["recovery"]["rollback_to"].is_null()
        || ["pending_commits", "pending_aborts"].iter().any(|name| {
            previous[*name]
                .as_array()
                .is_none_or(|items| !items.is_empty())
        })
    {
        return Err("previous controller is unsupported or has unfinished decisions".into());
    }
    // These are already committed retention records, not unacknowledged
    // commit/abort decisions. Keep them for the restored runtime's cleanup.
    let draining = previous["draining"]
        .as_array()
        .ok_or("missing previous retention records")?;
    for operation in draining {
        let id = operation["id"]
            .as_str()
            .ok_or("invalid retained operation")?;
        let digest = operation["candidate_digest"]
            .as_str()
            .ok_or("missing retained decision")?;
        if operation["phase"] != "DRAINING"
            || !protocol::canonical_hash(digest)
            || previous["completed"][id].as_str() != Some(digest)
        {
            return Err("retention record is not a completed decision".into());
        }
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
    let old_next_id = old_lifecycle.next_id;
    if old_lifecycle.next_id > MAX_QUERY_SHARDS {
        return Err("previous lifecycle exceeds the protocol ceiling".into());
    }
    let mut manifests: Vec<Manifest> =
        serde_json::from_value(previous["published"].clone()).map_err(|e| e.to_string())?;
    let legacy_https = data.join("control/https/current.json");
    let legacy_distribution: Option<DistributionState> = if legacy_https.is_file() {
        Some(read(&legacy_https)?)
    } else {
        None
    };
    let receipt_path = root.join("handoff.json");
    let mut receipt = if receipt_path.exists() {
        let mut receipt: Receipt = read(&receipt_path)?;
        if receipt.version != 1 {
            return Err("unsupported or already rolled-back handoff".into());
        }
        if receipt.initialized
            && (!root.join("publisher.json").is_file() || !https.join("current.json").is_file())
        {
            return Err(
                "initialized handoff state is missing; refusing lower recovery floors".into(),
            );
        }
        if receipt.transaction != transaction {
            if rollback
                || !receipt.completed
                || receipt
                    .rollback_epoch
                    .is_none_or(|epoch| old_recovery.epoch < epoch)
            {
                return Err("handoff belongs to another unfinished transaction".into());
            }
            let current: DistributionState = read(&https.join("current.json"))?;
            if current.current.eligible {
                return Err("previous HTTPS runtime has not withdrawn".into());
            }
            // A new reviewed deployment after completed restoration can reuse
            // the state. Its generation, recovery and revision floors remain.
            receipt.transaction = transaction.into();
            receipt.completed = false;
            receipt.previous_sha256 = previous_sha256.clone();
            receipt.rollback_epoch = None;
            save(&root, "handoff.json", &receipt)?;
        } else if !rollback && receipt.rollback_epoch.is_some() {
            return Err("a rolled-back transaction cannot resume forward progress".into());
        }
        receipt
    } else {
        if rollback || root.join("publisher.json").exists() || https.join("current.json").exists() {
            return Err("state exists without a handoff receipt; refusing to overwrite it".into());
        }
        let receipt = Receipt {
            version: 1,
            transaction: transaction.into(),
            completed: false,
            initialized: false,
            previous_sha256: previous_sha256.clone(),
            rollback_epoch: None,
        };
        save(&root, "handoff.json", &receipt)?;
        receipt
    };
    if !rollback && receipt.previous_sha256 != previous_sha256 {
        return Err("previous state changed since handoff began".into());
    }
    receipt.completed = false;
    save(&root, "handoff.json", &receipt)?;
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
    if let Some(legacy) = legacy_distribution {
        legacy.current.validate()?;
        state.recovery.epoch = state.recovery.epoch.max(legacy.current.recovery_epoch);
        state.recovery.revoked.extend(legacy.current.revoked);
        distribution.current.revision = distribution.current.revision.max(legacy.current.revision);
        manifests.extend(legacy.retained.into_iter().map(|r| r.manifest));
        if let Some(manifest) = legacy.current.manifest {
            manifests.push(manifest);
        }
    }
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
    state.lifecycle.next_id = state.lifecycle.next_id.max(old_next_id);
    state.lifecycle.identities = (0..state.lifecycle.next_id).collect();
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
    receipt.completed = true;
    receipt.initialized = true;
    save(&root, "handoff.json", &receipt)?;
    Ok(
        json!({"transaction":transaction,"rollback":rollback, "recovery_epoch":state.recovery.epoch,
        "next_generation":state.next_generation, "withdrawal_revision":distribution.current.withdrawal_revision}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transfer(data: &Path, rollback: bool) -> Result<Value, String> {
        super::transfer(data, rollback, "publisher-fixture")
    }

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
    fn failed_forward_validation_can_restore_the_untouched_predecessor() {
        let root = tempfile::tempdir().unwrap();
        previous(root.path());
        let old = fs::read(root.path().join("control/controller.json")).unwrap();
        let result = transfer(root.path(), true).unwrap();
        assert_eq!(result["unchanged"], true);
        assert_eq!(result["recovery_epoch"], 4);
        assert_eq!(
            fs::read(root.path().join("control/controller.json")).unwrap(),
            old
        );
        assert!(!root.path().join("publication/publisher.json").exists());
    }

    #[test]
    fn imported_state_opens_in_the_actual_publisher_without_resetting_floors() {
        let root = tempfile::tempdir().unwrap();
        previous(root.path());
        transfer(root.path(), false).unwrap();
        let cookie = root.path().join("cookie");
        fs::write(&cookie, "fixture:fixture").unwrap();
        let rpc =
            crate::zakura::ZakuraClient::from_cookie_file("http://127.0.0.1:1", cookie).unwrap();
        let publisher =
            crate::publisher::Publisher::open(&root.path().join("publication"), rpc).unwrap();
        let current = publisher.distribution().durable_current();
        assert_eq!(current.recovery_epoch, 4);
        assert!(current.revoked.contains(&"ab".repeat(32)));
        assert!(!current.eligible);
        let state: PublisherState = read(&root.path().join("publication/publisher.json")).unwrap();
        assert_eq!(state.next_generation, 23);
        assert_eq!(state.recovery.domain_epochs[&0], "4");
        assert_eq!(state.recovery.sealed[&0].0, 3_428_143);
        assert!(transfer(root.path(), true).unwrap_err().contains("owned"));
    }

    #[test]
    fn historical_operation_receipts_do_not_use_the_https_metadata_limit() {
        let root = tempfile::tempdir().unwrap();
        previous(root.path());
        let mut old = previous_state(&root.path().join("control/controller.json")).unwrap();
        old["retained_operation_history"] = json!("x".repeat(2 * 1024 * 1024));
        save(&root.path().join("control"), "controller.json", &old).unwrap();
        transfer(root.path(), false).unwrap();
        transfer(root.path(), true).unwrap();
        let restored = previous_state(&root.path().join("control/controller.json")).unwrap();
        assert_eq!(
            restored["retained_operation_history"],
            old["retained_operation_history"]
        );
        assert_eq!(restored["recovery"]["epoch"], "5");
    }

    #[test]
    fn a_new_reviewed_attempt_after_rollback_keeps_all_prior_floors() {
        let root = tempfile::tempdir().unwrap();
        previous(root.path());
        transfer(root.path(), false).unwrap();
        assert!(super::transfer(root.path(), false, "publisher-other").is_err());
        transfer(root.path(), true).unwrap();
        assert!(transfer(root.path(), false).is_err());
        let report = super::transfer(root.path(), false, "publisher-other").unwrap();
        assert_eq!(report["recovery_epoch"], 5);
        assert_eq!(report["next_generation"], 23);
        assert!(transfer(root.path(), true).is_err());
        let report = super::transfer(root.path(), true, "publisher-other").unwrap();
        assert_eq!(report["recovery_epoch"], 6);
    }

    #[test]
    fn reserved_receipts_cannot_start_a_runtime_or_replace_lost_initialized_state() {
        let root = tempfile::tempdir().unwrap();
        previous(root.path());
        assert!(verify_startup(root.path()).is_err());
        let old = previous_state(&root.path().join("control/controller.json")).unwrap();
        let publication = root.path().join("publication");
        fs::create_dir_all(&publication).unwrap();
        let receipt = Receipt {
            version: 1,
            transaction: "publisher-fixture".into(),
            completed: false,
            initialized: false,
            rollback_epoch: None,
            previous_sha256: hex::encode(Sha256::digest(serde_json::to_vec(&old).unwrap())),
        };
        save(&publication, "handoff.json", &receipt).unwrap();
        assert!(verify_startup(root.path()).is_err());
        transfer(root.path(), false).unwrap();
        verify_startup(root.path()).unwrap();
        fs::remove_file(publication.join("publisher.json")).unwrap();
        assert!(verify_startup(root.path()).is_err());
        assert!(transfer(root.path(), true).unwrap_err().contains("missing"));
    }

    #[test]
    fn existing_https_floors_are_merged_without_importing_serving_authority() {
        let root = tempfile::tempdir().unwrap();
        previous(root.path());
        fs::create_dir_all(root.path().join("control/https")).unwrap();
        save(
            &root.path().join("control/https"),
            "current.json",
            &DistributionState {
                current: Current {
                    version: VERSION,
                    revision: 900,
                    withdrawal_revision: 899,
                    recovery_epoch: 7,
                    revoked: ["cd".repeat(32)].into(),
                    eligible: false,
                    manifest: None,
                    inventory: None,
                },
                retained: Vec::new(),
            },
        )
        .unwrap();
        let result = transfer(root.path(), false).unwrap();
        assert_eq!(result["recovery_epoch"], 7);
        assert_eq!(result["withdrawal_revision"], 901);
        let current: DistributionState =
            read(&root.path().join("publication/https/current.json")).unwrap();
        assert_eq!(current.current.revoked.len(), 2);
        assert!(!current.current.eligible);
        assert_eq!(transfer(root.path(), true).unwrap()["recovery_epoch"], 8);
    }

    #[test]
    fn committed_retention_records_are_preserved_but_uncommitted_records_refuse() {
        let root = tempfile::tempdir().unwrap();
        previous(root.path());
        let mut old = previous_state(&root.path().join("control/controller.json")).unwrap();
        old["completed"] = json!({"retired":"ef".repeat(32)});
        old["draining"] = json!([{"id":"retired", "phase":"DRAINING", "candidate_digest":"ef".repeat(32),
            "attempt":1,"epoch":91,"expected_revision":16,"assignments":{},"affected_groups":[],
            "source_generations":[21],"require_both":[],"readiness":[],"infrastructure_resources":{}}]);
        save(&root.path().join("control"), "controller.json", &old).unwrap();
        transfer(root.path(), false).unwrap();
        verify_startup(root.path()).unwrap();
        transfer(root.path(), true).unwrap();
        assert!(verify_startup(root.path()).is_err());
        let restored = previous_state(&root.path().join("control/controller.json")).unwrap();
        assert_eq!(restored["draining"], old["draining"]);
        let other = tempfile::tempdir().unwrap();
        previous(other.path());
        old["draining"][0]["phase"] = json!("READY");
        save(&other.path().join("control"), "controller.json", &old).unwrap();
        assert!(transfer(other.path(), false).is_err());
        assert_eq!(transfer(other.path(), true).unwrap()["unchanged"], true);
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
