//! Offline recovery-state handoff for the direct HTTPS cutover and rollback.
//!
//! The deployment transaction stops the source first. These additional file
//! locks refuse a live source even when invoked outside that transaction. No
//! runtime loads the previous controller or treats copied bytes as authority.
mod persistence;
mod predecessor;
mod recovery;

use crate::durable_state::{DistributionState, PublisherState, RecoveryState};
use enhance_material::{
    protocol::MAX_QUERY_SHARDS,
    publication::{Current, VERSION},
};
use persistence::{lock, save, HandoffReceipt};
use predecessor::previous_state;
use recovery::{decimal, merge_recovery_floors, validate_recovery_floors};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::Path;

fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    crate::publication::read_metadata(path)
}
/// Refuse startup until every handoff write is durable. A reserved receipt is
/// not permission to initialize empty publication or recovery state.
pub fn verify_startup(data: &Path) -> Result<(), String> {
    if !data.join("control/controller.json").exists() {
        return Ok(());
    }
    let root = data.join("publication");
    let receipt: HandoffReceipt = read(&root.join("handoff.json"))
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
    let predecessor::PredecessorState {
        recovery: old_recovery,
        next_generation: old_next,
        lifecycle: old_lifecycle,
        mut manifests,
        sha256: previous_sha256,
    } = predecessor::validate_predecessor(&previous)?;
    let old_next_id = old_lifecycle.next_id;
    let legacy_https = data.join("control/https/current.json");
    let legacy_distribution: Option<DistributionState> = if legacy_https.is_file() {
        Some(read(&legacy_https)?)
    } else {
        None
    };
    let mut receipt = persistence::reserve_transfer(
        &root,
        &https,
        rollback,
        transaction,
        old_recovery.epoch,
        &previous_sha256,
    )?;
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
    validate_recovery_floors(&state.recovery)?;
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
    merge_recovery_floors(&mut state.recovery, &old_recovery);
    if !rollback {
        // The stopped predecessor owns the repaired journal. Unlike monotonic
        // recovery floors, a sealed crossing can be removed or replaced by a
        // reorg while that runtime runs between two cutover attempts.
        state.recovery.sealed = old_recovery.sealed.clone();
    }
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
        let floor = RecoveryState {
            epoch: manifest.recovery_epoch,
            domain_epochs: manifest.domain_recovery_epochs.clone(),
            ..Default::default()
        };
        validate_recovery_floors(&floor)?;
        merge_recovery_floors(&mut state.recovery, &floor);
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
    validate_recovery_floors(&state.recovery)?;
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
mod tests;
