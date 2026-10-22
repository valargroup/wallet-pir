//! Merge monotonic recovery floors without carrying serving eligibility forward.
use crate::durable_state::RecoveryState;
use enhance_material::protocol::{self, MAX_QUERY_SHARDS};

pub(super) fn validate_recovery_floors(recovery: &RecoveryState) -> Result<(), String> {
    for (id, epoch) in &recovery.domain_epochs {
        let number = decimal(epoch)?;
        if *id >= MAX_QUERY_SHARDS || number > recovery.epoch {
            return Err("invalid domain recovery floor".into());
        }
    }
    if recovery
        .revoked
        .iter()
        .any(|id| !protocol::canonical_hash(id))
        || recovery
            .sealed
            .iter()
            .any(|(id, (_, hash))| *id >= MAX_QUERY_SHARDS || !protocol::canonical_hash(hash))
    {
        return Err("invalid recovery identities".into());
    }
    Ok(())
}
pub(super) fn merge_recovery_floors(recovery: &mut RecoveryState, other: &RecoveryState) {
    recovery.epoch = recovery.epoch.max(other.epoch);
    recovery.revoked.extend(other.revoked.iter().cloned());
    for (id, epoch) in &other.domain_epochs {
        let previous = recovery
            .domain_epochs
            .entry(*id)
            .or_insert_with(|| "0".into());
        if decimal(epoch).unwrap() > decimal(previous).unwrap() {
            *previous = epoch.clone();
        }
    }
}

pub(super) fn decimal(text: &str) -> Result<u64, String> {
    let value: u64 = text.parse().map_err(|_| "invalid recovery integer")?;
    if value.to_string() != text {
        return Err("noncanonical recovery integer".into());
    }
    Ok(value)
}
