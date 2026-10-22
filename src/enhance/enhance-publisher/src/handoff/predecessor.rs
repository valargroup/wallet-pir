//! Bounded decoding of the stopped predecessor's historical controller format.
use super::recovery::{decimal, validate_recovery_floors};
use crate::durable_state::RecoveryState;
use enhance_query_server::protocol::{self, Lifecycle, Manifest, MAX_QUERY_SHARDS};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{fs::File, io::Read, path::Path};

// The predecessor retains completed operation receipts. Its offline file can
// exceed the current HTTPS metadata bound; that network bound stays unchanged.
const PREVIOUS_STATE_LIMIT: u64 = 16 * 1024 * 1024;

pub(super) fn previous_state(path: &Path) -> Result<Value, String> {
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

/// Validated identities from a stopped source, without any serving authority.
pub(super) struct PredecessorState {
    pub(super) recovery: RecoveryState,
    pub(super) next_generation: u64,
    pub(super) lifecycle: Lifecycle,
    pub(super) manifests: Vec<Manifest>,
    pub(super) sha256: String,
}

pub(super) fn validate_predecessor(previous: &Value) -> Result<PredecessorState, String> {
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
    let old_recovery: RecoveryState =
        serde_json::from_value(recovery).map_err(|e| e.to_string())?;
    validate_recovery_floors(&old_recovery)?;
    let old_next = previous["next_generation"]
        .as_u64()
        .filter(|n| *n > 0)
        .ok_or("missing previous generation reservation")?;
    let old_lifecycle: Lifecycle =
        serde_json::from_value(previous["lifecycle"].clone()).map_err(|e| e.to_string())?;
    if old_lifecycle.next_id > MAX_QUERY_SHARDS {
        return Err("previous lifecycle exceeds the protocol ceiling".into());
    }
    let manifests: Vec<Manifest> =
        serde_json::from_value(previous["published"].clone()).map_err(|e| e.to_string())?;
    Ok(PredecessorState {
        recovery: old_recovery,
        next_generation: old_next,
        lifecycle: old_lifecycle,
        manifests,
        sha256: previous_sha256,
    })
}
