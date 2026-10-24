//! The shards this provider holds tables for.
//!
//! A provider holds the whole recent tier and nothing of the archive, whose
//! queries go to the configured archive upstream. Fleet planning, where
//! several workers split the archive, is deployment tooling and never runs in
//! a query server. The archive still appears as one inert row, so the
//! assignment accounts for every shard; nothing routes by that row.

use zakura_pir_protocol::transparent::filter::ShardMap;

use transparent_shared::history::assignment::{
    shard_reserved_bytes, Assignment, AssignmentError, GeneratedBy, SetIdentity, WorkerAssignment,
    WorkerRole, ASSIGNMENT_SCHEMA,
};

/// Worker id of the provider's own row.
pub const PROVIDER: &str = "provider";
/// Worker id of the inert archive row.
pub const ARCHIVE_METADATA_ONLY: &str = "archive-metadata-only";

/// Selects the provider's holdings: every shard from `recent_from_shard` on.
///
/// `headroom` is the fraction of `cache_bytes` that must stay free of the
/// recent tier's runtimes, for retained revisions and the transient cost of a
/// build. A recent tier that does not fit is a refusal, not a provider that
/// thrashes. Deterministic for one map, identity and configuration.
pub fn local(
    map: &ShardMap,
    map_sha256: &str,
    recent_from_shard: u64,
    cache_bytes: u64,
    headroom: f64,
    generated_by: GeneratedBy,
) -> Result<Assignment, AssignmentError> {
    map.check_supported_format()
        .map_err(AssignmentError::Invalid)?;
    if !(0.0..1.0).contains(&headroom) {
        return Err(AssignmentError::Invalid(
            "headroom must be in [0, 1)".into(),
        ));
    }
    let shards = map.shards.len() as u64;
    if recent_from_shard > shards {
        return Err(AssignmentError::Invalid(format!(
            "recent_from_shard {recent_from_shard} is beyond the set's {shards} shards"
        )));
    }
    let costs: Vec<u64> = map
        .shards
        .iter()
        .map(shard_reserved_bytes)
        .collect::<Result<_, _>>()?;
    let recent: Vec<u64> = (recent_from_shard..shards).collect();
    let recent_bytes: u64 = recent.iter().map(|id| costs[*id as usize]).sum();
    let budget = (cache_bytes as f64 * (1.0 - headroom)) as u64;
    if recent_bytes > budget {
        return Err(AssignmentError::Invalid(format!(
            "recent replica {PROVIDER} would hold {} bytes of runtimes ({} shards) against a \
             budget of {} after {:.0}% headroom; the recent tier has outgrown its replicas",
            recent_bytes,
            recent.len(),
            budget,
            headroom * 100.0
        )));
    }
    let mut workers = Vec::new();
    if recent_from_shard > 0 {
        let archive: Vec<u64> = (0..recent_from_shard).collect();
        workers.push(WorkerAssignment {
            id: ARCHIVE_METADATA_ONLY.into(),
            role: WorkerRole::ArchiveOwner,
            replica_group: None,
            upstream: "unconfigured.invalid:0".into(),
            cache_bytes: u64::MAX,
            estimated_resident_bytes: archive.iter().map(|id| costs[*id as usize]).sum(),
            shards: archive,
        });
    }
    workers.push(WorkerAssignment {
        id: PROVIDER.into(),
        role: WorkerRole::RecentReplica,
        replica_group: Some("recent".into()),
        upstream: "127.0.0.1:0".into(),
        cache_bytes,
        shards: recent,
        estimated_resident_bytes: recent_bytes,
    });
    let assignment = Assignment {
        schema: ASSIGNMENT_SCHEMA.to_string(),
        set: SetIdentity::of(map, map_sha256, recent_from_shard),
        generated_by,
        workers,
        unassigned: Vec::new(),
    };
    assignment.check_shape()?;
    Ok(assignment)
}
