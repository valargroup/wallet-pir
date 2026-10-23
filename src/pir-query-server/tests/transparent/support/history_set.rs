//! Fixed chain identities, scripts and the four-worker fleet shared by the
//! assignment and provider tests.
use zakura_pir_protocol::transparent::filter as transparent_filter;

use transparent_filter::{BlockHash, ScriptBytes, ShardMap};
use transparent_shared::history::assignment::{
    shard_reserved_bytes, Assignment, GeneratedBy, SetIdentity, WorkerAssignment, WorkerRole,
    ASSIGNMENT_SCHEMA,
};

pub(crate) const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;
pub(crate) const FIRST: u64 = 3_428_143;

pub(crate) fn genesis() -> BlockHash {
    BlockHash::from_display_hex(GENESIS).unwrap()
}

pub(crate) fn hash_at(height: u64) -> BlockHash {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&height.to_le_bytes());
    BlockHash::from_internal_bytes(bytes)
}

pub(crate) fn script(tag: u32) -> ScriptBytes {
    let mut bytes = vec![0x76, 0xa9, 0x14];
    bytes.extend_from_slice(&tag.to_le_bytes());
    bytes.extend_from_slice(&[0u8; 16]);
    bytes.extend_from_slice(&[0x88, 0xac]);
    ScriptBytes::new(bytes)
}

pub(crate) fn map_sha256(map: &ShardMap) -> String {
    hex::encode(<sha2::Sha256 as sha2::Digest>::digest(
        serde_json::to_vec(map).unwrap(),
    ))
}

/// A hand-written four-worker fleet: two archive owners splitting the
/// archive contiguously, the first taking the larger half, and two recent
/// replicas holding every recent shard. Fleet planning is deployment tooling
/// (`transparent_tools::assignment`); serving tests only need a valid fleet.
pub(crate) fn fleet(map: &ShardMap, recent_from_shard: u64) -> Assignment {
    let cost = |ids: &[u64]| -> u64 {
        ids.iter()
            .map(|id| shard_reserved_bytes(&map.shards[*id as usize]).unwrap())
            .sum()
    };
    let archive: Vec<u64> = (0..recent_from_shard).collect();
    let recent: Vec<u64> = (recent_from_shard..map.shards.len() as u64).collect();
    let (first, second) = archive.split_at(archive.len().div_ceil(2));
    let mut workers = Vec::new();
    for (i, shards) in [first, second].into_iter().enumerate() {
        workers.push(WorkerAssignment {
            id: format!("archive-0{}", i + 1),
            role: WorkerRole::ArchiveOwner,
            replica_group: None,
            upstream: format!("10.0.1.{}:8093", i + 1),
            cache_bytes: 4 << 30,
            estimated_resident_bytes: cost(shards),
            shards: shards.to_vec(),
        });
    }
    for i in 0..2 {
        workers.push(WorkerAssignment {
            id: format!("recent-0{}", i + 1),
            role: WorkerRole::RecentReplica,
            replica_group: Some("recent".into()),
            upstream: format!("10.0.2.{}:8093", i + 1),
            cache_bytes: 4 << 30,
            estimated_resident_bytes: cost(&recent),
            shards: recent.clone(),
        });
    }
    let assignment = Assignment {
        schema: ASSIGNMENT_SCHEMA.to_string(),
        set: SetIdentity::of(map, &map_sha256(map), recent_from_shard),
        generated_by: generated(),
        workers,
        unassigned: Vec::new(),
    };
    assignment.check_shape().unwrap();
    assignment
}

pub(crate) fn generated() -> GeneratedBy {
    GeneratedBy {
        tool: "test".into(),
        source_sha: None,
        generated_at: "2026-09-08T00:00:00Z".into(),
    }
}
