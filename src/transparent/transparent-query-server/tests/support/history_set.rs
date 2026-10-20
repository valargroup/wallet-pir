//! Fixed chain identities, scripts and the four-worker roster shared by the
//! assignment and provider tests.
use zakura_pir_protocol::transparent::filter as transparent_filter;

use transparent_filter::{BlockHash, ScriptBytes, ShardMap};
use transparent_query_server::routing::router::RosterEntry;
use transparent_shared::history::assignment::{GeneratedBy, WorkerRole};

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

pub(crate) fn roster() -> Vec<RosterEntry> {
    let mut roster = Vec::new();
    for i in 0..2 {
        roster.push(RosterEntry {
            id: format!("archive-0{}", i + 1),
            role: WorkerRole::ArchiveOwner,
            replica_group: None,
            ssh_host: format!("10.0.1.{}", i + 1),
            upstream: format!("10.0.1.{}:8093", i + 1),
            cache_bytes: 4 << 30,
            archive_range: None,
        });
    }
    for i in 0..2 {
        roster.push(RosterEntry {
            id: format!("recent-0{}", i + 1),
            role: WorkerRole::RecentReplica,
            replica_group: Some("recent".into()),
            ssh_host: format!("10.0.2.{}", i + 1),
            upstream: format!("10.0.2.{}:8093", i + 1),
            cache_bytes: 4 << 30,
            archive_range: None,
        });
    }
    roster
}

pub(crate) fn generated() -> GeneratedBy {
    GeneratedBy {
        tool: "test".into(),
        source_sha: None,
        generated_at: "2026-09-08T00:00:00Z".into(),
    }
}
