//! A two-geometry history shard set: a sealed shard, then an unsealed tail.
//! Shared by the operator payload fixtures and the host verify-only test.
use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_protocol::transparent::filter as transparent_filter;
use zakura_pir_publisher::transparent as transparent_shard;

use std::collections::BTreeMap;
use std::path::Path;
use transparent_events::{ReceiveEvent, TransparentEvent, Txid};
use transparent_filter::{BlockHash, ScriptBytes, SealParameters, ShardMap, ShardMapEntry};
use transparent_shard::layout::{Geometry, DECOUPLED_2K_8K};

#[path = "revision.rs"]
mod revision;

const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;
const FIRST: u64 = 3_428_143;
const SPAN: u64 = 40;

/// The two cheapest registry geometries. A wider pair would say nothing more
/// about the payload shape and would cost megabytes per table to hash.
const GEOMETRIES: [&Geometry; 2] = [&DECOUPLED_2K_8K, &DECOUPLED_2K_8K];

fn genesis() -> BlockHash {
    BlockHash::from_display_hex(GENESIS).unwrap()
}

fn hash_at(height: u64) -> BlockHash {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&height.to_le_bytes());
    BlockHash::from_internal_bytes(bytes)
}

fn script(tag: u32) -> ScriptBytes {
    let mut bytes = vec![0x76, 0xa9, 0x14];
    bytes.extend_from_slice(&tag.to_le_bytes());
    bytes.extend_from_slice(&[0u8; 16]);
    bytes.extend_from_slice(&[0x88, 0xac]);
    ScriptBytes::new(bytes)
}

fn events(start: u64, end: u64) -> Vec<(ScriptBytes, TransparentEvent)> {
    (start..=end)
        .map(|height| {
            let mut txid = [0u8; 32];
            txid[..8].copy_from_slice(&height.to_le_bytes());
            (
                script((height % 8) as u32),
                TransparentEvent::Receive(ReceiveEvent {
                    metadata: None,
                    height: height as u32,
                    txid: Txid(txid),
                    transaction_index: 0,
                    output_index: 0,
                    value: 1_000 + height,
                    coinbase: false,
                }),
            )
        })
        .collect()
}

/// Publishes one shard and returns its map entry.
fn write_shard(
    dir: &Path,
    shard_id: u64,
    geometry: &'static Geometry,
    sealed: bool,
    parent_manifest_digest: &str,
) -> ShardMapEntry {
    let start = FIRST + shard_id * SPAN;
    let end = start + SPAN - 1;
    let content = events(start, end);
    revision::write(
        dir,
        &revision::Revision {
            shard_id,
            start,
            end,
            geometry,
            sealed,
            revision: 0,
            supersedes: "",
            parent_digest: parent_manifest_digest,
            children: 1,
            genesis: genesis(),
            hash: &hash_at,
        },
        &content,
    )
    .1
}

/// Publishes a two-geometry set: an earlier sealed shard, then an unsealed tail.
///
/// `check_shape` refuses a sealed shard after an unsealed one, so the tail is
/// last by construction — which is also what production looks like.
pub fn publish(dir: &Path) -> ShardMap {
    let mut entries = Vec::new();
    let mut parent = String::new();
    for (index, geometry) in GEOMETRIES.iter().enumerate() {
        let shard_id = index as u64;
        let sealed = index + 1 < GEOMETRIES.len();
        let entry = write_shard(dir, shard_id, geometry, sealed, &parent);
        parent = entry.manifest_digest.clone();
        entries.push(entry);
    }

    let seal = entries
        .iter()
        .map(|entry| {
            (
                entry.geometry.clone(),
                SealParameters {
                    max_scripts: 8_192,
                    max_page_rows: 2_048,
                    max_txids: 0,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    let map = ShardMap {
        genesis_hash: GENESIS.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: FIRST,
        seal,
        shards: entries,
        recuts: Vec::new(),
    };
    map.check_shape()
        .expect("the published map must be well formed");
    std::fs::write(
        dir.join("shards.json"),
        serde_json::to_vec_pretty(&map).unwrap(),
    )
    .unwrap();
    map
}
