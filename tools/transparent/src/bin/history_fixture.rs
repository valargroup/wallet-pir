//! Operator-rehearsal fixture for an external history worker; never a production tool.
//!
//! `history_fixture DIR END` writes a one-shard publication into `DIR`: revision
//! zero of shard zero over a synthetic chain from the first indexed height through
//! `FIRST + END`, its `shards.json`, and nothing else. Exact answers are checked
//! by `soak-query`, which reads the expected rows from these files, not the server.
use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_protocol::transparent::filter as transparent_filter;
use zakura_pir_publisher::transparent as transparent_shard;

use std::path::Path;
use transparent_events::{ReceiveEvent, TransparentEvent, Txid};
use transparent_filter::{BlockHash, ScriptBytes, SealParameters, ShardMap};
#[path = "../../../../src/pir-query-server/tests/transparent/support/revision.rs"]
mod revision;
use transparent_shard::layout::DECOUPLED_2K_8K;

const FIRST: u64 = 3_428_143;

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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (dir, end) = (
        Path::new(&args[1]),
        FIRST + args[2].parse::<u64>().expect("END"),
    );
    let genesis = BlockHash::from_display_hex(transparent_filter::MAINNET_GENESIS_DISPLAY).unwrap();
    let events: Vec<_> = (FIRST..=end)
        .flat_map(|height| {
            (0..12u64).map(move |n| {
                let mut txid = [0u8; 32];
                txid[..8].copy_from_slice(&height.to_le_bytes());
                txid[8..16].copy_from_slice(&n.to_le_bytes());
                (
                    script((n % 4) as u32),
                    TransparentEvent::Receive(ReceiveEvent {
                        metadata: None,
                        height: height as u32,
                        txid: Txid(txid),
                        transaction_index: n as u16,
                        output_index: 0,
                        value: 1_000 + n,
                        coinbase: false,
                    }),
                )
            })
        })
        .collect();
    let (_, entry) = revision::write(
        dir,
        &revision::Revision {
            shard_id: 0,
            start: FIRST,
            end,
            geometry: &DECOUPLED_2K_8K,
            sealed: false,
            revision: 0,
            supersedes: "",
            parent_digest: "",
            children: 1,
            genesis,
            hash: &hash_at,
        },
        &events,
    );
    let map = ShardMap {
        genesis_hash: transparent_filter::MAINNET_GENESIS_DISPLAY.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: FIRST,
        seal: std::collections::BTreeMap::from([(
            DECOUPLED_2K_8K.name.to_string(),
            SealParameters {
                max_scripts: 8_192,
                max_page_rows: 2_048,
                max_txids: 0,
            },
        )]),
        shards: vec![entry],
        recuts: Vec::new(),
    };
    let bytes = serde_json::to_vec_pretty(&map).unwrap();
    std::fs::write(dir.join("shards.json"), &bytes).unwrap();
    let canonical = serde_json::to_vec(&map).unwrap();
    println!(
        "{}",
        serde_json::json!({"directory": dir, "map_sha256": hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&canonical))})
    );
}
