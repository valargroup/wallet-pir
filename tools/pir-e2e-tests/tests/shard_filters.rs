//! Loading the public half of a published shard set.
//!
//! The failure this guards against is not a crash. A range filter that does not
//! match the digest its map publishes still parses, still answers queries, and
//! answers them *wrongly* — a wallet testing its scripts against it would
//! under-match and skip history it should have retrieved, then advance its
//! coverage over the gap. That is a silent wrong answer, so loading refuses
//! the set rather than accept it.

use zakura_pir_publisher::transparent::filter as transparent_filter;

use std::path::Path;
use transparent_filter::digest::filter_hash;
use transparent_filter::wire::{SealParameters, ShardMap, ShardMapEntry};
use transparent_publisher::history::shard_filters::ShardFilters;

const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;

fn entry(shard_id: u64, digest: &str, filter: &[u8]) -> ShardMapEntry {
    ShardMapEntry {
        shard_id,
        geometry: "decoupled-2k-8k".to_string(),
        start_height: 100 + shard_id * 10,
        end_height: 109 + shard_id * 10,
        // The map requires a chain: each shard's parent is the block before its
        // range, which is its predecessor's terminal.
        parent_block_hash: format!("{:064x}", shard_id),
        terminal_block_hash: format!("{:064x}", shard_id + 1),
        filter_hash: filter_hash(filter).to_display_hex(),
        scripts: 1,
        page_rows: 1,
        txids: 0,
        directory_segments: 1,
        page_segments: 1,
        manifest_digest: digest.to_string(),
        revision: 0,
        sealed: true,
    }
}

/// Writes a two-shard set. `corrupt` flips a byte of shard 1's filter *after*
/// its digest is recorded, which is exactly the on-disk damage the load is
/// meant to catch.
fn publish(dir: &Path, corrupt: bool) {
    let mut entries = Vec::new();
    for shard_id in 0..2u64 {
        let digest = format!("{:064x}", shard_id + 1);
        let filter = vec![shard_id as u8 + 7; 64];
        let shard_dir = dir.join(&digest);
        std::fs::create_dir_all(&shard_dir).unwrap();
        entries.push(entry(shard_id, &digest, &filter));
        let mut written = filter;
        if corrupt && shard_id == 1 {
            written[0] ^= 0xff;
        }
        std::fs::write(shard_dir.join("filter.bin"), &written).unwrap();
    }
    let map = ShardMap {
        genesis_hash: GENESIS.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: 100,
        seal: std::collections::BTreeMap::from([(
            "decoupled-2k-8k".to_string(),
            SealParameters {
                max_scripts: 8,
                max_page_rows: 8,
                max_txids: 0,
            },
        )]),
        shards: entries,
        recuts: Vec::new(),
    };
    std::fs::write(
        dir.join("shards.json"),
        serde_json::to_vec_pretty(&map).unwrap(),
    )
    .unwrap();
}

#[test]
fn unsupported_map_metadata_is_rejected_before_reading_filters() {
    for unknown_profile in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        publish(dir.path(), false);
        let path = dir.path().join("shards.json");
        let mut map: ShardMap = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        if unknown_profile {
            map.profile = "unregistered-range-profile".into();
        } else {
            map.range_envelope_version = transparent_filter::RANGE_ENVELOPE_VERSION + 1;
        }
        map.check_shape().unwrap(); // The structure alone cannot qualify the protocol.
        for entry in &map.shards {
            std::fs::remove_dir_all(dir.path().join(&entry.manifest_digest)).unwrap();
        }
        std::fs::write(path, serde_json::to_vec(&map).unwrap()).unwrap();
        assert!(matches!(
            ShardFilters::open(dir.path()),
            Err(transparent_publisher::history::shard_filters::ShardFilterError::Invalid(_))
        ));
    }
}

#[test]
fn a_filter_that_does_not_match_its_published_digest_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    publish(dir.path(), true);
    let text = match ShardFilters::open(dir.path()) {
        Ok(_) => panic!("a corrupt filter must not load"),
        Err(error) => error.to_string(),
    };
    assert!(text.contains("shard 1"), "{text}");
    assert!(text.contains("digest"), "{text}");
}

#[test]
fn a_published_set_loads_and_keeps_every_filter() {
    let dir = tempfile::tempdir().unwrap();
    publish(dir.path(), false);
    let filters = ShardFilters::open(dir.path()).expect("clean set loads");
    assert_eq!(filters.shard_count(), 2);
    assert_eq!(filters.covered_through(), Some(119));
    assert_eq!(filters.filter(0).unwrap(), vec![7u8; 64]);
    assert_eq!(filters.filter(1).unwrap(), vec![8u8; 64]);
    assert!(filters.filter(2).is_none(), "the set has no shard 2");
    // Stable across calls, so a wallet can pin it.
    assert_eq!(filters.map_digest(), filters.map_digest());
    assert_eq!(filters.map_digest().len(), 64);
}

/// A missing filter file is a partial set, and a partial set must not be
/// served: a wallet would take the shards it could get and advance coverage
/// over the range whose filter never arrived.
#[test]
fn a_set_missing_a_filter_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    publish(dir.path(), false);
    std::fs::remove_file(dir.path().join(format!("{:064x}", 2u64)).join("filter.bin")).unwrap();
    assert!(ShardFilters::open(dir.path()).is_err());
}
