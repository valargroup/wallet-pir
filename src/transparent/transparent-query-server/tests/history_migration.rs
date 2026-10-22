//! Offline History artifact migration from a publication that holds only
//! predecessor-format runtime files, as a real predecessor source pin does.
#![cfg(feature = "transparent-history")]

use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_protocol::transparent::filter as transparent_filter;
use zakura_pir_publisher::transparent as transparent_shard;

#[path = "support/history_set.rs"]
mod history_set;
#[path = "support/revision.rs"]
mod revision;

use history_set::*;
use pir_artifact::{catalog::Catalog, migration::sha256};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use transparent_events::{ReceiveEvent, TransparentEvent, Txid};
use transparent_filter::{SealParameters, ShardMap};
use transparent_shard::layout::DECOUPLED_2K_8K;
use transparent_shared::history::shardset::ShardSet;
use transparent_shared::publication::{self, migration, Finalized};
use transparent_shared::runtime::{disk::ShippedRuntimes, SharedParams};

fn publish(dir: &Path) -> ShardMap {
    let events = [(
        script(1),
        TransparentEvent::Receive(ReceiveEvent {
            metadata: None,
            height: FIRST as u32,
            txid: Txid([1; 32]),
            transaction_index: 0,
            output_index: 0,
            value: 123_456,
            coinbase: false,
        }),
    )];
    let (_, entry) = revision::write(
        dir,
        &revision::Revision {
            shard_id: 0,
            start: FIRST,
            end: FIRST + 1,
            geometry: &DECOUPLED_2K_8K,
            sealed: false,
            revision: 0,
            supersedes: "",
            parent_digest: "",
            children: 1,
            genesis: genesis(),
            hash: &hash_at,
        },
        &events,
    );
    let map = ShardMap {
        genesis_hash: GENESIS.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: FIRST,
        seal: BTreeMap::from([(
            entry.geometry.clone(),
            SealParameters {
                max_scripts: 8_192,
                max_page_rows: 2_048,
                max_txids: 0,
            },
        )]),
        shards: vec![entry],
        recuts: Vec::new(),
    };
    map.check_shape().unwrap();
    std::fs::write(dir.join("shards.json"), serde_json::to_vec(&map).unwrap()).unwrap();
    map
}

/// Rewrite every current runtime under its predecessor name and header, as
/// the previous release shipped it, and remove the current-format file.
fn predecessor_runtimes(root: &Path, set: &ShardSet) -> BTreeMap<PathBuf, PathBuf> {
    let runtimes = ShippedRuntimes::required(root.to_path_buf());
    let mut renamed = BTreeMap::new();
    for entry in &set.map.shards {
        let shard = set.revision(&entry.manifest_digest).unwrap();
        for table in shard.tables() {
            let shared = SharedParams::build(shard.geometry, table.kind()).unwrap();
            for segment in 0..shard.segments(table) {
                let source = shard.segment(table, segment).unwrap();
                let key = (
                    shard.runtime_identity(table).to_string(),
                    table.kind(),
                    segment,
                );
                let path = runtimes.path(&key, &shared, source.sha256());
                let previous = runtimes.previous_path(&key, &shared, source.sha256());
                let identity = previous
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .split_once('-')
                    .unwrap()
                    .1
                    .to_owned();
                let mut bytes = std::fs::read(&path).unwrap();
                bytes[..32].copy_from_slice(&hex::decode(identity).unwrap());
                let body = Sha256::digest(&bytes[64..]);
                bytes[32..64].copy_from_slice(&body);
                std::fs::write(&previous, bytes).unwrap();
                std::fs::remove_file(&path).unwrap();
                renamed.insert(
                    path.strip_prefix(root).unwrap().to_owned(),
                    previous.strip_prefix(root).unwrap().to_owned(),
                );
            }
        }
    }
    renamed
}

#[test]
fn history_migration_reads_only_predecessor_runtimes_and_preserves_sources() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("publication");
    std::fs::create_dir(&root).unwrap();
    publish(&root);
    let set = ShardSet::open(&root, 0).unwrap();
    let prepared =
        transparent_shared::runtime::prebuild::prepare_publication(&set, &root, &[], &|| false)
            .unwrap();
    assert!(prepared.failures.is_empty());
    let current = Finalized::prepare(&root, &set, FIRST).unwrap();
    let renamed = predecessor_runtimes(&root, &set);
    assert!(!renamed.is_empty());

    let mut old = serde_json::to_value(current.inventory()).unwrap();
    old["schema"] = "transparent-prepared-distribution-v1".into();
    old["runtime"] = publication::RUNTIME
        .replacen("runtime-v4/", "runtime-v3/", 1)
        .into();
    let mut inputs = Vec::new();
    let mut sources = 0;
    for artifact in old["artifacts"].as_array_mut().unwrap() {
        let mut relative = PathBuf::from(artifact["path"].as_str().unwrap());
        if let Some(previous) = renamed.get(&relative) {
            relative = previous.clone();
            artifact["path"] = previous.to_str().unwrap().into();
        }
        let bytes = std::fs::read(root.join(&relative)).unwrap();
        let expected = sha256(hex::encode(Sha256::digest(&bytes))).unwrap();
        let object = artifact.as_object_mut().unwrap();
        object.remove("checksum");
        sources += usize::from(object.remove("source_sha256").is_some());
        artifact["sha256"] = expected.digest.clone().into();
        inputs.push((relative, expected, bytes.len() as u64));
    }
    assert!(sources > 0, "fixture must exercise source commitments");
    let old_bytes = serde_json::to_vec(&old).unwrap();
    let previous = sha256(hex::encode(Sha256::digest(&old_bytes))).unwrap();
    let stage = directory.path().join("stage");
    let prepare = |old: serde_json::Value| {
        let mut catalog = Catalog::new(&root, &stage).unwrap();
        catalog.pin_inputs(&inputs).unwrap();
        let bytes = migration::prepare(&mut catalog, old, false)?;
        assert_eq!(bytes, current.bytes());
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
            catalog
                .finish("transparent-history", previous.clone(), &bytes)
                .unwrap(),
        )
    };

    // A predecessor checksum that disagrees with its pinned bytes is refused.
    let mut tampered = old.clone();
    let table = tampered["artifacts"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|a| a["path"].as_str().unwrap().ends_with(".bin"))
        .unwrap();
    table["sha256"] = "00".repeat(32).into();
    assert!(prepare(tampered).is_err());
    std::fs::remove_dir_all(&stage).ok();

    let prepared = prepare(old.clone()).unwrap();
    prepared.verify_files().unwrap();
    // Resume from the retained stage after the source is gone.
    std::fs::remove_dir_all(&root).unwrap();
    let mut prepared = prepare(old.clone()).unwrap();
    prepared.verify_predecessors().unwrap();
    prepared.install(&root).unwrap();
    let set = ShardSet::open(&root, 0).unwrap();
    assert_eq!(
        Finalized::prepare(&root, &set, FIRST).unwrap().bytes(),
        current.bytes()
    );
    prepared.restore_predecessors().unwrap();
    for (relative, expected, _) in inputs {
        expected
            .verify(&std::fs::read(root.join(relative)).unwrap())
            .unwrap();
    }
}
