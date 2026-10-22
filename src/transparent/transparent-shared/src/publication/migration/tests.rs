use super::*;
use crate::tx_detail::{distribution, prebuild, set::DisplaySet, synth, DisplayRole};
use sha2::{Digest, Sha256};
use zakura_pir_protocol::transparent::shard::display::{DisplaySealParams, TXID_2K8};

#[test]
fn display_migration_preserves_source_commitments_and_resumes_after_collection() {
    let directory = tempfile::tempdir().unwrap();
    let shard = synth::write_shard(
        directory.path(),
        &synth::ShardSpec {
            shard_id: 0,
            start_height: 1,
            end_height: 1,
            sealed: false,
            revision: 0,
            supersedes: String::new(),
            parent_manifest_digest: String::new(),
            n_buckets: 1,
            archive_target: 1,
            geometry: &TXID_2K8,
        },
        &[synth::record(7, 0, 25, 0)],
    )
    .unwrap();
    let (root, _) = synth::write_candidate(
        directory.path(),
        &DisplaySealParams {
            n_archive: 1,
            n_recent: 1,
            archive_target: 1,
            recent_window: 1,
            reorg_margin: 1,
        },
        std::slice::from_ref(&shard),
        "migration",
    )
    .unwrap();
    prebuild::build_shipped(&shard.dir, &root).unwrap();
    let current = distribution::Finalized::prepare(&root).unwrap().inventory;
    let set = DisplaySet::open_for_serving(&root, 0, DisplayRole::Combined).unwrap();
    let runtimes = ShippedRuntimes::required(root.clone());
    let mut old_paths = BTreeMap::new();
    for revision in set.current() {
        for (table, segment) in revision.targets() {
            let source = revision.segment(table, segment).unwrap();
            let shared =
                SharedParams::build_display(revision.geometry, crate::tx_detail::kind(table))
                    .unwrap();
            let key = crate::tx_detail::runtime_key(&revision.digest, table, segment);
            let path = runtimes.path(&key, &shared, source.sha256());
            let previous = runtimes.previous_path(&key, &shared, source.sha256());
            let mut bytes = std::fs::read(&path).unwrap();
            let identity = previous
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .split_once('-')
                .unwrap()
                .1;
            bytes[..32].copy_from_slice(&hex::decode(identity).unwrap());
            let body = Sha256::digest(&bytes[64..]);
            bytes[32..64].copy_from_slice(&body);
            std::fs::write(&previous, bytes).unwrap();
            std::fs::remove_file(&path).unwrap();
            old_paths.insert(path.strip_prefix(&root).unwrap().to_owned(), previous);
        }
    }
    let mut old = serde_json::to_value(&current).unwrap();
    old["schema"] = "transparent-display-prepared-distribution-v1".into();
    old["runtime"] = RUNTIME.replacen("runtime-v4/", "runtime-v3/", 1).into();
    let mut inputs = Vec::new();
    for artifact in old["artifacts"].as_array_mut().unwrap() {
        let relative = PathBuf::from(artifact["path"].as_str().unwrap());
        if let Some(previous) = old_paths.get(&relative) {
            artifact["path"] = previous
                .strip_prefix(&root)
                .unwrap()
                .to_str()
                .unwrap()
                .into();
        }
        let relative = PathBuf::from(artifact["path"].as_str().unwrap());
        let bytes = std::fs::read(root.join(&relative)).unwrap();
        let expected = sha256(hex::encode(Sha256::digest(&bytes))).unwrap();
        artifact.as_object_mut().unwrap().remove("checksum");
        artifact.as_object_mut().unwrap().remove("source_sha256");
        artifact["sha256"] = expected.digest.clone().into();
        inputs.push((relative, expected, bytes.len() as u64));
    }
    let old_bytes = serde_json::to_vec(&old).unwrap();
    let previous = sha256(hex::encode(Sha256::digest(&old_bytes))).unwrap();
    let stage = directory.path().join("stage");
    let prepare = || {
        let mut catalog = Catalog::new(&root, &stage).unwrap();
        catalog.pin_inputs(&inputs).unwrap();
        let bytes = super::prepare(&mut catalog, old.clone(), true).unwrap();
        assert_eq!(bytes, serde_json::to_vec(&current).unwrap());
        catalog
            .finish("transparent-display", previous.clone(), &bytes)
            .unwrap()
    };
    let prepared = prepare();
    prepared.verify_files().unwrap();
    std::fs::remove_dir_all(&root).unwrap();
    let mut prepared = prepare();
    prepared.verify_predecessors().unwrap();
    prepared.install(&root).unwrap();
    assert_eq!(
        distribution::Finalized::prepare(&root)
            .unwrap()
            .inventory
            .map_sha256,
        current.map_sha256
    );
    prepared.restore_predecessors().unwrap();
    for (relative, expected, _) in inputs {
        expected
            .verify(&std::fs::read(root.join(relative)).unwrap())
            .unwrap();
    }
}
