use super::*;
use crate::tx_detail::{
    fixture,
    publisher::{self, DisplayRoot, ShardSpec},
};
use std::{collections::BTreeMap, os::unix::fs::MetadataExt};
use transparent_shared::{
    publication::{CheckedRuntimes, Kind},
    tx_detail::{
        distribution::Finalized, prebuild::build_shipped_verified_until, set::DisplaySet,
        DisplayRole,
    },
};

fn stamp(path: &Path) -> (u64, i64, i64, u64) {
    let m = std::fs::metadata(path).unwrap();
    (m.ino(), m.ctime(), m.ctime_nsec(), m.nlink())
}

#[test]
fn archive_written_after_collection_scan_keeps_its_objects_and_can_be_retried() {
    use std::{sync::mpsc, time::Duration};
    let temp = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(temp.path()).unwrap();
    std::fs::create_dir(root.join(publisher::SEALED_DIR)).unwrap();
    let layout = DisplayRoot {
        geometry: "txid-2k8".into(),
        seal: fixture::params(),
        max_archive_shards: 4,
        network: zakura_pir_publisher::transparent::filter::NETWORK.into(),
        genesis_hash: fixture::GENESIS.into(),
        start_height: 101,
        base_parent: "11".repeat(32),
    };
    let spec = ShardSpec {
        shard_id: 0,
        start: 101,
        end: 102,
        parent_block_hash: layout.base_parent.clone(),
        terminal_block_hash: "22".repeat(32),
        parent_manifest_digest: String::new(),
        sealed: true,
        previous: None,
    };
    let records = (1..=10)
        .map(|id| fixture::record(id, 1))
        .collect::<Vec<_>>();
    let (attempted, blocked) = mpsc::channel();
    let (finished, completion) = mpsc::channel();
    std::thread::scope(|scope| {
        let mut writer = None;
        collect_with(&root, || {
            writer = Some(scope.spawn(|| {
                // Observe the actual collector lock without a scheduling delay.
                // Without that lock, finish the archive before the sweep, forcing
                // the original scan -> new reference -> deletion interleaving.
                let probe = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(root.join(writes::LOCK_FILE))
                    .unwrap();
                let waiting = probe.try_lock_shared().is_err();
                drop(probe);
                attempted.send(waiting).unwrap();
                let result = publisher::publish_shard(&root, &layout, &spec, &records);
                finished.send(()).unwrap();
                result
            }));
            if !blocked.recv_timeout(Duration::from_secs(5)).unwrap() {
                completion.recv_timeout(Duration::from_secs(10)).unwrap();
            }
        })
        .unwrap();
        let archive = writer.take().unwrap().join().unwrap().unwrap();
        publisher::verify_dir(&archive.directory, &archive.digest).unwrap();
        let retried = publisher::publish_shard(&root, &layout, &spec, &records).unwrap();
        assert_eq!(retried.digest, archive.digest);
    });
}

#[test]
fn display_advances_reuse_archive_objects_and_collection_respects_live_readers() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let layout = DisplayRoot {
        geometry: "txid-2k8".into(),
        seal: fixture::params(),
        max_archive_shards: 4,
        network: zakura_pir_publisher::transparent::filter::NETWORK.into(),
        genesis_hash: fixture::GENESIS.into(),
        start_height: 101,
        base_parent: "11".repeat(32),
    };
    let archive = publisher::publish_shard(
        root,
        &layout,
        &ShardSpec {
            shard_id: 0,
            start: 101,
            end: 102,
            parent_block_hash: layout.base_parent.clone(),
            terminal_block_hash: "22".repeat(32),
            parent_manifest_digest: String::new(),
            sealed: true,
            previous: None,
        },
        &(1..=10)
            .map(|id| fixture::record(id, 1))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let mut checked = CheckedRuntimes::default();
    let mut previous = None;
    let mut originals = BTreeMap::new();
    let mut runtime_bytes = 0;
    for cycle in 0..3 {
        let height = 103 + cycle;
        let recent = publisher::publish_shard(
            root,
            &layout,
            &ShardSpec {
                shard_id: 1,
                start: 103,
                end: height,
                parent_block_hash: archive.manifest.terminal_block_hash.clone(),
                terminal_block_hash: format!("{height:064x}"),
                parent_manifest_digest: archive.digest.clone(),
                sealed: false,
                previous: None,
            },
            &(0..5)
                .map(|id| fixture::record(20 + cycle as u32 * 10 + id, 1))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let map = layout.map(&[archive.entry.clone()], recent.entry);
        let (directory, _) = publisher::write_candidate(root, &map).unwrap();
        let set = DisplaySet::open_reusing_until(
            &directory,
            0,
            previous.as_ref().map(|(_, set)| set),
            &[],
            DisplayRole::Combined,
            &|| false,
        )
        .unwrap();
        let retained = previous
            .as_ref()
            .map(|(path, _): &(PathBuf, DisplaySet)| vec![path.clone()])
            .unwrap_or_default();
        for revision in set.current() {
            build_shipped_verified_until(revision, &directory, &retained, &|| false).unwrap();
        }
        adopt_runtimes(&directory, &|| false).unwrap();
        let finalized =
            Finalized::prepare_reusing_until(&directory, &set, &mut checked, &|| false).unwrap();
        for artifact in &finalized.inventory.artifacts {
            if matches!(artifact.kind, Kind::ArchiveTable | Kind::ArchiveRuntime) {
                let path = root.join(OBJECT_STORE).join(&artifact.sha256);
                let identity = stamp(&path);
                if cycle == 0 {
                    originals.insert(artifact.sha256.clone(), identity);
                    if matches!(artifact.kind, Kind::ArchiveRuntime) {
                        runtime_bytes += artifact.bytes;
                    }
                } else {
                    assert_eq!(
                        originals.get(&artifact.sha256),
                        Some(&identity),
                        "unchanged archive object metadata changed"
                    );
                }
            }
        }
        if cycle == 2 {
            assert!(
                checked.bytes_reused >= runtime_bytes,
                "archive runtime bodies were hashed again"
            );
            assert!(
                set.verification().bytes_reused > 0,
                "archive table bodies were hashed again"
            );
            let lease = PublicationLease::acquire(&directory).unwrap();
            publisher::collect(root, &Default::default(), 0).unwrap();
            assert!(
                directory.exists(),
                "a reader's publication was collected before opening its artifact"
            );
            let artifact = finalized
                .inventory
                .artifacts
                .iter()
                .find(|a| matches!(a.kind, Kind::RecentRuntime))
                .unwrap();
            let (physical, mut file) = transparent_shared::publication::open_artifact(
                &directory,
                &artifact.path,
                Some(&pir_publication::ObjectReference {
                    sha256: artifact.sha256.clone(),
                    bytes: artifact.bytes,
                }),
            )
            .unwrap();
            drop(lease);
            publisher::collect(root, &Default::default(), 0).unwrap();
            assert!(!directory.exists());
            assert!(!physical.exists(), "an unowned runtime was not collected");
            use std::io::Read;
            let mut bytes = [0; 32];
            file.read_exact(&mut bytes).unwrap();
        }
        previous = Some((directory, set));
        if cycle == 0 {
            std::thread::sleep(std::time::Duration::from_millis(1100));
        }
    }
}

#[test]
fn object_references_refuse_escape_and_existing_corrupt_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let publication = root.join("candidate-1");
    std::fs::create_dir(&publication).unwrap();
    let path = publication.join("ab.runtime");
    write_object(root, &path, b"runtime bytes").unwrap();
    let target = std::fs::canonicalize(&path).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
    std::fs::write(&target, b"runtime byteS").unwrap();
    assert!(write_object(root, &path, b"runtime bytes").is_err());
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink("../../outside", &path).unwrap();
    assert!(adopt_runtimes(&publication, &|| false).is_err());
}
