use super::*;
use transparent_shared::publication::Eligibility;
use transparent_shared::tx_detail::set::MAP_FILE;

fn live(root: &Path) -> DisplayLive {
    DisplayLive::new(
        DisplayRuntime::new_required(crate::transparent::config::ServiceConfig::default()),
        DisplayRole::Combined,
        3,
        root.join("active.json"),
        Vec::new(),
        None,
    )
    .unwrap()
}

fn current(revision: u64) -> Current {
    Current {
        schema: SCHEMA.into(),
        revision,
        recovery_epoch: 0,
        inventory_sha256: "11".repeat(32),
        network: "main".into(),
        genesis_hash: "22".repeat(32),
        anchor_height: 1,
        anchor_hash: "33".repeat(32),
        eligibility: Eligibility::Canonical,
        invalidated_from: None,
    }
}

fn saved(root: &Path) -> Floor {
    serde_json::from_slice(&std::fs::read(root.join("revocation-floor.json")).unwrap()).unwrap()
}

#[test]
fn repeated_invalid_history_does_not_block_a_newer_recovery_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let mut floor = Floor::default();
    let digest = BTreeSet::from(["44".repeat(32)]);
    {
        let owner = live(root.path());
        revoke(root.path(), &owner, &mut floor, &current(7), digest.clone()).unwrap();
        revoke(root.path(), &owner, &mut floor, &current(8), digest.clone()).unwrap();
        assert_eq!(saved(root.path()).revision, 7);
    }
    let owner = live(root.path());
    let mut floor = saved(root.path());
    revoke(root.path(), &owner, &mut floor, &current(9), digest.clone()).unwrap();
    assert_eq!(saved(root.path()).revision, 7);
    assert!(owner.revocations_cover(&digest));

    // Successful canonical recovery releases this live fence. A subsequent
    // invalidation of the same bytes must require another newer publication.
    owner.restore_verified(&digest).unwrap();
    revoke(root.path(), &owner, &mut floor, &current(10), digest).unwrap();
    assert_eq!(saved(root.path()).revision, 10);
}

#[test]
fn a_failed_floor_write_is_retried_without_moving_its_revision() {
    let root = tempfile::tempdir().unwrap();
    let owner = live(root.path());
    let mut floor = Floor::default();
    let digest = BTreeSet::from(["44".repeat(32)]);
    let path = root.path().join("revocation-floor.json");
    std::fs::create_dir(&path).unwrap();
    assert!(revoke(root.path(), &owner, &mut floor, &current(7), digest.clone()).is_err());
    assert!(owner.revocations_cover(&digest));
    std::fs::remove_dir(path).unwrap();
    revoke(root.path(), &owner, &mut floor, &current(8), digest).unwrap();
    assert_eq!(saved(root.path()).revision, 7);
}

#[test]
fn empty_revocation_does_not_delay_recovery() {
    let root = tempfile::tempdir().unwrap();
    let owner = live(root.path());
    let mut floor = Floor::default();
    revoke(
        root.path(),
        &owner,
        &mut floor,
        &current(7),
        BTreeSet::new(),
    )
    .unwrap();
    assert_eq!(saved(root.path()).revision, 0);
    assert!(saved(root.path()).digests.is_empty());
}

#[tokio::test]
async fn failed_sync_collects_old_downloads_but_preserves_restart_inputs() {
    use pir_pipeline::consumer::polling::SyncTask;
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    let root = root.path().canonicalize().unwrap();
    let objects = root.join("objects");
    let catalog = root.join(".sources");
    std::fs::create_dir(&objects).unwrap();
    std::fs::create_dir(&catalog).unwrap();
    let stage = |n: u64| {
        let digest = format!("{n:064x}");
        let object = objects.join(&digest);
        std::fs::write(&object, b"downloaded bytes").unwrap();
        let source = catalog.join(&digest);
        std::fs::create_dir(&source).unwrap();
        std::fs::hard_link(&object, source.join("rows.bin")).unwrap();
        let directory = root.join(format!("staging-{n}-100"));
        std::fs::create_dir(&directory).unwrap();
        symlink(format!("../.sources/{digest}"), directory.join(&digest)).unwrap();
        std::fs::hard_link(&object, directory.join(MAP_FILE)).unwrap();
        std::fs::File::open(&directory)
            .unwrap()
            .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(100 + n))
            .unwrap();
        directory
    };
    for n in 0..9 {
        stage(n);
    }
    let saved = root.join("staging-0-100");
    persist(
        &root.join("active.json"),
        &DisplayPublication {
            directory: saved.clone(),
            map_sha256: "44".repeat(32),
        },
    )
    .unwrap();
    let orphan = objects.join("ff".repeat(32));
    std::fs::write(&orphan, b"unused download").unwrap();
    let foreign = root.join("operator-backup");
    std::fs::create_dir(&foreign).unwrap();
    let mut sync = DisplaySync::new(
        Config {
            publisher: "https://127.0.0.1:1".into(),
            network: "main".into(),
            genesis_hash: "22".repeat(32),
            directory: root.clone(),
            cache_bytes: 1 << 20,
            max_download_bytes: 1 << 20,
            roots: Vec::new(),
        },
        crate::transparent::config::ServiceConfig::default(),
    )
    .unwrap();
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join(".sources.lock"))
        .unwrap();
    lock.lock().unwrap();
    assert!(sync
        .collect()
        .await
        .unwrap_err()
        .to_string()
        .contains("collection deferred"));
    assert!(orphan.exists());
    assert!(root.join("staging-1-100").exists());
    drop(lock);
    for end in [9, 12] {
        if end == 12 {
            for n in 9..12 {
                stage(n);
            }
        }
        // Discovery fails, but the failed pass must still reclaim space.
        assert!(sync.synchronize().await.is_err());
        assert_eq!(
            std::fs::read(saved.join(MAP_FILE)).unwrap(),
            b"downloaded bytes"
        );
        assert!(foreign.exists());
        assert!(!orphan.exists());
        for n in 1..end {
            let keep = n >= end - 3;
            assert_eq!(root.join(format!("staging-{n}-100")).exists(), keep);
            assert_eq!(catalog.join(format!("{n:064x}")).exists(), keep);
            assert_eq!(objects.join(format!("{n:064x}")).exists(), keep);
        }
    }
}
