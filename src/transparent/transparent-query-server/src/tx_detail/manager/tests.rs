use super::*;
use transparent_shared::publication::Eligibility;

fn live(root: &Path) -> DisplayLive {
    DisplayLive::new(
        DisplayRuntime::new_required(crate::config::ServiceConfig::default()),
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
