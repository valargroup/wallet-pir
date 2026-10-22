use super::persistence::save_with_sync;
use super::*;
use enhance_query_server::protocol::Lifecycle;
use std::{
    collections::BTreeMap,
    fs::{self, File},
};

#[test]
fn state_save_syncs_the_renamed_file_directory_and_reports_barrier_failure() {
    let root = tempfile::tempdir().unwrap();
    let value = json!({"rollback_epoch": 6});
    let mut synced = false;
    save_with_sync(root.path(), "reservation.json", &value, |directory| {
        assert_eq!(directory, root.path());
        assert_eq!(
            read::<Value>(&directory.join("reservation.json")).unwrap(),
            value
        );
        synced = true;
        File::open(directory)?.sync_all()
    })
    .unwrap();
    assert!(
        synced,
        "a successful save must persist the renamed directory entry"
    );

    let error = save_with_sync(root.path(), "reservation.json", &value, |_| {
        Err(std::io::Error::other("directory sync failed"))
    })
    .unwrap_err();
    assert!(
        error.contains("directory sync failed"),
        "a failed durability barrier must stop the handoff"
    );
}

fn transfer(data: &Path, rollback: bool) -> Result<Value, String> {
    super::transfer(data, rollback, "publisher-fixture")
}

fn previous(root: &Path) {
    fs::create_dir_all(root.join("control")).unwrap();
    save(&root.join("control"), "controller.json", &json!({
        "version":8, "next_generation":23, "lifecycle":Lifecycle::default(),
        "recovery":{"epoch":"4", "domain_epochs":{"0":"4"},
            "revoked":["ab".repeat(32)], "sealed":{"0":[3_428_143,"12".repeat(32)]}, "rollback_to":null},
        "operation":null, "draining":[], "pending_commits":[], "pending_aborts":[],
        "published":[], "epoch":91, "revision":17, "groups":[], "assignments":{},
        "unrelated_previous_field":{"kept":true}
    })).unwrap();
}

#[test]
fn import_preserves_every_floor_without_granting_serving_authority() {
    let root = tempfile::tempdir().unwrap();
    previous(root.path());
    let old = fs::read(root.path().join("control/controller.json")).unwrap();
    let report = transfer(root.path(), false).unwrap();
    assert_eq!(report["recovery_epoch"], 4);
    assert_eq!(report["next_generation"], 23);
    let state: PublisherState = read(&root.path().join("publication/publisher.json")).unwrap();
    assert!(state.recovery.revoked.contains(&"ab".repeat(32)));
    assert_eq!(state.recovery.domain_epochs[&0], "4");
    assert_eq!(state.recovery.sealed[&0].0, 3_428_143);
    let distribution: DistributionState =
        read(&root.path().join("publication/https/current.json")).unwrap();
    assert!(!distribution.current.eligible);
    assert!(distribution.current.manifest.is_none());
    transfer(root.path(), false).unwrap();
    assert_eq!(
        fs::read(root.path().join("control/controller.json")).unwrap(),
        old
    );
}

#[test]
fn failed_forward_validation_can_restore_the_untouched_predecessor() {
    let root = tempfile::tempdir().unwrap();
    previous(root.path());
    let old = fs::read(root.path().join("control/controller.json")).unwrap();
    let result = transfer(root.path(), true).unwrap();
    assert_eq!(result["unchanged"], true);
    assert_eq!(result["recovery_epoch"], 4);
    assert_eq!(
        fs::read(root.path().join("control/controller.json")).unwrap(),
        old
    );
    assert!(!root.path().join("publication/publisher.json").exists());
}

#[test]
fn imported_state_opens_in_the_actual_publisher_without_resetting_floors() {
    let root = tempfile::tempdir().unwrap();
    previous(root.path());
    transfer(root.path(), false).unwrap();
    let cookie = root.path().join("cookie");
    fs::write(&cookie, "fixture:fixture").unwrap();
    let rpc =
        crate::chain_source::ZakuraClient::from_cookie_file("http://127.0.0.1:1", cookie).unwrap();
    let publisher =
        crate::publisher::EnhancePublisher::open(&root.path().join("publication"), rpc).unwrap();
    let current = publisher.distribution().durable_current();
    assert_eq!(current.recovery_epoch, 4);
    assert!(current.revoked.contains(&"ab".repeat(32)));
    assert!(!current.eligible);
    let state: PublisherState = read(&root.path().join("publication/publisher.json")).unwrap();
    assert_eq!(state.next_generation, 23);
    assert_eq!(state.recovery.domain_epochs[&0], "4");
    assert_eq!(state.recovery.sealed[&0].0, 3_428_143);
    assert!(transfer(root.path(), true).unwrap_err().contains("owned"));
}

#[test]
fn historical_operation_receipts_do_not_use_the_https_metadata_limit() {
    let root = tempfile::tempdir().unwrap();
    previous(root.path());
    let mut old = previous_state(&root.path().join("control/controller.json")).unwrap();
    old["retained_operation_history"] = json!("x".repeat(2 * 1024 * 1024));
    save(&root.path().join("control"), "controller.json", &old).unwrap();
    transfer(root.path(), false).unwrap();
    transfer(root.path(), true).unwrap();
    let restored = previous_state(&root.path().join("control/controller.json")).unwrap();
    assert_eq!(
        restored["retained_operation_history"],
        old["retained_operation_history"]
    );
    assert_eq!(restored["recovery"]["epoch"], "5");
}

#[test]
fn a_new_cutover_uses_the_predecessors_sealed_records_after_reorg() {
    for sealed in [BTreeMap::new(), [(0, (3_428_200, "34".repeat(32)))].into()] {
        let root = tempfile::tempdir().unwrap();
        previous(root.path());
        transfer(root.path(), false).unwrap();
        transfer(root.path(), true).unwrap();

        // The restored runtime has already fenced and repaired a deep reorg.
        // A removed seal may stay absent or be replaced after finality.
        let path = root.path().join("control/controller.json");
        let mut old = previous_state(&path).unwrap();
        old["recovery"]["epoch"] = json!("6");
        old["recovery"]["domain_epochs"]["0"] = json!("6");
        old["recovery"]["sealed"] = serde_json::to_value(&sealed).unwrap();
        save(&root.path().join("control"), "controller.json", &old).unwrap();

        super::transfer(root.path(), false, "publisher-after-reorg").unwrap();
        let state: PublisherState = read(&root.path().join("publication/publisher.json")).unwrap();
        assert_eq!(
            state.recovery.sealed, sealed,
            "old HTTPS seals must not override the repaired predecessor"
        );
        assert_eq!(state.recovery.epoch, 6);
        assert_eq!(state.recovery.domain_epochs[&0], "6");
        assert!(state.recovery.revoked.contains(&"ab".repeat(32)));
        verify_startup(root.path()).unwrap();
    }
}

#[test]
fn a_new_reviewed_attempt_after_rollback_keeps_all_prior_floors() {
    let root = tempfile::tempdir().unwrap();
    previous(root.path());
    transfer(root.path(), false).unwrap();
    assert!(super::transfer(root.path(), false, "publisher-other").is_err());
    transfer(root.path(), true).unwrap();
    assert!(transfer(root.path(), false).is_err());
    let report = super::transfer(root.path(), false, "publisher-other").unwrap();
    assert_eq!(report["recovery_epoch"], 5);
    assert_eq!(report["next_generation"], 23);
    assert!(transfer(root.path(), true).is_err());
    let report = super::transfer(root.path(), true, "publisher-other").unwrap();
    assert_eq!(report["recovery_epoch"], 6);
}

#[test]
fn reserved_receipts_cannot_start_a_runtime_or_replace_lost_initialized_state() {
    let root = tempfile::tempdir().unwrap();
    previous(root.path());
    assert!(verify_startup(root.path()).is_err());
    let old = previous_state(&root.path().join("control/controller.json")).unwrap();
    let publication = root.path().join("publication");
    fs::create_dir_all(&publication).unwrap();
    let receipt = HandoffReceipt {
        version: 1,
        transaction: "publisher-fixture".into(),
        completed: false,
        initialized: false,
        rollback_epoch: None,
        previous_sha256: hex::encode(Sha256::digest(serde_json::to_vec(&old).unwrap())),
    };
    save(&publication, "handoff.json", &receipt).unwrap();
    assert!(verify_startup(root.path()).is_err());
    transfer(root.path(), false).unwrap();
    verify_startup(root.path()).unwrap();
    fs::remove_file(publication.join("publisher.json")).unwrap();
    assert!(verify_startup(root.path()).is_err());
    assert!(transfer(root.path(), true).unwrap_err().contains("missing"));
}

#[test]
fn existing_https_floors_are_merged_without_importing_serving_authority() {
    let root = tempfile::tempdir().unwrap();
    previous(root.path());
    fs::create_dir_all(root.path().join("control/https")).unwrap();
    save(
        &root.path().join("control/https"),
        "current.json",
        &DistributionState {
            current: Current {
                version: VERSION,
                revision: 900,
                withdrawal_revision: 899,
                recovery_epoch: 7,
                revoked: ["cd".repeat(32)].into(),
                eligible: false,
                manifest: None,
                inventory: None,
            },
            retained: Vec::new(),
        },
    )
    .unwrap();
    let result = transfer(root.path(), false).unwrap();
    assert_eq!(result["recovery_epoch"], 7);
    assert_eq!(result["withdrawal_revision"], 901);
    let current: DistributionState =
        read(&root.path().join("publication/https/current.json")).unwrap();
    assert_eq!(current.current.revoked.len(), 2);
    assert!(!current.current.eligible);
    assert_eq!(transfer(root.path(), true).unwrap()["recovery_epoch"], 8);
}

#[test]
fn committed_retention_records_are_preserved_but_uncommitted_records_refuse() {
    let root = tempfile::tempdir().unwrap();
    previous(root.path());
    let mut old = previous_state(&root.path().join("control/controller.json")).unwrap();
    old["completed"] = json!({"retired":"ef".repeat(32)});
    old["draining"] = json!([{"id":"retired", "phase":"DRAINING", "candidate_digest":"ef".repeat(32),
        "attempt":1,"epoch":91,"expected_revision":16,"assignments":{},"affected_groups":[],
        "source_generations":[21],"require_both":[],"readiness":[],"infrastructure_resources":{}}]);
    save(&root.path().join("control"), "controller.json", &old).unwrap();
    transfer(root.path(), false).unwrap();
    verify_startup(root.path()).unwrap();
    transfer(root.path(), true).unwrap();
    assert!(verify_startup(root.path()).is_err());
    let restored = previous_state(&root.path().join("control/controller.json")).unwrap();
    assert_eq!(restored["draining"], old["draining"]);
    let other = tempfile::tempdir().unwrap();
    previous(other.path());
    old["draining"][0]["phase"] = json!("READY");
    save(&other.path().join("control"), "controller.json", &old).unwrap();
    assert!(transfer(other.path(), false).is_err());
    assert_eq!(transfer(other.path(), true).unwrap()["unchanged"], true);
}

#[test]
fn rollback_merges_new_floors_and_retries_one_reserved_epoch_after_interruption() {
    let root = tempfile::tempdir().unwrap();
    previous(root.path());
    transfer(root.path(), false).unwrap();
    let publication = root.path().join("publication");
    let mut state: PublisherState = read(&publication.join("publisher.json")).unwrap();
    state.next_generation = 29;
    state.recovery.epoch = 5;
    state.recovery.domain_epochs.insert(0, "5".into());
    state.recovery.revoked.insert("cd".repeat(32));
    save(&publication, "publisher.json", &state).unwrap();
    // Simulate interruption after reserving rollback and persisting the new
    // source fence, before updating distribution or the previous format.
    let mut receipt: HandoffReceipt = read(&publication.join("handoff.json")).unwrap();
    receipt.rollback_epoch = Some(6);
    save(&publication, "handoff.json", &receipt).unwrap();
    state.recovery.epoch = 6;
    save(&publication, "publisher.json", &state).unwrap();
    for _ in 0..2 {
        let report = transfer(root.path(), true).unwrap();
        assert_eq!(report["recovery_epoch"], 6);
        assert_eq!(report["next_generation"], 29);
        let restored: Value = read(&root.path().join("control/controller.json")).unwrap();
        assert_eq!(restored["recovery"]["epoch"], "6");
        assert_eq!(
            restored["recovery"]["domain_epochs"]
                .as_object()
                .unwrap()
                .len(),
            MAX_QUERY_SHARDS as usize
        );
        assert_eq!(restored["recovery"]["revoked"].as_array().unwrap().len(), 2);
        assert_eq!(
            restored["epoch"], 91,
            "the previous controller lease is not a recovery epoch"
        );
        assert_eq!(restored["unrelated_previous_field"]["kept"], true);
        assert_eq!(restored["published"], json!([]));
    }
    assert!(
        transfer(root.path(), false).is_err(),
        "a completed rollback needs a fresh reviewed cutover"
    );
}

#[test]
fn running_owner_and_unfinished_recovery_refuse_without_overwriting_state() {
    let root = tempfile::tempdir().unwrap();
    previous(root.path());
    let owner = lock(root.path(), "publisher.lock").unwrap();
    assert!(transfer(root.path(), false).unwrap_err().contains("owned"));
    assert!(!root.path().join("publication/publisher.json").exists());
    drop(owner);
    let mut old: Value = read(&root.path().join("control/controller.json")).unwrap();
    old["recovery"]["rollback_to"] = json!(0);
    save(&root.path().join("control"), "controller.json", &old).unwrap();
    assert!(transfer(root.path(), false)
        .unwrap_err()
        .contains("unfinished"));
    assert!(!root.path().join("publication/publisher.json").exists());
}
