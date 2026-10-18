//! A cutover must refuse old state without modifying its durable bytes.
use enhance_pir_server::{
    control::{State, Store},
    store::RecordJournal,
    types::{DatabaseId, ENHANCE_LAYOUT},
    worker::Worker,
};

#[test]
fn full_ciphertext_journal_is_rejected_without_truncation() {
    let root = tempfile::tempdir().unwrap();
    let manifest = serde_json::to_vec(&serde_json::json!({
        "version": 7, "table": "enhance", "record_bytes": 737, "records_per_row": 33,
        "tree_size": 1, "blocks": [{"height": 3428143, "hash": "01".repeat(32), "first_position": 0, "action_count": 1}]
    })).unwrap();
    let records = vec![42; 737];
    std::fs::write(root.path().join("manifest.json"), &manifest).unwrap();
    std::fs::write(root.path().join("records.bin"), &records).unwrap();
    assert!(RecordJournal::open(root.path(), DatabaseId::Enhance, ENHANCE_LAYOUT).is_err());
    assert_eq!(
        std::fs::read(root.path().join("manifest.json")).unwrap(),
        manifest
    );
    assert_eq!(
        std::fs::read(root.path().join("records.bin")).unwrap(),
        records
    );
}

#[test]
fn old_controller_and_worker_state_are_rejected_without_rewrite() {
    let root = tempfile::tempdir().unwrap();
    let old = State {
        version: 4,
        ..State::default()
    };
    let bytes = serde_json::to_vec(&old).unwrap();
    let path = root.path().join("controller.json");
    std::fs::write(&path, &bytes).unwrap();
    assert!(Store::open(root.path()).is_err());
    assert_eq!(std::fs::read(path).unwrap(), bytes);
    for schema in [None, Some(10)] {
        let root = tempfile::tempdir().unwrap();
        let mut old = serde_json::json!({"epoch": 1, "revision": 1, "last_attempt": null,
            "retention": [], "candidate": null, "activated": null, "published": {}});
        if let Some(schema) = schema {
            old["schema_version"] = schema.into();
        }
        let bytes = serde_json::to_vec(&old).unwrap();
        let path = root.path().join("worker.json");
        std::fs::write(&path, &bytes).unwrap();
        assert!(Worker::open(root.path()).is_err());
        assert!(Worker::repair_rows(root.path(), root.path()).is_err());
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
}
