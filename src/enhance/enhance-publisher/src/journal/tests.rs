use super::*;
use crate::types::{EnhanceRecord, ENHANCE_LAYOUT, RECORD_BYTES};

fn record(byte: u8) -> EnhanceRecord {
    {
        let mut bytes = [byte; RECORD_BYTES];
        bytes[enhance_shared::RECORD_FLAGS_OFFSET..].fill(0);
        EnhanceRecord::from_bytes(bytes).unwrap()
    }
}

fn open(dir: &Path) -> RecordJournal {
    RecordJournal::open(dir, DatabaseId::Enhance, ENHANCE_LAYOUT).expect("open")
}

#[test]
fn append_failures_reopen_to_a_complete_committed_prefix() {
    for stage in [
        "record_write",
        "record_sync",
        "manifest_write",
        "manifest_sync",
        "manifest_rename",
        "directory_sync",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        store.append_block(10, "aa".into(), &[record(1)]).unwrap();
        store.fail_after = Some(stage);
        assert!(store
            .append_block(11, "bb".into(), &[record(2), record(3)])
            .is_err());
        assert!(store.read_records(0, 1).is_err());
        assert!(store
            .append_block(11, "retry".into(), &[record(4)])
            .is_err());
        assert!(store.rewind_to_height(None).is_err());
        drop(store);
        let mut recovered = open(dir.path());
        let committed = matches!(stage, "manifest_rename" | "directory_sync");
        assert_eq!(
            recovered.tree_size(),
            if committed { 3 } else { 1 },
            "{stage}"
        );
        assert_eq!(recovered.read_records(0, 1).unwrap(), record(1).as_bytes());
        if committed {
            assert_eq!(recovered.read_records(2, 1).unwrap(), record(3).as_bytes());
        } else {
            recovered
                .append_block(11, "replacement".into(), &[record(4)])
                .unwrap();
            assert_eq!(recovered.read_records(1, 1).unwrap(), record(4).as_bytes());
        }
        assert_eq!(
            fs::metadata(dir.path().join("records.bin")).unwrap().len(),
            recovered.tree_size() * RECORD_BYTES as u64
        );
    }
}

#[test]
fn rewind_failures_never_remove_committed_records() {
    for stage in [
        "manifest_write",
        "manifest_sync",
        "manifest_rename",
        "directory_sync",
        "truncate",
        "truncate_sync",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        store.append_block(10, "aa".into(), &[record(1)]).unwrap();
        store.append_block(11, "bb".into(), &[record(2)]).unwrap();
        store.fail_after = Some(stage);
        assert!(store.rewind_to_height(Some(10)).is_err());
        assert!(store.read_records(0, 1).is_err());
        drop(store);
        let mut recovered = open(dir.path());
        let committed = !matches!(stage, "manifest_write" | "manifest_sync");
        assert_eq!(
            recovered.tree_size(),
            if committed { 1 } else { 2 },
            "{stage}"
        );
        assert_eq!(recovered.read_records(0, 1).unwrap(), record(1).as_bytes());
        recovered.rewind_to_height(Some(10)).unwrap();
        recovered
            .append_block(11, "replacement".into(), &[record(4)])
            .unwrap();
        assert_eq!(recovered.read_records(1, 1).unwrap(), record(4).as_bytes());
    }
}

#[test]
fn empty_blocks_and_rewind_to_empty_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = open(dir.path());
    store
        .append_block(10, "empty".into(), &[] as &[EnhanceRecord])
        .unwrap();
    store.append_block(11, "aa".into(), &[record(1)]).unwrap();
    store.rewind_to_height(Some(10)).unwrap();
    drop(store);
    let mut store = open(dir.path());
    assert_eq!(store.tree_size(), 0);
    assert_eq!(store.last_block().unwrap().height, 10);
    store.rewind_to_height(None).unwrap();
    drop(store);
    let store = open(dir.path());
    assert!(store.last_block().is_none());
    assert_eq!(store.tree_size(), 0);
}

#[test]
fn append_restart_and_padding_are_deterministic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut store = open(dir.path());
    store
        .append_block(10, "aa".to_string(), &[record(1), record(2)])
        .expect("append");
    drop(store);

    let store = open(dir.path());
    assert_eq!(store.tree_size(), 2);
    assert_eq!(store.shard_ids(), 0..=0);
    let shard = store.read_shard_rows(0).expect("shard");
    assert_eq!(shard.len(), ENHANCE_LAYOUT.shard_bytes());
    assert_eq!(&shard[..RECORD_BYTES], record(1).as_bytes());
    assert_eq!(&shard[RECORD_BYTES..2 * RECORD_BYTES], record(2).as_bytes());
    assert!(shard[2 * RECORD_BYTES..].iter().all(|byte| *byte == 0));
}

#[test]
fn journal_refuses_concurrent_writers() {
    let dir = tempfile::tempdir().unwrap();
    let first = open(dir.path());
    assert!(RecordJournal::open(dir.path(), DatabaseId::Enhance, ENHANCE_LAYOUT).is_err());
    drop(first);
    assert!(RecordJournal::open(dir.path(), DatabaseId::Enhance, ENHANCE_LAYOUT).is_ok());
}

#[test]
fn records_must_match_the_layout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut store = open(dir.path());
    let short = vec![0u8; RECORD_BYTES - 1];
    assert!(store.append_block(10, "aa".to_string(), &[short]).is_err());
    assert_eq!(store.tree_size(), 0);
}

#[test]
fn incompatible_journals_are_rejected_without_mutation() {
    for version in [1, 3, 5] {
        let dir = tempfile::tempdir().unwrap();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "version": version, "table": "enhance", "tree_size": 2,
            "blocks": [{"height": 5, "hash": "aa", "first_position": 0, "action_count": 2}]
        }))
        .unwrap();
        let records = vec![7u8; 2 * 725];
        std::fs::write(dir.path().join("manifest.json"), &manifest).unwrap();
        std::fs::write(dir.path().join("records.bin"), &records).unwrap();
        assert!(RecordJournal::open(dir.path(), DatabaseId::Enhance, ENHANCE_LAYOUT).is_err());
        assert_eq!(
            std::fs::read(dir.path().join("manifest.json")).unwrap(),
            manifest
        );
        assert_eq!(
            std::fs::read(dir.path().join("records.bin")).unwrap(),
            records
        );
    }
}

#[test]
fn rewind_truncates_records_and_allows_replacement_blocks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut store = open(dir.path());
    store.append_block(10, "aa".into(), &[record(1)]).unwrap();
    store
        .append_block(11, "bb".into(), &[record(2), record(3)])
        .unwrap();
    store.rewind_to_height(Some(10)).unwrap();
    assert_eq!(store.tree_size(), 1);
    assert_eq!(store.last_block().unwrap().hash, "aa");
    store.append_block(11, "cc".into(), &[record(4)]).unwrap();
    assert_eq!(store.tree_size(), 2);
    assert_eq!(
        store.read_records(1, 1).unwrap(),
        record(4).as_bytes().to_vec()
    );
}
