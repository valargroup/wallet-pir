//! The store contract, run against both reference implementations.
//!
//! The suite itself lives in `transparent_wallet::testing` so a wallet that
//! brings its own store can run the same one; what is here is only the three
//! constructors and the reopen check.

use transparent_wallet::store::{Anchor, WalletStore};
use transparent_wallet::testing::{commit, identity, receive, script, suite};
use transparent_wallet::MemoryStore;
use transparent_wallet_store::SqliteStore;

#[test]
fn metadata_survives_reopen_and_cross_script_contradictions_are_atomic() {
    use transparent_events::{FeeState, TransactionMetadata};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metadata.sqlite");
    let metadata = TransactionMetadata {
        fee: FeeState::Exact(1000),
        transparent_input_count: 2,
        has_shielded_components: true,
    };
    let mut event = receive(1, 0, 5000, 10);
    event.event = event.event.with_metadata(Some(metadata));
    let mut store = SqliteStore::open(&path).unwrap();
    store.bind_set(&identity()).unwrap();
    store
        .commit_shard(commit(
            0,
            "r0",
            true,
            (0, 99),
            vec![event.clone()],
            vec![script(1)],
        ))
        .unwrap();
    drop(store);
    let mut store = SqliteStore::open(&path).unwrap();
    assert_eq!(store.events().unwrap(), vec![event.clone()]);
    let before = store.last_commit().unwrap();
    let mut conflict = event.clone();
    conflict.script = script(2);
    conflict.event = match conflict.event {
        transparent_events::TransparentEvent::Receive(mut r) => {
            r.output_index = 1;
            transparent_events::TransparentEvent::Receive(r)
        }
        _ => unreachable!(),
    }
    .with_metadata(Some(TransactionMetadata {
        fee: FeeState::Exact(1001),
        ..metadata
    }));
    assert!(store
        .commit_shard(commit(
            1,
            "r1",
            true,
            (0, 99),
            vec![conflict],
            vec![script(2)]
        ))
        .is_err());
    assert_eq!(store.events().unwrap(), vec![event]);
    assert_eq!(store.last_commit().unwrap(), before);
    assert!(store.coverage(&script(2)).unwrap().is_empty());
    let mut incompatible = identity();
    incompatible.shard_schema = transparent_shard::manifest::LEGACY_SCHEMA.into();
    assert!(store.bind_set(&incompatible).is_err());
}

#[test]
fn the_memory_store_meets_the_contract() {
    suite(|| MemoryStore::with_pending_limit(8));
}

#[test]
fn the_sqlite_store_meets_the_contract_in_memory() {
    suite(|| SqliteStore::open_in_memory().unwrap().with_pending_limit(8));
}

#[test]
fn the_sqlite_store_meets_the_contract_on_disk_and_survives_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wallet.sqlite");
    suite(|| {
        // Each run starts from a fresh file.
        let _ = std::fs::remove_file(&path);
        SqliteStore::open(&path).unwrap().with_pending_limit(8)
    });

    // What was committed is there after the process forgets everything.
    let _ = std::fs::remove_file(&path);
    let mut store = SqliteStore::open(&path).unwrap();
    store.bind_set(&identity()).unwrap();
    store
        .commit_shard(commit(
            0,
            "r0",
            true,
            (0, 99),
            vec![receive(1, 0, 5_000, 10)],
            vec![script(1)],
        ))
        .unwrap();
    store
        .commit_anchor(
            &Anchor {
                height: 99,
                hash: "h".into(),
            },
            99,
            99,
        )
        .unwrap();
    drop(store);
    let store = SqliteStore::open(&path).unwrap();
    assert_eq!(store.set_identity().unwrap(), Some(identity()));
    assert_eq!(store.anchor().unwrap().unwrap().height, 99);
    assert_eq!(store.ledger().unwrap().confirmed_balance(), 5_000);
    assert_eq!(store.coverage(&script(1)).unwrap().len(), 1);
    assert_eq!(store.last_commit().unwrap(), 2);
}

#[test]
fn migration_preserves_events_but_invalidates_unbound_legacy_progress() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.sqlite");
    let mut store = SqliteStore::open(&path).unwrap();
    store.bind_set(&identity()).unwrap();
    store
        .add_scripts(&[transparent_wallet::ScriptEntry {
            script: script(1),
            origin: transparent_wallet::ScriptOrigin::Derived,
            required_from: 0,
        }])
        .unwrap();
    store
        .commit_shard(commit(
            0,
            "r0",
            true,
            (0, 99),
            vec![receive(1, 0, 123, 10)],
            vec![script(1)],
        ))
        .unwrap();
    store
        .commit_anchor(
            &Anchor {
                height: 99,
                hash: "legacy".into(),
            },
            99,
            99,
        )
        .unwrap();
    drop(store);
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch(
        "ALTER TABLE coverage DROP COLUMN source_anchor;
        ALTER TABLE pending_work DROP COLUMN target_anchor;
        ALTER TABLE pending_work DROP COLUMN validated_events;
         ALTER TABLE pending_work DROP COLUMN page_boundary;
        UPDATE wallet_meta SET value = '1' WHERE key = 'schema_version';",
    )
    .unwrap();
    drop(db);
    let store = SqliteStore::open(&path).unwrap();
    assert_eq!(store.events().unwrap().len(), 1);
    assert_eq!(store.scripts().unwrap().len(), 1);
    assert_eq!(store.ledger().unwrap().confirmed_balance(), 123);
    assert!(store.anchor().unwrap().is_none());
    assert!(store.coverage(&script(1)).unwrap().is_empty());
    assert!(store.pending().unwrap().is_empty());
    drop(store);
    SqliteStore::open(&path).unwrap();
}

#[test]
fn fragment_boundary_survives_update_and_reopen() {
    use transparent_wallet::store::PageBoundary;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("boundary.sqlite");
    let mut store = SqliteStore::open(&path).unwrap();
    store.bind_set(&identity()).unwrap();
    let mut pending = transparent_wallet::testing::pending(1, "r0");
    let boundary = PageBoundary {
        event_bytes: 4029,
        last_event: receive(1, 0, 12, 10).event,
    };
    pending.next_ordinal = 1;
    pending.boundary = Some(boundary.clone());
    let mut c = commit(1, "r0", true, (0, 99), vec![], vec![]);
    c.pending_upsert.push(pending);
    store.commit_shard(c).unwrap();
    drop(store);
    let mut store = SqliteStore::open(&path).unwrap();
    let mut restored = store.pending().unwrap().remove(0);
    assert_eq!(restored.boundary, Some(boundary.clone()));
    restored.next_ordinal = 2;
    restored.boundary.as_mut().unwrap().event_bytes = 4042;
    let mut c = commit(1, "r0", true, (0, 99), vec![], vec![]);
    c.pending_upsert.push(restored.clone());
    store.commit_shard(c).unwrap();
    drop(store);
    assert_eq!(
        SqliteStore::open(&path).unwrap().pending().unwrap(),
        vec![restored]
    );
}

#[test]
fn v2_migration_preserves_events_and_adds_empty_boundary_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("v2.sqlite");
    let mut store = SqliteStore::open(&path).unwrap();
    store.bind_set(&identity()).unwrap();
    store
        .commit_shard(commit(
            0,
            "r0",
            true,
            (0, 99),
            vec![receive(1, 0, 123, 10)],
            vec![script(1)],
        ))
        .unwrap();
    drop(store);
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch(
        "ALTER TABLE pending_work DROP COLUMN page_boundary;
        UPDATE wallet_meta SET value = '2' WHERE key = 'schema_version';",
    )
    .unwrap();
    drop(db);
    let store = SqliteStore::open(&path).unwrap();
    assert_eq!(store.events().unwrap().len(), 1);
    assert_eq!(store.coverage(&script(1)).unwrap().len(), 1);
    drop(store);
    SqliteStore::open(&path).unwrap();
}
