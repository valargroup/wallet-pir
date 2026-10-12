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
    incompatible.shard_schema = "transparent-shard-v11".into();
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

/// Schema 5 is a fresh schema: a store from an earlier one holds walks and
/// coverage of a shard schema this build does not read, and is refused
/// rather than migrated.
#[test]
fn a_store_from_an_earlier_schema_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("v4.sqlite");
    let mut store = SqliteStore::open(&path).unwrap();
    store.bind_set(&identity()).unwrap();
    drop(store);
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("UPDATE wallet_meta SET value = '4' WHERE key = 'schema_version';")
        .unwrap();
    drop(db);
    assert!(SqliteStore::open(&path).is_err());
}

/// A walk's cursor and carried events, and the run cache, are on disk once
/// their commit returns.
#[test]
fn a_walk_cursor_and_the_run_cache_survive_reopening() {
    use transparent_wallet::store::RunCursor;
    use transparent_wallet::testing::{cached_run, pending};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("walk.sqlite");
    let mut store = SqliteStore::open(&path).unwrap();
    store.bind_set(&identity()).unwrap();
    let mut c = commit(1, "r0", true, (0, 99), vec![], vec![]);
    c.pending_upsert.push(pending(1, "r0"));
    let run = transparent_wallet::store::CachedRun {
        shard_id: 1,
        older: Some("older-child".into()),
        ..cached_run("child", 1, &[10, 20])
    };
    c.cached_runs.push(run.clone());
    store.commit_shard(c).unwrap();
    drop(store);
    let mut store = SqliteStore::open(&path).unwrap();
    let mut restored = store.pending().unwrap().remove(0);
    assert_eq!(
        restored,
        transparent_wallet::PendingPages {
            id: restored.id,
            ..pending(1, "r0")
        }
    );
    assert_eq!(store.cached_run("child", &script(1)).unwrap(), Some(run));
    restored.cursor = RunCursor {
        child: 2,
        row: 70_000,
        older_runs: Some(4),
    };
    restored.carried.reverse();
    let mut c = commit(1, "r0", true, (0, 99), vec![], vec![]);
    c.pending_upsert.push(restored.clone());
    store.commit_shard(c).unwrap();
    drop(store);
    assert_eq!(
        SqliteStore::open(&path).unwrap().pending().unwrap(),
        vec![restored]
    );
}
