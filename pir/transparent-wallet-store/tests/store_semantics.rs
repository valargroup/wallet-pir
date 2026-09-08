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
