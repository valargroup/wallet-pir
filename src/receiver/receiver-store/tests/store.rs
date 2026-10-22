mod common;
use common::{manifest, receiver, record};
use receiver_store::Error;
use zakura_pir_publisher::receiver::{
    snapshot::{lookup_row, row_for, Snapshot, ROW_BYTES, SLOTS},
    RawRecord, ReceiverBytes,
};

fn row<'a>(s: &'a Snapshot, r: &ReceiverBytes, page: u32) -> &'a [u8] {
    let n = row_for(&s.manifest, r, page).unwrap();
    &s.data[n * ROW_BYTES..(n + 1) * ROW_BYTES]
}

/// Valid Ironwood receivers, pinned in `fixtures/receivers.json` (see
/// [`common::seeded_receiver`]): `other` and `third` from spending keys of bytes 3 and
/// 4 at external index zero, and `spread` from the spending key of bytes 5 at external
/// indices 0 to 15.
fn pinned(key: &str) -> Vec<ReceiverBytes> {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/receivers.json")).unwrap();
    let values = match &fixture[key] {
        serde_json::Value::Array(values) => values.clone(),
        value => vec![value.clone()],
    };
    values
        .iter()
        .map(|v| {
            hex::decode(v.as_str().unwrap())
                .unwrap()
                .try_into()
                .unwrap()
        })
        .collect()
}

/// A valid receiver other than [`receiver`].
fn other_receiver() -> ReceiverBytes {
    pinned("other")[0]
}

#[test]
fn provider_store_keeps_latest_times_and_never_rewinds_its_cursor() {
    use receiver_store::PayoutStore;
    let dir = tempfile::tempdir().unwrap();
    let mut store = PayoutStore::open(dir.path().join("providers.sqlite")).unwrap();
    let payout = receiver();
    assert_eq!(store.cursor("near-payout-audit").unwrap(), None);
    assert_eq!(store.read("near-payout-audit").unwrap(), None);
    // A read that saw no completion.
    let read = |store: &mut PayoutStore, cursor, read_at| {
        store
            .record("near-payout-audit", &[], cursor, read_at)
            .unwrap()
    };
    store
        .record("near-payout-audit", &[(payout, [1; 32], 300)], 100, 300)
        .unwrap();
    read(&mut store, 90, 250);
    // A read behind the cursor advances neither the cursor nor the read time, even
    // one that began later.
    read(&mut store, 90, 400);
    assert_eq!(store.cursor("near-payout-audit").unwrap(), Some(100));
    assert_eq!(store.read("near-payout-audit").unwrap(), Some(300));
    // One that reaches the cursor advances the read time, never back.
    read(&mut store, 100, 350);
    read(&mut store, 100, 320);
    assert_eq!(store.read("near-payout-audit").unwrap(), Some(350));
    // One that advances the cursor sets its own read time, even an earlier one.
    read(&mut store, 110, 340);
    assert_eq!(store.cursor("near-payout-audit").unwrap(), Some(110));
    assert_eq!(store.read("near-payout-audit").unwrap(), Some(340));
    // A payout keeps the earliest time a read saw it complete, and a reused receiver
    // has one per transaction.
    store
        .record(
            "near-payout-audit",
            &[(payout, [1; 32], 500), (payout, [2; 32], 500)],
            110,
            500,
        )
        .unwrap();
    assert_eq!(store.payouts(450).unwrap(), [(payout, [1; 32])]);
    let both = [(payout, [1; 32]), (payout, [2; 32])];
    assert_eq!(store.payouts(600).unwrap(), both);
    // An earlier read committing after a later one lowers the payout to its time,
    // without moving the cursor and read time; a still later read cannot raise it.
    let saw = |store: &mut PayoutStore, read_at| {
        store
            .record(
                "near-payout-audit",
                &[(payout, [2; 32], read_at)],
                110,
                read_at,
            )
            .unwrap()
    };
    saw(&mut store, 300);
    assert_eq!(store.cursor("near-payout-audit").unwrap(), Some(110));
    assert_eq!(store.read("near-payout-audit").unwrap(), Some(500));
    saw(&mut store, 700);
    assert_eq!(store.payouts(400).unwrap(), both);
    assert!(store.payouts(299).unwrap().is_empty());
    // Reopening keeps everything.
    drop(store);
    let store = PayoutStore::open(dir.path().join("providers.sqlite")).unwrap();
    assert_eq!(store.payouts(400).unwrap(), both);
    assert_eq!(store.read("near-payout-audit").unwrap(), Some(700));
}

/// A read that fails partway, here on its completions, records nothing: neither its
/// receivers, nor its cursor and read time.
#[test]
fn a_failed_provider_read_records_nothing() {
    use receiver_store::PayoutStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("providers.sqlite");
    let mut store = PayoutStore::open(&path).unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fail BEFORE INSERT ON payouts
             BEGIN SELECT RAISE(ABORT, 'forced'); END;",
        )
        .unwrap();
    let payout = receiver();
    assert!(store
        .record("near-payout-audit", &[(payout, [1; 32], 300)], 100, 300)
        .is_err());
    drop(store);
    let store = PayoutStore::open(&path).unwrap();
    assert_eq!(store.cursor("near-payout-audit").unwrap(), None);
    assert_eq!(store.read("near-payout-audit").unwrap(), None);
    assert!(store.payouts(i64::MAX).unwrap().is_empty());
}

/// The previous release's audit store survives this release and its restart: the
/// previous release's own statements still read and update its feed, with its start
/// and cursor untouched, and its global receivers afterwards, as after a rollback, while
/// this release keeps its own feed's cursor, read time and payouts. A store this
/// release creates takes them too.
#[test]
fn the_previous_release_reopens_the_audit_store_after_this_one() {
    use receiver_store::PayoutStore;
    // The previous release's schema and feed statement, verbatim.
    const OPEN: &str =
        "CREATE TABLE IF NOT EXISTS receivers (receiver BLOB NOT NULL CHECK(length(receiver)=43),
            payout INTEGER NOT NULL CHECK(payout IN (0,1)), seen_at INTEGER NOT NULL,
            PRIMARY KEY(receiver,payout)) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS feeds (feed TEXT PRIMARY KEY, started_at INTEGER,
            cursor INTEGER NOT NULL, read_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS payouts (receiver BLOB NOT NULL CHECK(length(receiver)=43),
            txid BLOB NOT NULL CHECK(length(txid)=32), seen_at INTEGER NOT NULL,
            PRIMARY KEY(receiver,txid)) WITHOUT ROWID;";
    const RECORD: &str = "INSERT INTO feeds VALUES (?1,?2,?3,?4) ON CONFLICT(feed)
        DO UPDATE SET cursor=excluded.cursor, read_at=excluded.read_at
        WHERE excluded.cursor>cursor OR (excluded.cursor=cursor AND excluded.read_at>read_at)";
    /// Opens `path` as the previous release does and records one read of `feed`.
    fn previous(path: &std::path::Path, feed: &str, since: Option<i64>, cursor: i64, read_at: i64) {
        let db = rusqlite::Connection::open(path).unwrap();
        db.execute_batch(OPEN).unwrap();
        db.execute(RECORD, rusqlite::params![feed, since, cursor, read_at])
            .unwrap();
        db.execute(
            "INSERT INTO receivers VALUES (?1,1,?2) ON CONFLICT(receiver,payout)
             DO UPDATE SET seen_at=MAX(seen_at,excluded.seen_at)",
            rusqlite::params![receiver().as_slice(), read_at],
        )
        .unwrap();
    }
    /// The previous release's reads: the feed's start and how many receivers it holds.
    fn previous_reads(path: &std::path::Path, feed: &str) -> (Option<i64>, i64) {
        let db = rusqlite::Connection::open(path).unwrap();
        let started = db
            .query_row("SELECT started_at FROM feeds WHERE feed=?1", [feed], |r| {
                r.get(0)
            })
            .unwrap();
        let receivers = db
            .query_row("SELECT COUNT(*) FROM receivers", [], |r| r.get(0))
            .unwrap();
        (started, receivers)
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("providers.sqlite");
    previous(&path, "near-payouts", Some(40), 100, 300);
    let mut store = PayoutStore::open(&path).unwrap();
    store
        .record("near-payout-audit", &[(receiver(), [1; 32], 400)], 110, 400)
        .unwrap();
    drop(store);
    let store = PayoutStore::open(&path).unwrap();
    assert_eq!(store.cursor("near-payout-audit").unwrap(), Some(110));
    assert_eq!(store.read("near-payout-audit").unwrap(), Some(400));
    assert_eq!(store.payouts(400).unwrap(), [(receiver(), [1; 32])]);
    assert_eq!(store.cursor("near-payouts").unwrap(), Some(100));
    drop(store);
    // Rolled back: the previous release reopens the file and reads on.
    assert_eq!(previous_reads(&path, "near-payouts"), (Some(40), 1));
    previous(&path, "near-payouts", None, 120, 500);
    assert_eq!(previous_reads(&path, "near-payouts"), (Some(40), 1));
    let store = PayoutStore::open(&path).unwrap();
    assert_eq!(store.cursor("near-payouts").unwrap(), Some(120));
    assert_eq!(store.cursor("near-payout-audit").unwrap(), Some(110));
    assert_eq!(store.payouts(400).unwrap(), [(receiver(), [1; 32])]);
    // A store this release created accepts the previous release's statements.
    let fresh = dir.path().join("fresh.sqlite");
    PayoutStore::open(&fresh)
        .unwrap()
        .record("near-payout-audit", &[], 100, 300)
        .unwrap();
    previous(&fresh, "near-payouts", Some(50), 100, 300);
    assert_eq!(previous_reads(&fresh, "near-payouts"), (Some(50), 1));
}

/// Every query in one view sees the same state, even when a read commits between them.
#[test]
fn a_provider_view_reads_one_state() {
    use receiver_store::PayoutStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("providers.sqlite");
    let mut store = PayoutStore::open(&path).unwrap();
    store.record("near-payout-audit", &[], 100, 300).unwrap();
    let mut writer = PayoutStore::open(&path).unwrap();
    let reads = store
        .view(|view| -> Result<_, receiver_store::Error> {
            let before = view.read("near-payout-audit")?;
            writer.record("near-payout-audit", &[], 200, 400).unwrap();
            Ok((before, view.read("near-payout-audit")?))
        })
        .unwrap();
    assert_eq!(reads, (Some(300), Some(300)));
    assert_eq!(store.read("near-payout-audit").unwrap(), Some(400));
}

#[test]
fn durable_coverage_atomic_failure_and_reorg() {
    use receiver_store::{Config, IndexedBlock, Store};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("directory.sqlite");
    let config = Config {
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position: 200,
    };
    let mut store = Store::open(&path, config.clone()).unwrap();
    assert!(store.snapshot(8, 0).is_err());
    let empty = IndexedBlock {
        height: 100,
        hash: [10; 32],
        parent: [2; 32],
        start_position: 200,
        end_position: 200,
        coinbase_actions: 0,
        payments: vec![],
        commitments: vec![],
    };
    store.append(&empty).unwrap();
    assert_eq!(store.snapshot(8, 0).unwrap().manifest.end_height, 100);
    let mut r = record(0, 1);
    r.payment.position = 202;
    let mut block = IndexedBlock {
        height: 101,
        hash: [3; 32],
        parent: [10; 32],
        start_position: 200,
        end_position: 204,
        coinbase_actions: 2,
        payments: vec![(r.receiver, r.payment.clone())],
        commitments: vec![[0; 32], [0; 32], r.payment.cmx, [0; 32]],
    };
    // The first two positions belong to excluded coinbase outputs.
    store.append(&block).unwrap();
    drop(store);
    let mut store = Store::open(&path, config.clone()).unwrap();
    assert_eq!(store.tip().unwrap().position, 204);
    assert_eq!(store.counts().unwrap(), (1, 2));
    let old = store.snapshot(8, 0).unwrap();
    assert_eq!(
        lookup_row(&old.manifest, &r.receiver, 0, row(&old, &r.receiver, 0))
            .unwrap()
            .unwrap()
            .payment
            .position,
        202
    );
    block.height = 102;
    block.parent = [3; 32];
    block.hash = [4; 32];
    block.start_position = 204;
    block.end_position = 208;
    // Failure after inserting the block must roll back both the block and its records.
    assert!(store.append(&block).is_err());
    assert_eq!(store.tip().unwrap().height, 101);
    assert!(store.rewind(100, [99; 32]).is_err());
    store.rewind(100, [10; 32]).unwrap();
    assert_eq!(store.counts().unwrap(), (0, 0));
    block.height = 101;
    block.parent = [10; 32];
    block.start_position = 200;
    block.end_position = 203;
    block.commitments.truncate(3);
    block.coinbase_actions = 0;
    block.payments[0].1.block_hash = block.hash;
    store.append(&block).unwrap();
    let new = store.snapshot(8, 0).unwrap();
    assert_ne!(
        old.manifest.revision().unwrap(),
        new.manifest.revision().unwrap()
    );
    assert!(old.manifest.accept([1; 32], 100, 101, block.hash).is_err());
    let mut wrong = config.clone();
    wrong.genesis = [99; 32];
    assert!(Store::open(&path, wrong).is_err());
    store.rewind(99, config.start_parent).unwrap();
    assert!(store.snapshot(8, 0).is_err());
}

/// A payment in the coinbase's leading positions or in transaction zero is refused.
#[test]
fn coinbase_payments_are_refused() {
    use receiver_store::{Config, IndexedBlock, Store};
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position: 200,
    };
    let mut store = Store::open(dir.path().join("directory.sqlite"), config).unwrap();
    let block = |position, tx_index| {
        let mut payment = record(0, 1).payment;
        payment.height = 100;
        payment.position = position;
        payment.tx_index = tx_index;
        IndexedBlock {
            height: 100,
            hash: [3; 32],
            parent: [2; 32],
            start_position: 200,
            end_position: 203,
            coinbase_actions: 2,
            commitments: vec![payment.cmx; 3],
            payments: vec![(receiver(), payment)],
        }
    };
    assert!(store.append(&block(201, 1)).is_err());
    assert!(store.append(&block(202, 0)).is_err());
    store.append(&block(202, 1)).unwrap();
    assert_eq!(store.counts().unwrap(), (1, 2));
}

/// A block whose payments are out of chain order or give one transaction two txids is
/// not appended: here a receiver's later position in an earlier transaction or with
/// another txid in the same one, and another receiver's later position in an earlier
/// transaction or Action.
#[test]
fn unsnapshottable_payments_are_refused() {
    use receiver_store::{Config, IndexedBlock, Store};
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position: 200,
    };
    let mut store = Store::open(dir.path().join("directory.sqlite"), config).unwrap();
    let block = |payments: Vec<RawRecord>| IndexedBlock {
        height: 100,
        hash: [3; 32],
        parent: [2; 32],
        start_position: 200,
        end_position: 202,
        coinbase_actions: 0,
        commitments: vec![[6; 32]; 2],
        payments: payments
            .into_iter()
            .map(|mut r| {
                r.payment.height = 100;
                (r.receiver, r.payment)
            })
            .collect(),
    };
    let (mut first, second) = (record(0, 2), record(1, 2));
    (first.payment.tx_index, first.payment.action_index) = (3, 1);
    let mut other_txid = second.clone();
    (other_txid.payment.tx_index, other_txid.payment.action_index) = (3, 2);
    let mut earlier_tx = second.clone();
    earlier_tx.receiver = other_receiver();
    let mut earlier_action = earlier_tx.clone();
    (earlier_action.payment.txid, earlier_action.payment.tx_index) = (first.payment.txid, 3);
    for bad in [second.clone(), other_txid, earlier_tx, earlier_action] {
        assert!(matches!(
            store.append(&block(vec![first.clone(), bad])),
            Err(Error::Malformed)
        ));
        assert_eq!(store.tip().unwrap().height, 99);
        assert_eq!(store.counts().unwrap(), (0, 0));
    }
    first.payment.tx_index = 1;
    store.append(&block(vec![first, second])).unwrap();
    assert_eq!(store.counts().unwrap(), (2, 0));
}

/// A store's positions stay within the note commitment tree, so every tip it reaches
/// can be published: a start or block past `TREE_SIZE` is refused, and one ending
/// exactly at it is valid.
#[test]
fn store_positions_end_within_the_tree() {
    use receiver_store::{Config, IndexedBlock, Store};
    use zakura_pir_publisher::receiver::snapshot::TREE_SIZE;
    let dir = tempfile::tempdir().unwrap();
    let config = |start_position| Config {
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position,
    };
    assert!(matches!(
        Store::open(dir.path().join("over.sqlite"), config(TREE_SIZE + 1)),
        Err(Error::Malformed)
    ));
    let mut store = Store::open(dir.path().join("last.sqlite"), config(TREE_SIZE - 1)).unwrap();
    let crossing = IndexedBlock {
        height: 100,
        hash: [3; 32],
        parent: [2; 32],
        start_position: TREE_SIZE - 1,
        end_position: TREE_SIZE + 1,
        coinbase_actions: 0,
        payments: vec![],
        commitments: vec![[6; 32]; 2],
    };
    assert!(matches!(store.append(&crossing), Err(Error::Coverage)));
    // The last leaf fills the tree, which can still be published.
    store
        .append(&IndexedBlock {
            end_position: TREE_SIZE,
            commitments: vec![[6; 32]],
            ..crossing
        })
        .unwrap();
    let manifest = store.snapshot(8, 0).unwrap().manifest;
    assert_eq!(manifest.end_position, TREE_SIZE);
}

#[test]
fn crowded_buckets_retry_the_salt_before_growing() {
    use receiver_store::{Config, IndexedBlock, Store};
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position: 200,
    };
    let mut store = Store::open(dir.path().join("directory.sqlite"), config).unwrap();
    // Twenty pages in two rows overflow when fifteen share a row. The block hash is
    // the first salt, so choose one that crowds a row.
    let crowded = |hash: [u8; 32]| {
        let mut m = manifest(2);
        m.salt = hash;
        let first = (0..20)
            .filter(|&page| row_for(&m, &receiver(), page).unwrap() == 0)
            .count();
        !(6..=14).contains(&first)
    };
    let hash = (0..=255).map(|k| [k; 32]).find(|h| crowded(*h)).unwrap();
    let payments = (0..20)
        .map(|page| {
            let mut r = record(page, 20);
            r.payment.height = 100;
            r.payment.block_hash = hash;
            (r.receiver, r.payment)
        })
        .collect();
    store
        .append(&IndexedBlock {
            height: 100,
            hash,
            parent: [2; 32],
            start_position: 200,
            end_position: 220,
            coinbase_actions: 0,
            payments,
            commitments: vec![[6; 32]; 20],
        })
        .unwrap();
    let first = store.snapshot(2, 0).unwrap();
    assert_ne!(first.manifest.salt, hash);
    assert_eq!(first.manifest.records, 20);
    for page in 0..20 {
        let found = lookup_row(
            &first.manifest,
            &receiver(),
            page,
            row(&first, &receiver(), page),
        );
        assert_eq!(found.unwrap().unwrap().page, page);
    }
    // A rebuild of the same coverage picks the same salt.
    assert_eq!(
        store.snapshot(2, 0).unwrap().manifest.revision().unwrap(),
        first.manifest.revision().unwrap()
    );
}

/// Commitment ranges and payment positions follow appends, rewinds, reopening and
/// replacement blocks, and a range past the stored tip or larger than one Ironwood shard
/// is refused.
#[test]
fn commitment_ranges_follow_rewinds_reopen_and_replacement_blocks() {
    use receiver_store::{Config, IndexedBlock, Store, SHARD_LEAVES};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("directory.sqlite");
    let config = Config {
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position: 0,
    };
    let mut store = Store::open(&path, config.clone()).unwrap();
    let mut payment = record(0, 1).payment;
    (payment.height, payment.block_hash, payment.position) = (100, [3; 32], 2);
    let mut block = IndexedBlock {
        height: 100,
        hash: [3; 32],
        parent: [2; 32],
        start_position: 0,
        end_position: 3,
        coinbase_actions: 1,
        payments: vec![(receiver(), payment.clone())],
        commitments: vec![[1; 32], [2; 32], payment.cmx],
    };
    store.append(&block).unwrap();
    assert_eq!(store.commitments(0, 3).unwrap(), block.commitments);
    assert_eq!(store.commitments(1, 2).unwrap(), [[2; 32]]);
    assert_eq!(store.payment_positions(0, 3).unwrap(), [2]);
    assert!(store.commitments(0, 4).is_err());
    assert!(matches!(
        store.commitments(0, SHARD_LEAVES + 1),
        Err(Error::Capacity)
    ));
    (block.height, block.hash, block.parent) = (101, [4; 32], [3; 32]);
    (block.start_position, block.end_position) = (3, 5);
    block.commitments = vec![[3; 32], [4; 32]];
    block.payments.clear();
    store.append(&block).unwrap();
    assert_eq!(store.commitments(2, 5).unwrap()[1..], [[3; 32], [4; 32]]);
    store.rewind(100, [3; 32]).unwrap();
    assert!(store.commitments(0, 4).is_err());
    drop(store);
    let mut store = Store::open(&path, config).unwrap();
    block.hash = [5; 32];
    block.commitments[0] = [9; 32];
    store.append(&block).unwrap();
    assert_eq!(store.commitments(3, 5).unwrap(), [[9; 32], [4; 32]]);
}

/// Completed Ironwood shards are recorded in order with their final payloads, an
/// identical record is accepted again and a different root refused, and rewinding the
/// block that completed a shard removes it with its payloads.
#[test]
fn completed_shards_follow_their_completing_block() {
    use receiver_store::{Config, IndexedBlock, Store, SHARD_LEAVES};
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position: 0,
    };
    let mut store = Store::open(dir.path().join("directory.sqlite"), config).unwrap();
    let mut payment = record(0, 1).payment;
    (payment.height, payment.block_hash, payment.position) = (100, [3; 32], 7);
    let mut commitments = vec![[1; 32]; SHARD_LEAVES as usize - 1];
    commitments[7] = payment.cmx;
    store
        .append(&IndexedBlock {
            height: 100,
            hash: [3; 32],
            parent: [2; 32],
            start_position: 0,
            end_position: SHARD_LEAVES - 1,
            coinbase_actions: 0,
            payments: vec![(receiver(), payment)],
            commitments,
        })
        .unwrap();
    // The shard's last leaf is not stored yet.
    assert!(store.complete_shard(0, [8; 32], &[]).is_err());
    store
        .append(&IndexedBlock {
            height: 101,
            hash: [4; 32],
            parent: [3; 32],
            start_position: SHARD_LEAVES - 1,
            end_position: SHARD_LEAVES + 1,
            coinbase_actions: 0,
            payments: vec![],
            commitments: vec![[1; 32]; 2],
        })
        .unwrap();
    assert!(store.complete_shard(1, [8; 32], &[]).is_err());
    store.complete_shard(0, [8; 32], &[(7, [6; 512])]).unwrap();
    store.complete_shard(0, [8; 32], &[(7, [6; 512])]).unwrap();
    assert!(store.complete_shard(0, [9; 32], &[]).is_err());
    assert!(store
        .complete_shard(0, [8; 32], &[(SHARD_LEAVES, [6; 512])])
        .is_err());
    assert_eq!(store.completed_shards().unwrap(), 1);
    let shard = store.completed_shard(0).unwrap().unwrap();
    assert_eq!(
        (shard.root, shard.height, shard.hash),
        ([8; 32], 101, [4; 32])
    );
    assert_eq!(store.payloads(&[7, 8]).unwrap(), [Some([6; 512]), None]);
    store.rewind(100, [3; 32]).unwrap();
    assert_eq!(store.completed_shards().unwrap(), 0);
    assert_eq!(store.payloads(&[7]).unwrap(), [None]);
}

/// A correction replaces a stored block's payments under a higher revision, dropping
/// the replaced payments' final payloads; a block that differs from the stored one, a
/// revision that does not advance or an invalid payment changes nothing.
#[test]
fn corrections_replace_payments_on_an_unchanged_chain() {
    use receiver_store::{Config, IndexedBlock, Store};
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position: 0,
    };
    let mut store = Store::open(dir.path().join("directory.sqlite"), config).unwrap();
    let mut payment = record(0, 1).payment;
    (payment.height, payment.block_hash, payment.position) = (100, [3; 32], 1);
    let mut block = IndexedBlock {
        height: 100,
        hash: [3; 32],
        parent: [2; 32],
        start_position: 0,
        end_position: 3,
        coinbase_actions: 1,
        payments: vec![],
        commitments: vec![[1; 32], payment.cmx, payment.cmx],
    };
    store.append(&block).unwrap();
    assert_eq!(store.snapshot(8, 0).unwrap().manifest.records, 0);
    block.payments = vec![(receiver(), payment.clone())];
    assert!(store.correct(0, std::slice::from_ref(&block)).is_err());
    let mut changed = block.clone();
    changed.commitments[0] = [9; 32];
    assert!(store.correct(1, &[changed]).is_err());
    let mut coinbase = block.clone();
    coinbase.payments[0].1.position = 0;
    assert!(store.correct(1, &[coinbase]).is_err());
    assert_eq!(store.history_revision().unwrap(), 0);
    store.correct(1, std::slice::from_ref(&block)).unwrap();
    assert_eq!(store.history_revision().unwrap(), 1);
    assert!(store.snapshot(8, 0).is_err());
    assert_eq!(store.snapshot(8, 1).unwrap().manifest.records, 1);
    let mut second = payment.clone();
    (second.position, second.tx_index, second.txid) = (2, 2, [2; 32]);
    block.payments.push((receiver(), second));
    store.correct(2, &[block]).unwrap();
    assert_eq!(store.payment_positions(0, 3).unwrap(), [1, 2]);
}

/// A history with more payments than the table has slots fails with
/// [`Error::Capacity`], and builds at a larger size.
#[test]
fn a_history_beyond_the_slots_is_a_capacity_failure() {
    use receiver_store::{Config, IndexedBlock, Store};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("directory.sqlite");
    let config = Config {
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position: 200,
    };
    let mut store = Store::open(&path, config).unwrap();
    let n = SLOTS as u32 + 1;
    let payments = (0..n)
        .map(|page| {
            let mut r = record(page, n);
            r.payment.height = 100;
            (r.receiver, r.payment)
        })
        .collect();
    store
        .append(&IndexedBlock {
            height: 100,
            hash: [3; 32],
            parent: [2; 32],
            start_position: 200,
            end_position: 200 + u64::from(n),
            coinbase_actions: 0,
            payments,
            commitments: vec![[6; 32]; n as usize],
        })
        .unwrap();
    assert!(matches!(store.snapshot(1, 0), Err(Error::Capacity)));
    assert_eq!(store.snapshot(4, 0).unwrap().manifest.records, u64::from(n));
}

/// Sealed shards are recorded contiguously from the configured start through stored
/// blocks; a rewind past a shard's end removes it, and so does a correction at or before
/// its end, while earlier shards stay.
#[test]
fn sealed_shards_follow_rewinds_and_corrections() {
    use receiver_store::{Config, IndexedBlock, SealedShard, Store};
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(
        dir.path().join("directory.sqlite"),
        Config {
            genesis: [1; 32],
            start_height: 100,
            start_parent: [2; 32],
            start_position: 0,
        },
    )
    .unwrap();
    let block = |height: u32| IndexedBlock {
        height,
        hash: [height as u8; 32],
        parent: [if height == 100 { 2 } else { height as u8 - 1 }; 32],
        start_position: 0,
        end_position: 0,
        coinbase_actions: 0,
        payments: vec![],
        commitments: vec![],
    };
    for height in 100..104 {
        store.append(&block(height)).unwrap();
    }
    let shard = |start, end| SealedShard {
        start_height: start,
        end_height: end,
        segments: 1,
        rows: 8,
        payloads: 64,
    };
    assert!(store.seal(&shard(101, 101)).is_err());
    assert!(store.seal(&shard(100, 104)).is_err());
    store.seal(&shard(100, 100)).unwrap();
    store.seal(&shard(101, 102)).unwrap();
    assert!(store.seal(&shard(102, 103)).is_err());
    assert_eq!(
        store.sealed_shards().unwrap(),
        [shard(100, 100), shard(101, 102)]
    );
    store.rewind(101, [101; 32]).unwrap();
    assert_eq!(store.sealed_shards().unwrap(), [shard(100, 100)]);
    store.append(&block(102)).unwrap();
    store.seal(&shard(101, 102)).unwrap();
    store.correct(1, &[block(101)]).unwrap();
    assert_eq!(store.sealed_shards().unwrap(), [shard(100, 100)]);
    assert_eq!(store.records(100, 102).unwrap(), []);
}

/// Writes an earlier release's version 2 store at `legacy` holding one payment in block
/// 100, and returns its configuration and that payment's record.
fn legacy_store(
    legacy: &std::path::Path,
) -> (
    receiver_store::Config,
    zakura_pir_publisher::receiver::RawRecord,
) {
    let config = receiver_store::Config {
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position: 0,
    };
    let mut paid = record(0, 1);
    (
        paid.payment.height,
        paid.payment.block_hash,
        paid.payment.position,
    ) = (100, [3; 32], 0);
    let encoded = paid.encode().unwrap();
    let old = rusqlite::Connection::open(legacy).unwrap();
    old.execute_batch(
        "CREATE TABLE filter_revision (revision INTEGER NOT NULL);
        INSERT INTO filter_revision VALUES (0);
        CREATE TABLE config (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL);
        CREATE TABLE blocks (height INTEGER PRIMARY KEY, hash BLOB NOT NULL, end_position INTEGER NOT NULL, coinbase_actions INTEGER NOT NULL);
        CREATE TABLE payments (height INTEGER NOT NULL REFERENCES blocks(height) ON DELETE CASCADE,
            receiver BLOB NOT NULL, position INTEGER NOT NULL UNIQUE, txid BLOB NOT NULL, action INTEGER NOT NULL, record BLOB NOT NULL,
            UNIQUE(txid,action));
        CREATE TABLE commitments (position INTEGER PRIMARY KEY, height INTEGER NOT NULL REFERENCES blocks(height) ON DELETE CASCADE, cmx BLOB NOT NULL CHECK(length(cmx)=32));
        PRAGMA user_version=2;",
    )
    .unwrap();
    old.execute(
        "INSERT INTO config VALUES (1,?1)",
        [serde_json::to_string(&config).unwrap()],
    )
    .unwrap();
    old.execute(
        "INSERT INTO blocks VALUES (100,?1,1,0)",
        [[3u8; 32].as_slice()],
    )
    .unwrap();
    old.execute(
        "INSERT INTO commitments VALUES (0,100,?1)",
        [paid.payment.cmx.as_slice()],
    )
    .unwrap();
    old.execute(
        "INSERT INTO payments VALUES (100,?1,0,?2,0,?3)",
        rusqlite::params![
            paid.receiver.as_slice(),
            paid.payment.txid.as_slice(),
            &encoded[..285]
        ],
    )
    .unwrap();
    drop(old);
    (config, paid)
}

/// A correction or any other entry point opening a data directory that holds only a
/// version 2 store converts it first, rather than starting an empty version 3 store
/// beside it, and one that may not create a store refuses an empty directory.
#[test]
fn opening_a_data_directory_converts_its_version_2_store_first() {
    use receiver_store::open_in;
    let dir = tempfile::tempdir().unwrap();
    let (config, _) = legacy_store(&dir.path().join(receiver_store::LEGACY_STORE_FILE));
    let (store, converted) = open_in(dir.path(), config.clone(), false).unwrap();
    assert!(converted);
    assert_eq!(store.tip().unwrap().height, 100);
    drop(store);
    let (_, converted) = open_in(dir.path(), config.clone(), false).unwrap();
    assert!(!converted);
    let empty = tempfile::tempdir().unwrap();
    assert!(matches!(
        open_in(empty.path(), config, false),
        Err(Error::Coverage)
    ));
    assert!(!empty.path().join(receiver_store::STORE_FILE).exists());
}

/// An earlier release's version 2 store, with 285-byte records, is refused in place,
/// copied once into a version 3 store whose records gain a zero ordinal, and left
/// unchanged for a rollback; a leftover from an interrupted copy is redone, and a later
/// start keeps the converted store.
#[test]
fn a_version_2_store_converts_once_and_stays_for_rollback() {
    use receiver_store::{upgrade, Store};
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join(receiver_store::LEGACY_STORE_FILE);
    let path = dir.path().join(receiver_store::STORE_FILE);
    let (config, paid) = legacy_store(&legacy);
    let legacy_bytes = |path: &std::path::Path| {
        let db = rusqlite::Connection::open(path).unwrap();
        let version: u32 = db
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        let length: usize = db
            .query_row("SELECT length(record) FROM payments", [], |r| r.get(0))
            .unwrap();
        (version, length)
    };
    assert!(matches!(
        Store::open(&legacy, config.clone()),
        Err(Error::Malformed)
    ));
    assert_eq!(legacy_bytes(&legacy), (2, 285));
    // A copy interrupted before its rename is discarded and redone.
    std::fs::write(path.with_extension("upgrading"), b"partial").unwrap();
    assert!(upgrade(&legacy, &path).unwrap());
    assert_eq!(legacy_bytes(&legacy), (2, 285));
    assert_eq!(
        legacy_bytes(&path),
        (3, zakura_pir_publisher::receiver::RECORD_BYTES)
    );
    let mut store = Store::open(&path, config.clone()).unwrap();
    let snapshot = store.snapshot(8, 0).unwrap();
    assert_eq!(
        lookup_row(
            &snapshot.manifest,
            &paid.receiver,
            0,
            row(&snapshot, &paid.receiver, 0)
        )
        .unwrap()
        .unwrap(),
        paid
    );
    drop(store);
    assert!(!upgrade(&legacy, &path).unwrap());
    Store::open(&path, config).unwrap();
}
