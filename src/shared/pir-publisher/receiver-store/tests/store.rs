mod common;
use common::{manifest, receiver, record};
use receiver_directory::{
    snapshot::{lookup_row, row_for, Snapshot, ROW_BYTES, SLOTS},
    RawRecord, ReceiverBytes,
};
use receiver_store::Error;

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
    use receiver_store::ProviderStore;
    let dir = tempfile::tempdir().unwrap();
    let mut store = ProviderStore::open(dir.path().join("providers.sqlite")).unwrap();
    let (payout, refund) = (receiver(), other_receiver());
    assert_eq!(store.cursor("near-payouts").unwrap(), None);
    assert_eq!(store.read("near-payouts").unwrap(), None);
    assert_eq!(store.started("near-payouts").unwrap(), None);
    // A read that saw no completion.
    let read = |store: &mut ProviderStore, cursor, read_at| {
        store
            .record("near-payouts", None, &[], &[], cursor, read_at)
            .unwrap()
    };
    store
        .record(
            "near-payouts",
            Some(40),
            &[(payout, true, 100), (payout, true, 50)],
            &[(payout, [1; 32], 300)],
            100,
            300,
        )
        .unwrap();
    store
        .record(
            "near-refunds",
            Some(40),
            &[(refund, false, 200)],
            &[],
            200,
            300,
        )
        .unwrap();
    read(&mut store, 90, 250);
    // A read behind the cursor advances neither the cursor nor the read time, even
    // one that began later.
    read(&mut store, 90, 400);
    assert_eq!(store.cursor("near-payouts").unwrap(), Some(100));
    assert_eq!(store.read("near-payouts").unwrap(), Some(300));
    // One that reaches the cursor advances the read time, never back.
    read(&mut store, 100, 350);
    read(&mut store, 100, 320);
    assert_eq!(store.read("near-payouts").unwrap(), Some(350));
    // One that advances the cursor sets its own read time, even an earlier one.
    read(&mut store, 110, 340);
    assert_eq!(store.cursor("near-payouts").unwrap(), Some(110));
    assert_eq!(store.read("near-payouts").unwrap(), Some(340));
    // Recent holds the refund given since 150, and seen every receiver, payout or refund.
    let (recent, mut seen) = store.sets(150).unwrap();
    seen.sort();
    let mut every = vec![payout, refund];
    every.sort();
    assert_eq!((recent, seen), (vec![refund], every));
    // The feed's start is its initial read's; see the test below.
    assert_eq!(store.started("near-payouts").unwrap(), Some(40));
    // A payout keeps the earliest time a read saw it complete, and a reused receiver
    // has one per transaction.
    store
        .record(
            "near-payouts",
            None,
            &[],
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
    let saw = |store: &mut ProviderStore, read_at| {
        store
            .record(
                "near-payouts",
                None,
                &[],
                &[(payout, [2; 32], read_at)],
                110,
                read_at,
            )
            .unwrap()
    };
    saw(&mut store, 300);
    assert_eq!(store.cursor("near-payouts").unwrap(), Some(110));
    assert_eq!(store.read("near-payouts").unwrap(), Some(500));
    saw(&mut store, 700);
    assert_eq!(store.payouts(400).unwrap(), both);
    assert!(store.payouts(299).unwrap().is_empty());
    // Reopening keeps everything.
    drop(store);
    let store = ProviderStore::open(dir.path().join("providers.sqlite")).unwrap();
    assert_eq!(store.sets(100).unwrap().0.len(), 2);
    assert_eq!(store.payouts(400).unwrap(), both);
    assert_eq!(store.read("near-payouts").unwrap(), Some(700));
}

/// Only a feed's first read sets its start. A read that followed the cursor, before
/// or after a reopen, passes no start and never moves it.
#[test]
fn only_a_first_read_sets_a_feed_start() {
    use receiver_store::ProviderStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("providers.sqlite");
    let mut store = ProviderStore::open(&path).unwrap();
    // Initial reads, empty and nonempty, start their feeds.
    store
        .record("empty", Some(500), &[], &[], 500, 600)
        .unwrap();
    store
        .record("full", Some(500), &[(receiver(), true, 550)], &[], 550, 600)
        .unwrap();
    assert_eq!(store.started("empty").unwrap(), Some(500));
    assert_eq!(store.started("full").unwrap(), Some(500));
    // Reads behind, at and past the cursor leave the start alone, and a cursor-based
    // read never creates one.
    for cursor in [540, 550, 560] {
        store.record("full", None, &[], &[], cursor, 700).unwrap();
        assert_eq!(store.started("full").unwrap(), Some(500));
    }
    store.record("unstarted", None, &[], &[], 10, 20).unwrap();
    assert_eq!(store.started("unstarted").unwrap(), None);
    // After a reopen, as on a restart with another `--near-since`.
    drop(store);
    let mut store = ProviderStore::open(&path).unwrap();
    for cursor in [300, 900] {
        store.record("full", None, &[], &[], cursor, 1_000).unwrap();
        assert_eq!(store.started("full").unwrap(), Some(500));
    }
}

/// A read that fails partway, here on its completions, records nothing: neither its
/// receivers, nor its cursor and read time, nor the feed's start.
#[test]
fn a_failed_provider_read_records_nothing() {
    use receiver_store::ProviderStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("providers.sqlite");
    let mut store = ProviderStore::open(&path).unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fail BEFORE INSERT ON payouts
             BEGIN SELECT RAISE(ABORT, 'forced'); END;",
        )
        .unwrap();
    let payout = receiver();
    assert!(store
        .record(
            "near-payouts",
            Some(40),
            &[(payout, true, 100)],
            &[(payout, [1; 32], 300)],
            100,
            300
        )
        .is_err());
    drop(store);
    let store = ProviderStore::open(&path).unwrap();
    assert_eq!(store.sets(0).unwrap(), (vec![], vec![]));
    assert_eq!(store.cursor("near-payouts").unwrap(), None);
    assert_eq!(store.read("near-payouts").unwrap(), None);
    assert_eq!(store.started("near-payouts").unwrap(), None);
    assert!(store.payouts(i64::MAX).unwrap().is_empty());
}

/// Every query in one view sees the same state, even when a read commits between them.
#[test]
fn a_provider_view_reads_one_state() {
    use receiver_store::ProviderStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("providers.sqlite");
    let mut store = ProviderStore::open(&path).unwrap();
    store
        .record("near-payouts", Some(40), &[], &[], 100, 300)
        .unwrap();
    let mut writer = ProviderStore::open(&path).unwrap();
    let reads = store
        .view(|view| -> Result<_, receiver_store::Error> {
            let before = view.read("near-payouts")?;
            writer
                .record("near-payouts", None, &[], &[], 200, 400)
                .unwrap();
            Ok((before, view.read("near-payouts")?))
        })
        .unwrap();
    assert_eq!(reads, (Some(300), Some(300)));
    assert_eq!(store.read("near-payouts").unwrap(), Some(400));
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
    assert!(store.snapshot(8, &[]).is_err());
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
    assert_eq!(store.snapshot(8, &[]).unwrap().manifest.end_height, 100);
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
    let old = store.snapshot(8, &[]).unwrap();
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
    let new = store.snapshot(8, &[]).unwrap();
    assert_ne!(
        old.manifest.revision().unwrap(),
        new.manifest.revision().unwrap()
    );
    assert!(old.manifest.accept([1; 32], 100, 101, block.hash).is_err());
    let mut wrong = config.clone();
    wrong.genesis = [99; 32];
    assert!(Store::open(&path, wrong).is_err());
    store.rewind(99, config.start_parent).unwrap();
    assert!(store.snapshot(8, &[]).is_err());
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
    use receiver_directory::snapshot::TREE_SIZE;
    use receiver_store::{Config, IndexedBlock, Store};
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
    let manifest = store.snapshot(8, &[]).unwrap().manifest;
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
    let first = store.snapshot(2, &[]).unwrap();
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
        store.snapshot(2, &[]).unwrap().manifest.revision().unwrap(),
        first.manifest.revision().unwrap()
    );
}

/// A publication's commitments and payment positions come from one database view and
/// follow appends, rewinds, reopening and replacement blocks; the caller's commitment
/// limit admits exactly the history's length.
#[test]
fn store_witness_inputs_follow_rewinds_reopen_and_replacement_blocks() {
    use receiver_directory::snapshot::TREE_SIZE;
    use receiver_store::{Config, IndexedBlock, Store, WitnessInputs};
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
    payment.height = 100;
    payment.block_hash = [3; 32];
    payment.position = 2;
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
    let first = store.snapshot(8, &[]).unwrap();
    let expected = WitnessInputs {
        commitments: vec![[1; 32], [2; 32], payment.cmx],
        positions: [2].into_iter().collect(),
    };
    let len = first.manifest.end_position;
    assert_eq!(
        store.witness_inputs(&first.manifest, len).unwrap(),
        expected
    );
    assert!(matches!(
        store.witness_inputs(&first.manifest, len - 1),
        Err(Error::Capacity)
    ));
    block.height = 101;
    block.hash = [4; 32];
    block.parent = [3; 32];
    block.start_position = 3;
    block.end_position = 5;
    block.commitments = vec![[3; 32], [4; 32]];
    block.payments.clear();
    store.append(&block).unwrap();
    let second = store.snapshot(8, &[]).unwrap();
    let mut longer = expected.clone();
    longer.commitments.extend([[3; 32], [4; 32]]);
    assert_eq!(
        store.witness_inputs(&second.manifest, TREE_SIZE).unwrap(),
        longer
    );
    // The earlier publication still reads only its own history.
    assert_eq!(
        store.witness_inputs(&first.manifest, TREE_SIZE).unwrap(),
        expected
    );
    store.rewind(100, [3; 32]).unwrap();
    assert!(store.witness_inputs(&second.manifest, TREE_SIZE).is_err());
    assert_eq!(
        store.witness_inputs(&first.manifest, TREE_SIZE).unwrap(),
        expected
    );
    drop(store);
    let mut store = Store::open(&path, config).unwrap();
    block.hash = [5; 32];
    block.commitments[0] = [9; 32];
    store.append(&block).unwrap();
    let replacement = store.snapshot(8, &[]).unwrap();
    let mut replaced = expected.clone();
    replaced.commitments.extend([[9; 32], [4; 32]]);
    assert_eq!(
        store
            .witness_inputs(&replacement.manifest, TREE_SIZE)
            .unwrap(),
        replaced
    );
    assert!(store.witness_inputs(&second.manifest, TREE_SIZE).is_err());
    let mut wrong = replacement.manifest;
    wrong.end_position -= 1;
    assert!(store.witness_inputs(&wrong, TREE_SIZE).is_err());
}

/// Indexed blocks before the first commitment give an empty history, and the inputs
/// follow a commitment and a rewind back.
#[test]
fn store_witness_inputs_cover_empty_indexed_blocks() {
    use receiver_directory::snapshot::TREE_SIZE;
    use receiver_store::{Config, IndexedBlock, Store};
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position: 0,
    };
    let mut store = Store::open(dir.path().join("directory.sqlite"), config).unwrap();
    let mut block = IndexedBlock {
        height: 100,
        hash: [3; 32],
        parent: [2; 32],
        start_position: 0,
        end_position: 0,
        coinbase_actions: 0,
        payments: Vec::new(),
        commitments: Vec::new(),
    };
    store.append(&block).unwrap();
    (block.height, block.hash, block.parent) = (101, [4; 32], [3; 32]);
    store.append(&block).unwrap();
    let empty = store.snapshot(8, &[]).unwrap().manifest;
    assert_eq!((empty.records, empty.end_position), (0, 0));
    let inputs = store.witness_inputs(&empty, TREE_SIZE).unwrap();
    assert!(inputs.commitments.is_empty() && inputs.positions.is_empty());
    (block.height, block.hash, block.parent) = (102, [5; 32], [4; 32]);
    (block.end_position, block.commitments) = (1, vec![[1; 32]]);
    store.append(&block).unwrap();
    let one = store.snapshot(8, &[]).unwrap().manifest;
    assert_eq!(
        store.witness_inputs(&one, TREE_SIZE).unwrap().commitments,
        [[1; 32]]
    );
    store.rewind(101, [4; 32]).unwrap();
    assert_eq!(store.witness_inputs(&empty, TREE_SIZE).unwrap(), inputs);
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
    assert!(matches!(store.snapshot(1, &[]), Err(Error::Capacity)));
    assert_eq!(
        store.snapshot(4, &[]).unwrap().manifest.records,
        u64::from(n)
    );
}
