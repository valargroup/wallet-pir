mod common;
use common::{action, manifest, receiver, record};
use receiver_directory::{
    snapshot::{
        allow_small_tables, lookup_row, row_for, Snapshot, MAX_ROWS, MIN_ROWS, ROW_BYTES, SLOTS,
    },
    Error, Receiver, Record, RECORD_BYTES,
};

fn row<'a>(s: &'a Snapshot, r: &Receiver, page: u32) -> &'a [u8] {
    let n = row_for(&s.manifest, r, page).unwrap();
    &s.data[n * ROW_BYTES..(n + 1) * ROW_BYTES]
}

#[test]
fn public_refund_requires_authenticated_recovery() {
    assert!(action().recover_receiver().unwrap().is_some());
    let mut a = action();
    a.out_ciphertext[0] ^= 1;
    assert!(a.recover_receiver().unwrap().is_none());
    let mut a = action();
    a.enc_ciphertext[579] ^= 1;
    assert!(a.recover_receiver().unwrap().is_none());
    let mut a = action();
    a.cmx = [255; 32];
    assert!(a.recover_receiver().is_err());
}

#[test]
fn wire_layout_and_strict_empty_slots() {
    let r = record(0, 1);
    let b = r.encode().unwrap();
    assert_eq!(b.len(), 285);
    assert_eq!(&b[129..137], &200u64.to_le_bytes());
    assert_eq!(Record::decode(&b).unwrap(), Some(r));
    assert!(Record::decode(&[0; RECORD_BYTES]).unwrap().is_none());
    let mut corrupt = [0; RECORD_BYTES];
    corrupt[284] = 1;
    assert!(Record::decode(&corrupt).is_err());
    assert!(Record::decode(&b[..284]).is_err());
}

/// A manifest with a field this version does not know is refused, and the revision
/// binds every field, including each set's optional ones.
#[test]
fn manifests_refuse_unknown_fields_and_bind_every_field() {
    allow_small_tables();
    use receiver_directory::snapshot::Manifest;
    let s = Snapshot::build(manifest(8), &[record(0, 1)], &[]).unwrap();
    let json = serde_json::to_value(&s.manifest).unwrap();
    let read: Manifest = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(read.revision().unwrap(), s.manifest.revision().unwrap());
    let mut added = json.clone();
    added["added"] = 1.into();
    assert!(serde_json::from_value::<Manifest>(added).is_err());
    let mut nested = json;
    nested["filters"][0]["note"] = "later".into();
    assert!(serde_json::from_value::<Manifest>(nested).is_err());
    let mut other = s.manifest.clone();
    other.records += 1;
    assert_ne!(other.revision().unwrap(), s.manifest.revision().unwrap());
}

#[test]
fn pages_share_one_revision_and_build_order_is_stable() {
    allow_small_tables();
    let records = [record(0, 2), record(1, 2)];
    let s = Snapshot::build(manifest(8), &records, &[]).unwrap();
    let reversed =
        Snapshot::build(manifest(8), &[records[1].clone(), records[0].clone()], &[]).unwrap();
    assert_eq!(
        s.manifest.revision().unwrap(),
        reversed.manifest.revision().unwrap()
    );
    for r in &records {
        assert_eq!(
            lookup_row(
                &s.manifest,
                &r.receiver,
                r.page,
                row(&s, &r.receiver, r.page)
            )
            .unwrap(),
            Some(r.clone())
        );
    }
    let empty = Snapshot::build(manifest(8), &[], &[]).unwrap();
    assert!(lookup_row(
        &empty.manifest,
        &records[0].receiver,
        0,
        row(&empty, &records[0].receiver, 0)
    )
    .unwrap()
    .is_none());
    assert!(matches!(
        lookup_row(
            &empty.manifest,
            &records[0].receiver,
            1,
            row(&empty, &records[0].receiver, 1)
        ),
        Err(Error::MissingPage)
    ));
    assert!(Snapshot::build(manifest(8), &records[..1], &[]).is_err());
    assert!(Snapshot::build(manifest(8), &records[1..], &[]).is_err());
    assert!(Snapshot::build(manifest(8), &[record(0, 1), record(1, 2)], &[]).is_err());
    assert!(Snapshot::build(manifest(8), &[record(0, 1), record(0, 1)], &[]).is_err());
}

/// A valid receiver other than [`receiver`].
fn other_receiver() -> Receiver {
    let sk = orchard::keys::SpendingKey::from_bytes([3; 32]).unwrap();
    let fvk = orchard::keys::FullViewingKey::from(&sk);
    let address = fvk.address_at(0u32, orchard::keys::Scope::External);
    Receiver::from_bytes(address.to_raw_address_bytes()).unwrap()
}

#[test]
fn publications_commit_to_their_filters() {
    allow_small_tables();
    use receiver_directory::{
        filter::{Filters, PAID},
        snapshot::ProviderSet,
    };
    use sha2::{Digest, Sha256};
    let (paid, provider) = (receiver(), other_receiver());
    let sets = [
        ProviderSet {
            label: "near-intents/recent".into(),
            window_secs: Some(86_400),
            since_unix: 1_000,
            until_unix: 90_000,
            receivers: vec![provider],
        },
        ProviderSet {
            label: "near-intents/seen".into(),
            window_secs: None,
            since_unix: 1_000,
            until_unix: 90_000,
            receivers: vec![provider],
        },
    ];
    let s = Snapshot::build(manifest(8), &[record(0, 1)], &sets).unwrap();
    assert_eq!(
        <[u8; 32]>::from(Sha256::digest(&s.filters)),
        s.manifest.filters_sha256
    );
    let filters = Filters::decode(&s.filters).unwrap();
    s.manifest.check_filters(&filters).unwrap();
    let labels: Vec<_> = s
        .manifest
        .filters
        .iter()
        .map(|f| f.label.as_str())
        .collect();
    assert_eq!(labels, ["near-intents/recent", "near-intents/seen", PAID]);
    assert_eq!(s.manifest.filters[0].window_secs, Some(86_400));
    assert_eq!(s.manifest.filters[0].until_unix, Some(90_000));
    assert_eq!(s.manifest.filters[2].since_unix, None);
    assert_eq!(s.manifest.filters[2].until_unix, None);
    let key = s.manifest.salt;
    let set = |label| filters.get(label).unwrap().matches(&key, &[paid, provider]);
    assert_eq!(set(PAID), [true, false]);
    assert_eq!(set("near-intents/recent"), [false, true]);
    assert_eq!(set("near-intents/seen"), [false, true]);
    // The provider's data is part of the revision.
    let without = Snapshot::build(manifest(8), &[record(0, 1)], &[]).unwrap();
    assert_ne!(
        without.manifest.revision().unwrap(),
        s.manifest.revision().unwrap()
    );
    // A filter file is checked against the sets its manifest declares.
    let only_paid = Filters::decode(&without.filters).unwrap();
    assert!(s.manifest.check_filters(&only_paid).is_err());
    // A recent set must declare its window, labels must be known kinds, and a feed
    // cannot end before it started.
    for (label, window_secs, until_unix) in [
        ("near-intents/recent", None, 90_000),
        ("near-intents/other", None, 90_000),
        ("near-intents/seen", None, 999),
    ] {
        let bad = [ProviderSet {
            label: label.into(),
            window_secs,
            since_unix: 1_000,
            until_unix,
            receivers: vec![provider],
        }];
        assert!(Snapshot::build(manifest(8), &[record(0, 1)], &bad).is_err());
    }
}

#[cfg(feature = "store")]
#[test]
fn provider_store_keeps_latest_times_and_never_rewinds_its_cursor() {
    use receiver_directory::store::ProviderStore;
    let dir = tempfile::tempdir().unwrap();
    let mut store = ProviderStore::open(dir.path().join("provider.sqlite")).unwrap();
    let (payout, refund) = (receiver(), other_receiver());
    assert_eq!(store.cursor("near-payouts").unwrap(), None);
    assert_eq!(store.read("near-payouts").unwrap(), None);
    assert_eq!(store.started("near-payouts").unwrap(), None);
    // A read that matched no completion.
    let read = |store: &mut ProviderStore, cursor, read_at| {
        store
            .record("near-payouts", 60, &[], &[], cursor, read_at)
            .unwrap()
    };
    store
        .record(
            "near-payouts",
            40,
            &[(payout, true, 100), (payout, true, 50)],
            &[payout],
            100,
            300,
        )
        .unwrap();
    store
        .record("near-refunds", 40, &[(refund, false, 200)], &[], 200, 300)
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
    let (recent, seen) = store.sets(150).unwrap();
    assert_eq!((recent, seen), (vec![refund], vec![payout]));
    // A feed's start is its first read's.
    assert_eq!(store.started("near-payouts").unwrap(), Some(40));
    // A completion keeps when a read first saw it.
    store
        .record("near-payouts", 60, &[], &[payout, refund], 110, 500)
        .unwrap();
    assert_eq!(store.completed(0, 450).unwrap(), [payout]);
    assert_eq!(store.completed(450, 600).unwrap(), [refund]);
    // Reopening keeps everything.
    drop(store);
    let store = ProviderStore::open(dir.path().join("provider.sqlite")).unwrap();
    assert_eq!(store.sets(100).unwrap().0.len(), 2);
    assert_eq!(store.completed(0, 600).unwrap().len(), 2);
}

/// A read that fails partway, here on its completions, records nothing: neither its
/// receivers, nor its cursor and read time, nor the feed's start.
#[cfg(feature = "store")]
#[test]
fn a_failed_provider_read_records_nothing() {
    use receiver_directory::store::ProviderStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("provider.sqlite");
    let mut store = ProviderStore::open(&path).unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fail BEFORE INSERT ON completions
             BEGIN SELECT RAISE(ABORT, 'forced'); END;",
        )
        .unwrap();
    let payout = receiver();
    assert!(store
        .record(
            "near-payouts",
            40,
            &[(payout, true, 100)],
            &[payout],
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
    assert!(store.completed(0, i64::MAX).unwrap().is_empty());
}

/// Every query in one view sees the same state, even when a read commits between them.
#[cfg(feature = "store")]
#[test]
fn a_provider_view_reads_one_state() {
    use receiver_directory::store::ProviderStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("provider.sqlite");
    let mut store = ProviderStore::open(&path).unwrap();
    store
        .record("near-payouts", 40, &[], &[], 100, 300)
        .unwrap();
    let mut writer = ProviderStore::open(&path).unwrap();
    let reads = store
        .view(|view| -> Result<_, receiver_directory::Error> {
            let before = view.read("near-payouts")?;
            writer
                .record("near-payouts", 40, &[], &[], 200, 400)
                .unwrap();
            Ok((before, view.read("near-payouts")?))
        })
        .unwrap();
    assert_eq!(reads, (Some(300), Some(300)));
    assert_eq!(store.read("near-payouts").unwrap(), Some(400));
}

/// Without the test fixtures' allowance, a table below [`MIN_ROWS`] is refused.
#[test]
fn publications_below_the_minimum_rows_are_refused() {
    let small = manifest(MIN_ROWS / 2);
    assert!(small.validate().is_err());
    assert!(small.accept([1; 32], 100, 101, [3; 32]).is_err());
    assert!(Snapshot::build(small, &[record(0, 1)], &[]).is_err());
    let s = Snapshot::build(manifest(MIN_ROWS), &[record(0, 1)], &[]).unwrap();
    s.manifest.accept([1; 32], 100, 101, [3; 32]).unwrap();
}

#[test]
fn coverage_anchor_and_position_are_required() {
    allow_small_tables();
    let m = manifest(8);
    m.accept([1; 32], 100, 101, [3; 32]).unwrap();
    for (network, start, height, hash) in [
        ([9; 32], 100, 101, [3; 32]),
        ([1; 32], 99, 101, [3; 32]),
        ([1; 32], 100, 102, [3; 32]),
        ([1; 32], 100, 101, [9; 32]),
    ] {
        assert!(m.accept(network, start, height, hash).is_err());
    }
    let mut r = record(0, 1);
    r.payment.position = 300;
    assert!(Snapshot::build(m.clone(), &[r], &[]).is_err());
    let mut r = record(0, 1);
    r.payment.block_hash = [9; 32];
    assert!(Snapshot::build(m, &[r], &[]).is_err());
}

/// A row whose record claims more pages than the publication has records is
/// malformed.
#[test]
fn a_total_beyond_the_record_count_is_malformed() {
    allow_small_tables();
    let s = Snapshot::build(manifest(8), &[record(0, 1)], &[]).unwrap();
    let r = receiver();
    let mut b = row(&s, &r, 0).to_vec();
    assert!(lookup_row(&s.manifest, &r, 0, &b).unwrap().is_some());
    let slot = b
        .chunks(RECORD_BYTES)
        .position(|slot| slot.iter().any(|x| *x != 0))
        .unwrap();
    // The total pages field follows the version, receiver and page.
    b[slot * RECORD_BYTES + 48..slot * RECORD_BYTES + 52].copy_from_slice(&2u32.to_le_bytes());
    assert!(Record::decode(&b[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES]).is_ok());
    assert!(matches!(
        lookup_row(&s.manifest, &r, 0, &b),
        Err(Error::Malformed)
    ));
}

/// The note commitment tree ends at 2^32 positions.
#[test]
fn coverage_ends_within_the_tree() {
    let mut m = manifest(MIN_ROWS);
    m.end_position = 1 << 32;
    m.validate().unwrap();
    m.end_position += 1;
    assert!(m.validate().is_err());
}

#[test]
fn overflow_and_bad_padding_fail_closed() {
    allow_small_tables();
    let records: Vec<_> = (0..50).map(|p| record(p, 50)).collect();
    let mut m = manifest(4);
    let overflow = (0..100).any(|i| {
        m.salt[0] = i;
        matches!(
            Snapshot::build(m.clone(), &records, &[]),
            Err(Error::Capacity)
        )
    });
    assert!(overflow);
    let s = Snapshot::build(manifest(8), &[record(0, 1)], &[]).unwrap();
    let r = receiver();
    let mut b = row(&s, &r, 0).to_vec();
    b[ROW_BYTES - 1] = 1;
    assert!(lookup_row(&s.manifest, &r, 0, &b).is_err());
}

/// More records than slots is a capacity failure, found before placement; a bad row
/// count stays malformed.
#[test]
fn records_beyond_the_slots_are_a_capacity_failure() {
    allow_small_tables();
    let records = |n: u32| (0..n).map(|p| record(p, n)).collect::<Vec<_>>();
    let full = Snapshot::build(manifest(1), &records(SLOTS as u32), &[]).unwrap();
    assert_eq!(full.manifest.records, SLOTS as u64);
    assert!(matches!(
        Snapshot::build(manifest(1), &records(SLOTS as u32 + 1), &[]),
        Err(Error::Capacity)
    ));
    for rows in [0, 3, MAX_ROWS * 2] {
        assert!(matches!(
            Snapshot::build(manifest(rows), &records(SLOTS as u32 + 1), &[]),
            Err(Error::Malformed)
        ));
    }
    // A supplied manifest claiming more records than slots is malformed, not a
    // capacity failure.
    let mut claimed = full.manifest.clone();
    claimed.records += 1;
    assert!(matches!(claimed.validate(), Err(Error::Malformed)));
    assert!(matches!(
        claimed.accept([1; 32], 100, 101, [3; 32]),
        Err(Error::Malformed)
    ));
    let r = receiver();
    assert!(matches!(
        lookup_row(&claimed, &r, 0, row(&full, &r, 0)),
        Err(Error::Malformed)
    ));
}

#[cfg(feature = "store")]
#[test]
fn durable_coverage_atomic_failure_and_reorg() {
    allow_small_tables();
    use receiver_directory::store::{Config, IndexedBlock, Store};
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
#[cfg(feature = "store")]
#[test]
fn coinbase_payments_are_refused() {
    use receiver_directory::store::{Config, IndexedBlock, Store};
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

#[cfg(feature = "store")]
#[test]
fn crowded_buckets_retry_the_salt_before_growing() {
    allow_small_tables();
    use receiver_directory::store::{Config, IndexedBlock, Store};
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

#[test]
fn common_witnesses_bind_positions_and_reject_corrupt_or_stale_data() {
    allow_small_tables();
    use incrementalmerkletree::{frontier::CommitmentTree, witness::IncrementalWitness};
    use orchard::{note::ExtractedNoteCommitment, tree::MerkleHashOrchard};
    use receiver_directory::witness::WitnessSnapshot;
    let commitments: Vec<[u8; 32]> = (1..=20u8).map(|i| [i; 32]).collect();
    let mut manifest = manifest(8);
    manifest.start_position = 0;
    manifest.end_position = 20;
    manifest.records = 5;
    let positions = [0, 5, 6, 18, 19].into_iter().collect();
    let snapshot = WitnessSnapshot::build(&manifest, &commitments, &positions).unwrap();
    let encoded = snapshot.encode();
    let restored = WitnessSnapshot::decode(&encoded, &manifest).unwrap();
    for position in positions {
        let mut tree = CommitmentTree::<MerkleHashOrchard, 32>::empty();
        let mut witness = None;
        for (index, cmx) in commitments.iter().enumerate() {
            let hash =
                MerkleHashOrchard::from_cmx(&ExtractedNoteCommitment::from_bytes(cmx).unwrap());
            tree.append(hash).unwrap();
            if index == position as usize {
                witness = IncrementalWitness::from_tree(tree.clone());
            } else if let Some(w) = &mut witness {
                w.append(hash).unwrap();
            }
        }
        let expected = witness.unwrap().path().unwrap();
        assert_eq!(
            restored
                .path(position, commitments[position as usize])
                .unwrap()
                .as_slice(),
            expected
                .path_elems()
                .iter()
                .map(|h| h.to_bytes())
                .collect::<Vec<_>>()
        );
        assert!(restored.path(position, [31; 32]).is_err());
    }
    let mut stale = manifest.clone();
    stale.end_hash[0] ^= 1;
    assert!(WitnessSnapshot::decode(&encoded, &stale).is_err());
    assert!(WitnessSnapshot::decode(&encoded[..encoded.len() - 1], &manifest).is_err());
    let mut corrupt = encoded.clone();
    corrupt[152] = 32;
    assert!(WitnessSnapshot::decode(&corrupt, &manifest).is_err());
    let mut corrupt = encoded.clone();
    corrupt[116] ^= 1;
    assert!(WitnessSnapshot::decode(&corrupt, &manifest)
        .unwrap()
        .path(0, commitments[0])
        .is_err());
    assert_eq!(encoded, restored.encode());
}

#[cfg(feature = "store")]
#[test]
fn cached_store_proofs_follow_rewinds_reopen_and_replacement_blocks() {
    allow_small_tables();
    use receiver_directory::{
        store::{Config, IndexedBlock, Store},
        witness::WitnessCache,
    };
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
    let mut cache = WitnessCache::default();
    // A fresh cache builds from scratch; the reused one must match it byte for byte.
    let full = |store: &Store, m| {
        store
            .witnesses(m, &mut WitnessCache::default())
            .unwrap()
            .encode()
    };
    let expected = full(&store, &first.manifest);
    assert_eq!(
        store
            .witnesses(&first.manifest, &mut cache)
            .unwrap()
            .encode(),
        expected
    );
    block.height = 101;
    block.hash = [4; 32];
    block.parent = [3; 32];
    block.start_position = 3;
    block.end_position = 5;
    block.commitments = vec![[3; 32], [4; 32]];
    block.payments.clear();
    store.append(&block).unwrap();
    let second = store.snapshot(8, &[]).unwrap();
    assert_eq!(
        store
            .witnesses(&second.manifest, &mut cache)
            .unwrap()
            .encode(),
        full(&store, &second.manifest)
    );
    store.rewind(100, [3; 32]).unwrap();
    assert!(store.witnesses(&second.manifest, &mut cache).is_err());
    assert_eq!(
        store
            .witnesses(&first.manifest, &mut cache)
            .unwrap()
            .encode(),
        expected
    );
    drop(store);
    let mut store = Store::open(&path, config).unwrap();
    block.hash = [5; 32];
    block.commitments[0] = [9; 32];
    store.append(&block).unwrap();
    let replacement = store.snapshot(8, &[]).unwrap();
    assert_eq!(
        store
            .witnesses(&replacement.manifest, &mut cache)
            .unwrap()
            .encode(),
        full(&store, &replacement.manifest)
    );
    let mut wrong = replacement.manifest;
    wrong.end_position -= 1;
    assert!(store.witnesses(&wrong, &mut cache).is_err());
}

/// A history with more payments than the table has slots fails with
/// [`Error::Capacity`] before any record is loaded, and builds at a larger size.
#[cfg(feature = "store")]
#[test]
fn a_history_beyond_the_slots_fails_before_loading_records() {
    allow_small_tables();
    use receiver_directory::store::{Config, IndexedBlock, Store};
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
    assert!(matches!(store.snapshot(3, &[]), Err(Error::Malformed)));
    assert_eq!(
        store.snapshot(4, &[]).unwrap().manifest.records,
        u64::from(n)
    );
    // With every stored record undecodable, the count still decides first.
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute("UPDATE payments SET record=x'00'", [])
        .unwrap();
    assert!(matches!(store.snapshot(1, &[]), Err(Error::Capacity)));
    assert!(matches!(store.snapshot(4, &[]), Err(Error::Malformed)));
}
