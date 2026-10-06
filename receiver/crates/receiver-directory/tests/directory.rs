mod common;
use common::{action, manifest, receiver, record};
use receiver_directory::{
    snapshot::{lookup_row, row_for, Snapshot, ROW_BYTES},
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

#[test]
fn pages_share_one_revision_and_build_order_is_stable() {
    let records = [record(0, 2), record(1, 2)];
    let s = Snapshot::build(manifest(8), &records).unwrap();
    let reversed = Snapshot::build(manifest(8), &[records[1].clone(), records[0].clone()]).unwrap();
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
    let empty = Snapshot::build(manifest(8), &[]).unwrap();
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
    assert!(Snapshot::build(manifest(8), &records[..1]).is_err());
    assert!(Snapshot::build(manifest(8), &records[1..]).is_err());
    assert!(Snapshot::build(manifest(8), &[record(0, 1), record(1, 2)]).is_err());
    assert!(Snapshot::build(manifest(8), &[record(0, 1), record(0, 1)]).is_err());
}

#[test]
fn coverage_anchor_and_position_are_required() {
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
    assert!(Snapshot::build(m.clone(), &[r]).is_err());
    let mut r = record(0, 1);
    r.payment.block_hash = [9; 32];
    assert!(Snapshot::build(m, &[r]).is_err());
}

#[test]
fn overflow_and_bad_padding_fail_closed() {
    let records: Vec<_> = (0..50).map(|p| record(p, 50)).collect();
    let mut m = manifest(4);
    let overflow = (0..100).any(|i| {
        m.salt[0] = i;
        matches!(Snapshot::build(m.clone(), &records), Err(Error::Capacity))
    });
    assert!(overflow);
    let s = Snapshot::build(manifest(8), &[record(0, 1)]).unwrap();
    let r = receiver();
    let mut b = row(&s, &r, 0).to_vec();
    b[ROW_BYTES - 1] = 1;
    assert!(lookup_row(&s.manifest, &r, 0, &b).is_err());
}

#[cfg(feature = "store")]
#[test]
fn durable_coverage_atomic_failure_and_reorg() {
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
    assert!(store.snapshot(8).is_err());
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
    assert_eq!(store.snapshot(8).unwrap().manifest.end_height, 100);
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
    let old = store.snapshot(8).unwrap();
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
    let new = store.snapshot(8).unwrap();
    assert_ne!(
        old.manifest.revision().unwrap(),
        new.manifest.revision().unwrap()
    );
    assert!(old.manifest.accept([1; 32], 100, 101, block.hash).is_err());
    let mut wrong = config.clone();
    wrong.genesis = [99; 32];
    assert!(Store::open(&path, wrong).is_err());
    store.rewind(99, config.start_parent).unwrap();
    assert!(store.snapshot(8).is_err());
}

#[test]
fn common_witnesses_bind_positions_and_reject_corrupt_or_stale_data() {
    use incrementalmerkletree::{frontier::CommitmentTree, witness::IncrementalWitness};
    use orchard::{note::ExtractedNoteCommitment, tree::MerkleHashOrchard};
    use receiver_directory::witness::WitnessSnapshot;
    let commitments: Vec<[u8; 32]> = (1..=20u8).map(|i| [i; 32]).collect();
    let mut manifest = manifest(8);
    manifest.start_position = 0;
    manifest.end_position = 20;
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
    let first = store.snapshot(8).unwrap();
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
    let second = store.snapshot(8).unwrap();
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
    let replacement = store.snapshot(8).unwrap();
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
