//! Durable publication and repair behavior using real archive codecs.
use receiver_directory::{
    archive::{Coverage, Proof},
    blocks::BlockId,
    seen::SeenSet,
};
use receiver_store::{
    archive::{ArchiveStore, Block, HEAD_GRACE_SECS},
    Config,
};

/// A short history crossing a fixed page boundary.
fn config() -> Config {
    Config {
        genesis: [1; 32],
        start_height: 254,
        start_parent: [2; 32],
        start_position: 0,
    }
}

/// Produces deterministic canonical blocks, optionally correcting an empty block below a fork.
fn blocks(end: u32, correction: bool, fork: bool) -> Vec<Result<Block, receiver_store::Error>> {
    let mut parent = config().start_parent;
    (254..=end)
        .map(|height| {
            let hash = if fork && height >= 256 {
                [99; 32]
            } else {
                [height as u8; 32]
            };
            let corrected = correction && height == 254;
            let block = Block {
                id: BlockId { height, hash },
                parent,
                records: [u8::from(corrected); 32],
                receivers: if corrected { vec![[7; 43]] } else { vec![] },
            };
            parent = hash;
            Ok(block)
        })
        .collect()
}

/// Old revisions keep their full retirement grace even after a long idle period.
#[test]
fn retirement_grace_and_restart_keep_all_canonical_pages() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.sqlite");
    let mut store = ArchiveStore::open(&path, config()).unwrap();
    let first = store.stage(blocks(256, false, false), None, 0).unwrap();
    store.activate(&first, 0).unwrap();
    let second = store
        .stage(blocks(257, false, false), None, 10_000)
        .unwrap();
    store.activate(&second, 10_000).unwrap();
    store.prune(10_000 + HEAD_GRACE_SECS - 1, &[]).unwrap();
    assert!(store.range(first.digest().unwrap(), 1).is_ok());
    drop(store);
    let mut store = ArchiveStore::open(&path, config()).unwrap();
    assert_eq!(store.head().unwrap(), Some(second.clone()));
    store.prune(10_000 + HEAD_GRACE_SECS + 1, &[]).unwrap();
    assert!(store.range(first.digest().unwrap(), 1).is_err());
    for number in 0..=1 {
        let bytes = store.range(second.digest().unwrap(), number).unwrap();
        let page = Proof::decode(&second, number, &bytes).unwrap();
        assert_eq!(page.number(), number);
    }
    assert_eq!(second.archive, first.archive);
    assert_eq!(second.epoch, 0);
}

/// An empty-block correction and a simultaneous fork both survive restart without operator repair.
#[test]
fn combined_correction_and_fork_advance_epoch_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive.sqlite");
    let mut store = ArchiveStore::open(&path, config()).unwrap();
    let initial = store.stage(blocks(257, false, false), None, 0).unwrap();
    store.activate(&initial, 0).unwrap();
    let repaired = store.stage(blocks(257, true, true), None, 1).unwrap();
    assert_eq!(repaired.epoch, 1);
    assert!(store.head().unwrap().is_none());
    assert!(store.activate(&initial, 1).is_err());
    store.activate(&repaired, 1).unwrap();
    drop(store);
    let mut store = ArchiveStore::open(&path, config()).unwrap();
    let same = store.stage(blocks(257, true, true), None, 2).unwrap();
    assert_eq!(same, repaired);
    let page = Proof::decode(&same, 0, &store.range(same.digest().unwrap(), 0).unwrap()).unwrap();
    assert_eq!(page.filters()[0].matches(&[[7; 43]]), vec![true]);
    let fork_only = store.stage(blocks(256, true, false), None, 3).unwrap();
    assert_eq!(fork_only.epoch, 1);
    assert!(store.head().unwrap().is_none());
}

/// Missing or shrinking provider history never blocks payment publication or erases seen membership.
#[test]
fn provider_failure_and_replacement_identity() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = ArchiveStore::open(dir.path().join("a"), config()).unwrap();
    let seen = SeenSet::new(config().genesis, 0).union([&[7; 43]]);
    let reference = store.record_seen(&seen).unwrap();
    let first = store
        .stage(blocks(254, false, false), Some(reference.clone()), 0)
        .unwrap();
    store.activate(&first, 0).unwrap();
    assert!(store
        .record_seen(&SeenSet::new(config().genesis, 0))
        .is_err());
    let paid = store.stage(blocks(255, false, false), None, 1).unwrap();
    store.activate(&paid, 1).unwrap();
    store.prune(1000, &[]).unwrap();
    assert_eq!(
        store.seen(reference.digest).unwrap(),
        seen.encode().unwrap()
    );
    let mut replacement = ArchiveStore::open(dir.path().join("b"), config()).unwrap();
    let fresh = replacement
        .stage(blocks(255, false, false), None, 1)
        .unwrap();
    assert_ne!(fresh.archive, paid.archive);
}

/// An unchanged PIR session ID cannot reuse a declaration from before an indexing correction.
#[test]
fn coverage_is_revision_scoped_and_staged_sessions_survive_gc() {
    let mut store = ArchiveStore::open(":memory:", config()).unwrap();
    let first = store.stage(blocks(254, false, false), None, 0).unwrap();
    let coverage = Coverage {
        genesis: first.genesis,
        archive: first.archive,
        epoch: 0,
        session: [8; 32],
        first_height: first.first_height,
        frontier: first.frontier,
    };
    store.record_coverage(&coverage).unwrap();
    let next = store.stage(blocks(254, true, false), None, 1).unwrap();
    assert!(store.record_coverage(&coverage).is_err());
    assert!(store
        .coverage(next.archive, next.epoch, coverage.session)
        .is_err());
    let new = Coverage {
        epoch: next.epoch,
        ..coverage.clone()
    };
    store.record_coverage(&new).unwrap();
    store.prune(1000, &[coverage.session]).unwrap();
    assert_eq!(
        store.coverage(new.archive, new.epoch, new.session).unwrap(),
        new.encode().unwrap()
    );
    store.prune(1001, &[]).unwrap();
    assert!(store.coverage(new.archive, new.epoch, new.session).is_err());
}

/// Failed staging is atomic and cannot replace a good active head or lose its repair baseline.
#[test]
fn gap_and_wrong_parent_leave_prior_head_intact() {
    let mut store = ArchiveStore::open(":memory:", config()).unwrap();
    let initial = store.stage(blocks(256, false, false), None, 0).unwrap();
    store.activate(&initial, 0).unwrap();
    let mut gap = blocks(256, true, false);
    gap.remove(1).unwrap();
    assert!(store.stage(gap, None, 1).is_err());
    assert_eq!(store.head().unwrap(), Some(initial.clone()));
    let mut wrong = blocks(256, true, false);
    wrong[1].as_mut().unwrap().parent = [33; 32];
    assert!(store.stage(wrong, None, 1).is_err());
    assert_eq!(store.head().unwrap(), Some(initial));
    let fixed = store.stage(blocks(256, true, false), None, 2).unwrap();
    assert_eq!(fixed.epoch, 1);
}

mod common;

/// Real stored records produce empty and paid filters; a txid correction bumps the epoch even
/// though receiver membership does not change. PIR snapshots can stop before the filter tip.
#[test]
fn streamed_source_and_pir_prefix_share_exact_records() {
    use receiver_store::{IndexedBlock, Store};
    let config = Config {
        genesis: [1; 32],
        start_height: 100,
        start_parent: [2; 32],
        start_position: 200,
    };
    let mut source = Store::open(":memory:", config.clone()).unwrap();
    let mut archive = ArchiveStore::open(":memory:", config).unwrap();
    source
        .append(&IndexedBlock {
            height: 100,
            hash: [9; 32],
            parent: [2; 32],
            start_position: 200,
            end_position: 200,
            coinbase_actions: 0,
            payments: vec![],
            commitments: vec![],
        })
        .unwrap();
    let record = common::record(0, 1);
    let mut paid = IndexedBlock {
        height: 101,
        hash: [3; 32],
        parent: [9; 32],
        start_position: 200,
        end_position: 201,
        coinbase_actions: 0,
        payments: vec![(record.receiver, record.payment)],
        commitments: vec![[6; 32]],
    };
    source.append(&paid).unwrap();
    let head = source.stage_archive(&mut archive, None, 0).unwrap();
    let page = Proof::decode(&head, 0, &archive.range(head.digest().unwrap(), 0).unwrap()).unwrap();
    assert_eq!(page.filters()[0].count(), 0);
    assert_eq!(page.filters()[1].matches(&[record.receiver]), vec![true]);
    let earlier = source.snapshot_at(16, &[], 100).unwrap();
    assert_eq!(earlier.manifest.records, 0);
    assert_eq!(earlier.manifest.end_height, 100);
    assert_eq!(source.snapshot(16, &[]).unwrap().manifest.records, 1);
    source.rewind(100, [9; 32]).unwrap();
    paid.payments[0].1.txid = [55; 32];
    source.append(&paid).unwrap();
    let correction = source.stage_archive(&mut archive, None, 1).unwrap();
    assert_eq!(correction.epoch, head.epoch + 1);
    assert_eq!(correction.index_root, head.index_root);
}

/// Rebuilding against a shorter node cannot discard a suffix's correction baseline.
#[test]
fn shortened_rebuild_preserves_correction_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("archive");
    let mut store = ArchiveStore::open(&path, config()).unwrap();
    let initial = store.stage(blocks(257, false, false), None, 0).unwrap();
    store.activate(&initial, 0).unwrap();
    let prefix = store.stage(blocks(254, false, false), None, 1).unwrap();
    store.activate(&prefix, 1).unwrap();
    assert_eq!(prefix.epoch, 0);
    drop(store);
    let mut store = ArchiveStore::open(&path, config()).unwrap();
    let mut rebuilt = blocks(257, false, false);
    let changed = rebuilt[2].as_mut().unwrap();
    changed.records = [8; 32];
    changed.receivers = vec![[7; 43]];
    let repaired = store.stage(rebuilt, None, 2).unwrap();
    assert_eq!(repaired.archive, initial.archive);
    assert_eq!(repaired.epoch, 1);
}

/// An offline wallet may never observe an intervening fork before its original chain returns.
#[test]
fn returning_branch_keeps_its_own_correction_baseline() {
    let mut store = ArchiveStore::open(":memory:", config()).unwrap();
    let initial = store.stage(blocks(257, false, false), None, 0).unwrap();
    store.activate(&initial, 0).unwrap();
    let fork = store.stage(blocks(257, false, true), None, 1).unwrap();
    store.activate(&fork, 1).unwrap();
    let mut returned = blocks(257, false, false);
    let changed = returned[2].as_mut().unwrap();
    changed.records = [8; 32];
    changed.receivers = vec![[7; 43]];
    let repaired = store.stage(returned, None, 2).unwrap();
    assert_eq!(repaired.epoch, 1);
}
