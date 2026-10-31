use super::*;
use crate::probe::common;
use receiver_store::{Config, IndexedBlock};
use std::cell::RefCell;
use std::collections::HashSet;
use zakura_pir_publisher::receiver::MAX_RECEIVER_PAYMENTS;

thread_local! {
    /// `(height, shard start)` pairs whose whole block [`Segment::add`] refuses, as a
    /// block defeating every salt attempt would be.
    pub(crate) static REFUSED: RefCell<HashSet<(u32, u32)>> = RefCell::default();
}

/// Advances `sealer` and records every seal it stages, as publication does once each
/// shard is built.
fn record(sealer: &mut Sealer, store: &mut Store) -> Result<(), Error> {
    for shard in sealer.advance(store)? {
        store.seal(&shard)?;
    }
    Ok(())
}

/// The format's limits with a segment capacity of `capacity` and a seal target of `target`.
fn policy(capacity: u32, target: u32) -> Policy {
    Policy {
        target,
        capacity,
        ..Policy::default()
    }
}

/// A store starting at height 100 holding blocks of `blocks[i]` payments.
pub(crate) fn store(dir: &std::path::Path, blocks: &[usize]) -> Store {
    let mut store = Store::open(
        dir.join("directory.sqlite"),
        Config {
            genesis: [1; 32],
            start_height: 100,
            start_parent: [2; 32],
            start_position: 0,
        },
    )
    .unwrap();
    for count in blocks {
        append(&mut store, *count, |_| common::receiver());
    }
    store
}

/// Appends a block of `count` payments after the store's tip, payment `i` to
/// `receiver(i)`, each in its own transaction, returning it.
pub(crate) fn append(
    store: &mut Store,
    count: usize,
    receiver: impl Fn(usize) -> ReceiverBytes,
) -> IndexedBlock {
    append_with(store, count, 0, receiver)
}

/// [`append`] with `extra` further commitments after the payments.
fn append_with(
    store: &mut Store,
    count: usize,
    extra: usize,
    receiver: impl Fn(usize) -> ReceiverBytes,
) -> IndexedBlock {
    let tip = store.tip().unwrap();
    let height = tip.height + 1;
    let mut hash = [0; 32];
    hash[..4].copy_from_slice(&height.to_le_bytes());
    hash[4..8].copy_from_slice(&(count as u32).to_le_bytes());
    let payments = (0..count)
        .map(|i| {
            let mut payment = common::record(0, 1).payment;
            (payment.height, payment.block_hash) = (height, hash);
            payment.position = tip.position + i as u64;
            payment.tx_index = i as u32 + 1;
            payment.txid = [0; 32];
            payment.txid[..8].copy_from_slice(&payment.position.to_le_bytes());
            payment.cmx = [6; 32];
            (receiver(i), payment)
        })
        .collect();
    let block = IndexedBlock {
        height,
        hash,
        parent: tip.hash,
        start_position: tip.position,
        end_position: tip.position + (count + extra) as u64,
        coinbase_actions: 0,
        payments,
        commitments: vec![[6; 32]; count + extra],
    };
    store.append(&block).unwrap();
    block
}

/// The seals recorded so far, as `(start, end, segments)`.
fn sealed(store: &Store) -> Vec<(u32, u32, u32)> {
    store
        .sealed_shards()
        .unwrap()
        .iter()
        .map(|s| (s.start_height, s.end_height, s.segments))
        .collect()
}

/// The seals `policy` records over blocks of `blocks` payments.
fn seals(policy: Policy, blocks: &[usize]) -> Vec<(u32, u32, u32)> {
    let dir = tempfile::tempdir().unwrap();
    let mut store = store(dir.path(), blocks);
    record(&mut Sealer::new(policy), &mut store).unwrap();
    sealed(&store)
}

/// A block that would pass the companion's capacity seals the shard before it, never
/// splitting a block across shards.
#[test]
fn a_block_past_capacity_seals_the_shard_before_it() {
    assert_eq!(
        seals(policy(14, 1000), &[10, 3, 3, 0, 12]),
        [(100, 101, 1), (102, 103, 1)]
    );
}

/// Reaching the target seals after the block that reaches it.
#[test]
fn the_target_seals_after_the_block_that_reaches_it() {
    assert_eq!(
        seals(policy(1000, 4), &[2, 2, 1, 0, 3]),
        [(100, 101, 1), (102, 104, 1)]
    );
}

/// The default policy is the format's: 31/32 of a 65,536-row companion.
#[test]
fn the_default_policy_is_the_formats() {
    let p = Policy::default();
    assert_eq!(
        (p.target, p.capacity, p.max_offset),
        (317_440, 327_680, (1 << 24) - 1)
    );
}

/// A block too large for one segment seals the shard before it and becomes a shard of
/// its own, split in position order into segments that each hold; one needing a ninth
/// segment is refused, records nothing, not even the seal before it, and stops sealing
/// there.
#[test]
fn an_oversized_block_becomes_its_own_shard_of_up_to_eight_segments() {
    assert_eq!(
        seals(policy(10, 1000), &[3, 30, 2]),
        [(100, 100, 1), (101, 101, 3)]
    );
    assert_eq!(seals(policy(10, 1000), &[80]), [(100, 100, 8)]);
    let dir = tempfile::tempdir().unwrap();
    let mut store = store(dir.path(), &[3, 81]);
    let refused = record(&mut Sealer::new(policy(10, 1000)), &mut store);
    assert!(matches!(refused, Err(Error::Capacity)));
    assert_eq!(sealed(&store), []);
}

/// One receiver may hold 65,535 payments in a segment: a block taking it to that
/// count stays, its 65,536th payment in a later block seals before that block, and a
/// block of 65,536 payments to it splits so the last one starts a second segment.
#[test]
fn one_receiver_holds_at_most_65535_payments_in_a_segment() {
    let max = MAX_RECEIVER_PAYMENTS as usize;
    let dir = tempfile::tempdir().unwrap();
    let mut index = store(dir.path(), &[max, 1]);
    let mut sealer = Sealer::new(Policy::default());
    record(&mut sealer, &mut index).unwrap();
    assert_eq!(sealed(&index), [(100, 100, 1)]);
    assert_eq!(sealer.open().unwrap().1.payments, 1);
    let dir = tempfile::tempdir().unwrap();
    let mut index = store(dir.path(), &[max + 1]);
    record(&mut Sealer::new(Policy::default()), &mut index).unwrap();
    assert_eq!(sealed(&index), [(100, 100, 2)]);
    let parts = replay(
        &Policy::default(),
        &index,
        &index.sealed_shards().unwrap()[0],
    )
    .unwrap();
    let counts: Vec<_> = parts.iter().map(|(s, r)| (s.payments, r.len())).collect();
    assert_eq!(counts, [(max as u32, max), (1, 1)]);
}

/// The last representable height offset stays in an otherwise empty shard and the
/// next block seals it, empty, before that block.
#[test]
fn the_height_offset_limit_seals_an_empty_shard() {
    let limit = Policy {
        max_offset: 3,
        ..Policy::default()
    };
    assert_eq!(seals(limit, &[0, 0, 0, 0, 0, 1]), [(100, 103, 1)]);
}

/// A block that defeats every salt attempt in a segment that already holds blocks seals
/// the segment before it; the block then starts the next shard as usual. That seal goes
/// with the block that decided it: once a reorg replaces block 101, sealing matches a
/// fresh pass over the new chain.
#[test]
fn a_block_that_fails_placement_seals_before_it_and_places_after() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = store(dir.path(), &[2, 2, 2]);
    let mut sealer = Sealer::new(Policy::default());
    REFUSED.with(|r| r.borrow_mut().insert((101, 100)));
    record(&mut sealer, &mut store).unwrap();
    assert_eq!(sealed(&store), [(100, 100, 1)]);
    assert_eq!(store.sealed_shards().unwrap()[0].cause, 101);
    assert_eq!(
        sealer.open().map(|(start, s)| (start, s.payments)),
        Some((101, 4))
    );
    let kept = store.checkpoint(100).unwrap().hash;
    store.rewind(100, kept).unwrap();
    REFUSED.with(|r| r.borrow_mut().clear());
    append(&mut store, 3, |_| common::receiver());
    assert_eq!(sealed(&store), []);
    record(&mut sealer, &mut store).unwrap();
    assert_eq!(sealed(&store), seals(Policy::default(), &[2, 3]));
    assert_eq!(
        sealer.open().map(|(start, s)| (start, s.payments)),
        Some((100, 5))
    );
}

/// A first block a fresh segment refuses whole is split into a shard of its own even
/// when its payments fit one segment, and replays into that one segment.
#[test]
fn a_single_segment_split_replays() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = store(dir.path(), &[2, 2, 2]);
    REFUSED.with(|r| r.borrow_mut().extend([(101, 100), (101, 101)]));
    record(&mut Sealer::new(Policy::default()), &mut store).unwrap();
    assert_eq!(sealed(&store), [(100, 100, 1), (101, 101, 1)]);
    let shard = &store.sealed_shards().unwrap()[1];
    let parts = replay(&Policy::default(), &store, shard).unwrap();
    REFUSED.with(|r| r.borrow_mut().clear());
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].1.len(), 2);
}

/// Staged seals are recorded by the caller only: a sealer that staged a seal stages it
/// again until the store records it, and a store that never records one is sealed
/// afresh by a new sealer.
#[test]
fn seals_are_staged_until_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = store(dir.path(), &[4, 4, 4]);
    let mut sealer = Sealer::new(policy(1000, 4));
    let staged = sealer.advance(&store).unwrap();
    assert_eq!(staged.len(), 3);
    assert_eq!(sealer.advance(&store).unwrap(), staged);
    assert_eq!(sealed(&store), []);
    store.seal(&staged[0]).unwrap();
    assert_eq!(sealer.advance(&store).unwrap(), staged[1..]);
    assert_eq!(
        Sealer::new(policy(1000, 4)).advance(&store).unwrap(),
        staged[1..]
    );
}

/// A correction adding a payment to the open shard is replayed by a sealer kept
/// across it, sealing as a fresh one would.
#[test]
fn a_correction_inside_the_open_shard_replays_its_projection() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = store(dir.path(), &[2]);
    let mut block = append_with(&mut store, 1, 1, |_| common::receiver());
    let mut sealer = Sealer::new(policy(1000, 4));
    record(&mut sealer, &mut store).unwrap();
    assert!(store.sealed_shards().unwrap().is_empty());
    let mut added = block.payments[0].1.clone();
    (added.position, added.tx_index) = (added.position + 1, added.tx_index + 1);
    added.txid[..8].copy_from_slice(&added.position.to_le_bytes());
    block.payments.push((common::receiver(), added));
    store.correct(1, &[block]).unwrap();
    record(&mut sealer, &mut store).unwrap();
    assert_eq!(sealed(&store), [(100, 101, 1)]);
}

/// Sealing block by block across restarts records the seals one pass records.
#[test]
fn incremental_sealing_matches_a_fresh_replay() {
    let policy = policy(1000, 6);
    let blocks = [4, 0, 5, 9, 2, 14, 1];
    let dir = tempfile::tempdir().unwrap();
    let mut store = store(dir.path(), &[]);
    let mut sealer = Sealer::new(policy);
    for (i, count) in blocks.iter().enumerate() {
        append(&mut store, *count, |_| common::receiver());
        if i % 3 == 2 {
            sealer = Sealer::new(policy);
        }
        record(&mut sealer, &mut store).unwrap();
    }
    let incremental = sealed(&store);
    assert_eq!(incremental, seals(policy, &blocks));
    assert!(!incremental.is_empty());
}

/// The open segment and every sealed shard publish the same bytes whether the chain is
/// published after every block, in one batch, or after a restart: the projection is
/// replayed block by block, and the choice seeds come from the tip or sealing block.
#[test]
fn publication_cadence_and_restart_give_the_same_bytes() {
    let policy = policy(1000, 6);
    let blocks = [4, 0, 5, 3, 2];
    let receivers = [
        common::receiver(),
        common::seeded_receiver(1),
        common::seeded_receiver(2),
    ];
    let build = |every: bool, restart: bool| {
        let dir = tempfile::tempdir().unwrap();
        let mut store = store(dir.path(), &[]);
        let mut sealer = Sealer::new(policy);
        for count in blocks {
            append(&mut store, count, |i| receivers[i % 3]);
            if every {
                record(&mut sealer, &mut store).unwrap();
            }
        }
        if restart {
            sealer = Sealer::new(policy);
        }
        record(&mut sealer, &mut store).unwrap();
        let tip = store.tip().unwrap();
        let mut witnesses = crate::witness::Witnesses::default();
        let shard = store.sealed_shards().unwrap()[0].clone();
        let sealed = crate::shards::sealed(
            &mut store,
            &mut witnesses,
            &policy,
            &shard,
            &tip,
            0,
            &|| false,
        )
        .unwrap();
        let open = sealer
            .open()
            .map(|(start, s)| (start, s.projection.clone()))
            .unwrap();
        let open = crate::shards::open(
            &mut store,
            &mut witnesses,
            (open.0, &open.1),
            &tip,
            0,
            &|| false,
        )
        .unwrap();
        let bytes = |b: &crate::shards::Built| {
            (
                b.tables.directory.data.clone(),
                b.tables.directory.choice.clone(),
                b.tables.companion.data.clone(),
                b.tables.companion.manifest.clone(),
            )
        };
        (
            sealed.segments.iter().map(bytes).collect::<Vec<_>>(),
            bytes(&open),
        )
    };
    let once = build(false, false);
    assert_eq!(build(true, false), once);
    assert_eq!(build(true, true), once);
}
