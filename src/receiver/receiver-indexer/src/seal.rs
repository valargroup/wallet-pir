//! Sealing Receiver shards on placement and companion witness capacity.
//!
//! A shard is sealed on block boundaries, as Transparent's are, but on what its
//! payment table holds rather than a block count. Capacity is exact: a shard keeps a
//! block only if some salt still places every payment in the pinned payment geometry
//! and its companion witness table still holds one payload per payment. A block that
//! would breach either seals the shard before it; reaching the target seals the shard
//! after it. A block too large for one table becomes a shard of its own, split into
//! several segments that each place. Publication never covers a block it cannot place.
use receiver_store::{Checkpoint, Error, SealedShard, Store, SALT_ATTEMPTS};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use zakura_pir_publisher::receiver::inventory::MAX_SEGMENTS;
use zakura_pir_publisher::receiver::{
    snapshot::{self, Manifest, PROFILE, SLOTS, TREE_SIZE},
    witness, Hash, RawRecord, ReceiverBytes,
};

/// What a sealed segment holds. These are schema: shards sealed under other limits
/// are a different partition, so each shard records the rows it was sealed under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    /// Pinned payment table rows of every sealed segment.
    pub rows: u32,
    /// Most payloads one segment's companion witness table holds.
    pub payloads: u64,
    /// Payments after which an open shard seals at the next block boundary.
    pub target: u64,
}

impl Policy {
    /// Segments of `rows` rows, whose companion tables hold at most the largest
    /// witness table's payloads. Without a `target`, one seventh of the smaller
    /// capacity is kept for the block that crosses it; placement checks, not the
    /// target, keep every segment valid.
    pub fn new(rows: u32, target: Option<u64>) -> Result<Self, Error> {
        zakura_pir_publisher::receiver::session::validate_rows(rows)
            .map_err(|_| Error::Malformed)?;
        let payloads = witness::capacity(snapshot::MAX_ROWS)?;
        let capacity = snapshot::capacity(rows)?.min(payloads);
        let target = target.unwrap_or(capacity - capacity / 7);
        if target == 0 || target > capacity {
            return Err(Error::Malformed);
        }
        Ok(Self {
            rows,
            payloads,
            target,
        })
    }
}

/// The salt of a sealed shard's segment `segment` at `attempt`, fixed by the block
/// before the shard so a projection can place payments before the shard's end is known.
pub fn salt(parent: &Hash, segment: u32, attempt: u32) -> Hash {
    Sha256::new()
        .chain_update(b"ironwood-receiver/v1/shard-salt\0")
        .chain_update(parent)
        .chain_update(segment.to_le_bytes())
        .chain_update(attempt.to_le_bytes())
        .finalize()
        .into()
}

/// One segment's exact placement state under every salt, as payments are added in
/// position order: each salt's row occupancy, or `None` once a row overflowed.
struct Projection {
    rows: u32,
    payloads: u64,
    salts: Vec<(Manifest, Option<Vec<u8>>)>,
    pages: HashMap<ReceiverBytes, u32>,
    count: u64,
}

impl Projection {
    /// An empty segment `segment` of the shard after block `parent` on `genesis`.
    fn new(policy: &Policy, genesis: Hash, parent: &Hash, segment: u32) -> Self {
        let salts = (0..SALT_ATTEMPTS)
            .map(|attempt| {
                let manifest = Manifest {
                    profile: PROFILE.into(),
                    genesis,
                    history_revision: 0,
                    start_height: 0,
                    start_parent: [0; 32],
                    start_position: 0,
                    end_height: 0,
                    end_hash: [0; 32],
                    end_position: TREE_SIZE,
                    rows: policy.rows,
                    salt: salt(parent, segment, attempt),
                    records: 0,
                    data_sha256: [0; 32],
                };
                (manifest, Some(vec![0u8; policy.rows as usize]))
            })
            .collect();
        Self {
            rows: policy.rows,
            payloads: policy.payloads,
            salts,
            pages: HashMap::new(),
            count: 0,
        }
    }

    /// Adds `records`, in position order, if some salt still places every payment and
    /// the companion table still holds them; otherwise changes nothing.
    fn try_add(&mut self, records: &[RawRecord]) -> Result<bool, Error> {
        if self.count + records.len() as u64 > self.payloads.min(snapshot::capacity(self.rows)?) {
            return Ok(false);
        }
        let mut pages = HashMap::new();
        let placed: Vec<(ReceiverBytes, u32)> = records
            .iter()
            .map(|r| {
                let next = pages
                    .entry(r.receiver)
                    .or_insert_with(|| self.pages.get(&r.receiver).copied().unwrap_or(0));
                *next += 1;
                (r.receiver, *next - 1)
            })
            .collect();
        let mut outcomes = Vec::with_capacity(self.salts.len());
        for (manifest, occupancy) in &self.salts {
            let Some(occupancy) = occupancy else {
                outcomes.push(None);
                continue;
            };
            let mut added: HashMap<usize, u8> = HashMap::new();
            let mut fits = true;
            for (receiver, page) in &placed {
                let row = snapshot::row_for(manifest, receiver, *page)?;
                let count = added.entry(row).or_insert(0);
                *count += 1;
                if usize::from(occupancy[row]) + usize::from(*count) > SLOTS {
                    fits = false;
                    break;
                }
            }
            outcomes.push(fits.then_some(added));
        }
        if outcomes.iter().all(Option::is_none) {
            return Ok(false);
        }
        for ((_, occupancy), outcome) in self.salts.iter_mut().zip(outcomes) {
            match (occupancy.as_mut(), outcome) {
                (Some(rows), Some(added)) => {
                    for (row, count) in added {
                        rows[row] += count;
                    }
                }
                (_, None) => *occupancy = None,
                (None, Some(_)) => unreachable!("a dead salt places nothing"),
            }
        }
        self.pages.extend(pages);
        self.count += records.len() as u64;
        Ok(true)
    }
}

/// Splits one block's `records`, in position order, into the fewest consecutive
/// segments that each place under [`salt`] and fit their companion table, greedily.
/// More than [`MAX_SEGMENTS`] is [`Error::Capacity`].
pub fn split(
    policy: &Policy,
    genesis: Hash,
    parent: &Hash,
    records: &[RawRecord],
) -> Result<Vec<Vec<RawRecord>>, Error> {
    let mut segments: Vec<Vec<RawRecord>> = vec![Vec::new()];
    let mut projection = Projection::new(policy, genesis, parent, 0);
    for record in records {
        if !projection.try_add(std::slice::from_ref(record))? {
            let next = segments.len() as u32;
            projection = Projection::new(policy, genesis, parent, next);
            if !projection.try_add(std::slice::from_ref(record))? {
                return Err(Error::Capacity);
            }
            segments.push(Vec::new());
        }
        segments
            .last_mut()
            .expect("one segment")
            .push(record.clone());
    }
    if segments.len() > MAX_SEGMENTS {
        return Err(Error::Capacity);
    }
    Ok(segments)
}

/// The open shard: its first block, the block before it, the last block it absorbed and
/// its projection.
struct Open {
    start: u32,
    parent: Hash,
    through: Checkpoint,
    /// The store's correction revision the projection was built under.
    revision: u64,
    projection: Projection,
}

/// Decides seals incrementally across publications. Recreate on restart: it replays the
/// open shard from the store, and sealed shards are recorded there.
pub struct Sealer {
    policy: Policy,
    open: Option<Open>,
}

impl Sealer {
    /// A sealer under `policy`.
    pub fn new(policy: Policy) -> Self {
        Self { policy, open: None }
    }

    /// The policy new shards are sealed under.
    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// Absorbs every stored block after the last seal through the store's tip, recording
    /// each seal as it is decided. A block no number of segments can hold is an error
    /// that records nothing for it, so coverage never passes it.
    pub fn advance(&mut self, store: &mut Store, genesis: Hash) -> Result<(), Error> {
        let tip = store.tip()?;
        let revision = store.history_revision()?;
        loop {
            let next = match store.sealed_shards()?.last() {
                Some(last) => last.end_height + 1,
                None => store.config().start_height,
            };
            // A correction can change the open shard's payments without moving any
            // block, so the projection is replayed under each new correction revision.
            let valid = self.open.as_ref().is_some_and(|open| {
                open.start == next
                    && open.revision == revision
                    && store.checkpoint(open.through.height).ok().as_ref() == Some(&open.through)
            });
            if !valid {
                let parent = store.checkpoint(next - 1)?;
                self.open = Some(Open {
                    start: next,
                    parent: parent.hash,
                    through: parent.clone(),
                    revision,
                    projection: Projection::new(&self.policy, genesis, &parent.hash, 0),
                });
            }
            let open = self.open.as_mut().expect("set above");
            if open.through.height >= tip.height {
                return Ok(());
            }
            let height = open.through.height + 1;
            let records = store.records(height, height)?;
            let sealed = if open.projection.try_add(&records)? {
                open.through = store.checkpoint(height)?;
                (open.projection.count >= self.policy.target).then_some((height, 1))
            } else if open.through.height >= open.start {
                // Seal before the block that does not fit, then retry it alone.
                Some((open.through.height, 1))
            } else {
                let segments = split(&self.policy, genesis, &open.parent, &records)?;
                Some((height, segments.len() as u32))
            };
            if let Some((end, segments)) = sealed {
                self.seal(store, end, segments)?;
            }
        }
    }

    /// Records the open shard as sealed through `end` with `segments` segments.
    fn seal(&mut self, store: &mut Store, end: u32, segments: u32) -> Result<(), Error> {
        let open = self.open.take().expect("an open shard");
        store.seal(&SealedShard {
            start_height: open.start,
            end_height: end,
            segments,
            rows: self.policy.rows,
            payloads: self.policy.payloads,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::common;
    use receiver_store::{Config, IndexedBlock};

    /// A store whose blocks from height 100 hold `blocks[i]` payments each, the last of
    /// a block to `last` when given, every commitment `[6; 32]`.
    fn store(dir: &std::path::Path, blocks: &[usize], last: Option<ReceiverBytes>) -> Store {
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
            append(&mut store, *count, last);
        }
        store
    }

    /// Appends a block of `count` payments after the store's tip.
    fn append(store: &mut Store, count: usize, last: Option<ReceiverBytes>) -> IndexedBlock {
        append_with(store, count, 0, last)
    }

    /// Appends a block of `count` payments and `extra` further commitments after the
    /// store's tip, returning it.
    fn append_with(
        store: &mut Store,
        count: usize,
        extra: usize,
        last: Option<ReceiverBytes>,
    ) -> IndexedBlock {
        let tip = store.tip().unwrap();
        let height = tip.height + 1;
        let hash = [height as u8; 32];
        let payments = (0..count)
            .map(|i| {
                let mut payment = common::record(0, 1).payment;
                (payment.height, payment.block_hash) = (height, hash);
                payment.position = tip.position + i as u64;
                payment.tx_index = i as u32 + 1;
                payment.txid = [0; 32];
                payment.txid[..8].copy_from_slice(&payment.position.to_le_bytes());
                payment.cmx = [6; 32];
                let receiver = match last {
                    Some(last) if i + 1 == count => last,
                    _ => common::receiver(),
                };
                (receiver, payment)
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

    /// The seals `policy` records over blocks of `blocks` payments, as
    /// `(start, end, segments)`.
    fn seals(policy: Policy, blocks: &[usize]) -> Vec<(u32, u32, u32)> {
        let dir = tempfile::tempdir().unwrap();
        let mut store = store(dir.path(), blocks, None);
        Sealer::new(policy).advance(&mut store, [1; 32]).unwrap();
        store
            .sealed_shards()
            .unwrap()
            .iter()
            .map(|s| (s.start_height, s.end_height, s.segments))
            .collect()
    }

    /// A one-row payment table holds 14 records: a block that would make placement fail
    /// seals the shard before it, never splitting a block across shards.
    #[test]
    fn a_block_that_would_not_place_seals_the_shard_before_it() {
        let policy = Policy {
            rows: 1,
            payloads: 1000,
            target: 1000,
        };
        assert_eq!(
            seals(policy, &[10, 3, 3, 0, 12]),
            [(100, 101, 1), (102, 103, 1)]
        );
    }

    /// Companion witness capacity seals a shard as placement does.
    #[test]
    fn witness_capacity_seals_the_shard_before_a_block() {
        let policy = Policy {
            rows: 2,
            payloads: 8,
            target: 1000,
        };
        assert_eq!(seals(policy, &[5, 3, 1, 7]), [(100, 101, 1)]);
    }

    /// Reaching the target seals after the block that reaches it.
    #[test]
    fn the_target_seals_after_the_block_that_reaches_it() {
        let policy = Policy {
            rows: 1,
            payloads: 1000,
            target: 4,
        };
        assert_eq!(
            seals(policy, &[2, 2, 1, 0, 3]),
            [(100, 101, 1), (102, 104, 1)]
        );
    }

    /// A block too large for one table seals the shard before it and becomes a shard of
    /// its own, split into segments that each place; one too large for every segment a
    /// shard may have is refused and never sealed.
    #[test]
    fn an_oversized_block_becomes_its_own_shard_of_segments() {
        let policy = Policy {
            rows: 1,
            payloads: 1000,
            target: 1000,
        };
        assert_eq!(seals(policy, &[3, 30, 2]), [(100, 100, 1), (101, 101, 3)]);
        let dir = tempfile::tempdir().unwrap();
        let mut store = store(dir.path(), &[3, MAX_SEGMENTS * SLOTS + 1], None);
        let refused = Sealer::new(policy).advance(&mut store, [1; 32]);
        assert!(matches!(refused, Err(Error::Capacity)));
        assert_eq!(store.sealed_shards().unwrap().len(), 1);
    }

    /// A stop requested while a shard builds ends it inside the build, between one
    /// segment's witness check and the next, rather than after the whole shard.
    #[test]
    fn a_stop_ends_a_shard_build_between_its_segments() {
        let policy = Policy {
            rows: 1,
            payloads: 1000,
            target: 1000,
        };
        let dir = tempfile::tempdir().unwrap();
        let mut store = store(dir.path(), &[30], Some(common::seeded_receiver(3)));
        Sealer::new(policy).advance(&mut store, [1; 32]).unwrap();
        let shard = store.sealed_shards().unwrap().remove(0);
        assert_eq!(shard.segments, 3);
        let tip = store.tip().unwrap();
        let mut witnesses = crate::witness::Witnesses::default();
        // Each segment asks before its companion, before its check and once per batch of
        // checked records: the fourth ask is the second segment's first.
        let asked = std::cell::Cell::new(0);
        let stop = || {
            asked.set(asked.get() + 1);
            asked.get() == 4
        };
        let stopped = crate::shards::sealed(&mut store, &mut witnesses, &shard, &tip, 0, &stop);
        assert!(matches!(stopped, Err(Error::Stopped)));
        assert_eq!(asked.get(), 4);
    }

    /// Every segment of an oversized shard places, a receiver paid only by the block's
    /// last payment is in exactly the last segment, and the shard's filter holds it.
    #[test]
    fn every_segment_of_an_oversized_shard_places() {
        let policy = Policy {
            rows: 1,
            payloads: 1000,
            target: 1000,
        };
        let dir = tempfile::tempdir().unwrap();
        let last = common::seeded_receiver(3);
        let mut store = store(dir.path(), &[30], Some(last));
        Sealer::new(policy).advance(&mut store, [1; 32]).unwrap();
        let shard = store.sealed_shards().unwrap().remove(0);
        assert_eq!((shard.end_height, shard.segments), (100, 3));
        let tip = store.tip().unwrap();
        let mut witnesses = crate::witness::Witnesses::default();
        let built =
            crate::shards::sealed(&mut store, &mut witnesses, &shard, &tip, 0, &|| false).unwrap();
        let counts: Vec<u64> = built
            .segments
            .iter()
            .map(|s| s.snapshot.manifest.records)
            .collect();
        assert_eq!(counts, [14, 14, 2]);
        let holds = |s: &crate::shards::Built| {
            let row =
                zakura_pir_publisher::receiver::snapshot::row_for(&s.snapshot.manifest, &last, 0)
                    .unwrap();
            let bytes =
                &s.snapshot.data[row * snapshot::ROW_BYTES..(row + 1) * snapshot::ROW_BYTES];
            zakura_pir_publisher::receiver::snapshot::lookup_row(
                &s.snapshot.manifest,
                &last,
                0,
                bytes,
            )
            .unwrap()
            .is_some()
        };
        let found: Vec<bool> = built.segments.iter().map(holds).collect();
        assert_eq!(found, [false, false, true]);
        for segment in &built.segments {
            segment.snapshot.validate().unwrap();
            crate::verify::check_publication(&segment.snapshot, &segment.witnesses).unwrap();
        }
        assert_eq!(
            built.filter.matches(&[last, common::receiver()]),
            [true, true]
        );
    }

    /// A correction adding a payment to the open shard is replayed by a sealer kept
    /// across it, sealing as a fresh one would.
    #[test]
    fn a_correction_inside_the_open_shard_replays_its_projection() {
        let policy = Policy {
            rows: 1,
            payloads: 1000,
            target: 4,
        };
        let dir = tempfile::tempdir().unwrap();
        let mut store = store(dir.path(), &[2], None);
        let mut block = append_with(&mut store, 1, 1, None);
        let mut sealer = Sealer::new(policy);
        sealer.advance(&mut store, [1; 32]).unwrap();
        assert!(store.sealed_shards().unwrap().is_empty());
        let mut added = block.payments[0].1.clone();
        added.position += 1;
        added.tx_index += 1;
        added.txid[..8].copy_from_slice(&added.position.to_le_bytes());
        block.payments.push((common::receiver(), added));
        store.correct(1, &[block]).unwrap();
        sealer.advance(&mut store, [1; 32]).unwrap();
        let seals: Vec<_> = store
            .sealed_shards()
            .unwrap()
            .iter()
            .map(|s| (s.start_height, s.end_height))
            .collect();
        assert_eq!(seals, [(100, 101)]);
    }

    /// Sealing block by block across restarts records the seals one pass records.
    #[test]
    fn incremental_sealing_matches_a_fresh_replay() {
        let policy = Policy {
            rows: 1,
            payloads: 1000,
            target: 6,
        };
        let blocks = [4, 0, 5, 9, 2, 14, 1];
        let dir = tempfile::tempdir().unwrap();
        let mut store = store(dir.path(), &[], None);
        let mut sealer = Sealer::new(policy);
        for (i, count) in blocks.iter().enumerate() {
            append(&mut store, *count, None);
            if i % 3 == 2 {
                sealer = Sealer::new(policy);
            }
            sealer.advance(&mut store, [1; 32]).unwrap();
        }
        let incremental: Vec<_> = store
            .sealed_shards()
            .unwrap()
            .iter()
            .map(|s| (s.start_height, s.end_height, s.segments))
            .collect();
        assert_eq!(incremental, seals(policy, &blocks));
        assert!(!incremental.is_empty());
    }
}
