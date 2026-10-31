//! Sealing Receiver shards (plan S1.3).
//!
//! Each whole block is projected into the open segment under its committed tag salt
//! ([`Projection::project`]). A shard seals after the block that reaches
//! [`SEAL_TARGET`], and before a block that would pass the companion's capacity, take a
//! receiver past 65,535 payments, sit 2^24 or more heights after the shard's start, or
//! collide or fail placement under all of its own salt attempts. Every rule is decided
//! block by block, so the boundary never depends on when publication runs; the choice
//! table plays no part in it. A block too large for one segment becomes a shard of its
//! own, split in position order into up to eight segments.
//!
//! Seals are staged, not recorded: the publisher records each one only once its
//! segments, choice objects included, have been built, so a choice failure leaves the
//! recorded boundary where it was. Each seal names the block that decided it, and the
//! store drops the seal with that block.
use receiver_store::{Checkpoint, Error, SealedShard, Store};
use std::collections::HashMap;
use zakura_pir_publisher::receiver::inventory::MAX_SEGMENTS;
use zakura_pir_publisher::receiver::{
    initial_salt, within_limits, Projection, CAPACITY, MAX_HEIGHT_OFFSET, SEAL_SEED_ATTEMPTS,
    SEAL_TARGET,
};
use zakura_pir_publisher::receiver::{Hash, RawRecord, ReceiverBytes};

/// The limits a segment seals under: the format's ([`Policy::default`]); tests only
/// lower them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    /// Payments after which a shard seals at the next block boundary.
    pub target: u32,
    /// Payments one segment holds.
    pub capacity: u32,
    /// The largest height offset from a shard's start a block may have.
    pub max_offset: u32,
    /// Choice seeds a seal tries.
    pub seal_seeds: u32,
}

impl Default for Policy {
    /// The format's limits.
    fn default() -> Self {
        Self {
            target: SEAL_TARGET,
            capacity: CAPACITY,
            max_offset: MAX_HEIGHT_OFFSET,
            seal_seeds: SEAL_SEED_ATTEMPTS,
        }
    }
}

/// One segment's projection and payment counts, as blocks are added.
#[derive(Clone)]
pub struct Segment {
    pub projection: Projection,
    counts: HashMap<ReceiverBytes, u32>,
    payments: u32,
}

impl Segment {
    /// An empty segment `index` of the shard after block `parent`.
    pub fn new(parent: &Hash, index: u32) -> Self {
        Self {
            projection: Projection::new(initial_salt(parent, index)),
            counts: HashMap::new(),
            payments: 0,
        }
    }

    /// Adds `records`, paid by block `block` at `height` of a shard starting at
    /// `start`, if the limits hold and the projection places their receivers;
    /// otherwise changes nothing and returns false.
    pub fn add(
        &mut self,
        policy: &Policy,
        records: &[RawRecord],
        start: u32,
        height: u32,
        block: &Hash,
    ) -> bool {
        let mut counts: HashMap<ReceiverBytes, u32> = HashMap::new();
        for record in records {
            let count = counts
                .entry(record.receiver)
                .or_insert_with(|| self.counts.get(&record.receiver).copied().unwrap_or(0));
            *count = count.saturating_add(1);
        }
        let payments = u64::from(self.payments) + records.len() as u64;
        let most = counts.values().max().copied().unwrap_or(0);
        if !within_limits(payments, most.into(), start, height)
            || payments > u64::from(policy.capacity)
            || height - start > policy.max_offset
        {
            return false;
        }
        #[cfg(test)]
        if records.len() > 1 && tests::REFUSED.with(|r| r.borrow().contains(&(height, start))) {
            return false;
        }
        let paid: Vec<ReceiverBytes> = counts.keys().copied().collect();
        if self.projection.project(&paid, block).is_err() {
            return false;
        }
        self.counts.extend(counts);
        // At most the capacity, checked above.
        self.payments = payments as u32;
        true
    }
}

/// Splits one block's `records`, in position order, into consecutive segments of the
/// shard after `parent` that starts at the block, adding one payment at a time and
/// starting a segment when the current one refuses it. More than [`MAX_SEGMENTS`], or a
/// payment a fresh segment refuses, is [`Error::Capacity`]. Returns each segment with
/// its records.
pub fn split(
    policy: &Policy,
    parent: &Hash,
    height: u32,
    block: &Hash,
    records: &[RawRecord],
) -> Result<Vec<(Segment, Vec<RawRecord>)>, Error> {
    let mut segments = vec![(Segment::new(parent, 0), Vec::new())];
    for record in records {
        let (segment, held) = segments.last_mut().expect("one segment");
        if segment.add(policy, std::slice::from_ref(record), height, height, block) {
            held.push(record.clone());
            continue;
        }
        if segments.len() == MAX_SEGMENTS {
            return Err(Error::Capacity);
        }
        let mut segment = Segment::new(parent, segments.len() as u32);
        if !segment.add(policy, std::slice::from_ref(record), height, height, block) {
            return Err(Error::Capacity);
        }
        segments.push((segment, vec![record.clone()]));
    }
    Ok(segments)
}

/// The segments of sealed `shard`, projected again exactly as the sealer projected
/// them, each with its records in position order. A shard the stored blocks no longer
/// project into is [`Error::Coverage`].
pub fn replay(
    policy: &Policy,
    store: &Store,
    shard: &SealedShard,
) -> Result<Vec<(Segment, Vec<RawRecord>)>, Error> {
    let parent = store.checkpoint(shard.start_height - 1)?.hash;
    if shard.start_height == shard.end_height {
        // Whole if the block fits a fresh segment, else split, as the sealer decided.
        let (height, block) = (
            shard.start_height,
            store.checkpoint(shard.start_height)?.hash,
        );
        let records = store.records(height, height)?;
        let mut segment = Segment::new(&parent, 0);
        let parts = match segment.add(policy, &records, height, height, &block) {
            true => vec![(segment, records)],
            false => split(policy, &parent, height, &block, &records)?,
        };
        return match parts.len() == shard.segments as usize {
            true => Ok(parts),
            false => Err(Error::Coverage),
        };
    }
    let mut segment = Segment::new(&parent, 0);
    let mut held = Vec::new();
    for height in shard.start_height..=shard.end_height {
        let records = store.records(height, height)?;
        let block = store.checkpoint(height)?.hash;
        if !segment.add(policy, &records, shard.start_height, height, &block) {
            return Err(Error::Coverage);
        }
        held.extend(records);
    }
    Ok(vec![(segment, held)])
}

/// The open shard: its first block, the block before it, the last block it absorbed and
/// its segment.
struct Open {
    start: u32,
    parent: Checkpoint,
    through: Checkpoint,
    /// The store's correction revision the projection was built under.
    revision: u64,
    segment: Segment,
}

/// Decides seals incrementally across publications, staging each until the store
/// records it. Recreate on restart: it replays the open shard from the store.
#[derive(Default)]
pub struct Sealer {
    policy: Policy,
    open: Option<Open>,
    /// Seals decided but not yet recorded, each with its deciding block and the
    /// correction revision it was decided under.
    staged: Vec<(SealedShard, Checkpoint, u64)>,
}

impl Sealer {
    /// A sealer under `policy`.
    pub fn new(policy: Policy) -> Self {
        Self {
            policy,
            open: None,
            staged: Vec::new(),
        }
    }

    /// The policy new shards are sealed under.
    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// The open shard's first height and segment, once it absorbed a block.
    pub fn open(&self) -> Option<(u32, &Segment)> {
        self.open
            .as_ref()
            .filter(|open| open.through.height >= open.start)
            .map(|open| (open.start, &open.segment))
    }

    /// Absorbs every stored block after the last seal through the store's tip and
    /// returns the seals decided after the store's last one, in order, which the caller
    /// records with [`Store::seal`] once each shard is built. A block no number of
    /// segments can hold is an error that decides nothing for it, so coverage never
    /// passes it.
    pub fn advance(&mut self, store: &Store) -> Result<Vec<SealedShard>, Error> {
        let tip = store.tip()?;
        let revision = store.history_revision()?;
        let recorded = match store.sealed_shards()?.last() {
            Some(last) => last.end_height + 1,
            None => store.config().start_height,
        };
        // Seals the store has recorded since leave the stage; the rest stay only while
        // they continue the recorded ones and their deciding blocks and revision hold.
        self.staged.retain(|(s, ..)| s.start_height >= recorded);
        let holds = self
            .staged
            .first()
            .is_none_or(|(s, ..)| s.start_height == recorded)
            && self.staged.iter().all(|(_, cause, at)| {
                *at == revision && store.checkpoint(cause.height).ok().as_ref() == Some(cause)
            });
        if !holds {
            self.staged.clear();
        }
        loop {
            let next = self
                .staged
                .last()
                .map_or(recorded, |(s, ..)| s.end_height + 1);
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
                    segment: Segment::new(&parent.hash, 0),
                    through: parent.clone(),
                    parent,
                    revision,
                });
            }
            let open = self.open.as_mut().expect("set above");
            if open.through.height >= tip.height {
                return Ok(self.staged.iter().map(|(s, ..)| s.clone()).collect());
            }
            let height = open.through.height + 1;
            let records = store.records(height, height)?;
            let block = store.checkpoint(height)?;
            let sealed =
                if open
                    .segment
                    .add(&self.policy, &records, open.start, height, &block.hash)
                {
                    open.through = block.clone();
                    (open.segment.payments >= self.policy.target).then_some((height, 1, block))
                } else if open.through.height < open.start {
                    // A first block that does not fit a fresh segment whole is split into a
                    // shard of its own, even into one segment, as `replay` decides it.
                    let parts = split(
                        &self.policy,
                        &open.parent.hash,
                        height,
                        &block.hash,
                        &records,
                    )?;
                    Some((height, parts.len() as u32, block))
                } else {
                    // Seal before the block that does not fit, then retry it alone.
                    Some((open.through.height, 1, block))
                };
            if let Some((end, segments, cause)) = sealed {
                let open = self.open.take().expect("an open shard");
                let shard = SealedShard {
                    start_height: open.start,
                    end_height: end,
                    segments,
                    cause: cause.height,
                };
                self.staged.push((shard, cause, revision));
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
