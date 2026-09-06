//! Deciding where one shard ends and the next begins.
//!
//! Shards are sealed on what they *contain*, not on how many blocks they span.
//! Nothing bounds what a height range holds — transparent density on mainnet
//! varies several-fold across the chain, and a spam burst varies it further —
//! so a fixed-width shard would either waste most of a pinned table on a quiet
//! range or overflow it on a busy one. Sealing on content makes that variation
//! a difference in block span instead, which costs nothing.
//!
//! # Why three thresholds and not one
//!
//! The three tables are keyed differently, so no single quantity bounds them:
//!
//! - the filter and the directory are sized by *distinct scripts*;
//! - the pages table is sized by *page rows*, which is
//!   `sum over scripts of fragments_for(events)` — not the event count, because
//!   pages are per script and padded, so many three-event histories cost far
//!   more rows than the same events in one long history.
//!
//! Both are monotone as blocks stream in. That is a convenient property but not
//! the reason one incremental pass decides every boundary: what actually makes
//! the pass sound is that each block's per-script delta is *exact*, so the
//! projection a capacity decision is made on is the state absorbing will
//! produce. The distinction matters because packed row demand, carried here as
//! [`Occupancy::packed_page_rows`] and sealed on under [`PageBasis::Packed`],
//! is not monotone —
//! see [`crate::layout::PackedDemand`] for the counterexample. Nothing may
//! reason that content which does not fit now can never fit later.
//!
//! Distinct transaction ids are counted and reported but do not seal:
//! they size the optional transaction-detail table, which this POC does not
//! build. Over the Ironwood-to-tip journal that limit never bound in any
//! candidate policy, so dropping it moves no boundary.
//!
//! # Why capacity and target are separate numbers
//!
//! A threshold alone is not enough. Blocks arrive whole, and one block can add
//! thousands of scripts, so a shard that was just under a threshold could land
//! far past it — past what the pinned geometry actually holds. So each quantity
//! has two numbers: the `capacity` the tables really hold, and the `target` at
//! which sealing is preferred. A block that would breach capacity seals the
//! shard *before* it is added; reaching a target seals *after*. Targets should
//! sit below capacity by at least the largest block worth accommodating.
//!
//! # The block that fits nowhere
//!
//! Sealing cannot help a block whose own content exceeds a capacity: there is
//! nothing to seal before it. Refusing to publish it would let one purchased
//! block halt the service, so such a block is absorbed into a shard of its own,
//! and the builder gives that shard as many segments of the pinned geometry as
//! its content needs. The seal thresholds are per segment, so this changes
//! nothing for ordinary shards — the block that overran a capacity is above
//! every target too, so the shard closes immediately after it.

use crate::layout::{fragments_for, PackedDemand, INLINE_EVENTS};
use crate::records::MAX_SCRIPT_BYTES;
use std::collections::{HashMap, HashSet};
use transparent_events::{TransparentEvent, Txid};
use transparent_filter::ScriptBytes;

/// How full a shard is allowed to get.
///
/// `capacity` is what the pinned tables hold and is a hard limit. `target` is
/// where sealing is preferred, and must leave room for one more block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limit {
    pub target: u64,
    pub capacity: u64,
}

impl Limit {
    pub fn new(target: u64, capacity: u64) -> Result<Self, SealError> {
        if target == 0 || target > capacity {
            return Err(SealError::Limits(format!(
                "target {target} must be positive and at most capacity {capacity}"
            )));
        }
        Ok(Self { target, capacity })
    }
}

/// The seal parameters a shard set is built under.
///
/// These are schema, not tuning. Two shard sets built under different
/// thresholds are different partitions of the same chain, and a wallet holding
/// both would be holding incompatible coverage. Changing any of them re-shards.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealPolicy {
    /// Distinct scripts, which size the filter and the directory.
    pub scripts: Limit,
    /// Page rows, which size the pages table.
    pub page_rows: Limit,
}

/// Which page figure closes a shard.
///
/// Not part of [`SealPolicy`], which is schema: this selects between two ways
/// of counting the same content, and only one of them can be right for a given
/// builder. It exists so the packed figure can be sealed on and measured before
/// a packed builder exists to emit it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PageBasis {
    /// A row per fragment, which is what the v4 builder emits.
    #[default]
    Fragments,
    /// Short histories sharing rows, which is what the packed layout would ask
    /// for. Sealing on this while the builder still emits a row per fragment
    /// would undercount the table, so it is for measurement until the builder
    /// packs.
    Packed,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SealError {
    #[error("invalid seal limits: {0}")]
    Limits(String),
    #[error("block {height} does not follow {expected}")]
    OutOfOrder { height: u64, expected: u64 },
}

/// What a shard holds, as it accumulates.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Occupancy {
    pub scripts: u64,
    /// The figure that closes the shard, under the sealer's [`PageBasis`].
    pub page_rows: u64,
    /// A row per fragment, as the v4 builder emits them.
    pub fragments: u64,
    pub txids: u64,
    pub events: u64,
    /// Events that fit in directory entries and cost no page row.
    ///
    /// Tracked so `events - inline_events` gives the paged events exactly, and
    /// a caller can report how full the page rows actually are. Deriving it as
    /// `2 * scripts` would be wrong for every script holding a single event,
    /// which the measured distribution says is common.
    pub inline_events: u64,
    pub blocks: u64,
    /// Page rows the same content needs with short histories packed into shared
    /// rows.
    ///
    /// Equal to `page_rows` under [`PageBasis::Packed`]. Under
    /// [`PageBasis::Fragments`] it is a projection at boundaries chosen by the
    /// unpacked figure, and no packed table was built to match it.
    pub packed_page_rows: u64,
    /// The class counts behind `packed_page_rows`, for reporting.
    ///
    /// Excludes scripts too long for a directory entry, which the builder never
    /// pages. `page_rows` still counts them, which is why the two can disagree
    /// by more than packing alone explains.
    pub demand: PackedDemand,
}

impl Occupancy {
    /// Events that had to go into page rows.
    pub fn paged_events(&self) -> u64 {
        self.events.saturating_sub(self.inline_events)
    }
}

/// A sealed shard's extent and contents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedShard {
    pub shard_id: u64,
    pub start_height: u64,
    pub end_height: u64,
    pub occupancy: Occupancy,
    /// Which limit caused the seal. `None` for a shard closed by the end of the
    /// journal rather than by reaching a limit — that is the unsealed tail.
    pub reason: Option<SealReason>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SealReason {
    /// A target was reached after a block was added.
    ReachedTarget(&'static str),
    /// The next block would have breached a capacity, so the shard closed
    /// before it.
    WouldExceedCapacity(&'static str),
    /// The shard holds a single block that exceeds a capacity on its own, so it
    /// closed immediately after it. Its tables need more than one segment.
    BlockExceedsCapacity(&'static str),
}

/// Streams blocks in height order and emits shard boundaries.
///
/// Feed every block, in order, including empty ones — a height with no
/// supported activity is still part of a shard's span, and skipping it would
/// leave a gap that no consumer could distinguish from missing coverage.
pub struct Sealer {
    policy: SealPolicy,
    basis: PageBasis,
    next_shard_id: u64,
    /// Per-script event counts within the shard being accumulated.
    scripts: HashMap<Vec<u8>, u32>,
    txids: HashSet<Txid>,
    fragments: u64,
    /// Maintained rather than recomputed: `occupancy()` is reached up to five
    /// times per block, and a full scan of the script map each time is the
    /// difference between a runnable and an unrunnable full-journal census.
    inline_events: u64,
    /// Packed row demand, carried as a passenger. See [`Occupancy::packed_page_rows`].
    demand: PackedDemand,
    events: u64,
    start_height: Option<u64>,
    last_height: Option<u64>,
    /// The height the next block must have. Tracked separately from the shard
    /// being accumulated because it has to survive a seal: after a shard
    /// closes, the accumulator is empty but the journal has not restarted.
    next_expected: Option<u64>,
    first_height: u64,
}

impl Sealer {
    pub fn new(policy: SealPolicy, first_height: u64) -> Self {
        Self::with_basis(policy, first_height, PageBasis::default())
    }

    /// A sealer that closes on `basis` rather than on the default.
    pub fn with_basis(policy: SealPolicy, first_height: u64, basis: PageBasis) -> Self {
        Self {
            policy,
            basis,
            next_shard_id: 0,
            scripts: HashMap::new(),
            txids: HashSet::new(),
            fragments: 0,
            inline_events: 0,
            demand: PackedDemand::default(),
            events: 0,
            start_height: None,
            last_height: None,
            next_expected: None,
            first_height,
        }
    }

    /// The page figure a limit is compared against.
    fn page_rows(basis: PageBasis, fragments: u64, packed: u64) -> u64 {
        match basis {
            PageBasis::Fragments => fragments,
            PageBasis::Packed => packed,
        }
    }

    fn occupancy(&self) -> Occupancy {
        let packed = self.demand.rows();
        Occupancy {
            scripts: self.scripts.len() as u64,
            page_rows: Self::page_rows(self.basis, self.fragments, packed),
            fragments: self.fragments,
            txids: self.txids.len() as u64,
            events: self.events,
            inline_events: self.inline_events,
            blocks: match (self.start_height, self.last_height) {
                (Some(start), Some(last)) => last - start + 1,
                _ => 0,
            },
            packed_page_rows: packed,
            demand: self.demand,
        }
    }

    fn reset(&mut self) {
        self.scripts.clear();
        self.txids.clear();
        self.fragments = 0;
        self.inline_events = 0;
        self.demand = PackedDemand::default();
        self.events = 0;
        self.start_height = None;
        self.last_height = None;
    }

    fn close(&mut self, reason: Option<SealReason>) -> SealedShard {
        let shard = SealedShard {
            shard_id: self.next_shard_id,
            start_height: self.start_height.expect("a shard being closed has a start"),
            end_height: self.last_height.expect("a shard being closed has an end"),
            occupancy: self.occupancy(),
            reason,
        };
        self.next_shard_id += 1;
        self.reset();
        shard
    }

    /// What this shard's occupancy would become if `events` were added.
    ///
    /// Computed without mutating, because the answer decides whether the block
    /// belongs to this shard at all.
    fn projected(&self, events: &[(ScriptBytes, TransparentEvent)]) -> Occupancy {
        let mut added_scripts: HashMap<&[u8], u32> = HashMap::new();
        let mut added_txids: HashSet<Txid> = HashSet::new();
        for (script, event) in events {
            *added_scripts.entry(script.as_slice()).or_insert(0) += 1;
            added_txids.insert(event.txid());
        }

        let mut scripts = self.scripts.len() as u64;
        let mut fragments = self.fragments;
        let mut inline_events = self.inline_events;
        let mut demand = self.demand;
        for (script, added) in &added_scripts {
            let existing = self.scripts.get(*script).copied().unwrap_or(0);
            if existing == 0 {
                scripts += 1;
            }
            fragments += fragments_for(existing + added) - fragments_for(existing);
            inline_events += u64::from((existing + added).min(INLINE_EVENTS))
                - u64::from(existing.min(INLINE_EVENTS));
            if script.len() <= MAX_SCRIPT_BYTES {
                demand.shift(existing, existing + added);
            }
        }
        let txids = added_txids
            .iter()
            .filter(|txid| !self.txids.contains(*txid))
            .count() as u64
            + self.txids.len() as u64;

        let packed = demand.rows();
        Occupancy {
            scripts,
            page_rows: Self::page_rows(self.basis, fragments, packed),
            fragments,
            txids,
            events: self.events + events.len() as u64,
            inline_events,
            blocks: match (self.start_height, self.last_height) {
                (Some(start), Some(last)) => last - start + 1 + 1,
                _ => 1,
            },
            packed_page_rows: packed,
            demand,
        }
    }

    fn over_capacity(&self, projected: &Occupancy) -> Option<(&'static str, u64, u64)> {
        for (name, value, limit) in [
            ("scripts", projected.scripts, self.policy.scripts),
            ("page rows", projected.page_rows, self.policy.page_rows),
        ] {
            if value > limit.capacity {
                return Some((name, value, limit.capacity));
            }
        }
        None
    }

    fn reached_target(&self) -> Option<&'static str> {
        let occupancy = self.occupancy();
        for (name, value, limit) in [
            ("scripts", occupancy.scripts, self.policy.scripts),
            ("page rows", occupancy.page_rows, self.policy.page_rows),
        ] {
            if value >= limit.target {
                return Some(name);
            }
        }
        None
    }

    fn absorb(&mut self, height: u64, events: &[(ScriptBytes, TransparentEvent)]) {
        if self.start_height.is_none() {
            self.start_height = Some(height);
        }
        self.last_height = Some(height);

        // Aggregate the block by script before applying it, so this runs the
        // same per-script delta as `projected` rather than a second code path.
        // Applying events one at a time would also walk a history through every
        // intermediate packing class on its way to the one it lands in.
        let mut added: HashMap<&[u8], u32> = HashMap::new();
        for (script, event) in events {
            *added.entry(script.as_slice()).or_insert(0) += 1;
            self.txids.insert(event.txid());
            self.events += 1;
        }
        for (script, added) in added {
            let existing = self.scripts.get(script).copied().unwrap_or(0);
            let updated = existing + added;
            self.fragments += fragments_for(updated) - fragments_for(existing);
            self.inline_events +=
                u64::from(updated.min(INLINE_EVENTS)) - u64::from(existing.min(INLINE_EVENTS));
            // A script too long for a directory entry is filtered publicly but
            // never paged, so it contributes no packed rows. `page_rows` still
            // counts it, which is the v4 behaviour this projection rides on.
            if script.len() <= MAX_SCRIPT_BYTES {
                self.demand.shift(existing, updated);
            }
            self.scripts.insert(script.to_vec(), updated);
        }
    }

    /// Offers one block to the sealer, returning the shards it sealed.
    ///
    /// Usually none or one. A shard returned *before* this block was closed
    /// because adding the block would have breached a capacity, and does not
    /// contain it; a shard closed by reaching a target does contain it.
    ///
    /// Two shards come back only for a block that breaches a capacity on its
    /// own: the shard that ended before it, and the single-block shard holding
    /// it. That shard needs more than one segment per table, and the builder
    /// gives it those — no valid block is ever refused, because a service that
    /// stops publishing on one adversarial block is not available.
    pub fn push_block(
        &mut self,
        height: u64,
        events: &[(ScriptBytes, TransparentEvent)],
    ) -> Result<Vec<SealedShard>, SealError> {
        let expected = self.next_expected.unwrap_or(self.first_height);
        if height != expected {
            return Err(SealError::OutOfOrder { height, expected });
        }

        let mut sealed = Vec::new();
        let mut oversized = None;
        if let Some((quantity, _, _)) = self.over_capacity(&self.projected(events)) {
            if self.start_height.is_some() {
                sealed.push(self.close(Some(SealReason::WouldExceedCapacity(quantity))));
            }
            // Re-check against the now-empty shard. Still over means the block
            // exceeds a capacity by itself, so it becomes a shard of its own
            // and is sealed as soon as it is absorbed.
            if let Some((quantity, _, _)) = self.over_capacity(&self.projected(events)) {
                oversized = Some(quantity);
            }
        }

        self.absorb(height, events);
        self.next_expected = Some(height + 1);

        if let Some(quantity) = oversized {
            sealed.push(self.close(Some(SealReason::BlockExceedsCapacity(quantity))));
        } else if let Some(quantity) = self.reached_target() {
            sealed.push(self.close(Some(SealReason::ReachedTarget(quantity))));
        }
        Ok(sealed)
    }

    /// Closes whatever is still accumulating.
    ///
    /// The result is the tail: a shard that reached no limit and is therefore
    /// still growing as the chain does. It is published as a provisional
    /// revision, immutable under its own digest but expected to be superseded,
    /// and a consumer must not treat the range it covers as settled.
    pub fn finish(&mut self) -> Option<SealedShard> {
        self.start_height?;
        Some(self.close(None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::EVENTS_PER_PAGE;
    use transparent_events::ReceiveEvent;

    fn policy(scripts: (u64, u64), page_rows: (u64, u64)) -> SealPolicy {
        SealPolicy {
            scripts: Limit::new(scripts.0, scripts.1).unwrap(),
            page_rows: Limit::new(page_rows.0, page_rows.1).unwrap(),
        }
    }

    fn generous() -> SealPolicy {
        policy((1_000_000, 2_000_000), (1_000_000, 2_000_000))
    }

    fn script(tag: u32) -> ScriptBytes {
        let mut bytes = vec![0x76, 0xa9, 0x14];
        bytes.extend_from_slice(&tag.to_le_bytes());
        ScriptBytes::new(bytes)
    }

    fn event(height: u64, tag: u32, nonce: u32) -> (ScriptBytes, TransparentEvent) {
        let mut txid = [0u8; 32];
        txid[..4].copy_from_slice(&tag.to_le_bytes());
        txid[4..8].copy_from_slice(&nonce.to_le_bytes());
        (
            script(tag),
            TransparentEvent::Receive(ReceiveEvent {
                height: height as u32,
                txid: Txid(txid),
                transaction_index: 0,
                output_index: 0,
                value: 1,
                coinbase: false,
            }),
        )
    }

    /// The single shard a push was expected to seal.
    fn one(sealed: Result<Vec<SealedShard>, SealError>) -> SealedShard {
        let mut sealed = sealed.expect("no valid block is ever refused");
        assert_eq!(sealed.len(), 1, "expected exactly one sealed shard");
        sealed.pop().expect("one shard")
    }

    /// Distinct scripts, each with one event, so only the script limit bites.
    fn block_of_new_scripts(
        height: u64,
        first_tag: u32,
        count: u32,
    ) -> Vec<(ScriptBytes, TransparentEvent)> {
        (0..count)
            .map(|i| event(height, first_tag + i, 0))
            .collect()
    }

    #[test]
    fn an_empty_journal_seals_nothing() {
        let mut sealer = Sealer::new(generous(), 100);
        assert_eq!(sealer.finish(), None);
    }

    /// A shard that never reaches a limit is the tail: it must come out
    /// unsealed, so a consumer does not treat a still-growing range as
    /// immutable.
    #[test]
    fn a_journal_below_every_limit_yields_one_unsealed_tail() {
        let mut sealer = Sealer::new(generous(), 100);
        for height in 100..110 {
            assert_eq!(
                sealer.push_block(height, &block_of_new_scripts(height, 0, 3)),
                Ok(Vec::new())
            );
        }
        let tail = sealer.finish().expect("a tail");
        assert_eq!(tail.shard_id, 0);
        assert_eq!((tail.start_height, tail.end_height), (100, 109));
        assert_eq!(tail.reason, None);
        assert_eq!(tail.occupancy.blocks, 10);
        // Ten blocks of the same three scripts: three distinct scripts, thirty
        // events.
        assert_eq!(tail.occupancy.scripts, 3);
        assert_eq!(tail.occupancy.events, 30);
    }

    #[test]
    fn reaching_the_script_target_seals_the_block_that_reached_it() {
        let mut sealer = Sealer::new(policy((10, 1_000), (1_000, 10_000)), 100);
        assert_eq!(
            sealer.push_block(100, &block_of_new_scripts(100, 0, 4)),
            Ok(Vec::new())
        );
        let sealed = one(sealer.push_block(101, &block_of_new_scripts(101, 4, 6)));
        assert_eq!(sealed.reason, Some(SealReason::ReachedTarget("scripts")));
        assert_eq!((sealed.start_height, sealed.end_height), (100, 101));
        assert_eq!(sealed.occupancy.scripts, 10);
    }

    /// The case a single threshold cannot handle: a block large enough to carry
    /// the shard past what its tables hold. It must be sealed *before* the
    /// block, not after.
    #[test]
    fn a_block_that_would_breach_capacity_seals_the_shard_before_it() {
        let mut sealer = Sealer::new(policy((100, 120), (10_000, 20_000)), 100);
        assert_eq!(
            sealer.push_block(100, &block_of_new_scripts(100, 0, 90)),
            Ok(Vec::new())
        );
        // 90 + 50 = 140, past the capacity of 120, so this block starts a new
        // shard instead of overflowing the current one.
        let sealed = one(sealer.push_block(101, &block_of_new_scripts(101, 1_000, 50)));
        assert_eq!(
            sealed.reason,
            Some(SealReason::WouldExceedCapacity("scripts"))
        );
        assert_eq!((sealed.start_height, sealed.end_height), (100, 100));
        assert_eq!(sealed.occupancy.scripts, 90);

        let tail = sealer.finish().expect("the block that did not fit");
        assert_eq!((tail.start_height, tail.end_height), (101, 101));
        assert_eq!(tail.occupancy.scripts, 50);
    }

    /// No shard may exceed its capacity, whatever the block sizes. This is the
    /// property the shared parameter set depends on.
    #[test]
    fn no_sealed_shard_ever_exceeds_capacity() {
        let policy = policy((60, 100), (10_000, 20_000));
        let mut sealer = Sealer::new(policy, 100);
        let mut shards = Vec::new();
        let mut tag = 0u32;
        // Deliberately irregular block sizes, including several that alone are
        // a large fraction of capacity.
        for (offset, size) in [7u32, 40, 3, 55, 1, 90, 12, 30, 0, 45]
            .into_iter()
            .enumerate()
        {
            let height = 100 + offset as u64;
            let block = block_of_new_scripts(height, tag, size);
            tag += size;
            shards.extend(sealer.push_block(height, &block).unwrap());
        }
        shards.extend(sealer.finish());
        assert!(
            shards.len() > 1,
            "the fixture should produce several shards"
        );
        for shard in &shards {
            assert!(
                shard.occupancy.scripts <= policy.scripts.capacity,
                "shard {} holds {} scripts, capacity {}",
                shard.shard_id,
                shard.occupancy.scripts,
                policy.scripts.capacity
            );
        }
    }

    /// Boundaries must tile the journal exactly: gapless, non-overlapping, and
    /// ascending in shard id. A gap would be indistinguishable from a range the
    /// operator chose not to publish.
    #[test]
    fn shards_tile_the_journal_without_gaps_or_overlaps() {
        let mut sealer = Sealer::new(policy((25, 60), (10_000, 20_000)), 100);
        let mut shards = Vec::new();
        let mut tag = 0u32;
        for offset in 0..20u64 {
            let height = 100 + offset;
            let block = block_of_new_scripts(height, tag, 7);
            tag += 7;
            shards.extend(sealer.push_block(height, &block).unwrap());
        }
        shards.extend(sealer.finish());

        assert_eq!(shards[0].start_height, 100);
        assert_eq!(shards.last().unwrap().end_height, 119);
        for (index, shard) in shards.iter().enumerate() {
            assert_eq!(shard.shard_id, index as u64);
            assert!(shard.end_height >= shard.start_height);
            if index > 0 {
                assert_eq!(shard.start_height, shards[index - 1].end_height + 1);
            }
        }
        // Only the last shard may be unsealed. This fixture happens to end
        // exactly on a boundary, so it has no partial tail at all — which is a
        // legitimate outcome, not a missing one.
        assert!(shards[..shards.len() - 1]
            .iter()
            .all(|s| s.reason.is_some()));
    }

    /// A block larger than any single segment is still published. It becomes a
    /// shard of its own, which the builder gives the segments it needs; failing
    /// instead would let one purchased block stop the service.
    #[test]
    fn a_block_larger_than_a_segment_becomes_a_shard_of_its_own() {
        let mut sealer = Sealer::new(policy((10, 20), (10_000, 20_000)), 100);
        let sealed = one(sealer.push_block(100, &block_of_new_scripts(100, 0, 50)));
        assert_eq!(
            sealed.reason,
            Some(SealReason::BlockExceedsCapacity("scripts"))
        );
        assert_eq!((sealed.start_height, sealed.end_height), (100, 100));
        assert_eq!(sealed.occupancy.scripts, 50);
        assert_eq!(sealer.finish(), None, "the oversized block took the shard");
    }

    /// An oversized block arriving mid-shard closes the shard before it and
    /// then seals as its own, so the accumulating shard is never overrun and
    /// the block is never carried into a fresh one that could not hold it.
    #[test]
    fn an_oversized_block_after_a_seal_closes_two_shards() {
        let mut sealer = Sealer::new(policy((10, 20), (10_000, 20_000)), 100);
        assert_eq!(
            sealer.push_block(100, &block_of_new_scripts(100, 0, 8)),
            Ok(Vec::new())
        );
        let sealed = sealer
            .push_block(101, &block_of_new_scripts(101, 100, 50))
            .expect("no valid block is ever refused");
        assert_eq!(sealed.len(), 2);
        assert_eq!(
            sealed[0].reason,
            Some(SealReason::WouldExceedCapacity("scripts"))
        );
        assert_eq!((sealed[0].start_height, sealed[0].end_height), (100, 100));
        assert_eq!(
            sealed[1].reason,
            Some(SealReason::BlockExceedsCapacity("scripts"))
        );
        assert_eq!((sealed[1].start_height, sealed[1].end_height), (101, 101));
        assert_eq!(sealed[1].occupancy.scripts, 50);
    }

    /// The availability property, over irregular blocks including several that
    /// exceed a capacity on their own: every block is placed, every shard is
    /// non-empty, and the boundaries still tile the journal.
    #[test]
    fn no_valid_block_is_ever_refused() {
        let policy = policy((60, 100), (10_000, 20_000));
        let mut sealer = Sealer::new(policy, 100);
        let mut shards = Vec::new();
        let mut tag = 0u32;
        for (offset, size) in [7u32, 400, 3, 55, 1, 250, 12, 30, 0, 45]
            .into_iter()
            .enumerate()
        {
            let height = 100 + offset as u64;
            let block = block_of_new_scripts(height, tag, size);
            tag += size;
            shards.extend(
                sealer
                    .push_block(height, &block)
                    .expect("no valid block is ever refused"),
            );
        }
        shards.extend(sealer.finish());

        assert_eq!(shards[0].start_height, 100);
        assert_eq!(shards.last().unwrap().end_height, 109);
        for (index, shard) in shards.iter().enumerate() {
            assert_eq!(shard.shard_id, index as u64);
            if index > 0 {
                assert_eq!(shard.start_height, shards[index - 1].end_height + 1);
            }
        }
        // The shards that exceed capacity are exactly the single-block ones,
        // and they are the ones the builder must give extra segments.
        for shard in &shards {
            if shard.occupancy.scripts > policy.scripts.capacity {
                assert_eq!(shard.start_height, shard.end_height);
                assert_eq!(
                    shard.reason,
                    Some(SealReason::BlockExceedsCapacity("scripts"))
                );
            }
        }
    }

    /// Pages, not events, drive the pages table. A shard full of long histories
    /// should seal on page rows even though its script count stays small.
    #[test]
    fn a_shard_can_seal_on_page_rows_with_few_scripts() {
        let mut sealer = Sealer::new(policy((10_000, 20_000), (4, 10)), 100);
        // One script accumulating a long history: each full page is one row.
        let per_block = (INLINE_EVENTS + EVENTS_PER_PAGE) as usize;
        let mut sealed = None;
        for offset in 0..10u64 {
            let height = 100 + offset;
            let block: Vec<_> = (0..per_block)
                .map(|nonce| event(height, 0, offset as u32 * 1_000 + nonce as u32))
                .collect();
            let closed = sealer.push_block(height, &block).unwrap();
            if let Some(shard) = closed.into_iter().next() {
                sealed = Some(shard);
                break;
            }
        }
        let sealed = sealed.expect("page rows should seal a shard");
        assert_eq!(sealed.reason, Some(SealReason::ReachedTarget("page rows")));
        assert_eq!(sealed.occupancy.scripts, 1, "only one script was involved");
        assert!(sealed.occupancy.page_rows >= 4);
    }

    #[test]
    fn blocks_must_arrive_in_order() {
        let mut sealer = Sealer::new(generous(), 100);
        assert_eq!(
            sealer.push_block(101, &[]),
            Err(SealError::OutOfOrder {
                height: 101,
                expected: 100
            })
        );
        sealer.push_block(100, &[]).unwrap();
        assert_eq!(
            sealer.push_block(102, &[]),
            Err(SealError::OutOfOrder {
                height: 102,
                expected: 101
            })
        );
    }

    /// Empty blocks are part of a shard's span. Dropping them would make the
    /// span disagree with the chain it claims to cover.
    #[test]
    fn empty_blocks_extend_the_span_without_adding_occupancy() {
        let mut sealer = Sealer::new(generous(), 100);
        sealer
            .push_block(100, &block_of_new_scripts(100, 0, 2))
            .unwrap();
        for height in 101..105 {
            sealer.push_block(height, &[]).unwrap();
        }
        let tail = sealer.finish().unwrap();
        assert_eq!((tail.start_height, tail.end_height), (100, 104));
        assert_eq!(tail.occupancy.blocks, 5);
        assert_eq!(tail.occupancy.scripts, 2);
        assert_eq!(tail.occupancy.events, 2);
    }

    /// Sealing must be a function of the journal alone, so two operators
    /// reproduce the same boundaries and their digests stay comparable.
    #[test]
    fn the_same_journal_seals_identically_every_time() {
        let policy = policy((25, 60), (10_000, 20_000));
        let run = || {
            let mut sealer = Sealer::new(policy, 100);
            let mut shards = Vec::new();
            let mut tag = 0u32;
            for offset in 0..20u64 {
                let height = 100 + offset;
                let block = block_of_new_scripts(height, tag, 7);
                tag += 7;
                shards.extend(sealer.push_block(height, &block).unwrap());
            }
            shards.extend(sealer.finish());
            shards
        };
        assert_eq!(run(), run());
    }

    /// A journal that ends exactly where a shard sealed has no partial tail.
    /// Emitting an empty one would publish a shard covering no blocks.
    #[test]
    fn a_journal_ending_on_a_boundary_leaves_no_tail() {
        let mut sealer = Sealer::new(policy((7, 100), (10_000, 20_000)), 100);
        let sealed = one(sealer.push_block(100, &block_of_new_scripts(100, 0, 7)));
        assert_eq!(sealed.occupancy.scripts, 7);
        assert_eq!(sealer.finish(), None);
    }

    /// Sealing must survive across shards: the height check is about the
    /// journal, not about the shard currently accumulating.
    #[test]
    fn ordering_is_enforced_across_a_seal() {
        let mut sealer = Sealer::new(policy((7, 100), (10_000, 20_000)), 100);
        sealer
            .push_block(100, &block_of_new_scripts(100, 0, 7))
            .unwrap();
        assert_eq!(
            sealer.push_block(102, &[]),
            Err(SealError::OutOfOrder {
                height: 102,
                expected: 101
            })
        );
        assert_eq!(sealer.push_block(101, &[]), Ok(Vec::new()));
    }

    #[test]
    fn limits_must_be_positive_and_ordered() {
        assert!(Limit::new(0, 10).is_err());
        assert!(Limit::new(11, 10).is_err());
        assert!(Limit::new(10, 10).is_ok());
    }

    /// `inline_events` is maintained rather than recomputed, so it has to be
    /// checked against the scan it replaced. Nothing else would notice a drift.
    #[test]
    fn maintained_counters_match_a_full_recount() {
        let mut sealer = Sealer::new(generous(), 100);
        for height in 100..160u64 {
            // Reuse tags across blocks so histories grow, and mix in fresh ones.
            let mut block = Vec::new();
            for i in 0..12u32 {
                block.push(event(height, (height as u32 * 3 + i) % 40, i));
            }
            sealer.push_block(height, &block).unwrap();

            let occupancy = sealer.occupancy();
            let recounted: u64 = sealer
                .scripts
                .values()
                .map(|count| u64::from((*count).min(INLINE_EVENTS)))
                .sum();
            assert_eq!(occupancy.inline_events, recounted, "at height {height}");

            let fragments: u64 = sealer.scripts.values().map(|c| fragments_for(*c)).sum();
            assert_eq!(occupancy.fragments, fragments, "at height {height}");
        }
    }

    /// Packing never costs more rows than giving each history its own, and a
    /// block never adds more rows than it touches scripts.
    ///
    /// The second bound is what replaces monotonicity. Packed demand is not
    /// monotone — see `layout::tests::row_demand_can_fall_as_events_arrive` —
    /// so "a shard that overshot its target stays overshot" is not available as
    /// a reason for the gap between target and capacity. This bound is.
    #[test]
    fn a_block_adds_at_most_one_row_per_script_it_touches() {
        let mut sealer = Sealer::new(generous(), 100);
        for height in 100..200u64 {
            let mut block = Vec::new();
            // Irregular: some blocks empty, some wide, histories of every shape.
            for i in 0..(height as u32 % 17) {
                block.push(event(height, (height as u32 * 5 + i) % 60, i));
            }
            let touched: std::collections::HashSet<&[u8]> =
                block.iter().map(|(s, _)| s.as_slice()).collect();

            let before = sealer.occupancy().packed_page_rows;
            sealer.push_block(height, &block).unwrap();
            let after = sealer.occupancy();

            assert!(
                after.packed_page_rows <= before + touched.len() as u64,
                "height {height}: {before} -> {} for {} scripts",
                after.packed_page_rows,
                touched.len()
            );
            assert!(
                after.packed_page_rows <= after.fragments,
                "height {height}: packing cost more than not packing"
            );
        }
    }

    /// The projection a capacity decision is made on must equal what absorbing
    /// actually produces, for the packed figure as much as the unpacked one.
    /// If those drift, the builder's row count and the sealer's stop agreeing.
    #[test]
    fn the_projection_equals_what_absorbing_produces() {
        let mut sealer = Sealer::new(generous(), 100);
        for height in 100..180u64 {
            let block: Vec<_> = (0..(height as u32 % 9))
                .map(|i| event(height, (height as u32 * 7 + i) % 30, i))
                .collect();
            let projected = sealer.projected(&block);
            sealer.push_block(height, &block).unwrap();
            assert_eq!(sealer.occupancy(), projected, "at height {height}");
        }
    }

    /// Sealing on packed demand is what makes a smaller page table possible:
    /// the same journal, the same page limit, and far fewer shards, because the
    /// limit stops being reached by padding.
    ///
    /// The two bases must not be mixed. A sealer closing on packed rows against
    /// a builder that still emits one row per fragment would size the table for
    /// less than the builder writes, and the difference would come back as
    /// extra segments — which every wallet querying that generation pays for,
    /// because it must query all of them.
    #[test]
    fn the_basis_decides_where_a_shard_ends() {
        let policy = policy((1_000_000, 2_000_000), (40, 50));
        let mut counts = Vec::new();
        for basis in [PageBasis::Fragments, PageBasis::Packed] {
            let mut sealer = Sealer::with_basis(policy, 100, basis);
            let mut shards = Vec::new();
            for height in 100..200u64 {
                // Three events per script puts every one of them in class 1,
                // where 22 share a row.
                let block: Vec<_> = (0..3u32)
                    .flat_map(|nonce| {
                        (0..4u32).map(move |i| event(height, height as u32 * 4 + i, nonce))
                    })
                    .collect();
                shards.extend(sealer.push_block(height, &block).unwrap());
            }
            shards.extend(sealer.finish());
            counts.push(shards.len());
        }
        assert!(
            counts[1] < counts[0],
            "packed sealing should need fewer shards: {counts:?}"
        );
    }

    /// A script too long for a directory entry is filtered publicly and never
    /// paged, so it must not appear in packed demand. It still counts toward
    /// the unpacked figure, which is the v4 behaviour this projection rides on.
    #[test]
    fn an_unindexable_script_contributes_no_packed_rows() {
        let mut sealer = Sealer::new(generous(), 100);
        let long = ScriptBytes::new(vec![0x51; MAX_SCRIPT_BYTES + 1]);
        let (_, sample) = event(100, 0, 0);
        let block: Vec<_> = (0..10)
            .map(|i| {
                let (_, e) = event(100, 0, i);
                (long.clone(), e)
            })
            .collect();
        let _ = sample;
        sealer.push_block(100, &block).unwrap();

        let occupancy = sealer.occupancy();
        assert_eq!(occupancy.scripts, 1);
        assert!(
            occupancy.fragments > 0,
            "the unpacked figure still counts it"
        );
        assert_eq!(occupancy.packed_page_rows, 0);
        assert_eq!(occupancy.demand.paged_scripts(), 0);
    }
}
