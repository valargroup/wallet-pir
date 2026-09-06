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
//!   `sum over scripts of page_rows_for(events)` — not the event count, because
//!   pages are per script and padded, so many three-event histories cost far
//!   more rows than the same events in one long history.
//!
//! Both are monotone as blocks stream in, so one incremental pass decides every
//! boundary. Distinct transaction ids are counted and reported but do not seal:
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

use crate::layout::{page_rows_for, INLINE_EVENTS};
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
    pub page_rows: u64,
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
    next_shard_id: u64,
    /// Per-script event counts within the shard being accumulated.
    scripts: HashMap<Vec<u8>, u32>,
    txids: HashSet<Txid>,
    page_rows: u64,
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
        Self {
            policy,
            next_shard_id: 0,
            scripts: HashMap::new(),
            txids: HashSet::new(),
            page_rows: 0,
            events: 0,
            start_height: None,
            last_height: None,
            next_expected: None,
            first_height,
        }
    }

    fn occupancy(&self) -> Occupancy {
        Occupancy {
            scripts: self.scripts.len() as u64,
            page_rows: self.page_rows,
            txids: self.txids.len() as u64,
            events: self.events,
            inline_events: self
                .scripts
                .values()
                .map(|count| u64::from((*count).min(INLINE_EVENTS)))
                .sum(),
            blocks: match (self.start_height, self.last_height) {
                (Some(start), Some(last)) => last - start + 1,
                _ => 0,
            },
        }
    }

    fn reset(&mut self) {
        self.scripts.clear();
        self.txids.clear();
        self.page_rows = 0;
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
        let mut page_rows = self.page_rows;
        let mut inline_events = self.occupancy().inline_events;
        for (script, added) in &added_scripts {
            let existing = self.scripts.get(*script).copied().unwrap_or(0);
            if existing == 0 {
                scripts += 1;
            }
            page_rows += page_rows_for(existing + added) - page_rows_for(existing);
            inline_events += u64::from((existing + added).min(INLINE_EVENTS))
                - u64::from(existing.min(INLINE_EVENTS));
        }
        let txids = added_txids
            .iter()
            .filter(|txid| !self.txids.contains(*txid))
            .count() as u64
            + self.txids.len() as u64;

        Occupancy {
            scripts,
            page_rows,
            txids,
            events: self.events + events.len() as u64,
            inline_events,
            blocks: self.occupancy().blocks + 1,
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
        for (script, event) in events {
            let counter = self.scripts.entry(script.as_slice().to_vec()).or_insert(0);
            let before = page_rows_for(*counter);
            *counter += 1;
            self.page_rows += page_rows_for(*counter) - before;
            self.txids.insert(event.txid());
            self.events += 1;
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
}
