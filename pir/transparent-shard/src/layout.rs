//! Pinned table geometry, identical for every shard.
//!
//! Uniformity is the whole point. `ipir_sp::params_for_simplepir` is a pure
//! function of `(rows, item_size_bits)`, so shards that share a geometry share
//! one set of scheme parameters — a client validates parameters and derives its
//! query setup once for every shard it will ever query, instead of once per
//! shard. That saving is only available if *every* shard fits the same
//! geometry, which is what the seal logic exists to guarantee.
//!
//! Both tables use the scheme's smallest instance width, 3,584 bytes. The
//! directory carries two inline events per entry; the page row is sized for
//! storage rather than for the query count of the longest history, which is
//! what [`PAGE_ROW_BYTES`] explains.
//!
//! # Segments
//!
//! Row *counts* are pinned per segment, not per shard. A shard normally has one
//! segment of each table and nothing below is visible; a shard whose content
//! does not fit one segment gets another of the same geometry, so the parameter
//! set stays shared. A shard's logical row space is its segments concatenated,
//! and a row index is taken over that whole space — which is why placement
//! stays even instead of spilling into a last segment, and why a page extent
//! that crosses a segment is ordinary addressing.

use transparent_events::EVENT_BYTES;

/// Bytes in one directory row.
pub const DIRECTORY_ROW_BYTES: usize = 3_584;

/// Bytes in one page row.
///
/// The scheme's smallest instance, and chosen for storage rather than for query
/// count. A page belongs to one script, so a row is only ever as full as that
/// script's history: the measured distribution is p50 2 events, p90 5, p95 8,
/// which means a script that touches a page at all typically fills a few per
/// cent of it. At 17,920 bytes the published set stored 16.1 events per used
/// page row of 185 slots — 8.7% — and 6.04% of the fleet's pinned bytes were
/// real data.
///
/// The mainnet study's sweep picked 17,920 on a different criterion: it cut one
/// 9,152-event outlier from 255 page queries to 50. That study also recorded
/// that the sweep "does not select one global row width for both tables", which
/// is what this width acts on.
///
/// The trade is real and falls on the heaviest histories, which need
/// proportionally more rows. The design's answer for those is query budgets and
/// resumable work, not a wider row for everyone.
pub const PAGE_ROW_BYTES: usize = 3_584;

/// Rows in every shard's directory table.
///
/// Pinned, and identical for every shard: `params_for_simplepir` derives the
/// PIR parameters from the row count, so a shard with its own row count would
/// need its own parameter set. It pads rows to a multiple of 2,048, so a
/// smaller count buys nothing.
///
/// At 14 slots per row this holds 28,672 scripts against a seal target around
/// 8,000 — slack that is what makes two-choice placement succeed without ever
/// needing to relocate an entry.
pub const DIRECTORY_ROWS: usize = 2_048;

/// Rows in every shard's page table, per segment.
///
/// Sized so the two tables fill together, and measured rather than reasoned:
/// see the geometry sweep archived under
/// `docs/transparent-pir-evaluation/shard-utilisation/`.
///
/// Packing short histories into shared rows cut row demand roughly in half, and
/// at 8,192 that saving was invisible — a table padded to 8,192 costs the same
/// whether 6,763 rows are used or 3,489, so every row packing recovered was
/// already padding. At 4,096 the same journal stores 132.1 MB where the v4
/// layout stored 220.2 MB, over the same six generations, with no generation
/// needing a second segment.
///
/// Halving again loses. At 2,048 the page limit closes nearly every generation,
/// the range splits into eleven, and pinned bytes rise to 161.5 MB: the
/// directory is charged per generation, and eleven directories cost more than
/// the page rows saved. It also doubles the published setup a restoring wallet
/// fetches, which is the cost a smaller table was supposed to reduce.
///
/// The row count also sets query size — `params_for_simplepir` derives the
/// scheme from it — so this is 21,504 bytes off every page query as well. See
/// `transparent-shard-server/tests/geometry_costs.rs`, which pins that.
pub const PAGE_ROWS: usize = 4_096;

/// Events stored directly in a script's directory entry.
///
/// Two covered 79.18% of scripts active in the study's sample, which is the
/// point: most scripts never need a page query at all. Raising it improves that
/// coverage but widens every row, including the overwhelming majority that hold
/// far fewer events, and the study found four and eight did not pay for
/// themselves.
pub const INLINE_EVENTS: u32 = 2;

/// Events that fit in one fragment.
///
/// Derived from what a row leaves after its own header and one entry header,
/// which is the largest a single history's fragment can be. It came to 36 under
/// the v4 layout's 128-byte per-row header as well, and the assertion below
/// pins that: the manifest publishes this number and every fixture is written
/// against it, so a re-derivation that quietly moved it would be a schema
/// change wearing the clothes of a refactor.
pub const EVENTS_PER_PAGE: u32 =
    ((PAGE_ROW_BYTES - PAGE_ROW_HEADER_BYTES - PAGE_ENTRY_HEADER_BYTES) / EVENT_BYTES) as u32;

const _: () = assert!(EVENTS_PER_PAGE == 36);

/// Fragments a script's history occupies, which is also its page-query count.
///
/// This is a *per-history* quantity. It is not the number of rows a shard's
/// page table needs: under packed rows several short histories share one row,
/// so aggregate row demand is [`PackedDemand::rows`] and the two must not be
/// confused. Naming them apart is the point of this rename.
///
/// Monotone in `events`, unlike the aggregate. The first [`INLINE_EVENTS`] live
/// in the directory entry and cost no fragment. A script at or below that
/// threshold therefore occupies none at all — the common case, and the reason
/// inline events exist.
pub const fn fragments_for(events: u32) -> u64 {
    if events <= INLINE_EVENTS {
        return 0;
    }
    let paged = events - INLINE_EVENTS;
    // Integer ceiling division; `const fn` cannot call `div_ceil`.
    (paged as u64).div_ceil(EVENTS_PER_PAGE as u64)
}

/// Bytes of header at the start of a packed page row, before its first entry.
///
/// Carries the entry count and nothing else. Everything a reader needs to walk
/// the row is either here or in each entry's own header, so a row can be parsed
/// without consulting the directory entry that located it.
pub const PAGE_ROW_HEADER_BYTES: usize = 4;

/// Bytes of header on each entry within a packed page row.
///
/// The v4 layout spent 128 bytes of header per row because a row held one
/// script. Packing pays this per *entry* instead, so it has to be tight: 2 for
/// the script length, 40 for the script, 4 each for ordinal, fragment count,
/// event count and the two height bounds, and 2 reserved.
pub const PAGE_ENTRY_HEADER_BYTES: usize = 64;

/// Bytes one entry holding `events` events occupies.
pub const fn entry_bytes(events: u32) -> usize {
    PAGE_ENTRY_HEADER_BYTES + events as usize * EVENT_BYTES
}

/// Entries of a `p`-event history that fit in one packed row.
///
/// The reason packing is worth doing, and the reason it stops paying at 18:
/// 22 at `p` = 1, 13 at 2, 10 at 3, 7 at 4, 6 at 5, 5 at 6, and 1 from 18
/// upward, where an entry is more than half a row. The measured distribution is
/// p50 2 events, p90 5, p95 8, and the newest two stay inline, so the common
/// paged history lands in the range where this is worth five- to twenty-fold.
pub const fn entries_per_row(p: u32) -> u32 {
    ((PAGE_ROW_BYTES - PAGE_ROW_HEADER_BYTES) / entry_bytes(p)) as u32
}

/// The most entries any row can hold, which is the decoder's hard bound.
///
/// A decoder must reject a claimed count above this *before* it computes any
/// offset from it, which is what keeps the arithmetic unreachable by malformed
/// input rather than merely checked.
pub const MAX_ENTRIES_PER_ROW: usize = entries_per_row(1) as usize;

const _: () = assert!(PAGE_ROW_HEADER_BYTES + entry_bytes(EVENTS_PER_PAGE) <= PAGE_ROW_BYTES);
const _: () = assert!(entries_per_row(1) == 22);
const _: () = assert!(entries_per_row(2) == 13);
const _: () = assert!(entries_per_row(17) == 2);
const _: () = assert!(entries_per_row(18) == 1);
const _: () = assert!(entries_per_row(EVENTS_PER_PAGE) == 1);

/// How a script's history occupies the page table.
///
/// Stated over the script's *total* event count, so a transition is expressed
/// as a pair of shapes rather than as a set of special cases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// Within the inline allowance: no entry, no row, no query.
    None,
    /// One entry of `p` paged events, sharing a row with other histories.
    Short(u32),
    /// A dedicated contiguous run of this many rows, one full fragment each
    /// but for the last. Long histories are never packed with anything else.
    Long(u64),
}

/// The shape a history of `events` total events takes.
pub const fn shape_of(events: u32) -> Shape {
    if events <= INLINE_EVENTS {
        return Shape::None;
    }
    let paged = events - INLINE_EVENTS;
    if paged <= EVENTS_PER_PAGE {
        Shape::Short(paged)
    } else {
        Shape::Long((paged as u64).div_ceil(EVENTS_PER_PAGE as u64))
    }
}

/// A shard's aggregate packed page-row demand, maintained incrementally.
///
/// Holds one count per short class plus the rows long histories have reserved,
/// which is everything needed to compute `R = sum over p of
/// ceil(N[p] / entries_per_row(p)) + L`.
///
/// # R is not monotone
///
/// Unlike distinct scripts or event count, `R` can *fall* as events arrive.
/// With `N[1]` = 23 (two rows, since 22 fit in one) and `N[2]` = 12 (one row),
/// `R` is 3; move a single script from class 1 to class 2 and `N[1]` = 22 (one
/// row) and `N[2]` = 13 (one row), so `R` is 2.
///
/// Nothing in the seal logic depends on monotonicity — capacity is checked on a
/// projection of the exact post-absorb state, and a shard closes the moment a
/// target is crossed, so `R` never gets the chance to dip back under a threshold
/// it has passed. But no caller may reason that a shard which does not fit now
/// can never fit later, and no monotone-progress assertion may be made across
/// tail revisions: a republished tail covering more blocks can need fewer rows.
///
/// What replaces monotonicity, where the target-below-capacity gap needs a
/// bound, is that one block raises `R` by at most the number of distinct scripts
/// it touches. Each script's move decrements one class, which never raises that
/// class's ceiling, and increments another, which raises its ceiling by at most
/// one; a 36-to-37 crossing gives up one class-36 row and takes two long rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PackedDemand {
    /// Short histories by paged-event count. Index 0 is unused.
    n: [u64; EVENTS_PER_PAGE as usize + 1],
    /// Rows reserved by long histories, which share with nothing.
    long_rows: u64,
    /// Long histories, kept so the class counts can be checked against the
    /// number of scripts above the inline allowance.
    long_scripts: u64,
}

// `[u64; 37]` is past the length `Default` is derived for, so this is written
// out rather than derived.
impl Default for PackedDemand {
    fn default() -> Self {
        Self {
            n: [0; EVENTS_PER_PAGE as usize + 1],
            long_rows: 0,
            long_scripts: 0,
        }
    }
}

impl PackedDemand {
    /// Moves one script from `old` total events to `new`.
    ///
    /// Absolute rather than incremental on purpose: stating both endpoints
    /// covers crossing the inline allowance, moving between short classes, and
    /// growing out of the short classes entirely, without any of them being a
    /// case in the code.
    pub fn shift(&mut self, old: u32, new: u32) {
        debug_assert!(
            new >= old,
            "a script's history never shrinks within a shard"
        );
        match shape_of(old) {
            Shape::None => {}
            Shape::Short(p) => self.n[p as usize] -= 1,
            Shape::Long(rows) => {
                self.long_rows -= rows;
                self.long_scripts -= 1;
            }
        }
        match shape_of(new) {
            Shape::None => {}
            Shape::Short(p) => self.n[p as usize] += 1,
            Shape::Long(rows) => {
                self.long_rows += rows;
                self.long_scripts += 1;
            }
        }
    }

    /// Page rows this demand needs.
    ///
    /// Recomputed over the 36 classes rather than maintained as a running
    /// total, because the per-class ceilings do not move monotonically and a
    /// running total is where that would bite.
    pub fn rows(&self) -> u64 {
        (1..=EVENTS_PER_PAGE as usize)
            .map(|p| self.n[p].div_ceil(entries_per_row(p as u32) as u64))
            .sum::<u64>()
            + self.long_rows
    }

    /// Short histories in class `p`, for reporting and tests.
    pub fn class(&self, p: u32) -> u64 {
        self.n[p as usize]
    }

    /// Rows reserved by long histories.
    pub fn long_rows(&self) -> u64 {
        self.long_rows
    }

    /// Histories too long to pack.
    pub fn long_scripts(&self) -> u64 {
        self.long_scripts
    }

    /// Histories that occupy the page table at all.
    pub fn paged_scripts(&self) -> u64 {
        self.n.iter().sum::<u64>() + self.long_scripts
    }
}

/// Segments needed to hold `rows` rows, at `per_segment` rows each.
///
/// Never zero: a shard with no events still publishes an empty segment, because
/// a wallet must be able to establish absence by querying rather than by being
/// told there is nothing to query.
pub const fn segments_for(rows: u64, per_segment: u64) -> u32 {
    let needed = rows.div_ceil(per_segment);
    if needed == 0 {
        1
    } else {
        needed as u32
    }
}

/// Splits a row index over a shard's logical row space into its segment and the
/// row within that segment.
///
/// The row within the segment is what a query names. It is the same index in
/// every segment, because every segment is asked: naming the segment would
/// disclose which one holds the script, and the script is what chose it.
pub const fn split_row(row: u64, per_segment: u64) -> (u32, u64) {
    ((row / per_segment) as u32, row % per_segment)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_shard_always_has_at_least_one_segment_of_each_table() {
        assert_eq!(segments_for(0, PAGE_ROWS as u64), 1);
        assert_eq!(segments_for(1, PAGE_ROWS as u64), 1);
        assert_eq!(segments_for(PAGE_ROWS as u64, PAGE_ROWS as u64), 1);
        assert_eq!(segments_for(PAGE_ROWS as u64 + 1, PAGE_ROWS as u64), 2);
    }

    /// A page extent that runs past a segment boundary is ordinary addressing,
    /// not a special case: the shard's page space is its segments concatenated.
    #[test]
    fn a_row_splits_into_its_segment_and_its_row_within_it() {
        let per = PAGE_ROWS as u64;
        assert_eq!(split_row(0, per), (0, 0));
        assert_eq!(split_row(per - 1, per), (0, per - 1));
        assert_eq!(split_row(per, per), (1, 0));
        assert_eq!(split_row(2 * per + 7, per), (2, 7));
    }

    #[test]
    fn a_fragment_holds_the_events_its_width_allows() {
        assert_eq!(EVENTS_PER_PAGE, 36);
        assert!(
            PAGE_ROW_HEADER_BYTES
                + PAGE_ENTRY_HEADER_BYTES
                + EVENTS_PER_PAGE as usize * EVENT_BYTES
                <= PAGE_ROW_BYTES,
            "a full fragment must fit its row"
        );
    }

    /// The inline events are free of page cost. Most scripts in a shard never
    /// exceed them, so getting this boundary wrong would misprice the dominant
    /// case and, through the seal logic, the whole shard geometry.
    #[test]
    fn histories_within_the_inline_allowance_occupy_no_page() {
        assert_eq!(fragments_for(0), 0);
        assert_eq!(fragments_for(1), 0);
        assert_eq!(fragments_for(INLINE_EVENTS), 0);
        assert_eq!(fragments_for(INLINE_EVENTS + 1), 1);
    }

    #[test]
    fn pages_are_added_only_when_the_previous_one_is_full() {
        let full = INLINE_EVENTS + EVENTS_PER_PAGE;
        assert_eq!(fragments_for(full), 1);
        assert_eq!(fragments_for(full + 1), 2);
        assert_eq!(fragments_for(INLINE_EVENTS + 2 * EVENTS_PER_PAGE), 2);
    }

    /// Fragment cost is driven by the number of *scripts* with history, not by
    /// the event count: many short histories need far more fragments than the
    /// same events concentrated in a few long ones. This is why a shard cannot
    /// be sealed on its event count alone.
    ///
    /// This is the waste packing exists to recover, and it is a statement about
    /// *fragments*, not rows — see the row comparison below, where sharing
    /// closes most of this gap.
    #[test]
    fn many_short_histories_need_more_fragments_than_one_long_one() {
        let events = 3_000u32;
        let concentrated = fragments_for(events);
        let spread: u64 = (0..events / 3).map(|_| fragments_for(3)).sum();
        assert!(
            spread > concentrated * 10,
            "spread {spread} should dwarf concentrated {concentrated}"
        );
    }

    fn demand(histories: &[u32]) -> PackedDemand {
        let mut demand = PackedDemand::default();
        for events in histories {
            demand.shift(0, *events);
        }
        demand
    }

    /// The table the whole optimisation rests on. A one-event history costs a
    /// row to itself under v4 and a twenty-second of one here; from 18 paged
    /// events an entry is more than half a row and packing stops paying.
    #[test]
    fn entries_per_row_is_what_packing_buys() {
        for (p, expected) in [
            (1, 22),
            (2, 13),
            (3, 10),
            (4, 7),
            (5, 6),
            (6, 5),
            (7, 4),
            (8, 4),
            (11, 3),
            (12, 2),
            (17, 2),
            (18, 1),
            (EVENTS_PER_PAGE, 1),
        ] {
            assert_eq!(entries_per_row(p), expected, "entries_per_row({p})");
        }
    }

    /// A full fragment plus both headers must fit the row it is written into,
    /// with the tail left over to be zeroed.
    #[test]
    fn a_full_fragment_fits_its_row() {
        let used = PAGE_ROW_HEADER_BYTES + entry_bytes(EVENTS_PER_PAGE);
        assert_eq!(used, 3_524);
        assert!(used <= PAGE_ROW_BYTES);
    }

    /// The boundary between sharing a row and reserving a run of them. It is
    /// the one transition that changes a history's kind rather than its class.
    #[test]
    fn a_history_becomes_long_exactly_when_it_outgrows_a_fragment() {
        assert_eq!(shape_of(0), Shape::None);
        assert_eq!(shape_of(INLINE_EVENTS), Shape::None);
        assert_eq!(shape_of(INLINE_EVENTS + 1), Shape::Short(1));
        let full = INLINE_EVENTS + EVENTS_PER_PAGE;
        assert_eq!(shape_of(full), Shape::Short(EVENTS_PER_PAGE));
        assert_eq!(shape_of(full + 1), Shape::Long(2));
        assert_eq!(
            shape_of(INLINE_EVENTS + 3 * EVENTS_PER_PAGE),
            Shape::Long(3)
        );
    }

    /// `shift` is stated over absolute endpoints so that every transition —
    /// crossing the inline allowance, moving between classes, growing out of
    /// them — is the same two lines of code. This checks each one lands where
    /// the shape says it should.
    #[test]
    fn a_history_moves_between_classes_as_it_grows() {
        let mut d = PackedDemand::default();

        // Below the inline allowance nothing is occupied at all.
        d.shift(0, 2);
        assert_eq!(d, PackedDemand::default());

        // Crossing it opens class 1.
        d.shift(2, 3);
        assert_eq!(d.class(1), 1);
        assert_eq!(d.rows(), 1);

        // Class to class: one leaves, one arrives.
        d.shift(3, 7);
        assert_eq!(d.class(1), 0);
        assert_eq!(d.class(5), 1);

        // The last short class, then out of the short classes entirely.
        d.shift(7, INLINE_EVENTS + EVENTS_PER_PAGE);
        assert_eq!(d.class(EVENTS_PER_PAGE), 1);
        assert_eq!(d.long_scripts(), 0);
        d.shift(
            INLINE_EVENTS + EVENTS_PER_PAGE,
            INLINE_EVENTS + EVENTS_PER_PAGE + 1,
        );
        assert_eq!(d.class(EVENTS_PER_PAGE), 0);
        assert_eq!(d.long_scripts(), 1);
        assert_eq!(d.long_rows(), 2);

        // Long histories only ever reserve more rows.
        d.shift(
            INLINE_EVENTS + EVENTS_PER_PAGE + 1,
            INLINE_EVENTS + 5 * EVENTS_PER_PAGE,
        );
        assert_eq!(d.long_rows(), 5);
        assert_eq!(d.paged_scripts(), 1);

        // A script can arrive already long, in one block.
        d.shift(0, INLINE_EVENTS + 500);
        assert_eq!(d.long_scripts(), 2);
    }

    /// Packing can never cost more rows than giving every history its own, and
    /// it cannot beat the bytes it has to store. Both bounds hold at once for
    /// any mixture, which is the cheap sandwich that catches a wrong
    /// `entries_per_row` without recomputing it.
    #[test]
    fn packed_rows_sit_between_the_byte_floor_and_the_unpacked_cost() {
        let histories: Vec<u32> = (0..400u32).map(|i| 3 + (i * 7) % 90).collect();
        let d = demand(&histories);

        let unpacked: u64 = histories.iter().map(|e| fragments_for(*e)).sum();
        assert!(
            d.rows() <= unpacked,
            "packed {} should not exceed unpacked {unpacked}",
            d.rows()
        );

        let bytes: usize = histories
            .iter()
            .map(|e| {
                let paged = (e - INLINE_EVENTS) as usize;
                let full = paged / EVENTS_PER_PAGE as usize;
                let rest = paged % EVENTS_PER_PAGE as usize;
                full * entry_bytes(EVENTS_PER_PAGE)
                    + if rest > 0 {
                        entry_bytes(rest as u32)
                    } else {
                        0
                    }
            })
            .sum();
        let floor = (bytes as u64).div_ceil((PAGE_ROW_BYTES - PAGE_ROW_HEADER_BYTES) as u64);
        assert!(
            d.rows() >= floor,
            "packed {} is below the byte floor {floor}",
            d.rows()
        );
    }

    /// Every paged script is in exactly one class or is long. A shift that lost
    /// or duplicated one would show up here and nowhere else.
    #[test]
    fn every_paged_script_is_counted_exactly_once() {
        let histories: Vec<u32> = (0..300u32).map(|i| i % 120).collect();
        let d = demand(&histories);
        let expected = histories.iter().filter(|e| **e > INLINE_EVENTS).count() as u64;
        assert_eq!(d.paged_scripts(), expected);
    }

    /// Row demand is **not** monotone as events arrive, unlike distinct scripts
    /// or event count. This is the counterexample, recorded so the property is
    /// known rather than accidental: emptying a class's second, nearly-empty row
    /// can cost less than the class the script moves into gains.
    ///
    /// Nothing in [`crate::seal`] depends on monotonicity — capacity is checked
    /// against the exact post-absorb state and a shard closes the moment a
    /// target is crossed — but no caller may reason that content which does not
    /// fit now can never fit later.
    #[test]
    fn row_demand_can_fall_as_events_arrive() {
        let mut d = PackedDemand::default();
        // 23 one-event histories need two rows, since 22 share one.
        for _ in 0..23 {
            d.shift(0, INLINE_EVENTS + 1);
        }
        // 12 two-event histories need one, since 13 share one.
        for _ in 0..12 {
            d.shift(0, INLINE_EVENTS + 2);
        }
        let before = d.rows();
        assert_eq!(before, 3);

        // One more event for one script, and the total *falls*.
        d.shift(INLINE_EVENTS + 1, INLINE_EVENTS + 2);
        assert_eq!(d.class(1), 22);
        assert_eq!(d.class(2), 13);
        assert_eq!(d.rows(), 2, "row demand should fall from {before}");
    }
}
