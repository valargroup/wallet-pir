//! Pinned table geometry, identical for every shard.
//!
//! Uniformity is the whole point. `ipir_sp::params_for_simplepir` is a pure
//! function of `(rows, item_size_bits)`, so shards that share a geometry share
//! one set of scheme parameters — a client validates parameters and derives its
//! query setup once for every shard it will ever query, instead of once per
//! shard. That saving is only available if *every* shard fits the same
//! geometry, which is what the seal logic exists to guarantee.
//!
//! The row widths come from the mainnet study's measured sweep: a 3,584-byte
//! directory row with two inline events, and 17,920-byte page rows, which was
//! the best-performing page width it tested.
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
pub const PAGE_ROW_BYTES: usize = 17_920;

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

/// Rows in every shard's page table.
///
/// The census over the Ironwood-to-tip journal put page rows at a maximum of
/// 2,117 for a shard sealed near 8,000 scripts, so 2,048 would overflow and the
/// next multiple of the padding granularity is the smallest that fits.
pub const PAGE_ROWS: usize = 4_096;

/// Events stored directly in a script's directory entry.
///
/// Two covered 79.18% of scripts active in the study's sample, which is the
/// point: most scripts never need a page query at all. Raising it improves that
/// coverage but widens every row, including the overwhelming majority that hold
/// far fewer events, and the study found four and eight did not pay for
/// themselves.
pub const INLINE_EVENTS: u32 = 2;

/// Bytes of header at the start of a page, before its events.
///
/// Carries the exact script bytes, the page's ordinal and count, its event
/// count, and its minimum and maximum event heights. The script is here rather
/// than in each event because it is the key the page is stored under, and a
/// client must check it to reject a misplaced or collided row.
pub const PAGE_HEADER_BYTES: usize = 128;

/// Events that fit in one page.
pub const EVENTS_PER_PAGE: u32 = ((PAGE_ROW_BYTES - PAGE_HEADER_BYTES) / EVENT_BYTES) as u32;

/// Page rows a script's history occupies.
///
/// Monotone in `events`, which is what lets a shard's page total be maintained
/// incrementally as blocks stream in rather than recomputed per candidate
/// boundary.
///
/// The first [`INLINE_EVENTS`] live in the directory entry and cost no page. A
/// script at or below that threshold therefore occupies no page row at all —
/// the common case, and the reason inline events exist.
pub const fn page_rows_for(events: u32) -> u64 {
    if events <= INLINE_EVENTS {
        return 0;
    }
    let paged = events - INLINE_EVENTS;
    // Integer ceiling division; `const fn` cannot call `div_ceil`.
    (paged as u64).div_ceil(EVENTS_PER_PAGE as u64)
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
    fn a_page_holds_the_events_its_width_allows() {
        assert_eq!(EVENTS_PER_PAGE, 185);
        assert!(
            PAGE_HEADER_BYTES + EVENTS_PER_PAGE as usize * EVENT_BYTES <= PAGE_ROW_BYTES,
            "a full page must fit its row"
        );
    }

    /// The inline events are free of page cost. Most scripts in a shard never
    /// exceed them, so getting this boundary wrong would misprice the dominant
    /// case and, through the seal logic, the whole shard geometry.
    #[test]
    fn histories_within_the_inline_allowance_occupy_no_page() {
        assert_eq!(page_rows_for(0), 0);
        assert_eq!(page_rows_for(1), 0);
        assert_eq!(page_rows_for(INLINE_EVENTS), 0);
        assert_eq!(page_rows_for(INLINE_EVENTS + 1), 1);
    }

    #[test]
    fn pages_are_added_only_when_the_previous_one_is_full() {
        let full = INLINE_EVENTS + EVENTS_PER_PAGE;
        assert_eq!(page_rows_for(full), 1);
        assert_eq!(page_rows_for(full + 1), 2);
        assert_eq!(page_rows_for(INLINE_EVENTS + 2 * EVENTS_PER_PAGE), 2);
    }

    /// Page cost is driven by the number of *scripts* with history, not by the
    /// event count: many short histories cost far more rows than the same
    /// events concentrated in a few long ones. This is why a shard cannot be
    /// sealed on its event count alone.
    #[test]
    fn many_short_histories_cost_more_pages_than_one_long_one() {
        let events = 3_000u32;
        let concentrated = page_rows_for(events);
        let spread: u64 = (0..events / 3).map(|_| page_rows_for(3)).sum();
        assert!(
            spread > concentrated * 50,
            "spread {spread} should dwarf concentrated {concentrated}"
        );
    }
}
