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

use transparent_events::EVENT_BYTES;

/// Bytes in one directory row.
pub const DIRECTORY_ROW_BYTES: usize = 3_584;

/// Bytes in one page row.
pub const PAGE_ROW_BYTES: usize = 17_920;

/// Bytes in one transaction-detail row.
pub const TXDETAIL_ROW_BYTES: usize = 3_584;

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

#[cfg(test)]
mod tests {
    use super::*;

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
