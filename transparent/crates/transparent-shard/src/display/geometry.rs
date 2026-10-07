//! Display table shapes, kept apart from the history registry.
//!
//! History shards name `layout::PROFILES` entries; display shards name these.
//! A history consumer therefore refuses a display geometry name rather than
//! decoding it, and display tables are not forced to the history archive's
//! 32,768 rows, whose selection alone is 200 KB of upload per query.

use crate::layout::{Geometry, INLINE_EVENTS};
use crate::txid::ROW_BYTES;

/// The native minimum row count: 40,200 B of upload per query.
pub const TXID_2K: Geometry = Geometry {
    name: "txid-2k",
    directory_rows: 2_048,
    directory_row_bytes: ROW_BYTES,
    page_rows: 2_048,
    page_row_bytes: ROW_BYTES,
    inline_events: INLINE_EVENTS,
};

/// A comparison shape: 52,744 B of upload per query.
pub const TXID_4K: Geometry = Geometry {
    name: "txid-4k",
    directory_rows: 4_096,
    page_rows: 4_096,
    ..TXID_2K
};

pub const DISPLAY_PROFILES: &[Geometry] = &[TXID_2K, TXID_4K];

/// The display geometry named `name`; `None` must be treated as a hard error.
pub fn display_by_name(name: &str) -> Option<&'static Geometry> {
    DISPLAY_PROFILES.iter().find(|g| g.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_profiles_are_publishable_and_disjoint_from_history() {
        for geometry in DISPLAY_PROFILES {
            geometry.validate_publishable().unwrap();
            assert!(crate::layout::by_name(geometry.name).is_none());
            assert_eq!(display_by_name(geometry.name), Some(geometry));
        }
        for geometry in crate::layout::PROFILES {
            assert!(display_by_name(geometry.name).is_none());
        }
    }
}
