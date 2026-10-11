//! Display table shapes, kept apart from the history registry.
//!
//! History shards name `layout::PROFILES` entries; display shards name these.
//! A history consumer therefore refuses a display geometry name rather than
//! decoding it, and display tables are not forced to the history archive's
//! 32,768 rows, whose selection alone is 200 KB of upload per query.
//!
//! A display table uses only the directory fields: its rows and row width.
//! The page fields restate them so the shape passes the generic checks.

use crate::layout::{Geometry, INLINE_EVENTS};

/// The native minimum row count, with two 4,096-byte instances per row:
/// 72 entries of a row, 38,920 B of dithered upload and about 11.3 KB of
/// download per query.
pub const TXID_2K8: Geometry = Geometry {
    name: "txid-2k8",
    directory_rows: 2_048,
    directory_row_bytes: 8_192,
    page_rows: 2_048,
    page_row_bytes: 8_192,
    inline_events: INLINE_EVENTS,
};

pub const DISPLAY_PROFILES: &[Geometry] = &[TXID_2K8];

/// The display geometry named `name`; `None` must be treated as a hard error.
pub fn display_by_name(name: &str) -> Option<&'static Geometry> {
    DISPLAY_PROFILES.iter().find(|g| g.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txid::{slots_per_row, ENTRY_BYTES};

    #[test]
    fn display_profiles_are_native_shapes_disjoint_from_history() {
        for geometry in DISPLAY_PROFILES {
            geometry.validate().unwrap();
            assert_eq!(geometry.page_rows, geometry.directory_rows);
            assert_eq!(geometry.page_row_bytes, geometry.directory_row_bytes);
            assert!(crate::layout::by_name(geometry.name).is_none());
            assert_eq!(display_by_name(geometry.name), Some(geometry));
        }
        for geometry in crate::layout::PROFILES {
            assert!(display_by_name(geometry.name).is_none());
        }
        assert_eq!(slots_per_row(TXID_2K8.directory_row_bytes), 72);
        assert_eq!(TXID_2K8.directory_row_bytes - 72 * ENTRY_BYTES, 56);
    }
}
