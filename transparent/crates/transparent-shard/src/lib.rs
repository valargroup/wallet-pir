//! Building the immutable shards a wallet privately retrieves history from.
//!
//! A shard is a chain range with four published objects: a public activity
//! filter, a private script directory, private event pages, and an optional
//! private transaction-detail table. Its boundaries are decided by what it
//! contains rather than by a fixed block width — see [`seal`] for why — and
//! every shard names a table geometry from a small closed registry, which is
//! what lets one set of PIR parameters serve every shard that names the same
//! one.
//!
//! A set may mix them: archive geometry for old, dense history and a narrower
//! recent geometry for the window wallets synchronise constantly. The registry
//! is closed rather than open so a wallet validates a handful of parameter sets
//! once instead of one per shard, and so a shard naming a shape this build does
//! not know is refused rather than decoded as the shape it does know.
//!
//! This crate decides boundaries and lays out tables. It does not fetch chain
//! data and it does not serve queries.

pub mod build;
pub mod choice;
pub mod compact;
pub mod compact_v10;
pub mod display;
pub mod layout;
pub mod manifest;
pub mod packing;
pub mod page_row;
pub mod records;
pub mod seal;
pub mod tag;
pub mod txid;
pub mod txid_v1;
pub mod txid_v2x;

pub use build::{
    build_shard, candidate_rows, choice_table, place_scripts, placement_order, verify_choice,
    BuildError, BuiltShard, Placement,
};
pub use choice::{ChoiceError, ChoiceTable};
pub use layout::{
    by_name as geometry_by_name, Geometry, PackedDemand, ARCHIVE_32K, ARCHIVE_WIDE, DIRECTORY_ROWS,
    DIRECTORY_ROW_BYTES, EVENTS_PER_PAGE, INLINE_EVENTS, MAX_ENTRIES_PER_ROW,
    PAGE_ENTRY_HEADER_BYTES, PAGE_ROWS, PAGE_ROW_BYTES, PAGE_ROW_HEADER_BYTES, PROFILES, RECENT_4K,
    RECENT_4K_8K, RECENT_8K,
};
pub use manifest::{
    query_binding, ManifestLayout, ManifestOccupancy, ManifestSeal, ShardManifest, TableGeometry,
    SCHEMA,
};
pub use page_row::{decode_page_row, encode_page_row, PageEntry};
pub use records::{
    decode_directory_row, encode_directory_row, DirectoryEntry, RecordError,
    DIRECTORY_ENTRY_HEADER_BYTES, DIRECTORY_ROW_HEADER_BYTES, DIRECTORY_SLOTS,
    MAX_DIRECTORY_ENTRY_BYTES, MAX_SCRIPT_BYTES,
};
pub use seal::{
    ChoiceMeasure, Limit, Occupancy, SealError, SealPolicy, SealReason, SealedShard, Sealer,
};
pub use tag::{resolve_tag_salt, script_tag, tag_salt, SCRIPT_TAG_BYTES};
