//! Building the immutable shards a wallet privately retrieves history from.
//!
//! A shard is a chain range with four published objects: a public activity
//! filter, a private script directory, private event pages, and an optional
//! private transaction-detail table. Its boundaries are decided by what it
//! contains rather than by a fixed block width — see [`seal`] for why — and
//! every shard shares one pinned table geometry, which is what lets one set of
//! PIR parameters serve all of them.
//!
//! This crate decides boundaries and lays out tables. It does not fetch chain
//! data and it does not serve queries.

pub mod build;
pub mod layout;
pub mod manifest;
pub mod records;
pub mod seal;

pub use build::{build_shard, candidate_rows, BuildError, BuiltShard};
pub use layout::{
    entries_per_row, entry_bytes, fragments_for, shape_of, PackedDemand, Shape, DIRECTORY_ROWS,
    DIRECTORY_ROW_BYTES, EVENTS_PER_PAGE, INLINE_EVENTS, MAX_ENTRIES_PER_ROW,
    PAGE_ENTRY_HEADER_BYTES, PAGE_ROWS, PAGE_ROW_BYTES, PAGE_ROW_HEADER_BYTES,
};
pub use manifest::{
    ManifestLayout, ManifestOccupancy, ManifestSeal, ShardManifest, TableGeometry, SCHEMA,
};
pub use records::{
    decode_directory_row, encode_directory_row, DirectoryEntry, Page, RecordError,
    DIRECTORY_ENTRY_BYTES, DIRECTORY_SLOTS, MAX_SCRIPT_BYTES,
};
pub use seal::{Limit, Occupancy, SealError, SealPolicy, SealReason, SealedShard, Sealer};
