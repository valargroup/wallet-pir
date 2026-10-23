//! Prepared PIR state, and the budget it lives inside.
//!
//! A shard's plaintext table is a few tens of megabytes. The state needed to
//! *answer queries against it* is several times that, and building it takes
//! about a second. The predecessor of this module built that state on first use
//! and kept it forever, which has two failure modes that only appear at fleet
//! size.
//!
//! **It could not be bounded.** Nothing evicted, so a worker's memory was
//! decided by how many distinct shards its traffic happened to touch. The unit
//! file's `MemoryMax` was the only limit, and reaching it is an OOM kill rather
//! than a miss.
//!
//! **It duplicated work under concurrency.** The build ran outside the lock, so
//! N requests arriving together for a cold shard each built a full runtime and
//! discarded all but one — the worst possible behaviour exactly when the worker
//! is busiest.
//!
//! Both are fixed here: a byte-bounded LRU, and one build per identity that
//! every other caller waits on.
//!
//! # Reserving before building
//!
//! Room is reserved from the budget *before* a build starts, from the size the
//! geometry implies, because the build itself is what allocates. Checking
//! afterwards would mean the budget was only ever exceeded, never enforced.
//!
//! If the budget cannot be freed — because everything resident is pinned by a
//! request in flight — the caller is told so explicitly and can retry. That is
//! worse service than an unbounded cache gives, and it is the point: the
//! alternative is not "better service", it is being killed.
//!
//! # Releasing what the build did not use
//!
//! The reservation is an upper bound: it charges the compiled packing matrix
//! at eight-byte words, and a built runtime has so far held it at four (or
//! fewer, packed). Once a build or restore has produced the runtime, its entry
//! is charged what it actually holds, [`TableRuntime::held_bytes`], and the
//! difference returns to the budget. The release happens only after the
//! runtime exists, never before, so a build in flight is always charged its
//! bound, and an entry's charge can only fall. A cache that kept the bound
//! would hold barely more than half the runtimes its budget can carry.
//!
//! # Nothing is evicted while it is in use
//!
//! A caller holds a [`RuntimeHandle`] for as long as it needs the runtime, and
//! the cache treats an entry as pinned while any handle to it exists. Dropping
//! the accounting for a runtime a request is still evaluating against would
//! free budget that is not free.

use zakura_pir_protocol::transparent::shard as transparent_shard;
use zakura_pir_query_server::transparent::native as transparent_native;

pub mod disk;
pub(crate) mod filecache;
pub mod memory;
pub mod metrics;
pub mod prebuild;
pub mod prepare;
pub mod prewarm;
pub mod procmem;

use crate::runtime::metrics::Metrics;
use crate::{segment::SegmentSource, table::Table};
use rayon::prelude::*;
use reinspiring::native_kernel::PreparedU16Query;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use transparent_native::{
    self as native, NativeKeys, NativePreprocessed, NativeScheme, Profile, TableProfile,
};
use transparent_shard::layout::Geometry;
use zakura_pir_client::native::ProfileClientExt as _;
use zakura_pir_query_server::native::ProfileQueryExt as _;
use zakura_pir_query_server::transparent::native::TableQueryExt as _;
use zakura_pir_server_core::native::ProfilePublicationExt as _;

const _: () = assert!(transparent_shard::manifest::QUERY_ROW_QUANTUM as usize == native::D);

/// Explicit build policy for reproducible hardware-path comparisons. The
/// portable feature selects the library's existing fallback; it changes no
/// parameters or wire encoding and never depends on a client request.
pub const KERNEL_POLICY: &str = if cfg!(feature = "portable-kernel") {
    "chunked-split"
} else {
    "auto"
};

mod parameters;
pub use parameters::{held_bytes, reserved_bytes, warm_bytes, ParsedQuery, SharedParams};
mod table_runtime;
pub use table_runtime::TableRuntime;
mod scheduling;
pub use scheduling::{build_pool, validation_pool};
mod cache;
pub use cache::{CacheError, Produced, RuntimeCache, RuntimeHandle, RuntimeKey};

#[cfg(test)]
pub(crate) use cache::tests;
