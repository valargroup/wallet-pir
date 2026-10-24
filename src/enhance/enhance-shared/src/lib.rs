//! Enhance material and contracts shared by the Enhance publisher and query
//! server: record encoding, the wire protocol and coverage geometry, the typed
//! publication contract, prepared runtimes with their memory budgets, and
//! private evaluation over prepared shards.
//!
//! This package reads and writes only the prepared files it is handed.
//! HTTP listeners, admission, publication synchronization, peer coordination
//! and roles stay in `pir-query-server`; chain observation, journals and
//! publication stay in `enhance-publisher`. Neither may be a dependency here.
//!
//! Record types and constants are exported at the crate root. The serving
//! database layout lives separately in [`types`].

pub mod artifact;
#[cfg(any(test, feature = "client"))]
pub mod client;
pub mod ipir;
pub mod matvec;
#[cfg(feature = "native-reinspiring")]
pub mod native;
pub mod placement;
pub mod prepared_packing;
pub mod protocol;
pub mod publication;
mod record;
mod record_types;
pub mod runtime;
pub mod types;
pub mod wire;

pub use record::{EnhanceTransactionMetadata, InvalidEnhanceRecord};
pub use record_types::*;

mod packing_budget;
pub use packing_budget::PackingBudget;

/// Synthetic records and predecessors for the query server's regressions.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
