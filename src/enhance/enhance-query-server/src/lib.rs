//! Enhance PIR query serving: the wire protocol and client, record encoding,
//! prepared runtimes, workers, packing routers and the query ingress that
//! serves wallets.
//!
//! This library serves prepared publications. It must not depend on
//! `enhance-publisher`, chain readers, ingestion or tools; the publisher
//! exposes prepared material through HTTPS; the serving roles own activation.
//! `tools/ci/boundary.py` enforces the boundary.
//!
//! Record types and constants are exported at the crate root. The serving
//! database layout lives separately in [`types`].

use pir_control::admission;

/// Where the wallet-facing Enhance operations (`/init`, `/session/:id`,
/// `/sessions/:generation/:shard`, `/query`) are served: the canonical
/// `/enhance/v1` namespace and the deployed `/v1/enhance` alias reach the same
/// handlers. Control routes (`/internal/*`) are never mounted under either.
pub const PUBLIC_ROUTES: pir_control::public_routes::PublicMount =
    pir_control::public_routes::PublicMount::new("/enhance/v1", "/v1/enhance");
pub mod artifact;
#[cfg(any(test, feature = "client"))]
pub mod client;
pub mod internal_auth;
pub mod ipir;
#[cfg(feature = "native-reinspiring")]
pub mod native;
pub mod placement;
pub mod protocol;
mod record;
mod record_types;
pub mod runtime;
pub mod telemetry;
pub mod types;
pub mod wire;
pub mod worker;

pub use record::{EnhanceTransactionMetadata, InvalidEnhanceRecord};
pub use record_types::*;

mod packing_budget;
pub use packing_budget::PackingBudget;

pub mod matvec;
mod memory_observation;
mod response_body;

mod query_serving;
pub mod shutdown;

mod http_metrics;

mod query_timing;

pub mod ingress_sync;
pub mod prepared_packing;
pub mod publication;
pub mod router_sync;
pub mod service;
pub mod synchronization;
pub mod worker_sync;

#[cfg(test)]
mod test_support;
