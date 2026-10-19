//! Enhance PIR query serving: the wire protocol and client, record encoding,
//! prepared runtimes, workers, query ingress, packing routers and the public
//! [`gateway::QueryGateway`].
//!
//! This library serves prepared publications. It must not depend on
//! `enhance-publisher`, chain readers, ingestion or tools; the publisher
//! composes it and supplies publication state through
//! [`gateway::PublisherStatus`]. `tools/ci/boundary.py` enforces the boundary.
//!
//! Record types and constants are exported at the crate root. The serving
//! database layout lives separately in [`types`].

use pir_control::admission;
pub mod artifact;
#[cfg(any(test, feature = "client"))]
pub mod client;
pub mod gateway;
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

pub mod packing_router;
pub mod query_ingress;
mod query_serving;
mod serving_fence;

mod http_metrics;

mod query_timing;

pub mod prepared_packing;
pub mod service;

#[cfg(test)]
mod test_support;
