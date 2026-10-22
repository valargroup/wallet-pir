//! Enhance serving roles owned by the query server: workers, packing routers
//! and the wallet-facing query ingress, with their publication
//! synchronization, admission and HTTP surfaces.
//!
//! Records, the wire protocol, the publication contract and prepared runtimes
//! come from `enhance-shared`; the private evaluation messages and public
//! route mounts come from `enhance-query-server`.
pub mod internal_auth;
pub mod telemetry;

mod memory_observation;
mod response_body;

mod query_serving;
pub mod shutdown;

mod http_metrics;

mod query_timing;

pub mod ingress_sync;
pub mod router_sync;
pub mod service;
pub mod synchronization;
pub mod worker_sync;
