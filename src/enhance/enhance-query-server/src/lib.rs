//! Enhance PIR query serving: workers, packing routers and the query ingress
//! that serves wallets, with their publication synchronization, admission and
//! HTTP surfaces.
//!
//! Records, the wire protocol, the publication contract, prepared runtimes and
//! the wallet client live in `enhance-shared` and are re-exported here for
//! serving callers. This library must not depend on `enhance-publisher`, chain
//! readers, ingestion or tools; the publisher exposes prepared material through
//! HTTPS and the serving roles own activation. `tools/ci/boundary.py` enforces
//! the boundary.

pub use enhance_shared::*;
use pir_control::admission;
pub mod internal_auth;
pub mod telemetry;
pub mod worker;

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
