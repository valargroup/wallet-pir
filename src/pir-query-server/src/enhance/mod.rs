//! Enhance serving roles owned by the query server: workers, packing routers
//! and the wallet-facing query ingress, with their publication
//! synchronization, admission and HTTP surfaces.
//!
//! Records, the wire protocol, the publication contract and prepared runtimes
//! come from `enhance-shared`. The private evaluation messages live in
//! [`evaluation`].

/// Where the wallet-facing Enhance operations (`/init`, `/session/:id`,
/// `/sessions/:generation/:shard`, `/query`) are served: the canonical
/// `/enhance/v1` namespace and the deployed `/v1/enhance` alias reach the same
/// handlers. Control routes (`/internal/*`) are never mounted under either.
pub const PUBLIC_ROUTES: pir_control::public_routes::PublicMount =
    pir_control::public_routes::PublicMount::new("/enhance/v1", "/v1/enhance");
pub mod evaluation;
pub mod internal_auth;
pub mod telemetry;

mod memory_observation;
mod response_body;

mod query_serving;
pub mod shutdown;

mod http_metrics;

mod query_timing;

mod peers;

pub mod combined;
pub mod ingress_sync;
pub mod router_sync;
pub mod service;
pub mod synchronization;
pub mod worker_sync;
