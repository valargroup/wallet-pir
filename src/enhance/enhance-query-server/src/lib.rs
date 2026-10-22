//! Enhance PIR serving primitives below `pir-query-server`: the private
//! evaluation messages between ingress, routers and workers, and the
//! wallet-facing route mounts.
//!
//! Records, the wire protocol, the publication contract, prepared runtimes and
//! the wallet client live in `enhance-shared`. `pir-query-server` owns the
//! serving roles, their synchronization, admission and HTTP surfaces. This
//! library must not depend on `enhance-publisher`, chain readers, ingestion or
//! tools; `tools/ci/boundary.py` enforces the boundary.

/// Where the wallet-facing Enhance operations (`/init`, `/session/:id`,
/// `/sessions/:generation/:shard`, `/query`) are served: the canonical
/// `/enhance/v1` namespace and the deployed `/v1/enhance` alias reach the same
/// handlers. Control routes (`/internal/*`) are never mounted under either.
pub const PUBLIC_ROUTES: pir_control::public_routes::PublicMount =
    pir_control::public_routes::PublicMount::new("/enhance/v1", "/v1/enhance");
pub mod worker;
