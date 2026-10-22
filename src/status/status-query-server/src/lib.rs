//! Status PIR serving primitives below `pir-query-server`: the private peer
//! messages a router and worker exchange and the wallet-facing route mounts.
//!
//! Generations, the wire protocol and the publication contract live in
//! `status-shared`. `pir-query-server` owns Status roles, listeners,
//! admission, synchronization and outbound HTTP; chain observation and
//! publication remain in `status-publisher`, which this crate must never
//! depend on.
pub mod distributed;

/// Where the wallet-facing Status operations (`/init`, `/session/:id`,
/// `/query`) are served: the canonical `/status/v1` namespace and the deployed
/// `/v1/status` alias reach the same router handlers. Control, provider and
/// worker routes are never mounted under either.
pub const PUBLIC_ROUTES: pir_control::public_routes::PublicMount =
    pir_control::public_routes::PublicMount::new("/status/v1", "/v1/status");
