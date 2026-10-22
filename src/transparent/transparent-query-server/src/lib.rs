//! Private retrieval from a published transparent shard set.
//!
//! Serves the private directory and page tables of many shard revisions from
//! one process, under one shared parameter set per geometry per table. It also
//! serves the public shard map and range filters, which the filter service
//! serves at the same paths on its own host — so a wallet that wants its public
//! and private bytes to come from different origins still has that, and one
//! that wants a single endpoint has that too.
//!
//! History serving lives under [`history`], transaction-detail serving under
//! [`tx_detail`], and bootstrap, management, archive and routing under
//! [`routing`]. Admission, request answering, service configuration,
//! preparation lifecycle and worker control sockets are query-owned; verified
//! publications and prepared runtimes come from `transparent_shared`.

//!
//! The `transparent-history` and `transparent-detail` features compile each
//! product independently; neither implies the other. Admission, requests,
//! configuration, control, activation, preparation and the authenticated
//! publication source are shared by both.

#[cfg(not(any(feature = "transparent-history", feature = "transparent-detail")))]
compile_error!("enable transparent-history, transparent-detail, or both");

mod activation;
pub mod admission;
pub mod config;
pub mod control;
#[cfg(feature = "transparent-history")]
pub mod history;
mod objects;
pub mod preparation;
pub mod request;
#[cfg(feature = "transparent-history")]
pub mod routing;
mod source;
mod validation;
pub use source::{discover, is_incompatible};

/// Where the wallet-facing history and transaction-detail operations are
/// served: the canonical `/transparent/v1` namespace and the deployed `/v2`
/// alias reach the same handlers. Health, readiness and metrics stay at their
/// unprefixed private paths and are never mounted under either.
pub const PUBLIC_ROUTES: pir_control::public_routes::PublicMount =
    pir_control::public_routes::PublicMount::new(
        transparent_shared::http::CANONICAL_PREFIX,
        transparent_shared::http::LEGACY_PREFIX,
    );
#[cfg(feature = "transparent-detail")]
pub mod tx_detail;
