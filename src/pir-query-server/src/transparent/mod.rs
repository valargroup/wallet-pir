//! Transparent history and transaction-detail serving, owned by the query
//! server.
//!
//! Serves the private directory and page tables of many shard revisions from
//! one process, under one shared parameter set per geometry per table, along
//! with the public shard map and range filters the filter service serves at the
//! same paths on its own host.
//!
//! Each role checks publisher discovery before opening state, runs initial
//! synchronization under the existing retry rules, then serves the supplied
//! listener while supervising synchronization ([`history::process`],
//! [`tx_detail::process`]). Publication management, bootstrap, archive
//! forwarding and worker routing live under [`routing`]; admission, request
//! answering, activation, preparation, durable records, background validation
//! and the authenticated publication source are shared by both products.
//! Verified publications, tables and prepared runtimes come from
//! `transparent_shared`.
//!
//! The `transparent-history` and `transparent-detail` features compile each
//! product independently; neither implies the other.

pub mod admission;
pub mod config;
#[cfg(feature = "transparent-history")]
pub mod history;
mod objects;
pub mod preparation;
pub mod request;
#[cfg(feature = "transparent-history")]
pub mod routing;
mod source;
mod source_collection;
#[cfg(feature = "transparent-detail")]
pub mod tx_detail;
mod validation;
pub use source::{discover, discover_publisher, is_incompatible, Config as SourceConfig};

/// Where the wallet-facing history and transaction-detail operations are
/// served: the canonical `/transparent/v1` namespace and the deployed `/v2`
/// alias reach the same handlers. Health, readiness and metrics stay at their
/// unprefixed private paths and are never mounted under either.
pub const PUBLIC_ROUTES: pir_control::public_routes::PublicMount =
    pir_control::public_routes::PublicMount::new(
        transparent_shared::http::CANONICAL_PREFIX,
        transparent_shared::http::LEGACY_PREFIX,
    );
