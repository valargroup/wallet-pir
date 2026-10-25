//! HTTPS discovery, transfer limits and typed publication-source adapters.
//! The transport-neutral consumer engine and polling live in `pir-pipeline`.
//! Product adapters own authority policy; successful transfers never grant it.
mod discovery;
mod transfer;
mod transport;
pub use discovery::{is_incompatible, DiscoveryError, DISCOVERY_DEADLINE};
pub use pir_service_discovery as service_discovery;
pub use transfer::{Transfer, Verification};
pub use transport::{HttpsSource, Response};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[cfg(test)]
mod tests;

/// Checked object transfer reused by product source adapters.
pub mod objects;
/// Existing publication-envelope protocol over the shared consumer engine.
pub mod publication;
