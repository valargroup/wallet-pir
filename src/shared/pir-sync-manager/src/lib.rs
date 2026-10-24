//! HTTPS transfer, publisher discovery and transport limits used by every PIR
//! query server. Polling, publication succession and commit ordering are
//! application sequencing and live in `pir-query-server`'s `synchronization`
//! module. A successful transfer confers no permission to serve.
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
