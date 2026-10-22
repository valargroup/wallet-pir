//! Transparent publisher: Zakura ingest, the committed event journal, history
//! filter and shard publication, and transaction-detail publication.
//!
//! [`ingest`] follows the node and extracts events, [`history`] assembles
//! filters and shard publications for the root executor, and
//! [`tx_detail`] supplies transaction-detail construction and authority operations.

pub mod history;
pub mod ingest;
pub mod tx_detail;

pub mod commands;

/// Product service definitions used by the application composition root.
pub mod service;

// Exercise the host's actual executor without a product-to-application Cargo edge.
#[cfg(test)]
mod host_engine {
    pub use pir_publisher::publisher::{PreparedPublication, PublisherEngine};
}

/// An operational failure propagated to the publisher host.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Immutable publisher storage and durable state records.
pub mod storage;
