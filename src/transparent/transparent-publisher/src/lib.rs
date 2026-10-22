//! Transparent publisher: Zakura ingest, the committed event journal, history
//! filter and shard publication, and transaction-detail publication.
//!
//! [`ingest`] follows the node and extracts events, [`history`] assembles
//! filters and shard publications and runs their controller, and
//! [`tx_detail`] publishes and controls transaction-detail sets.

pub mod history;
pub mod ingest;
pub mod tx_detail;

pub mod commands;

/// Product service definitions used by the application composition root.
pub mod service;

// Exercise the host's actual executor without a product-to-application Cargo edge.
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../../pir-publisher/src/publisher/engine.rs"]
mod host_engine;
