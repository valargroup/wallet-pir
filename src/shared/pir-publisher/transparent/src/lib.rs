//! Transparent publisher: Zakura ingest, the committed event journal, history
//! filter and shard publication, and transaction-detail publication.
//!
//! [`ingest`] follows the node and extracts events, [`history`] assembles
//! filters and shard publications and runs their controller, and
//! [`tx_detail`] publishes and controls transaction-detail sets.

pub mod history;
pub mod ingest;
pub mod tx_detail;

#[cfg(test)]
mod test_chain;
