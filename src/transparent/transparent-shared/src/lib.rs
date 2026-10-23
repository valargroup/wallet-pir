//! Transparent PIR state shared by the publisher and the query server.
//!
//! Committed event journals and transaction-detail sidecars, history and
//! transaction-detail publication loading, distribution verification and the
//! prepared native runtimes with their disk formats and bounded caches live
//! here. Chain ingestion and publication lifecycle stay in the publisher;
//! admission, request answering, routing and worker control stay in the query
//! server. Nothing here reads a node.

pub mod history;
pub mod http;
pub mod journal;
pub mod publication;
pub mod runtime;
mod segment;
mod table;
pub mod tx_detail;
mod verification;
