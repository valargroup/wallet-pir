//! The Enhance PIR application: chain reading, ingestion, rebuilds and the
//! `enhance-pir-server` command line. The coordinator can host the Status
//! publisher from `status-publication`; the `status-pir` command line lives in
//! `status-pir-server`.
//!
//! Serving lives in `enhance-serving`, which never depends on the chain readers
//! here. Its modules are re-exported so existing paths under
//! `enhance_pir_server` keep resolving.

pub use enhance_serving::*;

pub mod application;
pub mod ingest;
pub mod rebuild;
pub mod zakura;
