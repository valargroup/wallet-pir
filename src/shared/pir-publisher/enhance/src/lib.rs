//! The Enhance PIR publisher: chain reading, ingestion and the record journals,
//! durable control state, publication decisions, preparation orchestration,
//! activation, recovery, retention, placement and capacity planning, and the
//! `enhance-pir-server` command line. Status publication is a separate process.
//!
//! Query serving lives in `enhance-query-server`, which never depends on this
//! package. [`coordinator::Coordinator`] composes its public
//! [`enhance_query_server::gateway::QueryGateway`] and implements the read-only
//! [`enhance_query_server::gateway::PublisherStatus`] it consults.

// Query-server modules the publication code builds on, named at the crate root
// so publisher modules refer to them alongside their own.
use enhance_query_server::{
    artifact, internal_auth, packing_router, prepared_packing, query_ingress, runtime, types, wire,
    worker, PackingBudget,
};

pub mod application;
pub mod capacity;
pub mod control;
pub mod coordinator;
pub mod ingest;
pub mod pool;
pub mod rebuild;
mod serving_control;
pub mod store;
mod telemetry;
pub mod zakura;

pub mod service;
