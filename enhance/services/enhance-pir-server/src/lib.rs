//! Coordinator and worker runtime for Ironwood Enhance PIR.

pub mod artifact;
pub mod capacity;
pub mod control;
pub mod coordinator;
pub mod exercise;
pub mod ingest;
pub mod internal_auth;
pub mod ipir;
pub mod runtime;
/// Isolated synthetic status-PIR backend; no production route is enabled implicitly.
pub mod status;
pub mod store;
mod telemetry;
pub mod types;
pub mod wire;
pub mod worker;
pub mod zakura;

pub use enhance_pir::{EnhanceRecord, EnhanceRecordParts};

mod packing_budget;
pub use packing_budget::PackingBudget;

pub mod matvec;
mod response_body;

pub mod packing_router;
pub mod pool;
pub mod query_ingress;
mod serving_control;
mod serving_fence;

mod http_metrics;

mod query_serving;
mod query_timing;

pub mod prepared_packing;
