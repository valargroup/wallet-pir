//! Coordinator and worker serving runtime for Ironwood Enhance PIR.
//!
//! This library serves prepared publications. It must not depend on chain
//! readers, ingestion or the `enhance-pir-server` application; those compose it
//! there. `tools/ci/boundary.py` enforces the boundary.

pub use enhance_runtime::artifact;
use pir_control::admission;
pub mod capacity;
pub mod control;
pub mod coordinator;
pub mod exercise;
pub mod internal_auth;
pub use enhance_runtime::ipir;
pub use enhance_runtime::runtime;
pub use enhance_runtime::store;
mod telemetry;
pub use enhance_runtime::types;
pub use enhance_runtime::wire;
pub mod worker;

pub use enhance_pir::{EnhanceRecord, EnhanceRecordParts};

use enhance_runtime::packing_budget;
pub use packing_budget::PackingBudget;

pub use enhance_runtime::matvec;
mod memory_observation;
mod response_body;

pub mod packing_router;
pub mod pool;
pub mod query_ingress;
mod serving_control;
mod serving_fence;
pub mod shutdown;

mod http_metrics;

mod query_serving;
mod query_timing;

pub use enhance_runtime::prepared_packing;
