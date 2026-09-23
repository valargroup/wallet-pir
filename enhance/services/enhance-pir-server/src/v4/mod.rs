//! Architecture-2 implementation, isolated from the serving v3 rollback path.
pub mod capacity;
pub mod control;
pub mod coordinator;
pub mod exercise;
pub mod runtime;
mod telemetry;
pub mod worker;
