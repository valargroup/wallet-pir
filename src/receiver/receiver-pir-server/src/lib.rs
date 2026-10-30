//! Receiver PIR serving with immutable publications and atomic session revocation.
#[cfg(test)]
#[path = "../../receiver-store/tests/common/mod.rs"]
mod common;
#[cfg(test)]
extern crate self as receiver_pir_server;
pub mod blocks;
pub mod distribution;
#[cfg(test)]
#[path = "../tests/common/prepared.rs"]
mod prepared;
pub mod publication;
pub mod tables;
pub use publication::{Publication, Publications};
