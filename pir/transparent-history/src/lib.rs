//! Private transparent-history retrieval: protocol types and client.
//!
//! Scope is deliberately narrow. This exists so the directory and page tables
//! the research harness already builds can be retrieved over real transport,
//! making the byte comparison against ordinary retrieval a measurement rather
//! than an estimate. It is not enabled for any wallet.

pub mod client;
pub mod types;

pub use client::{ByteCharges, ClientError, TableClient, TransparentHistoryClient};
pub use types::*;
