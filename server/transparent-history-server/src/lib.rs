//! Private transparent-history retrieval over a harness-built generation.
//!
//! Research service. It exists so the directory and page tables can be queried
//! over real HTTP, making the byte comparison against ordinary retrieval a
//! measurement rather than an estimate. It is not enabled for any wallet, and
//! it serves one bounded-range generation rather than lifetime history.

pub mod generation;
pub mod service;
