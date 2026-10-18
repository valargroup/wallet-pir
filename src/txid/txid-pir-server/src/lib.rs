//! Txid display PIR: the display worker's HTTP service and publication
//! control ([`display`]), and the controller that seals, rebuilds and
//! publishes display shards from the transparent event journal
//! ([`publication`]).

// Both workers compile the same transaction privately; product adapters stay local.
#[path = "../../../shared/activation/mod.rs"]
mod activation;

pub mod display;
pub mod publication;

// The unit tests share the integration tests' display world, which names this
// crate as its users do.
#[cfg(test)]
extern crate self as txid_pir_server;
