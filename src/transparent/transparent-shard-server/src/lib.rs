//! Private retrieval from a published transparent shard set.
//!
//! Serves the private directory and page tables of many shard revisions from
//! one process, under one shared parameter set per geometry per table. It also
//! serves the public shard map and range filters, which the filter service
//! serves at the same paths on its own host — so a wallet that wants its public
//! and private bytes to come from different origins still has that, and one
//! that wants a single endpoint has that too.

// Both workers compile the same transaction privately; product adapters stay local.
#[path = "../../../shared/activation/mod.rs"]
mod activation;
use transparent_server_backend::{admission, control, prewarm, request};
pub mod router;
pub mod service;

pub mod live;

// Retain the service library paths used by existing tools.
pub use transparent_server_backend::{
    assignment, memory, metrics, prebuild, procmem, runtime, shardset,
};

pub mod bootstrap;

pub mod manager;

pub mod archive;
