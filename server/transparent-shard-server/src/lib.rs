//! Private retrieval from a published transparent shard set.
//!
//! Serves the private directory and page tables of many shard revisions from
//! one process, under one shared parameter set per geometry per table. It also
//! serves the public shard map and range filters, which the filter service
//! serves at the same paths on its own host — so a wallet that wants its public
//! and private bytes to come from different origins still has that, and one
//! that wants a single endpoint has that too.

pub mod admission;
pub mod assignment;
pub mod metrics;
pub mod procmem;
pub mod router;
pub mod runtime;
pub mod service;
pub mod shardset;

pub mod live;
