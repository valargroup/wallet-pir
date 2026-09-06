//! Private retrieval from a published transparent shard set.
//!
//! Serves the private directory and page tables of many shards from one
//! process, under one shared parameter set per table. It does not serve
//! filters: those are public and belong to the filter service, and a wallet
//! must not learn to fetch public bytes from the same place it makes private
//! requests.

pub mod service;
pub mod shardset;
