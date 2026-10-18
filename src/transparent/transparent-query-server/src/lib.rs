//! Private retrieval from a published transparent shard set.
//!
//! Serves the private directory and page tables of many shard revisions from
//! one process, under one shared parameter set per geometry per table. It also
//! serves the public shard map and range filters, which the filter service
//! serves at the same paths on its own host — so a wallet that wants its public
//! and private bytes to come from different origins still has that, and one
//! that wants a single endpoint has that too.
//!
//! History serving lives under [`history`], transaction-detail serving under
//! [`tx_detail`], and bootstrap, management, archive and routing under
//! [`routing`]. Admission, request answering, service configuration,
//! preparation lifecycle and worker control sockets are query-owned; verified
//! publications and prepared runtimes come from `transparent_shared`.

mod activation;
pub mod admission;
pub mod config;
pub mod control;
pub mod history;
pub mod preparation;
pub mod request;
pub mod routing;
pub mod tx_detail;
