//! Enhance PIR serving primitives below `pir-query-server`: the private
//! evaluation messages between ingress, routers and workers. Wallet-facing
//! routes are mounted by `pir-query-server`; this library has no HTTP stack.
//!
//! Records, the wire protocol, the publication contract, prepared runtimes and
//! the wallet client live in `enhance-shared`. `pir-query-server` owns the
//! serving roles, their synchronization, admission and HTTP surfaces. This
//! library must not depend on `enhance-publisher`, chain readers, ingestion or
//! tools; `tools/ci/boundary.py` enforces the boundary.

pub mod worker;
