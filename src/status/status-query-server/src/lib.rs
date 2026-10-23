//! Status PIR serving primitives below `pir-query-server`: the private peer
//! messages a router and worker exchange. Wallet-facing routes are mounted by
//! `pir-query-server`; this library has no HTTP stack.
//!
//! Generations, the wire protocol and the publication contract live in
//! `status-shared`. `pir-query-server` owns Status roles, listeners,
//! admission, synchronization and outbound HTTP; chain observation and
//! publication remain in `status-publisher`, which this crate must never
//! depend on.
pub mod distributed;
