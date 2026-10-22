//! Transparent serving roles owned by the query server.
//!
//! Each role checks publisher discovery before opening state, runs initial
//! synchronization under the existing retry rules, then serves the supplied
//! listener while supervising synchronization. History and transaction-detail
//! roles compile independently. Tables, runtimes, handlers and the publication
//! source stay in `transparent_query_server` and `transparent_shared`.
#[cfg(feature = "transparent-detail")]
pub mod detail;
#[cfg(feature = "transparent-history")]
pub mod history;
