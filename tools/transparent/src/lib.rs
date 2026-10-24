//! Transparent PIR measurement, regression and benchmark tools.
//!
//! [`assignment`] plans a fleet's shard assignment, lists each worker's files
//! and renders the edge router. [`regression`] holds the independent wallet-state oracle and its frozen
//! fixture schema. With `benchmarks`, [`compact_blocks`] provides the pinned
//! compact-block baseline used by load generation and the block server.
//! Production packages never depend on this package.

pub mod assignment;
#[cfg(feature = "benchmarks")]
pub mod compact_blocks;
pub mod regression;
