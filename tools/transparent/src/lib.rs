//! Transparent PIR measurement, regression and benchmark tools.
//!
//! [`regression`] holds the independent wallet-state oracle and its frozen
//! fixture schema. With `benchmarks`, [`compact_blocks`] provides the pinned
//! compact-block baseline used by load generation and the block server.
//! Production packages never depend on this package.

#[cfg(feature = "benchmarks")]
pub mod compact_blocks;
pub mod regression;
