//! History uses the common Transparent publisher object store.
#[cfg(test)]
pub(crate) use crate::storage::INTERRUPT_INSTALL;
pub use crate::storage::{adopt_runtimes, adopt_segments, collect, Adopted, Collected};
