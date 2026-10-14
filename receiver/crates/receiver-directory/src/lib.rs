//! Service-side index storage around the independently released receiver protocol.
pub use receiver_protocol::*;
#[cfg(feature = "store")]
pub mod store;
