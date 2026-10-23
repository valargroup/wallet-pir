//! Unified query-server host. It owns product roles, listeners, admission,
//! synchronization and supervision; product libraries supply typed material,
//! evaluation and peer-message primitives.
pub mod config;
#[cfg(feature = "enhance")]
pub mod enhance;
mod host;
pub mod public;
#[cfg(feature = "status")]
pub mod status;
pub mod synchronization;
#[cfg(any(feature = "transparent-history", feature = "transparent-detail"))]
pub mod transparent;
pub use host::{run, verify};
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
