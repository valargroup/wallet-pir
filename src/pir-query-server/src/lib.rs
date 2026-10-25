//! Unified query-server host. It owns product roles, listeners, admission,
//! synchronization, supervision and the private messages those roles exchange.
//! Product libraries supply typed material and prepared evaluation.
pub mod config;
#[cfg(feature = "enhance")]
pub mod enhance;
mod host;
pub mod public;
pub mod readiness;
#[cfg(feature = "receiver")]
pub mod receiver;
#[cfg(feature = "status")]
pub mod status;
#[cfg(any(
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail"
))]
mod supervision;
pub mod synchronization;
#[cfg(any(feature = "transparent-history", feature = "transparent-detail"))]
pub mod transparent;
pub use host::{run, verify};
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[cfg(all(test, any(feature = "status", feature = "receiver")))]
#[path = "../tests/support/status_tls.rs"]
mod test_tls;
