//! Unified query-server host. Product libraries retain serving authority.
pub mod config;
#[cfg(feature = "enhance")]
mod enhance;
mod host;
pub use host::run;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
