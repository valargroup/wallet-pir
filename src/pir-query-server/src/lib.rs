//! Unified query-server host. Product libraries retain serving authority.
pub mod config;
mod host;
pub use host::{run, verify};
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
