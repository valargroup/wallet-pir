//! Unified application host. Product libraries retain publication authority.
pub mod config;
mod host;
mod publisher;
pub use host::run;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
