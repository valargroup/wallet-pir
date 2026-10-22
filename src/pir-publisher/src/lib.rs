//! Unified application host. Product libraries retain publication authority.
pub mod config;
mod host;
#[cfg(feature = "chain")]
mod ingestion;
pub mod publisher;
pub use host::run_registered;

/// Compose the configured product facades, then delegate to the generic host.
pub async fn run(config: config::Config, shutdown: pir_pipeline::service::Shutdown) -> Result<()> {
    let publishers = config.publishers()?;
    run_registered(config.management_listen, publishers, config.chain_settings(), shutdown).await
}
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
