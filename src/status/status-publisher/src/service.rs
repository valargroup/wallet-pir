//! Embeddable Status application; process signals and HTTP listeners belong to the host.
use crate::publisher::{Config as PublisherConfig, Publisher};
use pir_pipeline::service::{Http, Shutdown};
use serde::Deserialize;
use std::path::PathBuf;
use zakura_server_chain::NodeClient;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub publisher: PublisherConfig,
    pub rpc_url: String,
    pub cookie: PathBuf,
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        Publisher::new(self.publisher.clone())?;
        Ok(())
    }
}
pub async fn run(config: Config, http: Http<axum::Router>, stop: Shutdown) -> Result<()> {
    config.validate()?;
    let publisher = Publisher::new(config.publisher)?;
    let rpc = NodeClient::from_cookie_file(config.rpc_url, config.cookie)?;
    let thread = std::thread::Builder::new()
        .name("status-publisher".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .max_blocking_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(publisher.run_managed(rpc, http, stop))
        })?;
    tokio::task::spawn_blocking(move || {
        thread
            .join()
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                "Status publisher thread panicked".into()
            })?
    })
    .await?
}
