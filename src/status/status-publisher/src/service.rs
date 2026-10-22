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
        self.publisher.validate()
    }
}
pub async fn run(config: Config, http: Http<axum::Router>, stop: Shutdown) -> Result<()> {
    config.validate()?;
    let publisher = Publisher::new(config.publisher)?;
    let rpc = NodeClient::from_cookie_file(config.rpc_url, config.cookie)?;
    pir_pipeline::publisher::dedicated_runtime("status-publisher", move || async move {
        publisher.run_managed(rpc, http, stop).await
    })
    .await
}

/// Validate and declare this publisher without opening credentials or state.
pub fn configure(
    config: Config,
) -> Result<pir_pipeline::publisher::PublisherDefinition<axum::Router>> {
    use pir_pipeline::publisher::{
        PublisherDefinition, PublisherResources, ReadinessProbe, ReadinessRule,
    };
    config.validate()?;
    let resources = PublisherResources {
        listeners: vec![config.publisher.private_listen],
        writable_paths: vec![config.publisher.state_dir.clone()],
        probes: vec![ReadinessProbe {
            address: config.publisher.private_listen,
            path: "/control/active",
            rule: ReadinessRule::HttpSuccess,
        }],
    };
    Ok(PublisherDefinition::new(
        "status",
        resources,
        move |context| run(config, context.http, context.shutdown),
    ))
}
