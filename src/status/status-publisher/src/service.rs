//! Embeddable Status application; process signals and HTTP listeners belong to the host.
use crate::publisher::{Config as PublisherConfig, Publisher};
use pir_pipeline::ingestion::{ChainFeed, ChainSettings, IngestionExecutor};
use pir_pipeline::publisher::PublisherExecutor;
use pir_pipeline::service::{Http, Shutdown};
use serde::Deserialize;
use zakura_server_chain::NodeClient;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub publisher: PublisherConfig,
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        self.publisher.validate()
    }
}
/// Run Status on its dedicated runtime; the window follows the root chain.
pub async fn run<E: PublisherExecutor + IngestionExecutor>(
    config: Config,
    chain: ChainSettings,
    feed: ChainFeed,
    http: Http<axum::Router>,
    stop: Shutdown,
    executor: E,
) -> Result<()> {
    config.validate()?;
    let publisher = Publisher::new(config.publisher)?;
    let rpc = NodeClient::from_cookie_file(
        chain.rpc_url,
        chain
            .rpc_cookie
            .ok_or("Status publisher requires a node cookie")?,
    )?;
    pir_pipeline::publisher::dedicated_runtime("status-publisher", move || async move {
        publisher.run_managed(rpc, feed, http, stop, executor).await
    })
    .await
}

/// Validate and declare this publisher without opening credentials or state.
pub fn configure<E: PublisherExecutor + IngestionExecutor>(
    config: Config,
    chain: Option<&ChainSettings>,
    executor: E,
) -> Result<pir_pipeline::publisher::PublisherDefinition<axum::Router>> {
    use pir_pipeline::publisher::{
        PublisherDefinition, PublisherResources, ReadinessProbe, ReadinessRule,
    };
    config.validate()?;
    let chain = chain
        .cloned()
        .ok_or("enabled status publisher needs the root chain settings")?;
    if chain.rpc_cookie.is_none() {
        return Err("Status publisher requires a node cookie".into());
    }
    let resources = PublisherResources {
        listeners: vec![config.publisher.private_listen],
        writable_paths: vec![config.publisher.state_dir.clone()],
        probes: vec![ReadinessProbe {
            address: config.publisher.private_listen,
            path: "/control/active",
            rule: ReadinessRule::HttpSuccess,
        }],
    };
    Ok(
        PublisherDefinition::new("status", resources, move |context| async move {
            let feed = context
                .chain
                .ok_or("Status publisher started without the root chain")?;
            run(
                config,
                chain,
                feed,
                context.http,
                context.shutdown,
                executor,
            )
            .await
        })
        .following_chain(),
    )
}
