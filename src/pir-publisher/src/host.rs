//! Application composition. Products retain ownership of publication work.
mod listeners;
mod management;
mod products;
mod status;
mod supervision;

use crate::Result;
use listeners::ReservedListeners;
use pir_pipeline::publisher::PublisherDefinition;
use pir_pipeline::service::{Shutdown, Tasks};
use status::PublisherStatus;
use std::net::SocketAddr;
use supervision::ProductSupervisor;

/// Validate settings and reserve every listener before starting product work.
/// Ordinary product exits leave other owners running; a task panic or management
/// exit stops the host. Shutdown joins product operations before returning.
pub async fn run_registered(
    management_listen: SocketAddr,
    publishers: Vec<PublisherDefinition<axum::Router>>,
    chain: Option<pir_pipeline::ingestion::ChainSettings>,
    shutdown: Shutdown,
) -> Result<()> {
    let follows_chain = publishers.iter().any(PublisherDefinition::follows_chain);
    if follows_chain && chain.is_none() {
        return Err("enabled publishers need the root chain settings".into());
    }
    let layout = crate::config::validate_resources(management_listen, &publishers)?;
    let listeners = ReservedListeners::bind_all(&layout.listeners).await?;
    let status = PublisherStatus::default();
    let routes = management::router(status.clone())?;
    let mut management = tokio::spawn(
        listeners
            .http_adapter(shutdown.clone())
            .serve(management_listen, routes),
    );
    let mut auxiliary_tasks = Tasks::default();
    auxiliary_tasks.own(&management);

    // One follower serves every scheduled product; it reads credentials only
    // when a configured product follows the chain.
    let feed = match chain.filter(|_| follows_chain) {
        Some(chain) => Some(follow(chain, &shutdown, &mut auxiliary_tasks)?),
        None => None,
    };
    let mut products = ProductSupervisor::new(status);
    products::start_configured(publishers, &listeners, feed, &mut products);
    products.supervise(&mut management, &shutdown).await
}

#[cfg(feature = "chain")]
fn follow(
    chain: pir_pipeline::ingestion::ChainSettings,
    shutdown: &Shutdown,
    tasks: &mut Tasks,
) -> Result<pir_pipeline::ingestion::ChainFeed> {
    use crate::ingestion::NodeSource;
    use pir_node_rpc::{Credentials, HttpPolicy, RpcClient};
    use pir_pipeline::ingestion::ChainFollower;
    let client = RpcClient::new(
        chain.rpc_url,
        Credentials::read_once(&chain.rpc_cookie)?,
        "pir-publisher-follower",
        HttpPolicy::SHARED,
    )?;
    let follower = ChainFollower::new(std::sync::Arc::new(NodeSource(client)), chain.poll);
    let feed = follower.feed();
    let task = tokio::spawn(follower.run(shutdown.clone()));
    tasks.own(&task);
    Ok(feed)
}
#[cfg(not(feature = "chain"))]
fn follow(
    _: pir_pipeline::ingestion::ChainSettings,
    _: &Shutdown,
    _: &mut Tasks,
) -> Result<pir_pipeline::ingestion::ChainFeed> {
    Err("this build has no chain-following publisher".into())
}
