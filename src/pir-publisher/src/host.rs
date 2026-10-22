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
    shutdown: Shutdown,
) -> Result<()> {
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

    let mut products = ProductSupervisor::new(status);
    products::start_configured(publishers, &listeners, &mut products);
    products.supervise(&mut management, &shutdown).await
}
