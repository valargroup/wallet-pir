//! Application composition. Products retain ownership of publication work.
mod listeners;
mod management;
mod products;
mod status;
mod supervision;

use crate::{config::Config, Result};
use listeners::ReservedListeners;
use pir_pipeline::service::{Shutdown, Tasks};
use status::PublisherStatus;
use supervision::ProductSupervisor;

/// Validate settings and reserve every listener before starting product work.
/// Ordinary product exits leave other owners running; a task panic or management
/// exit stops the host. Shutdown joins product operations before returning.
pub async fn run(config: Config, shutdown: Shutdown) -> Result<()> {
    let mut layout = config.validated_layout()?;
    let listeners = ReservedListeners::bind_all(&layout.listeners).await?;
    let status = PublisherStatus::default();
    let routes = management::router(status.clone())?;
    let mut management = tokio::spawn(
        listeners
            .http_adapter(shutdown.clone())
            .serve(config.management_listen, routes),
    );
    let mut auxiliary_tasks = Tasks::default();
    auxiliary_tasks.own(&management);

    let mut products = ProductSupervisor::new(status);
    if let Err(error) =
        products::start_configured(&config, &mut layout.probes, &listeners, &mut products)
    {
        products.stop_and_join().await;
        shutdown.request();
        return Err(error);
    }
    products.supervise(&mut management, &shutdown).await
}
