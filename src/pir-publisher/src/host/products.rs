//! Generic registration startup; this module has no product dependencies.
use super::{listeners::ReservedListeners, supervision::ProductSupervisor};
use pir_pipeline::{
    publisher::{PublisherDefinition, ServiceContext},
    service::Shutdown,
};
pub(super) fn start_configured(
    publishers: Vec<PublisherDefinition<axum::Router>>,
    listeners: &ReservedListeners,
    supervisor: &mut ProductSupervisor,
) {
    for publisher in publishers {
        let name = publisher.name;
        let probes = publisher.resources.probes.clone();
        let shutdown = Shutdown::default();
        let context = ServiceContext {
            http: listeners.http_adapter(shutdown.clone()),
            shutdown: shutdown.clone(),
        };
        supervisor.start(name, probes, publisher.start(context), shutdown);
    }
}
