//! Feature-gated adapters to the existing product service entry points.
use super::{listeners::ReservedListeners, supervision::ProductSupervisor};
#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
use crate::config::active;
use crate::{
    config::Config,
    publisher::{PublisherProduct, ReadinessProbe},
    Result,
};
#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
use pir_pipeline::service::Shutdown;
use std::collections::BTreeMap;

pub(super) fn start_configured(
    config: &Config,
    probes: &mut BTreeMap<PublisherProduct, Vec<ReadinessProbe>>,
    listeners: &ReservedListeners,
    supervisor: &mut ProductSupervisor,
) -> Result<()> {
    #[cfg(any(
        feature = "enhance",
        feature = "status",
        feature = "transparent-history",
        feature = "transparent-detail",
        feature = "receiver"
    ))]
    macro_rules! product {
        ($field:ident, $identity:ident, $run:path) => {
            if let Some(settings) = active(&config.$field, PublisherProduct::$identity)? {
                let product = PublisherProduct::$identity;
                let stop = Shutdown::default();
                let probes = probes.remove(&product).expect("validated publisher probes");
                supervisor.start(
                    product,
                    probes,
                    $run(
                        settings.clone(),
                        listeners.http_adapter(stop.clone()),
                        stop.clone(),
                    ),
                    stop,
                );
            }
        };
    }
    #[cfg(feature = "enhance")]
    product!(enhance, Enhance, enhance_publisher::service::run);
    #[cfg(feature = "status")]
    product!(status, Status, status_publisher::service::run);
    #[cfg(feature = "transparent-history")]
    product!(
        transparent_history,
        TransparentHistory,
        transparent_publisher::history::controller::run_managed
    );
    #[cfg(feature = "transparent-history")]
    product!(
        transparent_filters,
        TransparentFilters,
        transparent_publisher::history::filter_service::run
    );
    #[cfg(feature = "transparent-detail")]
    product!(
        transaction_details,
        TransactionDetails,
        transparent_publisher::tx_detail::service::run
    );
    #[cfg(feature = "receiver")]
    product!(receiver, Receiver, receiver_indexer::service::run_isolated);

    let _ = (config, probes, listeners, supervisor);
    Ok(())
}
