//! Product adapters declare their host resources after product-owned validation.
#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
use super::Product;
use super::{layout::PublisherLayout, Config};
#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
use crate::publisher::{PublisherProduct, ReadinessRule};
use crate::Result;

pub(super) fn validate(config: &Config) -> Result<PublisherLayout> {
    let mut layout = PublisherLayout::default();
    if !config.management_listen.ip().is_loopback() {
        return Err("management listener must be loopback".into());
    }
    layout.declare_listener("management", config.management_listen)?;
    #[cfg(feature = "enhance")]
    if let Some(settings) = active(&config.enhance, PublisherProduct::Enhance)? {
        settings
            .validate()
            .map_err(|source| ProductValidationError {
                product: PublisherProduct::Enhance,
                source,
            })?;
        layout.declare_listener("enhance", settings.listen)?;
        if let Some(addr) = settings.artifact_listen {
            layout.declare_listener("enhance", addr)?;
        }
        layout.declare_writable_path(PublisherProduct::Enhance, &settings.data_dir)?;
        layout.declare_readiness_probe(
            PublisherProduct::Enhance,
            settings.listen,
            "/ready",
            ReadinessRule::HttpSuccess,
        );
    }
    #[cfg(feature = "status")]
    if let Some(settings) = active(&config.status, PublisherProduct::Status)? {
        settings
            .validate()
            .map_err(|source| ProductValidationError {
                product: PublisherProduct::Status,
                source,
            })?;
        layout.declare_listener("status", settings.publisher.private_listen)?;
        layout.declare_writable_path(PublisherProduct::Status, &settings.publisher.state_dir)?;
        layout.declare_readiness_probe(
            PublisherProduct::Status,
            settings.publisher.private_listen,
            "/control/active",
            ReadinessRule::HttpSuccess,
        );
    }
    #[cfg(feature = "transparent-history")]
    if let Some(settings) = active(
        &config.transparent_history,
        PublisherProduct::TransparentHistory,
    )? {
        settings
            .validate()
            .map_err(|source| ProductValidationError {
                product: PublisherProduct::TransparentHistory,
                source,
            })?;
        if settings.distribute_prepared && !settings.ship_runtimes {
            return Err("prepared distribution requires ship_runtimes".into());
        }
        layout.declare_listener("transparent_history", settings.listen)?;
        layout.declare_writable_path(
            PublisherProduct::TransparentHistory,
            &settings.publication_root,
        )?;
        if !settings.follow_journal {
            layout
                .declare_writable_path(PublisherProduct::TransparentHistory, &settings.data_dir)?;
        }
        if let Some(path) = &settings.snapshot_dir {
            layout.declare_writable_path(PublisherProduct::TransparentHistory, path)?;
        }
        layout.declare_readiness_probe(
            PublisherProduct::TransparentHistory,
            settings.listen,
            "/v1/status",
            ReadinessRule::ServingPhase,
        );
        if !settings.http_only {
            layout.declare_readiness_probe(
                PublisherProduct::TransparentHistory,
                settings.listen,
                "/v2/shards/init",
                ReadinessRule::HttpSuccess,
            );
        }
    }
    #[cfg(feature = "transparent-history")]
    if let Some(settings) = active(
        &config.transparent_filters,
        PublisherProduct::TransparentFilters,
    )? {
        settings
            .validate()
            .map_err(|source| ProductValidationError {
                product: PublisherProduct::TransparentFilters,
                source,
            })?;
        layout.declare_listener("transparent_filters", settings.listen)?;
        layout.declare_writable_path(PublisherProduct::TransparentFilters, &settings.data_dir)?;
        layout.declare_readiness_probe(
            PublisherProduct::TransparentFilters,
            settings.listen,
            "/v1/health",
            ReadinessRule::ServingPhase,
        );
    }
    #[cfg(feature = "transparent-detail")]
    if let Some(settings) = active(
        &config.transaction_details,
        PublisherProduct::TransactionDetails,
    )? {
        settings
            .validate()
            .map_err(|source| ProductValidationError {
                product: PublisherProduct::TransactionDetails,
                source,
            })?;
        layout.declare_listener("transaction_details", settings.status_listen)?;
        layout.declare_writable_path(PublisherProduct::TransactionDetails, &settings.root)?;
        layout.declare_writable_path(PublisherProduct::TransactionDetails, &settings.journal)?;
        layout.declare_readiness_probe(
            PublisherProduct::TransactionDetails,
            settings.status_listen,
            "/v1/status",
            ReadinessRule::TransactionDetailsPublished,
        );
    }
    #[cfg(feature = "receiver")]
    if let Some(settings) = active(&config.receiver, PublisherProduct::Receiver)? {
        settings
            .validate()
            .map_err(|source| ProductValidationError {
                product: PublisherProduct::Receiver,
                source,
            })?;
        if !settings.serve {
            return Err("unified Receiver publishing requires continuous serving".into());
        }
        layout.declare_listener("receiver", settings.bind)?;
        layout.declare_writable_path(PublisherProduct::Receiver, &settings.data_dir)?;
        layout.declare_readiness_probe(
            PublisherProduct::Receiver,
            settings.bind,
            "/v1/receiver/init",
            ReadinessRule::HttpSuccess,
        );
    }
    Ok(layout)
}

#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
pub(crate) fn active<T>(
    product: &Option<Product<T>>,
    identity: PublisherProduct,
) -> Result<Option<&T>> {
    match product {
        Some(Product {
            enabled: true,
            settings,
        }) => settings
            .as_ref()
            .map(Some)
            .ok_or_else(|| format!("enabled {} publisher has no settings", identity.name()).into()),
        _ => Ok(None),
    }
}

#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
#[derive(Debug)]
struct ProductValidationError {
    product: PublisherProduct,
    source: Box<dyn std::error::Error + Send + Sync>,
}
#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
impl std::fmt::Display for ProductValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "invalid {} publisher settings: {}",
            self.product.name(),
            self.source
        )
    }
}
#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
impl std::error::Error for ProductValidationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
