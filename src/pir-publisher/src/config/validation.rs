//! Generic validation of product-declared resources.
use super::layout::PublisherLayout;
use crate::Result;
use pir_pipeline::publisher::PublisherDefinition;
use std::net::SocketAddr;

pub(crate) fn validate(
    management_listen: SocketAddr,
    publishers: &[PublisherDefinition<axum::Router>],
) -> Result<PublisherLayout> {
    if !management_listen.ip().is_loopback() {
        return Err("management listener must be loopback".into());
    }
    let mut layout = PublisherLayout::default();
    layout.declare_listener("management", management_listen)?;
    for publisher in publishers {
        if layout.probes.contains_key(publisher.name) {
            return Err(format!("duplicate publisher {}", publisher.name).into());
        }
        for address in &publisher.resources.listeners {
            layout.declare_listener(publisher.name, *address)?;
        }
        for path in &publisher.resources.writable_paths {
            layout.declare_writable_path(publisher.name, path)?;
        }
        layout
            .probes
            .insert(publisher.name, publisher.resources.probes.clone());
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
pub(super) fn active<'a, T>(
    product: &'a Option<super::Product<T>>,
    identity: &'static str,
) -> Result<Option<&'a T>> {
    match product {
        Some(super::Product {
            enabled: true,
            settings,
        }) => settings
            .as_ref()
            .map(Some)
            .ok_or_else(|| format!("enabled {identity} publisher has no settings").into()),
        _ => Ok(None),
    }
}
