//! Static checks open no stores, sockets or credentials. Existing path prefixes
//! are inspected to detect aliases between writable directories.
mod layout;
#[cfg(feature = "receiver")]
mod receiver;
#[cfg(test)]
mod tests;
mod validation;

use crate::Result;
use serde::Deserialize;
use std::net::SocketAddr;

/// A configured product is inactive when omitted or disabled. Disabled products
/// need no settings and start no runtime work.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Product<T> {
    pub enabled: bool,
    pub settings: Option<T>,
}

/// Independently configured publishers hosted in one process.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub management_listen: SocketAddr,
    #[cfg(feature = "enhance")]
    pub enhance: Option<Product<enhance_publisher::service::Config>>,
    #[cfg(feature = "status")]
    pub status: Option<Product<status_publisher::service::Config>>,
    #[cfg(feature = "transparent-history")]
    pub transparent_history: Option<Product<transparent_publisher::service::HistoryConfig>>,
    #[cfg(feature = "transparent-history")]
    pub transparent_filters: Option<Product<transparent_publisher::service::FilterConfig>>,
    #[cfg(feature = "transparent-detail")]
    pub transaction_details: Option<Product<transparent_publisher::service::DetailConfig>>,
    #[cfg(feature = "receiver")]
    pub receiver: Option<Product<receiver_indexer::service::Config>>,
}

impl Config {
    /// Check product settings, listener conflicts and writable-path overlap.
    /// This cannot establish runtime readiness or availability of chain inputs.
    pub fn validate(&self) -> Result<()> {
        self.validated_layout().map(|_| ())
    }

    pub(crate) fn validated_layout(&self) -> Result<layout::PublisherLayout> {
        self.layout(&self.publishers()?)
    }
}

impl Config {
    pub(crate) fn layout(
        &self,
        publishers: &[pir_pipeline::publisher::PublisherDefinition<axum::Router>],
    ) -> Result<layout::PublisherLayout> {
        validation::validate(self.management_listen, publishers)
    }
    pub(crate) fn publishers(
        &self,
    ) -> Result<Vec<pir_pipeline::publisher::PublisherDefinition<axum::Router>>> {
        #[allow(unused_mut)]
        let mut publishers = Vec::new();
        #[allow(unused_macros)]
        macro_rules! register {
            ($field:ident, $configure:path) => {
                if let Some(settings) = validation::active(&self.$field, stringify!($field))? {
                    publishers.push($configure(settings.clone())?);
                }
            };
        }
        #[cfg(feature = "enhance")]
        register!(enhance, enhance_publisher::service::configure);
        #[cfg(feature = "status")]
        register!(status, status_publisher::service::configure);
        #[cfg(feature = "transparent-history")]
        register!(transparent_history, transparent_publisher::service::history);
        #[cfg(feature = "transparent-history")]
        register!(transparent_filters, transparent_publisher::service::filters);
        #[cfg(feature = "transparent-detail")]
        register!(transaction_details, transparent_publisher::service::details);
        #[cfg(feature = "receiver")]
        register!(receiver, receiver::configure);
        Ok(publishers)
    }
}

pub(crate) use validation::validate as validate_resources;
