//! Static checks open no stores, sockets or credentials. Existing path prefixes
//! are inspected to detect aliases between writable directories.
mod layout;
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
    pub transparent_history: Option<Product<transparent_publisher::history::controller::Config>>,
    #[cfg(feature = "transparent-history")]
    pub transparent_filters:
        Option<Product<transparent_publisher::history::filter_service::Config>>,
    #[cfg(feature = "transparent-detail")]
    pub transaction_details: Option<Product<transparent_publisher::tx_detail::service::Config>>,
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
        validation::validate(self)
    }
}

#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
pub(crate) use validation::active;
