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
    /// The one node source followed for every in-scope product. Receiver keeps
    /// its own multi-node settings.
    pub chain: Option<Chain>,
    #[cfg(feature = "enhance")]
    pub enhance: Option<Product<enhance_publisher::service::EnhancePublisherConfig>>,
    #[cfg(feature = "status")]
    pub status: Option<Product<status_publisher::service::Config>>,
    #[cfg(feature = "transparent-history")]
    pub transparent_history: Option<Product<transparent_publisher::service::HistoryConfig>>,
    #[cfg(feature = "transparent-detail")]
    pub transaction_details: Option<Product<transparent_publisher::service::DetailConfig>>,
    #[cfg(feature = "receiver")]
    pub receiver: Option<Product<receiver_indexer::service::Config>>,
}

/// Root chain source settings; required when any in-scope product is enabled.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Chain {
    pub rpc_url: String,
    pub rpc_cookie: std::path::PathBuf,
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
}
fn default_poll_ms() -> u64 {
    1000
}
impl Chain {
    /// Check a credential-free HTTP URL, an absolute cookie path and nonzero
    /// polling without reading the cookie.
    fn settings(&self) -> Result<pir_pipeline::ingestion::ChainSettings> {
        let url = reqwest::Url::parse(&self.rpc_url)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || !self.rpc_cookie.is_absolute()
            || self.poll_ms == 0
        {
            return Err("chain needs a credential-free HTTP RPC URL, an absolute cookie path and nonzero poll_ms".into());
        }
        Ok(pir_pipeline::ingestion::ChainSettings {
            rpc_url: self.rpc_url.clone(),
            rpc_cookie: self.rpc_cookie.clone(),
            poll: std::time::Duration::from_millis(self.poll_ms),
        })
    }
}

impl Config {
    pub(crate) fn chain_settings(&self) -> Option<pir_pipeline::ingestion::ChainSettings> {
        self.chain.as_ref().and_then(|chain| chain.settings().ok())
    }
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
        #[allow(unused_variables)]
        let chain = self.chain.as_ref().map(Chain::settings).transpose()?;
        #[allow(unused_variables)]
        let chain = chain.as_ref();
        #[allow(unused_macros)]
        macro_rules! register {
            ($field:ident, $configure:path $(, $extra:expr)*) => {
                if let Some(settings) = validation::active(&self.$field, stringify!($field))? {
                    publishers.push($configure(settings.clone() $(, $extra)*)?);
                }
            };
        }
        #[cfg(feature = "enhance")]
        register!(
            enhance,
            enhance_publisher::service::configure,
            chain,
            crate::publisher::PublisherEngine
        );
        #[cfg(feature = "status")]
        register!(
            status,
            status_publisher::service::configure,
            chain,
            crate::publisher::PublisherEngine
        );
        #[cfg(feature = "transparent-history")]
        register!(
            transparent_history,
            transparent_publisher::service::history,
            chain,
            crate::publisher::PublisherEngine
        );
        #[cfg(feature = "transparent-detail")]
        register!(
            transaction_details,
            transparent_publisher::service::details,
            chain,
            crate::publisher::PublisherEngine
        );
        #[cfg(feature = "receiver")]
        register!(receiver, receiver::configure);
        Ok(publishers)
    }
}

pub(crate) use validation::validate as validate_resources;
