//! Status PIR ingestion, publication and the `pir-publisher status` diagnostics.
//!
//! Ingestion checkpoints source observations independently of publication.
//! Prepared datasets are served by `pir-query-server` roles; this package never
//! faces wallets.

#[cfg(feature = "cli")]
use status_shared as status;
use status_shared::*;

pub mod authority;
#[cfg(feature = "cli")]
pub mod cli;
pub mod publisher;
pub mod source;

mod zakura {
    pub use zakura_server_chain::{NodeClient as ZakuraClient, NodeError as ZakuraError};
}

pub mod service;

#[cfg(feature = "cli")]
pub mod commands;

// Exercise the host's actual executor without a product-to-application Cargo edge.
#[cfg(test)]
#[allow(dead_code)]
#[path = "../../../pir-publisher/src/publisher/engine.rs"]
mod host_engine;
