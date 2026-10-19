//! Status PIR ingestion, publication and the `status-pir` application.
//!
//! Ingestion checkpoints source observations independently of publication.
//! Prepared datasets are served by `status-query-server`; Enhance can embed
//! this publisher without depending on the standalone command-line executable.

use pir_control::admission;
#[cfg(feature = "cli")]
use status_query_server as status;
use status_query_server::*;

pub mod authority;
#[cfg(feature = "cli")]
pub mod cli;
pub mod publisher;
pub mod source;

mod zakura {
    pub use zakura_server_chain::{NodeClient as ZakuraClient, NodeError as ZakuraError};
}

pub mod service;
