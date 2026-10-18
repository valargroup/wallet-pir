//! The Enhance and Status PIR application: chain reading, ingestion, rebuilds,
//! Status observation and publication, and the `enhance-pir-server` and
//! `status-pir` command lines.
//!
//! Serving lives in `enhance-serving` and `status-serving`, which never depend
//! on the chain readers here. Their modules are re-exported so existing paths
//! under `enhance_pir_server` keep resolving.

pub use enhance_serving::*;
use pir_control::admission;

pub mod application;
pub mod ingest;
pub mod rebuild;
/// Status serving plus its chain observation, publication and CLI.
pub mod status {
    pub use status_serving::*;
    pub mod cli;
    pub mod publisher;
    pub mod source;
}
pub mod zakura;
