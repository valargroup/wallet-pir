//! The `status-pir` subcommands. The binary only parses arguments and calls one
//! function here per subcommand.
//!
//! Production diagnostics use `probe-live` and `probe-live-load`.
//! Query roles run in `pir-query-server`; this binary never serves wallets.
pub mod http_client;
mod live;
mod live_load;
mod production;
mod synthetic;
pub use live::observe_live;
pub use live_load::probe_live_load;
pub use production::probe_live;
pub use synthetic::validate_distributed;

pub type AnyError = Box<dyn std::error::Error + Send + Sync>;
fn require(ok: bool, message: &str) -> Result<(), AnyError> {
    if ok {
        Ok(())
    } else {
        Err(message.into())
    }
}
