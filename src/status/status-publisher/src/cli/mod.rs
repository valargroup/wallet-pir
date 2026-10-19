//! The `status-pir` subcommands. The binary only parses arguments and calls one
//! function here per subcommand.
//!
//! Production runs `role`, `serve-distributed`, `probe-live` and
//! `probe-live-load`. The other commands serve or check the synthetic fixture,
//! or run the experimental single-process live loop.
pub mod http_client;
mod live;
mod live_load;
mod production;
mod synthetic;
pub use live::{observe_live, serve_live, LiveConfig};
pub use live_load::probe_live_load;
pub use production::{probe_live, role, serve_distributed, RoleConfig};
pub use synthetic::{probe, serve, validate, validate_distributed, ProbeConfig};

use ipir_sp::server::MatvecBackend;

pub type AnyError = Box<dyn std::error::Error + Send + Sync>;
fn require(ok: bool, message: &str) -> Result<(), AnyError> {
    if ok {
        Ok(())
    } else {
        Err(message.into())
    }
}
fn backend(cuda: bool) -> MatvecBackend {
    if cuda {
        MatvecBackend::Cuda { device: 0 }
    } else {
        MatvecBackend::Cpu
    }
}
