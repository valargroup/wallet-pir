//! Private Status role protocol. Transport authentication is supplied by restricted
//! SSH forwarding; every listener and configured peer must be loopback-only.
use crate::protocol::*;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf};

pub type Failure = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Worker,
    Router,
}

/// One private Status role, run by `pir-query-server`.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleConfig {
    pub role: Role,
    pub network_hex: String,
    pub state_dir: PathBuf,
    pub listen: SocketAddr,
    /// Router only: the wallet-facing `/v1/status/*` routes listen here, apart
    /// from control, artifacts, evaluation and telemetry.
    #[serde(default)]
    pub query_listen: Option<SocketAddr>,
    pub publisher: String,
    #[serde(default)]
    pub publisher_ca: Option<PathBuf>,
    pub worker_origin: String,
    #[serde(default)]
    pub cuda: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub epoch: u64,
    pub incarnation: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Health {
    pub role: Role,
    pub binding: Binding,
    pub active: Option<Hash>,
    pub cached_rows: Option<Hash>,
    pub ready: bool,
}

pub fn loopback_origin(origin: &str) -> Result<(), Failure> {
    let url = url::Url::parse(origin)?;
    let ip: std::net::IpAddr = url.host_str().ok_or("missing peer")?.parse()?;
    if url.scheme() != "http"
        || !ip.is_loopback()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "Status peer must be a loopback HTTP origin behind authenticated forwarding".into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_peers_reject_external_or_credentialed_origins() {
        for peer in [
            "https://127.0.0.1",
            "http://example.org",
            "http://10.0.0.1",
            "http://u:p@127.0.0.1",
            "http://127.0.0.1/a",
            "http://127.0.0.1/?q=1",
        ] {
            assert!(loopback_origin(peer).is_err(), "{peer}");
        }
        assert!(loopback_origin("http://127.0.0.1:8381").is_ok());
    }
}
