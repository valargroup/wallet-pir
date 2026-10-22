//! Private Status role protocol. Transport authentication is supplied by restricted
//! SSH forwarding; every listener and configured peer must be loopback-only.
use serde::{Deserialize, Serialize};
use status_shared::protocol::*;
use std::{net::SocketAddr, path::PathBuf};

pub type Failure = Box<dyn std::error::Error + Send + Sync>;

/// What a worker tells its router: the inventory and manifest it serves.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderStatus {
    pub inventory: Option<pir_publication::ObjectReference>,
    pub manifest: Option<Manifest>,
    pub synchronization: String,
}

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

impl RoleConfig {
    /// Validate role listeners and publisher/worker origins without network or
    /// filesystem access. The host checks collisions with other products.
    pub fn validate(&self) -> Result<(), Failure> {
        if !self.listen.ip().is_loopback()
            || self
                .query_listen
                .is_some_and(|addr| !addr.ip().is_loopback())
        {
            return Err("Status role listener must be loopback".into());
        }
        let url = url::Url::parse(&self.publisher)?;
        if url.scheme() != "https"
            || url.host_str().is_none()
            || url.path() != "/"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err("Status publisher must be an explicit HTTPS origin".into());
        }
        if self.role == Role::Router {
            loopback_origin(&self.worker_origin)?;
        }
        Ok(())
    }

    /// Addresses the host must reserve before any product starts.
    pub fn listens(&self) -> Vec<SocketAddr> {
        std::iter::once(self.listen)
            .chain(self.query_listen)
            .collect()
    }

    /// Directory exclusively owned by this role while it is running.
    pub fn state_dir(&self) -> &std::path::Path {
        &self.state_dir
    }

    /// Existing control endpoint used by the host's readiness check.
    pub fn probe(&self) -> (SocketAddr, &'static str) {
        (self.listen, "/control/health")
    }
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

    fn config() -> RoleConfig {
        RoleConfig {
            role: Role::Router,
            network_hex: "01".repeat(32),
            state_dir: PathBuf::from("/tmp/status-router"),
            listen: "127.0.0.1:8380".parse().unwrap(),
            query_listen: Some("127.0.0.1:8382".parse().unwrap()),
            publisher: "https://publisher.example".into(),
            publisher_ca: None,
            worker_origin: "http://127.0.0.1:8381".into(),
            cuda: false,
        }
    }

    #[test]
    fn role_configuration_reports_host_resources_without_opening_them() {
        let role = config();
        role.validate().unwrap();
        assert_eq!(
            role.listens(),
            vec![role.listen, role.query_listen.unwrap()]
        );
        assert_eq!(role.state_dir(), std::path::Path::new("/tmp/status-router"));
        assert_eq!(role.probe(), (role.listen, "/control/health"));
        let mut worker = role;
        worker.role = Role::Worker;
        worker.query_listen = None;
        worker.worker_origin.clear();
        worker.validate().unwrap();
        assert_eq!(worker.listens(), vec![worker.listen]);
    }

    #[test]
    fn role_configuration_rejects_public_listeners_and_invalid_origins() {
        for query in [false, true] {
            let mut role = config();
            let public = "0.0.0.0:8380".parse().unwrap();
            if query {
                role.query_listen = Some(public);
            } else {
                role.listen = public;
            }
            assert!(role.validate().is_err());
        }
        for origin in [
            "http://publisher.example",
            "https://publisher.example/path",
            "https://user:password@publisher.example",
            "https://publisher.example/?q=1",
            "https://publisher.example/#fragment",
        ] {
            let mut role = config();
            role.publisher = origin.into();
            assert!(role.validate().is_err(), "{origin}");
        }
        let mut role = config();
        role.worker_origin = "http://external.example".into();
        assert!(role.validate().is_err());
    }

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
