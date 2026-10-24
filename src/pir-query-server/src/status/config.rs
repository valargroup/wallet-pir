//! Operator configuration for one Status role hosted by `pir-query-server`.
use super::peers::{loopback_origin, Failure, Role};
use std::{net::SocketAddr, path::PathBuf};

/// One private Status role, run by `pir-query-server`.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleConfig {
    pub role: Role,
    pub network_hex: String,
    pub state_dir: PathBuf,
    /// Private control, provider, evaluation and telemetry routes. Router and
    /// combined roles answer wallets on the host's `public_listen`.
    pub listen: SocketAddr,
    pub publisher: String,
    #[serde(default)]
    pub publisher_ca: Option<PathBuf>,
    /// Router only: its worker's loopback origin. A combined role evaluates in
    /// process and names no peer.
    #[serde(default)]
    pub worker_origin: String,
}

impl RoleConfig {
    /// Validate role listeners and publisher/worker origins without network or
    /// filesystem access. The host checks collisions with other products.
    pub fn validate(&self) -> Result<(), Failure> {
        if !self.listen.ip().is_loopback() {
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
        match self.role {
            Role::Router => loopback_origin(&self.worker_origin)?,
            Role::Combined if !self.worker_origin.is_empty() => {
                return Err("a combined Status role evaluates locally and names no worker".into())
            }
            _ => {}
        }
        Ok(())
    }

    /// Addresses the host must reserve before any product starts.
    pub fn listens(&self) -> Vec<SocketAddr> {
        vec![self.listen]
    }

    /// Whether this role answers wallets on the host's `public_listen`.
    pub fn serves_wallets(&self) -> bool {
        matches!(self.role, Role::Router | Role::Combined)
    }

    /// Directory exclusively owned by this role while it is running.
    pub fn state_dir(&self) -> &std::path::Path {
        &self.state_dir
    }
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
            publisher: "https://publisher.example".into(),
            publisher_ca: None,
            worker_origin: "http://127.0.0.1:8381".into(),
        }
    }

    #[test]
    fn role_configuration_rejects_removed_cuda_selection() {
        for cuda in [false, true] {
            let error = serde_json::from_value::<RoleConfig>(serde_json::json!({
                "role": "worker",
                "network_hex": "01".repeat(32),
                "state_dir": "/tmp/status-worker",
                "listen": "127.0.0.1:8381",
                "publisher": "https://publisher.example",
                "worker_origin": "",
                "cuda": cuda,
            }))
            .unwrap_err();
            assert!(error.to_string().contains("unknown field `cuda`"));
        }
    }

    #[test]
    fn role_configuration_reports_host_resources_without_opening_them() {
        let role = config();
        role.validate().unwrap();
        assert_eq!(role.listens(), vec![role.listen]);
        assert!(role.serves_wallets());
        assert_eq!(role.state_dir(), std::path::Path::new("/tmp/status-router"));
        let mut worker = role;
        worker.role = Role::Worker;
        worker.worker_origin.clear();
        worker.validate().unwrap();
        assert_eq!(worker.listens(), vec![worker.listen]);
        assert!(!worker.serves_wallets());
    }

    #[test]
    fn role_configuration_rejects_public_listeners_and_invalid_origins() {
        let mut role = config();
        role.listen = "0.0.0.0:8380".parse().unwrap();
        assert!(role.validate().is_err());
        // The wallet listener moved to the host's `public_listen`.
        assert!(serde_json::from_value::<RoleConfig>(serde_json::json!({
            "role": "router", "network_hex": "01".repeat(32),
            "state_dir": "/tmp/status-router", "listen": "127.0.0.1:8380",
            "query_listen": "127.0.0.1:8382", "publisher": "https://publisher.example",
            "worker_origin": "http://127.0.0.1:8381",
        }))
        .is_err());
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
    fn combined_role_needs_no_peer_and_refuses_one() {
        let role: RoleConfig = serde_json::from_value(serde_json::json!({
            "role": "combined",
            "network_hex": "01".repeat(32),
            "state_dir": "/tmp/status-combined",
            "listen": "127.0.0.1:8380",
            "publisher": "https://publisher.example",
        }))
        .unwrap();
        role.validate().unwrap();
        assert_eq!(role.listens().len(), 1);
        let mut peer = role;
        peer.worker_origin = "http://127.0.0.1:8381".into();
        assert!(peer.validate().is_err());
    }
}
