//! Private Status peer messages exchanged between a router and its worker.
//! Transport authentication is supplied by restricted SSH forwarding; every
//! configured peer must be loopback-only.
use serde::{Deserialize, Serialize};
use status_shared::protocol::*;

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
