//! Private Status role protocol. Transport authentication is supplied by restricted
//! SSH forwarding; every listener and configured peer must be loopback-only.
use super::{artifact, index, Generation};
use crate::protocol::*;
use ipir_sp::server::MatvecBackend;
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
    pub artifact_origin: String,
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
pub struct Prepare {
    pub binding: Binding,
    pub manifest: Manifest,
    pub artifact_digest: Hash,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ready {
    pub binding: Binding,
    pub manifest: Manifest,
    pub artifact_digest: Hash,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Activate {
    pub binding: Binding,
    pub manifest: Manifest,
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

/// The most bytes a role accepts for one candidate artifact.
pub fn artifact_limit(role: Role) -> usize {
    match role {
        Role::Worker => ROWS * ROW_BYTES,
        Role::Router => Generation::hint_len(),
    }
}

/// Prepare a role's candidate from fetched, digest-checked artifact bytes. A
/// worker may receive a delta against `previous`'s rows; a router receives the
/// worker's full hint. Runs on the caller's thread pool.
pub fn prepare_candidate(
    role: Role,
    manifest: Manifest,
    bytes: Vec<u8>,
    previous: Option<&Generation>,
    backend: MatvecBackend,
) -> Result<Generation, Error> {
    match role {
        Role::Router => Generation::prepare_router_with_previous(manifest, &bytes, previous),
        Role::Worker => {
            let base = if bytes.len() < ROWS * ROW_BYTES {
                previous.map(|g| {
                    (
                        g.manifest().rows_digest,
                        g.units
                            .iter()
                            .flat_map(|u| u.rows.iter().copied())
                            .collect(),
                    )
                })
            } else {
                None
            };
            let rows = artifact::decode(bytes, manifest.rows_digest, base)?;
            let snapshot = index::Snapshot {
                network: manifest.network,
                salt: manifest.salt,
                start: manifest.coverage_start,
                height: manifest.anchor_height,
                anchor: manifest.anchor_hash,
                entries: manifest.entries,
                rows,
                digest: manifest.rows_digest,
                evicted_blocks: 0,
            };
            let (mut g, _) = Generation::prepare_worker(
                &snapshot,
                manifest.generation,
                manifest.recovery_epoch,
                manifest.observed_ms,
                previous,
                backend,
            )?;
            g.assign_manifest(manifest);
            Ok(g)
        }
    }
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
