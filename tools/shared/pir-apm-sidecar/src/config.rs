//! Sidecar configuration, rendered per host from the monitoring inventory.
//!
//! The configuration digest is the SHA-256 of the file bytes, so the inventory
//! renderer and the APM can compute the expected value without this parser.
use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub host_id: String,
    #[serde(default = "default_listen")]
    pub listen: SocketAddr,
    /// Loopback-only deploy readiness: identity and whether a collection exists.
    #[serde(default = "default_health")]
    pub health_listen: SocketAddr,
    pub tls: Tls,
    #[serde(default)]
    pub required_volumes: Vec<Volume>,
    #[serde(default)]
    pub units: Vec<String>,
    #[serde(default)]
    pub hooks: Vec<Hook>,
    /// Caddy admin metrics on loopback, served aggregated at `/v1/edge-metrics`.
    #[serde(default)]
    pub edge_metrics_url: Option<String>,
    /// Counter continuity across sidecar restarts.
    pub state_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    /// Relative paths resolve against `$CREDENTIALS_DIRECTORY`.
    pub cert: PathBuf,
    pub key: PathBuf,
    /// Dedicated monitoring CA that issued the collector's client certificate.
    pub client_ca: PathBuf,
    /// DNS name the collector certificate must carry; other same-CA certificates are refused.
    pub collector_identity: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Volume {
    pub path: String,
    /// Filesystem UUID bound at adoption; resolved through `/dev/disk/by-uuid`.
    pub uuid: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hook {
    pub name: String,
    pub argv: Vec<String>,
    #[serde(default = "default_hook_timeout")]
    pub timeout_seconds: u64,
}

fn default_listen() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], pir_host_observations::DEFAULT_PORT))
}
fn default_health() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], pir_host_observations::DEFAULT_PORT + 1))
}
fn default_hook_timeout() -> u64 {
    5
}

pub struct Loaded {
    pub config: Config,
    pub digest: String,
}

pub fn digest(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

impl Config {
    pub fn load(path: &Path) -> Result<Loaded> {
        let bytes = std::fs::read(path).context("reading sidecar config")?;
        let config: Config = serde_json::from_slice(&bytes).context("parsing sidecar config")?;
        config.validate()?;
        Ok(Loaded {
            config,
            digest: digest(&bytes),
        })
    }

    pub fn validate(&self) -> Result<()> {
        use pir_host_observations::valid_name;
        ensure!(valid_name(&self.host_id, 80), "invalid host_id");
        ensure!(
            self.health_listen.ip().is_loopback(),
            "health_listen must be a loopback address"
        );
        ensure!(
            !self.tls.collector_identity.is_empty(),
            "collector_identity required"
        );
        ensure!(self.units.len() <= 32, "too many units");
        for unit in &self.units {
            ensure!(
                valid_name(&unit.to_ascii_lowercase(), 100) && !unit.starts_with('-'),
                "invalid unit name"
            );
        }
        for v in &self.required_volumes {
            ensure!(
                v.path.starts_with('/') && v.path != "/" && !v.path.contains(".."),
                "required volume path must be an absolute non-root mount path"
            );
            ensure!(
                !v.uuid.is_empty()
                    && v.uuid
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() || b == b'-'),
                "invalid volume uuid"
            );
        }
        ensure!(self.hooks.len() <= 16, "too many hooks");
        for h in &self.hooks {
            ensure!(valid_name(&h.name, 48), "invalid hook name");
            ensure!(
                h.argv.first().is_some_and(|p| p.starts_with('/')),
                "hook argv[0] must be an absolute executable path"
            );
            ensure!(
                (1..=30).contains(&h.timeout_seconds),
                "hook timeout must be 1..=30 seconds"
            );
        }
        if let Some(url) = &self.edge_metrics_url {
            ensure!(
                url.starts_with("http://127.0.0.1:") || url.starts_with("http://[::1]:"),
                "edge metrics are read on loopback only"
            );
        }
        Ok(())
    }

    /// Every check name this configuration produces on every collection.
    pub fn expected_checks(&self) -> Vec<String> {
        let mut out = vec![
            "memory".to_string(),
            "swap".to_string(),
            "filesystems".to_string(),
            "certificate".to_string(),
        ];
        out.extend(
            self.required_volumes
                .iter()
                .map(|v| format!("volume:{}", v.path)),
        );
        for unit in &self.units {
            out.extend(
                ["active", "restarts", "oom"].map(|k| format!("unit:{}:{k}", unit.to_lowercase())),
            );
        }
        out.extend(self.hooks.iter().map(|h| format!("hook:{}", h.name)));
        if self.edge_metrics_url.is_some() {
            out.push("edge".into());
        }
        out
    }
}

pub fn credential(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    std::env::var_os("CREDENTIALS_DIRECTORY")
        .map(PathBuf::from)
        .unwrap_or_default()
        .join(path)
}
