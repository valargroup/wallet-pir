//! Static configuration checks never open stores, bind sockets, or read credentials.
use crate::Result;
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Product<T> {
    pub enabled: bool,
    pub settings: Option<T>,
}

impl<T> Product<T> {
    pub fn active(&self) -> Result<Option<&T>> {
        if !self.enabled {
            return Ok(None);
        }
        self.settings
            .as_ref()
            .map(Some)
            .ok_or_else(|| "enabled query server has no settings".into())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub management_listen: SocketAddr,
    #[cfg(feature = "enhance")]
    pub enhance: Option<Product<enhance_query_server::service::ServingRole>>,
    #[cfg(feature = "status")]
    pub status: Option<Product<status_query_server::distributed::RoleConfig>>,
    #[cfg(feature = "transparent-history")]
    pub transparent_history:
        Option<Product<transparent_query_server::history::process::HistoryProcess>>,
    #[cfg(feature = "transparent-detail")]
    pub transaction_details:
        Option<Product<transparent_query_server::tx_detail::process::DetailProcess>>,
}

#[cfg(any(feature = "transparent-history", feature = "transparent-detail"))]
const ORDINARY_VERIFY: &str =
    "product verify_only is reserved for pir-query-server --verify-only; remove it from serving configuration";

/// The one Transparent section a `--verify-only` run checks.
pub enum Verification<'a> {
    #[cfg(feature = "transparent-history")]
    History(&'a transparent_query_server::history::process::HistoryProcess),
    #[cfg(feature = "transparent-detail")]
    Detail(&'a transparent_query_server::tx_detail::process::DetailProcess),
    #[doc(hidden)]
    _Unused(std::marker::PhantomData<&'a ()>),
}

#[derive(Default)]
pub struct Layout {
    pub listeners: BTreeMap<SocketAddr, &'static str>,
    pub roots: Vec<(&'static str, PathBuf)>,
    pub probes: BTreeMap<&'static str, Vec<Probe>>,
}

#[derive(Clone)]
pub struct Probe {
    pub address: SocketAddr,
    pub path: &'static str,
}

pub fn active<T>(product: &Option<Product<T>>) -> Result<Option<&T>> {
    product
        .as_ref()
        .map(Product::active)
        .transpose()
        .map(Option::flatten)
}

pub fn private(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
    }
}

impl Layout {
    fn listener(&mut self, owner: &'static str, addr: SocketAddr) -> Result<()> {
        if !private(addr) || addr.port() == 0 {
            return Err("query server listeners need a fixed loopback or private address".into());
        }
        if self.listeners.keys().any(|other| {
            other == &addr
                || (other.port() == addr.port()
                    && (other.ip().is_unspecified() || addr.ip().is_unspecified()))
        }) {
            return Err(format!("conflicting query server listeners at {addr}").into());
        }
        self.listeners.insert(addr, owner);
        Ok(())
    }

    #[cfg_attr(
        not(any(
            feature = "enhance",
            feature = "status",
            feature = "transparent-history",
            feature = "transparent-detail"
        )),
        allow(dead_code)
    )]
    fn root(&mut self, owner: &'static str, root: &Path) -> Result<()> {
        if !root.is_absolute() {
            return Err("query server state paths must be absolute".into());
        }
        let path = normalize(root)?;
        if self.roots.iter().any(|(other, existing)| {
            *other != owner && (path.starts_with(existing) || existing.starts_with(&path))
        }) {
            return Err("enabled query servers have overlapping writable state paths".into());
        }
        self.roots.push((owner, path));
        Ok(())
    }

    #[cfg(any(
        feature = "enhance",
        feature = "status",
        feature = "transparent-history",
        feature = "transparent-detail"
    ))]
    fn probe(&mut self, name: &'static str, address: SocketAddr, path: &'static str) {
        self.probes
            .entry(name)
            .or_default()
            .push(Probe { address, path });
    }
}

#[cfg_attr(
    not(any(
        feature = "enhance",
        feature = "status",
        feature = "transparent-history",
        feature = "transparent-detail"
    )),
    allow(dead_code)
)]
fn normalize(path: &Path) -> Result<PathBuf> {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                result.pop();
            }
            std::path::Component::CurDir => {}
            _ => {
                result.push(component);
                if result.exists() {
                    result = result.canonicalize()?;
                }
            }
        }
    }
    Ok(result)
}

impl Config {
    /// Select the sole enabled compiled Transparent section for one-shot
    /// verification. Any other enabled product is refused, so a verification
    /// run cannot start serving work.
    pub fn verification(&self) -> Result<Verification<'_>> {
        #[allow(unused_mut)]
        let mut selected = Vec::new();
        #[cfg(feature = "enhance")]
        if self.enhance.as_ref().is_some_and(|p| p.enabled) {
            return Err("--verify-only accepts only a Transparent section; disable enhance".into());
        }
        #[cfg(feature = "status")]
        if self.status.as_ref().is_some_and(|p| p.enabled) {
            return Err("--verify-only accepts only a Transparent section; disable status".into());
        }
        #[cfg(feature = "transparent-history")]
        if let Some(worker) = active(&self.transparent_history)? {
            selected.push(Verification::History(worker));
        }
        #[cfg(feature = "transparent-detail")]
        if let Some(worker) = active(&self.transaction_details)? {
            selected.push(Verification::Detail(worker));
        }
        if selected.len() != 1 {
            return Err(
                "--verify-only requires exactly one enabled transparent_history or transaction_details section"
                    .into(),
            );
        }
        Ok(selected.pop().unwrap())
    }

    pub fn validate(&self) -> Result<Layout> {
        let mut layout = Layout::default();
        if !self.management_listen.ip().is_loopback() {
            return Err("management listener must be loopback".into());
        }
        layout.listener("management", self.management_listen)?;
        #[cfg(feature = "enhance")]
        if let Some(role) = active(&self.enhance)? {
            for addr in role.listens() {
                layout.listener("enhance", addr)?;
            }
            layout.root("enhance", role.state_dir())?;
            let (address, path) = role.probe();
            layout.probe("enhance", address, path);
        }
        #[cfg(feature = "status")]
        if let Some(role) = active(&self.status)? {
            if !role.listen.ip().is_loopback()
                || role
                    .query_listen
                    .is_some_and(|addr| !addr.ip().is_loopback())
            {
                return Err("Status role listener must be loopback".into());
            }
            layout.listener("status", role.listen)?;
            if let Some(addr) = role.query_listen {
                layout.listener("status", addr)?;
            }
            layout.root("status", &role.state_dir)?;
            layout.probe("status", role.listen, "/control/health");
        }
        #[cfg(feature = "transparent-history")]
        if let Some(worker) = active(&self.transparent_history)? {
            if worker.verify_only {
                return Err(ORDINARY_VERIFY.into());
            }
            layout.listener("transparent_history", worker.listen)?;
            layout.root("transparent_history", &worker.shard_dir)?;
            if let Some(path) = &worker.runtime_cache_dir {
                layout.root("transparent_history", path)?;
            }
            layout.probe("transparent_history", worker.listen, "/v1/ready");
        }
        #[cfg(feature = "transparent-detail")]
        if let Some(worker) = active(&self.transaction_details)? {
            if worker.verify_only {
                return Err(ORDINARY_VERIFY.into());
            }
            layout.listener("transaction_details", worker.listen)?;
            if let Some(path) = &worker.state_dir {
                layout.root("transaction_details", path)?;
            }
            if let Some(path) = &worker.publication_dir {
                layout.root("transaction_details", path)?;
            }
            layout.probe("transaction_details", worker.listen, "/v1/ready");
        }
        Ok(layout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(
        feature = "enhance",
        feature = "status",
        feature = "transparent-history",
        feature = "transparent-detail"
    ))]
    #[test]
    fn all_product_example_keeps_independent_roles() {
        let config: Config = serde_json::from_str(include_str!(
            "../../../tools/deploy/query-server/query-server.example.json"
        ))
        .unwrap();
        let layout = config.validate().unwrap();
        assert_eq!(layout.probes.len(), 4);
        assert_eq!(layout.listeners.len(), 5);
    }

    #[cfg(feature = "transparent-history")]
    #[test]
    fn disabled_products_need_no_settings_or_files() {
        let config: Config = serde_json::from_value(serde_json::json!({
            "management_listen": "127.0.0.1:18080",
            "transparent_history": {"enabled": false}
        }))
        .unwrap();
        let layout = config.validate().unwrap();
        assert_eq!(layout.listeners.len(), 1);
        assert!(layout.roots.is_empty());
    }

    #[test]
    fn overlapping_and_symlinked_state_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let mut layout = Layout::default();
        layout.root("one", temp.path()).unwrap();
        assert!(layout.root("two", &temp.path().join("child")).is_err());
        #[cfg(unix)]
        {
            let link = temp.path().join("alias");
            std::os::unix::fs::symlink(temp.path(), &link).unwrap();
            assert!(layout.root("two", &link).is_err());
        }
    }

    #[test]
    fn listener_conflicts_and_unknown_options_are_rejected() {
        let mut layout = Layout::default();
        let addr = "127.0.0.1:18080".parse().unwrap();
        layout.listener("one", addr).unwrap();
        assert!(layout.listener("two", addr).is_err());
        assert!(serde_json::from_str::<Config>(
            r#"{"management_listen":"127.0.0.1:18080","typo":true}"#
        )
        .is_err());
    }

    #[cfg(not(feature = "transparent-history"))]
    #[test]
    fn history_section_requires_the_compiled_product() {
        let raw =
            r#"{"management_listen":"127.0.0.1:18080","transparent_history":{"enabled":false}}"#;
        assert!(serde_json::from_str::<Config>(raw).is_err());
    }

    #[cfg(not(feature = "transparent-detail"))]
    #[test]
    fn detail_section_requires_the_compiled_product() {
        let raw =
            r#"{"management_listen":"127.0.0.1:18080","transaction_details":{"enabled":false}}"#;
        assert!(serde_json::from_str::<Config>(raw).is_err());
    }

    #[cfg(feature = "transparent-detail")]
    #[test]
    fn disabled_detail_needs_no_settings_and_does_no_work() {
        let config: Config = serde_json::from_value(serde_json::json!({
            "management_listen": "127.0.0.1:18080",
            "transaction_details": {"enabled": false}
        }))
        .unwrap();
        let layout = config.validate().unwrap();
        assert_eq!(layout.listeners.len(), 1);
        assert!(layout.roots.is_empty() && layout.probes.is_empty());
        assert!(config.verification().is_err());
    }

    #[cfg(any(feature = "transparent-history", feature = "transparent-detail"))]
    fn transparent(section: &str, verify_only: bool) -> Config {
        let temp = std::env::temp_dir().join("pir-query-server-verify-config");
        let settings = match section {
            "transparent_history" => serde_json::json!({
                "listen": "127.0.0.1:18092",
                "shard_dir": temp.join("history"),
                "verify_only": verify_only,
            }),
            _ => serde_json::json!({
                "listen": "127.0.0.1:18095",
                "role": "combined",
                "publication_dir": temp.join("detail"),
                "verify_only": verify_only,
            }),
        };
        serde_json::from_value(serde_json::json!({
            "management_listen": "127.0.0.1:18080",
            section: {"enabled": true, "settings": settings},
        }))
        .unwrap()
    }

    #[cfg(any(feature = "transparent-history", feature = "transparent-detail"))]
    #[test]
    fn verification_selects_one_section_and_ordinary_startup_rejects_it() {
        let sections = [
            #[cfg(feature = "transparent-history")]
            "transparent_history",
            #[cfg(feature = "transparent-detail")]
            "transaction_details",
        ];
        for section in sections {
            assert!(transparent(section, false).validate().is_ok(), "{section}");
            assert!(
                transparent(section, false).verification().is_ok(),
                "{section}"
            );
            let error = transparent(section, true).validate().err().unwrap();
            assert!(error.to_string().contains("--verify-only"), "{section}");
            assert!(
                transparent(section, true).verification().is_ok(),
                "{section}"
            );
        }
        let none: Config =
            serde_json::from_str(r#"{"management_listen":"127.0.0.1:18080"}"#).unwrap();
        assert!(none.verification().is_err());
    }

    #[cfg(all(feature = "transparent-history", feature = "transparent-detail"))]
    #[test]
    fn verification_refuses_two_transparent_sections() {
        let mut config = transparent("transparent_history", false);
        config.transaction_details = transparent("transaction_details", false).transaction_details;
        assert!(config.validate().is_ok());
        assert!(config.verification().is_err());
    }

    #[cfg(all(
        any(feature = "transparent-history", feature = "transparent-detail"),
        any(feature = "enhance", feature = "status")
    ))]
    #[test]
    fn verification_refuses_other_enabled_products() {
        let section = if cfg!(feature = "transparent-history") {
            "transparent_history"
        } else {
            "transaction_details"
        };
        let other = if cfg!(feature = "enhance") {
            "enhance"
        } else {
            "status"
        };
        // Settings are irrelevant: the enabled flag alone refuses verification.
        let config: Config = serde_json::from_value(serde_json::json!({
            "management_listen": "127.0.0.1:18080",
            other: {"enabled": true},
            section: {"enabled": false},
        }))
        .unwrap();
        let error = config.verification().err().unwrap();
        assert!(error.to_string().contains(other), "{error}");
    }
}
