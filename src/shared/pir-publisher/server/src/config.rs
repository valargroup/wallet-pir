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
            .ok_or_else(|| "enabled publisher has no settings".into())
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub management_listen: SocketAddr,
    pub enhance: Option<Product<enhance_publisher::service::Config>>,
    pub status: Option<Product<status_publisher::service::Config>>,
    pub transparent_history: Option<Product<transparent_publisher::history::controller::Config>>,
    pub transaction_details: Option<Product<transparent_publisher::tx_detail::service::Config>>,
    pub receiver: Option<Product<receiver_indexer::service::Config>>,
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
    pub kind: &'static str,
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
            return Err("publisher listeners need a fixed loopback or private address".into());
        }
        if self.listeners.keys().any(|other| {
            other == &addr
                || (other.port() == addr.port()
                    && (other.ip().is_unspecified() || addr.ip().is_unspecified()))
        }) {
            return Err(format!("conflicting publisher listeners at {addr}").into());
        }
        self.listeners.insert(addr, owner);
        Ok(())
    }
    fn root(&mut self, owner: &'static str, root: &Path) -> Result<()> {
        if !root.is_absolute() {
            return Err("publisher state paths must be absolute".into());
        }
        let path = normalize(root)?;
        if self.roots.iter().any(|(other, existing)| {
            *other != owner && (path.starts_with(existing) || existing.starts_with(&path))
        }) {
            return Err("enabled publishers have overlapping writable state paths".into());
        }
        self.roots.push((owner, path));
        Ok(())
    }
    fn probe(
        &mut self,
        name: &'static str,
        address: SocketAddr,
        path: &'static str,
        kind: &'static str,
    ) {
        self.probes.entry(name).or_default().push(Probe {
            address,
            path,
            kind,
        });
    }
}
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
                    // Resolve each existing prefix before interpreting a later `..`.
                    result = result.canonicalize()?;
                }
            }
        }
    }
    Ok(result)
}
impl Config {
    pub fn validate(&self) -> Result<Layout> {
        let mut layout = Layout::default();
        if !self.management_listen.ip().is_loopback() {
            return Err("management listener must be loopback".into());
        }
        layout.listener("management", self.management_listen)?;
        if let Some(c) = active(&self.enhance)? {
            c.validate()?;
            if c.status_config.is_some() {
                return Err("configure Status in its own product section".into());
            }
            layout.listener("enhance", c.listen)?;
            if let Some(addr) = c.artifact_listen {
                layout.listener("enhance", addr)?;
            }
            layout.root("enhance", &c.data_dir)?;
            layout.probe("enhance", c.listen, "/ready", "status");
        }
        if let Some(c) = active(&self.status)? {
            c.validate()?;
            layout.listener("status", c.publisher.private_listen)?;
            if let Some(addr) = c.public_listen {
                layout.listener("status", addr)?;
            }
            layout.root("status", &c.publisher.state_dir)?;
            layout.probe(
                "status",
                c.publisher.private_listen,
                "/v1/status/init",
                "status",
            );
        }
        if let Some(c) = active(&self.transparent_history)? {
            if c.distribute_prepared && !c.ship_runtimes {
                return Err("prepared distribution requires ship_runtimes".into());
            }
            layout.listener("transparent_history", c.listen)?;
            layout.root("transparent_history", &c.publication_root)?;
            if !c.follow_journal {
                layout.root("transparent_history", &c.data_dir)?;
            }
            if let Some(path) = &c.snapshot_dir {
                layout.root("transparent_history", path)?;
            }
            layout.probe("transparent_history", c.listen, "/v1/status", "history");
            layout.probe("transparent_history", c.listen, "/v2/shards/init", "status");
        }
        if let Some(c) = active(&self.transaction_details)? {
            c.validate()?;
            layout.listener("transaction_details", c.status_listen)?;
            layout.root("transaction_details", &c.root)?;
            layout.root("transaction_details", &c.journal)?;
            layout.probe(
                "transaction_details",
                c.status_listen,
                "/v1/status",
                "display",
            );
        }
        if let Some(c) = active(&self.receiver)? {
            c.validate()?;
            if !c.serve {
                return Err("unified Receiver publishing requires continuous serving".into());
            }
            layout.listener("receiver", c.bind)?;
            layout.root("receiver", &c.data_dir)?;
            layout.probe("receiver", c.bind, "/v1/receiver/init", "status");
        }
        Ok(layout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn all_product_example_keeps_independent_modes() {
        let config: Config = serde_json::from_str(include_str!(
            "../../../../../tools/deploy/publisher/publisher.example.json"
        ))
        .unwrap();
        let layout = config.validate().unwrap();
        assert_eq!(layout.probes.len(), 5);
        assert_eq!(layout.listeners.len(), 8);
    }
    #[test]
    fn disabled_products_need_no_settings_or_files() {
        let config: Config = serde_json::from_value(serde_json::json!({"management_listen":"127.0.0.1:18080", "receiver":{"enabled":false}})).unwrap();
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
            let child = temp.path().join("real/child");
            std::fs::create_dir_all(&child).unwrap();
            let alias = temp.path().join("child-alias");
            std::os::unix::fs::symlink(&child, &alias).unwrap();
            let mut layout = Layout::default();
            layout.root("one", &temp.path().join("real/state")).unwrap();
            assert!(layout.root("two", &alias.join("../state")).is_err());
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
}
