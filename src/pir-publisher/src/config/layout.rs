//! Static resource declarations, never bound sockets or opened product stores.
#[cfg(any(
    feature = "enhance",
    feature = "status",
    feature = "transparent-history",
    feature = "transparent-detail",
    feature = "receiver"
))]
use crate::publisher::ReadinessRule;
use crate::{
    publisher::{PublisherProduct, ReadinessProbe},
    Result,
};
use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};

#[derive(Default)]
pub(crate) struct PublisherLayout {
    pub(crate) listeners: BTreeMap<SocketAddr, &'static str>,
    pub(super) writable_paths: Vec<(PublisherProduct, PathBuf)>,
    pub(crate) probes: BTreeMap<PublisherProduct, Vec<ReadinessProbe>>,
}

fn is_private_listener(address: SocketAddr) -> bool {
    match address.ip() {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
    }
}

impl PublisherLayout {
    /// Declare a fixed private listener, rejecting reuse even by the same owner.
    pub(super) fn declare_listener(&mut self, owner: &'static str, addr: SocketAddr) -> Result<()> {
        if !is_private_listener(addr) || addr.port() == 0 {
            return Err(format!(
                "{owner} listener {addr} needs a fixed loopback or private address"
            )
            .into());
        }
        if self.listeners.keys().any(|other| {
            other == &addr
                || (other.port() == addr.port()
                    && (other.ip().is_unspecified() || addr.ip().is_unspecified()))
        }) {
            return Err(
                format!("{owner} listener conflicts with another publisher at {addr}").into(),
            );
        }
        self.listeners.insert(addr, owner);
        Ok(())
    }
    /// Resolve existing path prefixes before checking cross-product overlap.
    /// One product may declare nested paths; different products may not share
    /// writable state, including through symlinks or a later parent component.
    #[cfg_attr(
        not(any(
            feature = "enhance",
            feature = "status",
            feature = "transparent-history",
            feature = "transparent-detail",
            feature = "receiver"
        )),
        allow(dead_code)
    )]
    pub(super) fn declare_writable_path(
        &mut self,
        owner: PublisherProduct,
        root: &Path,
    ) -> Result<()> {
        if !root.is_absolute() {
            return Err(format!(
                "{} writable path must be absolute: {}",
                owner.name(),
                root.display()
            )
            .into());
        }
        let path = normalize_writable_path(root).map_err(|source| PathValidationError {
            product: owner,
            path: root.to_owned(),
            source,
        })?;
        if let Some((other, existing)) = self.writable_paths.iter().find(|(other, existing)| {
            *other != owner && (path.starts_with(existing) || existing.starts_with(&path))
        }) {
            return Err(format!(
                "{} writable path {} overlaps {} writable path {}",
                owner.name(),
                path.display(),
                other.name(),
                existing.display()
            )
            .into());
        }
        self.writable_paths.push((owner, path));
        Ok(())
    }
    #[cfg(any(
        feature = "enhance",
        feature = "status",
        feature = "transparent-history",
        feature = "transparent-detail",
        feature = "receiver"
    ))]
    pub(super) fn declare_readiness_probe(
        &mut self,
        product: PublisherProduct,
        address: SocketAddr,
        path: &'static str,
        rule: ReadinessRule,
    ) {
        self.probes
            .entry(product)
            .or_default()
            .push(ReadinessProbe {
                address,
                path,
                rule,
            });
    }
}
#[cfg_attr(
    not(any(
        feature = "enhance",
        feature = "status",
        feature = "transparent-history",
        feature = "transparent-detail",
        feature = "receiver"
    )),
    allow(dead_code)
)]
fn normalize_writable_path(path: &Path) -> std::io::Result<PathBuf> {
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

#[derive(Debug)]
struct PathValidationError {
    product: PublisherProduct,
    path: PathBuf,
    source: std::io::Error,
}
impl std::fmt::Display for PathValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "cannot resolve {} writable path {}: {}",
            self.product.name(),
            self.path.display(),
            self.source
        )
    }
}
impl std::error::Error for PathValidationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
