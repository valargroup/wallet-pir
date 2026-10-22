//! Wallet-facing route namespaces: one canonical prefix per product
//! operation group, plus the prefix that deployed clients already use.
//!
//! One selected server base serves every product under its own namespace
//! (`/enhance/v1`, `/status/v1`, `/transparent/v1`, `/receiver/v1`). The
//! older per-version prefixes (`/v1/enhance`, `/v1/status`, `/v2`, ...) stay as
//! explicit aliases of the same handlers. A product declares each public
//! operation once, as a suffix such as `/init`, and [`PublicMount::mount`]
//! registers that one handler at both prefixes. Nothing else is registered:
//! control, worker and metrics routes are never mounted here, so they cannot
//! appear under a canonical public prefix.

use axum::{routing::MethodRouter, Router};

/// Where one group of public operations is reachable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicMount {
    /// Product namespace and contract version, for example `/enhance/v1`.
    pub canonical: &'static str,
    /// The deployed prefix of the same operations, for example `/v1/enhance`.
    pub legacy: &'static str,
}

impl PublicMount {
    /// Both prefixes must start with `/` and must not end with one; operation
    /// suffixes supply the separating `/`.
    pub const fn new(canonical: &'static str, legacy: &'static str) -> Self {
        Self { canonical, legacy }
    }

    /// The prefixes an operation is registered under, canonical first.
    pub const fn prefixes(&self) -> [&'static str; 2] {
        [self.canonical, self.legacy]
    }

    /// Registers each `(suffix, handler)` operation at `canonical + suffix`
    /// and `legacy + suffix`, sharing one handler. Every suffix starts with
    /// `/`. Axum panics on construction if any resulting path is already
    /// routed, so two mounts that share a prefix cannot silently shadow each
    /// other's operations.
    pub fn mount<S>(
        self,
        mut router: Router<S>,
        operations: impl IntoIterator<Item = (&'static str, MethodRouter<S>)>,
    ) -> Router<S>
    where
        S: Clone + Send + Sync + 'static,
    {
        for (suffix, handler) in operations {
            debug_assert!(suffix.starts_with('/'), "operation suffix {suffix}");
            for prefix in self.prefixes() {
                router = router.route(&format!("{prefix}{suffix}"), handler.clone());
            }
        }
        router
    }

    /// The operation suffix of `path` (starting with `/`) when it lies under
    /// either prefix, so route classification sees one name per operation.
    pub fn operation<'a>(&self, path: &'a str) -> Option<&'a str> {
        self.prefixes().into_iter().find_map(|prefix| {
            path.strip_prefix(prefix)
                .filter(|suffix| suffix.starts_with('/'))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;

    const MOUNT: PublicMount = PublicMount::new("/product/v1", "/v1/product");

    #[test]
    fn operation_is_the_suffix_under_either_prefix() {
        assert_eq!(MOUNT.operation("/product/v1/init"), Some("/init"));
        assert_eq!(
            MOUNT.operation("/v1/product/session/ab"),
            Some("/session/ab")
        );
        assert_eq!(MOUNT.operation("/product/v1"), None);
        assert_eq!(MOUNT.operation("/product/v1x/init"), None);
        assert_eq!(MOUNT.operation("/internal/health"), None);
    }

    #[test]
    #[should_panic]
    fn overlapping_mounts_refuse_a_shadowed_operation() {
        let other = PublicMount::new("/product/v1", "/v2");
        let router: Router = MOUNT.mount(Router::new(), [("/init", get(|| async {}))]);
        let _ = other.mount(router, [("/init", get(|| async {}))]);
    }
}
