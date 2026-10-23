//! The one wallet-facing listener (`public_listen`) shared by every enabled
//! product.
//!
//! Each product owns explicit, disjoint wallet namespaces under its existing
//! canonical and deployed prefixes. Only those namespaces are routed here;
//! everything else, including product health, readiness, metrics, control,
//! provider and evaluation routes, answers 404 on this listener and stays on
//! the product's private listeners. A product installs its wallet router into
//! its [`Slot`] once it can serve; until then, and after the host withdraws a
//! stopped product, its namespaces answer 503 while other products continue.
use axum::{
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use std::sync::{Arc, RwLock};
use tower::ServiceExt;

/// Where one product's wallet router is installed on the shared listener.
#[derive(Clone, Default)]
pub struct Slot(Arc<RwLock<Option<Router>>>);

impl Slot {
    /// Serve `router` for this product's namespaces. Requests already
    /// dispatched keep the router they started with.
    pub fn install(&self, router: Router) {
        *self.0.write().unwrap() = Some(router);
    }

    /// Stop dispatching new requests to this product.
    pub fn withdraw(&self) {
        *self.0.write().unwrap() = None;
    }

    async fn forward(self, request: Request) -> Response {
        let router = self.0.read().unwrap().clone();
        match router {
            Some(router) => match router.oneshot(request).await {
                Ok(response) => response,
                Err(never) => match never {},
            },
            None => (
                StatusCode::SERVICE_UNAVAILABLE,
                "query service is not serving this product\n",
            )
                .into_response(),
        }
    }
}

/// One product's wallet namespaces and the slot that serves them.
pub struct Mount {
    pub product: &'static str,
    pub paths: Vec<String>,
    pub slot: Slot,
}

fn under(prefixes: [&'static str; 2], suffixes: &[&str]) -> Vec<String> {
    prefixes
        .into_iter()
        .flat_map(|prefix| {
            suffixes
                .iter()
                .map(move |suffix| format!("{prefix}{suffix}"))
        })
        .collect()
}

/// Enhance owns its whole canonical and deployed namespaces.
#[cfg(feature = "enhance")]
pub fn enhance() -> Mount {
    Mount {
        product: "enhance",
        paths: under(crate::enhance::PUBLIC_ROUTES.prefixes(), &["/*operation"]),
        slot: Slot::default(),
    }
}

/// Status owns its whole canonical and deployed namespaces.
#[cfg(feature = "status")]
pub fn status() -> Mount {
    Mount {
        product: "status",
        paths: under(crate::status::PUBLIC_ROUTES.prefixes(), &["/*operation"]),
        slot: Slot::default(),
    }
}

/// History shares the Transparent prefixes with transaction details and owns
/// the shard map, its revisions and the range filters below them.
#[cfg(feature = "transparent-history")]
pub fn transparent_history() -> Mount {
    Mount {
        product: "transparent_history",
        paths: under(
            crate::transparent::PUBLIC_ROUTES.prefixes(),
            &["/shards", "/shards/*operation", "/filters/*operation"],
        ),
        slot: Slot::default(),
    }
}

/// Transaction details own `/txid` below the shared Transparent prefixes.
#[cfg(feature = "transparent-detail")]
pub fn transaction_details() -> Mount {
    Mount {
        product: "transaction_details",
        paths: under(
            crate::transparent::PUBLIC_ROUTES.prefixes(),
            &["/txid/*operation"],
        ),
        slot: Slot::default(),
    }
}

/// The shared wallet-facing router. Mounts must name disjoint paths; a
/// duplicate is refused instead of shadowing another product.
pub fn router(mounts: &[Mount]) -> crate::Result<Router> {
    let mut seen = std::collections::BTreeSet::new();
    let mut router = Router::new();
    for mount in mounts {
        for path in &mount.paths {
            if !seen.insert(path.clone()) {
                return Err(
                    format!("{} wallet path {path} is already served", mount.product).into(),
                );
            }
            let slot = mount.slot.clone();
            router = router.route(path, any(move |request| slot.clone().forward(request)));
        }
    }
    Ok(router)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, routing::get};

    async fn status_of(router: &Router, path: &str) -> StatusCode {
        router
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    fn product(paths: &[&'static str]) -> Router {
        paths.iter().fold(Router::new(), |router, path| {
            router.route(path, get(|| async { "ok" }))
        })
    }

    #[tokio::test]
    async fn only_installed_wallet_namespaces_are_served() {
        let first = Mount {
            product: "first",
            paths: under(["/first/v1", "/v1/first"], &["/*operation"]),
            slot: Slot::default(),
        };
        let second = Mount {
            product: "second",
            paths: under(["/shared/v1", "/v2"], &["/txid/*operation"]),
            slot: Slot::default(),
        };
        let shared = router(&[first, second]).unwrap();
        // Nothing is installed yet: wallet paths refuse, others are absent.
        assert_eq!(
            status_of(&shared, "/first/v1/init").await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status_of(&shared, "/v1/health").await,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn private_routes_of_an_installed_product_stay_private() {
        let mount = Mount {
            product: "first",
            paths: under(["/first/v1", "/v1/first"], &["/*operation"]),
            slot: Slot::default(),
        };
        let slot = mount.slot.clone();
        let shared = router(&[mount]).unwrap();
        slot.install(product(&[
            "/first/v1/init",
            "/v1/first/init",
            "/internal/health",
            "/metrics",
        ]));
        assert_eq!(status_of(&shared, "/first/v1/init").await, StatusCode::OK);
        assert_eq!(status_of(&shared, "/v1/first/init").await, StatusCode::OK);
        assert_eq!(
            status_of(&shared, "/first/v1/unknown").await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_of(&shared, "/internal/health").await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(status_of(&shared, "/metrics").await, StatusCode::NOT_FOUND);
        slot.withdraw();
        assert_eq!(
            status_of(&shared, "/first/v1/init").await,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn overlapping_mounts_are_refused() {
        let mount = || Mount {
            product: "first",
            paths: under(["/first/v1", "/v1/first"], &["/*operation"]),
            slot: Slot::default(),
        };
        assert!(router(&[mount(), mount()]).is_err());
    }

    #[cfg(all(
        feature = "enhance",
        feature = "status",
        feature = "transparent-history",
        feature = "transparent-detail"
    ))]
    #[tokio::test]
    async fn every_product_composes_on_one_listener_with_aliases() {
        let mounts = [
            enhance(),
            status(),
            transparent_history(),
            transaction_details(),
        ];
        let slots: Vec<_> = mounts.iter().map(|m| m.slot.clone()).collect();
        let shared = router(&mounts).unwrap();
        for (slot, paths) in slots.iter().zip([
            &["/enhance/v1/init", "/v1/enhance/init"][..],
            &["/status/v1/init", "/v1/status/init"],
            &[
                "/transparent/v1/shards",
                "/v2/shards",
                "/v2/shards/init",
                "/v2/filters/shards",
            ],
            &["/transparent/v1/txid/init", "/v2/txid/init"],
        ]) {
            slot.install(product(paths));
            for path in paths {
                assert_eq!(status_of(&shared, path).await, StatusCode::OK, "{path}");
            }
        }
        for private in [
            "/internal/health",
            "/internal/metrics",
            "/control/health",
            "/evaluate",
            "/v1/provider-status",
            "/v1/health",
            "/v1/ready",
            "/metrics",
        ] {
            assert_eq!(
                status_of(&shared, private).await,
                StatusCode::NOT_FOUND,
                "{private}"
            );
        }
    }
}
