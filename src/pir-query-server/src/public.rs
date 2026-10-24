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

// Products and the routing tests name their namespaces with this.
#[cfg_attr(
    not(any(
        test,
        feature = "enhance",
        feature = "status",
        feature = "transparent-history",
        feature = "transparent-detail"
    )),
    allow(dead_code)
)]
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

/// Whether a mount path names `path`. A trailing `/*operation` matches every
/// non-empty path below its prefix; any other mount path matches exactly.
fn names(mount: &str, path: &str) -> bool {
    match mount.strip_suffix("/*operation") {
        Some(prefix) => path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.len() > 1 && rest.starts_with('/')),
        None => path == mount,
    }
}

/// The shared wallet-facing router. Mounts must name disjoint paths; a
/// duplicate is refused instead of shadowing another product.
///
/// Dispatch matches paths itself rather than through routes, so a product
/// router receives the wallet's request unchanged: no outer path parameters
/// or matched-path label precede its own.
pub fn router(mounts: &[Mount]) -> crate::Result<Router> {
    let mut seen = std::collections::BTreeSet::new();
    let mut table = Vec::new();
    for mount in mounts {
        for path in &mount.paths {
            if !seen.insert(path.clone()) {
                return Err(
                    format!("{} wallet path {path} is already served", mount.product).into(),
                );
            }
            table.push((path.clone(), mount.slot.clone()));
        }
    }
    let table = Arc::new(table);
    Ok(Router::new().fallback(move |request: Request| {
        let slot = table
            .iter()
            .find(|(path, _)| names(path, request.uri().path()))
            .map(|(_, slot)| slot.clone());
        async move {
            match slot {
                Some(slot) => slot.forward(request).await,
                None => StatusCode::NOT_FOUND.into_response(),
            }
        }
    }))
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

    /// Product routers extract their own path parameters: the shared
    /// listener forwards the request without parameters of its own.
    #[tokio::test]
    async fn product_path_parameters_reach_the_product_unchanged() {
        use axum::extract::Path;
        let mount = Mount {
            product: "first",
            paths: under(["/first/v1", "/v2"], &["/shards", "/shards/*operation"]),
            slot: Slot::default(),
        };
        let slot = mount.slot.clone();
        let shared = router(&[mount]).unwrap();
        slot.install(
            Router::new()
                .route("/v2/shards", get(|| async { "map" }))
                .route(
                    "/v2/shards/:id/revisions/:digest",
                    get(|Path((id, digest)): Path<(u64, String)>| async move {
                        format!("{id}:{digest}")
                    }),
                ),
        );
        let response = shared
            .clone()
            .oneshot(
                Request::get("/v2/shards/7/revisions/ab")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 64)
            .await
            .unwrap();
        assert_eq!(&body[..], b"7:ab");
        assert_eq!(status_of(&shared, "/v2/shards").await, StatusCode::OK);
        assert_eq!(
            status_of(&shared, "/v2/shards/").await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_of(&shared, "/v2/shardsx").await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_of(&shared, "/first/v1/shards/unknown").await,
            StatusCode::NOT_FOUND
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
