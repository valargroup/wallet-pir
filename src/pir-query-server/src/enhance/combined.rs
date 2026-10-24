//! One Enhance process serving wallets end to end. Its query ingress, packing
//! router and worker each keep their own state directory, publisher
//! authority and synchronization, exactly as separate processes would, and
//! call each other directly instead of over private HTTP.
//!
//! Wallets see a generation only after the ingress activates it, and the
//! ingress activates only what the router promised for exact loaded material,
//! which the router promised only after the worker did. The same readiness
//! promises, incarnation bindings and no-replay rules as the distributed roles
//! therefore make one coherent serving view.
use crate::enhance::{
    ingress_sync::QueryIngress, router_sync::PackingRouter, synchronization::Error,
    worker_sync::Worker,
};
use axum::{response::IntoResponse, routing::get, Json, Router};
use enhance_shared::{matvec::MatvecConfig, placement::PlacementPolicy};
use std::path::Path;

#[derive(Clone)]
pub struct Combined {
    pub worker: Worker,
    pub router: PackingRouter,
    pub ingress: QueryIngress,
}

/// Each component's synchronization pass, run by its own manager.
pub struct Synchronizers {
    pub worker: crate::enhance::worker_sync::Synchronizer,
    pub router: crate::enhance::router_sync::Synchronizer,
    pub ingress: crate::enhance::ingress_sync::Synchronizer,
}

/// Limits for one combined process, each the existing limit of its role.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub sealed_shards: usize,
    pub requests: usize,
    pub uploads: usize,
    pub max_objects: usize,
    pub packing_requests: usize,
}

impl Combined {
    /// Open the three components under `root/worker`, `root/router` and
    /// `root/ingress`; each holds its own lock, replay floors and cache.
    pub fn open(
        root: &Path,
        config: crate::enhance::worker_sync::Config,
        limits: Limits,
        backend: MatvecConfig,
    ) -> Result<Self, Error> {
        let source = config.source.clone();
        let shards = config.shards.clone();
        let worker = Worker::open(
            &root.join("worker"),
            config,
            PlacementPolicy {
                sealed_shards: limits.sealed_shards,
            },
            backend,
        )?;
        let router = PackingRouter::open_local(
            &root.join("router"),
            source.clone(),
            &worker,
            limits.max_objects,
            limits.packing_requests,
        )?;
        let ingress = QueryIngress::open_local(
            &root.join("ingress"),
            source,
            &router,
            &shards,
            limits.requests,
            limits.uploads,
        )?;
        Ok(Self {
            worker,
            router,
            ingress,
        })
    }

    #[cfg(test)]
    fn ingress_source(&self) -> crate::enhance::synchronization::Config {
        crate::enhance::synchronization::Config {
            publisher_origin: "https://publisher.example".into(),
            trusted_ca: None,
            download_budget_bytes: 16 * 1024 * 1024,
            cache_limit_bytes: 16 * 1024 * 1024,
        }
    }

    pub fn synchronizers(&self) -> Result<Synchronizers, Error> {
        Ok(Synchronizers {
            worker: self.worker.synchronizer()?,
            router: self.router.synchronizer()?,
            ingress: self.ingress.synchronizer()?,
        })
    }

    /// The wallet-facing routes: the ingress's, unchanged.
    pub fn public_router(&self) -> Router {
        self.ingress.public_router()
    }

    /// Serving eligibility: every component holds fresh authority for its
    /// active material. The ingress alone could still report an old promise.
    /// Waits until every component's admitted work has ended.
    pub(crate) async fn drained(&self) {
        self.ingress.drained().await;
        self.router.drained().await;
        self.worker.drained().await;
    }

    pub fn ready(&self) -> bool {
        self.ingress.ready() && self.router.ready() && self.worker.ready()
    }

    /// Private health and metrics. Health carries the shared bearer token
    /// like the ingress's; process memory is reported once for the process.
    pub fn control_router(&self) -> Router {
        Router::new()
            .route("/internal/health", get(health))
            .layer(crate::enhance::internal_auth::Token::from_env().layer())
            .route("/internal/metrics", get(metrics))
            .with_state(self.clone())
    }
}

async fn health(
    axum::extract::State(combined): axum::extract::State<Combined>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "role": "combined",
        "binary_sha256": pir_control::Identity::process().binary_sha256,
        "ready": combined.ready(),
        "ingress": combined.ingress.health(),
        "packing": combined.router.health(),
        "worker": combined.worker.health(),
    }))
}

async fn metrics(
    axum::extract::State(combined): axum::extract::State<Combined>,
) -> impl IntoResponse {
    (
        [("content-type", "text/plain; version=0.0.4")],
        format!(
            "{}{}{}{}",
            combined.ingress.metrics_text(),
            combined.router.metrics_text(false),
            combined.worker.metrics_text(false),
            crate::enhance::memory_observation::observe().render("enhance_combined_"),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::StatusCode};
    use tower::ServiceExt;

    fn combined(root: &Path) -> Combined {
        Combined::open(
            root,
            crate::enhance::worker_sync::Config {
                source: crate::enhance::synchronization::Config {
                    publisher_origin: "https://publisher.example".into(),
                    trusted_ca: None,
                    download_budget_bytes: 16 * 1024 * 1024,
                    cache_limit_bytes: 16 * 1024 * 1024,
                },
                shards: [0].into(),
            },
            Limits {
                sealed_shards: 6,
                requests: 1,
                uploads: 4,
                max_objects: 6,
                packing_requests: 4,
            },
            Default::default(),
        )
        .unwrap()
    }

    async fn call(router: Router, method: &str, path: &str) -> (StatusCode, serde_json::Value) {
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap_or_default())
    }

    /// Before any component holds authorized material the process is not
    /// eligible, wallets get the ordinary temporary refusal, and a second
    /// process cannot open the same state.
    #[tokio::test]
    async fn unsynchronized_combined_role_is_not_ready_and_refuses_wallets() {
        let root = tempfile::tempdir().unwrap();
        let role = combined(root.path());
        assert!(!role.ready());
        let (status, health) = call(role.control_router(), "GET", "/internal/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(health["role"], "combined");
        assert_eq!(health["ready"], false);
        for prefix in ["/enhance/v1", "/v1/enhance"] {
            let (status, body) = call(role.public_router(), "GET", &format!("{prefix}/init")).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(body["code"], "temporarily_unavailable");
        }
        for path in [
            "/internal/health",
            "/internal/metrics",
            "/internal/readiness",
            "/internal/evaluate",
            "/v1/enhance/internal/health",
        ] {
            let (status, _) = call(role.public_router(), "GET", path).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path} stays private");
        }
        assert!(Combined::open(
            root.path(),
            crate::enhance::worker_sync::Config {
                source: role.ingress_source(),
                shards: [0].into(),
            },
            Limits {
                sealed_shards: 6,
                requests: 1,
                uploads: 4,
                max_objects: 6,
                packing_requests: 4,
            },
            Default::default(),
        )
        .is_err());
    }

    /// A direct evaluation the worker refuses before accepting it is typed as
    /// nonacceptance, so only then may another replica be tried.
    #[tokio::test]
    async fn local_refusals_before_admission_are_typed_as_not_accepted() {
        let root = tempfile::tempdir().unwrap();
        let role = combined(root.path());
        let request = crate::enhance::evaluation::Evaluate {
            binding: Some(vec![0; enhance_shared::protocol::HEADER_BYTES]),
            generation: 1,
            shard_id: 0,
            epoch: "00".repeat(8),
            session_id: "00".repeat(32),
            coefficients: vec![1],
        };
        for incarnation in [role.worker.incarnation.clone(), "ee".repeat(16)] {
            match role
                .worker
                .evaluate_local(&incarnation, request.clone())
                .await
            {
                Err(crate::enhance::worker_sync::EvaluationFailure::NotAccepted((status, _))) => {
                    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE)
                }
                _ => panic!("refusal before admission must be typed as not accepted"),
            }
        }
    }
}
