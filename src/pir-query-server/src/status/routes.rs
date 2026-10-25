//! Role-specific HTTP routes, authority checks and router-worker handover.
use super::peers::{Binding, Health, ProviderStatus, Role};
use super::{
    serving::{evaluate, query, shared_bytes, Evaluator, HttpState},
    telemetry, Service,
};
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use status_shared::{
    now_ms,
    protocol::{Hash, Manifest},
    telemetry as counters,
};

impl Service {
    pub fn control_routes(&self) -> Router {
        Router::new()
            .route("/control/health", get(health))
            .route("/control/identity", get(identity))
            .route("/v1/provider-status", get(provider_status))
            .route("/v1/provider-ready/:id", get(provider_ready))
            .route("/v1/provider-pin/:id", post(provider_pin))
            .route("/v1/provider-commit/:id", post(provider_commit))
            .route("/evaluate", post(worker_query))
            .layer(axum::middleware::from_fn(|req, next| {
                telemetry::observe("worker", req, next)
            }))
            .layer(DefaultBodyLimit::max(512 * 1024))
            .with_state(self.clone())
            .merge(telemetry::routes())
    }
    pub fn query_routes(&self) -> Router {
        crate::status::PUBLIC_ROUTES
            .mount(
                Router::new(),
                [
                    ("/init", get(router_init)),
                    ("/session/:id", get(router_session)),
                    ("/query", post(router_query)),
                ],
            )
            .layer(axum::middleware::from_fn(|req, next| {
                telemetry::observe("router", req, next)
            }))
            .layer(DefaultBodyLimit::max(512 * 1024))
            .with_state(self.clone())
    }
    pub fn routes(&self) -> Router {
        self.control_routes().merge(self.query_routes())
    }
}

async fn identity(State(service): State<Service>) -> Json<serde_json::Value> {
    Json(
        serde_json::json!({"binary_sha256":pir_control::Identity::process().binary_sha256,"incarnation":service.incarnation}),
    )
}
/// Whether this role serves an unpoisoned generation its controller still
/// authorizes.
fn ready(state: &super::state::ServiceState) -> bool {
    !state.poisoned
        && state
            .controller
            .as_ref()
            .is_some_and(|controller| controller.check(&controller.current()).is_ok())
}
impl Service {
    pub(crate) async fn ready(&self) -> bool {
        ready(&*self.state.lock().await)
    }
}
async fn health(State(service): State<Service>) -> Json<Health> {
    let state = service.state.lock().await;
    let manifest = state
        .controller
        .as_ref()
        .map(|controller| controller.current().manifest().clone());
    let ready = ready(&state);
    Json(Health {
        role: service.role,
        binding: Binding {
            epoch: manifest.as_ref().map_or(0, |m| m.recovery_epoch),
            incarnation: service.incarnation.clone(),
        },
        active: manifest.as_ref().map(Manifest::id),
        cached_rows: manifest.as_ref().map(|m| m.rows_digest),
        ready,
    })
}
async fn provider_status(State(service): State<Service>) -> Json<ProviderStatus> {
    let state = service.state.lock().await;
    Json(ProviderStatus {
        inventory: state.inventory.clone(),
        manifest: state
            .controller
            .as_ref()
            .map(|controller| controller.current().manifest().clone()),
        synchronization: state.synchronization.clone(),
    })
}
async fn provider_ready(
    State(service): State<Service>,
    Path(id): Path<String>,
) -> Result<Json<bool>, StatusCode> {
    let id: Hash = hex::decode(id)
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .try_into()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let controller = service.available_controller().await?;
    let generation = controller.resolve(id)?;
    controller.check(&generation)?;
    Ok(Json(true))
}
/// The single colocated router keeps its acknowledged serving view and its
/// current incoming view. A new serial preparation supersedes an abandoned
/// incoming hold, without releasing the acknowledged view. These references
/// preserve the existing retention bound and never renew observation age.
pub(super) async fn provider_pin(
    State(service): State<Service>,
    Path(id): Path<String>,
) -> Result<Json<bool>, StatusCode> {
    if service.role != Role::Worker {
        return Err(StatusCode::NOT_FOUND);
    }
    let id: Hash = hex::decode(id)
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .try_into()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut state = service.state.lock().await;
    if state.poisoned {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let controller = state
        .controller
        .clone()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let generation = controller.resolve(id)?;
    controller.check(&generation)?;
    state
        .router_holds
        .retain(|generation| generation.manifest().fresh(now_ms()).is_ok());
    match state
        .router_holds
        .reserve(id, generation, |held| held.manifest().generation)
    {
        pir_pipeline::consumer::PeerReservation::Reserved => Ok(Json(true)),
        pir_pipeline::consumer::PeerReservation::PeerAdvanced => Err(StatusCode::CONFLICT),
    }
}
pub(super) async fn provider_commit(
    State(service): State<Service>,
    Path(id): Path<String>,
) -> Result<Json<bool>, StatusCode> {
    if service.role != Role::Worker {
        return Err(StatusCode::NOT_FOUND);
    }
    let id: Hash = hex::decode(id)
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .try_into()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut state = service.state.lock().await;
    if state.poisoned {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let controller = state
        .controller
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let generation = controller.resolve(id)?;
    controller.check(&generation)?;
    if state.router_holds.acknowledge(&id) {
        Ok(Json(true))
    } else {
        Err(StatusCode::CONFLICT)
    }
}

async fn worker_query(State(service): State<Service>, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    if service.role != Role::Worker {
        return Err(StatusCode::NOT_FOUND);
    }
    let controller = service.available_controller().await?;
    let generation = controller.for_body(&body)?;
    controller.check(&generation)?;
    let http_state = HttpState {
        controller: controller.clone(),
        evaluator: Evaluator::Local,
        permits: service.permits.clone(),
        #[cfg(test)]
        blocking_started: None,
    };
    let result = evaluate(State(http_state), body).await?;
    service.available_controller().await?;
    controller.check(&generation)?;
    Ok(result)
}
async fn router_init(State(service): State<Service>) -> Result<Json<Manifest>, StatusCode> {
    if !service.role.answers_wallets() {
        return Err(StatusCode::NOT_FOUND);
    }
    let controller = service.serving().await?;
    let generation = controller.current();
    controller.check(&generation)?;
    Ok(Json(generation.manifest().clone()))
}
async fn router_session(
    State(service): State<Service>,
    Path(id): Path<String>,
) -> Result<Bytes, StatusCode> {
    if !service.role.answers_wallets() {
        return Err(StatusCode::NOT_FOUND);
    }
    let id: Hash = hex::decode(id)
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .try_into()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let controller = service.available_controller().await?;
    let generation = controller.resolve(id)?;
    controller.check(&generation)?;
    Ok(shared_bytes(generation.public().clone()))
}
async fn router_query(
    State(service): State<Service>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    if !service.role.answers_wallets() {
        return Err(StatusCode::NOT_FOUND);
    }
    let controller = service.available_controller().await?;
    let generation = controller.for_body(&body)?;
    controller.check(&generation)?;
    // The ingress proxy sets the forwarding headers; the loopback peer address
    // carries no client identity.
    let key = pir_control::admission::client_key(&headers, None);
    let Some(slot) = service.clients.try_acquire(&key) else {
        counters::admission_rejected("router", "client_concurrency");
        return Err(StatusCode::TOO_MANY_REQUESTS);
    };
    let evaluator = if service.role == Role::Combined {
        Evaluator::Local
    } else {
        Evaluator::Peer {
            origin: service.worker_origin.clone(),
            http: service.serving_http.clone(),
        }
    };
    let http_state = HttpState {
        controller: controller.clone(),
        evaluator,
        permits: service.permits.clone(),
        #[cfg(test)]
        blocking_started: None,
    };
    let result = query(State(http_state), body, Some(slot)).await?;
    service.available_controller().await?;
    controller.check(&generation)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::Controller;
    use axum::{body::Body, http::Request};
    use status_shared::{protocol::PROTOCOL, Generation};
    use std::sync::Arc;
    use tower::ServiceExt;

    /// A router serving one fresh public-only generation.
    async fn router_service() -> (tempfile::TempDir, Service, Manifest) {
        let root = tempfile::tempdir().unwrap();
        let service = Service::open(
            Role::Router,
            [1; 32],
            root.path().join("router"),
            "http://127.0.0.1:1".into(),
        )
        .unwrap();
        let manifest = Manifest {
            protocol: PROTOCOL.into(),
            network: [1; 32],
            salt: [2; 32],
            generation: 3,
            recovery_epoch: 2,
            coverage_start: 1,
            anchor_height: 1,
            anchor_hash: [3; 32],
            observed_ms: now_ms(),
            entries: 0,
            rows_digest: [4; 32],
            public_digest: [5; 32],
        };
        service.state.lock().await.controller = Some(Controller::new(Generation::public_only(
            manifest.clone(),
            Arc::new(b"public material".to_vec()),
        )));
        (root, service, manifest)
    }

    async fn answer(router: Router, method: &str, path: &str) -> (StatusCode, Vec<u8>) {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .body(Body::from(vec![0; 4]))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, body.to_vec())
    }

    #[tokio::test]
    async fn canonical_and_legacy_public_paths_reach_the_same_handlers() {
        let (_root, service, manifest) = router_service().await;
        let session = format!("/session/{}", hex::encode(manifest.id()));
        for (method, operation) in [("GET", "/init"), ("GET", &*session), ("POST", "/query")] {
            let canonical = answer(
                service.query_routes(),
                method,
                &format!("/status/v1{operation}"),
            )
            .await;
            let legacy = answer(
                service.query_routes(),
                method,
                &format!("/v1/status{operation}"),
            )
            .await;
            assert_ne!(canonical.0, StatusCode::NOT_FOUND, "{operation} is routed");
            assert_eq!(canonical, legacy, "{operation}");
        }
        let (status, body) = answer(service.query_routes(), "GET", "/status/v1/init").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(serde_json::from_slice::<Manifest>(&body).unwrap(), manifest);
        let (status, body) = answer(
            service.query_routes(),
            "GET",
            &format!("/status/v1{session}"),
        )
        .await;
        assert_eq!((status, &*body), (StatusCode::OK, &b"public material"[..]));
    }

    #[tokio::test]
    async fn control_and_worker_routes_are_not_public_under_either_prefix() {
        let (_root, service, manifest) = router_service().await;
        let id = hex::encode(manifest.id());
        for prefix in ["/status/v1", "/v1/status"] {
            for (method, control) in [
                ("GET", "/control/health".to_string()),
                ("GET", "/control/identity".to_string()),
                ("GET", "/provider-status".to_string()),
                ("GET", "/v1/provider-status".to_string()),
                ("POST", format!("/v1/provider-pin/{id}")),
                ("POST", "/evaluate".to_string()),
                ("GET", "/internal/metrics".to_string()),
            ] {
                let path = format!("{prefix}{control}");
                for routes in [service.query_routes(), service.routes()] {
                    let (status, _) = answer(routes, method, &path).await;
                    assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
                }
            }
        }
    }
}
