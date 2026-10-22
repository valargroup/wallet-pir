//! Role-specific HTTP routes, authority checks and router-worker handover.
use super::{
    serving::{evaluate, query, shared_bytes, HttpState},
    telemetry, Service,
};
use crate::distributed::{Binding, Health, ProviderStatus, Role};
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
        Router::new()
            .route("/v1/status/init", get(router_init))
            .route("/v1/status/session/:id", get(router_session))
            .route("/v1/status/query", post(router_query))
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
async fn health(State(service): State<Service>) -> Json<Health> {
    let state = service.state.lock().await;
    let manifest = state
        .controller
        .as_ref()
        .map(|controller| controller.current().manifest().clone());
    let ready = !state.poisoned
        && state
            .controller
            .as_ref()
            .is_some_and(|controller| controller.check(&controller.current()).is_ok());
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
/// The single colocated router pins its old and incoming views until its local
/// activation is durable. These references provide backpressure through the
/// core'service existing retention bound; they never renew source observation age.
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
    if state
        .router_holds
        .iter()
        .any(|generation| generation.manifest().id() == id)
    {
        return Ok(Json(true));
    }
    if state.router_holds.len() >= 2 {
        return Err(StatusCode::CONFLICT);
    }
    state.router_holds.push(generation);
    Ok(Json(true))
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
    if !state
        .router_holds
        .iter()
        .any(|generation| generation.manifest().id() == id)
    {
        return Err(StatusCode::CONFLICT);
    }
    state
        .router_holds
        .retain(|generation| generation.manifest().id() == id);
    Ok(Json(true))
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
        origin: String::new(),
        http: service.serving_http.clone(),
        permits: service.permits.clone(),
    };
    let result = evaluate(State(http_state), body).await?;
    service.available_controller().await?;
    controller.check(&generation)?;
    Ok(result)
}
async fn router_init(State(service): State<Service>) -> Result<Json<Manifest>, StatusCode> {
    if service.role != Role::Router {
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
    if service.role != Role::Router {
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
    if service.role != Role::Router {
        return Err(StatusCode::NOT_FOUND);
    }
    let controller = service.available_controller().await?;
    let generation = controller.for_body(&body)?;
    controller.check(&generation)?;
    // The ingress proxy sets the forwarding headers; the loopback peer address
    // carries no client identity.
    let key = pir_control::admission::client_key(&headers, None);
    let Some(_slot) = service.clients.try_acquire(&key) else {
        counters::admission_rejected("router", "client_concurrency");
        return Err(StatusCode::TOO_MANY_REQUESTS);
    };
    let http_state = HttpState {
        controller: controller.clone(),
        origin: service.worker_origin.clone(),
        http: service.serving_http.clone(),
        permits: service.permits.clone(),
    };
    let result = query(State(http_state), body).await?;
    service.available_controller().await?;
    controller.check(&generation)?;
    Ok(result)
}
