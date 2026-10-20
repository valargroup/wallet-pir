//! Autonomous Status HTTPS consumers. Private loopback routes carry evaluation,
//! identity and readiness; only the publisher owns source observations.
mod admission;
mod serving;
mod sync;
mod telemetry;
#[cfg(all(test, feature = "native-reinspiring"))]
mod wallet_tests;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use ipir_sp::server::MatvecBackend;
use pir_control::admission::ClientSlots;
use serving::{evaluate, query, serving_http_client, shared_bytes, HttpState};
use status_query_server::publication::Failure;
use status_query_server::{
    distributed::*, now_ms, protocol::*, publication::*, telemetry as counters, Controller,
    Generation,
};
use std::{
    fs::File,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;
pub const PER_CLIENT_CONCURRENCY: usize = 2;
#[derive(Default)]
struct StateData {
    controller: Option<Controller>,
    inventory: Option<Object>,
    synchronization: String,
    poisoned: bool,
}
#[derive(Clone)]
pub struct Service {
    role: Role,
    network: Hash,
    incarnation: String,
    dir: PathBuf,
    _lock: Arc<File>,
    state: Arc<Mutex<StateData>>,
    worker_origin: String,
    serving_http: reqwest::Client,
    permits: Arc<admission::Admission>,
    clients: Arc<ClientSlots>,
}
impl Service {
    fn open(
        role: Role,
        network: Hash,
        dir: PathBuf,
        worker_origin: String,
    ) -> Result<Self, Failure> {
        if role == Role::Router {
            loopback_origin(&worker_origin)?;
        }
        let lock = pir_pipeline::service::lock(&dir)?;
        Ok(Self {
            role,
            network,
            incarnation: hex::encode(rand::random::<[u8; 32]>()),
            dir,
            _lock: Arc::new(lock),
            state: Arc::new(Mutex::new(StateData::default())),
            worker_origin,
            serving_http: serving_http_client()?,
            permits: Arc::new(admission::Admission::production()),
            clients: Arc::new(ClientSlots::new(PER_CLIENT_CONCURRENCY)),
        })
    }
    async fn serving(&self) -> Result<Controller, StatusCode> {
        let state = self.state.lock().await;
        if state.poisoned {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        let c = state
            .controller
            .clone()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
        c.check(&c.current())?;
        Ok(c)
    }
    pub fn control_routes(&self) -> Router {
        Router::new()
            .route("/control/health", get(health))
            .route("/control/identity", get(identity))
            .route("/v1/provider-status", get(provider_status))
            .route("/v1/provider-ready/:id", get(provider_ready))
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
pub async fn serve_role_on(
    config: RoleConfig,
    listen: tokio::net::TcpListener,
    query_listen: Option<tokio::net::TcpListener>,
) -> Result<(), Failure> {
    let network = hex::decode(&config.network_hex)?
        .try_into()
        .map_err(|_| "network must be 32 bytes")?;
    let service = Service::open(
        config.role,
        network,
        config.state_dir.clone(),
        config.worker_origin.clone(),
    )?;
    let task = sync::StatusSync::new(service.clone(), &config)?;
    let mut manager = pir_sync_manager::SyncManager::new(task);
    if let Err(error) = manager.poll_once().await {
        tracing::warn!(%error, "Status initial synchronization deferred");
    }
    let mut tasks = pir_pipeline::service::Tasks::default();
    let polling = tokio::spawn(manager.run());
    tasks.own(&polling);
    if let Some(query_listener) = query_listen {
        let app = service.query_routes();
        let wallet = tokio::spawn(async move { axum::serve(query_listener, app).await });
        tasks.own(&wallet);
        tokio::select! { result = axum::serve(listen, service.control_routes()) => result?, result = wallet => { result??; }, result = polling => { result?; return Err("Status synchronization stopped".into()); } }
    } else {
        tokio::select! { result = axum::serve(listen, service.routes()) => result?, result = polling => { result?; return Err("Status synchronization stopped".into()); } }
    }
    Ok(())
}
async fn identity(State(s): State<Service>) -> Json<serde_json::Value> {
    Json(
        serde_json::json!({"binary_sha256":pir_control::Identity::process().binary_sha256,"incarnation":s.incarnation}),
    )
}
async fn health(State(s): State<Service>) -> Json<Health> {
    let state = s.state.lock().await;
    let manifest = state
        .controller
        .as_ref()
        .map(|c| c.current().manifest().clone());
    let ready = !state.poisoned
        && state
            .controller
            .as_ref()
            .is_some_and(|c| c.check(&c.current()).is_ok());
    Json(Health {
        role: s.role,
        binding: Binding {
            epoch: manifest.as_ref().map_or(0, |m| m.recovery_epoch),
            incarnation: s.incarnation.clone(),
        },
        active: manifest.as_ref().map(Manifest::id),
        cached_rows: manifest.as_ref().map(|m| m.rows_digest),
        ready,
    })
}
async fn provider_status(State(s): State<Service>) -> Json<ProviderStatus> {
    let state = s.state.lock().await;
    Json(ProviderStatus {
        inventory: state.inventory.clone(),
        manifest: state
            .controller
            .as_ref()
            .map(|c| c.current().manifest().clone()),
        synchronization: state.synchronization.clone(),
    })
}
async fn provider_ready(
    State(s): State<Service>,
    Path(id): Path<String>,
) -> Result<Json<bool>, StatusCode> {
    let id: Hash = hex::decode(id)
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .try_into()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let c = s.serving().await?;
    let g = c.resolve(id)?;
    c.check(&g)?;
    Ok(Json(true))
}
async fn worker_query(State(s): State<Service>, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    if s.role != Role::Worker {
        return Err(StatusCode::NOT_FOUND);
    }
    let c = s.serving().await?;
    let http_state = HttpState {
        controller: c,
        origin: String::new(),
        http: s.serving_http.clone(),
        permits: s.permits.clone(),
    };
    let result = evaluate(State(http_state), body).await?;
    s.serving().await?;
    Ok(result)
}
async fn router_init(State(s): State<Service>) -> Result<Json<Manifest>, StatusCode> {
    if s.role != Role::Router {
        return Err(StatusCode::NOT_FOUND);
    }
    let c = s.serving().await?;
    let g = c.current();
    c.check(&g)?;
    Ok(Json(g.manifest().clone()))
}
async fn router_session(
    State(s): State<Service>,
    Path(id): Path<String>,
) -> Result<Bytes, StatusCode> {
    if s.role != Role::Router {
        return Err(StatusCode::NOT_FOUND);
    }
    let id: Hash = hex::decode(id)
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .try_into()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let c = s.serving().await?;
    let g = c.resolve(id)?;
    c.check(&g)?;
    Ok(shared_bytes(g.public().clone()))
}
async fn router_query(
    State(s): State<Service>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    if s.role != Role::Router {
        return Err(StatusCode::NOT_FOUND);
    }
    let c = s.serving().await?;
    // The ingress proxy sets the forwarding headers; the loopback peer address
    // carries no client identity.
    let key = pir_control::admission::client_key(&headers, None);
    let Some(_slot) = s.clients.try_acquire(&key) else {
        counters::admission_rejected("router", "client_concurrency");
        return Err(StatusCode::TOO_MANY_REQUESTS);
    };
    let http_state = HttpState {
        controller: c,
        origin: s.worker_origin.clone(),
        http: s.serving_http.clone(),
        permits: s.permits.clone(),
    };
    let result = query(State(http_state), body).await?;
    s.serving().await?;
    Ok(result)
}
