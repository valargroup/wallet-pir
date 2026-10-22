//! Shared role state and exclusive ownership of the local serving directory.
use super::{admission, serving::serving_http_client};
use crate::{
    distributed::{loopback_origin, Role},
    Controller,
};
use axum::http::StatusCode;
use pir_control::admission::ClientSlots;
use pir_publication::ObjectReference;
use status_shared::{protocol::Hash, publication::Failure, Generation};
use std::{fs::File, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

pub const PER_CLIENT_CONCURRENCY: usize = 2;
#[derive(Default)]
pub(super) struct ServiceState {
    pub(super) controller: Option<Controller>,
    pub(super) inventory: Option<ObjectReference>,
    pub(super) synchronization: String,
    pub(super) poisoned: bool,
    pub(super) router_holds: Vec<Arc<Generation>>,
}
/// Shared state for one router or worker. Clones retain the directory lock and
/// refer to the same serving authority and admission limits.
#[derive(Clone)]
pub struct Service {
    pub(super) role: Role,
    pub(super) network: Hash,
    pub(super) incarnation: String,
    pub(super) state_dir: PathBuf,
    pub(super) _lock: Arc<File>,
    pub(super) state: Arc<Mutex<ServiceState>>,
    pub(super) worker_origin: String,
    pub(super) serving_http: reqwest::Client,
    pub(super) permits: Arc<admission::Admission>,
    pub(super) clients: Arc<ClientSlots>,
}
impl Service {
    /// Lock the role directory and initialize unavailable serving state.
    pub(super) fn open(
        role: Role,
        network: Hash,
        state_dir: PathBuf,
        worker_origin: String,
    ) -> Result<Self, Failure> {
        if role == Role::Router {
            loopback_origin(&worker_origin)?;
        }
        let lock = pir_pipeline::service::lock(&state_dir)?;
        Ok(Self {
            role,
            network,
            incarnation: hex::encode(rand::random::<[u8; 32]>()),
            state_dir,
            _lock: Arc::new(lock),
            state: Arc::new(Mutex::new(ServiceState::default())),
            worker_origin,
            serving_http: serving_http_client()?,
            permits: Arc::new(admission::Admission::production()),
            clients: Arc::new(ClientSlots::new(PER_CLIENT_CONCURRENCY)),
        })
    }
    /// Return the active authority only while state and freshness permit serving.
    pub(super) async fn serving(&self) -> Result<Controller, StatusCode> {
        let controller = self.available_controller().await?;
        controller.check(&controller.current())?;
        Ok(controller)
    }
    /// A controller whose admission is open. Session handlers must check their
    /// requested generation; another view's invalidation cannot revoke it.
    pub(super) async fn available_controller(&self) -> Result<Controller, StatusCode> {
        let state = self.state.lock().await;
        if state.poisoned {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        let controller = state
            .controller
            .clone()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
        if controller.revoked() {
            return Err(StatusCode::GONE);
        }
        Ok(controller)
    }
}
