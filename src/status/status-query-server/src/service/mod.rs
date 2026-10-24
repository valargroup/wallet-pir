//! Autonomous Status serving roles hosted by `pir-query-server`.
//!
//! This facade checks publisher compatibility before opening role state, starts
//! synchronization, then serves the supplied listeners. Dropping its future
//! aborts owned synchronization and wallet-listener tasks.
mod admission;
mod routes;
mod serving;
mod state;
#[cfg(all(test, feature = "native-reinspiring"))]
#[path = "../../tests/support/status_http.rs"]
mod status_http;
mod sync;
mod telemetry;
#[cfg(all(test, feature = "native-reinspiring"))]
mod wallet_tests;

use crate::distributed::RoleConfig;
pub use state::{Service, PER_CLIENT_CONCURRENCY};
use status_shared::publication::{attempt_limit, Failure, PREFIX};

fn source(config: &RoleConfig) -> Result<pir_sync_manager::HttpsSource, Failure> {
    let roots = match &config.publisher_ca {
        Some(path) => vec![reqwest::Certificate::from_pem(&std::fs::read(path)?)?],
        None => Vec::new(),
    };
    pir_sync_manager::HttpsSource::new(&config.publisher, PREFIX, attempt_limit(), &roots)
}

/// Verify publisher compatibility without opening state or starting listeners.
pub async fn discover(config: &RoleConfig) -> Result<(), Failure> {
    source(config)?.discover().await?;
    Ok(())
}

/// Run one independently synchronizing role on listeners reserved by the host.
/// Publisher incompatibility is fatal; transient initial synchronization failure
/// leaves serving unavailable until a later pass succeeds.
pub async fn serve_role_on(
    config: RoleConfig,
    listen: tokio::net::TcpListener,
    query_listen: Option<tokio::net::TcpListener>,
) -> Result<(), Failure> {
    discover(&config).await?;
    let network = hex::decode(&config.network_hex)?
        .try_into()
        .map_err(|_| "network must be 32 bytes")?;
    let service = Service::open(
        config.role,
        network,
        config.state_dir.clone(),
        config.worker_origin.clone(),
    )?;
    let task = sync::synchronization(service.clone(), &config)?;
    // Handover polling accelerates as the original source observation ages;
    // preparation and both roles share its twenty-second freshness budget.
    let mut manager = sync::manager(task);
    if let Err(error) = manager.poll_once().await {
        if pir_sync_manager::is_incompatible(error.as_ref()) {
            return Err(error);
        }
        tracing::warn!(%error, "Status initial synchronization deferred");
    }
    let mut tasks = pir_pipeline::service::Tasks::default();
    let polling = tokio::spawn(manager.run());
    tasks.own(&polling);
    if let Some(query_listener) = query_listen {
        let app = service.query_routes();
        let wallet = tokio::spawn(async move { axum::serve(query_listener, app).await });
        tasks.own(&wallet);
        tokio::select! { result = axum::serve(listen, service.control_routes()) => result?, result = wallet => { result??; }, result = polling => return Err(result?), }
    } else {
        tokio::select! { result = axum::serve(listen, service.routes()) => result?, result = polling => return Err(result?), }
    }
    Ok(())
}
