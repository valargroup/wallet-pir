//! Status serving roles owned by the query server.
//!
//! The host checks publisher compatibility before opening role state, starts
//! synchronization, then serves the supplied listeners. Dropping the role
//! future aborts owned synchronization and wallet-listener tasks. Generation
//! material, the wire protocol and peer messages stay in `status-shared` and
//! `status-query-server`.
mod admission;
pub mod config;
mod controller;
mod routes;
mod serving;
mod state;
mod sync;
mod telemetry;
#[cfg(all(test, feature = "native-reinspiring"))]
mod wallet_tests;

/// Where the wallet-facing Status operations (`/init`, `/session/:id`,
/// `/query`) are served: the canonical `/status/v1` namespace and the deployed
/// `/v1/status` alias reach the same router handlers. Control, provider and
/// worker routes are never mounted under either.
pub const PUBLIC_ROUTES: pir_control::public_routes::PublicMount =
    pir_control::public_routes::PublicMount::new("/status/v1", "/v1/status");

pub use config::RoleConfig;
pub use controller::Controller;
pub use state::{Service, PER_CLIENT_CONCURRENCY};
pub use status_query_server::distributed::Role;
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
/// With `public`, the wallet routes are installed on the host's shared
/// listener and `listen` serves only private routes; without it, `listen`
/// serves both. Publisher incompatibility is fatal; transient initial
/// synchronization failure leaves serving unavailable until a later pass
/// succeeds. `stop` ends serving cooperatively: accepted requests and a
/// started publication pass finish first.
pub async fn serve_role_on(
    config: RoleConfig,
    listen: tokio::net::TcpListener,
    public: Option<crate::public::Slot>,
    readiness: crate::readiness::Readiness,
    stop: pir_pipeline::service::Shutdown,
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
    let ready = service.clone();
    readiness.register(move || {
        let service = ready.clone();
        async move { service.ready().await }
    });
    let task = sync::synchronization(service.clone(), &config)?;
    // The worker and router each poll the publisher. Their combined delay must
    // leave room for preparation inside Status's twenty-second freshness limit.
    let mut manager = crate::synchronization::SyncManager::new(task)
        .with_success_interval(std::time::Duration::from_secs(1));
    if let Err(error) = manager.poll_once().await {
        if pir_sync_manager::is_incompatible(error.as_ref()) {
            return Err(error);
        }
        tracing::warn!(%error, "Status initial synchronization deferred");
    }
    let routes = match public {
        Some(public) => {
            public.install(service.query_routes());
            service.control_routes()
        }
        None => service.routes(),
    };
    let stopped = stop.clone();
    let result = crate::supervision::supervise(
        "status",
        listen,
        routes,
        manager.run_until(async move { stopped.cancelled().await }),
        stop,
    )
    .await;
    // The host bounds how long it drains wallet requests, but blocking
    // evaluation and packing that outlived its request keep their permit,
    // client slot and generation; the role keeps its directory lock until
    // that work ends.
    service.permits.drained().await;
    result
}
