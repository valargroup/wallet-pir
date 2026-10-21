//! One Enhance serving role per process: worker, query ingress, or packing router.
//! Publication and chain reading stay in the publisher.
use crate::matvec::MatvecConfig;
use crate::placement::PlacementPolicy;
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

fn default_sealed_shards() -> usize {
    6
}
fn default_ingress_requests() -> usize {
    16
}
fn default_uploads() -> usize {
    crate::query_ingress::DEFAULT_UPLOADS
}
fn default_router_objects() -> usize {
    6
}
fn default_router_requests() -> usize {
    4
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    #[serde(default = "default_sealed_shards")]
    pub sealed_shards: usize,
    #[serde(default)]
    pub matvec: MatvecConfig,
    #[serde(default)]
    pub synchronization: Option<crate::worker_sync::Config>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngressConfig {
    pub data_dir: PathBuf,
    pub listen: SocketAddr,
    pub control_listen: SocketAddr,
    #[serde(default = "default_ingress_requests")]
    pub requests: usize,
    #[serde(default = "default_uploads")]
    pub uploads: usize,
    #[serde(default)]
    pub synchronization: Option<crate::ingress_sync::Config>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouterConfig {
    pub data_dir: PathBuf,
    #[serde(default)]
    pub artifact_origin: Option<String>,
    pub listen: SocketAddr,
    pub control_listen: SocketAddr,
    #[serde(default = "default_router_objects")]
    pub max_objects: usize,
    #[serde(default = "default_router_requests")]
    pub requests: usize,
    #[serde(default)]
    pub synchronization: Option<crate::router_sync::Config>,
}

/// Evaluate queries for the domains placed on this worker.
pub async fn run_worker(config: WorkerConfig) -> Result<(), Error> {
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    run_worker_on(config, listener, crate::shutdown::signal()).await
}

pub async fn run_worker_on(
    config: WorkerConfig,
    listener: tokio::net::TcpListener,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    if let Some(sync) = config.synchronization {
        let worker = crate::worker_sync::Worker::open(
            &config.data_dir,
            sync,
            PlacementPolicy {
                sealed_shards: config.sealed_shards,
            },
            config.matvec,
        )?;
        let synchronizer = worker.synchronizer()?;
        let manager = pir_sync_manager::SyncManager::new(synchronizer);
        tokio::select! {
            result = crate::shutdown::serve_until(listener, worker.router(), shutdown) => result?,
            // Only a terminal error ends the loop; keep its type for the host.
            error = manager.run() => return Err(error),
        }
        return Ok(());
    }
    let worker = crate::worker::Worker::open_with_backend(
        &config.data_dir,
        PlacementPolicy {
            sealed_shards: config.sealed_shards,
        },
        config.matvec,
    )?;
    crate::shutdown::serve_until(listener, worker.router(), shutdown).await?;
    Ok(())
}

/// Admit public queries and forward them to a packing router.
pub async fn run_ingress(config: IngressConfig) -> Result<(), Error> {
    let public = tokio::net::TcpListener::bind(config.listen).await?;
    let control = tokio::net::TcpListener::bind(config.control_listen).await?;
    run_ingress_on(config, public, control, crate::shutdown::signal()).await
}

pub async fn run_ingress_on(
    config: IngressConfig,
    public: tokio::net::TcpListener,
    control: tokio::net::TcpListener,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    if let Some(sync) = config.synchronization {
        let ingress = crate::ingress_sync::QueryIngress::open(
            &config.data_dir,
            sync,
            config.requests,
            config.uploads,
        )?;
        let manager = pir_sync_manager::SyncManager::new(ingress.synchronizer()?);
        let public_app = ingress
            .public_router()
            .into_make_service_with_connect_info::<SocketAddr>();
        let control_app = ingress.control_router();
        let (notify, public_stop, control_stop) = stop_pair(shutdown);
        let _notify = notify;
        let serving = async move {
            tokio::try_join!(
                async move {
                    axum::serve(public, public_app)
                        .with_graceful_shutdown(public_stop)
                        .await
                },
                crate::shutdown::serve_until(control, control_app, control_stop)
            )
            .map(|_| ())
        };
        tokio::select! {
            result = serving => result?,
            error = manager.run() => return Err(error),
        }
        return Ok(());
    }
    let ingress = crate::query_ingress::QueryIngress::open_with_uploads(
        &config.data_dir,
        config.requests,
        config.uploads,
    )?;
    let public_app = ingress
        .public_router()
        .into_make_service_with_connect_info::<SocketAddr>();
    let control_app = ingress.control_router();
    let (notify, public_stop, control_stop) = stop_pair(shutdown);
    let _notify = notify;
    tokio::try_join!(
        async move {
            axum::serve(public, public_app)
                .with_graceful_shutdown(public_stop)
                .await
        },
        crate::shutdown::serve_until(control, control_app, control_stop),
    )?;
    Ok(())
}

/// Decode and pack wallet queries. The artifact origin is local configuration.
pub async fn run_router(config: RouterConfig) -> Result<(), Error> {
    let public = tokio::net::TcpListener::bind(config.listen).await?;
    let control = tokio::net::TcpListener::bind(config.control_listen).await?;
    run_router_on(config, public, control, crate::shutdown::signal()).await
}

pub async fn run_router_on(
    config: RouterConfig,
    public: tokio::net::TcpListener,
    control: tokio::net::TcpListener,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    if let Some(sync) = config.synchronization {
        if config.artifact_origin.is_some() {
            return Err("autonomous router must use only its HTTPS publisher origin".into());
        }
        let router = crate::router_sync::PackingRouter::open(
            &config.data_dir,
            sync,
            config.max_objects,
            config.requests,
        )?;
        let manager = pir_sync_manager::SyncManager::new(router.synchronizer()?);
        let (notify, public_stop, control_stop) = stop_pair(shutdown);
        let _notify = notify;
        let serving = async move {
            tokio::try_join!(
                crate::shutdown::serve_until(public, router.public_router(), public_stop),
                crate::shutdown::serve_until(control, router.control_router(), control_stop)
            )
            .map(|_| ())
        };
        tokio::select! {
            result = serving => result?,
            // Only a terminal error ends the loop; keep its type for the host.
            error = manager.run() => return Err(error),
        }
        return Ok(());
    }
    let router = crate::packing_router::PackingRouter::open(
        &config.data_dir,
        config
            .artifact_origin
            .as_deref()
            .ok_or("router artifact origin is missing")?,
        config.max_objects,
        config.requests,
    )?;
    router.start_worker_health_monitor();
    let (notify, public_stop, control_stop) = stop_pair(shutdown);
    let _notify = notify;
    tokio::try_join!(
        crate::shutdown::serve_until(public, router.public_router(), public_stop),
        crate::shutdown::serve_until(control, router.control_router(), control_stop),
    )?;
    Ok(())
}

fn stop_pair(
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> (
    tokio::task::JoinHandle<()>,
    impl std::future::Future<Output = ()> + Send,
    impl std::future::Future<Output = ()> + Send,
) {
    let (sender, receiver) = tokio::sync::watch::channel(false);
    let notify = tokio::spawn(async move {
        shutdown.await;
        let _ = sender.send(true);
    });
    let mut first = receiver.clone();
    let mut second = receiver;
    (
        notify,
        async move {
            let _ = first.changed().await;
        },
        async move {
            let _ = second.changed().await;
        },
    )
}

/// One Enhance role in a query-server process.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "role", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ServingRole {
    Worker {
        listen: SocketAddr,
        data_dir: PathBuf,
        #[serde(default = "default_sealed_shards")]
        sealed_shards: usize,
        #[serde(default)]
        matvec: MatvecConfig,
        #[serde(default)]
        synchronization: Option<crate::worker_sync::Config>,
    },
    QueryIngress {
        data_dir: PathBuf,
        listen: SocketAddr,
        control_listen: SocketAddr,
        #[serde(default = "default_ingress_requests")]
        requests: usize,
        #[serde(default = "default_uploads")]
        uploads: usize,
        #[serde(default)]
        synchronization: Option<crate::ingress_sync::Config>,
    },
    PackingRouter {
        data_dir: PathBuf,
        #[serde(default)]
        artifact_origin: Option<String>,
        listen: SocketAddr,
        control_listen: SocketAddr,
        #[serde(default = "default_router_objects")]
        max_objects: usize,
        #[serde(default = "default_router_requests")]
        requests: usize,
        #[serde(default)]
        synchronization: Option<crate::router_sync::Config>,
    },
}

impl ServingRole {
    /// Configuration-only checks: do not read a CA file or open serving state.
    pub fn validate(&self) -> Result<(), Error> {
        if let Self::Worker {
            synchronization: Some(sync),
            sealed_shards,
            ..
        } = self
        {
            sync.validate(PlacementPolicy {
                sealed_shards: *sealed_shards,
            })?;
        }
        if let Self::QueryIngress {
            synchronization: Some(sync),
            ..
        } = self
        {
            sync.validate()?;
        }
        if let Self::PackingRouter {
            synchronization,
            artifact_origin,
            ..
        } = self
        {
            if let Some(sync) = synchronization {
                sync.validate()?;
                if artifact_origin.is_some() {
                    return Err("autonomous router must use only its HTTPS publisher origin".into());
                }
            } else {
                crate::packing_router::valid_origin(
                    artifact_origin
                        .as_deref()
                        .ok_or("router artifact origin is missing")?,
                )?;
            }
        }
        Ok(())
    }
    /// Check publisher service discovery for every autonomous Enhance role.
    /// The host runs this before binding listeners or opening state; legacy
    /// push roles have no publisher origin and are not checked.
    pub async fn discover(&self) -> Result<(), Error> {
        match self {
            Self::Worker {
                synchronization: Some(sync),
                ..
            } => sync.source.discover().await,
            Self::PackingRouter {
                synchronization: Some(sync),
                ..
            } => sync.source.discover().await,
            Self::QueryIngress {
                synchronization: Some(sync),
                ..
            } => sync.source.discover().await,
            _ => Ok(()),
        }
    }
    pub fn listens(&self) -> Vec<SocketAddr> {
        match self {
            Self::Worker { listen, .. } => vec![*listen],
            Self::QueryIngress {
                listen,
                control_listen,
                ..
            }
            | Self::PackingRouter {
                listen,
                control_listen,
                ..
            } => vec![*listen, *control_listen],
        }
    }
    pub fn state_dir(&self) -> &std::path::Path {
        match self {
            Self::Worker { data_dir, .. }
            | Self::QueryIngress { data_dir, .. }
            | Self::PackingRouter { data_dir, .. } => data_dir,
        }
    }
    pub fn probe(&self) -> (SocketAddr, &'static str) {
        match self {
            Self::Worker { listen, .. } => (*listen, "/internal/health"),
            Self::QueryIngress { control_listen, .. }
            | Self::PackingRouter { control_listen, .. } => (*control_listen, "/internal/health"),
        }
    }
}

pub async fn run_serving(
    role: ServingRole,
    listeners: Vec<tokio::net::TcpListener>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    let mut listeners = listeners;
    let take = |listeners: &mut Vec<tokio::net::TcpListener>| {
        listeners
            .pop()
            .ok_or_else(|| "reserved listener is missing".to_string())
    };
    match role {
        ServingRole::Worker {
            listen,
            data_dir,
            sealed_shards,
            matvec,
            synchronization,
        } => {
            // Bound in listen order, so the only listener is at the front.
            listeners.reverse();
            run_worker_on(
                WorkerConfig {
                    listen,
                    data_dir,
                    sealed_shards,
                    matvec,
                    synchronization,
                },
                take(&mut listeners)?,
                shutdown,
            )
            .await
        }
        ServingRole::QueryIngress {
            data_dir,
            listen,
            control_listen,
            requests,
            uploads,
            synchronization,
        } => {
            listeners.reverse();
            let public = take(&mut listeners)?;
            let control = take(&mut listeners)?;
            run_ingress_on(
                IngressConfig {
                    data_dir,
                    listen,
                    control_listen,
                    requests,
                    uploads,
                    synchronization,
                },
                public,
                control,
                shutdown,
            )
            .await
        }
        ServingRole::PackingRouter {
            data_dir,
            artifact_origin,
            listen,
            control_listen,
            max_objects,
            requests,
            synchronization,
        } => {
            listeners.reverse();
            let public = take(&mut listeners)?;
            let control = take(&mut listeners)?;
            run_router_on(
                RouterConfig {
                    data_dir,
                    artifact_origin,
                    listen,
                    control_listen,
                    max_objects,
                    requests,
                    synchronization,
                },
                public,
                control,
                shutdown,
            )
            .await
        }
    }
}
