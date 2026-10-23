//! One independently synchronizing Enhance serving role per process.
use enhance_shared::{matvec::MatvecConfig, placement::PlacementPolicy};
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
    256
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
    pub synchronization: crate::enhance::worker_sync::Config,
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
    pub synchronization: crate::enhance::ingress_sync::Config,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouterConfig {
    pub data_dir: PathBuf,
    pub listen: SocketAddr,
    pub control_listen: SocketAddr,
    #[serde(default = "default_router_objects")]
    pub max_objects: usize,
    #[serde(default = "default_router_requests")]
    pub requests: usize,
    pub synchronization: crate::enhance::router_sync::Config,
}
/// Configuration checks do not read trusted roots, open state or load a driver.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "role", rename_all = "kebab-case")]
pub enum ServingRole {
    Worker(WorkerConfig),
    QueryIngress(IngressConfig),
    PackingRouter(RouterConfig),
}
impl ServingRole {
    /// Check publisher compatibility before the host opens state or listeners.
    pub async fn discover(&self) -> Result<(), Error> {
        match self {
            Self::Worker(c) => c.synchronization.source.discover().await,
            Self::PackingRouter(c) => c.synchronization.source.discover().await,
            Self::QueryIngress(c) => c.synchronization.source.discover().await,
        }
    }
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Worker(c) => {
                c.synchronization.validate(PlacementPolicy {
                    sealed_shards: c.sealed_shards,
                })?;
                if c.matvec.matvec_backend == enhance_shared::matvec::Backend::Cpu
                    && c.matvec.cuda_device.is_some()
                {
                    return Err("CUDA device requires CUDA evaluator".into());
                }
                if c.matvec.matvec_backend == enhance_shared::matvec::Backend::Cuda
                    && !enhance_shared::matvec::CUDA_COMPILED
                {
                    return Err("CUDA evaluator is not compiled".into());
                }
            }
            Self::QueryIngress(c) => {
                c.synchronization.validate()?;
                if !(1..=64).contains(&c.requests) || c.uploads < c.requests || c.uploads > 4096 {
                    return Err("ingress concurrency exceeds qualified limits".into());
                }
            }
            Self::PackingRouter(c) => {
                c.synchronization.validate()?;
                if !(1..=6).contains(&c.max_objects)
                    || !(1..=4).contains(&c.requests)
                    || c.synchronization.workers.len() > c.max_objects
                {
                    return Err("router limits exceed qualification target".into());
                }
            }
        }
        Ok(())
    }
    pub fn listens(&self) -> Vec<SocketAddr> {
        match self {
            Self::Worker(c) => vec![c.listen],
            Self::QueryIngress(c) => vec![c.listen, c.control_listen],
            Self::PackingRouter(c) => vec![c.listen, c.control_listen],
        }
    }
    pub fn state_dir(&self) -> &std::path::Path {
        match self {
            Self::Worker(c) => &c.data_dir,
            Self::QueryIngress(c) => &c.data_dir,
            Self::PackingRouter(c) => &c.data_dir,
        }
    }
    pub fn probe(&self) -> (SocketAddr, &'static str) {
        (
            match self {
                Self::Worker(c) => c.listen,
                Self::QueryIngress(c) => c.control_listen,
                Self::PackingRouter(c) => c.control_listen,
            },
            "/internal/health",
        )
    }
}
pub async fn run_worker(config: WorkerConfig) -> Result<(), Error> {
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    run_worker_on(config, listener, crate::enhance::shutdown::signal()).await
}
pub async fn run_worker_on(
    config: WorkerConfig,
    listener: tokio::net::TcpListener,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    let worker = crate::enhance::worker_sync::Worker::open(
        &config.data_dir,
        config.synchronization,
        PlacementPolicy {
            sealed_shards: config.sealed_shards,
        },
        config.matvec,
    )?;
    let manager = crate::synchronization::SyncManager::new(worker.synchronizer()?);
    tokio::select! {
        result = crate::enhance::shutdown::serve_until(listener, worker.router(), shutdown) => result?,
        error = manager.run() => return Err(error),
    }
    Ok(())
}
pub async fn run_ingress(config: IngressConfig) -> Result<(), Error> {
    let public = tokio::net::TcpListener::bind(config.listen).await?;
    let control = tokio::net::TcpListener::bind(config.control_listen).await?;
    run_ingress_on(config, public, control, crate::enhance::shutdown::signal()).await
}
pub async fn run_ingress_on(
    config: IngressConfig,
    public: tokio::net::TcpListener,
    control: tokio::net::TcpListener,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    let ingress = crate::enhance::ingress_sync::QueryIngress::open(
        &config.data_dir,
        config.synchronization,
        config.requests,
        config.uploads,
    )?;
    let manager = crate::synchronization::SyncManager::new(ingress.synchronizer()?);
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
            crate::enhance::shutdown::serve_until(control, control_app, control_stop)
        )
        .map(|_| ())
    };
    tokio::select! { result = serving => result?, error = manager.run() => return Err(error), }
    Ok(())
}
pub async fn run_router(config: RouterConfig) -> Result<(), Error> {
    let public = tokio::net::TcpListener::bind(config.listen).await?;
    let control = tokio::net::TcpListener::bind(config.control_listen).await?;
    run_router_on(config, public, control, crate::enhance::shutdown::signal()).await
}
pub async fn run_router_on(
    config: RouterConfig,
    public: tokio::net::TcpListener,
    control: tokio::net::TcpListener,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    let router = crate::enhance::router_sync::PackingRouter::open(
        &config.data_dir,
        config.synchronization,
        config.max_objects,
        config.requests,
    )?;
    let manager = crate::synchronization::SyncManager::new(router.synchronizer()?);
    let (notify, public_stop, control_stop) = stop_pair(shutdown);
    let _notify = notify;
    let serving = async move {
        tokio::try_join!(
            crate::enhance::shutdown::serve_until(public, router.public_router(), public_stop),
            crate::enhance::shutdown::serve_until(control, router.control_router(), control_stop)
        )
        .map(|_| ())
    };
    tokio::select! { result = serving => result?, error = manager.run() => return Err(error), }
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
pub async fn run_serving(
    role: ServingRole,
    mut listeners: Vec<tokio::net::TcpListener>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    role.validate()?;
    listeners.reverse();
    let take = |listeners: &mut Vec<tokio::net::TcpListener>| {
        listeners
            .pop()
            .ok_or_else(|| "reserved listener is missing".to_string())
    };
    match role {
        ServingRole::Worker(config) => run_worker_on(config, take(&mut listeners)?, shutdown).await,
        ServingRole::QueryIngress(config) => {
            let public = take(&mut listeners)?;
            let control = take(&mut listeners)?;
            run_ingress_on(config, public, control, shutdown).await
        }
        ServingRole::PackingRouter(config) => {
            let public = take(&mut listeners)?;
            let control = take(&mut listeners)?;
            run_router_on(config, public, control, shutdown).await
        }
    }
}
