//! Enhance serving roles: a worker, packing router or query ingress each as
//! its own process, or all three combined in one process.
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
/// Ingress, packing router and worker in one process for the worker's
/// domains. Wallets reach it on the host's `public_listen`; `control_listen`
/// is private.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CombinedConfig {
    pub data_dir: PathBuf,
    pub control_listen: SocketAddr,
    #[serde(default = "default_sealed_shards")]
    pub sealed_shards: usize,
    #[serde(default)]
    pub matvec: MatvecConfig,
    #[serde(default = "default_ingress_requests")]
    pub requests: usize,
    #[serde(default = "default_uploads")]
    pub uploads: usize,
    #[serde(default = "default_router_objects")]
    pub max_objects: usize,
    #[serde(default = "default_router_requests")]
    pub packing_requests: usize,
    pub synchronization: crate::enhance::worker_sync::Config,
}
impl CombinedConfig {
    pub fn limits(&self) -> crate::enhance::combined::Limits {
        crate::enhance::combined::Limits {
            sealed_shards: self.sealed_shards,
            requests: self.requests,
            uploads: self.uploads,
            max_objects: self.max_objects,
            packing_requests: self.packing_requests,
        }
    }
}
fn validate_matvec(matvec: &MatvecConfig) -> Result<(), Error> {
    if matvec.matvec_backend == enhance_shared::matvec::Backend::Cpu && matvec.cuda_device.is_some()
    {
        return Err("CUDA device requires CUDA evaluator".into());
    }
    if matvec.matvec_backend == enhance_shared::matvec::Backend::Cuda
        && !enhance_shared::matvec::CUDA_COMPILED
    {
        return Err("CUDA evaluator is not compiled".into());
    }
    Ok(())
}
fn validate_ingress(requests: usize, uploads: usize) -> Result<(), Error> {
    if !(1..=64).contains(&requests) || uploads < requests || uploads > 4096 {
        return Err("ingress concurrency exceeds qualified limits".into());
    }
    Ok(())
}
fn validate_router(max_objects: usize, requests: usize, domains: usize) -> Result<(), Error> {
    if !(1..=6).contains(&max_objects) || !(1..=4).contains(&requests) || domains > max_objects {
        return Err("router limits exceed qualification target".into());
    }
    Ok(())
}
/// Configuration checks do not read trusted roots, open state or load a driver.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "role", rename_all = "kebab-case")]
pub enum ServingRole {
    Worker(WorkerConfig),
    QueryIngress(IngressConfig),
    PackingRouter(RouterConfig),
    Combined(CombinedConfig),
}
impl ServingRole {
    /// Check publisher compatibility before the host opens state or listeners.
    pub async fn discover(&self) -> Result<(), Error> {
        match self {
            Self::Worker(c) => c.synchronization.source.discover().await,
            Self::PackingRouter(c) => c.synchronization.source.discover().await,
            Self::QueryIngress(c) => c.synchronization.source.discover().await,
            Self::Combined(c) => c.synchronization.source.discover().await,
        }
    }
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Worker(c) => {
                c.synchronization.validate(PlacementPolicy {
                    sealed_shards: c.sealed_shards,
                })?;
                validate_matvec(&c.matvec)?;
            }
            Self::QueryIngress(c) => {
                c.synchronization.validate()?;
                validate_ingress(c.requests, c.uploads)?;
            }
            Self::PackingRouter(c) => {
                c.synchronization.validate()?;
                validate_router(c.max_objects, c.requests, c.synchronization.workers.len())?;
            }
            Self::Combined(c) => {
                c.synchronization.validate(PlacementPolicy {
                    sealed_shards: c.sealed_shards,
                })?;
                validate_matvec(&c.matvec)?;
                validate_ingress(c.requests, c.uploads)?;
                validate_router(
                    c.max_objects,
                    c.packing_requests,
                    c.synchronization.shards.len(),
                )?;
            }
        }
        Ok(())
    }
    pub fn listens(&self) -> Vec<SocketAddr> {
        match self {
            Self::Worker(c) => vec![c.listen],
            Self::QueryIngress(c) => vec![c.control_listen],
            Self::PackingRouter(c) => vec![c.listen, c.control_listen],
            Self::Combined(c) => vec![c.control_listen],
        }
    }
    /// Whether this role answers wallets on the host's `public_listen`. A
    /// packing router's `listen` serves only its ingress.
    pub fn serves_wallets(&self) -> bool {
        matches!(self, Self::QueryIngress(_) | Self::Combined(_))
    }
    pub fn state_dir(&self) -> &std::path::Path {
        match self {
            Self::Worker(c) => &c.data_dir,
            Self::QueryIngress(c) => &c.data_dir,
            Self::PackingRouter(c) => &c.data_dir,
            Self::Combined(c) => &c.data_dir,
        }
    }
}
pub async fn run_worker(config: WorkerConfig) -> Result<(), Error> {
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    run_worker_on(
        config,
        listener,
        Default::default(),
        crate::enhance::shutdown::signal(),
    )
    .await
}
pub async fn run_worker_on(
    config: WorkerConfig,
    listener: tokio::net::TcpListener,
    readiness: crate::readiness::Readiness,
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
    let ready = worker.clone();
    readiness.register(move || std::future::ready(ready.ready()));
    tokio::select! {
        result = crate::enhance::shutdown::serve_until(listener, worker.router(), shutdown) => result?,
        error = manager.run() => return Err(error),
    }
    Ok(())
}
pub async fn run_ingress_on(
    config: IngressConfig,
    public: crate::public::Slot,
    control: tokio::net::TcpListener,
    readiness: crate::readiness::Readiness,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    let ingress = crate::enhance::ingress_sync::QueryIngress::open(
        &config.data_dir,
        config.synchronization,
        config.requests,
        config.uploads,
    )?;
    let manager = crate::synchronization::SyncManager::new(ingress.synchronizer()?);
    let ready = ingress.clone();
    readiness.register(move || std::future::ready(ready.ready()));
    public.install(ingress.public_router());
    let serving =
        crate::enhance::shutdown::serve_until(control, ingress.control_router(), shutdown);
    tokio::select! { result = serving => result?, error = manager.run() => return Err(error), }
    Ok(())
}
pub async fn run_router(config: RouterConfig) -> Result<(), Error> {
    let public = tokio::net::TcpListener::bind(config.listen).await?;
    let control = tokio::net::TcpListener::bind(config.control_listen).await?;
    run_router_on(
        config,
        public,
        control,
        Default::default(),
        crate::enhance::shutdown::signal(),
    )
    .await
}
pub async fn run_router_on(
    config: RouterConfig,
    public: tokio::net::TcpListener,
    control: tokio::net::TcpListener,
    readiness: crate::readiness::Readiness,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    let router = crate::enhance::router_sync::PackingRouter::open(
        &config.data_dir,
        config.synchronization,
        config.max_objects,
        config.requests,
    )?;
    let manager = crate::synchronization::SyncManager::new(router.synchronizer()?);
    let ready = router.clone();
    readiness.register(move || std::future::ready(ready.ready()));
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
pub async fn run_combined_on(
    config: CombinedConfig,
    public: crate::public::Slot,
    control: tokio::net::TcpListener,
    readiness: crate::readiness::Readiness,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    let combined = crate::enhance::combined::Combined::open(
        &config.data_dir,
        config.synchronization.clone(),
        config.limits(),
        config.matvec,
    )?;
    let synchronizers = combined.synchronizers()?;
    let worker = crate::synchronization::SyncManager::new(synchronizers.worker);
    let router = crate::synchronization::SyncManager::new(synchronizers.router);
    let ingress = crate::synchronization::SyncManager::new(synchronizers.ingress);
    let ready = combined.clone();
    readiness.register(move || std::future::ready(ready.ready()));
    public.install(combined.public_router());
    let serving =
        crate::enhance::shutdown::serve_until(control, combined.control_router(), shutdown);
    // Each component synchronizes independently; only a terminal error from
    // any of them (a confirmed publisher incompatibility) stops the process.
    tokio::select! {
        result = serving => result?,
        error = worker.run() => return Err(error),
        error = router.run() => return Err(error),
        error = ingress.run() => return Err(error),
    }
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
/// Run one role on listeners reserved by the host. Wallet-facing roles
/// install their wallet router into `public`, the host's shared listener.
pub async fn run_serving(
    role: ServingRole,
    mut listeners: Vec<tokio::net::TcpListener>,
    public: Option<crate::public::Slot>,
    readiness: crate::readiness::Readiness,
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
        ServingRole::Worker(config) => {
            run_worker_on(config, take(&mut listeners)?, readiness, shutdown).await
        }
        ServingRole::QueryIngress(config) => {
            let public = public.ok_or("query ingress needs the host's public listener")?;
            let control = take(&mut listeners)?;
            run_ingress_on(config, public, control, readiness, shutdown).await
        }
        ServingRole::PackingRouter(config) => {
            let public = take(&mut listeners)?;
            let control = take(&mut listeners)?;
            run_router_on(config, public, control, readiness, shutdown).await
        }
        ServingRole::Combined(config) => {
            let public = public.ok_or("combined Enhance needs the host's public listener")?;
            let control = take(&mut listeners)?;
            run_combined_on(config, public, control, readiness, shutdown).await
        }
    }
}
