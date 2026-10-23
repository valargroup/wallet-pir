//! Transparent transaction-detail role: operator settings, publisher
//! discovery, initial synchronization with its retry rules, listener ownership
//! and supervision, and the local verification command. Display tables,
//! runtimes and handlers live beside it in this module.
use crate::transparent::config::ServiceConfig;
use crate::transparent::tx_detail::service::{DisplayRuntime, DisplayState};
use serde::Deserialize;
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::path::PathBuf;
use transparent_shared::history::shardset::DEFAULT_RETAIN_REVISIONS;
use transparent_shared::tx_detail::serves;
use transparent_shared::tx_detail::set::DisplaySet;
use transparent_shared::tx_detail::DisplayRole;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

fn default_bootstrap_max_bytes() -> u64 {
    64 << 30
}
fn default_listen() -> SocketAddr {
    "127.0.0.1:8095".parse().unwrap()
}
fn default_cache_bytes() -> u64 {
    4 << 30
}
fn default_build_slots() -> usize {
    1
}
fn default_query_slots() -> usize {
    2
}
fn default_retain_revisions() -> usize {
    DEFAULT_RETAIN_REVISIONS
}
fn default_query_waiters() -> usize {
    64
}
fn default_body_bytes() -> u64 {
    64 << 20
}
fn default_upload_deadline() -> u64 {
    10
}
fn default_query_deadline() -> u64 {
    30
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DetailProcess {
    #[serde(default)]
    pub publisher: Option<String>,
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
    #[serde(default)]
    pub publisher_network: Option<String>,
    #[serde(default)]
    pub publisher_genesis: Option<String>,
    #[serde(default)]
    pub publisher_ca: Option<PathBuf>,
    #[serde(default = "default_bootstrap_max_bytes")]
    pub bootstrap_max_bytes: u64,
    #[serde(default = "default_listen")]
    pub listen: SocketAddr,
    pub role: DisplayRole,
    #[serde(default)]
    pub publication_dir: Option<PathBuf>,
    #[serde(default = "default_cache_bytes")]
    pub cache_bytes: u64,
    #[serde(default = "default_build_slots")]
    pub build_slots: usize,
    #[serde(default = "default_query_slots")]
    pub query_slots: usize,
    #[serde(default = "default_retain_revisions")]
    pub retain_revisions: usize,
    #[serde(default = "default_query_waiters")]
    pub query_waiters: usize,
    #[serde(default = "default_body_bytes")]
    pub body_bytes: u64,
    #[serde(default = "default_upload_deadline")]
    pub upload_deadline_secs: u64,
    #[serde(default = "default_query_deadline")]
    pub query_deadline_secs: u64,
    #[serde(default)]
    pub verify_only: bool,
}

fn certificates(path: Option<PathBuf>) -> Result<Vec<reqwest::Certificate>, Error> {
    Ok(match path {
        Some(path) => vec![reqwest::Certificate::from_pem(&std::fs::read(path)?)?],
        None => Vec::new(),
    })
}

impl DetailProcess {
    /// Check the publisher's service discovery using the configured origin
    /// and CA. Opens no state; local verification without a publisher is a no-op.
    pub async fn discover(&self) -> Result<(), Error> {
        match &self.publisher {
            Some(publisher) => {
                crate::transparent::discover_publisher(
                    publisher,
                    &certificates(self.publisher_ca.clone())?,
                )
                .await
            }
            None => Ok(()),
        }
    }
}

pub async fn run(config: DetailProcess) -> Result<(), Error> {
    run_with(config, None, None, Default::default()).await
}

/// Serve on `reserved` (or a socket bound at `listen`), the provider listener
/// fleet routers and private probes reach. With `public`, the wallet routes
/// are also installed on the host's shared wallet listener once the first
/// checked publication is active.
pub async fn run_with(
    config: DetailProcess,
    mut reserved: Option<tokio::net::TcpListener>,
    public: Option<crate::public::Slot>,
    readiness: crate::readiness::Readiness,
) -> Result<(), Error> {
    if let Some(publisher) = config.publisher.clone() {
        if config.role != DisplayRole::Combined {
            return Err("managed display prototype requires combined holdings".into());
        }
        let bootstrap = crate::transparent::SourceConfig {
            publisher,
            network: config
                .publisher_network
                .clone()
                .ok_or("publisher network is required")?,
            genesis_hash: config
                .publisher_genesis
                .clone()
                .ok_or("publisher genesis is required")?,
            directory: config
                .state_dir
                .clone()
                .ok_or("managed display requires a state directory")?,
            cache_bytes: config.cache_bytes,
            max_download_bytes: config.bootstrap_max_bytes,
            roots: certificates(config.publisher_ca.clone())?,
        };
        let service = ServiceConfig {
            cache_bytes: config.cache_bytes,
            build_slots: config.build_slots,
            query_slots: config.query_slots,
            max_waiters: config.query_waiters,
            max_body_bytes: config.body_bytes,
            upload_deadline: std::time::Duration::from_secs(config.upload_deadline_secs),
            query_deadline: std::time::Duration::from_secs(config.query_deadline_secs),
        };
        let mut manager =
            crate::transparent::tx_detail::manager::Manager::new(bootstrap, service).await?;
        loop {
            match manager.poll_once().await {
                Ok(_) => break,
                Err(error) if crate::transparent::is_incompatible(&error) => return Err(error),
                Err(error) if manager.ready().await => {
                    tracing::warn!(%error, "new display publication refused; recovered checked predecessor");
                    break;
                }
                Err(error) if config.verify_only => return Err(error),
                Err(error) => {
                    tracing::warn!(%error, "initial display synchronization refused; retrying with checked inputs");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
        if config.verify_only {
            return Ok(());
        }
        let app = manager.router();
        let live = manager.live();
        readiness.register(move || std::future::ready(live.ready()));
        if let Some(public) = public {
            public.install(app.clone());
        }
        let mut synchronization = tokio::spawn(manager.run());
        let listener = match reserved.take() {
            Some(listener) => listener,
            None => tokio::net::TcpListener::bind(config.listen).await?,
        };
        tokio::select! {
            result = axum::serve(listener, app).into_future() => {
                synchronization.abort();
                result?;
            }
            result = &mut synchronization => {
                return Err(match result {
                    Ok(error) => error,
                    Err(join) => format!("display synchronization stopped: {join}").into(),
                });
            }
        }
        return Ok(());
    }
    // Serving always synchronizes from an HTTPS publisher; without one, only
    // a local publication is verified.
    if !config.verify_only {
        return Err("transaction-detail serving requires an HTTPS publisher".into());
    }
    let directory = config
        .publication_dir
        .clone()
        .ok_or("--verify-only without a publisher needs --publication-dir")?;
    let service = ServiceConfig {
        cache_bytes: config.cache_bytes,
        build_slots: config.build_slots,
        query_slots: config.query_slots,
        max_waiters: config.query_waiters,
        max_body_bytes: config.body_bytes,
        upload_deadline: std::time::Duration::from_secs(config.upload_deadline_secs),
        query_deadline: std::time::Duration::from_secs(config.query_deadline_secs),
    };
    let runtime = DisplayRuntime::new(service, None);
    let set = DisplaySet::open_for_serving(&directory, config.retain_revisions, config.role)?;
    let map_sha256 = set.map_digest.clone();
    let state = DisplayState::build(set, &runtime)?;
    let set = state.set();
    println!(
        "{}",
        serde_json::json!({
            "verified": true,
            "role": config.role.as_str(),
            "map_sha256": map_sha256,
            "shards": set.map.shards.len(),
            "held": set
                .current()
                .iter()
                .filter(|r| serves(config.role, r.manifest.sealed))
                .map(|r| &r.digest)
                .collect::<Vec<_>>(),
            "warm_target": state.warm_target(),
        })
    );
    Ok(())
}
