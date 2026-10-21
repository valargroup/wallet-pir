//! Transaction-detail worker process. `transparent-txid-server` parses arguments
//! and calls [`run`]; the query-server host uses the same function.
use super::live::{DisplayLive, DisplayPublication};
use super::service::{body_limit, router, DisplayRuntime, DisplayState};
use crate::config::{ReadinessMode, ServiceConfig};
use serde::Deserialize;
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::path::PathBuf;
use tower_http::limit::RequestBodyLimitLayer;
use transparent_shared::history::shardset::DEFAULT_RETAIN_REVISIONS;
use transparent_shared::tx_detail::set::DisplaySet;
use transparent_shared::tx_detail::DisplayRole;
use transparent_shared::tx_detail::{parse_role, serves};

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
fn default_restore_slots() -> u16 {
    4
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
    #[serde(default)]
    pub runtime_cache_dir: Option<PathBuf>,
    #[serde(default)]
    pub runtime_cache_max_bytes: Option<u64>,
    #[serde(default = "default_restore_slots")]
    pub runtime_restore_slots: u16,
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
    pub loaded_only: bool,
    #[serde(default)]
    pub verify_only: bool,
    #[serde(default)]
    pub control_socket: Option<PathBuf>,
    #[serde(default)]
    pub active_record: Option<PathBuf>,
    #[serde(default)]
    pub collect_root: Vec<PathBuf>,
}

fn certificates(path: Option<PathBuf>) -> Result<Vec<reqwest::Certificate>, Error> {
    Ok(match path {
        Some(path) => vec![reqwest::Certificate::from_pem(&std::fs::read(path)?)?],
        None => Vec::new(),
    })
}

impl DetailProcess {
    /// For an HTTPS consumer, check the publisher's service discovery using
    /// the configured origin and CA. Opens no state; a legacy role is a no-op.
    pub async fn discover(&self) -> Result<(), Error> {
        match &self.publisher {
            Some(publisher) => {
                crate::source::discover_publisher(
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
    run_with(config, None).await
}

pub async fn run_with(
    config: DetailProcess,
    mut reserved: Option<tokio::net::TcpListener>,
) -> Result<(), Error> {
    if let Some(publisher) = config.publisher.clone() {
        if config.role != DisplayRole::Combined {
            return Err("managed display prototype requires combined holdings".into());
        }
        let bootstrap = crate::source::Config {
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
            readiness: ReadinessMode::Warm,
        };
        let mut manager = crate::tx_detail::manager::Manager::new(bootstrap, service).await?;
        loop {
            match manager.poll_once().await {
                Ok(_) => break,
                Err(error) if crate::source::is_incompatible(&error) => return Err(error),
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
    let mut directory = config.publication_dir.clone();
    if let Some(record) = config.active_record.as_ref().filter(|r| r.exists()) {
        let active: DisplayPublication = serde_json::from_slice(&std::fs::read(record)?)?;
        directory = Some(active.directory);
    }
    let service = ServiceConfig {
        cache_bytes: config.cache_bytes,
        build_slots: config.build_slots,
        query_slots: config.query_slots,
        max_waiters: config.query_waiters,
        max_body_bytes: config.body_bytes,
        upload_deadline: std::time::Duration::from_secs(config.upload_deadline_secs),
        query_deadline: std::time::Duration::from_secs(config.query_deadline_secs),
        readiness: if config.loaded_only {
            ReadinessMode::LoadedOnly
        } else {
            ReadinessMode::Warm
        },
    };
    let disk = config
        .runtime_cache_dir
        .clone()
        .map(|dir| {
            let mut disk = transparent_shared::runtime::disk::DiskCache::new(
                dir,
                config
                    .runtime_cache_max_bytes
                    .unwrap_or(config.cache_bytes.saturating_mul(2)),
            )?;
            disk.restore_slots = usize::from(config.runtime_restore_slots);
            Ok::<_, std::io::Error>(disk)
        })
        .transpose()?;
    let runtime = DisplayRuntime::new(service, disk);
    let initial = match &directory {
        Some(dir) => {
            let set = DisplaySet::open(dir, config.retain_revisions, config.role)?;
            tracing::info!(
                role = config.role.as_str(),
                map_sha256 = set.map_digest,
                shards = set.map.shards.len(),
                held = set.current().iter().filter(|r| r.held()).count(),
                retained = set.revisions().len() - set.map.shards.len(),
                start = set.map.start_height,
                covered_through = set.map.covered_through(),
                "loaded display publication"
            );
            let publication = DisplayPublication {
                directory: dir.clone(),
                map_sha256: set.map_digest.clone(),
            };
            Some((DisplayState::build(set, &runtime)?, publication))
        }
        None => None,
    };
    if config.verify_only {
        let Some((state, publication)) = &initial else {
            return Err("--verify-only needs --publication-dir or an active record".into());
        };
        let set = state.set();
        println!(
            "{}",
            serde_json::json!({
                "verified": true,
                "role": config.role.as_str(),
                "map_sha256": publication.map_sha256,
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
        return Ok(());
    }
    let prewarm = initial.as_ref().map(|(state, _)| state.spawn_prewarm());
    let app = match config.control_socket.clone() {
        Some(socket) => {
            let live = DisplayLive::new(
                runtime,
                config.role,
                config.retain_revisions,
                config
                    .active_record
                    .clone()
                    .ok_or("a control socket requires an active record")?,
                config.collect_root.clone(),
                initial,
            )?;
            let app = live.router();
            tokio::spawn(async move {
                if let Err(error) = live.listen(&socket).await {
                    tracing::error!(%error, "control socket failed");
                    std::process::exit(1);
                }
            });
            app
        }
        None => {
            let Some((state, _)) = initial else {
                return Err("serving without a control socket needs --publication-dir".into());
            };
            router(state)
        }
    }
    .layer(RequestBodyLimitLayer::new(body_limit()));
    let listener = match reserved.take() {
        Some(listener) => listener,
        None => tokio::net::TcpListener::bind(config.listen).await?,
    };
    tracing::info!(listen = %config.listen, role = config.role.as_str(), "serving txid display");
    let _prewarm = prewarm;
    axum::serve(listener, app).await?;
    Ok(())
}

pub fn role_from_text(text: &str) -> Result<DisplayRole, String> {
    parse_role(text)
        .ok_or_else(|| format!("{text} is not recent-replica, archive-owner or combined"))
}
