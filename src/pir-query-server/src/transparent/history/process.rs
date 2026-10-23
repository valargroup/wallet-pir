//! Transparent history role: operator settings, publisher discovery, initial
//! synchronization with its retry rules, listener ownership and supervision,
//! and the local verification command. Shard tables, runtimes and handlers
//! live beside it in this module.
use crate::transparent::history::service::{ServiceConfig, ServiceState};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};
use transparent_shared::history::shardset::{
    LoadOptions, LoadScope, ShardSet, DEFAULT_RETAIN_REVISIONS,
};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

fn default_bootstrap_max_bytes() -> u64 {
    64 << 30
}
fn default_cache_bytes() -> u64 {
    8 << 30
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
fn default_listen() -> SocketAddr {
    "127.0.0.1:8092".parse().unwrap()
}
fn default_shard_dir() -> PathBuf {
    PathBuf::from("./transparent-shards")
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryProcess {
    #[serde(default)]
    pub publisher: Option<String>,
    #[serde(default)]
    pub archive_upstream: Option<String>,
    #[serde(default)]
    pub archive_ca: Option<PathBuf>,
    #[serde(default)]
    pub publisher_network: Option<String>,
    #[serde(default)]
    pub publisher_genesis: Option<String>,
    #[serde(default)]
    pub publisher_ca: Option<PathBuf>,
    #[serde(default = "default_bootstrap_max_bytes")]
    pub bootstrap_max_bytes: u64,
    #[serde(default)]
    pub full_holdings: bool,
    #[serde(default = "default_listen")]
    pub listen: SocketAddr,
    #[serde(default = "default_shard_dir")]
    pub shard_dir: PathBuf,
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
    pub retain_bytes: Option<u64>,
    #[serde(default)]
    pub prune_excess: bool,
    #[serde(default)]
    pub verify_only: bool,
}

fn certificates(path: Option<PathBuf>) -> Result<Vec<reqwest::Certificate>, Error> {
    Ok(match path {
        Some(path) => vec![reqwest::Certificate::from_pem(&std::fs::read(path)?)?],
        None => Vec::new(),
    })
}

impl HistoryProcess {
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

pub async fn run(config: HistoryProcess) -> Result<(), Error> {
    run_with(config, None, None, Default::default(), Default::default()).await
}

/// Serve on `reserved` (or a socket bound at `listen`), the provider listener
/// fleet routers and private probes reach. With `public`, the wallet routes
/// are also installed on the host's shared wallet listener once the first
/// checked publication is active. `stop` ends startup retries and serving
/// cooperatively: accepted requests and a started publication pass finish.
pub async fn run_with(
    config: HistoryProcess,
    mut reserved: Option<tokio::net::TcpListener>,
    public: Option<crate::public::Slot>,
    readiness: crate::readiness::Readiness,
    stop: pir_pipeline::service::Shutdown,
) -> Result<(), Error> {
    if let Some(publisher) = config.publisher.clone() {
        let provider = crate::transparent::routing::bootstrap::Config {
            publisher,
            network: config
                .publisher_network
                .clone()
                .ok_or("publisher network is required")?,
            genesis_hash: config
                .publisher_genesis
                .clone()
                .ok_or("publisher genesis is required")?,
            directory: config.shard_dir.clone(),
            cache_bytes: config.cache_bytes,
            max_download_bytes: config.bootstrap_max_bytes,
            roots: certificates(config.publisher_ca.clone())?,
        };
        if !config.verify_only || config.archive_upstream.is_some() || config.full_holdings {
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
                crate::transparent::routing::manager::Manager::new(provider, service).await?;
            if config.full_holdings {
                manager = manager.with_full_holdings();
            }
            if let Some(origin) = config.archive_upstream.clone() {
                manager = manager.with_archive(crate::transparent::routing::archive::Archive::new(
                    &origin,
                    certificates(config.archive_ca.clone())?,
                )?);
            }
            loop {
                match manager.poll_once().await {
                    Ok(_) => break,
                    Err(error) if crate::transparent::is_incompatible(&error) => return Err(error),
                    Err(error) if manager.live().is_some() => {
                        tracing::warn!(%error, "initial synchronization refused; recovered checked predecessor");
                        break;
                    }
                    Err(error) if config.verify_only => return Err(error),
                    Err(error) => {
                        tracing::warn!(%error, "initial synchronization refused; retrying with checked inputs");
                        if !crate::supervision::retry_after(
                            std::time::Duration::from_secs(1),
                            &stop,
                        )
                        .await
                        {
                            return Ok(());
                        }
                    }
                }
            }
            if config.verify_only {
                let live = manager.live().ok_or("provider did not activate")?;
                println!(
                    "{}",
                    serde_json::to_string(
                        &live
                            .command(crate::transparent::history::live::Command::Status)
                            .await?
                    )?
                );
                return Ok(());
            }
            let app = manager.router()?;
            let live = manager.live().ok_or("provider did not activate")?;
            readiness.register(move || std::future::ready(live.ready()));
            if let Some(public) = public {
                public.install(app.clone());
            }
            let listener = match reserved.take() {
                Some(listener) => listener,
                None => tokio::net::TcpListener::bind(config.listen).await?,
            };
            let stopped = stop.clone();
            return crate::supervision::supervise(
                "history",
                listener,
                app,
                manager.run_until(async move { stopped.cancelled().await }),
                stop,
            )
            .await;
        }
        let report = crate::transparent::routing::bootstrap::verify(provider).await?;
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }
    // Serving always synchronizes from an HTTPS publisher; without one, only
    // a local shard directory is verified.
    if !config.verify_only {
        return Err("history serving requires an HTTPS publisher".into());
    }
    let options = LoadOptions {
        retain_revisions: config.retain_revisions,
        retain_bytes: config.retain_bytes,
        prune_excess: config.prune_excess,
        scope: LoadScope::Whole,
    };
    let set = ShardSet::open_for_serving(&config.shard_dir, &options)?;
    tracing::info!(
        shards = set.len(),
        revisions = set.revisions().len(),
        prunable = set.prunable().len(),
        geometries = ?set.geometries().iter().map(|g| g.name).collect::<Vec<_>>(),
        map_sha256 = set.map_digest,
        start = set.map.start_height,
        covered_through = set.map.shards.last().map(|s| s.end_height),
        "loaded shard set"
    );
    for prunable in set.prunable() {
        tracing::warn!(
            shard = prunable.shard_id,
            revision = prunable.revision,
            bytes = prunable.bytes,
            path = %prunable.path.display(),
            "superseded revision past the retention bound; prune it"
        );
    }
    let service = ServiceConfig {
        cache_bytes: config.cache_bytes,
        build_slots: config.build_slots,
        query_slots: config.query_slots,
        max_waiters: config.query_waiters,
        max_body_bytes: config.body_bytes,
        upload_deadline: std::time::Duration::from_secs(config.upload_deadline_secs),
        query_deadline: std::time::Duration::from_secs(config.query_deadline_secs),
    };
    ServiceState::build(set, service)?;
    println!("{}", serde_json::json!({"verified":true}));
    Ok(())
}
