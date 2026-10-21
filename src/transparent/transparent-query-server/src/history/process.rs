//! History worker process. The `transparent-query-server` binary parses arguments
//! and calls [`run`]; the query-server host uses the same function.
use super::service::{router, ServiceConfig, ServiceState};
use serde::Deserialize;
use std::{future::IntoFuture, net::SocketAddr, path::PathBuf, sync::Arc};
use tower_http::limit::RequestBodyLimitLayer;
use transparent_shared::history::assignment::Assignment;
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
    #[serde(default)]
    pub runtime_cache_dir: Option<PathBuf>,
    #[serde(default)]
    pub runtime_cache_max_bytes: Option<u64>,
    #[serde(default = "default_restore_slots")]
    pub runtime_restore_slots: u16,
    #[serde(default)]
    pub runtime_cache_prune_set: Vec<PathBuf>,
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
    pub assignment: Option<PathBuf>,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub retain_bytes: Option<u64>,
    #[serde(default)]
    pub prune_excess: bool,
    #[serde(default)]
    pub verify_only: bool,
    #[serde(default)]
    pub control_socket: Option<PathBuf>,
    #[serde(default)]
    pub active_record: Option<PathBuf>,
}

fn certificates(path: Option<PathBuf>) -> Result<Vec<reqwest::Certificate>, Error> {
    Ok(match path {
        Some(path) => vec![reqwest::Certificate::from_pem(&std::fs::read(path)?)?],
        None => Vec::new(),
    })
}

impl HistoryProcess {
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

pub async fn run(config: HistoryProcess) -> Result<(), Error> {
    run_with(config, None).await
}

pub async fn run_with(
    mut config: HistoryProcess,
    mut reserved: Option<tokio::net::TcpListener>,
) -> Result<(), Error> {
    if let Some(publisher) = config.publisher.clone() {
        if config.control_socket.is_some()
            || config.assignment.is_some()
            || config.runtime_cache_dir.is_some()
        {
            return Err("HTTPS bootstrap verification requires its own directory and no fleet/cache configuration".into());
        }
        let provider = crate::routing::bootstrap::Config {
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
            let mut manager = crate::routing::manager::Manager::new(provider, service).await?;
            if config.full_holdings {
                manager = manager.with_full_holdings();
            }
            if let Some(origin) = config.archive_upstream.clone() {
                manager = manager.with_archive(crate::routing::archive::Archive::new(
                    &origin,
                    certificates(config.archive_ca.clone())?,
                )?);
            }
            loop {
                match manager.poll_once().await {
                    Ok(_) => break,
                    Err(error) if crate::source::is_incompatible(&error) => return Err(error),
                    Err(error) if manager.live().is_some() => {
                        tracing::warn!(%error, "initial synchronization refused; recovered checked predecessor");
                        break;
                    }
                    Err(error) if config.verify_only => return Err(error),
                    Err(error) => {
                        tracing::warn!(%error, "initial synchronization refused; retrying with checked inputs");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
            if config.verify_only {
                let live = manager.live().ok_or("provider did not activate")?;
                println!(
                    "{}",
                    serde_json::to_string(
                        &live.command(crate::history::live::Command::Status).await?
                    )?
                );
                return Ok(());
            }
            let app = manager.router()?;
            let listener = match reserved.take() {
                Some(listener) => listener,
                None => tokio::net::TcpListener::bind(config.listen).await?,
            };
            let mut polling = tokio::spawn(manager.run());
            // Supervise synchronization: a terminal error stops wallet serving
            // and is returned with its original type.
            tokio::select! {
                result = axum::serve(listener, app).into_future() => {
                    polling.abort();
                    result?;
                }
                result = &mut polling => {
                    return Err(match result {
                        Ok(error) => error,
                        Err(join) => format!("history synchronization stopped: {join}").into(),
                    });
                }
            }
            return Ok(());
        }
        let report = crate::routing::bootstrap::verify(provider).await?;
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }
    let active_record = config
        .active_record
        .clone()
        .unwrap_or_else(|| config.shard_dir.with_extension("active.json"));
    let mut runtimes = None;
    if config.control_socket.is_some() && active_record.exists() {
        let active: crate::history::live::Publication =
            serde_json::from_slice(&std::fs::read(&active_record)?)?;
        config.shard_dir = active.directory;
        config.assignment = active.assignment;
        runtimes = active.runtimes;
    }
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
    if !config.runtime_cache_prune_set.is_empty() {
        let mut keep = std::collections::HashSet::new();
        for set in &config.runtime_cache_prune_set {
            keep.extend(transparent_shared::runtime::disk::retained_digests(set)?);
        }
        let freed = disk
            .as_ref()
            .ok_or("runtime cache prune requires a cache directory")?
            .prune(&keep)?;
        println!("runtime cache pruned: {freed} bytes freed");
        return Ok(());
    }
    let scope = match (&config.assignment, &config.worker_id) {
        (Some(path), Some(worker_id)) => LoadScope::Assigned {
            assignment: Arc::new(Assignment::load(path)?),
            worker_id: worker_id.clone(),
        },
        _ => LoadScope::Whole,
    };
    let options = LoadOptions {
        retain_revisions: config.retain_revisions,
        retain_bytes: config.retain_bytes,
        prune_excess: config.prune_excess,
        scope,
    };
    let set = ShardSet::open_for_serving(&config.shard_dir, &options)?;
    tracing::info!(
        shards = set.len(),
        assigned = set.assigned_len(),
        revisions = set.revisions().len(),
        prunable = set.prunable().len(),
        geometries = ?set.geometries().iter().map(|g| g.name).collect::<Vec<_>>(),
        map_sha256 = set.map_digest,
        worker_id = set.scope().map(|scope| scope.worker_id.as_str()),
        assignment_sha256 = set.scope().map(|scope| scope.assignment_sha256.as_str()),
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
    if config.verify_only {
        let report = disk.as_ref().map(|disk| disk.check_set(&set)).transpose()?;
        ServiceState::build(set, service)?;
        println!(
            "{}",
            serde_json::json!({"verified":true,"runtime_cache":report})
        );
        return Ok(());
    }
    let map_sha256 = set.map_digest.clone();
    let state = ServiceState::build_with_runtimes(
        set,
        service,
        disk,
        runtimes
            .clone()
            .map(transparent_shared::runtime::disk::ShippedRuntimes::new),
    )?;
    let limit = state.max_query_bytes();
    let app = if let Some(socket) = config.control_socket.clone() {
        let live = crate::history::live::LiveService::new(
            state.clone(),
            crate::history::live::Publication {
                directory: config.shard_dir.clone(),
                assignment: config.assignment.clone(),
                map_sha256,
                runtimes,
            },
            service,
            options,
            active_record,
        )?;
        let app = live.router();
        tokio::spawn(async move {
            if let Err(error) = live.listen(&socket).await {
                tracing::error!(%error, "control socket failed");
                std::process::exit(1);
            }
        });
        app
    } else {
        router(state.clone())
    }
    .layer(RequestBodyLimitLayer::new(limit));
    let listener = match reserved.take() {
        Some(listener) => listener,
        None => tokio::net::TcpListener::bind(config.listen).await?,
    };
    tracing::info!(listen = %config.listen, max_query_bytes = limit, "serving");
    let _prewarm = state.spawn_prewarm();
    drop(state);
    axum::serve(listener, app).await?;
    Ok(())
}
