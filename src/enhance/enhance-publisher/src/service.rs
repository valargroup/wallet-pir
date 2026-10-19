//! Enhance coordinator application, shared by the legacy command and unified host.
use crate::{
    application::load_inventory, control::PlacementPolicy, coordinator::Coordinator,
    ingest::EnhanceJournal, zakura::ZakuraClient,
};
use pir_pipeline::service::{Http, Shutdown, Tasks};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
type Result<T, E = Box<dyn std::error::Error + Send + Sync>> = std::result::Result<T, E>;
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub status_config: Option<PathBuf>,
    #[serde(default)]
    pub packing_router_config: Option<PathBuf>,
    #[serde(default)]
    pub query_ingress: Vec<String>,
    #[serde(default)]
    pub pool_placement: bool,
    pub frontier_replicas: usize,
    #[serde(default)]
    pub pool_policy: Option<PathBuf>,
    #[serde(default)]
    pub artifact_listen: Option<SocketAddr>,
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub worker_config: PathBuf,
    pub zakura_rpc_url: String,
    #[serde(default)]
    pub zakura_cookie: Option<PathBuf>,
    #[serde(default)]
    pub isolated_fixture: bool,
    pub fixture_records: u64,
    pub fixture_append_records: u64,
    pub poll_seconds: u64,
    pub prevout_cache_outputs: usize,
    pub capacity_fallback_rows_per_second: f64,
    pub capacity_readiness_seconds: f64,
    pub capacity_burst_rows: u64,
    pub sealed_shards: usize,
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        PlacementPolicy {
            sealed_shards: self.sealed_shards,
        }
        .validate()?;
        crate::capacity::Policy {
            fallback_rows_per_second: self.capacity_fallback_rows_per_second,
            readiness_seconds: self.capacity_readiness_seconds,
            burst_rows: self.capacity_burst_rows,
        }
        .validate()?;
        if self.poll_seconds == 0 || self.isolated_fixture == self.zakura_cookie.is_some() {
            return Err(
                "select either canonical credentials or an isolated fixture and nonzero polling"
                    .into(),
            );
        }
        Ok(())
    }
}
pub async fn run(config: Config, http: Http<axum::Router>, shutdown: Shutdown) -> Result<()> {
    config.validate()?;
    let _lock = pir_pipeline::service::lock(&config.data_dir)?;
    let mut tasks = Tasks::default();
    let failure = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let placement_policy = PlacementPolicy {
        sealed_shards: config.sealed_shards,
    };
    let Config {
        status_config,
        packing_router_config,
        query_ingress,
        pool_placement,
        frontier_replicas,
        pool_policy,
        artifact_listen,
        listen,
        data_dir,
        worker_config,
        zakura_rpc_url,
        zakura_cookie,
        isolated_fixture,
        fixture_records,
        fixture_append_records,
        poll_seconds,
        prevout_cache_outputs,
        capacity_fallback_rows_per_second,
        capacity_readiness_seconds,
        capacity_burst_rows,
        sealed_shards: _,
    } = config;
    let capacity_policy = crate::capacity::Policy {
        fallback_rows_per_second: capacity_fallback_rows_per_second,
        readiness_seconds: capacity_readiness_seconds,
        burst_rows: capacity_burst_rows,
    };
    capacity_policy.validate()?;
    if poll_seconds == 0
        || (isolated_fixture && zakura_cookie.is_some())
        || (!isolated_fixture && zakura_cookie.is_none())
    {
        return Err(
            "select either a canonical RPC cookie or an isolated fixture; polling must be nonzero"
                .into(),
        );
    }
    let mut groups = load_inventory(&worker_config)?;
    for group in &mut groups {
        group.placement_policy = placement_policy;
    }
    std::fs::create_dir_all(&data_dir)?;
    let mode_path = data_dir.join("source-mode");
    let mode = if isolated_fixture {
        "synthetic-fixture"
    } else {
        "canonical-archive"
    };
    if mode_path.exists() {
        if std::fs::read_to_string(&mode_path)? != mode {
            return Err("cannot mix fixture and canonical data directories".into());
        }
    } else {
        std::fs::write(mode_path, mode)?;
    }
    // Before control state, the HTTP listener and the journal open, so
    // serving resumes on the adopted journal. Verification streams both
    // journals; chain catch-up stays in the poll loop after serving
    // starts. A rejected rebuild never stops startup.
    match crate::rebuild::adopt_staged(&data_dir)? {
        crate::rebuild::Adoption::None => {}
        crate::rebuild::Adoption::Adopted {
            receipt_id,
            height,
            changed_records,
            previous,
        } => tracing::info!(
            %receipt_id, height, changed_records, previous = %previous.display(),
            "adopted staged Enhance journal rebuild"
        ),
        crate::rebuild::Adoption::Rejected { reason, moved_to } => {
            tracing::error!(
                %reason, moved_to = %moved_to.display(),
                "rejected staged Enhance journal rebuild; serving the live journal"
            )
        }
    }
    let routers = packing_router_config
        .map(
            |path| -> Result<
                Vec<enhance_query_server::packing_router::RouterRegistration>,
                Box<dyn std::error::Error + Send + Sync>,
            > { Ok(serde_json::from_slice(&std::fs::read(path)?)?) },
        )
        .transpose()?
        .unwrap_or_default();
    if !routers.is_empty() && artifact_listen.is_none() {
        return Err("remote packing requires a private --artifact-listen".into());
    }
    let coordinator =
        Coordinator::open_with_serving(&data_dir.join("control"), groups, routers, query_ingress)?;
    if let Some(listen) = artifact_listen {
        let artifacts = coordinator.artifact_router();
        let artifact = tokio::spawn(http.serve(listen, artifacts));
        tasks.own(&artifact);
        let failed = failure.clone();
        let stop = shutdown.clone();
        let watch = tokio::spawn(async move {
            let _ = artifact.await;
            failed.store(true, std::sync::atomic::Ordering::SeqCst);
            stop.request();
        });
        tasks.own(&watch);
        let heartbeat = coordinator.clone();
        let heartbeat_task = tokio::spawn(async move {
            loop {
                heartbeat.refresh_routers().await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        tasks.own(&heartbeat_task);
    }
    if status_config.is_some() {
        return Err("configure Status as its own process".into());
    }
    let serving = coordinator.clone();
    let routes = serving.router();
    let task = tokio::spawn(http.serve(listen, routes));
    tasks.own(&task);
    let journal = EnhanceJournal::open(&data_dir)?;
    let rpc = zakura_cookie
        .map(|cookie| ZakuraClient::from_cookie_file(&zakura_rpc_url, &cookie))
        .transpose()?;
    let prevouts = crate::zakura::Prevouts::new(prevout_cache_outputs);
    crate::application::run_until(
        crate::application::Config {
            worker_config,
            placement_policy,
            pool_placement,
            pool_policy,
            frontier_replicas,
            capacity_policy,
            poll: Duration::from_secs(poll_seconds),
        },
        coordinator,
        journal,
        match rpc {
            Some(rpc) => crate::application::Source::Canonical {
                rpc,
                prevouts: Box::new(prevouts),
            },
            None => crate::application::Source::Fixture {
                records: fixture_records,
                append_records: fixture_append_records,
            },
        },
        task,
        shutdown,
    )
    .await?;
    if failure.load(std::sync::atomic::Ordering::SeqCst) {
        return Err("Enhance artifact listener stopped".into());
    }
    Ok(())
}
