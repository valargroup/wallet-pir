//! Enhance coordinator configuration. The hosts run it: `pir-publisher` owns
//! listeners, tasks and the `enhance-pir-server` command line.
use crate::{
    application::{load_inventory, Source},
    control::PlacementPolicy,
    coordinator::Coordinator,
    ingest::EnhanceJournal,
    zakura::ZakuraClient,
};
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

/// Durable state opened for a host: staged rebuilds are adopted and control
/// state is open, but no listener, task or chain source exists yet. The host
/// holds the data-directory lock for the lifetime of this value.
pub struct Opened {
    pub coordinator: Coordinator,
    pub listen: SocketAddr,
    pub artifact_listen: Option<SocketAddr>,
    source: SourceConfig,
}

struct SourceConfig {
    application: crate::application::Config,
    data_dir: PathBuf,
    zakura_rpc_url: String,
    zakura_cookie: Option<PathBuf>,
    fixture_records: u64,
    fixture_append_records: u64,
    prevout_cache_outputs: usize,
}

/// Validate the configuration, bind the data directory to one source mode,
/// adopt or reject a staged journal rebuild and open control state. Runs before
/// any listener so serving resumes on the adopted journal. A rejected rebuild
/// never stops startup.
pub fn open(config: Config) -> Result<Opened> {
    config.validate()?;
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
    if status_config.is_some() {
        return Err("configure Status as its own process".into());
    }
    Ok(Opened {
        coordinator,
        listen,
        artifact_listen,
        source: SourceConfig {
            application: crate::application::Config {
                worker_config,
                placement_policy,
                pool_placement,
                pool_policy,
                frontier_replicas,
                capacity_policy,
                poll: Duration::from_secs(poll_seconds),
            },
            data_dir,
            zakura_rpc_url,
            zakura_cookie,
            fixture_records,
            fixture_append_records,
            prevout_cache_outputs,
        },
    })
}

impl Opened {
    /// Open the record journal and the configured chain source after serving
    /// starts; chain catch-up then stays in the poll loop.
    pub fn into_source(
        self,
    ) -> Result<(
        Coordinator,
        crate::application::Config,
        EnhanceJournal,
        Source,
    )> {
        let Self {
            coordinator,
            source,
            ..
        } = self;
        let journal = EnhanceJournal::open(&source.data_dir)?;
        let rpc = source
            .zakura_cookie
            .map(|cookie| ZakuraClient::from_cookie_file(&source.zakura_rpc_url, &cookie))
            .transpose()?;
        let prevouts = crate::zakura::Prevouts::new(source.prevout_cache_outputs);
        let source_mode = match rpc {
            Some(rpc) => Source::Canonical {
                rpc,
                prevouts: Box::new(prevouts),
            },
            None => Source::Fixture {
                records: source.fixture_records,
                append_records: source.fixture_append_records,
            },
        };
        Ok((coordinator, source.application, journal, source_mode))
    }
}
