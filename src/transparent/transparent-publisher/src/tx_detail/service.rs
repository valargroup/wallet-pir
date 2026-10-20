//! Live transaction-detail publisher application, independent of CLI and process signals.
use super::{
    controller::{self, Controller, Outcome, Settings},
    serving::WorkersFile,
    source::{open_writer, LiveSource, ReplaySource, Source},
};
use crate::history::publication::BoxError;
use pir_pipeline::service::{Http, Shutdown, Tasks};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    #[default]
    Live,
    Replay,
    ReplayThenLive,
}
fn default_rpc_url() -> String {
    "http://127.0.0.1:8232".into()
}
fn default_blocks_per_step() -> u64 {
    25
}
fn default_step_interval_ms() -> u64 {
    2000
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub mode: Mode,
    #[serde(default = "default_blocks_per_step")]
    pub blocks_per_step: u64,
    #[serde(default = "default_step_interval_ms")]
    pub step_interval_ms: u64,
    #[serde(default)]
    pub replay_end: Option<u64>,
    #[serde(default)]
    pub exit_when_idle: bool,
    pub root: PathBuf,
    pub journal: PathBuf,
    #[serde(default)]
    pub workers: PathBuf,
    #[serde(default = "default_rpc_url")]
    pub rpc_url: String,
    #[serde(default)]
    pub rpc_cookie: PathBuf,
    pub poll_ms: u64,
    pub status_listen: SocketAddr,
    #[serde(default)]
    pub trust_sealed: bool,
    #[serde(default)]
    pub ship_runtimes: bool,
    /// Publish all prepared holdings without controlling query servers.
    #[serde(default)]
    pub http_only: bool,
    pub max_recent_records: Option<u64>,
}
impl Config {
    pub fn validate(&self) -> Result<(), BoxError> {
        if !matches!(self.mode, Mode::Replay) && self.rpc_cookie.as_os_str().is_empty() {
            return Err("live display publication requires rpc_cookie".into());
        }
        if self.http_only && !matches!(self.mode, Mode::Live) {
            return Err("HTTP publication requires a live canonicality source".into());
        }
        if self.blocks_per_step == 0 || self.step_interval_ms == 0 {
            return Err("display replay intervals must be positive".into());
        }
        if self.http_only && !self.ship_runtimes {
            return Err("display HTTP publishing requires shipped runtimes".into());
        }
        if !self.http_only && self.workers.as_os_str().is_empty() {
            return Err("display fleet mode requires workers".into());
        }
        if self.poll_ms == 0 {
            return Err("transaction-detail polling must be nonzero".into());
        }
        Ok(())
    }
}
pub async fn run(config: Config, http: Http<axum::Router>, stop: Shutdown) -> Result<(), BoxError> {
    config.validate()?;
    let workers = if config.http_only {
        Vec::new()
    } else {
        WorkersFile::load(&config.workers)?.workers
    };
    let store = open_writer(&config.journal)?;
    let rpc = if matches!(config.mode, Mode::Replay) {
        None
    } else {
        let rpc = crate::ingest::zakura::connect(&config.rpc_url, &config.rpc_cookie)?;
        if rpc.block_hash_display(0).await? != store.genesis_hash() {
            return Err("the node's genesis differs from the journal's".into());
        }
        Some(rpc)
    };
    let distribution = if config.http_only {
        Some(super::distribution::Distribution::new(
            &config.root,
            rpc.clone().ok_or("HTTP publication requires live RPC")?,
        )?)
    } else {
        None
    };
    let live = rpc.map(|rpc| LiveSource::new(rpc, Duration::from_millis(config.poll_ms)));
    let source = match config.mode {
        Mode::Live => Source::Live(live.ok_or("live mode requires RPC")?),
        Mode::Replay | Mode::ReplayThenLive => {
            let end = store.covered_through().ok_or("the journal is empty")?;
            Source::Replay(
                ReplaySource::new(
                    config.blocks_per_step,
                    Duration::from_millis(config.step_interval_ms),
                    config.replay_end.unwrap_or(end).min(end),
                ),
                live,
            )
        }
    };
    let settings = Settings {
        distribution: distribution.clone(),
        trust_sealed: config.trust_sealed,
        ship_runtimes: config.ship_runtimes.then_some(
            transparent_shared::tx_detail::prebuild::build_shipped as controller::Prebuild,
        ),
        max_recent_records: config.max_recent_records,
        exit_when_idle: config.exit_when_idle,
        ..Settings::default()
    };
    let status = settings.status.clone();
    let metrics = settings.metrics.clone();
    let mut owner = Tasks::default();
    let mut server = tokio::spawn(http.serve(config.status_listen, {
        let router = controller::status_router(status.clone(), metrics);
        match &distribution {
            Some(distribution) => router.merge(distribution.router()),
            None => router,
        }
    }));
    owner.own(&server);
    let mut controller = Controller::open(&config.root, store, source, workers, settings)?;
    // Listener failure asks for a boundary stop rather than cancelling activation.
    let watch_stop = stop.clone();
    let watcher = tokio::spawn(async move {
        let result = (&mut server).await;
        watch_stop.request();
        result
    });
    owner.own(&watcher);
    let result = controller.run_until(stop.clone()).await;
    if let Some(distribution) = &distribution {
        if !matches!(&result, Ok(Outcome::Idle)) {
            distribution.withdraw(0)?;
        }
    }
    match result? {
        Outcome::Idle => {}
        Outcome::Halted(reason) => {
            tracing::error!(%reason, "transaction-detail publisher halted");
            // Preserve the durable halt and the status endpoint until operator shutdown.
            stop.cancelled().await;
        }
    }
    if watcher.is_finished() {
        return Err("transaction-detail status listener stopped".into());
    }
    Ok(())
}
