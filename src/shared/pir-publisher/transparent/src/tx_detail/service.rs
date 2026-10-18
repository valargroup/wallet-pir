//! Live transaction-detail publisher application, independent of CLI and process signals.
use super::{
    controller::{self, Controller, Outcome, Settings},
    serving::WorkersFile,
    source::{open_writer, LiveSource, Source},
};
use crate::history::publication::BoxError;
use pir_pipeline::service::{Http, Shutdown, Tasks};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub root: PathBuf,
    pub journal: PathBuf,
    pub workers: PathBuf,
    pub rpc_url: String,
    pub rpc_cookie: PathBuf,
    pub poll_ms: u64,
    pub status_listen: SocketAddr,
    #[serde(default)]
    pub trust_sealed: bool,
    #[serde(default)]
    pub ship_runtimes: bool,
    pub max_recent_records: Option<u64>,
}
impl Config {
    pub fn validate(&self) -> Result<(), BoxError> {
        if self.poll_ms == 0 {
            return Err("transaction-detail polling must be nonzero".into());
        }
        Ok(())
    }
}
pub async fn run(config: Config, http: Http<axum::Router>, stop: Shutdown) -> Result<(), BoxError> {
    config.validate()?;
    let workers = WorkersFile::load(&config.workers)?.workers;
    let store = open_writer(&config.journal)?;
    let rpc = crate::ingest::zakura::connect(&config.rpc_url, &config.rpc_cookie)?;
    if rpc.block_hash_display(0).await? != store.genesis_hash() {
        return Err("the node's genesis differs from the journal's".into());
    }
    let source = Source::Live(LiveSource::new(rpc, Duration::from_millis(config.poll_ms)));
    let settings = Settings {
        trust_sealed: config.trust_sealed,
        ship_runtimes: config.ship_runtimes.then_some(
            transparent_shared::tx_detail::prebuild::build_shipped as controller::Prebuild,
        ),
        max_recent_records: config.max_recent_records,
        ..Settings::default()
    };
    let status = settings.status.clone();
    let metrics = settings.metrics.clone();
    let mut owner = Tasks::default();
    let mut server = tokio::spawn(http.serve(
        config.status_listen,
        controller::status_router(status.clone(), metrics),
    ));
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
    match controller.run_until(stop.clone()).await? {
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
