//! Live transaction-detail publisher application, independent of CLI and process signals.
use super::{
    controller::{self, Controller, Outcome, Settings},
    source::{open_writer, LiveSource, Source},
};
use crate::history::publication::BoxError;
use pir_pipeline::service::{Http, Shutdown, Tasks};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
fn default_rpc_url() -> String {
    "http://127.0.0.1:8232".into()
}
/// Live publication of every prepared holding over HTTPS.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub root: PathBuf,
    pub journal: PathBuf,
    #[serde(default = "default_rpc_url")]
    pub rpc_url: String,
    pub rpc_cookie: PathBuf,
    pub poll_ms: u64,
    pub status_listen: SocketAddr,
    #[serde(default)]
    pub trust_sealed: bool,
    pub max_recent_records: Option<u64>,
}
impl Config {
    pub fn validate(&self) -> Result<(), BoxError> {
        if self.rpc_cookie.as_os_str().is_empty() {
            return Err("live display publication requires rpc_cookie".into());
        }
        if self.poll_ms == 0 {
            return Err("transaction-detail polling must be nonzero".into());
        }
        Ok(())
    }
}
pub async fn run(config: Config, http: Http<axum::Router>, stop: Shutdown) -> Result<(), BoxError> {
    config.validate()?;
    let store = open_writer(&config.journal)?;
    let rpc = crate::ingest::zakura::connect(&config.rpc_url, &config.rpc_cookie)?;
    if rpc.block_hash_display(0).await? != store.genesis_hash() {
        return Err("the node's genesis differs from the journal's".into());
    }
    let distribution = super::distribution::Distribution::new(&config.root, rpc.clone())?;
    let source = Source::Live(LiveSource::new(rpc, Duration::from_millis(config.poll_ms)));
    let settings = Settings {
        distribution: Some(distribution.clone()),
        trust_sealed: config.trust_sealed,
        max_recent_records: config.max_recent_records,
        ..Settings::default()
    };
    let status = settings.status.clone();
    let metrics = settings.metrics.clone();
    let mut owner = Tasks::default();
    let mut server = tokio::spawn(http.serve(config.status_listen, {
        controller::status_router(status.clone(), metrics).merge(distribution.router())
    }));
    owner.own(&server);
    let mut controller = Controller::open(&config.root, store, source, settings)?;
    // Listener failure asks for a boundary stop rather than cancelling activation.
    let watch_stop = stop.clone();
    let watcher = tokio::spawn(async move {
        let result = (&mut server).await;
        watch_stop.request();
        result
    });
    owner.own(&watcher);
    let result = controller.run_until(stop.clone()).await;
    if !matches!(&result, Ok(Outcome::Idle)) {
        distribution.withdraw(0)?;
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
