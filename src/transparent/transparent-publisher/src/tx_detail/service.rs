//! Live transaction-detail publisher application, independent of CLI and process signals.
use super::{
    controller::{self, Controller, Outcome, Settings},
    ingestion::DetailIngestion,
    source::{open_writer, CommittedSource, Source},
};
use crate::ingest::zakura::NodeClient;
use crate::BoxError;
use pir_pipeline::ingestion::{ChainFeed, IngestionExecutor};
use pir_pipeline::service::{Http, Shutdown, Tasks};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};
use transparent_shared::journal::events::EventStore;
/// Live publication of every prepared holding over HTTPS. The node and its
/// polling come from the root chain settings.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub root: PathBuf,
    pub journal: PathBuf,
    pub status_listen: SocketAddr,
    pub max_recent_records: Option<u64>,
}
pub async fn run<E: pir_pipeline::publisher::PublisherExecutor + IngestionExecutor>(
    config: Config,
    rpc: NodeClient,
    feed: ChainFeed,
    executor: E,
    http: Http<axum::Router>,
    stop: Shutdown,
) -> Result<(), BoxError> {
    crate::storage::layout::require(&config.root, "transaction_details")?;
    // The writer recovers any uncommitted tail before the controller's
    // read-only view opens the same journal in place.
    let writer = open_writer(&config.journal)?;
    if rpc.block_hash_display(0).await? != writer.genesis_hash() {
        return Err("the node's genesis differs from the journal's".into());
    }
    let store = EventStore::open_existing(&config.journal)?;
    let distribution = super::distribution::Distribution::new(&config.root, rpc.clone())?;
    let view = std::sync::Arc::default();
    let (notify, updates) = tokio::sync::watch::channel(0u64);
    let source = Source::Committed(CommittedSource::new(std::sync::Arc::clone(&view), updates));
    let settings = Settings {
        distribution: Some(distribution.clone()),
        max_recent_records: config.max_recent_records,
        ..Settings::default()
    };
    let status = settings.status.clone();
    let metrics = settings.metrics.clone();
    let mut owner = Tasks::default();
    let mut server = tokio::spawn(http.serve(config.status_listen, {
        controller::status_router(status.clone(), metrics, distribution.clone())
            .merge(distribution.router())
    }));
    owner.own(&server);
    let mut controller = Controller::open(&config.root, store, source, settings)?;
    let ingestion = DetailIngestion::new(rpc, writer, controller.fence(), view, notify);
    // The writer stops once the controller returns, so a halted controller
    // leaves the journal where it stopped.
    let writer_stop = Shutdown::default();
    let ingest = tokio::spawn({
        let (executor, stop, writer_stop) = (executor.clone(), stop.clone(), writer_stop.clone());
        async move {
            let result = executor.ingest(ingestion, feed, writer_stop).await;
            if let Err(error) = &result {
                // A refused sealed rollback halts the controller; a journal
                // failure stops the service to restart from its checkpoint.
                tracing::error!(%error, "display journal ingestion stopped");
                if !matches!(error, super::ingestion::IngestError::SealedReorg(..)) {
                    stop.request();
                }
            }
            result
        }
    });
    owner.own(&ingest);
    // Listener failure asks for a boundary stop rather than cancelling activation.
    let watch_stop = stop.clone();
    let watcher = tokio::spawn(async move {
        let result = (&mut server).await;
        watch_stop.request();
        result
    });
    owner.own(&watcher);
    let result = controller.run_until(executor, stop.clone()).await;
    // The writer finishes its current batch and releases the journal.
    writer_stop.request();
    let ingested = ingest.await?;
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
    match ingested {
        Err(super::ingestion::IngestError::Store(error)) => Err(error.into()),
        _ => Ok(()),
    }
}
