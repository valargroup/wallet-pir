//! Product policy and state supplied to the shared publisher lifecycle.
use super::*;

pub(super) struct HistoryPublication {
    pub(super) config: Config,
    pub(super) rpc: NodeClient,
    pub(super) store: Arc<tokio::sync::Mutex<EventStore>>,
    pub(super) authority: Authority,
    pub(super) shutdown: pir_pipeline::service::Shutdown,
    pub(super) server: tokio::task::JoinHandle<std::io::Result<()>>,
    pub(super) ingest: tokio::task::JoinHandle<Result<(), BoxError>>,
    pub(super) updates: tokio::sync::watch::Receiver<u64>,
}
impl pir_pipeline::publisher::Workflow for HistoryPublication {
    type Error = BoxError;
    async fn step(&mut self) -> Result<pir_pipeline::publisher::Control, BoxError> {
        if self.server.is_finished() || self.ingest.is_finished() {
            return Err("history publisher background service stopped".into());
        }
        if let Err(error) =
            publish_once(&self.config, &self.rpc, &self.store, &self.authority).await
        {
            if self.shutdown.requested() {
                tracing::info!(%error, "publication cycle ended by shutdown");
                return Ok(pir_pipeline::publisher::Control::Continue);
            }
            self.authority
                .0
                .publication_failures
                .fetch_add(1, Ordering::Relaxed);
            tracing::error!(%error,"publication failed; keeping valid coverage and retrying");
            self.authority.0.status.write().unwrap()["publication_error"] =
                error.to_string().into();
        }
        let public_height = self.authority.0.active.read().unwrap().publication.height;
        let oldest = self
            .authority
            .0
            .observed
            .read()
            .unwrap()
            .range((public_height.saturating_add(1))..)
            .map(|(_, t)| t.elapsed().as_secs())
            .max()
            .unwrap_or(0);
        if oldest > 30 {
            tracing::error!(
                alert = "transparent_publication_stale",
                oldest_unpublished_seconds = oldest,
                "publication freshness target exceeded"
            );
        }
        if let Err(error) = collect_candidates(&self.config, &self.authority) {
            tracing::warn!(%error,"candidate collection failed");
        }
        Ok(pir_pipeline::publisher::Control::Continue)
    }
    async fn wait(&mut self) -> Result<(), BoxError> {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
            _ = self.updates.changed() => {},
        }
        Ok(())
    }
    async fn finish(&mut self) -> Result<(), BoxError> {
        self.shutdown.request();
        (&mut self.ingest).await??;
        Ok(())
    }
}
