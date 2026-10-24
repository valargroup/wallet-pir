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
    /// Set while committed coverage matches an observed tip.
    pub(super) current: Arc<AtomicBool>,
}
impl pir_pipeline::publisher::Publisher for HistoryPublication {
    type Error = BoxError;
    type Report = ();
    type Publication<'a>
        = preparation::HistoryPreparation<'a>
    where
        Self: 'a;
    async fn acquire(&mut self) -> Result<pir_pipeline::Input<Self::Publication<'_>>, BoxError> {
        if self.server.is_finished() || self.ingest.is_finished() {
            return Err("history publisher background service stopped".into());
        }
        // No new publication while the journal catches up; the active one
        // keeps serving until reconciliation withdraws it.
        if !self.current.load(Ordering::Acquire) {
            return Ok(Input::Unchanged);
        }
        Ok(
            match acquire_history(&self.config, &self.rpc, &self.store, &self.authority).await? {
                Input::Work(work) => Input::Work(preparation::HistoryPreparation::new(
                    &self.config,
                    &self.rpc,
                    &self.authority,
                    work,
                    DecoupledPolicy::for_geometry,
                    Some(self.config.archive_geometry.clone()),
                    Some(self.config.recent_from),
                )),
                Input::Unchanged => Input::Unchanged,
                Input::Deferred => Input::Deferred,
            },
        )
    }
    fn failed(&mut self, error: &BoxError) -> pir_pipeline::FailurePolicy {
        if self.server.is_finished() || self.ingest.is_finished() {
            return pir_pipeline::FailurePolicy::Stop;
        }
        if !self.shutdown.requested() {
            self.authority
                .0
                .publication_failures
                .fetch_add(1, Ordering::Relaxed);
            tracing::error!(%error, "publication failed; keeping valid coverage and retrying");
            self.authority.0.status.write().unwrap()["publication_error"] =
                error.to_string().into();
        }
        pir_pipeline::FailurePolicy::NextPoll
    }
    fn settled(&mut self) -> Result<pir_pipeline::publisher::Control, BoxError> {
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
