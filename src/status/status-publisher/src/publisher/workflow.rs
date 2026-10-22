//! Product policy and state supplied to the shared publisher lifecycle.
use super::*;

/// Observation capture remains independent while publication prepares material.
pub(super) struct Publication<'a> {
    pub(super) publisher: &'a Publisher,
    pub(super) rpc: ZakuraClient,
    pub(super) rx: watch::Receiver<Option<Arc<source::Observation>>>,
    pub(super) salt: [u8; 32],
    pub(super) authority: Option<authority::Authority>,
    pub(super) previous: Option<Generation>,
    pub(super) last_observed: u64,
    pub(super) server: tokio::task::JoinHandle<std::io::Result<()>>,
    pub(super) observer: tokio::task::JoinHandle<Result<(), ObservationError>>,
}
impl pir_pipeline::publisher::Workflow for Publication<'_> {
    type Error = Failure;
    async fn step(&mut self) -> Result<pir_pipeline::publisher::Control, Failure> {
        if self.server.is_finished() || self.observer.is_finished() {
            return Err("Status background service stopped".into());
        }
        let observed = self.rx.borrow().clone();
        if let Some(observed) = observed.filter(|o| o.observed_ms > self.last_observed) {
            self.publisher
                .open_authority(observed.snapshot.network, self.salt)?;
            // Detect invalidation independently of the publication outcome.
            let active = self.publisher.published.read().unwrap().manifest.clone();
            if let Some(active) = active {
                if self.rpc.block_hash(u64::from(active.anchor_height)).await? != active.anchor_hash
                {
                    self.publisher.withdraw();
                }
            }
            // Candidate validation can also withdraw. After any withdrawal
            // earlier answers may no longer be trusted: start a new recovery
            // epoch and prepare from scratch.
            if self.publisher.last_transition_withdrew() {
                self.publisher.recover();
                self.previous = None;
            }
            if self.authority.is_none() {
                self.authority = Some(authority::Authority::open(
                    self.publisher.config.state_dir.join("authority"),
                    observed.snapshot.network,
                    self.salt,
                )?);
            }
            self.last_observed = observed.observed_ms;
            match self
                .publisher
                .publish(
                    observed,
                    self.authority.as_mut().unwrap(),
                    &mut self.previous,
                    &self.rpc,
                )
                .await
            {
                Ok(g) => self.previous = Some(g),
                Err(error) => {
                    if error.downcast_ref::<authority::AuthorityError>().is_some()
                        || error
                            .downcast_ref::<pir_publication::PublicationAuthorityError>()
                            .is_some()
                    {
                        self.publisher.withdraw();
                        self.authority = None;
                    }
                    tracing::warn!(%error, "Status publication deferred");
                }
            }
        }
        Ok(pir_pipeline::publisher::Control::Continue)
    }
    async fn wait(&mut self) -> Result<(), Failure> {
        self.rx.changed().await?;
        Ok(())
    }
}
