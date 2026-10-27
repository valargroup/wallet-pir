use super::polling::SyncTask;
use super::{Consumer, ConsumerPass, DeferralReason, Error, PeerReservation};
use crate::Input;
use std::time::Instant;

/// Exactly one sequential consumer with at most one reusable loaded candidate.
/// Cancelling a pass never manufactures authority; the next pass settles owned
/// operations and re-observes before deciding whether prepared bytes can resume.
pub struct ConsumerEngine<C: Consumer> {
    context: C,
    pending_loaded: Option<C::Loaded>,
}
impl<C: Consumer> ConsumerEngine<C> {
    pub fn new(context: C) -> Self {
        Self {
            context,
            pending_loaded: None,
        }
    }
    pub fn context(&self) -> &C {
        &self.context
    }
    pub fn context_mut(&mut self) -> &mut C {
        &mut self.context
    }

    fn retain_resumable(&mut self) {
        if !self
            .pending_loaded
            .as_ref()
            .is_some_and(|loaded| self.context.keep_loaded(loaded))
        {
            self.pending_loaded = None;
        }
    }
    async fn deferred(&mut self) -> ConsumerPass<C::Report> {
        ConsumerPass::Deferred {
            report: self.context.serving_report().await,
        }
    }
    async fn run_pass(&mut self) -> Result<ConsumerPass<C::Report>, Error> {
        // A non-resumable candidate can own the very operation guard that
        // settlement needs. Drop it before waiting for owner work to finish.
        self.retain_resumable();
        self.context.settle().await?;
        let observation = self.context.observe().await?;
        self.retain_resumable();
        if !self.context.eligible(&observation) {
            return Ok(self.deferred().await);
        }
        self.context.check_serving(&observation).await?;
        if self.pending_loaded.is_none() {
            let started = Instant::now();
            let mut candidate = match self.context.select(&observation).await? {
                Input::Work(candidate) => candidate,
                Input::Unchanged | Input::Deferred => return Ok(self.deferred().await),
            };
            if self.context.reserve(&mut candidate).await? == PeerReservation::PeerAdvanced {
                self.context.deferred(DeferralReason::PeerAdvanced).await;
                return Ok(self.deferred().await);
            }
            let artifacts = self.context.obtain(&candidate).await?;
            self.context.phase_finished("fetch", started.elapsed());
            let started = Instant::now();
            self.pending_loaded = Some(self.context.load(candidate, artifacts).await?);
            self.context.phase_finished("load", started.elapsed());
        }
        let started = Instant::now();
        let latest = self.context.observe().await?;
        if self.context.invalidates_preparation(&latest) {
            self.pending_loaded = None;
        }
        if !self.context.eligible(&latest) {
            self.pending_loaded = None;
            return Ok(self.deferred().await);
        }
        let Some(loaded) = self.pending_loaded.take() else {
            self.context
                .deferred(DeferralReason::AuthorityAdvanced)
                .await;
            return Ok(self.deferred().await);
        };
        self.pending_loaded = Some(self.context.rebind(&latest, loaded).await?);
        let loaded = self
            .pending_loaded
            .as_ref()
            .expect("a candidate was rebound");
        let validation = self.context.validate(&latest, loaded)?;
        self.context.phase_finished("revalidate", started.elapsed());
        let started = Instant::now();
        let authority = self.context.authorize(&latest, &validation).await?;
        self.context.phase_finished("authority", started.elapsed());
        let loaded = self
            .pending_loaded
            .as_mut()
            .expect("the candidate is pending");
        if self.context.confirm(loaded).await? == PeerReservation::PeerAdvanced {
            self.pending_loaded = None;
            self.context.deferred(DeferralReason::PeerAdvanced).await;
            return Ok(self.deferred().await);
        }
        let loaded = self
            .pending_loaded
            .take()
            .expect("the candidate is reserved");
        let started = Instant::now();
        let (report, handover) = self.context.commit(loaded, validation, authority).await?;
        self.context.phase_finished("commit", started.elapsed());
        Ok(ConsumerPass::Committed { report, handover })
    }
}
impl<C: Consumer> SyncTask for ConsumerEngine<C> {
    type Error = Error;
    type Report = ConsumerPass<C::Report>;
    async fn synchronize(&mut self) -> Result<Self::Report, Error> {
        self.context.pass_started().await;
        let result = loop {
            let result = match self.run_pass().await {
                Err(error) => match self.context.deferral(&error) {
                    Some(reason) => {
                        self.retain_resumable();
                        self.context.deferred(reason).await;
                        Ok(self.deferred().await)
                    }
                    None => {
                        self.pending_loaded = None;
                        Err(error)
                    }
                },
                result => result,
            };
            if self.context.continue_after(&result) {
                continue;
            }
            break result;
        };
        self.context.pass_finished(&result).await;
        result
    }
    fn terminal(&self, error: &Error) -> bool {
        self.context.terminal(error)
    }
    fn transient(&self, error: &Error) -> bool {
        self.context.transient(error)
    }
}
