//! Product policy and state supplied to the shared publisher lifecycle.
use super::*;

pub(super) enum DetailWake {
    Background(Option<Background>),
    Source,
}
pub(super) struct DetailPublication<'a> {
    pub(super) controller: &'a mut Controller,
    pub(super) wake: Option<DetailWake>,
}
impl pir_pipeline::publisher::Publisher for DetailPublication<'_> {
    type Error = BoxError;
    type Report = ();
    type Publication<'a>
        = super::preparation::DisplayPreparation<'a>
    where
        Self: 'a;
    async fn background<E: pir_pipeline::publisher::PublisherExecutor>(
        &mut self,
        executor: E,
    ) -> Result<(), BoxError> {
        match self.wake.take() {
            Some(DetailWake::Background(Some(event))) => self.controller.background(event),
            Some(DetailWake::Background(None)) => {
                return Err("archive preparation channel ended".into())
            }
            Some(DetailWake::Source) => self.controller.poll_source().await?,
            None => {}
        }
        let controller = &mut *self.controller;
        while let Ok(event) = controller.receiver.try_recv() {
            controller.background(event);
        }
        if controller.halted.is_none() {
            controller.schedule(executor);
        }
        Ok(())
    }
    async fn acquire(
        &mut self,
    ) -> Result<pir_pipeline::publisher::PublicationInput<Self::Publication<'_>>, BoxError> {
        let controller = &mut *self.controller;
        // No new publication while the journal writer catches up; the
        // still-valid predecessor keeps serving.
        if controller.halted.is_some() || !controller.source.current() {
            return Ok(pir_pipeline::publisher::PublicationInput::Deferred);
        }
        if controller.retry_at.is_none_or(|t| Instant::now() >= t) {
            if controller.needs_cycle() {
                return Ok(pir_pipeline::publisher::PublicationInput::NewMaterial(
                    super::preparation::DisplayPreparation::new(controller),
                ));
            }
            controller.failures = 0;
            controller.retry_at = None;
        }
        Ok(pir_pipeline::publisher::PublicationInput::Unchanged)
    }
    fn completed(&mut self, _: ()) {
        self.controller.settings.metrics.cycle_finished(true);
        self.controller.failures = 0;
        self.controller.retry_at = None;
    }
    fn failed(&mut self, error: &BoxError) -> pir_pipeline::FailurePolicy {
        if self.controller.shutdown.requested() {
            return pir_pipeline::FailurePolicy::NextPoll;
        }
        let Some(error) = error.downcast_ref::<CycleError>() else {
            return pir_pipeline::FailurePolicy::Stop;
        };
        let controller = &mut *self.controller;
        controller.settings.metrics.cycle_finished(false);
        match error {
            CycleError::Halt(halt) => controller.halt(halt.clone()),
            CycleError::Retry(error) => {
                tracing::warn!(%error, "display publication failed; retrying");
                controller
                    .timeline
                    .record("error", json!({"stage": "cycle", "error": error}));
                controller.retry_at = Some(Instant::now() + backoff(controller.failures));
                controller.failures += 1;
                controller.stalled(error);
                controller.collect();
            }
        }
        pir_pipeline::FailurePolicy::NextPoll
    }
    fn settled(&mut self) -> Result<pir_pipeline::publisher::Control, BoxError> {
        let controller = &mut *self.controller;
        controller.check_recent_bound();
        controller.update_status();
        Ok(
            if controller.halted.is_some() || controller.finite_source_complete() {
                pir_pipeline::publisher::Control::Complete
            } else {
                pir_pipeline::publisher::Control::Continue
            },
        )
    }
    async fn wait(&mut self) -> Result<(), BoxError> {
        let controller = &mut self.controller;
        let now = Instant::now();
        let wake_at = controller
            .queue
            .iter()
            .filter_map(|job| job.retry_at)
            .chain(controller.retry_at)
            .min()
            .unwrap_or(now + Duration::from_secs(3600));
        self.wake = tokio::select! {
            event = controller.receiver.recv() => Some(DetailWake::Background(event)),
            () = controller.source.wait() => Some(DetailWake::Source),
            _ = tokio::time::sleep_until(wake_at.max(now).into()) => None,
        };
        Ok(())
    }
    async fn finish(&mut self) -> Result<(), BoxError> {
        // A wait can consume completion just before the root observes shutdown.
        // Apply that saved completion before waiting for any outstanding build.
        if let Some(DetailWake::Background(Some(event))) = self.wake.take() {
            self.controller.background(event);
        }
        while self.controller.building {
            if let Some(event) = self.controller.receiver.recv().await {
                self.controller.background(event);
            }
        }
        Ok(())
    }
}
