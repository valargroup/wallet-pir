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
impl pir_pipeline::publisher::Workflow for DetailPublication<'_> {
    type Error = BoxError;
    async fn step(&mut self) -> Result<pir_pipeline::publisher::Control, BoxError> {
        match self.wake.take() {
            Some(DetailWake::Background(Some(event))) => self.controller.background(event),
            Some(DetailWake::Background(None)) => unreachable!("the controller holds a sender"),
            Some(DetailWake::Source) => self.controller.poll_source().await?,
            None => {}
        }
        self.controller.publication_step().await
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
        while self.controller.building {
            if let Some(event) = self.controller.receiver.recv().await {
                self.controller.background(event);
            }
        }
        Ok(())
    }
}
