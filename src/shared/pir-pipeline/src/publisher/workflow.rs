use crate::FailurePolicy;
use std::{future::Future, time::Duration};

/// A complete pass either schedules another wake or finishes a bounded rehearsal.
#[derive(Clone, Copy)]
pub enum Control {
    Continue,
    Complete,
}

/// Product operations and policies used by the common service. A step composes
/// typed PublicationPass operations and may handle product background events.
/// Waiting is cancellable; a step and final drain are always awaited to completion.
#[allow(async_fn_in_trait)]
pub trait Workflow {
    type Error;
    async fn step(&mut self) -> Result<Control, Self::Error>;
    fn failed(&mut self, _error: &Self::Error) -> FailurePolicy {
        FailurePolicy::Stop
    }
    async fn wait(&mut self) -> Result<(), Self::Error>;
    async fn finish(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// One shared loop for polling, notifications and deadline-driven publishers.
/// Products select wake and failure policies without overriding lifecycle execution.
pub struct PublisherService<W> {
    workflow: W,
}
impl<W: Workflow> PublisherService<W> {
    pub fn new(workflow: W) -> Self {
        Self { workflow }
    }
    pub async fn run(mut self, shutdown: impl Future<Output = ()>) -> Result<(), W::Error> {
        tokio::pin!(shutdown);
        let outcome = async {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => return Ok(()),
                    _ = std::future::ready(()) => {},
                }
                match self.workflow.step().await {
                    Ok(Control::Complete) => return Ok(()),
                    Ok(Control::Continue) => {}
                    Err(error) => {
                        if self.workflow.failed(&error) == FailurePolicy::Stop {
                            return Err(error);
                        }
                    }
                }
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    result = self.workflow.wait() => result?,
                }
            }
        }
        .await;
        let drained = self.workflow.finish().await;
        outcome.and(drained)
    }
}

pub(crate) struct Polling<'a, T> {
    pub task: &'a mut T,
    pub poll: Duration,
}
impl<T: crate::Task> Workflow for Polling<'_, T> {
    type Error = T::Error;
    async fn step(&mut self) -> Result<Control, Self::Error> {
        let started = tokio::time::Instant::now();
        let result = self.task.run().await;
        self.task.finished(started.elapsed());
        result.map(|()| Control::Continue)
    }
    fn failed(&mut self, error: &Self::Error) -> FailurePolicy {
        self.task.failed(error)
    }
    async fn wait(&mut self) -> Result<(), Self::Error> {
        tokio::time::sleep(self.poll).await;
        Ok(())
    }
}
