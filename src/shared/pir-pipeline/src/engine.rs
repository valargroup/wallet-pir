//! One execution order for every prepared publisher, independent of its data.
use crate::service::Shutdown;
use crate::{
    publisher::{
        Control, Publication, PublicationInput, Publisher, PublisherExecutor, PublisherRun,
    },
    FailurePolicy,
};

/// Executes source-bound publication operations and joins every started phase.
/// Shutdown interrupts waits, never a file write or an uncertain state commit.
#[derive(Clone, Copy, Default)]
pub struct PublisherEngine;

/// Completion of preparation for one exact work item. Construction is private
/// to the engine; retaining it also retains the product's preparation ownership.
pub struct PreparedPublication<P: Publication> {
    publication: P,
    candidate: P::Candidate,
}
impl<P: Publication> PreparedPublication<P> {
    /// Inspect the prepared descriptor without obtaining commit authority.
    pub fn candidate(&self) -> &P::Candidate {
        &self.candidate
    }

    /// Recheck retained preparation against the live source before guarded advancement.
    pub async fn commit(mut self) -> Result<P::Report, P::Error> {
        self.publication.check()?;
        self.publication.recheck(&mut self.candidate).await?;
        self.publication.check()?;
        self.publication.commit(self.candidate).await
    }
}
impl PublisherEngine {
    /// Join each preparation phase, checking invalidation after every await.
    /// A successful return retains ownership but grants no serving authority.
    pub async fn prepare<P: Publication>(
        &self,
        mut publication: P,
    ) -> Result<PreparedPublication<P>, P::Error> {
        publication.check()?;
        let material = publication.build().await?;
        publication.check()?;
        let stored = publication.store(material).await?;
        publication.check()?;
        let verified = publication.verify(stored).await?;
        publication.check()?;
        let candidate = publication.assemble(verified).await?;
        publication.check()?;
        Ok(PreparedPublication {
            publication,
            candidate,
        })
    }
}

impl PublisherExecutor for PublisherEngine {
    async fn publish<P: Publication>(&self, publication: P) -> Result<P::Report, P::Error> {
        self.prepare(publication).await?.commit().await
    }

    fn run<'a, P: Publisher + 'a>(
        &'a self,
        mut publisher: P,
        shutdown: Shutdown,
    ) -> PublisherRun<'a, P::Error> {
        Box::pin(async move {
            let outcome = async {
                while !shutdown.requested() {
                    let attempt = async {
                        publisher.background(*self).await?;
                        match publisher.acquire().await? {
                            PublicationInput::NewMaterial(publication)
                            | PublicationInput::MetadataRefresh(publication) => {
                                self.publish(publication).await.map(Some)
                            }
                            PublicationInput::Unchanged | PublicationInput::Deferred => Ok(None),
                        }
                    }
                    .await;
                    match attempt {
                        Ok(Some(report)) => publisher.completed(report),
                        Ok(None) => {}
                        Err(error) => {
                            if publisher.failed(&error) == FailurePolicy::Stop {
                                return Err(error);
                            }
                        }
                    }
                    if matches!(publisher.settled()?, Control::Complete) {
                        break;
                    }
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        result = publisher.wait() => result?,
                    }
                }
                Ok(())
            }
            .await;
            let drained = publisher.finish().await;
            outcome.and(drained)
        })
    }
}
