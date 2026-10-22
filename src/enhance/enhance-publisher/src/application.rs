//! Canonical acquisition and local preparation run independently. A bounded
//! latest-work slot coalesces source progress while one publication prepares.
use crate::{
    chain_source::{Prevouts, ZakuraClient},
    ingest::EnhanceJournal,
    publisher::{EnhancePublisher, PublicationWork},
};
use pir_pipeline::{FailurePolicy, Input};
use std::time::Duration;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Canonical node access and the shared spent-output cache used during acquisition.
pub struct CanonicalSource {
    pub rpc: ZakuraClient,
    pub prevouts: Prevouts,
}
/// Polling cadence for canonical chain acquisition.
pub struct AcquisitionConfig {
    pub poll: Duration,
}

/// Persist withdrawal before rewind; journal mutation commits independently of
/// preparation. A prepared token can only publish under its original fence.
pub async fn collect(
    journal: &mut EnhanceJournal,
    publisher: &EnhancePublisher,
    source: &CanonicalSource,
) -> Result<Option<PublicationWork>> {
    let tip = source.rpc.tip_height().await?;
    let mut ancestor = None;
    for block in journal.records().blocks().iter().rev() {
        if block.height <= tip && source.rpc.block_hash_display(block.height).await? == block.hash {
            ancestor = Some((block.height, block.first_position + block.action_count));
            break;
        }
    }
    if journal.committed_height() != ancestor.map(|a| a.0) {
        publisher.revoke_after(ancestor.map_or(0, |a| a.1))?;
        journal.rewind_to_height(ancestor.map(|a| a.0))?;
    }
    let next = journal
        .committed_height()
        .map_or(enhance_material::ACTIVATION_HEIGHT, |h| h + 1);
    for height in next..=tip {
        journal.append_block(
            &crate::chain_source::block(&source.rpc, height, &source.prevouts).await?,
        )?;
    }
    publisher.work(journal.records()).map_err(Into::into)
}

/// Supervise acquisition, HTTP, and one coalescing publication task until
/// shutdown. Journal commits continue during preparation; in-flight publication
/// finishes its owner operation before the publication task exits.
pub async fn run_until(
    config: AcquisitionConfig,
    publisher: EnhancePublisher,
    journal: EnhanceJournal,
    source: CanonicalSource,
    http: tokio::task::JoinHandle<std::io::Result<()>>,
    shutdown: pir_pipeline::service::Shutdown,
) -> Result<()> {
    let (latest, pending) = tokio::sync::watch::channel::<Option<PublicationWork>>(None);
    let preparing = publisher.clone();
    let stop = shutdown.clone();
    let publication = tokio::spawn(async move {
        pir_pipeline::publisher::PublisherService::new(PublicationWorkflow {
            publisher: preparing,
            pending,
            closed: false,
        })
        .run(stop.cancelled())
        .await
    });
    let result = pir_pipeline::supervise(
        &mut CanonicalAcquisition {
            publisher,
            journal,
            source,
            latest,
            http,
            publication: &publication,
        },
        config.poll,
        shutdown.cancelled(),
    )
    .await;
    // Dropping acquisition closes the latest-work slot. A running publication
    // finishes its current owner operation before observing shutdown or closure.
    publication.await??;
    result
}
struct CanonicalAcquisition<'a> {
    publisher: EnhancePublisher,
    journal: EnhanceJournal,
    source: CanonicalSource,
    latest: tokio::sync::watch::Sender<Option<PublicationWork>>,
    http: tokio::task::JoinHandle<std::io::Result<()>>,
    publication: &'a tokio::task::JoinHandle<Result<()>>,
}
impl pir_pipeline::Task for CanonicalAcquisition<'_> {
    type Error = Box<dyn std::error::Error + Send + Sync>;
    async fn run(&mut self) -> Result<()> {
        if self.http.is_finished() || self.publication.is_finished() {
            return Err("Enhance publisher task stopped".into());
        }
        let input = pir_pipeline::acquire(
            async {
                Ok::<_, Self::Error>(
                    match collect(&mut self.journal, &self.publisher, &self.source).await? {
                        Some(work) => Input::Work(work),
                        None => Input::Unchanged,
                    },
                )
            },
            &mut (),
        )
        .await?;
        if let Input::Work(work) = input {
            self.latest.send_replace(Some(work));
        }
        Ok(())
    }
    fn failed(&mut self, error: &Self::Error) -> FailurePolicy {
        if self.http.is_finished() || self.publication.is_finished() {
            FailurePolicy::Stop
        } else {
            tracing::error!(%error, "Enhance canonical acquisition deferred");
            FailurePolicy::NextPoll
        }
    }
}

struct PublicationWorkflow {
    publisher: EnhancePublisher,
    pending: tokio::sync::watch::Receiver<Option<PublicationWork>>,
    closed: bool,
}
impl pir_pipeline::publisher::Workflow for PublicationWorkflow {
    type Error = Box<dyn std::error::Error + Send + Sync>;
    async fn step(&mut self) -> Result<pir_pipeline::publisher::Control> {
        use pir_pipeline::publisher::{Control, PublicationPass};
        if self.closed {
            return Ok(Control::Complete);
        }
        let work = self.pending.borrow_and_update().clone();
        if let Some(work) = work {
            PublicationPass::new(
                async { Ok::<_, String>(Input::Work(work)) },
                |work| async { Ok(Input::Work(work)) },
                |work| self.publisher.publish(work),
            )
            .run(&mut ())
            .await?;
        }
        Ok(Control::Continue)
    }
    fn failed(&mut self, error: &Self::Error) -> FailurePolicy {
        tracing::error!(%error, "Enhance local publication deferred");
        FailurePolicy::NextPoll
    }
    async fn wait(&mut self) -> Result<()> {
        self.closed = self.pending.changed().await.is_err();
        Ok(())
    }
}
