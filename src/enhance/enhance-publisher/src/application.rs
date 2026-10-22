//! Journal ingestion, scheduled by the root publisher, and local preparation
//! run independently. A bounded latest-work slot coalesces committed coverage
//! while one publication prepares.
use crate::{
    chain_source::{Prevouts, ZakuraClient},
    ingest::EnhanceJournal,
    publisher::{EnhancePublisher, PublicationWork},
};
use pir_pipeline::{
    ingestion::{BlockId, ChainFeed, ChainObservation, Ingestion},
    FailurePolicy, Input,
};
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Canonical node access and the shared spent-output cache used during extraction.
pub struct CanonicalSource {
    pub rpc: ZakuraClient,
    pub prevouts: Prevouts,
}

/// The Enhance journal's writer operations. Withdrawal persists before rewind;
/// journal commits proceed independently of preparation, and a prepared token
/// can only publish under its original fence.
pub struct EnhanceIngestion {
    pub journal: EnhanceJournal,
    pub publisher: EnhancePublisher,
    pub source: CanonicalSource,
    latest: tokio::sync::watch::Sender<Option<PublicationWork>>,
}
fn identity(block: &crate::journal::JournalBlock) -> BlockId {
    BlockId {
        height: block.height,
        hash: block.hash.clone(),
    }
}
impl Ingestion for EnhanceIngestion {
    type Error = Box<dyn std::error::Error + Send + Sync>;
    fn start_height(&self) -> u64 {
        enhance_shared::ACTIVATION_HEIGHT
    }
    fn committed(&self) -> Option<BlockId> {
        self.journal.records().blocks().last().map(identity)
    }
    fn checkpoint_at_or_below(&self, height: u64) -> Option<BlockId> {
        let blocks = self.journal.records().blocks();
        let index = blocks.partition_point(|block| block.height <= height);
        index.checked_sub(1).map(|index| identity(&blocks[index]))
    }
    async fn withdraw_above(&mut self, keep: Option<&BlockId>) -> Result<()> {
        let retained = keep
            .and_then(|keep| {
                self.journal
                    .records()
                    .blocks()
                    .iter()
                    .find(|block| block.height == keep.height)
            })
            .map_or(0, |block| block.first_position + block.action_count);
        Ok(self.publisher.revoke_after(retained)?)
    }
    async fn rewind(&mut self, keep: Option<&BlockId>) -> Result<()> {
        Ok(self.journal.rewind_to_height(keep.map(|keep| keep.height))?)
    }
    async fn ingest(&mut self, from: u64, through: u64) -> Result<()> {
        for height in from..=through {
            let block =
                crate::chain_source::block(&self.source.rpc, height, &self.source.prevouts).await?;
            self.journal.append_block(&block)?;
        }
        Ok(())
    }
    /// Offer publication only for coverage reconciled with the observed tip.
    async fn caught_up(&mut self, _: &ChainObservation) -> Result<()> {
        if let Some(work) = self.publisher.work(self.journal.records())? {
            self.latest.send_replace(Some(work));
        }
        Ok(())
    }
    fn failed(&mut self, error: &Self::Error) -> FailurePolicy {
        tracing::error!(%error, "Enhance journal ingestion deferred");
        FailurePolicy::NextPoll
    }
}

/// Supervise root-scheduled ingestion, HTTP, and one coalescing publication
/// task until shutdown. Journal commits continue during preparation; in-flight
/// publication finishes its owner operation before the publication task exits.
pub async fn run_until<E>(
    executor: E,
    feed: ChainFeed,
    publisher: EnhancePublisher,
    journal: EnhanceJournal,
    source: CanonicalSource,
    http: tokio::task::JoinHandle<std::io::Result<()>>,
    shutdown: pir_pipeline::service::Shutdown,
) -> Result<()>
where
    E: pir_pipeline::publisher::PublisherExecutor + pir_pipeline::ingestion::IngestionExecutor,
{
    let (latest, pending) = tokio::sync::watch::channel::<Option<PublicationWork>>(None);
    let preparing = publisher.clone();
    let stop = shutdown.clone();
    let publishing = executor.clone();
    let publication = tokio::spawn(async move {
        publishing
            .run(
                PublicationWorkflow {
                    publisher: preparing,
                    shutdown: stop.clone(),
                    pending,
                    closed: false,
                },
                stop,
            )
            .await
            .map_err(Box::<dyn std::error::Error + Send + Sync>::from)
    });
    // A stopped listener or publication task stops ingestion after its operation.
    let mut publication = std::pin::pin!(publication);
    let mut http = std::pin::pin!(http);
    let ingestion = EnhanceIngestion {
        journal,
        publisher,
        source,
        latest,
    };
    let ingesting = executor.ingest(ingestion, feed, shutdown.clone());
    let mut ingesting = std::pin::pin!(ingesting);
    let unexpected = tokio::select! {
        result = &mut ingesting => {
            // Dropping ingestion closes the latest-work slot. A running publication
            // finishes its current owner operation before observing shutdown or closure.
            drop(ingesting);
            publication.await??;
            return result;
        }
        joined = &mut publication => Some(joined),
        _ = &mut http => None,
    };
    let requested = shutdown.requested();
    shutdown.request();
    let result = ingesting.await;
    match unexpected {
        Some(joined) => joined??,
        None => publication.await??,
    }
    if !requested {
        return result.and(Err("Enhance publisher task stopped".into()));
    }
    result
}

struct PublicationWorkflow {
    shutdown: pir_pipeline::service::Shutdown,
    publisher: EnhancePublisher,
    pending: tokio::sync::watch::Receiver<Option<PublicationWork>>,
    closed: bool,
}
impl pir_pipeline::publisher::Publisher for PublicationWorkflow {
    type Error = String;
    type Report = ();
    type Publication<'a>
        = crate::publisher::preparation::EnhancePublication<'a>
    where
        Self: 'a;
    async fn acquire(
        &mut self,
    ) -> std::result::Result<pir_pipeline::Input<Self::Publication<'_>>, String> {
        match self.pending.borrow_and_update().clone() {
            Some(work) => self
                .publisher
                .publication(work)
                .map(|work| Input::Work(work.stopping(self.shutdown.clone()))),
            None => Ok(Input::Unchanged),
        }
    }
    fn settled(&mut self) -> std::result::Result<pir_pipeline::publisher::Control, String> {
        Ok(if self.closed {
            pir_pipeline::publisher::Control::Complete
        } else {
            pir_pipeline::publisher::Control::Continue
        })
    }
    fn completed(&mut self, _: ()) {
        self.publisher.publication_finished(None);
    }
    fn failed(&mut self, error: &Self::Error) -> FailurePolicy {
        self.publisher.publication_finished(Some(error.clone()));
        tracing::error!(%error, "Enhance local publication deferred");
        FailurePolicy::NextPoll
    }
    async fn wait(&mut self) -> std::result::Result<(), String> {
        self.closed = self.pending.changed().await.is_err();
        Ok(())
    }
}
