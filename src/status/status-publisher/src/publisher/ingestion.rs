//! The Status rolling window as a root-scheduled journal. Disconnected blocks
//! become retained fork observations; every caught-up pass, including an
//! unchanged tip, collects a fresh complete mempool before it is offered.
use super::*;
use pir_pipeline::ingestion::{BlockId, CanonicalChain, ChainFuture, ChainObservation, Ingestion};
use source::SourceError;

/// Why one Status ingestion operation did not complete.
#[derive(Debug, thiserror::Error)]
pub(super) enum ObservationError {
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error("Status observation consumer closed")]
    Closed,
}

pub(super) struct StatusIngestion {
    pub(super) source: source::RollingWindow,
    pub(super) rpc: ZakuraClient,
    pub(super) publisher: Publisher,
    pub(super) tx: watch::Sender<Option<Arc<source::Observation>>>,
}

fn identity((height, hash): (u32, Hash)) -> BlockId {
    BlockId {
        height: u64::from(height),
        hash: zakura_server_chain::hash::display(&hash),
    }
}
fn parse(displayed: &str, error: SourceError) -> Result<Hash, SourceError> {
    zakura_server_chain::hash::parse_display(displayed).map_err(|_| error)
}

impl Ingestion for StatusIngestion {
    type Error = ObservationError;
    fn start_height(&self, tip: u64) -> u64 {
        self.source.first_height(tip)
    }
    fn check_network(&self, genesis: &str) -> Result<(), ObservationError> {
        let genesis = parse(genesis, SourceError::Genesis)?;
        if self
            .source
            .network()
            .is_some_and(|network| network != genesis)
        {
            return Err(SourceError::Genesis.into());
        }
        Ok(())
    }
    fn committed(&self) -> Option<BlockId> {
        self.source.committed().map(identity)
    }
    fn checkpoint_at_or_below(&self, height: u64) -> Option<BlockId> {
        self.source.checkpoint_at_or_below(height).map(identity)
    }
    /// Withdraw an active manifest anchored above the retained window before
    /// its blocks are disconnected; the publication loop then recovers.
    async fn withdraw_above(&mut self, keep: Option<&BlockId>) -> Result<(), ObservationError> {
        let anchored_above = self
            .publisher
            .published
            .read()
            .unwrap()
            .manifest
            .as_ref()
            .is_some_and(|active| {
                keep.is_none_or(|keep| u64::from(active.anchor_height) > keep.height)
            });
        if anchored_above {
            self.publisher.withdraw();
        }
        Ok(())
    }
    async fn rewind(&mut self, keep: Option<&BlockId>) -> Result<(), ObservationError> {
        let keep = keep.map(|keep| keep.height as u32);
        Ok(self.source.disconnect_above(keep)?)
    }
    async fn ingest(&mut self, from: u64, through: u64) -> Result<(), ObservationError> {
        let genesis = parse(
            &self
                .rpc
                .block_hash_display(0)
                .await
                .map_err(SourceError::from)?,
            SourceError::Genesis,
        )?;
        Ok(self
            .source
            .extend(&self.rpc, genesis, from, through)
            .await?)
    }
    async fn caught_up(&mut self, observation: &ChainObservation) -> Result<(), ObservationError> {
        let began = std::time::Instant::now();
        let genesis = parse(&observation.genesis, SourceError::Genesis)?;
        let anchor = parse(&observation.tip.hash, SourceError::TipChanged)?;
        let observed = self
            .source
            .snapshot(&self.rpc, genesis, observation.tip.height, anchor, began)
            .await?;
        self.tx
            .send(Some(Arc::new(observed)))
            .map_err(|_| ObservationError::Closed)
    }
    fn failed(&mut self, error: &ObservationError) -> pir_pipeline::FailurePolicy {
        match error {
            ObservationError::Source(error) => {
                tracing::warn!(%error, "Status source observation failed");
                pir_pipeline::FailurePolicy::NextPoll
            }
            ObservationError::Closed => pir_pipeline::FailurePolicy::Stop,
        }
    }
}

/// Canonical hash lookups through Status's own node client.
pub(super) struct NodeChain(pub(super) ZakuraClient);
impl CanonicalChain for NodeChain {
    fn hash_at(&self, height: u64) -> ChainFuture<'_, String> {
        Box::pin(async move { Ok(self.0.block_hash_display(height).await?) })
    }
}
