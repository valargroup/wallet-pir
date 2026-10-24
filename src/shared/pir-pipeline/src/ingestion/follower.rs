//! The one root chain follower for every scheduled product.
use crate::{
    ingestion::{BlockId, CanonicalChain, ChainFeed, ChainFuture, ChainObservation},
    service::Shutdown,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

/// Tip reads for the follower; canonical hash reads come from [`CanonicalChain`].
pub trait ChainSource: CanonicalChain {
    fn tip_height(&self) -> ChainFuture<'_, u64>;
}

/// Polls the source once per pass and publishes every observation, including
/// unchanged tips. Coalescing is safe because each product reconciles its own
/// durable checkpoints, not the follower's previous pass.
pub struct ChainFollower {
    source: Arc<dyn ChainSource>,
    poll: Duration,
    observations: watch::Sender<Option<ChainObservation>>,
}

impl ChainFollower {
    pub fn new(source: Arc<dyn ChainSource>, poll: Duration) -> Self {
        Self {
            source,
            poll,
            observations: watch::channel(None).0,
        }
    }

    pub fn feed(&self) -> ChainFeed {
        ChainFeed {
            chain: Arc::new(Canonical(self.source.clone())),
            observations: self.observations.subscribe(),
        }
    }

    /// Follow until shutdown. A source outage publishes nothing. A different
    /// genesis than the first pass observed is still delivered, so each
    /// product applies its own network policy; schedulers never reconcile it.
    pub async fn run(self, shutdown: Shutdown) {
        let mut genesis = None::<String>;
        let mut pass = 0;
        while !shutdown.requested() {
            match observe(&*self.source).await {
                Ok((observed_genesis, tip)) => {
                    if genesis.get_or_insert_with(|| observed_genesis.clone()) != &observed_genesis
                    {
                        tracing::error!(%observed_genesis, "chain source changed network");
                    }
                    pass += 1;
                    self.observations.send_replace(Some(ChainObservation {
                        genesis: observed_genesis,
                        tip,
                        pass,
                    }));
                }
                Err(error) => tracing::warn!(%error, "chain follower pass failed"),
            }
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(self.poll) => {}
            }
        }
    }
}

/// Read genesis and an anchored tip: the tip hash is reread after its height
/// so a tip that moved during the pass is reported at the height read.
async fn observe(source: &dyn ChainSource) -> crate::ingestion::Result<(String, BlockId)> {
    let genesis = source.hash_at(0).await?;
    let height = source.tip_height().await?;
    let hash = source.hash_at(height).await?;
    Ok((genesis, BlockId { height, hash }))
}

struct Canonical(Arc<dyn ChainSource>);
impl CanonicalChain for Canonical {
    fn hash_at(&self, height: u64) -> ChainFuture<'_, String> {
        self.0.hash_at(height)
    }
}
