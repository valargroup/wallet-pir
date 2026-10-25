//! Chain-ordered journal ingestion scheduled by the root publisher.
//!
//! The root owns one chain follower and, per product, live and backfill
//! schedulers that drive one writer. Products supply a journal's committed
//! coverage, withdrawal before rewind, bounded block ingestion and the action
//! taken once coverage reaches the observed tip; they never follow the tip.
use crate::FailurePolicy;
mod follower;
mod schedule;
#[cfg(test)]
mod tests;

pub use follower::{ChainFollower, ChainSource};
pub use schedule::{catch_up, Outcome};
use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc, time::Duration};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// A block identity; the hash is in RPC display order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockId {
    pub height: u64,
    pub hash: String,
}

/// One follower pass: the genesis and tip identities read together. Every pass
/// is delivered, including an unchanged tip, so products with time-based
/// observations keep their cadence; `pass` distinguishes repeated tips.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainObservation {
    pub genesis: String,
    pub tip: BlockId,
    pub pass: u64,
}

/// Root chain settings shared by every scheduled product. Products may build
/// their own extraction client from them; they never poll the tip themselves.
#[derive(Clone, Debug)]
pub struct ChainSettings {
    pub rpc_url: String,
    pub rpc_cookie: PathBuf,
    pub poll: Duration,
}

pub type ChainFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Canonical block hashes from the follower's source, used to reconcile each
/// durable checkpoint rather than trusting the follower's previous pass.
pub trait CanonicalChain: Send + Sync {
    fn hash_at(&self, height: u64) -> ChainFuture<'_, String>;
}

/// The follower's observations and canonical lookups, injected at service start.
#[derive(Clone)]
pub struct ChainFeed {
    pub chain: Arc<dyn CanonicalChain>,
    pub observations: tokio::sync::watch::Receiver<Option<ChainObservation>>,
}

/// One product journal's writer operations. Each method commits atomically or
/// fails without advancing coverage; the root never cancels one in progress.
pub trait Ingestion: Send {
    type Error: std::fmt::Display + Send;
    /// Highest block to ingest from this observation. A product may wait for
    /// confirmations; reconciliation still checks the full observed chain so
    /// increasing the depth cannot hide an orphaned committed block.
    fn target_height(&self, tip: u64) -> u64 {
        tip
    }
    /// The first height an empty journal covers when the observed tip is
    /// `tip`; a rolling window starts below the tip, a journal at a fixed height.
    fn start_height(&self, tip: u64) -> u64;
    /// Called with every live observation before reconciliation, including one
    /// from a different network than the first observation, which the
    /// schedulers never reconcile: apply the product's network policy here,
    /// and withdraw dependents already known to be orphaned before a deep
    /// ancestor search.
    fn observed(
        &mut self,
        _observation: &ChainObservation,
    ) -> impl Future<Output = std::result::Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }
    /// The newest committed block, or `None` for an empty journal.
    fn committed(&self) -> Option<BlockId>;
    /// The newest committed block at or below `height`, for reconciliation.
    fn checkpoint_at_or_below(&self, height: u64) -> impl Future<Output = Option<BlockId>> + Send;
    /// Durably withdraw or halt anything that depends on blocks above `keep`
    /// using the product's existing rules. Always runs before [`Self::rewind`].
    fn withdraw_above(
        &mut self,
        keep: Option<&BlockId>,
    ) -> impl Future<Output = std::result::Result<(), Self::Error>> + Send;
    /// Remove committed blocks above `keep`; `None` empties the journal.
    fn rewind(
        &mut self,
        keep: Option<&BlockId>,
    ) -> impl Future<Output = std::result::Result<(), Self::Error>> + Send;
    /// Commit blocks `from..=through` in order, checking each block's continuity
    /// with committed state. A failure may leave a committed prefix.
    fn ingest(
        &mut self,
        from: u64,
        through: u64,
    ) -> impl Future<Output = std::result::Result<(), Self::Error>> + Send;
    /// Called after reconciliation whenever committed coverage equals the
    /// observed tip, including unchanged tips. Never called while catching up.
    fn caught_up(
        &mut self,
        _observation: &ChainObservation,
    ) -> impl Future<Output = std::result::Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }
    fn failed(&mut self, _error: &Self::Error) -> FailurePolicy {
        FailurePolicy::NextPoll
    }
}

pub type IngestionRun<'a, E> =
    Pin<Box<dyn Future<Output = std::result::Result<(), E>> + Send + 'a>>;

/// The root injects ingestion scheduling without a product-to-host dependency.
/// The returned future owns the writer until both schedulers have drained.
pub trait IngestionExecutor: Clone + Send + Sync + 'static {
    fn ingest<'a, I: Ingestion + 'a>(
        &'a self,
        ingestion: I,
        feed: ChainFeed,
        shutdown: crate::service::Shutdown,
    ) -> IngestionRun<'a, I::Error>;
}

impl IngestionExecutor for crate::engine::PublisherEngine {
    fn ingest<'a, I: Ingestion + 'a>(
        &'a self,
        ingestion: I,
        feed: ChainFeed,
        shutdown: crate::service::Shutdown,
    ) -> IngestionRun<'a, I::Error> {
        schedule::run(ingestion, feed, shutdown)
    }
}
