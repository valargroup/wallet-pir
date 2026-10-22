//! Root chain following and per-product journal scheduling.
mod follower;
mod schedule;
#[cfg(test)]
mod tests;

#[allow(unused_imports)]
pub use follower::{ChainFollower, ChainSource, NodeSource};

use crate::publisher::PublisherEngine;
use pir_pipeline::{
    ingestion::{ChainFeed, Ingestion, IngestionExecutor, IngestionRun},
    service::Shutdown,
};

impl IngestionExecutor for PublisherEngine {
    fn ingest<'a, I: Ingestion + 'a>(
        &'a self,
        ingestion: I,
        feed: ChainFeed,
        shutdown: Shutdown,
    ) -> IngestionRun<'a, I::Error> {
        schedule::run(ingestion, feed, shutdown)
    }
}
