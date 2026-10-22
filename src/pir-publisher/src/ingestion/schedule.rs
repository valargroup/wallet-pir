//! Live and backfill scheduling over one product writer.
//!
//! Both schedulers lock the same writer for one atomic operation at a time, so
//! live reconciliation (including reorg withdrawal) interleaves with each
//! bounded backfill batch instead of waiting for a long catch-up to finish.
use pir_pipeline::{
    ingestion::{BlockId, CanonicalChain, ChainFeed, ChainObservation, Ingestion, IngestionRun},
    service::Shutdown,
    FailurePolicy,
};
use std::sync::Arc;
use tokio::sync::{watch, Mutex};

/// Gaps up to this many blocks extend through the live scheduler.
pub(crate) const LIVE_BLOCKS: u64 = 8;
/// One resumable backfill batch.
pub(crate) const BACKFILL_BLOCKS: u64 = 64;

/// How reconciliation left the journal relative to one observation.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Coverage {
    /// Committed coverage equals the observed tip.
    Current,
    /// The node is behind committed coverage, whose blocks it still agrees with.
    Ahead,
    /// Blocks `from..=tip` are missing.
    Missing { from: u64 },
}

/// Walk durable checkpoints newest first until one matches the canonical chain,
/// withdrawing then rewinding anything above it. A node behind the journal is
/// not an invalidation: only blocks it reports differently are removed.
pub(crate) async fn reconcile<I: Ingestion>(
    writer: &mut I,
    chain: &dyn CanonicalChain,
    observation: &ChainObservation,
) -> Result<Coverage, Outcome<I::Error>> {
    let tip = &observation.tip;
    let newest = writer.checkpoint_at_or_below(tip.height);
    let mut candidate = newest.clone();
    let keep = loop {
        let Some(block) = candidate else { break None };
        let canonical = if block.height == tip.height {
            tip.hash.clone()
        } else {
            chain.hash_at(block.height).await.map_err(Outcome::Source)?
        };
        if canonical == block.hash {
            break Some(block);
        }
        candidate = block
            .height
            .checked_sub(1)
            .and_then(|below| writer.checkpoint_at_or_below(below));
    };
    if keep != newest {
        writer
            .withdraw_above(keep.as_ref())
            .await
            .map_err(Outcome::Writer)?;
        writer
            .rewind(keep.as_ref())
            .await
            .map_err(Outcome::Writer)?;
    }
    Ok(match writer.committed() {
        Some(block) if block.height > tip.height => Coverage::Ahead,
        Some(block) if block.height == tip.height => Coverage::Current,
        Some(block) => Coverage::Missing {
            from: block.height + 1,
        },
        None if writer.start_height() > tip.height => Coverage::Ahead,
        None => Coverage::Missing {
            from: writer.start_height(),
        },
    })
}

/// Why one scheduled operation did not complete.
pub(crate) enum Outcome<E> {
    /// The follower's source failed; nothing advanced, retry on the next pass.
    Source(pir_pipeline::ingestion::Error),
    /// A writer operation failed; the product's failure policy applies.
    Writer(E),
}

/// Accept a batch only when its newest committed block is canonical; otherwise
/// the next reconciliation withdraws and rewinds it.
async fn anchored<I: Ingestion>(
    writer: &mut I,
    chain: &dyn CanonicalChain,
    from: u64,
    through: u64,
) -> Result<bool, Outcome<I::Error>> {
    writer
        .ingest(from, through)
        .await
        .map_err(Outcome::Writer)?;
    let Some(committed) = writer.committed() else {
        return Ok(false);
    };
    let canonical = chain
        .hash_at(committed.height)
        .await
        .map_err(Outcome::Source)?;
    Ok(canonical == committed.hash)
}

/// The root ingestion executor for one product: a live scheduler following the
/// root observations and a backfill scheduler toward fixed targets, sharing
/// one writer. Shutdown completes the current operation, then drains both.
pub(crate) fn run<'a, I: Ingestion + 'a>(
    ingestion: I,
    feed: ChainFeed,
    shutdown: Shutdown,
) -> IngestionRun<'a, I::Error> {
    Box::pin(async move {
        let writer = Arc::new(Mutex::new(ingestion));
        let targets = Arc::new(watch::channel::<Option<BlockId>>(None).0);
        let live = live(
            writer.clone(),
            feed.clone(),
            targets.clone(),
            shutdown.clone(),
        );
        let pending = targets.subscribe();
        let backfill = backfill(writer, feed, targets, pending, shutdown.clone());
        let (live, backfill) = tokio::join!(
            async {
                let result = live.await;
                // Either scheduler stopping stops its peer after its batch.
                shutdown.request();
                result
            },
            async {
                let result = backfill.await;
                shutdown.request();
                result
            }
        );
        live.and(backfill)
    })
}

/// Apply the writer's failure policy; `Ok` means retry on the next pass.
fn settle<I: Ingestion>(writer: &mut I, outcome: Outcome<I::Error>) -> Result<(), I::Error> {
    match outcome {
        Outcome::Source(error) => {
            tracing::warn!(%error, "chain source unavailable; ingestion retries next pass");
            Ok(())
        }
        Outcome::Writer(error) => match writer.failed(&error) {
            FailurePolicy::NextPoll => {
                tracing::error!(%error, "ingestion deferred");
                Ok(())
            }
            FailurePolicy::Stop => Err(error),
        },
    }
}

async fn live<I: Ingestion>(
    writer: Arc<Mutex<I>>,
    mut feed: ChainFeed,
    targets: Arc<watch::Sender<Option<BlockId>>>,
    shutdown: Shutdown,
) -> Result<(), I::Error> {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            changed = feed.observations.changed() => if changed.is_err() { return Ok(()) },
        }
        let Some(observation) = feed.observations.borrow_and_update().clone() else {
            continue;
        };
        let mut writer = writer.lock().await;
        let pass = async {
            match reconcile(&mut *writer, &*feed.chain, &observation).await? {
                Coverage::Current => writer
                    .caught_up(&observation)
                    .await
                    .map_err(Outcome::Writer),
                Coverage::Ahead => Ok(()),
                Coverage::Missing { from } if observation.tip.height - from < LIVE_BLOCKS => {
                    let through = observation.tip.height;
                    if anchored(&mut *writer, &*feed.chain, from, through).await?
                        && writer.committed().as_ref() == Some(&observation.tip)
                    {
                        writer
                            .caught_up(&observation)
                            .await
                            .map_err(Outcome::Writer)?;
                    }
                    Ok(())
                }
                Coverage::Missing { .. } => {
                    // A long gap belongs to the backfill scheduler; dispatch never waits on it.
                    targets.send_if_modified(|target| {
                        let idle = target.is_none();
                        if idle {
                            *target = Some(observation.tip.clone());
                        }
                        idle
                    });
                    Ok(())
                }
            }
        }
        .await;
        if let Err(outcome) = pass {
            settle(&mut *writer, outcome)?;
        }
    }
}

async fn backfill<I: Ingestion>(
    writer: Arc<Mutex<I>>,
    feed: ChainFeed,
    targets: Arc<watch::Sender<Option<BlockId>>>,
    mut pending: watch::Receiver<Option<BlockId>>,
    shutdown: Shutdown,
) -> Result<(), I::Error> {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            changed = pending.changed() => if changed.is_err() { return Ok(()) },
        }
        let Some(target) = pending.borrow_and_update().clone() else {
            continue;
        };
        let fixed = ChainObservation {
            genesis: String::new(),
            tip: target.clone(),
            pass: 0,
        };
        while !shutdown.requested() {
            let mut writer = writer.lock().await;
            let batch = async {
                match reconcile(&mut *writer, &*feed.chain, &fixed).await? {
                    Coverage::Missing { from } => {
                        let through = target.height.min(from + BACKFILL_BLOCKS - 1);
                        anchored(&mut *writer, &*feed.chain, from, through).await?;
                        Ok(false)
                    }
                    Coverage::Current | Coverage::Ahead => Ok(true),
                }
            }
            .await;
            match batch {
                Ok(true) => break,
                Ok(false) => {}
                Err(outcome) => {
                    // A target that is no longer canonical, or a failed batch,
                    // returns control to the live scheduler's next observation.
                    settle(&mut *writer, outcome)?;
                    break;
                }
            }
            drop(writer);
            tokio::task::yield_now().await;
        }
        // Release the fixed target. The live scheduler reconciles it with its
        // latest observation before the product may publish again.
        targets.send_replace(None);
        pending.borrow_and_update();
    }
}
