use super::schedule::{self, BACKFILL_BLOCKS, LIVE_BLOCKS};
use pir_pipeline::{
    ingestion::{BlockId, CanonicalChain, ChainFeed, ChainFuture, ChainObservation, Ingestion},
    service::Shutdown,
    FailurePolicy,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::watch;

/// A chain whose hash at each height can be replaced to model reorgs.
#[derive(Default)]
struct Chain {
    hashes: Mutex<Vec<String>>,
    down: Mutex<bool>,
}
impl Chain {
    fn new(tip: u64, fork: &str) -> Arc<Self> {
        let chain = Arc::new(Self::default());
        chain.extend(tip, fork);
        chain
    }
    /// Make heights from the current length through `tip` canonical on `fork`.
    fn extend(&self, tip: u64, fork: &str) {
        let mut hashes = self.hashes.lock().unwrap();
        for height in hashes.len() as u64..=tip {
            hashes.push(format!("{fork}{height}"));
        }
    }
    fn reorg(&self, from: u64, tip: u64, fork: &str) {
        self.hashes.lock().unwrap().truncate(from as usize);
        self.extend(tip, fork);
    }
    fn tip(&self, pass: u64) -> ChainObservation {
        let hashes = self.hashes.lock().unwrap();
        let height = hashes.len() as u64 - 1;
        ChainObservation {
            genesis: hashes[0].clone(),
            tip: BlockId {
                height,
                hash: hashes[height as usize].clone(),
            },
            pass,
        }
    }
}
impl CanonicalChain for Chain {
    fn hash_at(&self, height: u64) -> ChainFuture<'_, String> {
        let result = if *self.down.lock().unwrap() {
            Err("source outage".into())
        } else {
            self.hashes
                .lock()
                .unwrap()
                .get(height as usize)
                .cloned()
                .ok_or_else(|| "beyond tip".into())
        };
        Box::pin(async move { result })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Withdraw(Option<u64>),
    Rewind(Option<u64>),
    Ingest(u64, u64),
    CaughtUp(u64),
}

/// A journal that records every writer operation in order.
struct Journal {
    start: u64,
    blocks: Vec<BlockId>,
    chain: Arc<Chain>,
    events: Arc<Mutex<Vec<Event>>>,
    fail_ingest: Arc<Mutex<bool>>,
}
impl Ingestion for Journal {
    type Error = String;
    fn start_height(&self) -> u64 {
        self.start
    }
    fn committed(&self) -> Option<BlockId> {
        self.blocks.last().cloned()
    }
    fn checkpoint_at_or_below(&self, height: u64) -> Option<BlockId> {
        self.blocks
            .iter()
            .rev()
            .find(|b| b.height <= height)
            .cloned()
    }
    async fn withdraw_above(&mut self, keep: Option<&BlockId>) -> Result<(), String> {
        self.events
            .lock()
            .unwrap()
            .push(Event::Withdraw(keep.map(|b| b.height)));
        Ok(())
    }
    async fn rewind(&mut self, keep: Option<&BlockId>) -> Result<(), String> {
        self.events
            .lock()
            .unwrap()
            .push(Event::Rewind(keep.map(|b| b.height)));
        self.blocks
            .retain(|block| keep.is_some_and(|keep| block.height <= keep.height));
        Ok(())
    }
    async fn ingest(&mut self, from: u64, through: u64) -> Result<(), String> {
        if *self.fail_ingest.lock().unwrap() {
            return Err("extraction failed".into());
        }
        self.events
            .lock()
            .unwrap()
            .push(Event::Ingest(from, through));
        for height in from..=through {
            let hash = self
                .chain
                .hash_at(height)
                .await
                .map_err(|e| e.to_string())?;
            self.blocks.push(BlockId { height, hash });
        }
        Ok(())
    }
    async fn caught_up(&mut self, observation: &ChainObservation) -> Result<(), String> {
        self.events
            .lock()
            .unwrap()
            .push(Event::CaughtUp(observation.tip.height));
        Ok(())
    }
    fn failed(&mut self, _: &String) -> FailurePolicy {
        FailurePolicy::NextPoll
    }
}

struct Harness {
    chain: Arc<Chain>,
    events: Arc<Mutex<Vec<Event>>>,
    fail_ingest: Arc<Mutex<bool>>,
    observations: watch::Sender<Option<ChainObservation>>,
    shutdown: Shutdown,
    task: tokio::task::JoinHandle<Result<(), String>>,
    pass: u64,
}
impl Harness {
    /// Start scheduling a journal holding canonical blocks `start..=committed`.
    fn start(chain: Arc<Chain>, start: u64, committed: Option<u64>) -> Self {
        let blocks = committed.map_or(vec![], |through| {
            (start..=through)
                .map(|height| BlockId {
                    height,
                    hash: chain.hashes.lock().unwrap()[height as usize].clone(),
                })
                .collect()
        });
        Self::with_blocks(chain, start, blocks)
    }
    fn with_blocks(chain: Arc<Chain>, start: u64, blocks: Vec<BlockId>) -> Self {
        let events = Arc::new(Mutex::new(vec![]));
        let fail_ingest = Arc::new(Mutex::new(false));
        let (observations, receiver) = watch::channel(None);
        let shutdown = Shutdown::default();
        let journal = Journal {
            start,
            blocks,
            chain: chain.clone(),
            events: events.clone(),
            fail_ingest: fail_ingest.clone(),
        };
        let feed = ChainFeed {
            chain: chain.clone(),
            observations: receiver,
        };
        let task = tokio::spawn(schedule::run(journal, feed, shutdown.clone()));
        Self {
            chain,
            events,
            fail_ingest,
            observations,
            shutdown,
            task,
            pass: 0,
        }
    }
    fn observe(&mut self) {
        self.pass += 1;
        self.observations
            .send_replace(Some(self.chain.tip(self.pass)));
    }
    /// Wait until `done` holds for the recorded events.
    async fn until(&self, done: impl Fn(&[Event]) -> bool) -> Vec<Event> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let events = self.events.lock().unwrap().clone();
                if done(&events) {
                    return events;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("scheduler reached the expected state")
    }
    /// Let pending scheduler work settle, then return the recorded events.
    async fn settled(&self) -> Vec<Event> {
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.events.lock().unwrap().clone()
    }
    async fn stop(self) {
        self.shutdown.request();
        self.task.await.unwrap().unwrap();
    }
}

fn caught_up(events: &[Event], height: u64) -> bool {
    events.contains(&Event::CaughtUp(height))
}

#[tokio::test]
async fn unchanged_tip_ingests_nothing_and_signals_each_pass() {
    let chain = Chain::new(120, "a");
    let mut harness = Harness::start(chain, 100, Some(120));
    harness.observe();
    harness.until(|e| caught_up(e, 120)).await;
    harness.observe();
    let events = harness
        .until(|e| e.iter().filter(|e| **e == Event::CaughtUp(120)).count() == 2)
        .await;
    assert_eq!(events, vec![Event::CaughtUp(120), Event::CaughtUp(120)]);
    harness.stop().await;
}

#[tokio::test]
async fn empty_journals_start_at_their_own_height() {
    for start in [3, 9] {
        let chain = Chain::new(10, "a");
        let mut harness = Harness::start(chain, start, None);
        harness.observe();
        let events = harness.until(|e| caught_up(e, 10)).await;
        assert_eq!(events, vec![Event::Ingest(start, 10), Event::CaughtUp(10)]);
        harness.stop().await;
    }
}

#[tokio::test]
async fn ordinary_extension_goes_through_the_live_scheduler() {
    let chain = Chain::new(50, "a");
    let mut harness = Harness::start(chain.clone(), 0, Some(50));
    harness.observe();
    harness.until(|e| caught_up(e, 50)).await;
    chain.extend(52, "a");
    harness.observe();
    let events = harness.until(|e| caught_up(e, 52)).await;
    assert_eq!(
        events,
        vec![
            Event::CaughtUp(50),
            Event::Ingest(51, 52),
            Event::CaughtUp(52)
        ]
    );
    harness.stop().await;
}

#[tokio::test]
async fn real_gap_backfills_in_bounded_batches_without_publication() {
    let gap = BACKFILL_BLOCKS * 2 + 5;
    let chain = Chain::new(10 + gap, "a");
    let mut harness = Harness::start(chain, 0, Some(10));
    harness.observe();
    harness
        .until(|e| e.contains(&Event::Ingest(11 + 2 * BACKFILL_BLOCKS, 10 + gap)))
        .await;
    let events = harness.settled().await;
    // Publication stays suppressed until a live pass reconciles the finished target.
    assert!(!events.iter().any(|e| matches!(e, Event::CaughtUp(_))));
    assert!(events.iter().all(|e| match e {
        Event::Ingest(from, through) => through - from < BACKFILL_BLOCKS,
        _ => true,
    }));
    harness.observe();
    harness.until(|e| caught_up(e, 10 + gap)).await;
    harness.stop().await;
}

#[tokio::test]
async fn skipped_notifications_reconcile_against_durable_checkpoints() {
    let chain = Chain::new(30, "a");
    let mut harness = Harness::start(chain.clone(), 0, Some(30));
    // Several chain changes happen between two delivered observations.
    chain.reorg(28, 31, "b");
    chain.reorg(29, 33, "c");
    harness.observe();
    let events = harness.until(|e| caught_up(e, 33)).await;
    assert_eq!(
        events,
        vec![
            Event::Withdraw(Some(27)),
            Event::Rewind(Some(27)),
            Event::Ingest(28, 33),
            Event::CaughtUp(33)
        ]
    );
    harness.stop().await;
}

#[tokio::test]
async fn same_height_reorg_withdraws_before_rewinding() {
    let chain = Chain::new(20, "a");
    let mut harness = Harness::start(chain.clone(), 0, Some(20));
    chain.reorg(20, 20, "b");
    harness.observe();
    let events = harness.until(|e| caught_up(e, 20)).await;
    assert_eq!(
        events,
        vec![
            Event::Withdraw(Some(19)),
            Event::Rewind(Some(19)),
            Event::Ingest(20, 20),
            Event::CaughtUp(20)
        ]
    );
    harness.stop().await;
}

#[tokio::test]
async fn deep_reorg_below_every_checkpoint_empties_the_journal() {
    let chain = Chain::new(12, "a");
    let mut harness = Harness::start(chain.clone(), 5, Some(12));
    chain.reorg(1, 12, "b");
    harness.observe();
    let events = harness.until(|e| caught_up(e, 12)).await;
    assert_eq!(&events[..2], &[Event::Withdraw(None), Event::Rewind(None)]);
    harness.stop().await;
}

#[tokio::test]
async fn lagging_node_neither_rewinds_nor_publishes() {
    let chain = Chain::new(40, "a");
    let mut harness = Harness::start(chain.clone(), 0, Some(40));
    chain.hashes.lock().unwrap().truncate(36);
    harness.observe();
    assert_eq!(harness.settled().await, vec![]);
    chain.extend(41, "a");
    harness.observe();
    harness.until(|e| caught_up(e, 41)).await;
    harness.stop().await;
}

#[tokio::test]
async fn source_outage_and_writer_failure_do_not_advance() {
    let chain = Chain::new(40, "a");
    let mut harness = Harness::start(chain.clone(), 0, Some(38));
    *chain.down.lock().unwrap() = true;
    harness.observe();
    *harness.fail_ingest.lock().unwrap() = true;
    assert_eq!(harness.settled().await, vec![]);
    *chain.down.lock().unwrap() = false;
    harness.observe();
    assert_eq!(harness.settled().await, vec![]);
    *harness.fail_ingest.lock().unwrap() = false;
    harness.observe();
    let events = harness.until(|e| caught_up(e, 40)).await;
    assert_eq!(events, vec![Event::Ingest(39, 40), Event::CaughtUp(40)]);
    harness.stop().await;
}

#[tokio::test]
async fn reorg_during_backfill_is_withdrawn_between_batches() {
    let tip = 10 + BACKFILL_BLOCKS * 20;
    let chain = Chain::new(tip, "a");
    let mut harness = Harness::start(chain.clone(), 0, Some(10));
    harness.observe();
    harness.until(|e| e.len() >= 2).await;
    // The committed prefix is orphaned while the backfill is still running.
    chain.reorg(5, tip, "b");
    harness.observe();
    let events = harness
        .until(|e| e.contains(&Event::Withdraw(Some(4))))
        .await;
    let withdrawn = events.iter().position(|e| *e == Event::Withdraw(Some(4)));
    assert_eq!(events[withdrawn.unwrap() + 1], Event::Rewind(Some(4)));
    harness.stop().await;
}

#[tokio::test]
async fn shutdown_finishes_the_current_batch_and_drains() {
    let chain = Chain::new(10 + BACKFILL_BLOCKS * 50, "a");
    let mut harness = Harness::start(chain, 0, Some(10));
    harness.observe();
    harness.until(|e| !e.is_empty()).await;
    harness.stop().await;
}

#[test]
fn live_window_is_smaller_than_a_backfill_batch() {
    assert!(LIVE_BLOCKS < BACKFILL_BLOCKS);
}
