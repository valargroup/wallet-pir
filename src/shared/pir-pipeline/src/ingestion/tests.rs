use super::schedule::{self, BACKFILL_BLOCKS, LIVE_BLOCKS};
use crate::{
    ingestion::{
        BlockId, CanonicalChain, ChainFeed, ChainFollower, ChainFuture, ChainObservation,
        ChainSource, Ingestion,
    },
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

impl ChainSource for Chain {
    fn tip_height(&self) -> ChainFuture<'_, u64> {
        let height = self.hashes.lock().unwrap().len() as u64 - 1;
        Box::pin(async move { Ok(height) })
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
    networks: Arc<Mutex<Vec<String>>>,
    hold: Arc<Mutex<Option<Hold>>>,
}

/// Blocks the batch ending at `through` after it is recorded and before it
/// reads the chain, until one permit is added to `gate`.
#[derive(Clone)]
struct Hold {
    through: u64,
    entered: Arc<tokio::sync::Notify>,
    gate: Arc<tokio::sync::Semaphore>,
}
impl Hold {
    fn at(through: u64) -> Self {
        Self {
            through,
            entered: Arc::default(),
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}

impl Ingestion for Journal {
    type Error = String;
    fn start_height(&self, _tip: u64) -> u64 {
        self.start
    }
    async fn observed(&mut self, observation: &ChainObservation) -> Result<(), String> {
        self.networks
            .lock()
            .unwrap()
            .push(observation.genesis.clone());
        Ok(())
    }
    fn committed(&self) -> Option<BlockId> {
        self.blocks.last().cloned()
    }
    async fn checkpoint_at_or_below(&self, height: u64) -> Option<BlockId> {
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
        let hold = self.hold.lock().unwrap().clone();
        if let Some(hold) = hold.filter(|hold| hold.through == through) {
            hold.entered.notify_one();
            hold.gate.acquire().await.unwrap().forget();
        }
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
    networks: Arc<Mutex<Vec<String>>>,
    hold: Arc<Mutex<Option<Hold>>>,
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
        let networks = Arc::new(Mutex::new(vec![]));
        let hold = Arc::new(Mutex::new(None));
        let (observations, receiver) = watch::channel(None);
        let shutdown = Shutdown::default();
        let journal = Journal {
            start,
            blocks,
            chain: chain.clone(),
            events: events.clone(),
            fail_ingest: fail_ingest.clone(),
            networks: networks.clone(),
            hold: hold.clone(),
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
            networks,
            hold,
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
async fn a_backfill_target_the_chain_moved_past_is_replanned_without_withdrawal() {
    let tip = 10 + BACKFILL_BLOCKS * 2;
    let chain = Chain::new(tip, "a");
    let mut harness = Harness::start(chain.clone(), 0, Some(10));
    let hold = Hold::at(tip);
    *harness.hold.lock().unwrap() = Some(hold.clone());
    harness.observe();
    // The final batch toward fixed target a(tip) is in progress when the node
    // replaces the tip at the same height; the batch then appends b(tip).
    hold.entered.notified().await;
    chain.reorg(tip, tip, "b");
    hold.gate.add_permits(1);
    harness.observe();
    let events = harness.until(|e| caught_up(e, tip)).await;
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::Withdraw(_) | Event::Rewind(_))),
        "canonical b(tip) was never withdrawn: {events:?}"
    );
    assert_eq!(events.last(), Some(&Event::CaughtUp(tip)));
    // Later passes stay current rather than looping on the retired target.
    harness.observe();
    let events = harness
        .until(|e| e.iter().filter(|e| **e == Event::CaughtUp(tip)).count() == 2)
        .await;
    assert!(!events.iter().any(|e| matches!(e, Event::Withdraw(_))));
    harness.stop().await;
}

#[tokio::test]
async fn a_tip_that_grows_past_a_backfill_target_is_still_caught_up() {
    let tip = 10 + BACKFILL_BLOCKS;
    let chain = Chain::new(tip, "a");
    let mut harness = Harness::start(chain.clone(), 0, Some(10));
    let hold = Hold::at(tip);
    *harness.hold.lock().unwrap() = Some(hold.clone());
    harness.observe();
    hold.entered.notified().await;
    // The fixed target ends here. The remaining gap is one block larger than
    // the live scheduler ingests itself, so only a second backfill target
    // reaches the new tip.
    let grown = tip + LIVE_BLOCKS + 1;
    chain.extend(grown, "a");
    hold.gate.add_permits(1);
    let events = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            harness.observe();
            tokio::time::sleep(Duration::from_millis(5)).await;
            let events = harness.events.lock().unwrap().clone();
            if caught_up(&events, grown) {
                return events;
            }
        }
    })
    .await
    .expect("growth beyond the fixed target was ingested");
    assert_eq!(events.last(), Some(&Event::CaughtUp(grown)));
    harness.stop().await;
}

#[tokio::test]
async fn release_target_keeps_a_replacement_published_into_the_empty_slot() {
    let (targets, mut pending) = watch::channel(Some(observation(1, 10)));
    let _ = pending.borrow_and_update();
    let publisher = targets.clone();
    let mut slot = publisher.subscribe();
    let _ = slot.borrow_and_update();
    let live = tokio::spawn(async move {
        slot.changed().await.unwrap();
        assert!(publisher.send_if_modified(|slot| {
            let idle = slot.is_none();
            if idle {
                *slot = Some(observation(2, 30));
            }
            idle
        }));
    });
    tokio::task::yield_now().await;
    let kept = schedule::release_target(&targets, &mut pending).await;
    live.await.unwrap();
    assert_eq!(kept.unwrap().tip.height, 30);
    assert!(
        !targets.send_if_modified(|slot| slot.is_none()),
        "an occupied slot is not published again"
    );
}

fn observation(pass: u64, height: u64) -> ChainObservation {
    ChainObservation {
        genesis: "g".into(),
        tip: BlockId {
            height,
            hash: format!("h{height}"),
        },
        pass,
    }
}

#[tokio::test]
async fn a_retired_backfill_target_is_replanned_from_the_latest_observation() {
    let tip = 10 + BACKFILL_BLOCKS * 3;
    let chain = Chain::new(tip, "a");
    let mut harness = Harness::start(chain.clone(), 0, Some(10));
    let hold = Hold::at(10 + BACKFILL_BLOCKS);
    *harness.hold.lock().unwrap() = Some(hold.clone());
    harness.observe();
    hold.entered.notified().await;
    // The first batch reads the chain after blocks inside and above it, and
    // the fixed target, move to fork b; what it commits is canonical.
    chain.reorg(20, tip, "b");
    hold.gate.add_permits(1);
    let events = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            harness.observe();
            tokio::time::sleep(Duration::from_millis(5)).await;
            let events = harness.events.lock().unwrap().clone();
            if caught_up(&events, tip) {
                return events;
            }
        }
    })
    .await
    .expect("the replanned target was reached");
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, Event::Withdraw(_) | Event::Rewind(_))),
        "canonical blocks were never withdrawn: {events:?}"
    );
    harness.stop().await;
}

#[tokio::test]
async fn one_shot_catch_up_refuses_an_observation_the_chain_moved_past() {
    let tip = 10 + BACKFILL_BLOCKS * 2;
    let chain = Chain::new(tip, "a");
    let events = Arc::new(Mutex::new(vec![]));
    let hold = Hold::at(tip);
    let mut journal = Journal {
        start: 0,
        blocks: (0..=10)
            .map(|height| BlockId {
                height,
                hash: format!("a{height}"),
            })
            .collect(),
        chain: chain.clone(),
        events: events.clone(),
        fail_ingest: Arc::default(),
        networks: Arc::default(),
        hold: Arc::new(Mutex::new(Some(hold.clone()))),
    };
    let frozen = chain.tip(1);
    let reorg = {
        let chain = chain.clone();
        async move {
            hold.entered.notified().await;
            chain.reorg(tip, tip, "b");
            hold.gate.add_permits(1);
        }
    };
    let (stale, ()) = tokio::join!(schedule::catch_up(&mut journal, &*chain, &frozen), reorg);
    assert!(matches!(stale, Err(schedule::Outcome::Source(_))));
    assert_eq!(journal.committed().unwrap().hash, format!("b{tip}"));
    schedule::catch_up(&mut journal, &*chain, &chain.tip(2))
        .await
        .unwrap();
    let events = events.lock().unwrap().clone();
    assert!(!events
        .iter()
        .any(|e| matches!(e, Event::Withdraw(_) | Event::Rewind(_))));
    assert_eq!(events.last(), Some(&Event::CaughtUp(tip)));
}

#[tokio::test]
async fn shutdown_finishes_the_current_batch_and_drains() {
    let chain = Chain::new(10 + BACKFILL_BLOCKS * 50, "a");
    let mut harness = Harness::start(chain, 0, Some(10));
    harness.observe();
    harness.until(|e| !e.is_empty()).await;
    harness.stop().await;
}

#[tokio::test]
async fn another_network_reaches_the_product_but_is_never_reconciled() {
    let chain = Chain::new(40, "a");
    let mut harness = Harness::start(chain.clone(), 0, Some(40));
    harness.observe();
    harness.until(|e| caught_up(e, 40)).await;
    chain.reorg(0, 45, "z");
    harness.observe();
    let events = harness.settled().await;
    assert_eq!(events, vec![Event::CaughtUp(40)]);
    assert_eq!(*harness.networks.lock().unwrap(), ["a0", "z0"]);
    harness.stop().await;
}

/// The next delivered observation satisfying `done`.
async fn next(
    observations: &mut watch::Receiver<Option<ChainObservation>>,
    done: fn(&ChainObservation) -> bool,
) -> ChainObservation {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            observations.changed().await.unwrap();
            let seen = observations.borrow_and_update().clone().unwrap();
            if done(&seen) {
                return seen;
            }
        }
    })
    .await
    .expect("the follower delivered the observation")
}

#[tokio::test]
async fn the_follower_delivers_unchanged_tips_and_network_switches() {
    let chain = Chain::new(5, "a");
    let follower = ChainFollower::new(chain.clone(), Duration::from_millis(2));
    let mut feed = follower.feed();
    let shutdown = Shutdown::default();
    let task = tokio::spawn(follower.run(shutdown.clone()));
    let first = next(&mut feed.observations, |o| o.tip.height == 5).await;
    let repeated = next(&mut feed.observations, |o| o.tip.height == 5).await;
    assert!(repeated.pass > first.pass, "unchanged tips are delivered");
    chain.reorg(0, 3, "z");
    let switched = next(&mut feed.observations, |o| o.genesis == "z0").await;
    assert_eq!(switched.tip.hash, "z3");
    shutdown.request();
    task.await.unwrap();
}

/// A journal whose backfill blocks inside one batch until released.
struct Blocked {
    entered: Arc<tokio::sync::Notify>,
    gate: Arc<tokio::sync::Semaphore>,
}
impl Ingestion for Blocked {
    type Error = String;
    fn start_height(&self, _tip: u64) -> u64 {
        0
    }
    fn committed(&self) -> Option<BlockId> {
        None
    }
    async fn checkpoint_at_or_below(&self, _height: u64) -> Option<BlockId> {
        None
    }
    async fn withdraw_above(&mut self, _: Option<&BlockId>) -> Result<(), String> {
        Ok(())
    }
    async fn rewind(&mut self, _: Option<&BlockId>) -> Result<(), String> {
        Ok(())
    }
    async fn ingest(&mut self, _from: u64, _through: u64) -> Result<(), String> {
        self.entered.notify_one();
        let _permit = self.gate.acquire().await.unwrap();
        Err("released".into())
    }
}

#[tokio::test]
async fn a_blocked_backfill_never_delays_another_products_observations_or_withdrawal() {
    let chain = Chain::new(10 + BACKFILL_BLOCKS * 4, "a");
    let tip = chain.tip(0).tip.height;
    let follower = ChainFollower::new(chain.clone(), Duration::from_millis(2));
    let shutdown = Shutdown::default();
    let (entered, gate) = (
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(tokio::sync::Semaphore::new(0)),
    );
    let blocked = tokio::spawn(schedule::run(
        Blocked {
            entered: entered.clone(),
            gate: gate.clone(),
        },
        follower.feed(),
        shutdown.clone(),
    ));
    let events = Arc::new(Mutex::new(vec![]));
    let current = Journal {
        start: 0,
        blocks: (0..=tip)
            .map(|height| BlockId {
                height,
                hash: format!("a{height}"),
            })
            .collect(),
        chain: chain.clone(),
        events: events.clone(),
        fail_ingest: Arc::default(),
        networks: Arc::default(),
        hold: Arc::default(),
    };
    let scheduled = tokio::spawn(schedule::run(current, follower.feed(), shutdown.clone()));
    let following = tokio::spawn(follower.run(shutdown.clone()));
    type Done = Box<dyn Fn(&[Event]) -> bool + Send>;
    let wait = |done: Done| {
        let events = events.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !done(&events.lock().unwrap()) {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("the unblocked product progressed")
        }
    };
    // The first product is inside a long backfill batch.
    entered.notified().await;
    // The other keeps its unchanged-tip cadence meanwhile.
    wait(Box::new(move |e| {
        e.iter().filter(|e| **e == Event::CaughtUp(tip)).count() >= 5
    }))
    .await;
    // And withdraws before rewinding on a reorg, still while blocked.
    chain.reorg(tip - 1, tip, "b");
    wait(Box::new(move |e| {
        e.contains(&Event::CaughtUp(tip)) && e.contains(&Event::Rewind(Some(tip - 2)))
    }))
    .await;
    let recorded = events.lock().unwrap().clone();
    let withdrawn = recorded
        .iter()
        .position(|e| *e == Event::Withdraw(Some(tip - 2)))
        .expect("withdrawal precedes the rewind");
    assert_eq!(recorded[withdrawn + 1], Event::Rewind(Some(tip - 2)));
    assert_eq!(
        gate.available_permits(),
        0,
        "the backfill was still blocked"
    );
    gate.add_permits(1);
    shutdown.request();
    scheduled.await.unwrap().unwrap();
    blocked.await.unwrap().unwrap();
    following.await.unwrap();
}

const _: () = assert!(LIVE_BLOCKS < BACKFILL_BLOCKS);
