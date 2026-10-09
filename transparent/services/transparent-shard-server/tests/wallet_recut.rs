//! A declared re-cut, as a wallet holding history meets it.
//!
//! The first publication has five sealed `recent-4k` shards and an unsealed
//! tail. The second keeps the first shard byte for byte, merges the next
//! three into one `recent-8k` shard (standing in for `archive-wide`, which is
//! hundreds of megabytes a shard), renumbers the sealed shard above them at a
//! higher revision, renumbers and lengthens the tail, and declares every entry
//! it replaced. Each is served on its own over real HTTP PIR, and a SQLite
//! store synced from the first, holding settled history, provisional tail
//! coverage and unfinished page work, syncs from the second.
//!
//! What a wallet must get: the ledger an independent traversal gives, no
//! private query for any height it still holds, no rollback below the
//! replaced tail, and unfinished work dropped and done again under the shard
//! that now covers it. The same re-cut undeclared is refused, and one
//! published while a sync is reading ends that sync and not the next.
mod common;
use common::*;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use transparent_events::{ReceiveEvent, SpendEvent, TransparentEvent};
use transparent_filter::{Recut, ScriptBytes, ShardMap, ShardMapEntry, SupersededShard};
use transparent_shard::layout::{RECENT_4K, RECENT_8K};
use transparent_wallet::client::Table;
use transparent_wallet::http::{HttpOptions, HttpShardTransport};
use transparent_wallet::transport::{BoxError, FilterSource, ShardTransport};
use transparent_wallet::{
    Anchor, Completion, IncompleteReason, Ledger, ScriptEntry, ScriptOrigin, StaticChain,
    StaticScripts, SyncError, SyncReport, WalletStore, WorkLimits,
};
use transparent_wallet_store::SqliteStore;

/// Where both tails start: the five sealed shards end just below.
const TAIL: u64 = FIRST + 5 * SPAN;
/// The old tail's last block, and every sync's target.
const TARGET: u64 = TAIL + 99;
/// The renumbered tail's last block.
const LONGER_TAIL: u64 = TAIL + 199;

fn receive(height: u64, txid_tag: u64, value: u64) -> TransparentEvent {
    TransparentEvent::Receive(ReceiveEvent {
        metadata: None,
        height: height as u32,
        txid: txid(txid_tag),
        transaction_index: 1,
        output_index: 0,
        value,
        coinbase: false,
    })
}

/// The chain, as one list. Script 1 is received in the first shard and spent
/// inside the re-cut span; script 2 is received below it, above it and in
/// both tails, once above the target; script 3 has a paged history in the
/// tail, script 5 one inside the re-cut span; three hundred others fill every
/// shard's filter.
fn events() -> Vec<(ScriptBytes, TransparentEvent)> {
    let mut events = vec![
        (script(1), receive(FIRST + 5, 100, 50_000)),
        (
            script(1),
            TransparentEvent::Spend(SpendEvent {
                metadata: None,
                height: (FIRST + 3 * SPAN + 10) as u32,
                spending_txid: txid(200),
                transaction_index: 0,
                input_index: 0,
                spent_txid: txid(100),
                spent_output_index: 0,
            }),
        ),
        (script(2), receive(FIRST + SPAN + 3, 300, 7_000)),
        (script(2), receive(FIRST + 4 * SPAN + 7, 301, 7_001)),
        (script(2), receive(TAIL + 7, 302, 7_002)),
        (script(2), receive(TAIL + 150, 303, 7_003)),
    ];
    let long = u64::from(transparent_shard::EVENTS_PER_PAGE + transparent_shard::INLINE_EVENTS + 5);
    for i in 0..long {
        events.push((script(3), receive(TAIL + 20 + i % 60, 1_000 + i, 11)));
        events.push((
            script(5),
            receive(FIRST + 2 * SPAN + 20 + i % 100, 5_000 + i, 13),
        ));
    }
    for slot in 0..6 {
        for tag in 100..400u32 {
            let height = FIRST + slot * SPAN + u64::from(tag % 50);
            events.push((
                script(tag),
                receive(height, u64::from(tag) * 1_000 + slot, 1),
            ));
        }
    }
    events
}

fn sealed(start: u64, end: u64, geometry: &'static transparent_shard::layout::Geometry) -> Laid {
    Laid {
        start,
        end,
        geometry,
        sealed: true,
        revision: 0,
    }
}

/// Five sealed `recent-4k` shards and a tail at revision 0.
fn before_layout() -> Vec<Laid> {
    let mut shards: Vec<Laid> = (0..5)
        .map(|k| sealed(FIRST + k * SPAN, FIRST + (k + 1) * SPAN - 1, &RECENT_4K))
        .collect();
    shards.push(Laid {
        start: TAIL,
        end: TARGET,
        geometry: &RECENT_4K,
        sealed: false,
        revision: 0,
    });
    shards
}

/// Shards 1–3 merged into one `recent-8k` shard; shard 4 renumbered 2 at
/// revision 1; the tail renumbered 3, longer, at revision 1.
fn after_layout() -> Vec<Laid> {
    vec![
        sealed(FIRST, FIRST + SPAN - 1, &RECENT_4K),
        sealed(FIRST + SPAN, FIRST + 4 * SPAN - 1, &RECENT_8K),
        Laid {
            revision: 1,
            ..sealed(FIRST + 4 * SPAN, TAIL - 1, &RECENT_4K)
        },
        Laid {
            start: TAIL,
            end: LONGER_TAIL,
            geometry: &RECENT_4K,
            sealed: false,
            revision: 1,
        },
    ]
}

fn superseded(entry: &ShardMapEntry) -> SupersededShard {
    SupersededShard {
        shard_id: entry.shard_id,
        geometry: entry.geometry.clone(),
        start_height: entry.start_height,
        end_height: entry.end_height,
        terminal_block_hash: entry.terminal_block_hash.clone(),
        manifest_digest: entry.manifest_digest.clone(),
        revision: entry.revision,
        sealed: entry.sealed,
    }
}

/// Both publications, each in its own directory. The second declares every
/// entry of the first from shard 1 up, tail included.
struct Publications {
    before_dir: tempfile::TempDir,
    before: ShardMap,
    after_dir: tempfile::TempDir,
    after: ShardMap,
}

fn publications(events: &[(ScriptBytes, TransparentEvent)]) -> Publications {
    let before_dir = tempfile::tempdir().unwrap();
    let before = publish_laid(before_dir.path(), events, &before_layout(), vec![], hash_at);
    let recut = Recut {
        epoch: 1,
        from_height: FIRST + SPAN,
        superseded: before.shards[1..].iter().map(superseded).collect(),
    };
    let after_dir = tempfile::tempdir().unwrap();
    let after = publish_laid(
        after_dir.path(),
        events,
        &after_layout(),
        vec![recut],
        hash_at,
    );
    assert_eq!(
        before.shards[0], after.shards[0],
        "the prefix below the re-cut is unchanged"
    );
    Publications {
        before_dir,
        before,
        after_dir,
        after,
    }
}

fn wallet(tags: &[u32]) -> StaticScripts {
    StaticScripts(
        tags.iter()
            .map(|tag| ScriptEntry {
                script: script(*tag).as_slice().to_vec(),
                origin: ScriptOrigin::Derived,
                required_from: FIRST,
            })
            .collect(),
    )
}

/// The ledger an independent traversal gives `tags` through `through`.
fn expected(events: &[(ScriptBytes, TransparentEvent)], tags: &[u32], through: u64) -> Ledger {
    let held: Vec<_> = events
        .iter()
        .filter(|(_, event)| event_height(event) <= through)
        .cloned()
        .collect();
    let scripts: Vec<ScriptBytes> = tags.iter().map(|tag| script(*tag)).collect();
    traverse(&[held], &scripts, 0)
}

/// Counts the private queries each shard id receives.
struct Counting {
    inner: HttpShardTransport,
    queries: Arc<Mutex<BTreeMap<u64, u64>>>,
}

impl ShardTransport for Counting {
    fn init(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        self.inner.init()
    }
    fn manifest(&mut self, shard_id: u64, revision: &str) -> Result<(Vec<u8>, u64), BoxError> {
        self.inner.manifest(shard_id, revision)
    }
    fn setup(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        segment: u32,
    ) -> Result<(Vec<u8>, u64), BoxError> {
        self.inner.setup(shard_id, revision, table, segment)
    }
    fn query(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        body: &[u8],
    ) -> Result<Vec<u8>, BoxError> {
        *self.queries.lock().unwrap().entry(shard_id).or_default() += 1;
        self.inner.query(shard_id, revision, table, body)
    }
}

/// Public bytes from the first publication until the map is refetched, then
/// from the second: a wallet holding a map whose publisher re-cut it.
struct Swapped {
    before: PublishedFilters,
    after: PublishedFilters,
    refreshed: bool,
}

impl FilterSource for Swapped {
    fn shard_map(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        self.refreshed = true;
        self.after.shard_map()
    }
    fn filter(&mut self, shard_id: u64) -> Result<(Vec<u8>, u64), BoxError> {
        if self.refreshed {
            self.after.filter(shard_id)
        } else {
            self.before.filter(shard_id)
        }
    }
}

/// One sync of the store at `path` to `target`, against the service at
/// `base`, with every block both publications cover accepted. Returns the
/// private queries it made per shard id.
async fn run(
    path: &Path,
    base: &str,
    filters: impl FilterSource + Send + 'static,
    map: &ShardMap,
    scripts: StaticScripts,
    target: u64,
    limits: WorkLimits,
) -> (Result<SyncReport, SyncError>, BTreeMap<u64, u64>) {
    run_on(hash_at, path, base, filters, map, scripts, target, limits).await
}

/// [`run`] with the wallet's chain given by `chain_hash`.
#[allow(clippy::too_many_arguments)]
async fn run_on(
    chain_hash: fn(u64) -> transparent_filter::BlockHash,
    path: &Path,
    base: &str,
    filters: impl FilterSource + Send + 'static,
    map: &ShardMap,
    scripts: StaticScripts,
    target: u64,
    limits: WorkLimits,
) -> (Result<SyncReport, SyncError>, BTreeMap<u64, u64>) {
    let (path, base, map) = (path.to_owned(), base.to_owned(), map.clone());
    let queries = Arc::new(Mutex::new(BTreeMap::new()));
    let counted = queries.clone();
    let report = tokio::task::spawn_blocking(move || {
        let mut store = SqliteStore::open(path).unwrap();
        let chain = StaticChain {
            hashes: (FIRST - 1..=LONGER_TAIL)
                .map(|height| (height, chain_hash(height).to_display_hex()))
                .collect(),
        };
        let mut transport = Counting {
            inner: HttpShardTransport::new(base, &HttpOptions::default()).unwrap(),
            queries: counted,
        };
        let geometry = transport.inner.geometry().unwrap();
        let (mut filters, mut scripts) = (filters, scripts);
        transparent_wallet::sync_into(
            &mut store,
            &map,
            0,
            &geometry,
            &chain,
            &mut scripts,
            &mut filters,
            &mut transport,
            &limits,
            &Anchor {
                height: target,
                hash: chain_hash(target).to_display_hex(),
            },
        )
    })
    .await
    .unwrap();
    let queries = queries.lock().unwrap().clone();
    (report, queries)
}

fn budget(queries: u64) -> WorkLimits {
    WorkLimits {
        max_queries: Some(queries),
        max_private_bytes: None,
    }
}

fn complete(report: &SyncReport) {
    assert_eq!(report.completion, Completion::Complete);
}

/// Syncs a store from the first publication until it holds settled history
/// through the last sealed shard, provisional coverage of the tail for
/// scripts 1 and 2, and script 3's unfinished pages in the tail.
async fn held_before_the_re_cut(path: &Path, base: &str, sets: &Publications) {
    let dir = sets.before_dir.path();
    let filters = || PublishedFilters::load(dir, &sets.before);
    let (report, _) = run(
        path,
        base,
        filters(),
        &sets.before,
        wallet(&[1, 2, 3]),
        TAIL - 1,
        WorkLimits::UNLIMITED,
    )
    .await;
    complete(&report.unwrap());
    // A budget of one query stops before the first page: the tail's directory
    // answers in one commit, which leaves script 3's pages owed.
    let (report, _) = run(
        path,
        base,
        filters(),
        &sets.before,
        wallet(&[1, 2, 3]),
        TARGET,
        budget(1),
    )
    .await;
    assert!(matches!(
        report.unwrap().completion,
        Completion::Incomplete {
            reason: IncompleteReason::QueryBudget,
            ..
        }
    ));
    let store = SqliteStore::open(path).unwrap();
    let pending = store.pending().unwrap();
    let tail = &sets.before.shards[5];
    assert!(!pending.is_empty());
    assert!(pending
        .iter()
        .all(|item| item.shard_id == 5 && item.revision_digest == tail.manifest_digest));
    let provisional = store.provisional().unwrap();
    assert!(provisional
        .iter()
        .any(|range| range.script == script(2).as_slice()));
    assert!(provisional
        .iter()
        .all(|range| range.revision_digest == tail.manifest_digest));
}

/// The case the declaration exists for: the store keeps everything below the
/// tail under the revisions it read it from, the only private queries go to
/// the renumbered tail, the only rollback is the replaced tail's, and script
/// 3's unfinished pages are redone there.
#[tokio::test(flavor = "multi_thread")]
async fn a_declared_re_cut_keeps_history_and_redoes_unfinished_tail_work() {
    let events = events();
    let sets = publications(&events);
    let before_base = serve(sets.before_dir.path()).await;
    let db = tempfile::tempdir().unwrap();
    let path = db.path().join("wallet.sqlite");
    held_before_the_re_cut(&path, &before_base, &sets).await;

    // Only the re-cut publication is served from here on.
    let base = serve(sets.after_dir.path()).await;
    let (report, queries) = run(
        &path,
        &base,
        PublishedFilters::load(sets.after_dir.path(), &sets.after),
        &sets.after,
        wallet(&[1, 2, 3]),
        TARGET,
        WorkLimits::UNLIMITED,
    )
    .await;
    let report = report.unwrap();
    complete(&report);
    compare(&report.ledger, &expected(&events, &[1, 2, 3], TARGET));
    assert_eq!(
        queries.keys().copied().collect::<Vec<_>>(),
        vec![3],
        "no private query below the tail, whose heights the store still holds"
    );
    assert_eq!(report.matched_shards, vec![3]);
    assert_eq!(
        report.rolled_back_to,
        Some(TAIL - 1),
        "rolled back only from the replaced tail, never as a reorg below it"
    );
    assert_eq!(
        report.replaced_revisions,
        vec![sets.before.shards[5].manifest_digest.clone()]
    );
    assert_eq!(report.provisional.len(), 1);
    assert_eq!(
        report.provisional[0].manifest_digest,
        sets.after.shards[3].manifest_digest
    );
    assert_eq!(report.provisional[0].revision, 1);

    let store = SqliteStore::open(&path).unwrap();
    assert!(store.pending().unwrap().is_empty());
    assert_eq!(
        store.anchor().unwrap().unwrap().height,
        TARGET,
        "the anchor commits on the re-cut map"
    );
    for tag in [1, 2, 3] {
        let held: Vec<_> = store
            .coverage(script(tag).as_slice())
            .unwrap()
            .into_iter()
            .map(|range| (range.start_height, range.end_height, range.revision_digest))
            .collect();
        let mut kept: Vec<_> = sets.before.shards[..5]
            .iter()
            .map(|entry| {
                (
                    entry.start_height,
                    entry.end_height,
                    entry.manifest_digest.clone(),
                )
            })
            .collect();
        kept.push((TAIL, TARGET, sets.after.shards[3].manifest_digest.clone()));
        assert_eq!(held, kept, "script {tag} keeps the revisions it read");
    }
}

/// Without the declaration the same map rewrites sealed history the wallet
/// holds, over blocks its chain still accepts. Whatever the newest stored
/// range (here a provisional tail), that is refused as a sealed rewrite, with
/// nothing rolled back or read; the declared map is then accepted by the
/// same store.
#[tokio::test(flavor = "multi_thread")]
async fn the_same_re_cut_undeclared_is_refused() {
    let events = events();
    let sets = publications(&events);
    let before_base = serve(sets.before_dir.path()).await;
    let db = tempfile::tempdir().unwrap();
    let path = db.path().join("wallet.sqlite");
    held_before_the_re_cut(&path, &before_base, &sets).await;
    let (commits, pending) = {
        let store = SqliteStore::open(&path).unwrap();
        (store.last_commit().unwrap(), store.pending().unwrap())
    };

    let base = serve(sets.after_dir.path()).await;
    let mut undeclared = sets.after.clone();
    undeclared.recuts.clear();
    let (report, queries) = run(
        &path,
        &base,
        PublishedFilters::load(sets.after_dir.path(), &undeclared),
        &undeclared,
        wallet(&[1, 2, 3]),
        TARGET,
        WorkLimits::UNLIMITED,
    )
    .await;
    match report {
        Err(SyncError::SealedRewrite {
            start_height,
            revision_digest,
        }) => {
            assert_eq!(
                start_height,
                FIRST + SPAN,
                "the lowest rewritten sealed shard"
            );
            assert_eq!(revision_digest, sets.before.shards[1].manifest_digest);
        }
        other => panic!("expected a refusal, got {:?}", other.map(|r| r.completion)),
    }
    assert!(queries.is_empty());
    let store = SqliteStore::open(&path).unwrap();
    assert_eq!(store.last_commit().unwrap(), commits);
    assert_eq!(store.pending().unwrap(), pending);
    drop(store);

    let (report, _) = run(
        &path,
        &base,
        PublishedFilters::load(sets.after_dir.path(), &sets.after),
        &sets.after,
        wallet(&[1, 2, 3]),
        TARGET,
        WorkLimits::UNLIMITED,
    )
    .await;
    let report = report.unwrap();
    complete(&report);
    compare(&report.ledger, &expected(&events, &[1, 2, 3], TARGET));
}

/// Syncs a store from the first publication until scripts 1 and 2 hold
/// settled history and provisional tail coverage, and script 5 holds the
/// inline events and unfinished pages of shard 2, its first matched shard,
/// which the re-cut merges away. Returns what shard 2's revision saved.
async fn held_with_merged_pages(
    path: &Path,
    base: &str,
    sets: &Publications,
) -> Vec<transparent_wallet::StoredEvent> {
    let filters = || PublishedFilters::load(sets.before_dir.path(), &sets.before);
    let (report, _) = run(
        path,
        base,
        filters(),
        &sets.before,
        wallet(&[1, 2]),
        TARGET,
        WorkLimits::UNLIMITED,
    )
    .await;
    complete(&report.unwrap());
    let (report, _) = run(
        path,
        base,
        filters(),
        &sets.before,
        wallet(&[1, 2, 5]),
        TARGET,
        budget(1),
    )
    .await;
    assert_ne!(report.unwrap().completion, Completion::Complete);
    let merged = sets.before.shards[2].manifest_digest.clone();
    let store = SqliteStore::open(path).unwrap();
    let pending = store.pending().unwrap();
    assert!(!pending.is_empty());
    assert!(pending
        .iter()
        .all(|item| item.shard_id == 2 && item.revision_digest == merged));
    let saved: Vec<_> = store
        .events()
        .unwrap()
        .into_iter()
        .filter(|stored| stored.revision_digest == merged)
        .collect();
    assert!(
        !saved.is_empty(),
        "the directory's inline events were saved"
    );
    saved
}

/// The coverage a script holds below the tail: start, end and revision.
fn held_below_tail(store: &SqliteStore, tag: u32) -> Vec<(u64, u64, String)> {
    store
        .coverage(script(tag).as_slice())
        .unwrap()
        .into_iter()
        .filter(|range| range.end_height < TAIL)
        .map(|range| (range.start_height, range.end_height, range.revision_digest))
        .collect()
}

/// Unfinished pages under a sealed shard the re-cut merged away. The pages
/// are dropped and read again under the wider shard; what that revision had
/// already saved stays, since the target it was read for is still on the
/// wallet's chain, and nothing above it is rolled back or re-read: the
/// renumbered sealed shard gets no private query, and scripts 1 and 2 keep
/// every revision they read below the tail.
#[tokio::test(flavor = "multi_thread")]
async fn unfinished_work_in_a_merged_shard_is_redone_under_the_wider_one() {
    let events = events();
    let sets = publications(&events);
    let before_base = serve(sets.before_dir.path()).await;
    let db = tempfile::tempdir().unwrap();
    let path = db.path().join("wallet.sqlite");
    let saved = held_with_merged_pages(&path, &before_base, &sets).await;
    let merged = sets.before.shards[2].manifest_digest.clone();
    let kept: Vec<_> = sets.before.shards[..5]
        .iter()
        .map(|entry| {
            (
                entry.start_height,
                entry.end_height,
                entry.manifest_digest.clone(),
            )
        })
        .collect();

    let base = serve(sets.after_dir.path()).await;
    let (report, queries) = run(
        &path,
        &base,
        PublishedFilters::load(sets.after_dir.path(), &sets.after),
        &sets.after,
        wallet(&[1, 2, 5]),
        TARGET,
        WorkLimits::UNLIMITED,
    )
    .await;
    let report = report.unwrap();
    complete(&report);
    compare(&report.ledger, &expected(&events, &[1, 2, 5], TARGET));
    assert!(
        queries.contains_key(&1),
        "the merged shard redoes the pages"
    );
    assert_eq!(queries.get(&0), None);
    assert_eq!(
        queries.get(&2),
        None,
        "the renumbered sealed shard is held under its old revision"
    );
    assert_eq!(report.rolled_back_to, Some(TAIL - 1));
    assert!(report.replaced_revisions.contains(&merged));

    let store = SqliteStore::open(&path).unwrap();
    assert!(store.pending().unwrap().is_empty());
    let events_now = store.events().unwrap();
    for stored in &saved {
        assert!(
            events_now.contains(stored),
            "saved under the merged revision"
        );
    }
    for tag in [1, 2] {
        assert_eq!(
            held_below_tail(&store, tag),
            kept,
            "script {tag} keeps the revisions it read"
        );
    }
    let five = store.coverage(script(5).as_slice()).unwrap();
    assert!(five.iter().any(|range| {
        range.start_height == FIRST + SPAN
            && range.end_height == TAIL - 1 - SPAN
            && range.revision_digest == sets.after.shards[1].manifest_digest
    }));
}

/// Kept events with no coverage over them yet: a budget that stops the sync
/// before the wider shard is read leaves them in the store, with no
/// unfinished work and the script's gap still open, and the balance is not
/// reported as synced. The next sync reads the gap and reaches the
/// traversal's ledger.
#[tokio::test(flavor = "multi_thread")]
async fn kept_events_wait_for_the_wider_shard_across_a_budget_stop() {
    let events = events();
    let sets = publications(&events);
    let before_base = serve(sets.before_dir.path()).await;
    let db = tempfile::tempdir().unwrap();
    let path = db.path().join("wallet.sqlite");
    let saved = held_with_merged_pages(&path, &before_base, &sets).await;

    let base = serve(sets.after_dir.path()).await;
    let filters = || PublishedFilters::load(sets.after_dir.path(), &sets.after);
    let (report, queries) = run(
        &path,
        &base,
        filters(),
        &sets.after,
        wallet(&[1, 2, 5]),
        TARGET,
        budget(0),
    )
    .await;
    let report = report.unwrap();
    assert!(matches!(
        report.completion,
        Completion::Incomplete {
            reason: IncompleteReason::QueryBudget,
            pending: 0,
        }
    ));
    assert!(
        queries.is_empty(),
        "stopped before the wider shard was read"
    );
    assert!(report.covered_through < TARGET);
    {
        let store = SqliteStore::open(&path).unwrap();
        assert!(store.pending().unwrap().is_empty());
        let events_now = store.events().unwrap();
        for stored in &saved {
            assert!(events_now.contains(stored));
        }
        let gap = FIRST + 2 * SPAN;
        assert!(store
            .coverage(script(5).as_slice())
            .unwrap()
            .iter()
            .all(|range| !(range.start_height..=range.end_height).contains(&gap)));
    }

    let (report, _) = run(
        &path,
        &base,
        filters(),
        &sets.after,
        wallet(&[1, 2, 5]),
        TARGET,
        WorkLimits::UNLIMITED,
    )
    .await;
    let report = report.unwrap();
    complete(&report);
    compare(&report.ledger, &expected(&events, &[1, 2, 5], TARGET));
}

/// The rule that refuses an undeclared rewrite asks the wallet's chain about
/// the block each changed sealed range rests on, so an honest reorg is not
/// mistaken for one. Here the chain forks inside shard 2 under a store that
/// holds settled history, a provisional tail and unfinished tail pages; the
/// publisher republishes every shard from there; the wallet rolls back to
/// the last block both branches share and reads the new branch.
#[tokio::test(flavor = "multi_thread")]
async fn an_honest_reorg_of_republished_sealed_shards_rolls_back_rather_than_refusing() {
    let events = events();
    let sets = publications(&events);
    let before_base = serve(sets.before_dir.path()).await;
    let db = tempfile::tempdir().unwrap();
    let path = db.path().join("wallet.sqlite");
    held_before_the_re_cut(&path, &before_base, &sets).await;

    const FORK: u64 = FIRST + 2 * SPAN + 50;
    fn forked(height: u64) -> transparent_filter::BlockHash {
        hash_forked(FORK)(height)
    }
    // The new branch drops script 2's receive in shard 4 and pays script 1
    // past the fork instead.
    let mut branch: Vec<_> = events
        .iter()
        .filter(|(s, event)| !(*s == script(2) && event_height(event) == FIRST + 4 * SPAN + 7))
        .cloned()
        .collect();
    branch.push((script(1), receive(FORK + 10, 4_242, 123)));
    let branch_dir = tempfile::tempdir().unwrap();
    let republished = publish_laid(branch_dir.path(), &branch, &before_layout(), vec![], forked);
    assert_eq!(republished.shards[1], sets.before.shards[1]);
    assert_ne!(republished.shards[2], sets.before.shards[2]);

    let base = serve(branch_dir.path()).await;
    let (report, _) = run_on(
        forked,
        &path,
        &base,
        PublishedFilters::load(branch_dir.path(), &republished),
        &republished,
        wallet(&[1, 2, 3]),
        TARGET,
        WorkLimits::UNLIMITED,
    )
    .await;
    let report = report.unwrap();
    complete(&report);
    assert_eq!(
        report.rolled_back_to,
        Some(FIRST + 2 * SPAN - 1),
        "rolled back to the last stored block both branches share"
    );
    compare(&report.ledger, &expected(&branch, &[1, 2, 3], TARGET));
}

/// Sealing is not delayed for finality, so a publisher can follow a shallow
/// reorg through a shard it has just sealed before the wallet's chain does.
/// Until the chain moves, the republished shard ends on a block the chain
/// rejects: the sync stops as for an unknown block, with nothing rolled back
/// or read, and is not refused as a sealed rewrite. Once the chain follows,
/// the same map is an ordinary reorg.
#[tokio::test(flavor = "multi_thread")]
async fn a_shallow_reorg_the_publisher_followed_first_waits_for_the_chain() {
    let events = events();
    let sets = publications(&events);
    let before_base = serve(sets.before_dir.path()).await;
    let db = tempfile::tempdir().unwrap();
    let path = db.path().join("wallet.sqlite");
    let (report, _) = run(
        &path,
        &before_base,
        PublishedFilters::load(sets.before_dir.path(), &sets.before),
        &sets.before,
        wallet(&[1, 2, 3]),
        TARGET,
        WorkLimits::UNLIMITED,
    )
    .await;
    complete(&report.unwrap());

    // The fork is inside shard 4, the newest sealed shard; the tail, now
    // longer and at a higher revision, covers the wallet's target.
    const FORK: u64 = FIRST + 4 * SPAN + 150;
    fn forked(height: u64) -> transparent_filter::BlockHash {
        hash_forked(FORK)(height)
    }
    let mut branch = events.clone();
    branch.push((script(1), receive(FORK + 10, 4_343, 321)));
    let mut layout = before_layout();
    layout[5] = Laid {
        start: TAIL,
        end: LONGER_TAIL,
        geometry: &RECENT_4K,
        sealed: false,
        revision: 1,
    };
    let branch_dir = tempfile::tempdir().unwrap();
    let republished = publish_laid(branch_dir.path(), &branch, &layout, vec![], forked);
    assert_eq!(republished.shards[3], sets.before.shards[3]);
    assert_ne!(republished.shards[4], sets.before.shards[4]);
    let base = serve(branch_dir.path()).await;
    let commits = SqliteStore::open(&path).unwrap().last_commit().unwrap();

    // The wallet's chain has not seen the reorg.
    let (report, queries) = run(
        &path,
        &base,
        PublishedFilters::load(branch_dir.path(), &republished),
        &republished,
        wallet(&[1, 2, 3]),
        TARGET,
        WorkLimits::UNLIMITED,
    )
    .await;
    let report = report.expect("not refused as a sealed rewrite");
    assert_eq!(
        report.completion,
        Completion::Incomplete {
            reason: IncompleteReason::ChainUnknown { height: TAIL - 1 },
            pending: 0,
        }
    );
    assert!(queries.is_empty());
    assert_eq!(
        SqliteStore::open(&path).unwrap().last_commit().unwrap(),
        commits,
        "nothing rolled back or read"
    );

    // The wallet's chain follows the reorg.
    let (report, _) = run_on(
        forked,
        &path,
        &base,
        PublishedFilters::load(branch_dir.path(), &republished),
        &republished,
        wallet(&[1, 2, 3]),
        TARGET,
        WorkLimits::UNLIMITED,
    )
    .await;
    let report = report.unwrap();
    complete(&report);
    assert_eq!(
        report.rolled_back_to,
        Some(FIRST + 4 * SPAN - 1),
        "rolled back to the last stored block both branches share"
    );
    compare(&report.ledger, &expected(&branch, &[1, 2, 3], TARGET));
}

/// A map cannot hold back a reorg the wallet's own chain shows. The chain
/// reorganises inside the tail, orphaning receives the store holds; the map
/// follows it but also changes the sealed shard below the fork, undeclared,
/// to end on a block the chain rejects, which the wallet cannot settle. The
/// reorg is still rolled back first, on the chain alone, and the orphaned
/// events are gone before the sync stops on the unsettled shard.
#[tokio::test(flavor = "multi_thread")]
async fn a_hostile_change_below_a_reorg_cannot_hold_the_rollback_back() {
    let events = events();
    let sets = publications(&events);
    let before_base = serve(sets.before_dir.path()).await;
    let db = tempfile::tempdir().unwrap();
    let path = db.path().join("wallet.sqlite");
    let (report, _) = run(
        &path,
        &before_base,
        PublishedFilters::load(sets.before_dir.path(), &sets.before),
        &sets.before,
        wallet(&[1, 2, 3]),
        TARGET,
        WorkLimits::UNLIMITED,
    )
    .await;
    complete(&report.unwrap());

    // The fork is inside the tail; the new branch drops script 3's receives
    // at and past it.
    const FORK: u64 = TAIL + 50;
    fn forked(height: u64) -> transparent_filter::BlockHash {
        hash_forked(FORK)(height)
    }
    // The publisher's chain agrees with it except at the last sealed
    // shard's end, where it names a block the wallet's chain rejects.
    fn hostile(height: u64) -> transparent_filter::BlockHash {
        if height == TAIL - 1 {
            transparent_filter::BlockHash::from_internal_bytes([0xee; 32])
        } else {
            forked(height)
        }
    }
    let branch: Vec<_> = events
        .iter()
        .filter(|(s, event)| !(*s == script(3) && event_height(event) >= FORK))
        .cloned()
        .collect();
    assert!(
        branch.len() < events.len(),
        "the reorg orphans held receives"
    );
    let mut layout = before_layout();
    layout[5] = Laid {
        start: TAIL,
        end: LONGER_TAIL,
        geometry: &RECENT_4K,
        sealed: false,
        revision: 1,
    };
    let hostile_dir = tempfile::tempdir().unwrap();
    let map = publish_laid(hostile_dir.path(), &branch, &layout, vec![], hostile);
    assert_eq!(map.shards[3], sets.before.shards[3]);
    assert_ne!(
        map.shards[4], sets.before.shards[4],
        "changed below the fork"
    );
    let base = serve(hostile_dir.path()).await;

    let (report, queries) = run_on(
        forked,
        &path,
        &base,
        PublishedFilters::load(hostile_dir.path(), &map),
        &map,
        wallet(&[1, 2, 3]),
        TARGET,
        WorkLimits::UNLIMITED,
    )
    .await;
    let report = report.unwrap();
    assert_eq!(
        report.rolled_back_to,
        Some(TAIL - 1),
        "rolled back to the last stored block the chain still accepts"
    );
    assert_eq!(
        report.completion,
        Completion::Incomplete {
            reason: IncompleteReason::ChainUnknown { height: TAIL - 1 },
            pending: 0,
        },
        "then stopped on the shard the chain cannot settle"
    );
    assert!(queries.is_empty());
    compare(&report.ledger, &expected(&events, &[1, 2, 3], TAIL - 1));
    let store = SqliteStore::open(&path).unwrap();
    assert!(store
        .events()
        .unwrap()
        .iter()
        .all(|stored| event_height(&stored.event) < TAIL));
}

/// A re-cut published while a sync reads the map before it: the first
/// revision the service no longer holds sends the wallet to the map, which
/// declares a re-cut the sync did not start from. That sync ends diverged;
/// the next starts from the re-cut map and completes without re-reading what
/// the first one covered.
#[tokio::test(flavor = "multi_thread")]
async fn a_re_cut_published_mid_sync_ends_that_sync_and_the_next_completes() {
    let events = events();
    let sets = publications(&events);
    let base = serve(sets.after_dir.path()).await;
    let db = tempfile::tempdir().unwrap();
    let path = db.path().join("wallet.sqlite");
    let swapped = Swapped {
        before: PublishedFilters::load(sets.before_dir.path(), &sets.before),
        after: PublishedFilters::load(sets.after_dir.path(), &sets.after),
        refreshed: false,
    };
    let (report, queries) = run(
        &path,
        &base,
        swapped,
        &sets.before,
        wallet(&[1, 2, 3]),
        TARGET,
        WorkLimits::UNLIMITED,
    )
    .await;
    match report {
        Err(SyncError::MapDiverged(reason)) => assert!(reason.contains("epoch"), "{reason}"),
        other => panic!("expected divergence, got {:?}", other.map(|r| r.completion)),
    }
    assert!(queries.contains_key(&0), "shard 0, unchanged, was read");

    let (report, queries) = run(
        &path,
        &base,
        PublishedFilters::load(sets.after_dir.path(), &sets.after),
        &sets.after,
        wallet(&[1, 2, 3]),
        TARGET,
        WorkLimits::UNLIMITED,
    )
    .await;
    let report = report.unwrap();
    complete(&report);
    compare(&report.ledger, &expected(&events, &[1, 2, 3], TARGET));
    assert_eq!(queries.get(&0), None, "shard 0 is still held");
    assert_eq!(report.map_refreshes, 0);
}
