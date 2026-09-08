//! A returning wallet: syncs that continue what an earlier sync left in a
//! durable store, against the real service over HTTP.
//!
//! Every case ends by comparing the store's ledger with an independent
//! traversal of the same events, and every recovery path — crash, restart,
//! replaced tail, reorg, budget, overload — has to reach that equality.

mod common;

use common::*;
use std::path::Path;
use transparent_events::{ReceiveEvent, TransparentEvent};
use transparent_filter::{ScriptBytes, ShardMap};
use transparent_shard::layout::{RECENT_4K, RECENT_8K};
use transparent_wallet::client::Table;
use transparent_wallet::http::{HttpOptions, HttpShardTransport};
use transparent_wallet::store::{
    Anchor, CoverageRange, PendingPages, ScriptEntry, ScriptOrigin, SetIdentity, SetupBlob,
    SetupKey, ShardCommit, StoreError, StoredEvent, WalletStore,
};
use transparent_wallet::transport::{refusal, BoxError, ShardTransport};
use transparent_wallet::{
    sync_into, Completion, IncompleteReason, ScriptProvider, StaticChain, StaticScripts, SyncError,
    SyncReport, WorkLimits,
};
use transparent_wallet_store::SqliteStore;

fn entry(tag: u32, required_from: u64) -> ScriptEntry {
    ScriptEntry {
        script: script(tag).as_slice().to_vec(),
        origin: ScriptOrigin::Derived,
        required_from,
    }
}

fn wallet(tags: &[u32]) -> Vec<ScriptBytes> {
    tags.iter().map(|tag| script(*tag)).collect()
}

/// One sync of `store` against the service at `base`, off the async runtime.
#[allow(clippy::too_many_arguments)]
async fn run<S, P, T>(
    store: S,
    base: String,
    dir: &Path,
    map: ShardMap,
    provider: P,
    chain: StaticChain,
    limits: WorkLimits,
    wrap: impl FnOnce(HttpShardTransport) -> T + Send + 'static,
) -> (S, Result<SyncReport, SyncError>)
where
    S: WalletStore + Send + 'static,
    P: ScriptProvider + Send + 'static,
    T: ShardTransport + Send + 'static,
{
    let filters = PublishedFilters::load(dir, &map);
    tokio::task::spawn_blocking(move || {
        let mut store = store;
        let mut provider = provider;
        let mut filters = filters;
        let mut transport = wrap(HttpShardTransport::new(&base, &HttpOptions::default()).unwrap());
        let geometry = transport.geometry().unwrap();
        let map_bytes = serde_json::to_vec(&map).unwrap().len() as u64;
        let result = sync_into(
            &mut store,
            &map,
            map_bytes,
            &geometry,
            &chain,
            &mut provider,
            &mut filters,
            &mut transport,
            &limits,
        );
        (store, result)
    })
    .await
    .unwrap()
}

trait GeometryOf {
    fn geometry(&mut self) -> Result<transparent_wallet::ServiceGeometry, BoxError>;
}

impl<T: ShardTransport> GeometryOf for T {
    fn geometry(&mut self) -> Result<transparent_wallet::ServiceGeometry, BoxError> {
        let (raw, _) = self.init()?;
        transparent_wallet::http::parse_init(&raw)
    }
}

fn fresh_store(dir: &Path) -> SqliteStore {
    SqliteStore::open(dir.join("wallet.sqlite")).unwrap()
}

fn complete(report: &SyncReport) {
    assert_eq!(
        report.completion,
        Completion::Complete,
        "{:?}",
        report.completion
    );
}

/// The receive found last time resolves the spend found this time: the case
/// a one-shot sync cannot handle and a store exists for.
#[tokio::test(flavor = "multi_thread")]
async fn a_spend_synced_later_resolves_against_a_receive_persisted_earlier() {
    let all = chain();
    let dir_a = tempfile::tempdir().unwrap();
    let map_a = publish(dir_a.path(), &all[..2]);
    let dir_b = tempfile::tempdir().unwrap();
    let map_b = publish(dir_b.path(), &all);
    // The prefix agrees: the sealed shards are the same publication.
    assert_eq!(
        map_a.shards[1].manifest_digest,
        map_b.shards[1].manifest_digest
    );
    let store_dir = tempfile::tempdir().unwrap();

    let base = serve(dir_a.path()).await;
    let (store, report) = run(
        fresh_store(store_dir.path()),
        base,
        dir_a.path(),
        map_a.clone(),
        StaticScripts(vec![entry(1, FIRST), entry(2, FIRST), entry(3, FIRST)]),
        StaticChain::from_map(&map_a),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    let report = report.unwrap();
    complete(&report);
    let long = u64::from(transparent_shard::EVENTS_PER_PAGE + transparent_shard::INLINE_EVENTS + 5);
    assert_eq!(
        report.ledger.confirmed_balance(),
        50_000 + 7_000 + 11 * long
    );
    assert!(report.ledger.spends().is_empty());
    compare(&report.ledger, &traverse(&all[..2], &wallet(&[1, 2, 3]), 0));

    let base = serve(dir_b.path()).await;
    let (store, report) = run(
        store,
        base,
        dir_b.path(),
        map_b.clone(),
        StaticScripts(vec![entry(1, FIRST), entry(2, FIRST), entry(3, FIRST)]),
        StaticChain::from_map(&map_b),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    let report = report.unwrap();
    complete(&report);
    assert_eq!(
        report.ledger.spends().len(),
        1,
        "the shard 2 spend resolved"
    );
    assert!(report.ledger.unresolved().is_empty());
    assert_eq!(
        report.matched_shards,
        vec![2, 3],
        "only the new shards were read"
    );
    compare(&report.ledger, &traverse(&all, &wallet(&[1, 2, 3]), 0));
    assert_eq!(
        store.anchor().unwrap().unwrap().height,
        map_b.shards[3].end_height
    );
    assert_eq!(
        report.provisional.len(),
        1,
        "the unsealed tail is provisional"
    );
}

/// A higher required height offered later does not lower what the wallet
/// already required; the receive stays and the spend still resolves.
#[tokio::test(flavor = "multi_thread")]
async fn raising_required_from_after_the_fact_does_not_drop_retained_receives() {
    let all = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &all[..1]);
    let store_dir = tempfile::tempdir().unwrap();
    let base = serve(dir.path()).await;
    let (store, report) = run(
        fresh_store(store_dir.path()),
        base,
        dir.path(),
        map.clone(),
        StaticScripts(vec![entry(1, FIRST)]),
        StaticChain::from_map(&map),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    complete(&report.unwrap());

    let dir_b = tempfile::tempdir().unwrap();
    let map_b = publish(dir_b.path(), &all);
    let base = serve(dir_b.path()).await;
    let (store, report) = run(
        store,
        base,
        dir_b.path(),
        map_b.clone(),
        StaticScripts(vec![entry(1, FIRST + 2 * SPAN)]),
        StaticChain::from_map(&map_b),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    let report = report.unwrap();
    complete(&report);
    assert_eq!(
        store.scripts().unwrap()[0].required_from,
        FIRST,
        "never raised"
    );
    assert_eq!(report.ledger.spends().len(), 1);
    assert!(report.ledger.unresolved().is_empty());
    compare(&report.ledger, &traverse(&all, &wallet(&[1]), 0));
}

/// A query budget stops the sync between pages; every later call resumes
/// from the pending work, and no page is fetched twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_query_budget_stops_between_pages_and_resumes_exactly() {
    let all = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &all);
    let store_dir = tempfile::tempdir().unwrap();
    let base = serve(dir.path()).await;
    let mut store = fresh_store(store_dir.path());
    let mut page_queries = 0u64;
    let mut calls = 0;
    loop {
        let (s, report) = run(
            store,
            base.clone(),
            dir.path(),
            map.clone(),
            StaticScripts(vec![entry(1, FIRST), entry(2, FIRST), entry(3, FIRST)]),
            StaticChain::from_map(&map),
            WorkLimits {
                max_queries: Some(3),
                max_private_bytes: None,
            },
            |t| t,
        )
        .await;
        store = s;
        let report = report.unwrap();
        page_queries += report.charges.pages.queries;
        calls += 1;
        assert!(calls < 40, "the budget loop must converge");
        match report.completion {
            Completion::Complete => {
                compare(&report.ledger, &traverse(&all, &wallet(&[1, 2, 3]), 0));
                break;
            }
            Completion::Incomplete { reason, .. } => {
                assert_eq!(reason, IncompleteReason::QueryBudget);
                assert!(
                    store.anchor().unwrap().is_none(),
                    "no anchor while incomplete"
                );
            }
        }
    }
    // Script 3's history needs pages; every one was fetched exactly once.
    let expected_pages =
        (transparent_shard::EVENTS_PER_PAGE + 5).div_ceil(transparent_shard::EVENTS_PER_PAGE);
    assert_eq!(page_queries, u64::from(expected_pages));
    assert!(calls > 1, "the budget was actually hit");
}

/// The process forgets everything between syncs; the file does not.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_wallet_resumes_from_its_commits_without_refetching() {
    let all = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &all);
    let store_dir = tempfile::tempdir().unwrap();
    let base = serve(dir.path()).await;
    let (store, report) = run(
        fresh_store(store_dir.path()),
        base.clone(),
        dir.path(),
        map.clone(),
        StaticScripts(vec![entry(1, FIRST), entry(2, FIRST), entry(3, FIRST)]),
        StaticChain::from_map(&map),
        WorkLimits {
            max_queries: Some(4),
            max_private_bytes: None,
        },
        |t| t,
    )
    .await;
    let first = report.unwrap();
    assert!(matches!(first.completion, Completion::Incomplete { .. }));
    drop(store);

    let reopened = fresh_store(store_dir.path());
    assert!(reopened.last_commit().unwrap() > 0, "commits survived");
    let (store, report) = run(
        reopened,
        base,
        dir.path(),
        map.clone(),
        StaticScripts(vec![entry(1, FIRST), entry(2, FIRST), entry(3, FIRST)]),
        StaticChain::from_map(&map),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    let second = report.unwrap();
    complete(&second);
    compare(&second.ledger, &traverse(&all, &wallet(&[1, 2, 3]), 0));
    // Filters the first run cached cost nothing the second time.
    assert!(
        second.charges.filter_bytes < first.charges.filter_bytes + 1,
        "second run downloaded {} filter bytes, first {}",
        second.charges.filter_bytes,
        first.charges.filter_bytes
    );
    assert_eq!(store.pending().unwrap().len(), 0);
}

/// A store that fails once, either before or after a chosen commit reaches
/// the inner store: the two sides of the persistence boundary.
struct CrashAt<S> {
    inner: S,
    /// Fail the first commit that records pending page work.
    before: bool,
    armed: bool,
}

impl<S: WalletStore> WalletStore for CrashAt<S> {
    fn set_identity(&self) -> Result<Option<SetIdentity>, StoreError> {
        self.inner.set_identity()
    }
    fn bind_set(&mut self, identity: &SetIdentity) -> Result<(), StoreError> {
        self.inner.bind_set(identity)
    }
    fn anchor(&self) -> Result<Option<Anchor>, StoreError> {
        self.inner.anchor()
    }
    fn scripts(&self) -> Result<Vec<ScriptEntry>, StoreError> {
        self.inner.scripts()
    }
    fn add_scripts(&mut self, entries: &[ScriptEntry]) -> Result<usize, StoreError> {
        self.inner.add_scripts(entries)
    }
    fn coverage(&self, script: &[u8]) -> Result<Vec<CoverageRange>, StoreError> {
        self.inner.coverage(script)
    }
    fn provisional(&self) -> Result<Vec<CoverageRange>, StoreError> {
        self.inner.provisional()
    }
    fn events(&self) -> Result<Vec<StoredEvent>, StoreError> {
        self.inner.events()
    }
    fn commit_shard(&mut self, commit: ShardCommit) -> Result<u64, StoreError> {
        let creates_pending = commit.pending_upsert.iter().any(|p| p.id.is_none());
        if self.armed && creates_pending {
            self.armed = false;
            if self.before {
                return Err(StoreError::Io("crash before the commit".into()));
            }
            self.inner.commit_shard(commit)?;
            return Err(StoreError::Io("crash after the commit".into()));
        }
        self.inner.commit_shard(commit)
    }
    fn commit_anchor(
        &mut self,
        anchor: &Anchor,
        settled: u64,
        covered: u64,
    ) -> Result<u64, StoreError> {
        self.inner.commit_anchor(anchor, settled, covered)
    }
    fn rollback_above(&mut self, height: u64, reason: &str) -> Result<u64, StoreError> {
        self.inner.rollback_above(height, reason)
    }
    fn promote_provisional(&mut self, shard_id: u64, digest: &str) -> Result<(), StoreError> {
        self.inner.promote_provisional(shard_id, digest)
    }
    fn pending(&self) -> Result<Vec<PendingPages>, StoreError> {
        self.inner.pending()
    }
    fn pending_limit(&self) -> usize {
        self.inner.pending_limit()
    }
    fn setup(&self, key: &SetupKey) -> Result<Option<SetupBlob>, StoreError> {
        self.inner.setup(key)
    }
    fn put_setup(&mut self, key: &SetupKey, blob: &SetupBlob) -> Result<(), StoreError> {
        self.inner.put_setup(key, blob)
    }
    fn filter(&self, digest: &str, hash: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.inner.filter(digest, hash)
    }
    fn put_filter(
        &mut self,
        digest: &str,
        hash: &str,
        sealed: bool,
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        self.inner.put_filter(digest, hash, sealed, bytes)
    }
    fn last_commit(&self) -> Result<u64, StoreError> {
        self.inner.last_commit()
    }
}

/// A crash on either side of the directory commit that records page work
/// leaves the store consistent, and the next sync finishes exactly, fetching
/// each page once in total.
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_around_the_pending_commit_resumes_without_duplicating_pages() {
    for before in [true, false] {
        let all = chain();
        let dir = tempfile::tempdir().unwrap();
        let map = publish(dir.path(), &all);
        let store_dir = tempfile::tempdir().unwrap();
        let base = serve(dir.path()).await;
        let crashing = CrashAt {
            inner: fresh_store(store_dir.path()),
            before,
            armed: true,
        };
        let (crashed, report) = run(
            crashing,
            base.clone(),
            dir.path(),
            map.clone(),
            StaticScripts(vec![entry(1, FIRST), entry(2, FIRST), entry(3, FIRST)]),
            StaticChain::from_map(&map),
            WorkLimits::UNLIMITED,
            |t| t,
        )
        .await;
        let error = report.err().expect("the crash surfaces as an error");
        assert!(
            matches!(error, SyncError::Store(StoreError::Io(_))),
            "{error}"
        );
        let first_pages = 0u64;
        let inner = crashed.inner;
        let pending_after_crash = inner.pending().unwrap().len();
        if before {
            assert_eq!(pending_after_crash, 0, "the refused commit wrote nothing");
        } else {
            assert_eq!(pending_after_crash, 1, "the commit landed before the crash");
        }
        drop(inner);

        let (store, report) = run(
            fresh_store(store_dir.path()),
            base,
            dir.path(),
            map.clone(),
            StaticScripts(vec![entry(1, FIRST), entry(2, FIRST), entry(3, FIRST)]),
            StaticChain::from_map(&map),
            WorkLimits::UNLIMITED,
            |t| t,
        )
        .await;
        let report = report.unwrap();
        complete(&report);
        compare(&report.ledger, &traverse(&all, &wallet(&[1, 2, 3]), 0));
        let expected_pages =
            (transparent_shard::EVENTS_PER_PAGE + 5).div_ceil(transparent_shard::EVENTS_PER_PAGE);
        assert_eq!(
            first_pages + report.charges.pages.queries,
            u64::from(expected_pages),
            "before={before}: every page fetched exactly once across both runs"
        );
        assert!(store.pending().unwrap().is_empty());
    }
}

/// A wallet whose rules add a script after activity gets that script's
/// whole required range discovered, and an imported script gets its own.
struct GapLimit {
    given: Vec<ScriptEntry>,
    advanced: bool,
}

impl ScriptProvider for GapLimit {
    fn scripts(&mut self) -> Vec<ScriptEntry> {
        self.given.clone()
    }
    fn on_activity(&mut self, active: &[Vec<u8>]) -> Vec<ScriptEntry> {
        if self.advanced || !active.contains(&script(1).as_slice().to_vec()) {
            return Vec::new();
        }
        self.advanced = true;
        vec![entry(2, FIRST)]
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn added_and_imported_scripts_are_discovered_over_their_earlier_ranges() {
    let all = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &all);
    let store_dir = tempfile::tempdir().unwrap();
    let base = serve(dir.path()).await;
    // Gap advance: script 1 is active, so the wallet's rule adds script 2,
    // which is then discovered from the start.
    let (store, report) = run(
        fresh_store(store_dir.path()),
        base.clone(),
        dir.path(),
        map.clone(),
        GapLimit {
            given: vec![entry(1, FIRST)],
            advanced: false,
        },
        StaticChain::from_map(&map),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    let report = report.unwrap();
    complete(&report);
    assert_eq!(report.scripts_added, 1);
    compare(&report.ledger, &traverse(&all, &wallet(&[1, 2]), 0));

    // An imported script with its history in shard 1 only, required from
    // the set's first height: discovered without re-reading anything else.
    let mut imported = entry(3, FIRST);
    imported.origin = ScriptOrigin::Imported;
    let (_store, report) = run(
        store,
        base,
        dir.path(),
        map.clone(),
        StaticScripts(vec![entry(1, FIRST), entry(2, FIRST), imported]),
        StaticChain::from_map(&map),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    let report = report.unwrap();
    complete(&report);
    assert_eq!(
        report.matched_shards,
        vec![1],
        "only script 3's shard held new history"
    );
    compare(&report.ledger, &traverse(&all, &wallet(&[1, 2, 3]), 0));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unused_wallet_persists_coverage_and_pays_only_cached_filters_next_time() {
    let all = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &all);
    let store_dir = tempfile::tempdir().unwrap();
    let base = serve(dir.path()).await;
    let (store, report) = run(
        fresh_store(store_dir.path()),
        base.clone(),
        dir.path(),
        map.clone(),
        StaticScripts(vec![entry(999, FIRST)]),
        StaticChain::from_map(&map),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    let first = report.unwrap();
    complete(&first);
    assert_eq!(first.charges.queries(), 0);
    assert!(first.charges.filter_bytes > 0);
    assert_eq!(first.ledger.confirmed_balance(), 0);
    assert!(
        store.anchor().unwrap().is_some(),
        "an empty history is still synced"
    );
    let (_, report) = run(
        store,
        base,
        dir.path(),
        map.clone(),
        StaticScripts(vec![entry(999, FIRST)]),
        StaticChain::from_map(&map),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    let second = report.unwrap();
    complete(&second);
    assert_eq!(second.charges.queries(), 0);
    assert_eq!(
        second.charges.filter_bytes, 0,
        "everything was covered already"
    );
}

/// A provisional tail the publisher has replaced is truncated and re-derived
/// under the new revision, never appended to.
#[tokio::test(flavor = "multi_thread")]
async fn a_superseded_provisional_tail_is_truncated_and_replayed_not_appended() {
    let before = chain();
    let dir_a = tempfile::tempdir().unwrap();
    let map_a = publish(dir_a.path(), &before);
    let mut after = before.clone();
    after[3].push((
        script(2),
        TransparentEvent::Receive(ReceiveEvent {
            height: (FIRST + 3 * SPAN + 150) as u32,
            txid: txid(777),
            transaction_index: 4,
            output_index: 0,
            value: 9_000,
            coinbase: false,
        }),
    ));
    let dir_b = tempfile::tempdir().unwrap();
    let map_b = publish_with(
        dir_b.path(),
        &after,
        |_| &RECENT_8K,
        1,
        &map_a.shards[3].manifest_digest,
        hash_at,
    );
    assert_ne!(
        map_a.shards[3].manifest_digest,
        map_b.shards[3].manifest_digest
    );
    let store_dir = tempfile::tempdir().unwrap();

    let base = serve(dir_a.path()).await;
    let (store, report) = run(
        fresh_store(store_dir.path()),
        base,
        dir_a.path(),
        map_a.clone(),
        StaticScripts(vec![entry(1, FIRST), entry(2, FIRST)]),
        StaticChain::from_map(&map_a),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    let first = report.unwrap();
    complete(&first);
    assert_eq!(
        first.provisional[0].manifest_digest,
        map_a.shards[3].manifest_digest
    );

    let base = serve(dir_b.path()).await;
    let (store, report) = run(
        store,
        base,
        dir_b.path(),
        map_b.clone(),
        StaticScripts(vec![entry(1, FIRST), entry(2, FIRST)]),
        StaticChain::from_map(&map_b),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    let second = report.unwrap();
    complete(&second);
    assert_eq!(
        second.replaced_revisions,
        vec![map_a.shards[3].manifest_digest.clone()]
    );
    assert_eq!(
        second.rolled_back_to,
        Some(map_b.shards[3].start_height - 1)
    );
    assert_eq!(
        second.matched_shards,
        vec![3],
        "only the tail was re-derived"
    );
    assert_eq!(second.provisional.len(), 1);
    assert_eq!(
        second.provisional[0].manifest_digest,
        map_b.shards[3].manifest_digest
    );
    assert_eq!(second.provisional[0].revision, 1);
    compare(&second.ledger, &traverse(&after, &wallet(&[1, 2]), 0));
    assert_eq!(
        second.ledger.confirmed_balance(),
        7_000 + 7_001 + 9_000,
        "the receive under the old revision is counted once, from the new one"
    );
    let _ = store;
}

/// A reorg below the tier boundary: sealed archive shards change under the
/// wallet, its chain rejects the old terminal hashes, and everything above the
/// accepted ancestor is rolled back and re-derived exactly.
#[tokio::test(flavor = "multi_thread")]
async fn a_reorg_below_the_tier_boundary_rewinds_to_the_accepted_ancestor() {
    let all = chain();
    let two_tier = |shard: u64| -> &'static transparent_shard::layout::Geometry {
        if shard < 2 {
            &RECENT_4K
        } else {
            &RECENT_8K
        }
    };
    let dir_a = tempfile::tempdir().unwrap();
    let map_a = publish_with(dir_a.path(), &all, two_tier, 0, "", hash_at);
    let fork = FIRST + SPAN + 100; // inside shard 1, the archive side
    let mut forked = all.clone();
    // The reorganised chain drops script 3's history past the fork and adds a
    // receive for script 1 instead.
    forked[1].retain(|(s, event)| {
        s.as_slice() != script(3).as_slice()
            || match event {
                TransparentEvent::Receive(r) => u64::from(r.height) < fork,
                TransparentEvent::Spend(sp) => u64::from(sp.height) < fork,
            }
    });
    forked[1].push((
        script(1),
        TransparentEvent::Receive(ReceiveEvent {
            height: (fork + 1) as u32,
            txid: txid(4242),
            transaction_index: 5,
            output_index: 0,
            value: 123,
            coinbase: false,
        }),
    ));
    let dir_b = tempfile::tempdir().unwrap();
    let map_b = publish_with(dir_b.path(), &forked, two_tier, 0, "", hash_forked(fork));
    assert_ne!(
        map_a.shards[1].terminal_block_hash,
        map_b.shards[1].terminal_block_hash
    );
    assert_eq!(
        map_a.shards[0].terminal_block_hash,
        map_b.shards[0].terminal_block_hash
    );
    let store_dir = tempfile::tempdir().unwrap();

    let base = serve(dir_a.path()).await;
    let (store, report) = run(
        fresh_store(store_dir.path()),
        base,
        dir_a.path(),
        map_a.clone(),
        StaticScripts(vec![entry(1, FIRST), entry(2, FIRST), entry(3, FIRST)]),
        StaticChain::from_map(&map_a),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    complete(&report.unwrap());

    let base = serve(dir_b.path()).await;
    let (store, report) = run(
        store,
        base,
        dir_b.path(),
        map_b.clone(),
        StaticScripts(vec![entry(1, FIRST), entry(2, FIRST), entry(3, FIRST)]),
        StaticChain::from_map(&map_b),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    let report = report.unwrap();
    complete(&report);
    assert_eq!(
        report.rolled_back_to,
        Some(map_a.shards[0].end_height),
        "rolled back to the last block both chains share"
    );
    assert_eq!(report.matched_shards, vec![1, 2, 3]);
    compare(&report.ledger, &traverse(&forked, &wallet(&[1, 2, 3]), 0));
    assert_eq!(
        store.anchor().unwrap().unwrap().hash,
        map_b.shards[3].terminal_block_hash
    );
    for range in store.coverage(script(1).as_slice()).unwrap() {
        assert_ne!(
            range.terminal_block_hash,
            map_a.shards[1].terminal_block_hash
        );
    }
}

/// A map from another set, or one that ends below an anchor the chain still
/// accepts, is refused before anything is read and leaves the store as it was.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_refresh_that_names_a_lower_anchor_or_another_set_is_refused() {
    let all = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &all);
    let store_dir = tempfile::tempdir().unwrap();
    let base = serve(dir.path()).await;
    let (store, report) = run(
        fresh_store(store_dir.path()),
        base.clone(),
        dir.path(),
        map.clone(),
        StaticScripts(vec![entry(1, FIRST), entry(2, FIRST)]),
        StaticChain::from_map(&map),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    complete(&report.unwrap());
    let commits_before = store.last_commit().unwrap();

    // Another set: same shape, different genesis.
    let mut other = map.clone();
    other.genesis_hash = "ff".repeat(32);
    let (store, report) = run(
        store,
        base.clone(),
        dir.path(),
        other.clone(),
        StaticScripts(vec![entry(1, FIRST)]),
        StaticChain::from_map(&other),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    assert!(
        matches!(report, Err(SyncError::MapDiverged(_))),
        "{:?}",
        report.err()
    );
    assert_eq!(store.last_commit().unwrap(), commits_before);

    // A shorter map, while the chain still accepts the anchor the store holds.
    let dir_short = tempfile::tempdir().unwrap();
    let short = publish(dir_short.path(), &all[..2]);
    let (store, report) = run(
        store,
        base,
        dir_short.path(),
        short,
        StaticScripts(vec![entry(1, FIRST)]),
        StaticChain::from_map(&map),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    match report {
        Err(SyncError::AnchorRegressed { stored, offered }) => {
            assert_eq!(stored, map.shards[3].end_height);
            assert_eq!(offered, map.shards[1].end_height);
        }
        other => panic!("expected an anchor regression, got {:?}", other.err()),
    }
    assert_eq!(store.last_commit().unwrap(), commits_before);
}

/// Refuses every query after the first `allow` with a retryable overload.
struct OverloadedAfter<T> {
    inner: T,
    allow: u32,
}

impl<T: ShardTransport> ShardTransport for OverloadedAfter<T> {
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
        if self.allow == 0 {
            let body = br#"{"error":"no cache capacity is free","retry":"retry shortly"}"#;
            return Err(refusal(503, Some("0"), body, shard_id, revision).expect("a refusal"));
        }
        self.allow -= 1;
        self.inner.query(shard_id, revision, table, body)
    }
}

/// An overload that never lifts leaves the pending work in the store, no
/// anchor, and a coverage figure that stops where the work stopped; the next
/// sync against a healthy service finishes exactly.
#[tokio::test(flavor = "multi_thread")]
async fn an_unrelenting_overload_persists_pending_and_never_reports_a_balance_as_synced() {
    let all = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &all);
    let store_dir = tempfile::tempdir().unwrap();
    let base = serve(dir.path()).await;
    // Shard 0: script 1 (2 directory queries). Shard 1: scripts 2 and 3
    // (4 directory queries). Then every page query is refused.
    let (store, report) = run(
        fresh_store(store_dir.path()),
        base.clone(),
        dir.path(),
        map.clone(),
        StaticScripts(vec![entry(1, FIRST), entry(2, FIRST), entry(3, FIRST)]),
        StaticChain::from_map(&map),
        WorkLimits::UNLIMITED,
        |t| OverloadedAfter { inner: t, allow: 6 },
    )
    .await;
    let report = report.unwrap();
    match report.completion {
        Completion::Incomplete {
            reason: IncompleteReason::Overloaded { shard_id },
            pending,
        } => {
            assert_eq!(shard_id, 1);
            assert_eq!(pending, 1, "script 3's page work is owed");
        }
        other => panic!("expected an overload, got {other:?}"),
    }
    assert!(
        store.anchor().unwrap().is_none(),
        "no anchor for an incomplete sync"
    );
    assert_eq!(
        report.covered_through, map.shards[0].end_height,
        "coverage stops at the last shard every script is covered through"
    );
    assert_eq!(store.pending().unwrap().len(), 1);

    let (store, report) = run(
        store,
        base,
        dir.path(),
        map.clone(),
        StaticScripts(vec![entry(1, FIRST), entry(2, FIRST), entry(3, FIRST)]),
        StaticChain::from_map(&map),
        WorkLimits::UNLIMITED,
        |t| t,
    )
    .await;
    let report = report.unwrap();
    complete(&report);
    compare(&report.ledger, &traverse(&all, &wallet(&[1, 2, 3]), 0));
    assert!(store.pending().unwrap().is_empty());
    assert!(store.anchor().unwrap().is_some());
}
