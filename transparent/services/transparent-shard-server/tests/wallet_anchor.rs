//! Accepted checkpoints inside published shards, recovered through real HTTP PIR.
mod common;
use common::*;
use std::path::Path;
use transparent_filter::ShardMap;
use transparent_wallet::{
    http::HttpOptions, http::HttpShardTransport, store::ScriptOrigin, Anchor, Completion,
    ScriptEntry, ScriptProvider, StaticChain, StaticScripts, WalletStore, WorkLimits,
};
use transparent_wallet_store::SqliteStore;

fn anchor(height: u64) -> Anchor {
    Anchor {
        height,
        hash: hash_at(height).to_display_hex(),
    }
}
fn scripts() -> StaticScripts {
    StaticScripts(
        (1..=3)
            .map(|tag| ScriptEntry {
                script: script(tag).as_slice().to_vec(),
                origin: ScriptOrigin::Derived,
                required_from: FIRST,
            })
            .collect(),
    )
}
fn expected(
    all: &[Vec<(
        transparent_filter::ScriptBytes,
        transparent_events::TransparentEvent,
    )>],
    through: u64,
) -> transparent_wallet::Ledger {
    let mut events = all
        .iter()
        .flatten()
        .filter(|(s, _)| (1..=3).any(|n| script(n) == *s))
        .filter(|(_, e)| match e {
            transparent_events::TransparentEvent::Receive(e) => u64::from(e.height) <= through,
            transparent_events::TransparentEvent::Spend(e) => u64::from(e.height) <= through,
        })
        .map(|(s, e)| (s.as_slice().to_vec(), *e))
        .collect::<Vec<_>>();
    let mut ledger = transparent_wallet::Ledger::new();
    ledger.replay(&mut events).unwrap();
    ledger
}
#[allow(clippy::too_many_arguments)]
async fn run<P: ScriptProvider + Send + 'static>(
    path: &Path,
    dir: &Path,
    base: &str,
    map: &ShardMap,
    target: Anchor,
    provider: P,
    limits: WorkLimits,
) -> Result<transparent_wallet::SyncReport, transparent_wallet::SyncError> {
    let mut filters = PublishedFilters::load(dir, map);
    let map = map.clone();
    let base = base.to_string();
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut store = SqliteStore::open(path).unwrap();
        let mut provider = provider;
        let mut chain = StaticChain::default();
        // No accepted future headers: the wallet knows only its chosen prefix.
        for h in FIRST.saturating_sub(1)..=target.height {
            chain.hashes.insert(h, hash_at(h).to_display_hex());
        }
        let mut transport = HttpShardTransport::new(base, &HttpOptions::default()).unwrap();
        let geometry = transport.geometry().unwrap();
        transparent_wallet::sync_into(
            &mut store,
            &map,
            0,
            &geometry,
            &chain,
            &mut provider,
            &mut filters,
            &mut transport,
            &limits,
            &target,
        )
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn arbitrary_anchors_survive_restart_overlap_and_exact_rollback() {
    let all = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &all);
    let base = serve(dir.path()).await;
    let db = tempfile::tempdir().unwrap();
    let path = db.path().join("wallet.sqlite");
    for h in [
        FIRST + 4,
        FIRST + 5,
        FIRST + SPAN + 30,
        FIRST + SPAN + 60,
        FIRST + 2 * SPAN + 9,
        FIRST + 2 * SPAN + 10,
        FIRST + 3 * SPAN + 20,
    ] {
        let result = run(
            &path,
            dir.path(),
            &base,
            &map,
            anchor(h),
            scripts(),
            WorkLimits::UNLIMITED,
        )
        .await
        .unwrap();
        assert_eq!(result.completion, Completion::Complete);
        assert_eq!(result.covered_through, h);
        let store = SqliteStore::open(&path).unwrap();
        compare(&store.ledger().unwrap(), &expected(&all, h));
        assert_eq!(store.anchor().unwrap(), Some(anchor(h)));
        for s in scripts().scripts() {
            let ranges = store.coverage(&s.script).unwrap();
            assert!(ranges.iter().all(|r| r.end_height <= h));
            let last = ranges.iter().max_by_key(|r| r.end_height).unwrap();
            assert_eq!(last.terminal_block_hash, anchor(h).hash);
            assert_eq!(
                last.source_anchor.as_ref().unwrap().height,
                map.shard_for_height(h).unwrap().end_height
            );
        }
    }
    let h = FIRST + SPAN + 35;
    let mut store = SqliteStore::open(&path).unwrap();
    store
        .rollback_above(&anchor(h), "accepted common ancestor inside shard")
        .unwrap();
    compare(&store.ledger().unwrap(), &expected(&all, h));
    drop(store);
    let result = run(
        &path,
        dir.path(),
        &base,
        &map,
        anchor(FIRST + 2 * SPAN + 12),
        scripts(),
        WorkLimits::UNLIMITED,
    )
    .await
    .unwrap();
    assert_eq!(result.completion, Completion::Complete);
    assert_eq!(
        result.rolled_back_to, None,
        "partial sealed coverage is not a reorg"
    );
    compare(&result.ledger, &expected(&all, FIRST + 2 * SPAN + 12));
}
#[tokio::test(flavor = "multi_thread")]
async fn pagination_progress_counts_future_records_and_restarts_on_a_new_target() {
    let all = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &all);
    let base = serve(dir.path()).await;
    let db = tempfile::tempdir().unwrap();
    let path = db.path().join("wallet.sqlite");
    let h = FIRST + SPAN + 30;
    let result = run(
        &path,
        dir.path(),
        &base,
        &map,
        anchor(h),
        scripts(),
        WorkLimits {
            max_queries: Some(5),
            max_private_bytes: None,
        },
    )
    .await
    .unwrap();
    assert_ne!(result.completion, Completion::Complete);
    let store = SqliteStore::open(&path).unwrap();
    assert!(!store.pending().unwrap().is_empty());
    assert!(store.anchor().unwrap().is_none());
    drop(store);
    let result = run(
        &path,
        dir.path(),
        &base,
        &map,
        anchor(h),
        scripts(),
        WorkLimits::UNLIMITED,
    )
    .await
    .unwrap();
    assert_eq!(result.completion, Completion::Complete);
    compare(&result.ledger, &expected(&all, h));
    // Re-reading the same page data at a later target must not count stored events twice.
    let result = run(
        &path,
        dir.path(),
        &base,
        &map,
        anchor(h + 20),
        scripts(),
        WorkLimits {
            max_queries: Some(3),
            max_private_bytes: None,
        },
    )
    .await
    .unwrap();
    assert_ne!(result.completion, Completion::Complete);
    let result = run(
        &path,
        dir.path(),
        &base,
        &map,
        anchor(h + 30),
        scripts(),
        WorkLimits::UNLIMITED,
    )
    .await
    .unwrap();
    assert_eq!(result.completion, Completion::Complete);
    compare(&result.ledger, &expected(&all, h + 30));
}
struct NoFutureDiscovery;
impl ScriptProvider for NoFutureDiscovery {
    fn scripts(&mut self) -> Vec<ScriptEntry> {
        vec![ScriptEntry {
            script: script(3).as_slice().to_vec(),
            origin: ScriptOrigin::Derived,
            required_from: FIRST,
        }]
    }
    fn on_activity(&mut self, active: &[Vec<u8>]) -> Vec<ScriptEntry> {
        assert!(active.is_empty(), "future-only script advanced discovery");
        vec![]
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn future_only_pages_do_not_advance_discovery() {
    let all = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &all);
    let base = serve(dir.path()).await;
    let db = tempfile::tempdir().unwrap();
    let path = db.path().join("wallet.sqlite");
    let result = run(
        &path,
        dir.path(),
        &base,
        &map,
        anchor(FIRST + SPAN + 10),
        NoFutureDiscovery,
        WorkLimits::UNLIMITED,
    )
    .await
    .unwrap();
    assert_eq!(result.completion, Completion::Complete);
    assert_eq!(result.ledger.confirmed_balance(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_rejected_and_unpublished_targets_do_not_commit_progress() {
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &chain());
    let base = serve(dir.path()).await;
    let filters = PublishedFilters::load(dir.path(), &map);
    tokio::task::spawn_blocking(move || {
        let mut filters = filters;
        let mut store = SqliteStore::open_in_memory().unwrap();
        let mut transport = HttpShardTransport::new(base, &HttpOptions::default()).unwrap();
        let geometry = transport.geometry().unwrap();
        let mut chain = StaticChain::from_map(&map);
        let mut scripts = scripts();
        let h = FIRST + 17;
        let r = transparent_wallet::sync_into(
            &mut store,
            &map,
            0,
            &geometry,
            &chain,
            &mut scripts,
            &mut filters,
            &mut transport,
            &WorkLimits::UNLIMITED,
            &anchor(h),
        )
        .unwrap();
        assert!(matches!(
            r.completion,
            Completion::Incomplete {
                reason: transparent_wallet::IncompleteReason::ChainUnknown { .. },
                ..
            }
        ));
        chain.hashes.insert(h, "ff".repeat(32));
        assert!(transparent_wallet::sync_into(
            &mut store,
            &map,
            0,
            &geometry,
            &chain,
            &mut scripts,
            &mut filters,
            &mut transport,
            &WorkLimits::UNLIMITED,
            &anchor(h)
        )
        .is_err());
        let boundary = map.shards[0].end_height;
        let conflicting = Anchor {
            height: boundary,
            hash: "ee".repeat(32),
        };
        chain.hashes.insert(boundary, conflicting.hash.clone());
        assert!(
            transparent_wallet::sync_into(
                &mut store,
                &map,
                0,
                &geometry,
                &chain,
                &mut scripts,
                &mut filters,
                &mut transport,
                &WorkLimits::UNLIMITED,
                &conflicting
            )
            .is_err(),
            "publication hash must agree even when the wallet accepts its own target"
        );
        let future = map.shards.last().unwrap().end_height + 1;
        chain.hashes.insert(future, anchor(future).hash);
        let r = transparent_wallet::sync_into(
            &mut store,
            &map,
            0,
            &geometry,
            &chain,
            &mut scripts,
            &mut filters,
            &mut transport,
            &WorkLimits::UNLIMITED,
            &anchor(future),
        )
        .unwrap();
        assert!(matches!(
            r.completion,
            Completion::Incomplete {
                reason: transparent_wallet::IncompleteReason::PublicationBehind { .. },
                ..
            }
        ));
        assert_eq!(store.last_commit().unwrap(), 0);
        assert!(store.anchor().unwrap().is_none());
        assert!(store.events().unwrap().is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replaced_tail_rewinds_pending_events_without_completed_coverage() {
    let mut all = chain();
    let mut pages = Vec::new();
    all[1].retain(|(s, e)| {
        if *s == script(3) {
            let mut e = *e;
            if let transparent_events::TransparentEvent::Receive(r) = &mut e {
                r.height += (2 * SPAN) as u32;
            }
            pages.push((s.clone(), e));
            false
        } else {
            true
        }
    });
    all[3].extend(pages);
    let old_dir = tempfile::tempdir().unwrap();
    let old = publish(old_dir.path(), &all);
    let old_base = serve(old_dir.path()).await;
    let db = tempfile::tempdir().unwrap();
    let path = db.path().join("wallet.sqlite");
    let provider = || {
        StaticScripts(vec![ScriptEntry {
            script: script(3).as_slice().to_vec(),
            origin: ScriptOrigin::Derived,
            required_from: FIRST,
        }])
    };
    let h = FIRST + 3 * SPAN + 30;
    let result = run(
        &path,
        old_dir.path(),
        &old_base,
        &old,
        anchor(h),
        provider(),
        WorkLimits {
            max_queries: Some(3),
            max_private_bytes: None,
        },
    )
    .await
    .unwrap();
    assert_ne!(result.completion, Completion::Complete);
    let store = SqliteStore::open(&path).unwrap();
    assert!(!store.pending().unwrap().is_empty());
    assert!(store
        .coverage(script(3).as_slice())
        .unwrap()
        .iter()
        .all(|r| r.end_height < FIRST + 3 * SPAN));
    drop(store);
    all[3].push((
        script(3),
        transparent_events::TransparentEvent::Receive(transparent_events::ReceiveEvent {
            metadata: None,
            height: (h + 5) as u32,
            txid: txid(99999),
            transaction_index: 0,
            output_index: 0,
            value: 17,
            coinbase: false,
        }),
    ));
    let new_dir = tempfile::tempdir().unwrap();
    let map = publish_with(
        new_dir.path(),
        &all,
        |_| &transparent_shard::layout::RECENT_8K,
        1,
        &old.shards[3].manifest_digest,
        hash_at,
    );
    let base = serve(new_dir.path()).await;
    let result = run(
        &path,
        new_dir.path(),
        &base,
        &map,
        anchor(h + 20),
        provider(),
        WorkLimits::UNLIMITED,
    )
    .await
    .unwrap();
    assert_eq!(result.completion, Completion::Complete);
    assert_eq!(result.rolled_back_to, Some(FIRST + 3 * SPAN - 1));
    assert!(result
        .replaced_revisions
        .contains(&old.shards[3].manifest_digest));
    let mut expected_events = all
        .iter()
        .flatten()
        .filter(|(s, e)| {
            *s == script(3)
                && match e {
                    transparent_events::TransparentEvent::Receive(r) => {
                        u64::from(r.height) <= h + 20
                    }
                    _ => false,
                }
        })
        .map(|(s, e)| (s.as_slice().to_vec(), *e))
        .collect::<Vec<_>>();
    let mut expected = transparent_wallet::Ledger::new();
    expected.replay(&mut expected_events).unwrap();
    compare(&result.ledger, &expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_exact_common_ancestor_inside_a_shard_survives_a_real_fork() {
    let all = chain();
    let old_dir = tempfile::tempdir().unwrap();
    let old = publish(old_dir.path(), &all);
    let base = serve(old_dir.path()).await;
    let db = tempfile::tempdir().unwrap();
    let path = db.path().join("wallet.sqlite");
    let initial = run(
        &path,
        old_dir.path(),
        &base,
        &old,
        anchor(old.shards.last().unwrap().end_height),
        scripts(),
        WorkLimits::UNLIMITED,
    )
    .await
    .unwrap();
    assert_eq!(initial.completion, Completion::Complete);
    let fork = FIRST + SPAN + 35;
    let mut changed = all.clone();
    changed[1].retain(|(s, e)| {
        *s != script(3)
            || match e {
                transparent_events::TransparentEvent::Receive(r) => u64::from(r.height) < fork,
                transparent_events::TransparentEvent::Spend(s) => u64::from(s.height) < fork,
            }
    });
    changed[1].push((
        script(1),
        transparent_events::TransparentEvent::Receive(transparent_events::ReceiveEvent {
            metadata: None,
            height: (fork + 3) as u32,
            txid: txid(88888),
            transaction_index: 0,
            output_index: 0,
            value: 71,
            coinbase: false,
        }),
    ));
    let new_dir = tempfile::tempdir().unwrap();
    let map = publish_with(
        new_dir.path(),
        &changed,
        |_| &transparent_shard::layout::RECENT_8K,
        0,
        "",
        hash_forked(fork),
    );
    let base = serve(new_dir.path()).await;
    let mut store = SqliteStore::open(&path).unwrap();
    store
        .rollback_above(&anchor(fork - 1), "host verified exact common ancestor")
        .unwrap();
    drop(store);
    let target = Anchor {
        height: fork + 20,
        hash: hash_forked(fork)(fork + 20).to_display_hex(),
    };
    let mut filters = PublishedFilters::load(new_dir.path(), &map);
    let result = tokio::task::spawn_blocking(move || {
        let chain = StaticChain {
            hashes: (FIRST - 1..=target.height)
                .map(|h| (h, hash_forked(fork)(h).to_display_hex()))
                .collect(),
        };
        let mut store = SqliteStore::open(path).unwrap();
        let mut transport = HttpShardTransport::new(base, &HttpOptions::default()).unwrap();
        let geometry = transport.geometry().unwrap();
        transparent_wallet::sync_into(
            &mut store,
            &map,
            0,
            &geometry,
            &chain,
            &mut scripts(),
            &mut filters,
            &mut transport,
            &WorkLimits::UNLIMITED,
            &target,
        )
        .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(result.completion, Completion::Complete);
    assert_eq!(
        result.rolled_back_to, None,
        "a verified partial prefix must not be mistaken for a changed full shard"
    );
    compare(&result.ledger, &expected(&changed, fork + 20));
}
