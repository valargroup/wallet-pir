//! The store contract, runnable against any [`WalletStore`].
//!
//! Every rule the sync relies on is exercised here without a network: a
//! refused commit writes nothing, a repeated commit changes nothing, a repeat
//! that differs is a contradiction, a rollback removes exactly the suffix,
//! setup reuse is keyed by set and revision, the pending bound refuses a whole
//! commit, and an empty store says so. The reference stores run it, and so
//! should any wallet that brings its own storage: a store that passes cannot
//! drift from the two the sync is tested over.
//!
//! Behind the `testing` feature so a wallet's own test suite can call
//! [`suite`] with a constructor for its store.

use crate::client::Table;
use crate::ledger::LedgerError;
use crate::store::{
    Anchor, CoverageKind, PendingPages, ScriptEntry, ScriptOrigin, SetIdentity, SetupBlob,
    SetupKey, ShardCommit, StoreError, StoredEvent, WalletStore,
};
use std::collections::BTreeMap;
use transparent_events::{ReceiveEvent, SpendEvent, TransparentEvent, Txid};
use transparent_filter::SealParameters;

pub fn txid(tag: u8) -> Txid {
    Txid([tag; 32])
}

pub fn script(tag: u8) -> Vec<u8> {
    vec![0x76, 0xa9, 0x14, tag]
}

pub fn receive(tag: u8, index: u32, value: u64, height: u32) -> StoredEvent {
    StoredEvent {
        script: script(tag),
        event: TransparentEvent::Receive(ReceiveEvent {
            height,
            txid: txid(tag),
            transaction_index: 0,
            output_index: index,
            value,
            coinbase: false,
        }),
        shard_id: 0,
        revision_digest: "r0".into(),
    }
}

pub fn spend(script_tag: u8, spent: u8, spent_index: u32, spender: u8, height: u32) -> StoredEvent {
    StoredEvent {
        script: script(script_tag),
        event: TransparentEvent::Spend(SpendEvent {
            height,
            spending_txid: txid(spender),
            transaction_index: 1,
            input_index: 0,
            spent_txid: txid(spent),
            spent_output_index: spent_index,
        }),
        shard_id: 0,
        revision_digest: "r0".into(),
    }
}

pub fn identity() -> SetIdentity {
    SetIdentity {
        network: "main".into(),
        genesis_hash: "00".repeat(32),
        profile: "zcash-transparent-range-v1".into(),
        range_envelope_version: 1,
        start_height: 0,
        seal: BTreeMap::from([(
            "recent-8k".to_string(),
            SealParameters {
                max_scripts: 1,
                max_page_rows: 1,
                max_txids: 0,
            },
        )]),
    }
}

pub fn commit(
    shard_id: u64,
    revision: &str,
    sealed: bool,
    range: (u64, u64),
    events: Vec<StoredEvent>,
    covered: Vec<Vec<u8>>,
) -> ShardCommit {
    ShardCommit {
        source_anchor: None,
        shard_id,
        revision_digest: revision.into(),
        sealed,
        start_height: range.0,
        end_height: range.1,
        terminal_block_hash: format!("{:064x}", range.1),
        events: events
            .into_iter()
            .map(|mut stored| {
                stored.shard_id = shard_id;
                stored.revision_digest = revision.into();
                stored
            })
            .collect(),
        covered_scripts: covered,
        pending_upsert: Vec::new(),
        pending_complete: Vec::new(),
    }
}

pub fn pending(script_tag: u8, revision: &str) -> PendingPages {
    PendingPages {
        validated_events: 0,
        boundary: None,
        target_anchor: None,
        id: None,
        shard_id: 1,
        revision_digest: revision.into(),
        script: script(script_tag),
        first_page: 4,
        page_count: 3,
        inline: Vec::new(),
        next_ordinal: 0,
        attempts: 0,
    }
}

/// The contract, over any store.
/// The contract, over any store. `make` returns a fresh, empty store each
/// time; its pending limit should be small (the reference tests use 8) so the
/// bound is exercised cheaply.
pub fn suite<S: WalletStore>(make: impl Fn() -> S) {
    an_empty_store_reports_nothing(&make);
    a_commit_is_idempotent_and_a_differing_repeat_is_refused_whole(&make);
    coverage_merges_settled_ranges_and_keeps_provisional_ones_apart(&make);
    rollback_removes_only_the_suffix(&make);
    a_retried_spend_cannot_change_its_script(&make);
    setup_is_keyed_by_set_revision_table_and_segment(&make);
    a_commit_past_the_pending_bound_is_refused_whole(&make);
    required_from_is_never_raised_and_the_set_is_bound_once(&make);
    promotion_settles_a_provisional_revision_in_place(&make);
}

fn an_empty_store_reports_nothing<S: WalletStore>(make: &impl Fn() -> S) {
    let store = make();
    assert!(store.set_identity().unwrap().is_none());
    assert!(store.anchor().unwrap().is_none());
    assert!(store.scripts().unwrap().is_empty());
    assert!(store.events().unwrap().is_empty());
    assert!(store.pending().unwrap().is_empty());
    assert_eq!(store.last_commit().unwrap(), 0);
    let ledger = store.ledger().unwrap();
    assert_eq!(ledger.confirmed_balance(), 0);
    assert!(ledger.history().is_empty());
}

fn a_commit_is_idempotent_and_a_differing_repeat_is_refused_whole<S: WalletStore>(
    make: &impl Fn() -> S,
) {
    let mut store = make();
    store.bind_set(&identity()).unwrap();
    let first = commit(
        0,
        "r0",
        true,
        (0, 99),
        vec![receive(1, 0, 5_000, 10), spend(1, 1, 0, 2, 50)],
        vec![script(1)],
    );
    let id = store.commit_shard(first.clone()).unwrap();
    assert_eq!(id, 1);
    let events_after = store.events().unwrap();
    assert_eq!(events_after.len(), 2);

    // The identical commit again: a new commit id, no new rows.
    let again = store.commit_shard(first).unwrap();
    assert_eq!(again, 2);
    assert_eq!(store.events().unwrap(), events_after);
    assert_eq!(store.coverage(&script(1)).unwrap().len(), 1);

    // A repeat whose receive differs in value is a contradiction, and the
    // whole commit — including an otherwise fine new receive — is refused.
    let differing = commit(
        0,
        "r0",
        true,
        (0, 99),
        vec![receive(1, 0, 9_999, 10), receive(3, 0, 1, 20)],
        vec![script(3)],
    );
    match store.commit_shard(differing) {
        Err(StoreError::Contradiction(LedgerError::ConflictingReceive(_, 0))) => {}
        other => panic!("expected a conflicting receive, got {other:?}"),
    }
    assert_eq!(store.events().unwrap(), events_after, "nothing was written");
    assert!(store.coverage(&script(3)).unwrap().is_empty());

    // The same outpoint spent by another transaction is a double spend.
    let double = commit(
        1,
        "r1",
        true,
        (100, 199),
        vec![spend(1, 1, 0, 7, 150)],
        vec![script(1)],
    );
    match store.commit_shard(double) {
        Err(StoreError::Contradiction(LedgerError::DoubleSpend(_, 0))) => {}
        other => panic!("expected a double spend, got {other:?}"),
    }
    assert_eq!(store.events().unwrap(), events_after);
    assert_eq!(store.last_commit().unwrap(), 2);
}

fn coverage_merges_settled_ranges_and_keeps_provisional_ones_apart<S: WalletStore>(
    make: &impl Fn() -> S,
) {
    let mut store = make();
    store.bind_set(&identity()).unwrap();
    store
        .commit_shard(commit(1, "r1", true, (100, 199), vec![], vec![script(1)]))
        .unwrap();
    store
        .commit_shard(commit(0, "r0", true, (0, 99), vec![], vec![script(1)]))
        .unwrap();
    store
        .commit_shard(commit(2, "r2", false, (200, 249), vec![], vec![script(1)]))
        .unwrap();
    let ranges = store.coverage(&script(1)).unwrap();
    assert_eq!(ranges.len(), 3, "one range per shard, in height order");
    assert_eq!((ranges[0].start_height, ranges[0].end_height), (0, 99));
    assert_eq!((ranges[1].start_height, ranges[1].end_height), (100, 199));
    assert_eq!(ranges[0].kind, CoverageKind::Settled);
    assert_eq!(ranges[2].kind, CoverageKind::Provisional);
    assert_eq!(ranges[2].revision_digest, "r2");
    let provisional = store.provisional().unwrap();
    assert_eq!(provisional.len(), 1);
}

fn rollback_removes_only_the_suffix<S: WalletStore>(make: &impl Fn() -> S) {
    let mut store = make();
    store.bind_set(&identity()).unwrap();
    store
        .commit_shard(commit(
            0,
            "r0",
            true,
            (0, 199),
            vec![receive(1, 0, 5_000, 10), receive(2, 0, 3_000, 150)],
            vec![script(1), script(2)],
        ))
        .unwrap();
    let mut tail = commit(
        1,
        "r1",
        false,
        (200, 249),
        vec![spend(1, 1, 0, 3, 220)],
        vec![script(1), script(2)],
    );
    tail.pending_upsert.push(pending(2, "r1"));
    store.commit_shard(tail).unwrap();
    store
        .commit_anchor(
            &Anchor {
                height: 249,
                hash: "tail".into(),
            },
            199,
            249,
        )
        .unwrap();
    assert_eq!(store.pending().unwrap().len(), 1);
    assert_eq!(store.ledger().unwrap().confirmed_balance(), 3_000);

    store
        .rollback_above(
            &Anchor {
                height: 199,
                hash: "h199".into(),
            },
            "tail replaced",
        )
        .unwrap();
    let events = store.events().unwrap();
    assert_eq!(events.len(), 2, "the spend above the cut is gone");
    assert_eq!(store.ledger().unwrap().confirmed_balance(), 8_000);
    for tag in [1u8, 2] {
        let ranges = store.coverage(&script(tag)).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].end_height, 199);
        assert_eq!(ranges[0].kind, CoverageKind::Settled);
    }
    assert!(store.provisional().unwrap().is_empty());
    assert!(
        store.pending().unwrap().is_empty(),
        "pending work of the replaced revision is gone with it"
    );
    let anchor = store.anchor().unwrap().unwrap();
    assert_eq!(anchor.height, 199);
}

fn setup_is_keyed_by_set_revision_table_and_segment<S: WalletStore>(make: &impl Fn() -> S) {
    let mut store = make();
    let key = SetupKey {
        set_digest: "set-a".into(),
        revision_digest: "rev-a".into(),
        table: Table::Directory,
        segment: 0,
    };
    let blob = SetupBlob {
        public_params_base64: "AAAA".into(),
        public_params_sha256: "ab".repeat(32),
    };
    store.put_setup(&key, &blob).unwrap();
    assert_eq!(store.setup(&key).unwrap(), Some(blob.clone()));
    for other in [
        SetupKey {
            set_digest: "set-b".into(),
            ..key.clone()
        },
        SetupKey {
            revision_digest: "rev-b".into(),
            ..key.clone()
        },
        SetupKey {
            table: Table::Pages,
            ..key.clone()
        },
        SetupKey {
            segment: 1,
            ..key.clone()
        },
    ] {
        assert_eq!(store.setup(&other).unwrap(), None, "{other:?}");
    }
    store
        .put_filter("rev-a", "hash-a", true, b"filter bytes")
        .unwrap();
    assert_eq!(
        store.filter("rev-a", "hash-a").unwrap(),
        Some(b"filter bytes".to_vec())
    );
    assert_eq!(store.filter("rev-a", "hash-b").unwrap(), None);
    assert_eq!(store.filter("rev-b", "hash-a").unwrap(), None);
}

fn a_commit_past_the_pending_bound_is_refused_whole<S: WalletStore>(make: &impl Fn() -> S) {
    let mut store = make();
    store.bind_set(&identity()).unwrap();
    let limit = store.pending_limit();
    let mut over = commit(1, "r1", false, (0, 99), vec![receive(1, 0, 1, 5)], vec![]);
    for tag in 0..=limit {
        over.pending_upsert.push(pending((tag % 250) as u8, "r1"));
    }
    match store.commit_shard(over) {
        Err(StoreError::PendingLimit { limit: reported }) => assert_eq!(reported, limit),
        other => panic!("expected the pending bound, got {other:?}"),
    }
    assert!(store.events().unwrap().is_empty(), "nothing was written");
    assert!(store.pending().unwrap().is_empty());
    let mut within = commit(1, "r1", false, (0, 99), vec![], vec![]);
    within.pending_upsert.push(pending(1, "r1"));
    store.commit_shard(within).unwrap();
    let held = store.pending().unwrap();
    assert_eq!(held.len(), 1);
    assert!(held[0].id.is_some());
    // Advancing it keeps its id; finishing it removes it.
    let mut progressed = held[0].clone();
    progressed.next_ordinal = 2;
    let mut advance = commit(1, "r1", false, (0, 99), vec![], vec![]);
    advance.pending_upsert.push(progressed);
    store.commit_shard(advance).unwrap();
    let held = store.pending().unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].next_ordinal, 2);
    let mut finish = commit(1, "r1", false, (0, 99), vec![], vec![script(1)]);
    finish.pending_complete.push(held[0].id.unwrap());
    store.commit_shard(finish).unwrap();
    assert!(store.pending().unwrap().is_empty());
}

fn required_from_is_never_raised_and_the_set_is_bound_once<S: WalletStore>(make: &impl Fn() -> S) {
    let mut store = make();
    store.bind_set(&identity()).unwrap();
    let added = store
        .add_scripts(&[ScriptEntry {
            script: script(1),
            origin: ScriptOrigin::Derived,
            required_from: 100,
        }])
        .unwrap();
    assert_eq!(added, 1);
    let added = store
        .add_scripts(&[
            ScriptEntry {
                script: script(1),
                origin: ScriptOrigin::Derived,
                required_from: 500,
            },
            ScriptEntry {
                script: script(2),
                origin: ScriptOrigin::Imported,
                required_from: 0,
            },
        ])
        .unwrap();
    assert_eq!(added, 1, "only the new script counts");
    let scripts = store.scripts().unwrap();
    let one = scripts.iter().find(|e| e.script == script(1)).unwrap();
    assert_eq!(one.required_from, 100, "never raised");
    let two = scripts.iter().find(|e| e.script == script(2)).unwrap();
    assert_eq!(two.origin, ScriptOrigin::Imported);

    // The same lineage with a new geometry continues; another chain does not.
    let mut widened = identity();
    widened.seal.insert(
        "archive-wide".into(),
        SealParameters {
            max_scripts: 2,
            max_page_rows: 2,
            max_txids: 0,
        },
    );
    store.bind_set(&widened).unwrap();
    let mut other = identity();
    other.genesis_hash = "ff".repeat(32);
    match store.bind_set(&other) {
        Err(StoreError::SetMismatch { .. }) => {}
        other => panic!("expected a set mismatch, got {other:?}"),
    }
}

fn promotion_settles_a_provisional_revision_in_place<S: WalletStore>(make: &impl Fn() -> S) {
    let mut store = make();
    store.bind_set(&identity()).unwrap();
    store
        .commit_shard(commit(0, "r0", true, (0, 99), vec![], vec![script(1)]))
        .unwrap();
    store
        .commit_shard(commit(1, "r1", false, (100, 149), vec![], vec![script(1)]))
        .unwrap();
    assert_eq!(store.coverage(&script(1)).unwrap().len(), 2);
    store.promote_provisional(1, "r1").unwrap();
    let ranges = store.coverage(&script(1)).unwrap();
    assert_eq!(ranges.len(), 2, "the range keeps its shard identity");
    assert!(ranges
        .iter()
        .all(|range| range.kind == CoverageKind::Settled));
    assert!(store.provisional().unwrap().is_empty());
}

fn a_retried_spend_cannot_change_its_script<S: WalletStore>(make: &impl Fn() -> S) {
    let mut store = make();
    store.bind_set(&identity()).unwrap();
    store
        .commit_shard(commit(
            0,
            "r0",
            true,
            (0, 99),
            vec![receive(1, 0, 123, 10), spend(1, 1, 0, 3, 20)],
            vec![script(1)],
        ))
        .unwrap();
    let before = store.events().unwrap();
    let id = store.last_commit().unwrap();
    assert!(store
        .commit_shard(commit(
            0,
            "r0",
            true,
            (0, 99),
            vec![spend(2, 1, 0, 3, 20)],
            vec![script(2)]
        ))
        .is_err());
    assert_eq!(store.events().unwrap(), before);
    assert_eq!(store.last_commit().unwrap(), id);
}
