//! Two things the previous service could not do, and one it did wrongly.
//!
//! **Revisions.** A growing tail is republished under a new manifest digest
//! beside the old one. The loader used to index by shard id, so that ordinary
//! state — two directories, one shard — made it refuse to start. Now both are
//! held, each is answered under its own identity, and a digest that has been
//! pruned is refused explicitly rather than answered from whatever is current.
//!
//! **A bounded cache.** Runtimes used to be built on first use and kept
//! forever, so a worker's memory was decided by which shards its traffic
//! happened to touch. Now there is a budget, and it is enforced by reserving
//! before building rather than by checking afterwards.
//!
//! **Single-flight.** The build ran outside the lock, so N concurrent requests
//! for a cold shard each built a full runtime and discarded all but one. That
//! is asserted against directly here: the build counter must reach one, not N.
#![cfg(feature = "transparent-history")]

use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_protocol::transparent::filter as transparent_filter;
use zakura_pir_publisher::transparent as transparent_shard;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tower::ServiceExt;
use transparent_events::{ReceiveEvent, TransparentEvent, Txid};
use transparent_filter::{BlockHash, ScriptBytes, SealParameters, ShardMap};
#[path = "support/artifact_validation.rs"]
mod artifact_validation;
#[path = "support/revision.rs"]
mod revision;
use transparent_query_server::history::service::{router, ServiceConfig, ServiceState};
use transparent_shard::layout::{Geometry, DECOUPLED_2K_8K};
use transparent_shard::manifest::ShardManifest;
use transparent_shared::history::shardset::{
    LoadOptions, ShardSet, Table, TableRef, DEFAULT_RETAIN_REVISIONS,
};
use transparent_shared::runtime::metrics::Metrics;
use transparent_shared::runtime::{reserved_bytes, CacheError, RuntimeCache, SharedParams};

/// The table of a kind in these single-page-shard fixtures.
fn tref(table: Table) -> TableRef {
    match table {
        Table::Pages => TableRef::Pages(0),
        _ => TableRef::Directory,
    }
}

/// The smallest registry geometry, so a runtime is cheap enough to build
/// several times in a test.
const GEOMETRY: Geometry = DECOUPLED_2K_8K;

const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;
const FIRST: u64 = 3_428_143;

fn genesis() -> BlockHash {
    BlockHash::from_display_hex(GENESIS).unwrap()
}

fn hash_at(height: u64) -> BlockHash {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&height.to_le_bytes());
    BlockHash::from_internal_bytes(bytes)
}

fn script(tag: u32) -> ScriptBytes {
    let mut bytes = vec![0x76, 0xa9, 0x14];
    bytes.extend_from_slice(&tag.to_le_bytes());
    bytes.extend_from_slice(&[0u8; 16]);
    bytes.extend_from_slice(&[0x88, 0xac]);
    ScriptBytes::new(bytes)
}

fn events_through(last: u64) -> Vec<(ScriptBytes, TransparentEvent)> {
    (0..=last - FIRST)
        .map(|i| {
            let height = FIRST + i;
            let mut txid = [0u8; 32];
            txid[..8].copy_from_slice(&height.to_le_bytes());
            (
                script(i as u32 % 8),
                TransparentEvent::Receive(ReceiveEvent {
                    metadata: None,
                    height: height as u32,
                    txid: Txid(txid),
                    transaction_index: 0,
                    output_index: 0,
                    value: 1_000 + i,
                    coinbase: false,
                }),
            )
        })
        .collect()
}

/// Writes one revision of shard zero and returns its manifest digest.
fn write_revision(dir: &Path, end: u64, revision: u32, supersedes: &str) -> (String, u64) {
    write_revision_geometry(dir, end, revision, supersedes, &GEOMETRY)
}

fn write_revision_geometry(
    dir: &Path,
    end: u64,
    revision: u32,
    supersedes: &str,
    geometry: &'static Geometry,
) -> (String, u64) {
    let events = events_through(end);
    let (_, entry) = revision::write(
        dir,
        &revision::Revision {
            shard_id: 0,
            start: FIRST,
            end,
            geometry,
            // The tail: a revision that supersedes another is by
            // construction not final, and a sealed shard would have nothing
            // to supersede.
            sealed: false,
            revision,
            supersedes,
            parent_digest: "",
            children: 1,
            genesis: genesis(),
            hash: &hash_at,
        },
        &events,
    );
    (entry.manifest_digest, entry.scripts)
}

/// Writes a map naming `digest` as the current revision of shard zero.
fn write_map(dir: &Path, digest: &str, end: u64, revision: u32, scripts: u64) -> ShardMap {
    write_map_geometry(dir, digest, end, revision, scripts, &GEOMETRY)
}

fn write_map_geometry(
    dir: &Path,
    digest: &str,
    end: u64,
    revision: u32,
    scripts: u64,
    geometry: &'static Geometry,
) -> ShardMap {
    let map = ShardMap {
        genesis_hash: GENESIS.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: FIRST,
        seal: std::collections::BTreeMap::from([(
            geometry.name.to_string(),
            SealParameters {
                max_scripts: 8_192,
                max_page_rows: 2_048,
                max_txids: 0,
            },
        )]),
        shards: vec![transparent_filter::ShardMapEntry {
            shard_id: 0,
            geometry: geometry.name.to_string(),
            start_height: FIRST,
            end_height: end,
            parent_block_hash: hash_at(FIRST - 1).to_display_hex(),
            terminal_block_hash: hash_at(end).to_display_hex(),
            filter_hash: {
                let raw = std::fs::read(dir.join(digest).join("manifest.json")).unwrap();
                let manifest: ShardManifest = serde_json::from_slice(&raw).unwrap();
                manifest.filter_hash
            },
            scripts,
            page_rows: 0,
            txids: 0,
            directory_segments: 1,
            page_segments: {
                let raw = std::fs::read(dir.join(digest).join("manifest.json")).unwrap();
                let manifest: ShardManifest = serde_json::from_slice(&raw).unwrap();
                manifest.page_children.len() as u32
            },
            manifest_digest: digest.to_string(),
            revision,
            sealed: false,
        }],
        recuts: Vec::new(),
    };
    // `page_rows` is reporting only, but the loader cross-checks everything the
    // manifest and the map both carry, so it must agree.
    let raw = std::fs::read(dir.join(digest).join("manifest.json")).unwrap();
    let manifest: ShardManifest = serde_json::from_slice(&raw).unwrap();
    let mut map = map;
    map.shards[0].page_rows = manifest.occupancy.page_rows;
    std::fs::write(
        dir.join("shards.json"),
        serde_json::to_vec_pretty(&map).unwrap(),
    )
    .unwrap();
    map
}

/// A tail published twice: an older revision, and the one the map names.
fn two_revisions(dir: &Path) -> (String, String) {
    let (old, _) = write_revision(dir, FIRST + 40, 0, "");
    let (new, scripts) = write_revision(dir, FIRST + 80, 1, &old);
    write_map(dir, &new, FIRST + 80, 1, scripts);
    (old, new)
}

async fn get(state: &ServiceState, path: &str) -> (StatusCode, Vec<u8>) {
    let response = router(state.clone())
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, body.to_vec())
}

fn state_with(dir: &Path, config: ServiceConfig) -> ServiceState {
    let set = ShardSet::open(dir, DEFAULT_RETAIN_REVISIONS).expect("load");
    ServiceState::build(set, config).expect("state")
}

/// One runtime's reservation at the test geometry, so budgets can be expressed
/// in runtimes rather than in bytes that would drift with the scheme.
fn one_runtime(table: Table) -> u64 {
    reserved_bytes(table, table.rows(&GEOMETRY), table.row_bytes(&GEOMETRY))
}

/// A republished tail leaves its predecessor on disk. That is what the
/// publisher writes by design, and the loader used to refuse to start on it.
#[test]
fn unsupported_map_metadata_is_rejected_before_revision_loading_or_reuse() {
    let original = tempfile::tempdir().unwrap();
    let (digest, scripts) = write_revision(original.path(), FIRST + 2, 0, "");
    let valid = write_map(original.path(), &digest, FIRST + 2, 0, scripts);
    let options =
        transparent_shared::history::shardset::LoadOptions::whole(DEFAULT_RETAIN_REVISIONS);
    let previous = ShardSet::open_with(original.path(), &options).unwrap();
    let previous_json = previous.map_json.clone();
    for unknown_profile in [false, true] {
        let candidate = tempfile::tempdir().unwrap();
        let mut map = valid.clone();
        if unknown_profile {
            map.profile = "unregistered-range-profile".into();
        } else {
            map.range_envelope_version = transparent_filter::RANGE_ENVELOPE_VERSION + 1;
        }
        map.check_shape().unwrap();
        std::fs::write(
            candidate.path().join("shards.json"),
            serde_json::to_vec(&map).unwrap(),
        )
        .unwrap();
        // No revision directories exist: unsupported metadata must be the first refusal.
        for reuse in [None, Some(&previous)] {
            let result = ShardSet::open_reusing(candidate.path(), &options, reuse);
            match result {
                Err(transparent_shared::history::shardset::LoadError::Invalid(error)) => {
                    assert!(error.starts_with("shard map is malformed:"), "{error}");
                }
                _ => panic!("unsupported map must fail before revision loading"),
            }
        }
        assert_eq!(previous.map_json, previous_json);
        assert_eq!(previous.map.profile, valid.profile);
        assert_eq!(
            previous.map.range_envelope_version,
            valid.range_envelope_version
        );
    }
    assert!(ShardSet::open_reusing(original.path(), &options, Some(&previous)).is_ok());
}

#[test]
fn a_set_holding_a_superseded_revision_loads() {
    let dir = tempfile::tempdir().unwrap();
    let (old, new) = two_revisions(dir.path());
    assert_ne!(old, new);

    let set = ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).expect("load");
    assert_eq!(set.len(), 1, "the map names one shard");
    assert_eq!(set.revisions().len(), 2, "both revisions are held");
    assert_eq!(set.get(0).unwrap().digest, new, "the map names the current");
    assert!(set.revision(&old).is_some(), "the predecessor is reachable");
    assert!(set.revision(&new).is_some());
}

#[tokio::test]
async fn retained_history_artifacts_are_checked_and_stay_withdrawn_after_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let (old, new) = two_revisions(dir.path());
    let options = LoadOptions::whole(DEFAULT_RETAIN_REVISIONS);
    let set = ShardSet::open_for_serving(dir.path(), &options).unwrap();
    artifact_validation::damage_received(
        set.revision(&old)
            .unwrap()
            .segment(TableRef::Directory, 0)
            .unwrap(),
    );
    drop(set);
    for pending in [true, false] {
        let set = ShardSet::open_for_serving(dir.path(), &options).unwrap();
        let state = ServiceState::build(set, ServiceConfig::default()).unwrap();
        artifact_validation::assert_withdrawn(
            router(state),
            &format!("/v2/shards/0/revisions/{old}/manifest"),
            &format!("/v2/shards/0/revisions/{new}/manifest"),
            pending,
        )
        .await;
    }
}

/// Retention is bounded. Keeping every revision a growing tail ever had would
/// make the set grow without limit, one publication at a time.
#[test]
fn superseded_revisions_beyond_the_bound_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    two_revisions(dir.path());
    let error = match ShardSet::open(dir.path(), 0) {
        Err(error) => error,
        Ok(_) => panic!("one superseded revision exceeds a bound of none"),
    };
    assert!(
        error.to_string().contains("superseded revisions"),
        "expected a retention error, got {error}"
    );
}

/// A directory the map does not name, and that is not a predecessor of anything
/// it names, does not belong to the set.
///
/// It is likelier to be a publication the map has not caught up with than junk,
/// and serving it would mean answering for a revision no wallet was told about.
#[test]
fn a_revision_ahead_of_the_map_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (old, _) = write_revision(dir.path(), FIRST + 40, 0, "");
    let (new, _) = write_revision(dir.path(), FIRST + 80, 1, &old);
    // The map still names the *old* one, as it would between writing the shard
    // directories and swapping the map.
    let raw = std::fs::read(dir.path().join(&old).join("manifest.json")).unwrap();
    let manifest: ShardManifest = serde_json::from_slice(&raw).unwrap();
    write_map(dir.path(), &old, FIRST + 40, 0, manifest.occupancy.scripts);
    let _ = new;

    let error = match ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS) {
        Err(error) => error,
        Ok(_) => panic!("a revision ahead of the map is not servable"),
    };
    assert!(
        error.to_string().contains("the map names revision"),
        "expected an ahead-of-map error, got {error}"
    );
}

/// A wallet that fetched setup from a revision must keep being answered from
/// *that* revision after the tail is republished.
///
/// The two revisions cover different ranges, so their tables differ and so does
/// their published setup. Answering the old address from the new bytes would
/// not error anywhere — it would hand back rows for a range the wallet did not
/// ask about.
#[tokio::test]
async fn each_revision_is_answered_under_its_own_identity() {
    let dir = tempfile::tempdir().unwrap();
    let (old, new) = two_revisions(dir.path());
    let state = state_with(dir.path(), ServiceConfig::default());

    let mut setups = Vec::new();
    for revision in [&old, &new] {
        let (status, raw) = get(
            &state,
            &format!("/v2/shards/0/revisions/{revision}/setup/directory/0"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "revision {revision} must answer");
        let setup: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(setup["manifest_digest"], revision.as_str());
        setups.push(setup["public_params_sha256"].as_str().unwrap().to_string());
    }
    assert_ne!(
        setups[0], setups[1],
        "two revisions cover different ranges, so their setup must differ"
    );
}

/// A revision this worker does not hold is a stale client, not a bad request.
///
/// `409` with the current map digest says exactly that: the request was well
/// formed and addressed to a publication that is gone. The wallet refreshes the
/// map and re-derives that range, which is what its provisional-coverage
/// contract already requires. A `404` or a silent answer from the current
/// revision would both be worse — the second silently so.
#[tokio::test]
async fn a_pruned_revision_is_refused_with_the_current_map_digest() {
    let dir = tempfile::tempdir().unwrap();
    let (_, new) = two_revisions(dir.path());
    let state = state_with(dir.path(), ServiceConfig::default());

    let gone = "cc".repeat(32);
    let (status, raw) = get(
        &state,
        &format!("/v2/shards/0/revisions/{gone}/setup/directory/0"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let body: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert!(body["error"].as_str().unwrap().contains(&gone));
    assert!(body["retry"].as_str().unwrap().contains("refresh"));

    // The digest of the map the wallet should refetch, so it can tell whether
    // refreshing will actually help.
    let (_, raw) = get(&state, "/v2/shards").await;
    let served = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&raw));
    assert_eq!(body["map_sha256"].as_str().unwrap(), served);
    assert!(!new.is_empty());
}

/// The map is served from the bytes it was loaded with, so the digest describes
/// what was actually sent.
///
/// The filter service publishes the same map on its own host and digests it the
/// same way. Re-serializing per request lets field ordering or whitespace
/// differ between the two origins, and a wallet comparing them would read that
/// as the two services disagreeing about the set.
#[tokio::test]
async fn the_served_map_matches_its_advertised_digest() {
    let dir = tempfile::tempdir().unwrap();
    two_revisions(dir.path());
    let state = state_with(dir.path(), ServiceConfig::default());

    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/v2/shards")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let advertised = response
        .headers()
        .get("x-shard-map-sha256")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        advertised,
        hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&body))
    );

    // And the same bytes come back a second time.
    let (_, again) = get(&state, "/v2/filters/shards").await;
    assert_eq!(body.to_vec(), again);
}

/// Concurrent first-touches of one runtime must produce one build.
///
/// The previous implementation built outside the lock, so this test would have
/// counted eight builds and thrown seven away — the worst possible behaviour at
/// exactly the moment the worker is busiest.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cold_requests_build_once() {
    let dir = tempfile::tempdir().unwrap();
    let (_, new) = two_revisions(dir.path());
    let state = state_with(dir.path(), ServiceConfig::default());

    let path = format!("/v2/shards/0/revisions/{new}/setup/directory/0");
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let state = state.clone();
        let path = path.clone();
        tasks.push(tokio::spawn(async move { get(&state, &path).await.0 }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), StatusCode::OK);
    }

    let metrics = state.metrics();
    assert_eq!(
        metrics.builds.load(Ordering::Relaxed),
        1,
        "eight concurrent requests must not build eight runtimes"
    );
    assert!(
        metrics.build_coalesced.load(Ordering::Relaxed) >= 1,
        "the requests that did not build must have waited on the one that did"
    );
}

/// A budget with room for one runtime holds one runtime.
///
/// Asking for a second must evict the first rather than exceed the budget. The
/// previous cache had no budget to exceed: it grew until the unit's
/// `MemoryMax` killed the process, which is a miss the operator finds out about
/// from the journal.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_runtime_evicts_the_first_rather_than_exceeding_the_budget() {
    let dir = tempfile::tempdir().unwrap();
    let (_, new) = two_revisions(dir.path());
    let budget = one_runtime(Table::Directory).max(one_runtime(Table::Pages));
    let state = state_with(
        dir.path(),
        ServiceConfig {
            cache_bytes: budget,
            ..ServiceConfig::default()
        },
    );

    for table in ["directory", "pages-0"] {
        let (status, _) = get(
            &state,
            &format!("/v2/shards/0/revisions/{new}/setup/{table}/0"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{table} must be servable");
    }

    let metrics = state.metrics();
    assert_eq!(metrics.builds.load(Ordering::Relaxed), 2);
    assert_eq!(
        metrics.evictions.load(Ordering::Relaxed),
        1,
        "the second runtime must have displaced the first"
    );
    assert!(
        metrics.resident_bytes.load(Ordering::Relaxed) <= budget,
        "the cache must never report more than its budget"
    );
    assert_eq!(metrics.cache_entries.load(Ordering::Relaxed), 1);
}

/// A budget too small for even one runtime refuses work rather than taking it.
///
/// This is worse service than an unbounded cache gives, and it is the point:
/// the alternative to refusing is not better service, it is being killed by the
/// OOM reaper partway through someone else's query. The refusal is retryable
/// and says so.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_that_cannot_fit_the_budget_is_refused_retryably() {
    let dir = tempfile::tempdir().unwrap();
    let (_, new) = two_revisions(dir.path());
    let state = state_with(
        dir.path(),
        ServiceConfig {
            cache_bytes: 1 << 20,
            ..ServiceConfig::default()
        },
    );

    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .uri(format!("/v2/shards/0/revisions/{new}/setup/directory/0"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers().get("retry-after").unwrap(),
        "1",
        "an overload must tell the client it is worth retrying"
    );

    let metrics = state.metrics();
    assert_eq!(metrics.overloads.load(Ordering::Relaxed), 1);
    assert_eq!(
        metrics.builds.load(Ordering::Relaxed),
        0,
        "nothing may be built once the budget has already been exceeded"
    );
    assert_eq!(metrics.resident_bytes.load(Ordering::Relaxed), 0);
}

/// A segment replaced on disk after startup must fail the build.
///
/// The set is verified at load and the bytes are then released, so the only
/// thing standing between a corrupted file and a served runtime is the check
/// on the way back in. Without it, releasing the plaintext would have traded a
/// memory saving for a hole in the verification.
#[tokio::test(flavor = "multi_thread")]
async fn a_segment_corrupted_after_loading_fails_its_build() {
    let dir = tempfile::tempdir().unwrap();
    let (_, new) = two_revisions(dir.path());
    let state = state_with(dir.path(), ServiceConfig::default());

    let path = dir.path().join(&new).join("directory.0.bin");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[17] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();

    let (status, _) = get(
        &state,
        &format!("/v2/shards/0/revisions/{new}/setup/directory/0"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let metrics = state.metrics();
    assert_eq!(metrics.build_failures.load(Ordering::Relaxed), 1);
    // A failed build holds no memory, so it must hold no reservation either.
    assert_eq!(metrics.resident_bytes.load(Ordering::Relaxed), 0);
}

/// A runtime nothing is holding may be evicted; one a request is still reading
/// may not.
///
/// This is the invariant the byte accounting rests on. Evicting a pinned
/// runtime would subtract its reservation from the resident total while the
/// memory itself was still allocated and still being read — the cache would
/// believe it had room it does not have, and would then admit work on the
/// strength of it. Refusing is the correct answer even though it is the worse
/// one for the request being refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_runtime_in_use_is_not_evicted_to_make_room() {
    let dir = tempfile::tempdir().unwrap();
    let (_, new) = two_revisions(dir.path());
    let set = ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).expect("load");
    let shard = set.revision(&new).expect("the current revision");

    let metrics = Arc::new(Metrics::default());
    // Room for exactly one runtime, so the second request must either evict the
    // first or be refused.
    let budget = one_runtime(Table::Directory).max(one_runtime(Table::Pages));
    let cache = RuntimeCache::new(budget, 1, metrics.clone());

    let shared = |table| Arc::new(SharedParams::build(shard.geometry, table).unwrap());

    // Held across the second request, exactly as a query handler holds it while
    // evaluating against the runtime.
    let held = cache
        .get(
            (new.clone(), Table::Directory, 0),
            shared(Table::Directory),
            shard.segment(tref(Table::Directory), 0).unwrap().clone(),
        )
        .await
        .expect("the first runtime fits an empty cache");

    let refused = cache
        .get(
            (new.clone(), Table::Pages, 0),
            shared(Table::Pages),
            shard.segment(tref(Table::Pages), 0).unwrap().clone(),
        )
        .await;
    assert!(
        matches!(refused, Err(CacheError::Overloaded)),
        "a pinned runtime must not be evicted to admit another"
    );
    assert_eq!(metrics.evictions.load(Ordering::Relaxed), 0);
    assert_eq!(cache.entries(), 1);
    // Charged what it holds, which leaves less than a second bound free.
    let charged = held.get().held_bytes();
    assert_eq!(cache.resident_bytes(), charged);
    assert!(charged + one_runtime(Table::Pages) > budget);
    cache.evict_unpinned();
    assert_eq!(
        cache.resident_bytes(),
        charged,
        "trimming preserves live handles"
    );

    // Once nothing is holding it, the same request succeeds by evicting it.
    drop(held);
    cache
        .get(
            (new.clone(), Table::Pages, 0),
            shared(Table::Pages),
            shard.segment(tref(Table::Pages), 0).unwrap().clone(),
        )
        .await
        .expect("an unpinned runtime may be evicted");
    assert_eq!(metrics.evictions.load(Ordering::Relaxed), 1);
    assert_eq!(cache.entries(), 1);
    assert!(cache.resident_bytes() <= budget);
    cache.evict_unpinned();
    assert_eq!(cache.resident_bytes(), 0);
    assert_eq!(cache.entries(), 0);
}

/// A built runtime is charged what it holds, not its reservation, so a
/// budget that could carry one bound and one built runtime now holds both
/// without evicting either. The charge on the cached one could only fall, so
/// every admission that succeeded before still succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn a_built_runtime_returns_its_unused_reservation_to_the_budget() {
    let dir = tempfile::tempdir().unwrap();
    let (_, new) = two_revisions(dir.path());
    let set = ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).expect("load");
    let shard = set.revision(&new).expect("the current revision");
    let shared = |table| Arc::new(SharedParams::build(shard.geometry, table).unwrap());
    // The 8-bit history tables plan at their bound (a one-digit matrix is
    // eight-byte words on a table with data), so here the budget is the two
    // bounds; what is checked is that each is charged what it holds.
    let budget = shared(Table::Directory).held_bytes() + one_runtime(Table::Pages);
    assert!(budget <= one_runtime(Table::Directory) + one_runtime(Table::Pages));
    let metrics = Arc::new(Metrics::default());
    let cache = RuntimeCache::new(budget, 1, metrics.clone());
    let mut handles = Vec::new();
    for table in [Table::Directory, Table::Pages] {
        handles.push(
            cache
                .get(
                    (new.clone(), table, 0),
                    shared(table),
                    shard.segment(tref(table), 0).unwrap().clone(),
                )
                .await
                .expect("both fit at their built size"),
        );
    }
    let held: u64 = handles.iter().map(|handle| handle.get().held_bytes()).sum();
    assert_eq!(cache.resident_bytes(), held);
    assert_eq!(metrics.resident_bytes.load(Ordering::Relaxed), held);
    assert_eq!(metrics.cache_entries.load(Ordering::Relaxed), 2);
    assert_eq!(metrics.evictions.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.overloads.load(Ordering::Relaxed), 0);
    drop(handles);
    cache.evict_unpinned();
    assert_eq!(cache.resident_bytes(), 0);
    assert_eq!(metrics.evictions.load(Ordering::Relaxed), 2);
}

/// A warm worker is checked against its runtimes' built size plus one bound
/// per runtime the prewarm can have in flight, on both its memory and its
/// disk cache, and becomes ready under exactly that budget: every target
/// warm, nothing evicted or refused, each snapshot charged its file length.
/// A restart restores both from disk at the same charge.
#[tokio::test(flavor = "multi_thread")]
async fn background_warming_respects_memory_and_disk_budgets() {
    use transparent_shared::runtime::disk::DiskCache;
    use transparent_shared::runtime::warm_bytes;
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("set");
    std::fs::create_dir(&dir).unwrap();
    two_revisions(&dir);
    let tables = [Table::Directory, Table::Pages]
        .map(|table| SharedParams::build(&GEOMETRY, table).unwrap());
    // One restore and one build at a time, so one runtime can be in flight.
    let needed = warm_bytes(&tables, 1);
    let planned: u64 = tables.iter().map(SharedParams::held_bytes).sum();
    // Equal for the 8-bit history tables, which plan at their bound.
    assert!(needed <= one_runtime(Table::Directory) + one_runtime(Table::Pages));
    let entries: u64 = tables.iter().map(DiskCache::planned_entry_bytes).sum();
    let excess = tables
        .iter()
        .map(|shared| DiskCache::entry_bytes(shared) - DiskCache::planned_entry_bytes(shared))
        .max()
        .unwrap();
    let disk = |name, limit| {
        let mut disk = DiskCache::new(root.path().join(name), limit).unwrap();
        disk.restore_slots = 1;
        disk
    };
    let open = || ShardSet::open(&dir, DEFAULT_RETAIN_REVISIONS).unwrap();
    let preflight = disk("cache", entries + excess).check_set(&open()).unwrap();
    assert_eq!(preflight["missing_bytes"], entries);
    assert_eq!(preflight["assignment_bytes"], entries);
    assert!(disk("short", entries + excess - 1)
        .check_set(&open())
        .is_err());
    let config = |cache_bytes| ServiceConfig {
        cache_bytes,
        ..ServiceConfig::default()
    };
    assert!(
        ServiceState::build_with_disk(
            open(),
            config(needed - 1),
            Some(disk("cache", entries + excess))
        )
        .is_ok(),
        "startup does not require the whole assignment to be resident"
    );

    let warm = |state: ServiceState| async move {
        state.spawn_prewarm().await.unwrap();
        let (status, body) = get(&state, "/v1/ready").await;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        let ready: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(ready["warm_runtimes"], 2);
        assert_eq!(ready["target_runtimes"], 2);
        assert_eq!(ready["prewarm_failed"], 0);
        let metrics = state.metrics().clone();
        assert_eq!(metrics.target_runtimes.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.cache_entries.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.evictions.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.overloads.load(Ordering::Relaxed), 0);
        assert!(metrics.resident_bytes.load(Ordering::Relaxed) <= planned);
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            while metrics.disk_save_pending.load(Ordering::Relaxed) != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        (state, metrics)
    };
    let cold = disk("cache", entries + excess);
    let (state, metrics) =
        warm(ServiceState::build_with_disk(open(), config(needed), Some(cold.clone())).unwrap())
            .await;
    assert_eq!(metrics.builds.load(Ordering::Relaxed), 2);
    assert_eq!(metrics.disk_write_failures.load(Ordering::Relaxed), 0);
    // At most the plan: a nearly empty table's one-digit matrix may be
    // written at four-byte words.
    assert!(cold.used_bytes().unwrap() <= entries);
    drop(state);

    let (_state, metrics) =
        warm(ServiceState::build_with_disk(open(), config(needed), Some(cold.clone())).unwrap())
            .await;
    assert_eq!(metrics.builds.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.disk_hits.load(Ordering::Relaxed), 2);
    // A restored matrix is mapped at the width it was written: at most the plan.
    assert!(metrics.resident_bytes.load(Ordering::Relaxed) <= planned);
}

#[tokio::test(flavor = "multi_thread")]
async fn revision_churn_bounds_runtimes_and_collects_idle_snapshots() {
    use transparent_query_server::history::live::{Command, LiveService, Publication};
    use transparent_shared::history::shardset::LoadOptions;
    let root = tempfile::tempdir().unwrap();
    let a = root.path().join("initial");
    std::fs::create_dir(&a).unwrap();
    let (_, mut previous) = two_revisions(&a);
    let set = ShardSet::open(&a, 3).unwrap();
    let mut expected = set.map_digest.clone();
    let pair = one_runtime(Table::Directory) + one_runtime(Table::Pages);
    let config = ServiceConfig {
        cache_bytes: pair * 8,
        ..ServiceConfig::default()
    };
    let disk =
        transparent_shared::runtime::disk::DiskCache::new(root.path().join("cache"), pair * 16)
            .unwrap();
    let preflight = disk.check_set(&set).unwrap();
    assert!(
        preflight["assignment_bytes"].as_u64().unwrap() <= pair + 4096,
        "preflight budgets current warm revisions only"
    );
    let state = ServiceState::build_with_disk(set, config, Some(disk.clone())).unwrap();
    state.spawn_prewarm().await.unwrap();
    // A slow reader of the oldest snapshot must not pin every later one.
    let held_reader = state.clone();
    let metrics = state.metrics().clone();
    assert_eq!(
        metrics.builds.load(Ordering::Relaxed),
        2,
        "retained tails are not prewarmed"
    );
    let live = LiveService::new(
        state,
        Publication {
            directory: a,
            assignment: None,
            map_sha256: expected.clone(),
            runtimes: None,
        },
        config,
        LoadOptions::whole(3),
        root.path().join("active.json"),
    )
    .unwrap();
    for revision in 2..8 {
        let directory = root.path().join(format!("generation-{revision}"));
        std::fs::create_dir(&directory).unwrap();
        let height = FIRST + 80 + revision as u64;
        let (digest, scripts) = write_revision(&directory, height, revision, &previous);
        write_map(&directory, &digest, height, revision, scripts);
        let map_sha256 = ShardSet::open(&directory, 3).unwrap().map_digest;
        live.command(Command::Prepare {
            expected: expected.clone(),
            publication: Publication {
                directory,
                assignment: None,
                map_sha256: map_sha256.clone(),
                runtimes: None,
            },
        })
        .await
        .unwrap();
        // Each pair is charged its built size, not its reservation.
        let built = [Table::Directory, Table::Pages]
            .map(|table| SharedParams::build(&GEOMETRY, table).unwrap().held_bytes());
        assert!(
            metrics.cache_entries.load(Ordering::Relaxed) <= 4,
            "preparing a cold candidate cannot require more than the current and preparing pair"
        );
        let resident = metrics.resident_bytes.load(Ordering::Relaxed);
        assert!(resident <= (built[0] + built[1]) * 2, "{resident}");
        assert!(
            resident < pair * 2,
            "the pairs are charged below their bound"
        );
        live.command(Command::Activate {
            expected,
            map_sha256: map_sha256.clone(),
        })
        .await
        .unwrap();
        live.command(Command::Collect).await.unwrap();
        assert!(
            disk.used_bytes().unwrap() <= pair * 16,
            "disk budget exceeded during churn"
        );
        let status = live.command(Command::Status).await.unwrap();
        assert!(status["retired_snapshots"].as_u64().unwrap() <= 4);
        assert!(status["revisions"].as_array().unwrap().len() <= 6);
        expected = map_sha256;
        previous = digest;
    }
    drop(held_reader);
    // Live collection can defer while optional snapshot writes own the lock.
    // Once those writers finish, require full reclamation with the same retained
    // set; deferred collection must not turn into permanent cache growth.
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while metrics.disk_save_pending.load(Ordering::Relaxed) != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("snapshot writers did not finish");
    let collected = live.command(Command::Collect).await.unwrap();
    assert_eq!(collected["disk_collection_deferred"], false);
    let status = live.command(Command::Status).await.unwrap();
    assert_eq!(status["retired_snapshots"], 3);
    assert_eq!(status["revisions"].as_array().unwrap().len(), 4);
    let cached = std::fs::read_dir(root.path().join("cache"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "runtime"))
        .count();
    assert!(
        cached <= 8,
        "at most the four live/retained revision pairs remain on disk"
    );
}

/// Live collection must defer optional disk pruning while a snapshot writer
/// holds its lock, keeping readiness and subsequent control operations responsive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_collection_keeps_status_and_readiness_responsive() {
    use std::time::Duration;
    use transparent_query_server::history::live::{Command, LiveService, Publication};
    use transparent_shared::history::shardset::LoadOptions;
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("initial");
    std::fs::create_dir(&directory).unwrap();
    two_revisions(&directory);
    let set = ShardSet::open(&directory, 3).unwrap();
    let map_sha256 = set.map_digest.clone();
    let cache_dir = root.path().join("cache");
    let disk =
        transparent_shared::runtime::disk::DiskCache::new(cache_dir.clone(), 64 * 1024 * 1024)
            .unwrap();
    let config = ServiceConfig::default();
    let state = ServiceState::build_with_disk(set, config, Some(disk)).unwrap();
    let live = LiveService::new(
        state,
        Publication {
            directory,
            assignment: None,
            map_sha256,
            runtimes: None,
        },
        config,
        LoadOptions::whole(3),
        root.path().join("active.json"),
    )
    .unwrap();
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(cache_dir.join(".lock"))
        .unwrap();
    file.lock().unwrap();
    // An independent thread releases the lock even if the buggy implementation
    // blocks executor threads; the regression fails instead of hanging forever.
    let (release, released) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let _ = released.recv_timeout(Duration::from_secs(3));
        drop(file);
    });
    let collector = tokio::spawn({
        let live = live.clone();
        async move { live.command(Command::Collect).await }
    });
    let collected = tokio::time::timeout(Duration::from_millis(500), collector).await;
    let status = tokio::spawn({
        let live = live.clone();
        async move { live.command(Command::Status).await }
    });
    let ready = tokio::spawn(
        live.router()
            .oneshot(Request::get("/v1/ready").body(Body::empty()).unwrap()),
    );
    let next_mutation = tokio::spawn({
        let live = live.clone();
        async move {
            live.command(Command::Discard {
                map_sha256: "unused".into(),
            })
            .await
        }
    });
    let responsive = tokio::time::timeout(Duration::from_millis(500), async {
        status.await.unwrap().unwrap();
        next_mutation.await.unwrap().unwrap();
        ready.await.unwrap().unwrap()
    })
    .await;
    // Only release the writer after collection and the next mutation had their
    // opportunity to finish. A blocking collection fails the bounded check.
    let _ = release.send(());
    writer.join().unwrap();
    let collected = collected
        .expect("collection waited for the cache writer")
        .unwrap()
        .unwrap();
    assert_eq!(collected["disk_collection_deferred"], true);
    assert_eq!(collected["disk_freed_bytes"], 0);
    assert!(
        responsive.is_ok(),
        "collection blocked control or readiness"
    );
    assert_eq!(responsive.unwrap().status(), StatusCode::OK);
    let collected = live.command(Command::Collect).await.unwrap();
    assert_eq!(collected["disk_collection_deferred"], false);
}

/// A blocked publication write must not occupy the async executor. The FIFO
/// exercises real file I/O without changing the production persistence path.
/// Platforms differ on whether the later FIFO fsync succeeds; only the blocked
/// interval is used to check serving behavior and cancellation ordering.
#[cfg(unix)]
async fn blocked_publication_write(invalidate: bool) {
    use std::time::{Duration, Instant};
    use transparent_query_server::history::live::{Command, LiveService, Publication};
    use transparent_shared::history::shardset::LoadOptions;
    let root = tempfile::tempdir().unwrap();
    let a = root.path().join("a");
    let b = root.path().join("b");
    std::fs::create_dir_all(&a).unwrap();
    std::fs::create_dir_all(&b).unwrap();
    let (old, count) = write_revision(&a, FIRST + 1, 0, "");
    write_map(&a, &old, FIRST + 1, 0, count);
    let set = ShardSet::open(&a, 3).unwrap();
    let old_map = set.map_digest.clone();
    let config = ServiceConfig::default();
    let record = root.path().join("active.json");
    let live = LiveService::new(
        ServiceState::build(set, config).unwrap(),
        Publication {
            directory: a,
            assignment: None,
            map_sha256: old_map.clone(),
            runtimes: None,
        },
        config,
        LoadOptions::whole(3),
        record.clone(),
    )
    .unwrap();
    let command = if invalidate {
        Command::Invalidate {
            expected: old_map.clone(),
            from_height: FIRST,
            keep_digests: Default::default(),
        }
    } else {
        let (new, count) = write_revision(&b, FIRST + 2, 1, &old);
        write_map(&b, &new, FIRST + 2, 1, count);
        let map_sha256 = ShardSet::open(&b, 3).unwrap().map_digest;
        live.command(Command::Prepare {
            expected: old_map.clone(),
            publication: Publication {
                directory: b,
                assignment: None,
                map_sha256: map_sha256.clone(),
                runtimes: None,
            },
        })
        .await
        .unwrap();
        Command::Activate {
            expected: old_map.clone(),
            map_sha256,
        }
    };
    let temporary = if invalidate {
        record.with_extension("invalid.tmp")
    } else {
        record.with_extension("tmp")
    };
    assert!(std::process::Command::new("mkfifo")
        .arg(&temporary)
        .status()
        .unwrap()
        .success());
    let (release, released) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        // Independent of Tokio, so the regression terminates even if the
        // only executor thread is blocked inside File::create.
        let _ = released.recv_timeout(Duration::from_secs(3));
        use std::io::Read;
        let mut file = std::fs::File::open(temporary).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
    });
    let began = Instant::now();
    let mutation = tokio::spawn({
        let live = live.clone();
        async move { live.command(command).await }
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    let pending = !mutation.is_finished();
    let status = live.command(Command::Status).await.unwrap();
    let response = live
        .router()
        .oneshot(Request::get("/v1/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let responsive = began.elapsed() < Duration::from_secs(1);
    // Cancellation must not release the activation's operation guard while
    // its disk operation remains in flight.
    mutation.abort();
    let _ = mutation.await;
    let next = tokio::spawn({
        let live = live.clone();
        async move {
            live.command(Command::Discard {
                map_sha256: "unused".into(),
            })
            .await
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let serialized = invalidate || !next.is_finished();
    let _ = release.send(());
    reader.join().unwrap();
    next.await.unwrap().unwrap();
    assert!(pending, "publication write did not remain blocked");
    assert!(responsive, "publication disk I/O blocked status/readiness");
    assert!(
        serialized,
        "cancelled activation released mutation serialization"
    );
    assert_eq!(status["active"]["map_sha256"], old_map);
    assert_eq!(
        response.status(),
        if invalidate {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::OK
        }
    );
    if invalidate {
        assert!(status["revoked_revisions"].as_u64().unwrap() > 0);
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn blocked_activation_keeps_executor_responsive() {
    blocked_publication_write(false).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn blocked_invalidation_refuses_without_blocking_executor() {
    blocked_publication_write(true).await;
}

/// Makes `directory` writable and searchable but unreadable, so a durable
/// record write inside it renames into place and then fails to sync the
/// directory. Panics for a user whom permissions do not bind.
#[cfg(unix)]
fn fail_after_rename(directory: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o300)).unwrap();
    assert!(
        std::fs::File::open(directory).is_err(),
        "a post-rename write failure needs an unprivileged test user"
    );
}

/// A durable activation write that fails installs nothing in memory and
/// reports failure, both when it fails before the record is renamed into
/// place and after. The candidate stays prepared, nothing retries on its own,
/// and a later activation by the controller succeeds. A post-rename failure
/// leaves the new record on disk, which a restart serves.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_activation_write_installs_nothing() {
    use std::os::unix::fs::PermissionsExt;
    use transparent_query_server::history::live::{Command, LiveService, Publication};
    use transparent_shared::history::shardset::LoadOptions;
    let root = tempfile::tempdir().unwrap();
    let a = root.path().join("a");
    let b = root.path().join("b");
    let records = root.path().join("records");
    for dir in [&a, &b, &records] {
        std::fs::create_dir_all(dir).unwrap();
    }
    let (old, count) = write_revision(&a, FIRST + 1, 0, "");
    write_map(&a, &old, FIRST + 1, 0, count);
    let set = ShardSet::open(&a, 3).unwrap();
    let old_map = set.map_digest.clone();
    let config = ServiceConfig::default();
    let record = records.join("active.json");
    let live = LiveService::new(
        ServiceState::build(set, config).unwrap(),
        Publication {
            directory: a,
            assignment: None,
            map_sha256: old_map.clone(),
            runtimes: None,
        },
        config,
        LoadOptions::whole(3),
        record.clone(),
    )
    .unwrap();
    let (new, count) = write_revision(&b, FIRST + 2, 1, &old);
    write_map(&b, &new, FIRST + 2, 1, count);
    let new_map = ShardSet::open(&b, 3).unwrap().map_digest;
    live.command(Command::Prepare {
        expected: old_map.clone(),
        publication: Publication {
            directory: b,
            assignment: None,
            map_sha256: new_map.clone(),
            runtimes: None,
        },
    })
    .await
    .unwrap();
    let activate = || Command::Activate {
        expected: old_map.clone(),
        map_sha256: new_map.clone(),
    };
    let served = |live: &LiveService| {
        let router = live.router();
        async move {
            let response = router
                .oneshot(Request::get("/v2/shards").body(Body::empty()).unwrap())
                .await
                .unwrap();
            response.headers()["x-shard-map-sha256"]
                .to_str()
                .unwrap()
                .to_owned()
        }
    };
    let saved = || -> Option<String> {
        std::fs::read(&record).ok().map(|bytes| {
            serde_json::from_slice::<Publication>(&bytes)
                .unwrap()
                .map_sha256
        })
    };
    let unchanged = |status: &serde_json::Value| {
        assert_eq!(status["active"]["map_sha256"], old_map);
        assert_eq!(status["candidate"]["map_sha256"], new_map);
        assert_eq!(status["retired_snapshots"], 0);
    };

    // Before the rename: the temporary record cannot be created.
    std::fs::create_dir(record.with_extension("tmp")).unwrap();
    assert!(live.command(activate()).await.is_err());
    unchanged(&live.command(Command::Status).await.unwrap());
    assert_eq!(served(&live).await, old_map);
    assert_eq!(saved(), None);
    std::fs::remove_dir(record.with_extension("tmp")).unwrap();

    // After the rename: the record names the candidate, but the directory
    // sync fails, so the activation is still refused and nothing swaps.
    fail_after_rename(&records);
    let refused = live.command(activate()).await;
    std::fs::set_permissions(&records, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(refused.is_err());
    unchanged(&live.command(Command::Status).await.unwrap());
    assert_eq!(served(&live).await, old_map);
    assert_eq!(saved().as_deref(), Some(new_map.as_str()));

    // The controller's retry activates the same candidate.
    assert_eq!(
        live.command(activate()).await.unwrap(),
        serde_json::json!({"active": new_map})
    );
    let status = live.command(Command::Status).await.unwrap();
    assert_eq!(status["active"]["map_sha256"], new_map);
    assert!(status["candidate"].is_null());
    assert_eq!(status["retired_snapshots"], 1);
    assert_eq!(served(&live).await, new_map);
    assert_eq!(saved().as_deref(), Some(new_map.as_str()));
}

#[cfg(unix)]
#[path = "support/control_socket.rs"]
mod control_socket;

/// A controller that closes its control connection while activation is
/// blocked in the durable write leaves command serialization held until the
/// write returns, while status over the socket and readiness and answers
/// over loopback HTTP stay responsive. The controller's next activation then
/// completes the same candidate.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_disconnected_activation_keeps_serialization_over_the_control_socket() {
    use control_socket::{connectable, send, send_and_disconnect, serve, until_holding};
    use std::time::{Duration, Instant};
    use transparent_query_server::history::live::{LiveService, Publication};
    use transparent_shared::history::shardset::LoadOptions;
    let root = tempfile::tempdir().unwrap();
    let a = root.path().join("a");
    let b = root.path().join("b");
    std::fs::create_dir_all(&a).unwrap();
    std::fs::create_dir_all(&b).unwrap();
    let (old, count) = write_revision(&a, FIRST + 1, 0, "");
    write_map(&a, &old, FIRST + 1, 0, count);
    let set = ShardSet::open(&a, 3).unwrap();
    let old_map = set.map_digest.clone();
    let config = ServiceConfig::default();
    let record = root.path().join("active.json");
    let live = LiveService::new(
        ServiceState::build(set, config).unwrap(),
        Publication {
            directory: a,
            assignment: None,
            map_sha256: old_map.clone(),
            runtimes: None,
        },
        config,
        LoadOptions::whole(3),
        record.clone(),
    )
    .unwrap();
    let socket = root.path().join("control.sock");
    tokio::spawn({
        let live = live.clone();
        let socket = socket.clone();
        async move { live.listen(&socket).await }
    });
    let url = serve(live.router()).await;
    let (new, count) = write_revision(&b, FIRST + 2, 1, &old);
    write_map(&b, &new, FIRST + 2, 1, count);
    let new_map = ShardSet::open(&b, 3).unwrap().map_digest;
    let http = reqwest::Client::new();
    let served = || async {
        let response = http.get(format!("{url}/v2/shards")).send().await.unwrap();
        assert_eq!(response.status(), 200);
        response.headers()["x-shard-map-sha256"]
            .to_str()
            .unwrap()
            .to_owned()
    };
    let socket_task = socket.clone();
    let control = move |command: serde_json::Value, wait: u64| {
        let socket = socket_task.clone();
        tokio::task::spawn_blocking(move || {
            connectable(&socket);
            send(&socket, &command, Duration::from_millis(wait))
        })
    };
    let prepared = control(
        serde_json::json!({"operation": "prepare", "expected": old_map,
            "publication": {"directory": b, "assignment": null, "map_sha256": new_map}}),
        60_000,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(prepared["ok"], true, "{prepared}");
    let activate =
        serde_json::json!({"operation": "activate", "expected": old_map, "map_sha256": new_map});

    let blocked = control_socket::BlockedWrite::at(record.clone());
    tokio::task::spawn_blocking({
        let socket = socket.clone();
        let activate = activate.clone();
        let old_map = old_map.clone();
        move || {
            send_and_disconnect(&socket, &activate);
            // Status leaves out a candidate it cannot lock without waiting.
            until_holding(&socket, |status| {
                status["result"]["candidate"].is_null()
                    && status["result"]["active"]["map_sha256"] == old_map
            });
        }
    })
    .await
    .unwrap();
    let began = Instant::now();
    let status = control(serde_json::json!({"operation": "status"}), 5_000)
        .await
        .unwrap()
        .expect("status waited for the blocked write");
    let ready = http.get(format!("{url}/v1/ready")).send().await.unwrap();
    assert_eq!(ready.status(), 200);
    assert_eq!(served().await, old_map);
    assert!(
        began.elapsed() < Duration::from_secs(2),
        "a blocked activation write stalled status or serving"
    );
    assert_eq!(status["result"]["active"]["map_sha256"], old_map);
    // The disconnected activation still holds serialization: a command that
    // needs it gets no reply while the write is blocked.
    let waiting = control(
        serde_json::json!({"operation": "discard", "map_sha256": "unused"}),
        60_000,
    );
    let held = control(serde_json::json!({"operation": "collect"}), 300)
        .await
        .unwrap();
    assert!(held.is_none(), "serialization was released: {held:?}");
    let restore = tokio::task::spawn_blocking(move || blocked.release())
        .await
        .unwrap();
    assert_eq!(waiting.await.unwrap().unwrap()["ok"], true);
    restore.restore();

    // The controller retries; the same candidate becomes active.
    let activated = control(activate, 60_000).await.unwrap().unwrap();
    assert_eq!(activated["ok"], true, "{activated}");
    assert_eq!(served().await, new_map);
    let saved: Publication = serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
    assert_eq!(saved.map_sha256, new_map);
}

/// Hot activation keeps the old revision usable, shares bounded runtime state,
/// and refuses orphaned answers after a reorg without restarting the server.
#[tokio::test(flavor = "multi_thread")]
async fn live_prepare_activate_and_invalidate() {
    use transparent_query_server::history::live::{Command, LiveService, Publication};
    use transparent_shared::history::shardset::LoadOptions;
    let root = tempfile::tempdir().unwrap();
    let a = root.path().join("a");
    let b = root.path().join("b");
    std::fs::create_dir_all(&a).unwrap();
    std::fs::create_dir_all(&b).unwrap();
    let (old, count) = write_revision(&a, FIRST + 1, 0, "");
    write_map(&a, &old, FIRST + 1, 0, count);
    let set = ShardSet::open(&a, 3).unwrap();
    let old_map = set.map_digest.clone();
    let config = ServiceConfig {
        cache_bytes: 1 << 30,
        ..ServiceConfig::default()
    };
    let state = ServiceState::build(set, config).unwrap();
    state.spawn_prewarm().await.unwrap();
    let metrics = state.metrics().clone();
    let live = LiveService::new(
        state,
        Publication {
            directory: a.clone(),
            assignment: None,
            map_sha256: old_map.clone(),
            runtimes: None,
        },
        config,
        LoadOptions::whole(3),
        root.path().join("active.json"),
    )
    .unwrap();
    let (new, count) = write_revision(&b, FIRST + 2, 1, &old);
    write_map(&b, &new, FIRST + 2, 1, count);
    let new_map = ShardSet::open(&b, 3).unwrap().map_digest;
    assert!(live
        .command(Command::Activate {
            expected: old_map.clone(),
            map_sha256: new_map.clone()
        })
        .await
        .is_err());
    live.command(Command::Prepare {
        expected: old_map.clone(),
        publication: Publication {
            directory: b.clone(),
            assignment: None,
            map_sha256: new_map.clone(),
            runtimes: None,
        },
    })
    .await
    .unwrap();
    // One successful preparation, observed once per phase.
    assert_eq!(
        metrics
            .prepare_seconds
            .iter()
            .map(|h| h.count())
            .collect::<Vec<_>>(),
        [1, 1, 1]
    );
    let before = live
        .router()
        .oneshot(Request::get("/v2/shards").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(before.headers()["x-shard-map-sha256"], old_map);
    // A fork notice arriving after preparation invalidates that preparation,
    // even when the currently advertised predecessor is below the fork.
    live.command(Command::Invalidate {
        expected: old_map.clone(),
        from_height: FIRST + 2,
        keep_digests: Default::default(),
    })
    .await
    .unwrap();
    assert!(live
        .command(Command::Activate {
            expected: old_map.clone(),
            map_sha256: new_map.clone(),
        })
        .await
        .is_err());
    live.command(Command::Prepare {
        expected: old_map.clone(),
        publication: Publication {
            directory: b,
            assignment: None,
            map_sha256: new_map.clone(),
            runtimes: None,
        },
    })
    .await
    .unwrap();
    live.command(Command::Activate {
        expected: old_map.clone(),
        map_sha256: new_map.clone(),
    })
    .await
    .unwrap();
    live.command(Command::Activate {
        expected: old_map,
        map_sha256: new_map.clone(),
    })
    .await
    .unwrap();
    let after = live
        .router()
        .oneshot(Request::get("/v2/shards").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(after.headers()["x-shard-map-sha256"], new_map);
    assert_eq!(metrics.prewarm_failed.load(Ordering::Relaxed), 0);
    live.command(Command::Invalidate {
        expected: new_map.clone(),
        from_height: FIRST + 2,
        keep_digests: Default::default(),
    })
    .await
    .unwrap();
    let response = live
        .router()
        .oneshot(
            Request::get(format!("/v2/shards/0/revisions/{new}/manifest"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        live.router()
            .oneshot(Request::get("/v2/shards").body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let saved: Publication =
        serde_json::from_slice(&std::fs::read(root.path().join("active.json")).unwrap()).unwrap();
    assert_eq!(saved.map_sha256, new_map);
    // A previously orphaned branch may become canonical again. Explicit
    // endpoint revalidation permits a fresh warm snapshot, not resurrection
    // of the invalidated active snapshot in place.
    live.command(Command::Invalidate {
        expected: new_map.clone(),
        from_height: FIRST,
        keep_digests: std::collections::BTreeSet::from([new.clone()]),
    })
    .await
    .unwrap();
    live.command(Command::Prepare {
        expected: new_map.clone(),
        publication: saved.clone(),
    })
    .await
    .unwrap();
    live.command(Command::Activate {
        expected: new_map.clone(),
        map_sha256: new_map,
    })
    .await
    .unwrap();
    assert_eq!(
        live.router()
            .oneshot(Request::get("/v1/ready").body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let status = live.command(Command::Status).await.unwrap();
    assert_eq!(status["invalidated"], false);
    assert!(status["revoked_revisions"].as_u64().unwrap() > 0);
    assert!(status["preparing"].is_null());
    drop(live);
    let restarted_state = state_with(&saved.directory, config);
    restarted_state.spawn_prewarm().await.unwrap();
    let restarted = LiveService::new(
        restarted_state,
        saved,
        config,
        LoadOptions::whole(3),
        root.path().join("active.json"),
    )
    .unwrap();
    let status = restarted.command(Command::Status).await.unwrap();
    assert_eq!(status["invalidated"], false);
    assert!(status["revoked_revisions"].as_u64().unwrap() > 0);
    assert_eq!(
        restarted
            .router()
            .oneshot(
                Request::get(format!("/v2/shards/0/revisions/{old}/manifest"))
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
}

#[path = "support/burst.rs"]
mod burst;
#[path = "support/query.rs"]
mod burst_query;

/// A worker offered the runtimes its publisher prepared loads them at
/// startup, at every preparation and again after a restart, building
/// nothing, and the offer is carried in the durable active record a
/// restart reads. A reorg between preparation and activation still fences
/// the candidate.
#[tokio::test(flavor = "multi_thread")]
async fn prepared_runtimes_serve_preparation_and_restart_within_reorg_fences() {
    use std::path::PathBuf;
    use transparent_query_server::history::live::{Command, LiveService, Publication};
    use transparent_shared::history::shardset::LoadOptions;
    use transparent_shared::runtime::prebuild::prepare_publication;
    let prepare = |dir: &Path, retained: &[PathBuf]| {
        let prepared =
            prepare_publication(&ShardSet::open(dir, 3).unwrap(), dir, retained, &|| false)
                .unwrap();
        assert!(prepared.failures.is_empty(), "{:?}", prepared.failures);
        prepared
    };
    let root = tempfile::tempdir().unwrap();
    let initial = root.path().join("initial");
    std::fs::create_dir(&initial).unwrap();
    let (_, mut previous) = two_revisions(&initial);
    let first = prepare(&initial, &[]);
    assert_eq!(first.linked(), 0);
    let pair = one_runtime(Table::Directory) + one_runtime(Table::Pages);
    let config = ServiceConfig {
        cache_bytes: pair * 8,
        ..ServiceConfig::default()
    };
    let set = ShardSet::open(&initial, 3).unwrap();
    let mut expected = set.map_digest.clone();
    let publication = Publication {
        directory: initial.clone(),
        assignment: None,
        map_sha256: expected.clone(),
        runtimes: Some(initial.clone()),
    };
    let state =
        ServiceState::build_with_runtimes(set, config, None, publication.shipped()).unwrap();
    state.spawn_prewarm().await.unwrap();
    let metrics = state.metrics().clone();
    assert_eq!(
        metrics.builds.load(Ordering::Relaxed),
        0,
        "cold load builds nothing"
    );
    assert_eq!(metrics.shipped_loads.load(Ordering::Relaxed), 2);
    let record = root.path().join("active.json");
    let live = LiveService::new(
        state,
        publication,
        config,
        LoadOptions::whole(3),
        record.clone(),
    )
    .unwrap();
    let mut previous_dir = initial;
    for revision in 2..5 {
        let directory = root.path().join(format!("generation-{revision}"));
        std::fs::create_dir(&directory).unwrap();
        let height = FIRST + 80 + revision as u64;
        let (digest, scripts) = write_revision(&directory, height, revision, &previous);
        write_map(&directory, &digest, height, revision, scripts);
        prepare(&directory, std::slice::from_ref(&previous_dir));
        let map_sha256 = ShardSet::open(&directory, 3).unwrap().map_digest;
        let reply = live
            .command(Command::Prepare {
                expected: expected.clone(),
                publication: Publication {
                    directory: directory.clone(),
                    assignment: None,
                    map_sha256: map_sha256.clone(),
                    runtimes: Some(directory.clone()),
                },
            })
            .await
            .unwrap();
        let produced = &reply["runtimes"];
        assert_eq!(produced["runtimes_offered"], true, "{reply}");
        assert_eq!(produced["built"], 0, "{reply}");
        assert_eq!(produced["restored"], 0, "{reply}");
        assert_eq!(produced["shipped_fallbacks"], 0, "{reply}");
        assert_eq!(reply["verification"]["bytes_hashed"], 0, "{reply}");
        live.command(Command::Activate {
            expected,
            map_sha256: map_sha256.clone(),
        })
        .await
        .unwrap();
        live.command(Command::Collect).await.unwrap();
        expected = map_sha256;
        previous = digest;
        previous_dir = directory;
    }
    assert_eq!(metrics.builds.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.shipped_fallbacks.load(Ordering::Relaxed), 0);

    // A restart reads the active record, which names the runtimes, and a
    // fresh process with an empty cache loads them rather than building.
    let active: Publication = serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
    assert_eq!(active.runtimes.as_deref(), Some(previous_dir.as_path()));
    let restarted = ServiceState::build_with_runtimes(
        ShardSet::open(&active.directory, 3).unwrap(),
        config,
        None,
        active.shipped(),
    )
    .unwrap();
    restarted.spawn_prewarm().await.unwrap();
    let restart = restarted.metrics();
    assert_eq!(
        restart.builds.load(Ordering::Relaxed),
        0,
        "restart builds nothing"
    );
    assert_eq!(restart.shipped_loads.load(Ordering::Relaxed), 2);
    drop(restarted);

    // A reorg between preparation and activation fences the prepared
    // candidate whatever runtimes it was offered.
    let directory = root.path().join("orphan");
    std::fs::create_dir(&directory).unwrap();
    let height = FIRST + 90;
    let (digest, scripts) = write_revision(&directory, height, 5, &previous);
    write_map(&directory, &digest, height, 5, scripts);
    prepare(&directory, std::slice::from_ref(&previous_dir));
    let map_sha256 = ShardSet::open(&directory, 3).unwrap().map_digest;
    live.command(Command::Prepare {
        expected: expected.clone(),
        publication: Publication {
            directory: directory.clone(),
            assignment: None,
            map_sha256: map_sha256.clone(),
            runtimes: Some(directory),
        },
    })
    .await
    .unwrap();
    live.command(Command::Invalidate {
        expected: expected.clone(),
        from_height: height,
        keep_digests: Default::default(),
    })
    .await
    .unwrap();
    let refused = live
        .command(Command::Activate {
            expected,
            map_sha256,
        })
        .await
        .unwrap_err();
    assert!(refused.contains("reorg invalidated"), "{refused}");
}

/// A publication invalidated while its runtimes are prepared stops between
/// segments and reports it, so it is never offered; what it finished stays
/// a complete file a later publication may link, never a partial one.
#[test]
fn an_invalidated_preparation_stops_between_segments() {
    use std::sync::atomic::AtomicUsize;
    use transparent_shared::runtime::prebuild::{prepare_publication, PrepareError};
    let dir = tempfile::tempdir().unwrap();
    two_revisions(dir.path());
    let set = ShardSet::open(dir.path(), 3).unwrap();
    let targets = set.warm_targets().len();
    assert!(targets > 2, "several segments to stop between");
    let asked = AtomicUsize::new(0);
    // Invalidated once the first segment is done.
    let superseded = || asked.fetch_add(1, Ordering::SeqCst) >= 1;
    assert!(matches!(
        prepare_publication(&set, dir.path(), &[], &superseded),
        Err(PrepareError::Superseded)
    ));
    let files: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".runtime") || name.ends_with(".partial"))
        .collect();
    assert_eq!(files.len(), 1, "{files:?}");
    assert!(files[0].ends_with(".runtime"));
    // Prepared again, the finished file is kept and the rest built.
    let prepared = prepare_publication(&set, dir.path(), &[], &|| false).unwrap();
    assert_eq!(prepared.files.len(), targets);
    assert_eq!(prepared.linked(), 1);
    assert_eq!(prepared.built(), targets - 1);
}

/// Restart and restaging do not require semantic evidence or read runtime bodies.
#[tokio::test(flavor = "multi_thread")]
async fn required_material_checks_size_without_warming_or_rehashing() {
    use transparent_shared::runtime::{disk::ShippedRuntimes, prebuild::prepare_publication};
    let root = tempfile::tempdir().unwrap();
    two_revisions(root.path());
    let set = ShardSet::open(root.path(), 3).unwrap();
    let prepared = prepare_publication(&set, root.path(), &[], &|| false).unwrap();
    assert!(prepared.failures.is_empty());
    let count = set
        .current()
        .map(|shard| {
            shard
                .tables()
                .into_iter()
                .map(|table| shard.segments(table) as usize)
                .sum::<usize>()
        })
        .sum::<usize>();
    let shard = set.current().next().unwrap();
    let table = shard.tables().into_iter().next().unwrap();
    let source = shard.segment(table, 0).unwrap();
    let params = SharedParams::build(shard.geometry, table.kind()).unwrap();
    let key = (shard.runtime_identity(table).to_string(), table.kind(), 0);
    let path =
        ShippedRuntimes::required(root.path().to_path_buf()).path(&key, &params, source.sha256());
    let build = || {
        ServiceState::build_with_runtimes(
            ShardSet::open_for_serving(root.path(), &LoadOptions::whole(3)).unwrap(),
            ServiceConfig::default(),
            None,
            Some(ShippedRuntimes::required(root.path().to_path_buf())),
        )
        .unwrap()
    };
    assert_eq!(build().check_required_material().unwrap(), count);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[64] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(build().check_required_material().unwrap(), count);
    std::fs::write(&path, b"short").unwrap();
    assert!(build().check_required_material().is_err());
}

#[tokio::test]
async fn a_cold_archive_is_ready_before_any_runtime_is_loaded() {
    let root = tempfile::tempdir().unwrap();
    two_revisions(root.path());
    let set = ShardSet::open_for_serving(root.path(), &LoadOptions::whole(3)).unwrap();
    assert_eq!(set.verification().bytes_hashed, 0);
    let state = ServiceState::build(set, ServiceConfig::default()).unwrap();
    let (status, body) = get(&state, "/v1/ready").await;
    assert_eq!(status, StatusCode::OK);
    let ready: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(ready["ready"], true);
    assert_eq!(ready["warm_runtimes"], 0);
    assert_eq!(ready["prewarm_finished"], false);
}
