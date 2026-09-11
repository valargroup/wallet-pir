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

use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tower::ServiceExt;
use transparent_events::{ReceiveEvent, TransparentEvent, Txid};
use transparent_filter::{filter_hash, BlockHash, ScriptBytes, SealParameters, ShardMap};
use transparent_shard::build::build_shard;
use transparent_shard::layout::{Geometry, RECENT_4K};
use transparent_shard::manifest::{
    ManifestLayout, ManifestOccupancy, ManifestSeal, ShardManifest, TableGeometry, SCHEMA,
};
use transparent_shard_server::metrics::Metrics;
use transparent_shard_server::runtime::{reserved_bytes, CacheError, RuntimeCache, SharedParams};
use transparent_shard_server::service::{router, ServiceConfig, ServiceState};
use transparent_shard_server::shardset::{ShardSet, Table, DEFAULT_RETAIN_REVISIONS};

/// The smallest registry geometry, so a runtime is cheap enough to build
/// several times in a test.
const GEOMETRY: Geometry = RECENT_4K;

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
    let built = build_shard(
        0,
        FIRST,
        end,
        genesis(),
        hash_at(end),
        transparent_filter::RANGE_PROFILE,
        geometry,
        &events,
    )
    .expect("build");

    let manifest = ShardManifest {
        schema: SCHEMA.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        geometry: geometry.name.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        genesis_hash: GENESIS.to_string(),
        shard_id: 0,
        start_height: FIRST,
        end_height: end,
        parent_block_hash: hash_at(FIRST - 1).to_display_hex(),
        terminal_block_hash: hash_at(end).to_display_hex(),
        parent_manifest_digest: String::new(),
        // The tail: a revision that supersedes another is by construction not
        // final, and a sealed shard would have nothing to supersede.
        sealed: false,
        revision,
        supersedes: supersedes.to_string(),
        seal: ManifestSeal {
            scripts_target: 8_192,
            scripts_capacity: 16_384,
            page_rows_target: 2_048,
            page_rows_capacity: 4_096,
        },
        layout: ManifestLayout {
            max_script_bytes: transparent_shard::MAX_SCRIPT_BYTES as u32,
            inline_events: transparent_shard::INLINE_EVENTS,
            events_per_page: transparent_shard::EVENTS_PER_PAGE,
            page_row_header_bytes: transparent_shard::PAGE_ROW_HEADER_BYTES as u32,
            page_entry_header_bytes: transparent_shard::PAGE_ENTRY_HEADER_BYTES as u32,
            directory_choices: transparent_shard::build::DIRECTORY_CHOICES as u32,
        },
        filter_hash: filter_hash(built.filter.as_slice()).to_display_hex(),
        directory_segments: built
            .directory
            .iter()
            .map(|segment| TableGeometry {
                rows: geometry.directory_rows,
                row_bytes: geometry.directory_row_bytes as u32,
                sha256: hex::encode(<sha2::Sha256 as sha2::Digest>::digest(segment)),
            })
            .collect(),
        page_segments: built
            .pages
            .iter()
            .map(|segment| TableGeometry {
                rows: geometry.page_rows,
                row_bytes: geometry.page_row_bytes as u32,
                sha256: hex::encode(<sha2::Sha256 as sha2::Digest>::digest(segment)),
            })
            .collect(),
        occupancy: ManifestOccupancy {
            scripts: built.scripts,
            page_rows: built.page_rows,
            fragments: built.fragments,
            events: built.events,
            blocks: end - FIRST + 1,
            txids: 0,
            excluded_scripts: built.excluded_scripts,
        },
    };

    let digest = manifest.digest();
    let shard_dir = dir.join(&digest);
    std::fs::create_dir_all(&shard_dir).unwrap();
    std::fs::write(shard_dir.join("manifest.json"), manifest.canonical_bytes()).unwrap();
    std::fs::write(shard_dir.join("filter.bin"), built.filter.as_slice()).unwrap();
    for (index, segment) in built.directory.iter().enumerate() {
        std::fs::write(shard_dir.join(format!("directory.{index}.bin")), segment).unwrap();
    }
    for (index, segment) in built.pages.iter().enumerate() {
        std::fs::write(shard_dir.join(format!("pages.{index}.bin")), segment).unwrap();
    }
    (digest, built.scripts)
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
            page_segments: 1,
            manifest_digest: digest.to_string(),
            revision,
            sealed: false,
        }],
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
    let (rlwe, scheme) = ipir_sp::params_for_simplepir(
        table.rows(&GEOMETRY),
        (table.row_bytes(&GEOMETRY) as u64) * 8,
    )
    .unwrap();
    reserved_bytes(&rlwe, &scheme)
}

/// A republished tail leaves its predecessor on disk. That is what the
/// publisher writes by design, and the loader used to refuse to start on it.
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
            &format!("/v1/shards/0/revisions/{revision}/setup/directory/0"),
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
        &format!("/v1/shards/0/revisions/{gone}/setup/directory/0"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let body: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert!(body["error"].as_str().unwrap().contains(&gone));
    assert!(body["retry"].as_str().unwrap().contains("refresh"));

    // The digest of the map the wallet should refetch, so it can tell whether
    // refreshing will actually help.
    let (_, raw) = get(&state, "/v1/shards").await;
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
                .uri("/v1/shards")
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
    let (_, again) = get(&state, "/v1/filters/shards").await;
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

    let path = format!("/v1/shards/0/revisions/{new}/setup/directory/0");
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

    for table in ["directory", "pages"] {
        let (status, _) = get(
            &state,
            &format!("/v1/shards/0/revisions/{new}/setup/{table}/0"),
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
                .uri(format!("/v1/shards/0/revisions/{new}/setup/directory/0"))
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
        &format!("/v1/shards/0/revisions/{new}/setup/directory/0"),
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
            shard.segment(Table::Directory, 0).unwrap().clone(),
        )
        .await
        .expect("the first runtime fits an empty cache");

    let refused = cache
        .get(
            (new.clone(), Table::Pages, 0),
            shared(Table::Pages),
            shard.segment(Table::Pages, 0).unwrap().clone(),
        )
        .await;
    assert!(
        matches!(refused, Err(CacheError::Overloaded)),
        "a pinned runtime must not be evicted to admit another"
    );
    assert_eq!(metrics.evictions.load(Ordering::Relaxed), 0);
    assert_eq!(cache.entries(), 1);
    assert_eq!(cache.resident_bytes(), budget);
    cache.evict_unpinned();
    assert_eq!(
        cache.resident_bytes(),
        budget,
        "trimming preserves live handles"
    );

    // Once nothing is holding it, the same request succeeds by evicting it.
    drop(held);
    cache
        .get(
            (new.clone(), Table::Pages, 0),
            shared(Table::Pages),
            shard.segment(Table::Pages, 0).unwrap().clone(),
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

#[tokio::test(flavor = "multi_thread")]
async fn revision_churn_bounds_runtimes_and_collects_idle_snapshots() {
    use transparent_shard_server::live::{Command, LiveService, Publication};
    use transparent_shard_server::service::ReadinessMode;
    use transparent_shard_server::shardset::LoadOptions;
    let root = tempfile::tempdir().unwrap();
    let a = root.path().join("initial");
    std::fs::create_dir(&a).unwrap();
    let (_, mut previous) = two_revisions(&a);
    let set = ShardSet::open(&a, 3).unwrap();
    let mut expected = set.map_digest.clone();
    let pair = one_runtime(Table::Directory) + one_runtime(Table::Pages);
    let config = ServiceConfig {
        cache_bytes: pair * 8,
        readiness: ReadinessMode::Warm,
        ..ServiceConfig::default()
    };
    let disk = transparent_shard_server::runtime::disk::DiskCache::new(
        root.path().join("cache"),
        pair * 16,
    )
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
            },
        })
        .await
        .unwrap();
        assert_eq!(
            metrics.resident_bytes.load(Ordering::Relaxed),
            pair * 2,
            "only the current and preparing pair remain resident, even with spare cache capacity"
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
    assert_eq!(
        cached, 8,
        "only the four live/retained revision pairs remain on disk"
    );
}

/// Live collection must defer optional disk pruning while a snapshot writer
/// holds its lock, keeping readiness and subsequent control operations responsive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_collection_keeps_status_and_readiness_responsive() {
    use std::time::Duration;
    use transparent_shard_server::live::{Command, LiveService, Publication};
    use transparent_shard_server::shardset::LoadOptions;
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("initial");
    std::fs::create_dir(&directory).unwrap();
    two_revisions(&directory);
    let set = ShardSet::open(&directory, 3).unwrap();
    let map_sha256 = set.map_digest.clone();
    let cache_dir = root.path().join("cache");
    let disk = transparent_shard_server::runtime::disk::DiskCache::new(
        cache_dir.clone(),
        64 * 1024 * 1024,
    )
    .unwrap();
    let config = ServiceConfig::default();
    let state = ServiceState::build_with_disk(set, config, Some(disk)).unwrap();
    let live = LiveService::new(
        state,
        Publication {
            directory,
            assignment: None,
            map_sha256,
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
    use transparent_shard_server::live::{Command, LiveService, Publication};
    use transparent_shard_server::shardset::LoadOptions;
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

/// Hot activation keeps the old revision usable, shares bounded runtime state,
/// and refuses orphaned answers after a reorg without restarting the server.
#[tokio::test(flavor = "multi_thread")]
async fn live_prepare_activate_and_invalidate() {
    use transparent_shard_server::live::{Command, LiveService, Publication};
    use transparent_shard_server::service::ReadinessMode;
    use transparent_shard_server::shardset::LoadOptions;
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
        readiness: ReadinessMode::Warm,
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
        },
    })
    .await
    .unwrap();
    let before = live
        .router()
        .oneshot(Request::get("/v1/shards").body(Body::empty()).unwrap())
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
        .oneshot(Request::get("/v1/shards").body(Body::empty()).unwrap())
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
            Request::get(format!("/v1/shards/0/revisions/{new}/manifest"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(
        live.router()
            .oneshot(Request::get("/v1/shards").body(Body::empty()).unwrap())
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
                Request::get(format!("/v1/shards/0/revisions/{old}/manifest"))
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
#[path = "../examples/support/query.rs"]
mod burst_query;
