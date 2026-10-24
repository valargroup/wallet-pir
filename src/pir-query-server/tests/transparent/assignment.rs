//! A fleet worker serves its subset under the set's global identity.
//!
//! Six shards in two geometries, an assignment splitting them across two
//! archive owners and two recent replicas, and a worker loaded as each. Ids
//! stay global: a request for a shard another worker owns is refused with
//! the assignment named, never answered from a renumbered local copy. Every
//! worker still serves the whole set's public bytes. Readiness in warm mode
//! waits for every assigned runtime; the pilot's loaded-only mode does not.
#![cfg(feature = "transparent-history")]

use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_protocol::transparent::filter as transparent_filter;
use zakura_pir_publisher::transparent as transparent_shard;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tower::ServiceExt;
use transparent_events::{ReceiveEvent, TransparentEvent, Txid};
use transparent_filter::{ScriptBytes, SealParameters, ShardMap, ShardMapEntry};
#[path = "support/history_set.rs"]
mod history_set;
#[path = "support/revision.rs"]
mod revision;
use history_set::*;
use pir_query_server::transparent::history::service::{router, ServiceConfig, ServiceState};
use transparent_shard::layout::{Geometry, DECOUPLED_2K_8K};
use transparent_shard::manifest::{ShardManifest, SCHEMA};
use transparent_shared::history::assignment::{
    Assignment, SetIdentity, WorkerAssignment, WorkerRole, ASSIGNMENT_SCHEMA,
};
use transparent_shared::history::shardset::{LoadOptions, LoadScope, ShardSet};

const SPAN: u64 = 40;
/// Four archive shards at the narrow geometry, two recent at the wider one.
const ARCHIVE: u64 = 4;
const RECENT: u64 = 2;

fn events(start: u64, end: u64) -> Vec<(ScriptBytes, TransparentEvent)> {
    (start..=end)
        .map(|height| {
            let mut txid = [0u8; 32];
            txid[..8].copy_from_slice(&height.to_le_bytes());
            (
                script((height % 8) as u32),
                TransparentEvent::Receive(ReceiveEvent {
                    metadata: None,
                    height: height as u32,
                    txid: Txid(txid),
                    transaction_index: 0,
                    output_index: 0,
                    value: 1_000 + height,
                    coinbase: false,
                }),
            )
        })
        .collect()
}

fn write_shard(
    dir: &Path,
    shard_id: u64,
    geometry: &'static Geometry,
    sealed: bool,
    parent_manifest_digest: &str,
) -> ShardMapEntry {
    let start = FIRST + shard_id * SPAN;
    let end = start + SPAN - 1;
    revision::write(
        dir,
        &revision::Revision {
            shard_id,
            start,
            end,
            geometry,
            sealed,
            revision: 0,
            supersedes: "",
            parent_digest: parent_manifest_digest,
            children: 1,
            genesis: genesis(),
            hash: &hash_at,
        },
        &events(start, end),
    )
    .1
}

/// Four archive shards then two recent, the last unsealed.
fn publish(dir: &Path) -> ShardMap {
    let mut entries = Vec::new();
    let mut parent = String::new();
    for shard_id in 0..ARCHIVE + RECENT {
        // One geometry for both tiers: the smallest, so runtimes are cheap
        // to build here. The plan splits tiers by shard id, not geometry.
        let geometry: &'static Geometry = &DECOUPLED_2K_8K;
        let sealed = shard_id + 1 < ARCHIVE + RECENT;
        let entry = write_shard(dir, shard_id, geometry, sealed, &parent);
        parent = entry.manifest_digest.clone();
        entries.push(entry);
    }
    let seal = entries
        .iter()
        .map(|entry| {
            (
                entry.geometry.clone(),
                SealParameters {
                    max_scripts: 8_192,
                    max_page_rows: 2_048,
                    max_txids: 0,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let map = ShardMap {
        genesis_hash: GENESIS.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: FIRST,
        seal,
        shards: entries,
        recuts: Vec::new(),
    };
    map.check_shape().unwrap();
    std::fs::write(
        dir.join("shards.json"),
        serde_json::to_vec_pretty(&map).unwrap(),
    )
    .unwrap();
    map
}

fn planned(map: &ShardMap) -> Assignment {
    fleet(map, ARCHIVE)
}

fn load_as(dir: &Path, assignment: &Assignment, worker_id: &str, options: LoadOptions) -> ShardSet {
    ShardSet::open_with(
        dir,
        &LoadOptions {
            scope: LoadScope::Assigned {
                assignment: Arc::new(assignment.clone()),
                worker_id: worker_id.into(),
            },
            ..options
        },
    )
    .expect("load")
}

async fn get(state: &ServiceState, path: &str) -> (StatusCode, Vec<u8>) {
    let response = router(state.clone())
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 4 << 20)
        .await
        .unwrap()
        .to_vec();
    (status, body)
}

fn warm_config() -> ServiceConfig {
    ServiceConfig {
        cache_bytes: 4 << 30,
        ..ServiceConfig::default()
    }
}

#[test]
fn an_assignment_for_another_map_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    let mut assignment = planned(&map);
    assignment.set.map_sha256 = "ff".repeat(32);
    let error = ShardSet::open_with(
        dir.path(),
        &LoadOptions {
            scope: LoadScope::Assigned {
                assignment: Arc::new(assignment),
                worker_id: "archive-01".into(),
            },
            ..LoadOptions::whole(3)
        },
    )
    .err()
    .expect("refused");
    assert!(error.to_string().contains("generated for map"), "{error}");
}

#[tokio::test]
async fn a_query_for_an_unassigned_shard_is_misdirected_and_public_bytes_are_served_for_all() {
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    let assignment = planned(&map);
    let set = load_as(dir.path(), &assignment, "archive-01", LoadOptions::whole(3));
    let state = ServiceState::build(set, ServiceConfig::default()).unwrap();
    // Shard 3 is archive-02's.
    let other = &map.shards[3].manifest_digest;
    let (status, body) = get(
        &state,
        &format!("/v2/shards/3/revisions/{other}/setup/directory/0"),
    )
    .await;
    assert_eq!(status, StatusCode::MISDIRECTED_REQUEST);
    let refused: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(refused["error"]
        .as_str()
        .unwrap()
        .contains("not assigned to worker archive-01"));
    // But its filter and manifest are served, as every worker holds them.
    let (status, filter) = get(&state, "/v2/filters/shards/3/filter").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        filter,
        std::fs::read(dir.path().join(other).join("filter.bin")).unwrap()
    );
    let (status, manifest) = get(&state, &format!("/v2/shards/3/revisions/{other}/manifest")).await;
    assert_eq!(status, StatusCode::OK);
    let manifest: ShardManifest = serde_json::from_slice(&manifest).unwrap();
    assert_eq!(&manifest.digest(), other);
    // And an assigned shard's setup is answered.
    let mine = &map.shards[0].manifest_digest;
    let (status, _) = get(
        &state,
        &format!("/v2/shards/0/revisions/{mine}/setup/directory/0"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // A digest nobody published is still a stale client, not a misdirection.
    let (status, _) = get(
        &state,
        &format!(
            "/v2/shards/0/revisions/{}/setup/directory/0",
            "aa".repeat(32)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    // init and health carry the identity.
    let (_, init) = get(&state, "/v2/shards/init").await;
    let init: serde_json::Value = serde_json::from_slice(&init).unwrap();
    assert_eq!(init["shards"], 6);
    assert_eq!(init["assigned_shards"], 2);
    assert_eq!(init["worker_id"], "archive-01");
    assert_eq!(init["assignment_sha256"], assignment.digest());
    // Set-wide: the owner declares every geometry the set names, since a
    // wallet may fetch init from any worker and refuses a map naming a
    // geometry the document leaves out. Both tiers here share one.
    let mut declared: Vec<&str> = init["geometries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["name"].as_str().unwrap())
        .collect();
    declared.sort_unstable();
    assert_eq!(declared, ["decoupled-2k-8k"]);
}

/// Readiness checks metadata and file sizes only: a worker is ready before
/// background warming finishes, and warming then loads every assigned runtime.
#[tokio::test(flavor = "multi_thread")]
async fn ready_does_not_wait_for_background_warming() {
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    let assignment = planned(&map);
    let set = load_as(dir.path(), &assignment, "recent-01", LoadOptions::whole(3));
    let state = ServiceState::build(set, warm_config()).unwrap();
    let (status, body) = get(&state, "/v1/ready").await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let ready: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(ready["mode"], "loaded-only");
    assert_eq!(ready["warm_runtimes"], 0);
    assert_eq!(ready["prewarm_finished"], false);
    assert_eq!(
        ready["target_runtimes"], 4,
        "two shards, two tables, one segment each"
    );
    assert_eq!(ready["assignment_sha256"], assignment.digest());
    assert_eq!(ready["role"], "recent-replica");
    state.spawn_prewarm().await.unwrap();
    let (status, body) = get(&state, "/v1/ready").await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let ready: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(ready["warm_runtimes"], 4);
    assert_eq!(ready["prewarm_finished"], true);
}

/// Preparing a candidate reclaims idle runtimes, but never those of the
/// active snapshot's current revisions: directory and page runtimes, which
/// page shards key by their own identity, both stay resident.
#[tokio::test(flavor = "multi_thread")]
async fn preparation_eviction_keeps_current_directory_and_page_runtimes() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    let assignment = planned(&map);
    let set = load_as(dir.path(), &assignment, "recent-01", LoadOptions::whole(3));
    assert!(
        set.current().any(|shard| shard
            .runtime_identities()
            .iter()
            .any(|identity| *identity != shard.digest)),
        "the fixture's page tables have their own runtime identity"
    );
    let state = ServiceState::build(set, warm_config()).unwrap();
    state.spawn_prewarm().await.unwrap();
    let resident = state.metrics().cache_entries.load(Ordering::Acquire);
    assert_eq!(resident, 4, "two shards, directory and pages each");
    state.evict_unpinned_except_current();
    assert_eq!(
        state.metrics().cache_entries.load(Ordering::Acquire),
        resident
    );
    assert_eq!(state.metrics().evictions.load(Ordering::Acquire), 0);
}

/// Runtimes load on demand under the cache budget, so an assignment whose
/// runtimes do not all fit the cache at once is accepted and ready.
#[tokio::test(flavor = "multi_thread")]
async fn an_assignment_larger_than_the_cache_is_accepted_and_ready() {
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    let assignment = planned(&map);
    let set = load_as(dir.path(), &assignment, "recent-01", LoadOptions::whole(3));
    let state = ServiceState::build(
        set,
        ServiceConfig {
            cache_bytes: 64 << 20,
            ..ServiceConfig::default()
        },
    )
    .unwrap();
    let (status, body) = get(&state, "/v1/ready").await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
}

#[test]
fn retention_by_bytes_keeps_newest_first_and_reports_the_rest_prunable() {
    // One shard, republished three times as a growing tail.
    let dir = tempfile::tempdir().unwrap();
    let mut entries = Vec::new();
    let mut digests: Vec<String> = Vec::new();
    for revision in 0..4u32 {
        let end = FIRST + 10 + revision as u64 * 5;
        let supersedes = digests.last().cloned().unwrap_or_default();
        let (_, entry) = revision::write(
            dir.path(),
            &revision::Revision {
                shard_id: 0,
                start: FIRST,
                end,
                geometry: &DECOUPLED_2K_8K,
                sealed: false,
                revision,
                supersedes: &supersedes,
                parent_digest: "",
                children: 1,
                genesis: genesis(),
                hash: &hash_at,
            },
            &events(FIRST, end),
        );
        digests.push(entry.manifest_digest.clone());
        entries = vec![entry];
    }
    let map = ShardMap {
        genesis_hash: GENESIS.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: FIRST,
        seal: BTreeMap::from([(
            DECOUPLED_2K_8K.name.to_string(),
            SealParameters {
                max_scripts: 8_192,
                max_page_rows: 2_048,
                max_txids: 0,
            },
        )]),
        shards: entries,
        recuts: Vec::new(),
    };
    std::fs::write(
        dir.path().join("shards.json"),
        serde_json::to_vec_pretty(&map).unwrap(),
    )
    .unwrap();

    // Three superseded revisions on disk. A byte bound that fits one keeps the
    // newest and reports the two older ones prunable.
    // The two newest superseded revisions have page shards with rows, the
    // oldest two do not.
    let one_revision = DECOUPLED_2K_8K.directory_bytes_per_segment()
        + DECOUPLED_2K_8K.page_bytes_per_segment()
        + 1024;
    let set = ShardSet::open_with(
        dir.path(),
        &LoadOptions {
            retain_revisions: 3,
            retain_bytes: Some(one_revision),
            prune_excess: true,
            scope: LoadScope::Whole,
        },
    )
    .unwrap();
    assert_eq!(set.revisions().len(), 2, "current plus one retained");
    assert!(
        set.revision(&digests[2]).is_some(),
        "the newest superseded is kept"
    );
    let prunable: Vec<u32> = set.prunable().iter().map(|p| p.revision).collect();
    assert_eq!(prunable, vec![1, 0]);
    // The same bound without permission to prune refuses to start, as before.
    let error = ShardSet::open_with(
        dir.path(),
        &LoadOptions {
            retain_revisions: 3,
            retain_bytes: Some(one_revision),
            prune_excess: false,
            scope: LoadScope::Whole,
        },
    )
    .err()
    .expect("refused");
    assert!(error.to_string().contains("prune the set"), "{error}");
}

#[test]
fn a_hand_written_assignment_round_trips_and_checks_shape() {
    let assignment = Assignment {
        schema: ASSIGNMENT_SCHEMA.into(),
        set: SetIdentity {
            shard_schema: SCHEMA.into(),
            map_sha256: "00".repeat(32),
            network: "main".into(),
            genesis_hash: GENESIS.into(),
            shards: 2,
            start_height: FIRST,
            covered_through: FIRST + 79,
            recent_from_shard: 1,
        },
        generated_by: generated(),
        workers: vec![
            WorkerAssignment {
                id: "a".into(),
                role: WorkerRole::ArchiveOwner,
                replica_group: None,
                upstream: "10.0.0.1:8093".into(),
                cache_bytes: 1,
                shards: vec![0],
                estimated_resident_bytes: 0,
            },
            WorkerAssignment {
                id: "r".into(),
                role: WorkerRole::RecentReplica,
                replica_group: Some("recent".into()),
                upstream: "10.0.0.2:8093".into(),
                cache_bytes: 1,
                shards: vec![1],
                estimated_resident_bytes: 0,
            },
        ],
        unassigned: vec![],
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("assignment.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&assignment).unwrap()).unwrap();
    let loaded = Assignment::load(&path).unwrap();
    assert_eq!(loaded, assignment);
    assert_eq!(loaded.digest(), assignment.digest());
}

#[tokio::test]
async fn a_publication_held_under_another_assignment_row_is_refused() {
    use pir_query_server::transparent::history::live::{
        Command, LiveService, Publication, ASSIGNMENT_CHANGED,
    };
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    let assignment = planned(&map);
    let held = dir.path().join("assignment.json");
    std::fs::write(&held, assignment.canonical_bytes()).unwrap();
    let options = LoadOptions {
        scope: LoadScope::Assigned {
            assignment: Arc::new(assignment.clone()),
            worker_id: "recent-01".into(),
        },
        ..LoadOptions::whole(3)
    };
    let set = ShardSet::open_with(dir.path(), &options).unwrap();
    let digest = set.map_digest.clone();
    let state = ServiceState::build(set, warm_config()).unwrap();
    state.spawn_prewarm().await.unwrap();
    let publication = |path: &Path| Publication {
        directory: dir.path().to_path_buf(),
        assignment: Some(path.to_path_buf()),
        map_sha256: digest.clone(),
        runtimes: None,
    };
    let live = LiveService::new(
        state,
        publication(&held),
        warm_config(),
        options,
        dir.path().join("active.json"),
    )
    .unwrap();
    let prepare = |path: &Path| Command::Prepare {
        expected: digest.clone(),
        publication: publication(path),
    };
    // The same row, even in another file, is the publication already held.
    let copy = dir.path().join("copy.json");
    std::fs::write(&copy, assignment.canonical_bytes()).unwrap();
    live.command(prepare(&copy)).await.unwrap();
    // Another worker's row changing does not concern this one.
    let mut peer = assignment.clone();
    peer.workers
        .iter_mut()
        .find(|w| w.id == "recent-02")
        .unwrap()
        .cache_bytes += 1;
    let peer_path = dir.path().join("peer.json");
    std::fs::write(&peer_path, peer.canonical_bytes()).unwrap();
    live.command(prepare(&peer_path)).await.unwrap();
    // This worker's own row changing is refused, not reported prepared.
    let mut changed = assignment.clone();
    changed
        .workers
        .iter_mut()
        .find(|w| w.id == "recent-01")
        .unwrap()
        .cache_bytes += 1;
    let changed_path = dir.path().join("changed.json");
    std::fs::write(&changed_path, changed.canonical_bytes()).unwrap();
    assert_eq!(
        live.command(prepare(&changed_path)).await.unwrap_err(),
        ASSIGNMENT_CHANGED
    );
}

#[path = "support/golden.rs"]
mod golden;

/// Every refusal the history worker sends, and a setup it answers, as served:
/// status, content type, retry delay and exact body. The display worker shares
/// this request path, so a change to one must not move the other's bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_answers_are_pinned() {
    use golden::{Transcript, Upload};
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    let assignment = planned(&map);
    let set = load_as(dir.path(), &assignment, "archive-01", LoadOptions::whole(3));
    let app = router(ServiceState::build(set, ServiceConfig::default()).unwrap());
    let digest = |id: usize| map.shards[id].manifest_digest.clone();
    let file_sha256 = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(
        std::fs::read(dir.path().join("shards.json")).unwrap(),
    ));
    let names: Vec<(String, String)> = (0..map.shards.len())
        .map(|id| (digest(id), format!("rev{id}")))
        .chain([
            (map_sha256(&map), "map".into()),
            (file_sha256, "map-file".into()),
            ("ab".repeat(32), "unknown".into()),
        ])
        .collect();
    let names: Vec<(&str, &str)> = names
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    let mut t = Transcript::new(&names);
    let (d0, d3, unknown) = (digest(0), digest(3), "ab".repeat(32));
    let path =
        |id: usize, digest: &str, tail: &str| format!("/v2/shards/{id}/revisions/{digest}/{tail}");

    // Setup: answered, unknown table, stale, another shard's revision, and
    // a shard assigned to another worker.
    t.send(&app, &path(0, &d0, "setup/directory/0"), Upload::None)
        .await;
    t.send(&app, &path(0, &d0, "setup/bogus/0"), Upload::None)
        .await;
    t.send(&app, &path(0, &d0, "setup/directory/9"), Upload::None)
        .await;
    t.send(&app, &path(0, &unknown, "setup/directory/0"), Upload::None)
        .await;
    t.send(&app, &path(1, &d0, "setup/directory/0"), Upload::None)
        .await;
    t.send(&app, &path(3, &d3, "setup/directory/0"), Upload::None)
        .await;

    // Query: unknown table, stale, misdirected, no declared length, a wrong
    // length, a body of the right length that is not a query, and one bound
    // to the revision but otherwise empty.
    let query = path(0, &d0, "query/directory");
    t.send(
        &app,
        &path(0, &d0, "query/bogus"),
        Upload::Sized(vec![0; 16]),
    )
    .await;
    t.send(
        &app,
        &path(0, &unknown, "query/directory"),
        Upload::Sized(vec![0; 16]),
    )
    .await;
    t.send(
        &app,
        &path(3, &d3, "query/directory"),
        Upload::Sized(vec![0; 16]),
    )
    .await;
    t.send(&app, &query, Upload::Unsized(vec![0; 16])).await;
    t.send(&app, &query, Upload::Sized(vec![0; 16])).await;
    let length = expected_query_length(&app, &query).await;
    t.send(&app, &query, Upload::Sized(vec![0; length])).await;
    let mut binding = transparent_shard::manifest::query_binding(&d0, "directory").to_vec();
    binding.resize(length, 0);
    t.send(&app, &query, Upload::Sized(binding)).await;
    t.check("history_answers.txt");
}

/// The length a query to `path` must have, read from the refusal of a wrong one.
async fn expected_query_length(app: &axum::Router, path: &str) -> usize {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("content-length", "1")
                .body(Body::from(vec![0u8]))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let error = error["error"].as_str().unwrap();
    error
        .split("must be exactly ")
        .nth(1)
        .and_then(|rest| rest.split([' ', ',']).next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("no length in {error:?}"))
}

/// With prepared runtimes beside a publication, each worker's transfer
/// inventory lists exactly the runtimes of its own shards, and a worker
/// staged from it loads every assigned runtime from them without building.
#[path = "support/provider.rs"]
mod provider;
