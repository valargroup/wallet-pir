//! A fleet worker serves its subset under the set's global identity.
//!
//! Six shards in two geometries, an assignment splitting them across two
//! archive owners and two recent replicas, and a worker loaded as each. Ids
//! stay global: a request for a shard another worker owns is refused with
//! the assignment named, never answered from a renumbered local copy. Every
//! worker still serves the whole set's public bytes. Readiness in warm mode
//! waits for every assigned runtime; the pilot's loaded-only mode does not.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tower::ServiceExt;
use transparent_events::{ReceiveEvent, TransparentEvent, Txid};
use transparent_filter::{
    filter_hash, BlockHash, ScriptBytes, SealParameters, ShardMap, ShardMapEntry,
};
use transparent_shard::build::build_shard;
use transparent_shard::layout::{Geometry, RECENT_4K, RECENT_8K};
use transparent_shard::manifest::{
    ManifestLayout, ManifestOccupancy, ManifestSeal, ShardManifest, TableGeometry, SCHEMA,
};
use transparent_shard_server::assignment::{
    Assignment, GeneratedBy, SetIdentity, WorkerAssignment, WorkerRole, ASSIGNMENT_SCHEMA,
};
use transparent_shard_server::router::{files_for, plan, render_caddyfile, RosterEntry};
use transparent_shard_server::service::{router, ReadinessMode, ServiceConfig, ServiceState};
use transparent_shard_server::shardset::{LoadOptions, LoadScope, ShardSet};

const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;
const FIRST: u64 = 3_428_143;
const SPAN: u64 = 40;
/// Four archive shards at the narrow geometry, two recent at the wider one.
const ARCHIVE: u64 = 4;
const RECENT: u64 = 2;

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

fn events(start: u64, end: u64) -> Vec<(ScriptBytes, TransparentEvent)> {
    (start..=end)
        .map(|height| {
            let mut txid = [0u8; 32];
            txid[..8].copy_from_slice(&height.to_le_bytes());
            (
                script((height % 8) as u32),
                TransparentEvent::Receive(ReceiveEvent {
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
    let built = build_shard(
        shard_id,
        start,
        end,
        genesis(),
        hash_at(end),
        transparent_filter::RANGE_PROFILE,
        geometry,
        &events(start, end),
    )
    .expect("build");
    let table_geometry = |segments: &[Vec<u8>], rows: u64, row_bytes: u32| {
        segments
            .iter()
            .map(|segment| TableGeometry {
                rows,
                row_bytes,
                sha256: hex::encode(<sha2::Sha256 as sha2::Digest>::digest(segment)),
            })
            .collect::<Vec<_>>()
    };
    let manifest = ShardManifest {
        schema: SCHEMA.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        geometry: geometry.name.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        genesis_hash: GENESIS.to_string(),
        shard_id,
        start_height: start,
        end_height: end,
        parent_block_hash: hash_at(start - 1).to_display_hex(),
        terminal_block_hash: hash_at(end).to_display_hex(),
        tag_salt_counter: built.tag_salt_counter,
        parent_manifest_digest: parent_manifest_digest.to_string(),
        sealed,
        revision: 0,
        supersedes: String::new(),
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
        directory_segments: table_geometry(
            &built.directory,
            geometry.directory_rows,
            geometry.directory_row_bytes as u32,
        ),
        page_segments: table_geometry(
            &built.pages,
            geometry.page_rows,
            geometry.page_row_bytes as u32,
        ),
        occupancy: ManifestOccupancy {
            scripts: built.scripts,
            page_rows: built.page_rows,
            fragments: built.fragments,
            events: built.events,
            blocks: SPAN,
            txids: 0,
            excluded_scripts: built.excluded_scripts,
        },
        directory_choice: None,
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
    ShardMapEntry {
        shard_id,
        geometry: geometry.name.to_string(),
        start_height: start,
        end_height: end,
        parent_block_hash: manifest.parent_block_hash.clone(),
        terminal_block_hash: manifest.terminal_block_hash.clone(),
        filter_hash: manifest.filter_hash.clone(),
        scripts: built.scripts,
        page_rows: built.page_rows,
        txids: 0,
        directory_segments: built.directory_segments(),
        page_segments: built.page_segments(),
        manifest_digest: digest,
        revision: manifest.revision,
        sealed,
    }
}

/// Four archive shards then two recent, the last unsealed.
fn publish(dir: &Path) -> ShardMap {
    let mut entries = Vec::new();
    let mut parent = String::new();
    for shard_id in 0..ARCHIVE + RECENT {
        let geometry: &'static Geometry = if shard_id < ARCHIVE {
            &RECENT_4K
        } else {
            &RECENT_8K
        };
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
    };
    map.check_shape().unwrap();
    std::fs::write(
        dir.join("shards.json"),
        serde_json::to_vec_pretty(&map).unwrap(),
    )
    .unwrap();
    map
}

fn map_sha256(map: &ShardMap) -> String {
    hex::encode(<sha2::Sha256 as sha2::Digest>::digest(
        serde_json::to_vec(map).unwrap(),
    ))
}

fn roster() -> Vec<RosterEntry> {
    let mut roster = Vec::new();
    for i in 0..2 {
        roster.push(RosterEntry {
            id: format!("archive-0{}", i + 1),
            role: WorkerRole::ArchiveOwner,
            replica_group: None,
            ssh_host: format!("10.0.1.{}", i + 1),
            upstream: format!("10.0.1.{}:8093", i + 1),
            cache_bytes: 4 << 30,
        });
    }
    for i in 0..2 {
        roster.push(RosterEntry {
            id: format!("recent-0{}", i + 1),
            role: WorkerRole::RecentReplica,
            replica_group: Some("recent".into()),
            ssh_host: format!("10.0.2.{}", i + 1),
            upstream: format!("10.0.2.{}:8093", i + 1),
            cache_bytes: 4 << 30,
        });
    }
    roster
}

fn generated() -> GeneratedBy {
    GeneratedBy {
        tool: "test".into(),
        source_sha: None,
        generated_at: "2026-09-08T00:00:00Z".into(),
    }
}

fn planned(map: &ShardMap) -> Assignment {
    plan(map, &map_sha256(map), &roster(), ARCHIVE, 0.1, generated()).unwrap()
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
        readiness: ReadinessMode::Warm,
        ..ServiceConfig::default()
    }
}

#[test]
fn a_subset_loads_only_assigned_tables_and_keeps_global_ids() {
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    let assignment = planned(&map);
    let owner = load_as(dir.path(), &assignment, "archive-01", LoadOptions::whole(3));
    assert_eq!(owner.len(), 6, "the map is global");
    assert_eq!(owner.assigned_len(), 2);
    assert!(owner.is_assigned(0) && owner.is_assigned(1));
    assert!(!owner.is_assigned(2) && !owner.is_assigned(4));
    // Every shard's manifest and filter are held; only assigned tables are.
    for shard_id in 0..6 {
        let meta = owner.meta(shard_id).expect("metadata for every shard");
        assert_eq!(meta.manifest.shard_id, shard_id);
        assert_eq!(
            meta.filter,
            std::fs::read(dir.path().join(&meta.digest).join("filter.bin")).unwrap()
        );
    }
    assert!(owner.get(0).is_some());
    assert!(owner.get(3).is_none());
    assert_eq!(owner.scope().unwrap().worker_id, "archive-01");
    assert_eq!(
        owner.scope().unwrap().assignment_sha256,
        assignment.digest()
    );
    // The tables of an unassigned shard need not even exist on this worker.
    let replica_dir = tempfile::tempdir().unwrap();
    let files = files_for(dir.path(), &map, &assignment, "recent-01").unwrap();
    for file in &files {
        let from = dir.path().join(file.trim_end_matches('/'));
        let to = replica_dir.path().join(file.trim_end_matches('/'));
        if from.is_dir() {
            copy_dir(&from, &to);
        } else {
            std::fs::create_dir_all(to.parent().unwrap()).unwrap();
            std::fs::copy(&from, &to).unwrap();
        }
    }
    let replica = load_as(
        replica_dir.path(),
        &assignment,
        "recent-01",
        LoadOptions::whole(3),
    );
    assert_eq!(replica.assigned_len(), 2);
    assert!(replica.is_assigned(4) && replica.is_assigned(5));
    assert!(replica.meta(0).is_some(), "archive metadata was copied too");
    assert!(!replica_dir
        .path()
        .join(&map.shards[0].manifest_digest)
        .join("directory.0.bin")
        .exists());
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
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
        &format!("/v1/shards/3/revisions/{other}/setup/directory/0"),
    )
    .await;
    assert_eq!(status, StatusCode::MISDIRECTED_REQUEST);
    let refused: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(refused["error"]
        .as_str()
        .unwrap()
        .contains("not assigned to worker archive-01"));
    // But its filter and manifest are served, as every worker holds them.
    let (status, filter) = get(&state, "/v1/filters/shards/3/filter").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        filter,
        std::fs::read(dir.path().join(other).join("filter.bin")).unwrap()
    );
    let (status, manifest) = get(&state, &format!("/v1/shards/3/revisions/{other}/manifest")).await;
    assert_eq!(status, StatusCode::OK);
    let manifest: ShardManifest = serde_json::from_slice(&manifest).unwrap();
    assert_eq!(&manifest.digest(), other);
    // And an assigned shard's setup is answered.
    let mine = &map.shards[0].manifest_digest;
    let (status, _) = get(
        &state,
        &format!("/v1/shards/0/revisions/{mine}/setup/directory/0"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // A digest nobody published is still a stale client, not a misdirection.
    let (status, _) = get(
        &state,
        &format!(
            "/v1/shards/0/revisions/{}/setup/directory/0",
            "aa".repeat(32)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    // init and health carry the identity.
    let (_, init) = get(&state, "/v1/shards/init").await;
    let init: serde_json::Value = serde_json::from_slice(&init).unwrap();
    assert_eq!(init["shards"], 6);
    assert_eq!(init["assigned_shards"], 2);
    assert_eq!(init["worker_id"], "archive-01");
    assert_eq!(init["assignment_sha256"], assignment.digest());
    // Set-wide: the owner declares the other geometry's parameters
    // too, since a wallet may fetch init from any worker and refuses a map
    // naming a geometry the document leaves out.
    let mut declared: Vec<&str> = init["geometries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["name"].as_str().unwrap())
        .collect();
    declared.sort_unstable();
    assert_eq!(declared, ["recent-4k", "recent-8k"]);
    assert_eq!(
        init["geometries"].as_array().unwrap().len(),
        2,
        "every geometry the set names, not only the held one"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ready_waits_for_every_assigned_runtime_in_warm_mode_and_not_in_pilot_mode() {
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    let assignment = planned(&map);
    let set = load_as(dir.path(), &assignment, "recent-01", LoadOptions::whole(3));
    let state = ServiceState::build(set, warm_config()).unwrap();
    let (status, body) = get(&state, "/v1/ready").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let ready: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(ready["ready"], false);
    assert_eq!(ready["reason"], "prewarming");
    assert_eq!(
        ready["target_runtimes"], 4,
        "two shards, two tables, one segment each"
    );
    assert_eq!(ready["assignment_sha256"], assignment.digest());
    state.spawn_prewarm().await.unwrap();
    let (status, body) = get(&state, "/v1/ready").await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let ready: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(ready["warm_runtimes"], 4);
    assert_eq!(ready["prewarm_finished"], true);
    assert_eq!(ready["role"], "recent-replica");

    let pilot = load_as(dir.path(), &assignment, "recent-01", LoadOptions::whole(3));
    let pilot = ServiceState::build(pilot, ServiceConfig::default()).unwrap();
    let (status, body) = get(&pilot, "/v1/ready").await;
    assert_eq!(status, StatusCode::OK);
    let ready: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(ready["mode"], "loaded-only");
    assert_eq!(ready["warm_runtimes"], 0);
}

#[test]
fn an_assignment_that_does_not_fit_the_cache_is_refused_in_warm_mode() {
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    let assignment = planned(&map);
    let set = load_as(dir.path(), &assignment, "recent-01", LoadOptions::whole(3));
    let error = ServiceState::build(
        set,
        ServiceConfig {
            cache_bytes: 64 << 20,
            readiness: ReadinessMode::Warm,
            ..ServiceConfig::default()
        },
    )
    .err()
    .expect("refused");
    assert!(error.contains("cache budget"), "{error}");
    // The same budget is accepted in loaded-only mode, cold and slow.
    let set = load_as(dir.path(), &assignment, "recent-01", LoadOptions::whole(3));
    ServiceState::build(
        set,
        ServiceConfig {
            cache_bytes: 64 << 20,
            ..ServiceConfig::default()
        },
    )
    .unwrap();
}

#[test]
fn the_plan_balances_owners_and_the_router_matches_the_golden_file() {
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    let assignment = planned(&map);
    let owners: Vec<&WorkerAssignment> = assignment
        .workers
        .iter()
        .filter(|w| w.role == WorkerRole::ArchiveOwner)
        .collect();
    assert_eq!(owners[0].shards, vec![0, 1]);
    assert_eq!(owners[1].shards, vec![2, 3]);
    assert_eq!(
        owners[0].estimated_resident_bytes,
        owners[1].estimated_resident_bytes
    );
    for replica in assignment
        .workers
        .iter()
        .filter(|w| w.role == WorkerRole::RecentReplica)
    {
        assert_eq!(replica.shards, vec![4, 5]);
    }
    // The router is rendered from the assignment alone, so it can be pinned
    // against a digest-free golden file: the digest line is the only thing
    // that varies with the map's content.
    let rendered = render_caddyfile(&assignment, "transparent.example");
    let stable: String = rendered
        .lines()
        .filter(|line| !line.starts_with("# Rendered by shard-assign"))
        .map(|line| format!("{line}\n"))
        .collect();
    let golden = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../ops/fixtures/transparent-shard/Caddyfile.router");
    if std::env::var_os("UPDATE_OPS_FIXTURES").is_some() {
        std::fs::write(&golden, &stable).unwrap();
    }
    let expected = std::fs::read_to_string(&golden).unwrap_or_else(|error| {
        panic!(
            "{}: {error}; run with UPDATE_OPS_FIXTURES=1",
            golden.display()
        )
    });
    assert_eq!(
        stable,
        expected,
        "{} is stale; run with UPDATE_OPS_FIXTURES=1",
        golden.display()
    );
    // Over-budget rosters are refused rather than planned.
    let mut small = roster();
    for entry in &mut small {
        entry.cache_bytes = 64 << 20;
    }
    assert!(plan(&map, &map_sha256(&map), &small, ARCHIVE, 0.1, generated()).is_err());
}

#[test]
fn retention_by_bytes_keeps_newest_first_and_reports_the_rest_prunable() {
    // One shard, republished three times as a growing tail.
    let dir = tempfile::tempdir().unwrap();
    let mut entries = Vec::new();
    let mut digests = Vec::new();
    for revision in 0..4u32 {
        let end = FIRST + 10 + revision as u64 * 5;
        let built = build_shard(
            0,
            FIRST,
            end,
            genesis(),
            hash_at(end),
            transparent_filter::RANGE_PROFILE,
            &RECENT_4K,
            &events(FIRST, end),
        )
        .unwrap();
        let mut manifest = manifest_for(&built, end, revision);
        manifest.supersedes = digests.last().cloned().unwrap_or_default();
        let digest = manifest.digest();
        let shard_dir = dir.path().join(&digest);
        std::fs::create_dir_all(&shard_dir).unwrap();
        std::fs::write(shard_dir.join("manifest.json"), manifest.canonical_bytes()).unwrap();
        std::fs::write(shard_dir.join("filter.bin"), built.filter.as_slice()).unwrap();
        std::fs::write(shard_dir.join("directory.0.bin"), &built.directory[0]).unwrap();
        std::fs::write(shard_dir.join("pages.0.bin"), &built.pages[0]).unwrap();
        digests.push(digest.clone());
        entries = vec![ShardMapEntry {
            shard_id: 0,
            geometry: RECENT_4K.name.to_string(),
            start_height: FIRST,
            end_height: end,
            parent_block_hash: manifest.parent_block_hash.clone(),
            terminal_block_hash: manifest.terminal_block_hash.clone(),
            filter_hash: manifest.filter_hash.clone(),
            scripts: built.scripts,
            page_rows: built.page_rows,
            txids: 0,
            directory_segments: 1,
            page_segments: 1,
            manifest_digest: digest,
            revision,
            sealed: false,
        }];
    }
    let map = ShardMap {
        genesis_hash: GENESIS.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: FIRST,
        seal: BTreeMap::from([(
            RECENT_4K.name.to_string(),
            SealParameters {
                max_scripts: 8_192,
                max_page_rows: 2_048,
                max_txids: 0,
            },
        )]),
        shards: entries,
    };
    std::fs::write(
        dir.path().join("shards.json"),
        serde_json::to_vec_pretty(&map).unwrap(),
    )
    .unwrap();

    // Three superseded revisions on disk. A byte bound that fits one keeps the
    // newest and reports the two older ones prunable.
    let one_revision = 2 * (4096 * 4096) + 1024;
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

fn manifest_for(
    built: &transparent_shard::build::BuiltShard,
    end: u64,
    revision: u32,
) -> ShardManifest {
    ShardManifest {
        schema: SCHEMA.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        geometry: RECENT_4K.name.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        genesis_hash: GENESIS.to_string(),
        shard_id: 0,
        start_height: FIRST,
        end_height: end,
        parent_block_hash: hash_at(FIRST - 1).to_display_hex(),
        terminal_block_hash: hash_at(end).to_display_hex(),
        tag_salt_counter: built.tag_salt_counter,
        parent_manifest_digest: String::new(),
        sealed: false,
        revision,
        supersedes: String::new(),
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
        directory_segments: vec![TableGeometry {
            rows: RECENT_4K.directory_rows,
            row_bytes: RECENT_4K.directory_row_bytes as u32,
            sha256: hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&built.directory[0])),
        }],
        page_segments: vec![TableGeometry {
            rows: RECENT_4K.page_rows,
            row_bytes: RECENT_4K.page_row_bytes as u32,
            sha256: hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&built.pages[0])),
        }],
        occupancy: ManifestOccupancy {
            scripts: built.scripts,
            page_rows: built.page_rows,
            fragments: built.fragments,
            events: built.events,
            blocks: end - FIRST + 1,
            txids: 0,
            excluded_scripts: built.excluded_scripts,
        },
        directory_choice: None,
    }
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
    use transparent_shard_server::live::{Command, LiveService, Publication, ASSIGNMENT_CHANGED};
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
