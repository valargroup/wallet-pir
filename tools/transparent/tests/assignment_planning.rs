//! Fleet planning, per-worker file lists and the edge router, over a
//! published set.
//!
//! Six shards split across two archive owners and two recent replicas by the
//! deployment planner; each worker copies only its listed files and loads
//! under the set's global identity. The rendered router is pinned against a
//! golden file. Moved here from the query server, which never plans a fleet.

use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_protocol::transparent::filter as transparent_filter;
use zakura_pir_publisher::transparent as transparent_shard;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use transparent_events::{ReceiveEvent, TransparentEvent, Txid};
use transparent_filter::{ScriptBytes, SealParameters, ShardMap, ShardMapEntry};
#[allow(dead_code)]
#[path = "../../../src/pir-query-server/tests/transparent/support/history_set.rs"]
mod history_set;
#[allow(dead_code)]
#[path = "../../../src/pir-query-server/tests/transparent/support/revision.rs"]
mod revision;
use history_set::*;
use pir_query_server::transparent::history::service::{ServiceConfig, ServiceState};
use transparent_shard::layout::{Geometry, DECOUPLED_2K_8K};
use transparent_shared::history::assignment::{Assignment, WorkerAssignment, WorkerRole};
use transparent_shared::history::shardset::{LoadOptions, LoadScope, ShardSet};
use transparent_tools::assignment::{
    files_for, files_with_runtimes, plan, render_caddyfile, RosterEntry,
};

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
    plan(map, &map_sha256(map), &roster(), ARCHIVE, 0.1, generated()).unwrap()
}

/// The four-worker roster the planner splits: two archive owners and two
/// recent replicas.
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
            archive_range: None,
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
            archive_range: None,
        });
    }
    roster
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

fn warm_config() -> ServiceConfig {
    ServiceConfig {
        cache_bytes: 4 << 30,
        ..ServiceConfig::default()
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
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
    let golden =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/transparent-shard/Caddyfile.router");
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

#[tokio::test(flavor = "multi_thread")]
async fn each_worker_receives_only_the_prepared_runtimes_of_its_shards() {
    use std::sync::atomic::Ordering;
    use transparent_shared::runtime::disk::ShippedRuntimes;
    use transparent_shared::runtime::prebuild::prepare_publication;
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    let assignment = planned(&map);
    let set = ShardSet::open(dir.path(), 3).unwrap();
    let prepared = prepare_publication(&set, dir.path(), &[], &|| false).unwrap();
    assert!(prepared.failures.is_empty());
    let runtimes = |files: &[String]| -> std::collections::BTreeSet<String> {
        files
            .iter()
            .filter(|f| f.ends_with(".runtime"))
            .cloned()
            .collect()
    };
    // Without the flag nothing changes.
    let plain = files_for(dir.path(), &map, &assignment, "recent-01").unwrap();
    assert!(runtimes(&plain).is_empty());
    assert_eq!(
        plain,
        files_with_runtimes(dir.path(), &map, &assignment, "recent-01", false).unwrap()
    );
    let mut every = std::collections::BTreeSet::new();
    for worker in ["archive-01", "archive-02", "recent-01", "recent-02"] {
        let files = files_with_runtimes(dir.path(), &map, &assignment, worker, true).unwrap();
        let mine = runtimes(&files);
        let scope = assignment.scope_for(worker).unwrap();
        // Exactly the files of this worker's shards' runtimes.
        let expected: std::collections::BTreeSet<String> = set
            .revisions()
            .iter()
            .filter(|shard| scope.assigned.contains(&shard.manifest.shard_id))
            .flat_map(|shard| shard.runtime_identities())
            .flat_map(|identity| {
                let prefix =
                    hex::encode(<sha2::Sha256 as sha2::Digest>::digest(identity.as_bytes()));
                prepared
                    .files
                    .iter()
                    .filter(move |file| file.name.starts_with(&format!("{prefix}-")))
                    .map(|file| file.name.clone())
            })
            .collect();
        assert_eq!(mine, expected, "{worker}");
        assert!(!mine.is_empty(), "{worker}");
        every.extend(mine.iter().cloned());
        // Staged from its inventory, it loads every assigned runtime from them.
        let staged = tempfile::tempdir().unwrap();
        for file in &files {
            let from = dir.path().join(file.trim_end_matches('/'));
            let to = staged.path().join(file.trim_end_matches('/'));
            if from.is_dir() {
                copy_dir(&from, &to);
            } else {
                std::fs::create_dir_all(to.parent().unwrap()).unwrap();
                std::fs::copy(&from, &to).unwrap();
            }
        }
        let loaded = load_as(staged.path(), &assignment, worker, LoadOptions::whole(3));
        let state = ServiceState::build_with_runtimes(
            loaded,
            warm_config(),
            None,
            Some(ShippedRuntimes::new(staged.path().to_path_buf())),
        )
        .unwrap();
        state.spawn_prewarm().await.unwrap();
        let metrics = state.metrics();
        assert_eq!(metrics.builds.load(Ordering::Relaxed), 0, "{worker}");
        assert_eq!(
            metrics.shipped_fallbacks.load(Ordering::Relaxed),
            0,
            "{worker}"
        );
        assert!(
            metrics.shipped_loads.load(Ordering::Relaxed) > 0,
            "{worker}"
        );
    }
    assert_eq!(
        every.len(),
        prepared.files.len(),
        "every runtime has an owner"
    );
}
