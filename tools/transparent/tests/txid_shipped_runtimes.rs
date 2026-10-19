//! Runtimes shipped beside a display publication, end to end on a worker.
//!
//! The publisher writes the recent revision's runtimes as plain files at the
//! candidate root. The candidate must still open and verify as before, a
//! recent replica must load and self-check those runtimes instead of building
//! them, publish exactly what a local build publishes, and fall back to a
//! counted build for a file it cannot use; collection removes the candidate
//! with its files.

use zakura_pir_publisher::transparent as transparent_shard;

use std::path::{Path, PathBuf};
use transparent_publisher::tx_detail::publisher::verify_dir;
use transparent_query_server::config::ServiceConfig;
use transparent_query_server::tx_detail::live::{
    DisplayCommand, DisplayLive, DisplayPublication, Reply,
};
use transparent_query_server::tx_detail::service::DisplayRuntime;
use transparent_shard::display::{DisplaySealParams, TXID_2K8};
use transparent_shared::runtime::disk::ShippedRuntimes;
use transparent_shared::runtime::{SharedParams, TableRuntime};
use transparent_shared::tx_detail::prebuild::build_shipped;
use transparent_shared::tx_detail::set::{DisplaySet, MAP_FILE};
use transparent_shared::tx_detail::synth::{self, Published, ShardSpec};
use transparent_shared::tx_detail::DisplayRole;
use transparent_shared::tx_detail::{kind, runtime_key};

fn spec(shard_id: u64, start: u64, end: u64, sealed: bool, n_buckets: u32) -> ShardSpec {
    ShardSpec {
        shard_id,
        start_height: start,
        end_height: end,
        sealed,
        revision: 0,
        supersedes: String::new(),
        parent_manifest_digest: String::new(),
        n_buckets,
        archive_target: 1,
        geometry: &TXID_2K8,
    }
}

fn params() -> DisplaySealParams {
    DisplaySealParams {
        n_archive: 1,
        n_recent: 2,
        archive_target: 1,
        recent_window: 1,
        reorg_margin: 1,
    }
}

/// One archive and one two-bucket recent shard in the store under `root`.
fn shards(root: &Path) -> Vec<Published> {
    let records = synth::records(60, 11);
    let archive = synth::write_shard(root, &spec(0, 10, 19, true, 1), &records[..20]).unwrap();
    let mut recent = spec(1, 20, 29, false, 2);
    recent.parent_manifest_digest = archive.digest.clone();
    let recent = synth::write_shard(root, &recent, &records[20..]).unwrap();
    vec![archive, recent]
}

fn runtime_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_file() && path.extension().is_some_and(|e| e == "runtime"))
        .collect();
    files.sort();
    files
}

fn worker(root: &Path) -> DisplayLive {
    DisplayLive::new(
        DisplayRuntime::new(ServiceConfig::default(), None),
        DisplayRole::RecentReplica,
        3,
        root.join("worker/active.json"),
        vec![root.to_path_buf()],
        None,
    )
    .unwrap()
}

async fn metrics(live: &DisplayLive) -> String {
    use tower::ServiceExt;
    let request = axum::http::Request::get("/metrics")
        .body(axum::body::Body::empty())
        .unwrap();
    let response = live.router().oneshot(request).await.unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(body.to_vec()).unwrap()
}

async fn prepare(live: &DisplayLive, directory: &Path, map_sha256: &str) -> Reply {
    let publication = DisplayPublication {
        directory: directory.to_path_buf(),
        map_sha256: map_sha256.to_string(),
    };
    live.command(DisplayCommand::Prepare {
        expected: live.expected(),
        publication,
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recent_replica_serves_shipped_runtimes_and_falls_back_per_file() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let shards = shards(root);
    let recent = &shards[1];
    let (dir, sha) = synth::write_candidate(root, &params(), &shards, "shipped").unwrap();
    let files = build_shipped(&dir.join(&recent.digest), &dir).unwrap();
    let set = DisplaySet::open(&dir, 3, DisplayRole::RecentReplica).unwrap();
    let targets = set.held(&recent.digest).unwrap().targets();
    // The two buckets' tables, each one segment.
    assert_eq!(targets.len(), 2);
    assert_eq!(files.len(), targets.len());
    assert_eq!(runtime_files(&dir).len(), targets.len());
    assert!(set.shipped.is_some());
    // Root-level files leave the candidate readable and every revision
    // directory exactly its manifest's files.
    for shard in &shards {
        verify_dir(&dir.join(&shard.digest), &shard.digest).unwrap();
    }
    let archive_owner = DisplaySet::open(&dir, 3, DisplayRole::ArchiveOwner).unwrap();
    assert!(archive_owner.held(&shards[0].digest).is_some());

    let live = worker(root);
    let reply = prepare(&live, &dir, &sha).await;
    assert_eq!(reply["built"], targets.len() as u64, "{reply:?}");
    assert_eq!(reply["shipped"], targets.len() as u64, "{reply:?}");
    assert_eq!(reply["shipped_fallbacks"], 0, "{reply:?}");
    assert!(reply["self_check_ms"].as_f64().unwrap() > 0.0);
    // Before any activation the worker's own `/metrics` counts the prepare
    // and its self-check.
    let text = metrics(&live).await;
    for family in ["prepare", "self_check"] {
        let count =
            format!("transparent_display_{family}_seconds_count{{role=\"recent-replica\"}} 1\n");
        assert!(text.contains(&count), "{family}:\n{text}");
    }

    // Each file is the runtime a local build of its segment makes, named by
    // the identity the worker looks it up by.
    let revision = set.held(&recent.digest).unwrap();
    let view = ShippedRuntimes::new(dir.clone());
    for (table, segment) in &targets {
        let shared = SharedParams::build_display(&TXID_2K8, kind(*table)).unwrap();
        let source = revision.segment(*table, *segment).unwrap();
        let rows = source.load().unwrap();
        let local = TableRuntime::build(&shared, &rows).unwrap();
        let key = runtime_key(&recent.digest, *table, *segment);
        let shipped = view.load(&key, &shared, source.sha256()).unwrap();
        assert_eq!(shipped.public_params, local.public_params);
        assert_eq!(shipped.public_params_epoch, local.public_params_epoch);
        shipped.self_check(&shared, &rows).unwrap();
    }
    // An unused candidate carrying the same files, for collection below.
    let (old, _) = synth::write_candidate(root, &params(), &shards, "old").unwrap();
    for file in runtime_files(&dir) {
        std::fs::hard_link(&file, old.join(file.file_name().unwrap())).unwrap();
    }
    live.command(DisplayCommand::Activate {
        expected: String::new(),
        map_sha256: sha.clone(),
    })
    .await
    .unwrap();

    // A candidate missing one file: that bucket's table is built here and
    // counted.
    let (partial, partial_sha) = {
        let mut next = spec(1, 20, 30, false, 2);
        next.parent_manifest_digest = shards[0].digest.clone();
        next.revision = 1;
        next.supersedes = recent.digest.clone();
        let records = synth::records(60, 11);
        let next = synth::write_shard(root, &next, &records[20..45]).unwrap();
        let (dir, sha) = synth::write_candidate(
            root,
            &params(),
            &[shards[0].clone(), next.clone()],
            "partial",
        )
        .unwrap();
        build_shipped(&dir.join(&next.digest), &dir).unwrap();
        std::fs::remove_file(&runtime_files(&dir)[0]).unwrap();
        (dir, sha)
    };
    let reply = prepare(&live, &partial, &partial_sha).await;
    assert_eq!(reply["shipped"], 1, "{reply:?}");
    assert_eq!(reply["shipped_fallbacks"], 1, "{reply:?}");
    live.command(DisplayCommand::Activate {
        expected: sha.clone(),
        map_sha256: partial_sha.clone(),
    })
    .await
    .unwrap();

    // A candidate without shipped files is not a shipped one: no fallbacks.
    let fresh = worker(&temp.path().join("elsewhere"));
    let (plain, plain_sha) = synth::write_candidate(root, &params(), &shards, "plain").unwrap();
    assert!(runtime_files(&plain).is_empty());
    let reply = prepare(&fresh, &plain, &plain_sha).await;
    assert_eq!(reply["built"], targets.len() as u64);
    assert_eq!(reply["shipped"], 0);
    assert_eq!(reply["shipped_fallbacks"], 0);

    // Collection removes an unused candidate whole, shipped files included:
    // the newest three unused candidates stay, older ones go. The first
    // candidate is still held by the retired snapshot.
    for label in ["c1", "c2", "c3"] {
        std::thread::sleep(std::time::Duration::from_millis(20));
        synth::write_candidate(root, &params(), &shards, label).unwrap();
    }
    let collected = live.command(DisplayCommand::Collect).await.unwrap();
    let removed: Vec<String> = collected["removed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p.as_str().unwrap().to_string())
        .collect();
    assert!(removed.contains(&old.display().to_string()), "{removed:?}");
    assert!(!old.exists());
    assert_eq!(runtime_files(&dir).len(), targets.len());
    assert!(
        partial.join(MAP_FILE).is_file(),
        "the active candidate stays"
    );
    assert_eq!(runtime_files(&partial).len(), 1);
}
