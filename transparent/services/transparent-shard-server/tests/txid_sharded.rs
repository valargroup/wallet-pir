//! The tiered, bucketed txid display publication over real HTTP: an
//! archive-owner and a recent-replica worker behind an edge that routes
//! `/v1/txid/archive/` to the owner, as the deployment's static route does,
//! and the reference client.
#[path = "../examples/support/bytemeter.rs"]
mod bytemeter;
#[path = "../examples/support/txdisplay.rs"]
mod txdisplay;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Router;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tower::ServiceExt;
use transparent_events::Txid;
use transparent_shard::display::{self, DisplaySealParams, DisplayTable, TXID_2K};
use transparent_shard::txid::TransparentDisplayRecord;
use transparent_shard_server::assignment::WorkerRole;
use transparent_shard_server::display::live::{DisplayCommand, DisplayLive, DisplayPublication};
use transparent_shard_server::display::service::{router, DisplayRuntime, DisplayState};
use transparent_shard_server::display::set::DisplaySet;
use transparent_shard_server::display::synth::{self, Published, ShardSpec};
use transparent_shard_server::service::{ReadinessMode, ServiceConfig};
use txdisplay::{DisplayClient, LookupResult};

#[path = "support/display_world.rs"]
mod display_world;
use display_world::*;

/// Every lookup sends exactly two directory queries, then exactly one query
/// per page of the record, each a fixed 40,200-byte upload.
fn assert_transcript(report: &txdisplay::LookupReport, pages: usize) {
    let directory: Vec<_> = report
        .queries
        .iter()
        .filter(|q| q.table.starts_with("directory-"))
        .collect();
    let page_queries = report.queries.iter().filter(|q| q.table == "pages").count();
    assert_eq!(directory.len(), 2, "{report:?}");
    assert_eq!(page_queries, pages, "{report:?}");
    assert!(report.queries[..2]
        .iter()
        .all(|q| q.table.starts_with("directory-")));
    for query in &report.queries {
        assert_eq!(query.up_body, QUERY_BYTES);
        assert_eq!(query.status, 200);
    }
}

async fn found(
    client: &DisplayClient,
    record: &TransparentDisplayRecord,
    height: u64,
) -> txdisplay::LookupReport {
    let report = client.lookup(record.txid, Some(height)).await.unwrap();
    match &report.result {
        LookupResult::Found(actual) => assert_eq!(actual, record),
        other => panic!("{other:?} for a published record"),
    }
    report
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lookups_follow_the_fixed_transcript_in_both_tiers() {
    let world = World::start().await;
    let client = world.client();
    for (records, height, sealed) in [
        (&world.a0_records, 150, true),
        (&world.r0_records, 230, false),
    ] {
        for (record, pages) in records[40..44].iter().zip([0usize, 1, 2, 5]) {
            let report = found(&client, record, height).await;
            assert_eq!(report.sealed, Some(sealed));
            assert_eq!(report.bucket, Some(0));
            assert_eq!(
                report.class,
                if pages == 0 {
                    "inline".to_string()
                } else {
                    format!("pages-{pages}")
                }
            );
            assert_transcript(&report, pages);
        }
        let absent = client.lookup(Txid([0xee; 32]), Some(height)).await.unwrap();
        assert_eq!(absent.result, LookupResult::Absent);
        assert_transcript(&absent, 0);
    }

    // Coinciding candidate rows still cost two queries, and the duplicate
    // decoded row is dropped before the entry is found.
    let coincident = world.r0_records.last().unwrap();
    let [a, b] = display::candidate_rows(&coincident.txid, 1, 0, 2_048);
    assert_eq!(a, b);
    let report = found(&client, coincident, 255).await;
    assert_transcript(&report, 0);
    assert!(report.queries.iter().all(|q| q.row == a));

    // Placement comes from the caller; nothing is asked to discover it.
    for height in [None, Some(99), Some(261)] {
        let report = client.lookup(coincident.txid, height).await.unwrap();
        assert_eq!(report.result, LookupResult::PlacementUnknown);
        assert!(report.queries.is_empty());
    }

    // A server publishing a codec this client does not know is unsupported.
    world.faults.foreign_codec.store(true, Ordering::Release);
    let report = world
        .client()
        .lookup(coincident.txid, Some(255))
        .await
        .unwrap();
    assert_eq!(report.result, LookupResult::Unsupported);
    world.faults.foreign_codec.store(false, Ordering::Release);

    // Errors are errors, never absence: a closed port, and a 503.
    let closed = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };
    let unreachable = DisplayClient::new(&format!("http://{closed}"), None, None).unwrap();
    assert!(matches!(
        unreachable.lookup(coincident.txid, Some(255)).await,
        Err(txdisplay::LookupError::Transport(_))
    ));
    world.faults.queries.store(1, Ordering::Release);
    let error = client
        .lookup(Txid([0xee; 32]), Some(150))
        .await
        .unwrap_err();
    assert_eq!(error.status(), Some(503));

    // An interrupted overflow fails the lookup; the retry finds the record.
    let paged = &world.a0_records[43];
    world.faults.pages.store(1, Ordering::Release);
    let error = client.lookup(paged.txid, Some(150)).await.unwrap_err();
    assert_eq!(error.status(), Some(503));
    found(&client, paged, 150).await;

    // The computed bytes are the bytes on the wire, cold and warm.
    let upstream: std::net::SocketAddr = world.url.trim_start_matches("http://").parse().unwrap();
    let meter = bytemeter::ByteMeter::start(upstream).await.unwrap();
    let metered = DisplayClient::new(
        &format!("http://{}", meter.addr),
        None,
        Some(client.profiles()),
    )
    .unwrap();
    for (record, height) in [
        (&world.a0_records[40], 150),
        (&world.a0_records[40], 150),
        (&world.a0_records[43], 150),
        (&world.r0_records[41], 230),
    ] {
        let before = meter.snapshot();
        let report = found(&metered, record, height).await;
        let after = meter.snapshot();
        assert_eq!(after.0 - before.0, report.bytes.up(), "{report:?}");
        assert_eq!(after.1 - before.1, report.bytes.down(), "{report:?}");
    }
    // A warm inline lookup moves only its two directory queries.
    let warm = found(&metered, &world.a0_records[40], 150).await;
    assert_eq!(warm.bytes.query_up + warm.bytes.query_down, 91_696);
    assert_eq!(warm.bytes.setup + warm.bytes.manifest + warm.bytes.map, 0);
}

/// The recent shard's oldest range seals as shard 1 and a new recent shard 2
/// follows: `a1` and `r1`, with `p1` publishing them beside `a0` and `p2`
/// dropping `a0` from the window. Shards and candidates are written up front,
/// because the owner collects `<root>/sealed` as it would its staged root.
struct Seal {
    a1: Published,
    r1: Published,
    r1_records: Vec<TransparentDisplayRecord>,
    p1: (PathBuf, String),
    p2: (PathBuf, String),
}

impl Seal {
    fn write(world: &World) -> Self {
        let root = world.root.path();
        let a1 = synth::write_shard(
            root,
            &spec(1, 200, 240, true, &world.a0.digest),
            &world.r0_records,
        )
        .unwrap();
        let r1_records = records(30, &[]);
        let r1 =
            synth::write_shard(root, &spec(2, 241, 300, false, &a1.digest), &r1_records).unwrap();
        let p1 = synth::write_candidate(
            root,
            &params(1),
            &[world.a0.clone(), a1.clone(), r1.clone()],
            "p1",
        )
        .unwrap();
        let p2 = synth::write_candidate(root, &params(1), &[a1.clone(), r1.clone()], "p2").unwrap();
        Self {
            a1,
            r1,
            r1_records,
            p1,
            p2,
        }
    }
}

async fn collect(worker: &Worker) -> serde_json::Value {
    worker.command(DisplayCommand::Collect).await.unwrap()["removed"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_is_staged_then_published_without_rebuilding_archives() {
    let world = World::start().await;
    // A stale client holds the first map, manifests and recent setup.
    let stale = world.client();
    let moved = &world.r0_records[40];
    found(&stale, moved, 210).await;

    let Seal {
        a1,
        r1_records,
        p1,
        p2,
        ..
    } = Seal::write(&world);
    // Shipped but not yet staged: collection keeps a seal no map has
    // published, and removes the copy of shard 0, which p0 publishes.
    assert_eq!(
        collect(&world.archive).await,
        serde_json::json!([world.a0.dir.display().to_string()])
    );
    assert!(a1.dir.exists());
    let staged = world
        .archive
        .command(DisplayCommand::Stage {
            directory: a1.dir.clone(),
        })
        .await
        .unwrap();
    assert_eq!(staged["built"], 2);
    assert_eq!(staged["digest"], a1.digest);
    // A replica holds no sealed shard and refuses to stage one.
    assert!(world
        .recent
        .command(DisplayCommand::Stage {
            directory: a1.dir.clone(),
        })
        .await
        .is_err());

    // The owner takes the staged archive over by inode: nothing is built.
    let before = world.archive.builds();
    let prepared = world.archive.publish(&p1.0, &p1.1).await;
    assert_eq!(prepared["built"], 0, "{prepared}");
    assert_eq!(prepared["reused"], 4);
    assert_eq!(world.archive.builds(), before);
    let status = world.archive.command(DisplayCommand::Status).await.unwrap();
    assert_eq!(status["staged"], serde_json::json!([]));
    assert_eq!(status["active"]["map_sha256"], p1.1);
    // Published and serving from p1's links, the staged copy is collected.
    assert_eq!(
        collect(&world.archive).await,
        serde_json::json!([a1.dir.display().to_string()])
    );
    // The replica builds only the new recent shard's two tables.
    let prepared = world.recent.publish(&p1.0, &p1.1).await;
    assert_eq!(prepared["built"], 2, "{prepared}");

    // The stale client is still answered from the retained recent revision.
    let report = found(&stale, moved, 210).await;
    assert_eq!(report.digest.as_deref(), Some(world.r0.digest.as_str()));
    assert_eq!(report.stale_retries, 0);
    // A fresh client reaches the same record in the new archive.
    let fresh = world.client();
    let report = found(&fresh, moved, 210).await;
    assert_eq!(report.digest.as_deref(), Some(a1.digest.as_str()));
    assert_eq!(report.sealed, Some(true));
    found(&fresh, &r1_records[42], 290).await;
    // The archive that was already sealed did not change.
    let (_, map) = world.get("/v1/txid/shards").await;
    assert_eq!(map["shards"][0]["manifest_digest"], world.a0.digest);

    // The window drops the oldest archive: shard ids are not renumbered and
    // the remaining digests are unchanged.
    assert_eq!(world.archive.publish(&p2.0, &p2.1).await["built"], 0);
    assert_eq!(world.recent.publish(&p2.0, &p2.1).await["built"], 0);
    let (_, map) = world.get("/v1/txid/shards").await;
    assert_eq!(map["first_shard_id"], 1);
    assert_eq!(map["shards"][0]["manifest_digest"], a1.digest);
    let fresh = world.client();
    let old = &world.a0_records[40];
    assert_eq!(
        fresh.lookup(old.txid, Some(150)).await.unwrap().result,
        LookupResult::PlacementUnknown
    );
    // A client that still holds the old map is answered from the retired
    // snapshot that holds the dropped archive, while it is resident.
    found(&stale, old, 150).await;
    // p2's prepare evicted the retired recent revision. A stale client is
    // sent to refresh its map rather than costing the replica a cold build.
    let builds = world.recent.builds();
    let report = found(&stale, moved, 210).await;
    assert_eq!(report.stale_retries, 1);
    assert_eq!(report.digest.as_deref(), Some(a1.digest.as_str()));
    assert_eq!(world.recent.builds(), builds);
    for worker in [&world.archive, &world.recent] {
        assert_eq!(collect(worker).await, serde_json::json!([]));
    }
}

/// Every display table source follows a revision into the candidate that
/// takes it over: once the directories it was first verified in are gone,
/// including the staged copy collection removes, cold builds read the
/// active candidate's links. Loaded-only workers pin nothing, so every
/// runtime can be evicted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relinked_revisions_rebuild_after_their_first_directories_are_removed() {
    let world = World::start_with(loaded_only()).await;
    let root = world.root.path();
    let Seal {
        a1,
        r1,
        r1_records,
        p1,
        p2,
    } = Seal::write(&world);
    world
        .archive
        .command(DisplayCommand::Stage {
            directory: a1.dir.clone(),
        })
        .await
        .unwrap();
    for (dir, map) in [&p1, &p2] {
        world.archive.publish(dir, map).await;
        world.recent.publish(dir, map).await;
    }
    collect(&world.archive).await;
    assert!(!a1.dir.exists(), "the staged copy is collected");
    for dir in [
        &world.p0.0,
        &p1.0,
        &root.join("sealed"),
        &root.join("recent"),
    ] {
        std::fs::remove_dir_all(dir).unwrap();
    }
    for worker in [&world.archive, &world.recent] {
        worker.runtime.cache.evict_unpinned();
        assert_eq!(worker.runtime.cache.entries(), 0);
    }

    for (worker, tier, revision) in [
        (&world.archive, "archive", &a1),
        (&world.recent, "recent", &r1),
    ] {
        let builds = worker.builds();
        let shard_id = revision.manifest.shard_id;
        for table in revision.manifest.tables() {
            let segments = revision.manifest.segments(table).unwrap().len();
            for segment in 0..segments {
                let path = format!(
                    "/v1/txid/{tier}/shards/{shard_id}/revisions/{}/setup/{}/{segment}",
                    revision.digest,
                    table.label()
                );
                let (status, body) = world.get(&path).await;
                assert_eq!(status, 200, "{path}: {body}");
            }
        }
        assert!(worker.builds() > builds, "{tier} setup was built cold");
    }
    let client = world.client();
    for record in &world.r0_records[40..44] {
        found(&client, record, 210).await;
    }
    for record in &r1_records[40..44] {
        found(&client, record, 290).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reorg_invalidates_only_the_recent_shard() {
    let world = World::start().await;
    let expected = world.p0.1.clone();
    let before = world.recent.command(DisplayCommand::Status).await.unwrap();
    let refused = world
        .recent
        .command(DisplayCommand::Invalidate {
            expected: expected.clone(),
            from_height: 150,
        })
        .await
        .unwrap_err();
    assert_eq!(refused, "reorg reaches sealed display shard 0");
    assert_eq!(
        world.recent.command(DisplayCommand::Status).await.unwrap(),
        before,
        "a refused invalidation changes nothing"
    );

    let client = world.client();
    found(&client, &world.r0_records[40], 230).await;
    world
        .recent
        .command(DisplayCommand::Invalidate {
            expected,
            from_height: 250,
        })
        .await
        .unwrap();
    // The map and readiness keep serving; the recent revision does not.
    assert_eq!(world.get("/v1/ready").await.0, 200);
    assert_eq!(world.get("/v1/txid/shards").await.0, 200);
    let error = client
        .lookup(world.r0_records[40].txid, Some(230))
        .await
        .unwrap_err();
    assert_eq!(error.status(), Some(409));
    found(&client, &world.a0_records[41], 150).await;

    // The next publication replaces the orphaned recent shard.
    let r0b = synth::write_shard(
        world.root.path(),
        &spec(1, 200, 249, false, &world.a0.digest),
        &world.r0_records[..44],
    )
    .unwrap();
    let p1 = synth::write_candidate(
        world.root.path(),
        &params(1),
        &[world.a0.clone(), r0b],
        "after-reorg",
    )
    .unwrap();
    world.recent.publish(&p1.0, &p1.1).await;
    found(&world.client(), &world.r0_records[40], 230).await;
}

/// Status, headers and body of one GET through the edge.
async fn raw(world: &World, path: &str) -> (u16, reqwest::header::HeaderMap, Vec<u8>) {
    let response = reqwest::get(format!("{}{path}", world.url)).await.unwrap();
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    (status, headers, response.bytes().await.unwrap().to_vec())
}

fn header<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> &'a str {
    headers.get(name).unwrap().to_str().unwrap()
}

/// The recent map and index chunks are served beside the full map, which is
/// unchanged; a seal moves the recent map and the newest chunk, and a client
/// still holding the old recent map gets its chunk from the retired snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_split_map_is_served_beside_the_unchanged_full_map() {
    use transparent_shard::display::split::{index_file, RECENT_MAP_FILE};
    use transparent_shard::display::{DisplayIndexChunk, DisplayRecentMap};
    let world = World::start().await;
    let file = |dir: &Path, name: &str| std::fs::read(dir.join(name)).unwrap();

    let (status, headers, full) = raw(&world, "/v1/txid/shards").await;
    assert_eq!(status, 200);
    assert_eq!(full, file(&world.p0.0, "txid-shards.json"));
    assert_eq!(header(&headers, "x-txid-map-sha256"), world.p0.1);

    let (status, headers, recent) = raw(&world, "/v1/txid/map").await;
    assert_eq!(status, 200);
    assert_eq!(recent, file(&world.p0.0, RECENT_MAP_FILE));
    assert_eq!(
        header(&headers, "x-txid-map-sha256"),
        hex::encode(Sha256::digest(&recent))
    );
    assert_eq!(header(&headers, "cache-control"), "no-cache");
    let map: DisplayRecentMap = serde_json::from_slice(&recent).unwrap();
    map.check_shape().unwrap();
    assert_eq!((map.archives, map.chunks.len()), (1, 1));
    assert_eq!(
        map.recent.as_ref().unwrap().manifest_digest,
        world.r0.digest
    );

    let digest = map.chunks[0].sha256.clone();
    let (status, headers, chunk) = raw(&world, &map.chunk_path(0)).await;
    assert_eq!(status, 200);
    assert_eq!(chunk, file(&world.p0.0, &index_file(&digest)));
    assert_eq!(hex::encode(Sha256::digest(&chunk)), digest);
    assert_eq!(
        header(&headers, "cache-control"),
        "public, max-age=31536000, immutable"
    );
    let parsed: DisplayIndexChunk = serde_json::from_slice(&chunk).unwrap();
    map.check_chunk(0, &parsed).unwrap();
    assert_eq!(parsed.shards[0].manifest_digest, world.a0.digest);
    // The right digest under another base is a bad request; an unknown
    // digest is stale.
    assert_eq!(
        raw(&world, &format!("/v1/txid/map/32/{digest}")).await.0,
        400
    );
    let unknown = "ab".repeat(32);
    assert_eq!(
        raw(&world, &format!("/v1/txid/map/0/{unknown}")).await.0,
        409
    );

    // A seal: shard 1 joins chunk 0, so the chunk and the recent map move.
    let Seal { a1, r1, p1, .. } = Seal::write(&world);
    world.archive.publish(&p1.0, &p1.1).await;
    world.recent.publish(&p1.0, &p1.1).await;
    let (_, _, full) = raw(&world, "/v1/txid/shards").await;
    assert_eq!(full, file(&p1.0, "txid-shards.json"));
    let (_, _, moved) = raw(&world, "/v1/txid/map").await;
    let moved: DisplayRecentMap = serde_json::from_slice(&moved).unwrap();
    assert_eq!((moved.archives, moved.chunks.len()), (2, 1));
    assert_ne!(moved.chunks[0].sha256, digest);
    assert_eq!(moved.recent.as_ref().unwrap().manifest_digest, r1.digest);
    let (status, _, sealed) = raw(&world, &moved.chunk_path(0)).await;
    assert_eq!(status, 200);
    let sealed: DisplayIndexChunk = serde_json::from_slice(&sealed).unwrap();
    moved.check_chunk(0, &sealed).unwrap();
    assert_eq!(sealed.shards[1].manifest_digest, a1.digest);
    // The chunk the old recent map names is still answered.
    let (status, _, old) = raw(&world, &map.chunk_path(0)).await;
    assert_eq!((status, old), (200, chunk));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn misaddressed_requests_are_refused() {
    let root = tempfile::tempdir().unwrap();
    let mut a = spec(0, 1, 10, true, "");
    a.n_buckets = 2;
    let records = synth::records(80, 4);
    let a0 = synth::write_shard(root.path(), &a, &records[..40]).unwrap();
    let mut r = spec(1, 11, 20, false, &a0.digest);
    r.n_buckets = 2;
    let r0 = synth::write_shard(root.path(), &r, &records[40..]).unwrap();
    let (dir, map_sha256) =
        synth::write_candidate(root.path(), &params(2), &[a0.clone(), r0.clone()], "n2").unwrap();
    let config = ServiceConfig {
        readiness: ReadinessMode::LoadedOnly,
        ..config()
    };
    let owner = router(
        DisplayState::build(
            DisplaySet::open(&dir, 3, WorkerRole::ArchiveOwner).unwrap(),
            &DisplayRuntime::new(config, None),
        )
        .unwrap(),
    );
    let replica = router(
        DisplayState::build(
            DisplaySet::open(&dir, 3, WorkerRole::RecentReplica).unwrap(),
            &DisplayRuntime::new(config, None),
        )
        .unwrap(),
    );
    async fn send(app: &Router, path: &str, body: Option<Vec<u8>>) -> (StatusCode, String) {
        let builder = axum::http::Request::builder().uri(path);
        let request = match body {
            Some(body) => builder
                .method("POST")
                .header("content-length", body.len().to_string())
                .body(Body::from(body)),
            None => builder.body(Body::empty()),
        }
        .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&body).into_owned())
    }
    let padded = |digest: &str, table: DisplayTable| {
        let mut body = display::query_binding(digest, table).to_vec();
        body.resize(QUERY_BYTES as usize, 0);
        body
    };
    let archive =
        |digest: &str, tail: &str| format!("/v1/txid/archive/shards/0/revisions/{digest}/{tail}");
    let a = &a0.digest;

    // A body bound to bucket 1 is refused by bucket 0's table.
    let (status, body) = send(
        &owner,
        &archive(a, "query/directory-0"),
        Some(padded(a, DisplayTable::Directory(1))),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, _) = send(
        &owner,
        &archive(a, "query/directory-0"),
        Some(padded(&r0.digest, DisplayTable::Directory(0))),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Buckets at or past the shard's count, and malformed labels.
    for label in ["directory-2", "directory-01", "txdirectory"] {
        let (status, _) = send(&owner, &archive(a, &format!("setup/{label}/0")), None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{label}");
        let (status, _) = send(
            &owner,
            &archive(a, &format!("query/{label}")),
            Some(padded(a, DisplayTable::Directory(0))),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{label}");
    }
    // The tier is the revision's own, and the role must hold it.
    let recent_path =
        |digest: &str| format!("/v1/txid/recent/shards/0/revisions/{digest}/setup/pages/0");
    assert_eq!(
        send(&owner, &recent_path(a), None).await.0,
        StatusCode::MISDIRECTED_REQUEST
    );
    assert_eq!(
        send(&replica, &archive(a, "setup/pages/0"), None).await.0,
        StatusCode::MISDIRECTED_REQUEST
    );
    let r_digest = &r0.digest;
    assert_eq!(
        send(
            &owner,
            &format!("/v1/txid/recent/shards/1/revisions/{r_digest}/setup/pages/0"),
            None
        )
        .await
        .0,
        StatusCode::MISDIRECTED_REQUEST
    );
    assert_eq!(
        send(
            &replica,
            &format!("/v1/txid/archive/shards/1/revisions/{r_digest}/setup/pages/0"),
            None
        )
        .await
        .0,
        StatusCode::MISDIRECTED_REQUEST
    );
    // A digest of another shard is a bad request; an unknown one is stale.
    assert_eq!(
        send(
            &owner,
            &format!("/v1/txid/archive/shards/3/revisions/{a}/setup/pages/0"),
            None
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    let unknown = "ab".repeat(32);
    let (status, body) = send(&owner, &archive(&unknown, "setup/directory-0/0"), None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["map_sha256"], map_sha256);
    let (status, _) = send(
        &replica,
        &format!("/v1/txid/shards/0/revisions/{unknown}/manifest"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    // Metadata is served for every revision by either role.
    for app in [&owner, &replica] {
        for (id, digest) in [(0, a), (1, r_digest)] {
            let (status, body) = send(
                app,
                &format!("/v1/txid/shards/{id}/revisions/{digest}/manifest"),
                None,
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(hex::encode(Sha256::digest(body.as_bytes())), *digest);
        }
        let (status, init) = send(app, "/v1/txid/init", None).await;
        assert_eq!(status, StatusCode::OK);
        let init: serde_json::Value = serde_json::from_str(&init).unwrap();
        assert_eq!(init["map_sha256"], map_sha256);
        assert_eq!(init["geometries"][0]["txdirectory"]["rows"], 2_048);
    }
}

/// Kills a spawned worker however the test ends.
struct Spawned(std::process::Child);

impl Drop for Spawned {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The worker binary starts with nothing to serve, takes a publication
/// through `txid-control`, and serves it again from its record on restart.
#[test]
fn the_worker_binary_takes_and_restores_a_publication_by_control() {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let root = tempfile::tempdir().unwrap();
    let a0 =
        synth::write_shard(root.path(), &spec(0, 100, 199, true, ""), &records(10, &[])).unwrap();
    let r0 = synth::write_shard(
        root.path(),
        &spec(1, 200, 260, false, &a0.digest),
        &records(20, &[]),
    )
    .unwrap();
    let (dir, map_sha256) =
        synth::write_candidate(root.path(), &params(1), &[a0, r0], "p0").unwrap();
    let socket = root.path().join("control.sock");
    let record = root.path().join("worker/active.json");
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let url = format!("http://127.0.0.1:{port}");
    let spawn = || {
        let _ = std::fs::remove_file(&socket);
        let child = Command::new(env!("CARGO_BIN_EXE_transparent-txid-server"))
            .args([
                "--role",
                "recent-replica",
                "--listen",
                &format!("127.0.0.1:{port}"),
            ])
            .arg("--control-socket")
            .arg(&socket)
            .arg("--active-record")
            .arg(&record)
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !(socket.exists() && reqwest::blocking::get(format!("{url}/v1/health")).is_ok()) {
            assert!(std::time::Instant::now() < deadline, "worker did not start");
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        Spawned(child)
    };
    let control = |command: serde_json::Value| {
        let mut child = Command::new(env!("CARGO_BIN_EXE_txid-control"))
            .arg(&socket)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(command.to_string().as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        let reply: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap_or_default();
        (output.status.success(), reply)
    };
    let ready = || reqwest::blocking::get(format!("{url}/v1/ready")).unwrap();

    let worker = spawn();
    let (ok, status) = control(serde_json::json!({"operation": "status"}));
    assert!(ok, "{status}");
    assert_eq!(status["active"], serde_json::Value::Null);
    assert_eq!(status["role"], "recent-replica");
    assert_eq!(status["warm"], false);
    assert_eq!(ready().status(), 503);
    let (ok, prepared) = control(serde_json::json!({
        "operation": "prepare",
        "expected": "",
        "publication": {"directory": dir, "map_sha256": map_sha256},
    }));
    assert!(ok, "{prepared}");
    assert_eq!(prepared["warm"], true);
    assert_eq!(prepared["built"], 2);
    let (ok, activated) = control(serde_json::json!({
        "operation": "activate", "expected": "", "map_sha256": map_sha256,
    }));
    assert!(ok, "{activated}");
    let body: serde_json::Value = ready().json().unwrap();
    assert_eq!(body["map_sha256"], map_sha256);
    assert_eq!(body["warm"], true);
    let (ok, refused) = control(serde_json::json!({
        "operation": "invalidate", "expected": map_sha256, "from_height": 150,
    }));
    assert!(!ok);
    assert_eq!(refused["error"], "reorg reaches sealed display shard 0");
    let (ok, _) = control(serde_json::json!({"operation": "stage", "directory": "relative"}));
    assert!(!ok);
    drop(worker);

    // A restart serves the recorded publication once its runtimes are warm.
    let _worker = spawn();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while ready().status() != 200 {
        assert!(
            std::time::Instant::now() < deadline,
            "restarted worker never warmed"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let (ok, status) = control(serde_json::json!({"operation": "status"}));
    assert!(ok);
    assert_eq!(status["active"]["map_sha256"], map_sha256);
}
