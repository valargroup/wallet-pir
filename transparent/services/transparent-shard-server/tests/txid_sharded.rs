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

const QUERY_BYTES: u64 = 40_200;

fn params(buckets: u32) -> DisplaySealParams {
    DisplaySealParams {
        n_archive: buckets,
        n_recent: buckets,
        archive_target: 1,
        recent_floor: 1,
        reorg_margin: 1,
    }
}

fn spec(shard_id: u64, start: u64, end: u64, sealed: bool, parent: &str) -> ShardSpec {
    ShardSpec {
        shard_id,
        start_height: start,
        end_height: end,
        sealed,
        revision: 0,
        supersedes: String::new(),
        parent_manifest_digest: parent.to_string(),
        n_buckets: 1,
        archive_target: 1,
        geometry: &TXID_2K,
    }
}

/// An inline record and records of one, two and five pages.
fn classes(seed: u64) -> Vec<TransparentDisplayRecord> {
    vec![
        synth::record(seed, 1, 25, 0),
        synth::record(seed, 2, 1_000, 0),
        synth::record(seed, 3, 5_000, 0),
        synth::record(seed, 4, 17_000, 0),
    ]
}

/// A record of `seed` whose two candidate rows coincide in `shard_id`.
fn coincident(seed: u64, shard_id: u64) -> TransparentDisplayRecord {
    let index = (1_000u64..)
        .find(|index| {
            let [a, b] = display::candidate_rows(&synth::txid(seed, *index), shard_id, 0, 2_048);
            a == b
        })
        .unwrap();
    synth::record(seed, index, 25, 0)
}

fn records(seed: u64, extra: &[TransparentDisplayRecord]) -> Vec<TransparentDisplayRecord> {
    let mut records = synth::records(40, seed + 1_000);
    records.extend(classes(seed));
    records.extend_from_slice(extra);
    records
}

fn config() -> ServiceConfig {
    ServiceConfig {
        cache_bytes: 2 << 30,
        readiness: ReadinessMode::Warm,
        ..ServiceConfig::default()
    }
}

/// Faults the edge injects in front of the workers.
#[derive(Clone, Default)]
struct Faults {
    /// Fail this many page queries with 503.
    pages: Arc<AtomicUsize>,
    /// Fail this many queries of any table with 503.
    queries: Arc<AtomicUsize>,
    /// Publish an init document naming a codec this client does not know.
    foreign_codec: Arc<AtomicBool>,
}

fn take(counter: &AtomicUsize) -> bool {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
        .is_ok()
}

#[derive(Clone)]
struct Edge {
    archive: Router,
    recent: Router,
    faults: Faults,
}

async fn edge(State(edge): State<Edge>, request: Request) -> Response {
    let path = request.uri().path().to_string();
    let fail = path.contains("/query/")
        && (take(&edge.faults.queries)
            || path.ends_with("/query/pages") && take(&edge.faults.pages));
    if fail {
        let _ = axum::body::to_bytes(request.into_body(), usize::MAX).await;
        return (StatusCode::SERVICE_UNAVAILABLE, "injected").into_response();
    }
    let target = if path.starts_with("/v1/txid/archive/") {
        edge.archive
    } else {
        edge.recent
    };
    let response = target.oneshot(request).await.unwrap();
    if path == "/v1/txid/init" && edge.faults.foreign_codec.load(Ordering::Acquire) {
        let (parts, body) = response.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let mut init: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        init["codec"] = "transparent-txid-display-v9".into();
        let mut parts = parts;
        parts.headers.remove("content-length");
        return Response::from_parts(parts, Body::from(init.to_string()));
    }
    response
}

struct Worker {
    live: DisplayLive,
    runtime: Arc<DisplayRuntime>,
}

impl Worker {
    fn new(root: &Path, role: WorkerRole) -> Self {
        let runtime = DisplayRuntime::new(config(), None);
        let live = DisplayLive::new(
            runtime.clone(),
            role,
            3,
            root.join(format!("{}.active.json", role.as_str())),
            None,
            None,
        )
        .unwrap();
        Self { live, runtime }
    }

    fn builds(&self) -> u64 {
        self.runtime.metrics.builds.load(Ordering::Relaxed)
    }

    async fn command(&self, command: DisplayCommand) -> Result<serde_json::Value, String> {
        self.live
            .command(command)
            .await
            .map(serde_json::Value::Object)
    }

    /// Prepares and activates a publication; returns the prepare reply.
    async fn publish(&self, directory: &Path, map_sha256: &str) -> serde_json::Value {
        let expected = self.live.expected();
        let prepared = self
            .command(DisplayCommand::Prepare {
                expected: expected.clone(),
                publication: DisplayPublication {
                    directory: directory.to_path_buf(),
                    map_sha256: map_sha256.to_string(),
                },
            })
            .await
            .unwrap();
        self.command(DisplayCommand::Activate {
            expected,
            map_sha256: map_sha256.to_string(),
        })
        .await
        .unwrap();
        prepared
    }
}

struct World {
    root: tempfile::TempDir,
    archive: Worker,
    recent: Worker,
    faults: Faults,
    url: String,
    server: tokio::task::JoinHandle<()>,
    /// Archive shard 0 over 100..=199 and recent shard 1 over 200..=260.
    a0: Published,
    r0: Published,
    a0_records: Vec<TransparentDisplayRecord>,
    r0_records: Vec<TransparentDisplayRecord>,
    p0: (PathBuf, String),
}

impl World {
    async fn start() -> Self {
        let root = tempfile::tempdir().unwrap();
        let a0_records = records(10, &[]);
        let r0_records = records(20, &[coincident(20, 1)]);
        let a0 =
            synth::write_shard(root.path(), &spec(0, 100, 199, true, ""), &a0_records).unwrap();
        let r0 = synth::write_shard(
            root.path(),
            &spec(1, 200, 260, false, &a0.digest),
            &r0_records,
        )
        .unwrap();
        let p0 = synth::write_candidate(root.path(), &params(1), &[a0.clone(), r0.clone()], "p0")
            .unwrap();
        let archive = Worker::new(root.path(), WorkerRole::ArchiveOwner);
        let recent = Worker::new(root.path(), WorkerRole::RecentReplica);
        // Both start empty and take the publication through control, as a
        // freshly deployed worker does.
        assert_eq!(archive.publish(&p0.0, &p0.1).await["built"], 2);
        assert_eq!(recent.publish(&p0.0, &p0.1).await["built"], 2);
        let faults = Faults::default();
        let app = Router::new().fallback(edge).with_state(Edge {
            archive: archive.live.router(),
            recent: recent.live.router(),
            faults: faults.clone(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            root,
            archive,
            recent,
            faults,
            url,
            server,
            a0,
            r0,
            a0_records,
            r0_records,
            p0,
        }
    }

    fn client(&self) -> DisplayClient {
        DisplayClient::new(&self.url, None, None).unwrap()
    }

    async fn get(&self, path: &str) -> (u16, serde_json::Value) {
        let response = reqwest::get(format!("{}{path}", self.url)).await.unwrap();
        let status = response.status().as_u16();
        let body = response.bytes().await.unwrap();
        (status, serde_json::from_slice(&body).unwrap_or_default())
    }
}

impl Drop for World {
    fn drop(&mut self) {
        self.server.abort();
    }
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_is_staged_then_published_without_rebuilding_archives() {
    let world = World::start().await;
    let root = world.root.path();
    // A stale client holds the first map, manifests and recent setup.
    let stale = world.client();
    let moved = &world.r0_records[40];
    found(&stale, moved, 210).await;

    // The recent shard's oldest range seals as shard 1; a new recent follows.
    let sealed_part: Vec<_> = world.r0_records.clone();
    let a1 = synth::write_shard(
        root,
        &spec(1, 200, 240, true, &world.a0.digest),
        &sealed_part,
    )
    .unwrap();
    let r1_records = records(30, &[]);
    let r1 = synth::write_shard(root, &spec(2, 241, 300, false, &a1.digest), &r1_records).unwrap();
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

    let p1 = synth::write_candidate(
        root,
        &params(1),
        &[world.a0.clone(), a1.clone(), r1.clone()],
        "p1",
    )
    .unwrap();
    // The owner takes the staged archive over by inode: nothing is built.
    let before = world.archive.builds();
    let prepared = world.archive.publish(&p1.0, &p1.1).await;
    assert_eq!(prepared["built"], 0, "{prepared}");
    assert_eq!(prepared["reused"], 4);
    assert_eq!(world.archive.builds(), before);
    let status = world.archive.command(DisplayCommand::Status).await.unwrap();
    assert_eq!(status["staged"], serde_json::json!([]));
    assert_eq!(status["active"]["map_sha256"], p1.1);
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
    let p2 = synth::write_candidate(root, &params(1), &[a1.clone(), r1.clone()], "p2").unwrap();
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
        let collected = worker.command(DisplayCommand::Collect).await.unwrap();
        assert_eq!(collected["removed"], serde_json::json!([]));
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
