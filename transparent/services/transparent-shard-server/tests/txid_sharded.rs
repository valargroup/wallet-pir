//! The tiered, bucketed txid display publication over real HTTP: an
//! archive-owner and a recent-replica worker behind an edge that routes
//! `/v1/txid/archive/` to the owner, as the deployment's static route does,
//! and the wallet client (`transparent-txid-client`).
#[path = "../examples/support/bytemeter.rs"]
mod bytemeter;

use axum::body::Body;
use axum::http::StatusCode;
use axum::Router;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tower::ServiceExt;
use transparent_events::Txid;
use transparent_shard::display::{self, DisplayTable, TXID_4K};
use transparent_shard::txid::{DisplayRecord, Tag};
use transparent_shard_server::assignment::WorkerRole;
use transparent_shard_server::display::live::DisplayCommand;
use transparent_shard_server::display::service::{router, DisplayRuntime, DisplayState};
use transparent_shard_server::display::set::DisplaySet;
use transparent_shard_server::display::synth::{self, Published};
use transparent_shard_server::service::{ReadinessMode, ServiceConfig};
use transparent_txid_client::{
    Placement, Route, Tier, TxidDisplayClient, TxidError, TxidLookup, TxidReply,
};

#[path = "support/display_world.rs"]
mod display_world;
use display_world::*;

const ABSENT: Txid = Txid([0xee; 32]);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lookups_follow_the_fixed_transcript_in_both_tiers() {
    let world = World::start().await;
    let mut wallet = world.wallet();
    for (records, height, tier) in [
        (&world.a0_records, 150, Tier::Archive),
        (&world.r0_records, 230, Tier::Recent),
    ] {
        // Once the shard's metadata is cached, every lookup is the same two
        // directory queries: each case the format distinguishes, and absence.
        assert_eq!(wallet.lookup(ABSENT, height).unwrap(), TxidLookup::Absent);
        wallet.http.take_log();
        for record in &records[CLASSES..CLASSES + 6] {
            let (provenance, log) = wallet.found(record, height);
            assert_eq!(provenance.tier, tier);
            assert_eq!(routes(&log), [Route::Query, Route::Query]);
            assert_queries(&log, 0);
        }
        assert_eq!(wallet.lookup(ABSENT, height).unwrap(), TxidLookup::Absent);
        assert_queries(&wallet.http.take_log(), 0);
    }

    // Coinciding candidate rows still cost two queries, and the duplicate
    // decoded row is dropped before the entry is found.
    let coincident = world.r0_records.last().unwrap();
    let [a, b] = display::candidate_rows(&Tag::of(&coincident.txid), 1, 0, 2_048);
    assert_eq!(a, b);
    let (_, log) = wallet.found(coincident, 255);
    assert_eq!(routes(&log), [Route::Query, Route::Query]);
    assert_queries(&log, 0);

    // Placement comes from the caller; nothing is asked to discover it.
    for (height, placement) in [(99, Placement::Below), (261, Placement::Above)] {
        assert_eq!(
            wallet.lookup(coincident.txid, height).unwrap(),
            TxidLookup::PlacementUnknown(placement)
        );
        assert_eq!(posts(&wallet.http.take_log()), 0);
    }

    // A server publishing a codec this client does not know is unsupported.
    world.faults.foreign_codec.store(true, Ordering::Release);
    let mut foreign = world.wallet();
    assert_eq!(
        foreign.lookup(coincident.txid, 255).unwrap(),
        TxidLookup::Unsupported
    );
    assert_eq!(posts(&foreign.http.take_log()), 0);
    world.faults.foreign_codec.store(false, Ordering::Release);

    // Errors are errors, never absence: a closed port, and a 503.
    let closed = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };
    let mut unreachable = Wallet::at(&format!("http://{closed}"));
    assert!(matches!(
        unreachable.lookup(coincident.txid, 255),
        Err(TxidError::Transport(_))
    ));
    world.faults.queries.store(1, Ordering::Release);
    assert_eq!(
        wallet.lookup(ABSENT, 150),
        Err(TxidError::Unavailable { retry_after: None })
    );
    wallet.http.take_log();

    // A lookup whose second query is shed fails rather than deciding from
    // one row; the retry finds the entry.
    let record = &world.a0_records[CLASSES + 2];
    let mut queries = 0;
    wallet.http.intercept = Some(Box::new(move |request| {
        queries += usize::from(request.route == Route::Query);
        (request.route == Route::Query && queries == 2).then(|| TxidReply {
            status: 503,
            ..TxidReply::default()
        })
    }));
    assert_eq!(
        wallet.lookup(record.txid, 150),
        Err(TxidError::Unavailable { retry_after: None })
    );
    assert_eq!(posts(&wallet.http.take_log()), 2);
    wallet.http.intercept = None;
    wallet.found(record, 150);

    // On the wire, a warm found lookup and a warm absent one are the same
    // bytes each way, whatever the entry holds: two requests of one size,
    // two responses of one size.
    let upstream: std::net::SocketAddr = world.url.trim_start_matches("http://").parse().unwrap();
    let meter = bytemeter::ByteMeter::start(upstream).await.unwrap();
    let mut metered = Wallet::at(&format!("http://{}", meter.addr));
    metered.found(&world.a0_records[CLASSES], 150);
    let mut wire = Vec::new();
    for txid in [
        world.a0_records[CLASSES].txid,
        ABSENT,
        world.a0_records[CLASSES + 1].txid,
        world.a0_records[CLASSES + 2].txid,
        world.a0_records[3].txid,
        Txid([0xdd; 32]),
    ] {
        let before = meter.snapshot();
        let lookup = metered.lookup(txid, 150).unwrap();
        let after = meter.snapshot();
        assert!(matches!(
            lookup,
            TxidLookup::Found { .. } | TxidLookup::Absent
        ));
        let log = metered.http.take_log();
        assert_eq!(routes(&log), [Route::Query, Route::Query]);
        let (up, down) = body_bytes(&log);
        assert_eq!((up as u64, down as u64), (2 * QUERY_BYTES, 2 * REPLY_BYTES));
        let measured = (after.0 - before.0, after.1 - before.1);
        assert!(measured.0 > up as u64 && measured.1 > down as u64);
        wire.push(measured);
    }
    assert!(wire.windows(2).all(|w| w[0] == w[1]), "{wire:?}");
    eprintln!("wire bytes of a warm lookup, up and down: {:?}", wire[0]);
}

/// The recent shard's oldest range seals as shard 1 and a new recent shard 2
/// follows: `a1` and `r1`, with `p1` publishing them beside `a0` and `p2`
/// dropping `a0` from the window. Shards and candidates are written up front,
/// because the owner collects `<root>/sealed` as it would its staged root.
struct Seal {
    a1: Published,
    r1: Published,
    r1_records: Vec<DisplayRecord>,
    p1: (PathBuf, String),
    p2: (PathBuf, String),
}

impl Seal {
    fn write(world: &World) -> Self {
        let root = world.root.path();
        let a1 = write_shard(
            root,
            &spec(1, 200, 240, true, &world.a0.digest),
            &world.r0_records,
        );
        let r1_records = records(30, &[]);
        let r1 = write_shard(root, &spec(2, 241, 300, false, &a1.digest), &r1_records);
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

/// How many times a transcript was sent back to refresh its map.
fn stale_retries(log: &[Sent]) -> usize {
    log.iter().filter(|s| s.status == 409).count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seal_is_staged_then_published_without_rebuilding_archives() {
    let world = World::start().await;
    // A stale wallet holds the first map, manifests and recent setup.
    let mut stale = world.wallet();
    let moved = &world.r0_records[CLASSES];
    stale.found(moved, 210);

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
    assert_eq!(staged["built"], 1);
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
    assert_eq!(prepared["reused"], 2);
    assert_eq!(world.archive.builds(), before);
    let status = world.archive.command(DisplayCommand::Status).await.unwrap();
    assert_eq!(status["staged"], serde_json::json!([]));
    assert_eq!(status["active"]["map_sha256"], p1.1);
    // Published and serving from p1's links, the staged copy is collected.
    assert_eq!(
        collect(&world.archive).await,
        serde_json::json!([a1.dir.display().to_string()])
    );
    // The replica builds only the new recent shard's one table.
    let prepared = world.recent.publish(&p1.0, &p1.1).await;
    assert_eq!(prepared["built"], 1, "{prepared}");

    // The stale wallet is still answered from the retained recent revision.
    let (provenance, log) = stale.found(moved, 210);
    assert_eq!(provenance.manifest_digest, world.r0.digest);
    assert_eq!(stale_retries(&log), 0);
    assert_eq!(routes(&log), [Route::Query, Route::Query]);
    // A fresh wallet reaches the same entry in the new archive.
    let mut fresh = world.wallet();
    let (provenance, _) = fresh.found(moved, 210);
    assert_eq!(provenance.manifest_digest, a1.digest);
    assert_eq!(provenance.tier, Tier::Archive);
    fresh.found(&r1_records[CLASSES + 2], 290);
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
    let old = &world.a0_records[CLASSES];
    assert_eq!(
        world.wallet().lookup(old.txid, 150).unwrap(),
        TxidLookup::PlacementUnknown(Placement::Below)
    );
    // A wallet that still holds the old map is answered from the retired
    // snapshot that holds the dropped archive, while it is resident.
    let (provenance, log) = stale.found(old, 150);
    assert_eq!(provenance.manifest_digest, world.a0.digest);
    assert_eq!(stale_retries(&log), 0);
    // p2's prepare evicted the retired recent revision. A stale wallet is
    // sent to refresh its map rather than costing the replica a cold build.
    let builds = world.recent.builds();
    let (provenance, log) = stale.found(moved, 210);
    assert_eq!(stale_retries(&log), 1);
    assert_eq!(routes(&log).iter().filter(|r| **r == Route::Map).count(), 1);
    assert_eq!(provenance.manifest_digest, a1.digest);
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
    let mut wallet = world.wallet();
    for record in &world.r0_records[CLASSES..CLASSES + 6] {
        wallet.found(record, 210);
    }
    for record in &r1_records[CLASSES..CLASSES + 6] {
        wallet.found(record, 290);
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

    let mut wallet = world.wallet();
    wallet.found(&world.r0_records[CLASSES], 230);
    world
        .recent
        .command(DisplayCommand::Invalidate {
            expected,
            from_height: 250,
        })
        .await
        .unwrap();
    // The map and readiness keep serving; the recent revision does not, so
    // the wallet refreshes its map once, is refused again, and says stale.
    assert_eq!(world.get("/v1/ready").await.0, 200);
    assert_eq!(world.get("/v1/txid/shards").await.0, 200);
    assert_eq!(
        wallet.lookup(world.r0_records[CLASSES].txid, 230),
        Err(TxidError::Stale)
    );
    assert_eq!(stale_retries(&wallet.http.take_log()), 2);
    wallet.found(&world.a0_records[CLASSES + 1], 150);

    // The next publication replaces the orphaned recent shard.
    let r0b = write_shard(
        world.root.path(),
        &spec(1, 200, 249, false, &world.a0.digest),
        &world.r0_records[..CLASSES + 6],
    );
    let p1 = synth::write_candidate(
        world.root.path(),
        &params(1),
        &[world.a0.clone(), r0b],
        "after-reorg",
    )
    .unwrap();
    world.recent.publish(&p1.0, &p1.1).await;
    world.wallet().found(&world.r0_records[CLASSES], 230);
}

/// Every query of a transcript uploads `bytes`.
fn assert_uploads(log: &[Sent], bytes: u64) {
    let posts: Vec<&Sent> = log.iter().filter(|s| s.route == Route::Query).collect();
    assert_eq!(posts.len(), 2);
    assert!(posts
        .iter()
        .all(|post| post.status == 200 && post.body.len() as u64 == bytes));
}

/// A txid-4k query: the 8-byte binding and the 4,096-row native selection.
const QUERY_BYTES_4K: u64 = 52_744;

/// A fresh lineage replaces the publication behind the same edge, as a
/// display re-cut or re-layout does: a lower start, shard ids restarted from
/// 0, every digest new, and an archive of another registered geometry.
/// Wallets holding the old map are answered from the retired snapshot while
/// it is resident, then sent to refresh once; they follow the new map with
/// one init refetch for the new geometry and keep nothing of the old lineage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wallets_follow_a_fresh_lineage_without_downtime() {
    let world = World::start().await;
    let (a0, r0) = (&world.a0.digest, &world.r0.digest);
    // Transactions mined below the old start.
    let older = records(40, &[]);
    let sorted = |digests: &[&String]| {
        let mut digests: Vec<String> = digests.iter().map(|d| d.to_string()).collect();
        digests.sort();
        digests
    };

    // `held` places by the default rule: its map was just fetched, so a
    // height below it is placed from it with no request.
    let mut held = world.wallet();
    held.found(&world.a0_records[CLASSES], 150);
    held.found(&world.r0_records[CLASSES], 230);
    assert_eq!(
        held.lookup(older[CLASSES].txid, 60).unwrap(),
        TxidLookup::PlacementUnknown(Placement::Below)
    );
    assert!(held.http.take_log().is_empty());
    let old_map = held.client.map_sha256().unwrap().to_string();
    assert_eq!(held.client.cached_revisions(), sorted(&[a0, r0]));
    // `aged` counts any map it did not fetch in the same lookup as stale.
    let mut aged = world.wallet();
    aged.client =
        TxidDisplayClient::with_profiles(profiles()).with_placement_refresh_age(Duration::ZERO);
    aged.found(&world.a0_records[CLASSES + 1], 150);

    // The fresh lineage, from height 50, over the same transactions.
    let root = world.root.path();
    let mut first = spec(0, 50, 139, true, "");
    first.geometry = &TXID_4K;
    let b0 = write_shard(root, &first, &older);
    let b1 = write_shard(
        root,
        &spec(1, 140, 209, true, &b0.digest),
        &world.a0_records,
    );
    let b2 = write_shard(
        root,
        &spec(2, 210, 270, false, &b1.digest),
        &world.r0_records,
    );
    let fresh = synth::write_candidate(
        root,
        &params(1),
        &[b0.clone(), b1.clone(), b2.clone()],
        "fresh",
    )
    .unwrap();
    // Each worker prepares it beside the old publication, then swaps.
    assert_eq!(world.archive.publish(&fresh.0, &fresh.1).await["built"], 2);
    assert_eq!(world.recent.publish(&fresh.0, &fresh.1).await["built"], 1);
    let (_, init) = world.get("/v1/txid/init").await;
    assert_eq!(init["geometries"].as_array().unwrap().len(), 2, "{init}");

    // The old map's archive is answered from the retired snapshot while its
    // runtime is resident.
    let (provenance, log) = held.found(&world.a0_records[CLASSES + 2], 150);
    assert_eq!(
        (&provenance.manifest_digest, &provenance.map_sha256),
        (a0, &old_map)
    );
    assert_eq!(routes(&log), [Route::Query, Route::Query]);

    // Once evicted, the old revision is refused with a 409: the wallet
    // refreshes its map once and finds the entry in the new recent shard.
    // The new map names nothing the wallet held, so it keeps nothing.
    for worker in [&world.archive, &world.recent] {
        worker.runtime.cache.evict_unpinned();
    }
    let (provenance, log) = held.found(&world.r0_records[CLASSES], 230);
    assert_eq!(stale_retries(&log), 1);
    assert_eq!(
        routes(&log),
        [
            Route::Query,
            Route::Map,
            Route::Manifest,
            Route::Setup,
            Route::Query,
            Route::Query
        ]
    );
    assert_eq!(
        (provenance.shard_id, &provenance.manifest_digest),
        (2, &b2.digest)
    );
    assert_ne!(provenance.map_sha256, old_map);
    assert_eq!(held.client.cached_revisions(), [b2.digest.as_str()]);
    // The old archive's height lies in the new archive 1, through a new
    // index chunk.
    let (provenance, log) = held.found(&world.a0_records[CLASSES + 2], 150);
    assert_eq!(
        routes(&log),
        [
            Route::MapChunk,
            Route::Manifest,
            Route::Setup,
            Route::Query,
            Route::Query
        ]
    );
    assert_eq!(
        (provenance.shard_id, &provenance.manifest_digest),
        (1, &b1.digest)
    );
    // Below the old start, in the txid-4k archive: init, cached from the old
    // publication, lacks the geometry and is fetched once more.
    let (provenance, log) = held.found(&older[CLASSES], 60);
    assert_eq!(
        routes(&log),
        [
            Route::Init,
            Route::Manifest,
            Route::Setup,
            Route::Query,
            Route::Query
        ]
    );
    assert_uploads(&log, QUERY_BYTES_4K);
    assert_eq!(
        (provenance.shard_id, &provenance.manifest_digest),
        (0, &b0.digest)
    );
    assert_eq!(
        held.client.cached_revisions(),
        sorted(&[&b0.digest, &b1.digest, &b2.digest])
    );
    // Warm: two queries only.
    let (_, log) = held.found(&older[CLASSES + 1], 61);
    assert_eq!(routes(&log), [Route::Query, Route::Query]);

    // `aged` still holds the old map, which places 60 below it. That map is
    // stale, so it is fetched again first, and the lookup reaches the new
    // archive with one map, chunk and init request.
    let (provenance, log) = aged.found(&older[CLASSES + 2], 60);
    assert_eq!(
        routes(&log),
        [
            Route::Map,
            Route::MapChunk,
            Route::Init,
            Route::Manifest,
            Route::Setup,
            Route::Query,
            Route::Query
        ]
    );
    assert_uploads(&log, QUERY_BYTES_4K);
    assert_eq!(provenance.manifest_digest, b0.digest);
    assert_eq!(aged.client.cached_revisions(), [b0.digest.as_str()]);
    // Below the new start: one refresh, then placed below.
    assert_eq!(
        aged.lookup(older[CLASSES].txid, 10).unwrap(),
        TxidLookup::PlacementUnknown(Placement::Below)
    );
    assert_eq!(routes(&aged.http.take_log()), [Route::Map]);

    // A new wallet sees only the new lineage.
    let mut wallet = world.wallet();
    for (record, height, shard) in [
        (&older[CLASSES + 3], 100, &b0),
        (&world.a0_records[CLASSES + 3], 150, &b1),
        (&world.r0_records[CLASSES + 3], 230, &b2),
    ] {
        let (provenance, _) = wallet.found(record, height);
        assert_eq!(provenance.manifest_digest, shard.digest);
    }
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
    for label in ["directory-2", "directory-01", "txdirectory", "pages"] {
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
        |digest: &str| format!("/v1/txid/recent/shards/0/revisions/{digest}/setup/directory-0/0");
    assert_eq!(
        send(&owner, &recent_path(a), None).await.0,
        StatusCode::MISDIRECTED_REQUEST
    );
    assert_eq!(
        send(&replica, &archive(a, "setup/directory-0/0"), None)
            .await
            .0,
        StatusCode::MISDIRECTED_REQUEST
    );
    let r_digest = &r0.digest;
    assert_eq!(
        send(
            &owner,
            &format!("/v1/txid/recent/shards/1/revisions/{r_digest}/setup/directory-0/0"),
            None
        )
        .await
        .0,
        StatusCode::MISDIRECTED_REQUEST
    );
    assert_eq!(
        send(
            &replica,
            &format!("/v1/txid/archive/shards/1/revisions/{r_digest}/setup/directory-0/0"),
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
            &format!("/v1/txid/archive/shards/3/revisions/{a}/setup/directory-0/0"),
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
    let a0 = write_shard(root.path(), &spec(0, 100, 199, true, ""), &records(10, &[]));
    let r0 = write_shard(
        root.path(),
        &spec(1, 200, 260, false, &a0.digest),
        &records(20, &[]),
    );
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
    assert_eq!(prepared["built"], 1);
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
