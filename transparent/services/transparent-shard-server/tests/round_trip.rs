//! End to end: publish a small shard set, serve it, and retrieve rows from
//! several shards privately over the real HTTP surface.
//!
//! The point is not that PIR works — that is tested where the scheme lives.
//! The point is that *many shards in one process* behave: rows come back from
//! the shard they were asked of, a query built for one shard is refused by
//! another, and the parameters really are shared rather than per shard.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::path::Path;
use tower::ServiceExt;
use transparent_filter::{filter_hash, BlockHash, ScriptBytes, ShardMap, ShardMapEntry};
use transparent_shard::build::build_shard;
use transparent_shard::layout::{Geometry, RECENT_8K};
use transparent_shard::manifest::{
    query_binding, ManifestLayout, ManifestOccupancy, ManifestSeal, ShardManifest, TableGeometry,
    SCHEMA,
};
use transparent_shard_server::service::{router, ServiceConfig, ServiceState};
use transparent_shard_server::shardset::{ShardSet, Table, DEFAULT_RETAIN_REVISIONS};

/// The geometry this fixture publishes at.
const GEOMETRY: Geometry = RECENT_8K;

const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;
const SHARDS: u64 = 3;
const SPAN: u64 = 100;
const FIRST: u64 = 3_428_143;

fn genesis() -> BlockHash {
    BlockHash::from_display_hex(GENESIS).unwrap()
}

fn hash_at(height: u64) -> BlockHash {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&height.to_le_bytes());
    BlockHash::from_internal_bytes(bytes)
}

fn script(shard: u64, tag: u32) -> ScriptBytes {
    let mut bytes = vec![0x76, 0xa9, 0x14];
    bytes.extend_from_slice(&shard.to_le_bytes());
    bytes.extend_from_slice(&tag.to_le_bytes());
    bytes.extend_from_slice(&[0x88, 0xac]);
    ScriptBytes::new(bytes)
}

/// Publishes `SHARDS` shards, each holding a handful of scripts with enough
/// history to need pages.
fn publish(dir: &Path) -> ShardMap {
    let mut entries = Vec::new();
    let mut parent_digest = String::new();
    for shard_id in 0..SHARDS {
        let start = FIRST + shard_id * SPAN;
        let end = start + SPAN - 1;
        let terminal = hash_at(end);

        let mut events = Vec::new();
        for tag in 0..40u32 {
            // Enough events that some scripts page and some stay inline.
            for i in 0..(2 + tag % 7) {
                let mut txid = [0u8; 32];
                txid[..4].copy_from_slice(&tag.to_le_bytes());
                txid[4..8].copy_from_slice(&i.to_le_bytes());
                txid[8..16].copy_from_slice(&shard_id.to_le_bytes());
                events.push((
                    script(shard_id, tag),
                    transparent_events::TransparentEvent::Receive(
                        transparent_events::ReceiveEvent {
                            metadata: None,
                            height: (start + u64::from(i) % SPAN) as u32,
                            txid: transparent_events::Txid(txid),
                            transaction_index: 0,
                            output_index: i,
                            value: 1_000 + u64::from(i),
                            coinbase: false,
                        },
                    ),
                ));
            }
        }

        let built = build_shard(
            shard_id,
            start,
            end,
            genesis(),
            terminal,
            transparent_filter::RANGE_PROFILE,
            &GEOMETRY,
            &events,
        )
        .expect("build");

        let manifest = ShardManifest {
            schema: SCHEMA.to_string(),
            profile: transparent_filter::RANGE_PROFILE.to_string(),
            geometry: GEOMETRY.name.to_string(),
            network: transparent_filter::NETWORK.to_string(),
            genesis_hash: GENESIS.to_string(),
            shard_id,
            start_height: start,
            end_height: end,
            parent_block_hash: hash_at(start - 1).to_display_hex(),
            terminal_block_hash: terminal.to_display_hex(),
            tag_salt_counter: built.tag_salt_counter,
            parent_manifest_digest: parent_digest.clone(),
            sealed: shard_id + 1 < SHARDS,
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
            directory_segments: built
                .directory
                .iter()
                .map(|segment| TableGeometry {
                    rows: GEOMETRY.directory_rows,
                    row_bytes: GEOMETRY.directory_row_bytes as u32,
                    sha256: hex::encode(<sha2::Sha256 as sha2::Digest>::digest(segment)),
                })
                .collect(),
            page_segments: built
                .pages
                .iter()
                .map(|segment| TableGeometry {
                    rows: GEOMETRY.page_rows,
                    row_bytes: GEOMETRY.page_row_bytes as u32,
                    sha256: hex::encode(<sha2::Sha256 as sha2::Digest>::digest(segment)),
                })
                .collect(),
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

        entries.push(ShardMapEntry {
            shard_id,
            geometry: GEOMETRY.name.to_string(),
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
            manifest_digest: digest.clone(),
            revision: 0,
            sealed: manifest.sealed,
        });
        parent_digest = digest;
    }

    let map = ShardMap {
        genesis_hash: GENESIS.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: FIRST,
        seal: std::collections::BTreeMap::from([(
            GEOMETRY.name.to_string(),
            transparent_filter::SealParameters {
                max_scripts: 8_192,
                max_page_rows: 2_048,
                max_txids: 0,
            },
        )]),
        shards: entries,
        recuts: Vec::new(),
    };
    std::fs::write(
        dir.join("shards.json"),
        serde_json::to_vec_pretty(&map).unwrap(),
    )
    .unwrap();
    map
}

struct Fixture {
    state: ServiceState,
    set: ShardSet,
    _dir: tempfile::TempDir,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    publish(dir.path());
    let set = ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).expect("load");
    let served = ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).expect("load");
    Fixture {
        state: ServiceState::build(served, ServiceConfig::default()).expect("state"),
        set,
        _dir: dir,
    }
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

async fn post(state: &ServiceState, path: &str, body: Vec<u8>) -> (StatusCode, Vec<u8>) {
    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("content-length", body.len().to_string())
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, body.to_vec())
}

/// The manifest digest naming the current revision of `shard_id`.
///
/// Every private route is addressed by revision, so a test that wants to reach
/// a shard has to say which publication of it it means — the same thing a
/// wallet reads out of the map.
fn revision_of(f: &Fixture, shard_id: u64) -> String {
    f.set.get(shard_id).unwrap().digest.clone()
}

/// Builds a client for one table and retrieves `row` from `shard_id` with the
/// 49-bit query every deployed wallet sends.
async fn retrieve(f: &Fixture, shard_id: u64, table: Table, row: usize) -> Vec<u8> {
    retrieve_at(f, shard_id, table, row, false).await
}

/// [`retrieve`], with the 44-bit dithered query when `dithered`.
async fn retrieve_at(
    f: &Fixture,
    shard_id: u64,
    table: Table,
    row: usize,
    dithered: bool,
) -> Vec<u8> {
    let (status, raw) = get(&f.state, "/v1/shards/init").await;
    assert_eq!(status, StatusCode::OK);
    let init: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let geometries = init["geometries"].as_array().unwrap();
    let published = geometries
        .iter()
        .find(|entry| entry["name"] == GEOMETRY.name)
        .expect("the fixture's geometry is published");
    let scheme_key = match table {
        Table::Directory => "directory_scheme",
        Table::Pages => "pages_scheme",
        Table::TxDirectory => unreachable!("history-only fixture"),
    };
    let scheme: transparent_native::NativeScheme =
        serde_json::from_value(published[scheme_key].clone()).unwrap();
    let dithered_scheme: transparent_native::NativeScheme =
        serde_json::from_value(published[format!("{scheme_key}_dq44")].clone()).unwrap();

    let revision = revision_of(f, shard_id);
    let (status, raw) = get(
        &f.state,
        &format!(
            "/v1/shards/{shard_id}/revisions/{revision}/setup/{}/0",
            table.as_str()
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let setup: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let public_params = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        setup["public_params"].as_str().unwrap(),
    )
    .unwrap();

    // Re-derive rather than trust: a client that adopted the server's
    // parameters would decode against whatever geometry the server chose.
    let profile = transparent_native::TableProfile::new(
        transparent_shard::manifest::SCHEMA,
        GEOMETRY.name,
        table.as_str(),
        table.rows(&GEOMETRY),
        table.row_bytes(&GEOMETRY),
    )
    .unwrap();
    assert_eq!(
        scheme, profile.scheme,
        "the served scheme must be the one a client re-derives"
    );
    assert_eq!(
        dithered_scheme, profile.dithered_scheme,
        "so must the served dithered scheme"
    );
    assert_eq!(public_params.len(), profile.scheme.public_bytes);

    let (secret, upload) = if dithered {
        profile.prepare_dithered(row).unwrap()
    } else {
        profile.prepare(row).unwrap()
    };
    let used = if dithered { &dithered_scheme } else { &scheme };
    assert_eq!(upload.len(), used.request_bytes);
    let binding = query_binding(&revision, table.as_str());
    let mut body = binding.to_vec();
    body.extend(upload);

    let (status, response) = post(
        &f.state,
        &format!(
            "/v1/shards/{shard_id}/revisions/{revision}/query/{}",
            table.as_str()
        ),
        body,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query failed");
    assert_eq!(&response[..8], &binding);
    // One body per segment. These fixtures are single-segment shards, so the
    // answer is one body; the multi-segment case has its own test.
    assert_eq!(
        response.len(),
        16 + profile.scheme.response_bytes,
        "one body per segment"
    );

    profile
        .decode(&secret, &public_params, &response[16..])
        .unwrap()
}

fn raw_row(table_bytes: &[u8], row_bytes: usize, row: usize) -> &[u8] {
    &table_bytes[row * row_bytes..(row + 1) * row_bytes]
}

#[tokio::test]
async fn rows_retrieved_from_each_shard_equal_that_shards_published_table() {
    let f = fixture();
    for shard_id in 0..SHARDS {
        for (table, row) in [(Table::Directory, 11usize), (Table::Pages, 3)] {
            let decoded = retrieve(&f, shard_id, table, row).await;
            let shard = f.set.get(shard_id).unwrap();
            let published = shard.segment(table, 0).unwrap().load().unwrap();
            assert_eq!(
                decoded,
                raw_row(&published, table.row_bytes(&GEOMETRY) as usize, row),
                "shard {shard_id} {} row {row}",
                table.as_str()
            );
        }
    }
}

/// The dithered 44-bit query answers the same rows as the 49-bit one, from
/// the same server and the same published masks.
#[tokio::test]
async fn dithered_queries_retrieve_each_shards_published_rows() {
    let f = fixture();
    for shard_id in 0..SHARDS {
        for (table, row) in [(Table::Directory, 11usize), (Table::Pages, 3)] {
            let shard = f.set.get(shard_id).unwrap();
            let published = shard.segment(table, 0).unwrap().load().unwrap();
            let expected = raw_row(&published, table.row_bytes(&GEOMETRY) as usize, row);
            assert_eq!(
                retrieve_at(&f, shard_id, table, row, true).await,
                expected,
                "shard {shard_id} {} row {row}",
                table.as_str()
            );
        }
    }
}

/// A full-length 49-bit query body for the fixture's geometry, prefixed as
/// `revision` and `table` require.
fn padded_body(revision: &str, table: Table) -> Vec<u8> {
    padded_body_of(
        revision,
        table,
        transparent_native::request_len(table.rows(&GEOMETRY) as usize),
    )
}

/// A query body of `request` bytes after the binding.
fn padded_body_of(revision: &str, table: Table, request: usize) -> Vec<u8> {
    let mut body = query_binding(revision, table.as_str()).to_vec();
    body.resize(8 + request, 0);
    body
}

/// Exactly two lengths are queries: the 49-bit one and the 44-bit dithered
/// one. A body a byte either side of either, or the other table geometry's
/// dithered length, is refused from its declared length before any work.
#[tokio::test]
async fn only_the_two_query_lengths_are_accepted() {
    let f = fixture();
    let revision = revision_of(&f, 0);
    let rows = Table::Directory.rows(&GEOMETRY) as usize;
    let legacy = transparent_native::request_len(rows);
    let dithered = transparent_native::dithered_request_len(rows);
    let refused = [
        dithered - 1,
        dithered + 1,
        legacy - 1,
        transparent_native::dithered_request_len(2 * rows),
        transparent_native::request_len_bits(rows, 43),
    ];
    for request in refused {
        let (status, raw) = post(
            &f.state,
            &format!("/v1/shards/0/revisions/{revision}/query/directory"),
            padded_body_of(&revision, Table::Directory, request),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{request} bytes");
        let text = String::from_utf8_lossy(&raw);
        assert!(
            text.contains(&format!("exactly {} or {} bytes", 8 + legacy, 8 + dithered)),
            "{text}"
        );
    }
    let metrics = f.state.metrics();
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&metrics.query_length_rejections),
        refused.len() as u64
    );
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&metrics.cache_misses),
        0
    );
}

/// The binding in the query prefix is precisely so a query cannot be answered
/// by the wrong shard. Without it a client would decode plausible nonsense.
#[tokio::test]
async fn a_query_built_for_one_shard_is_refused_by_another() {
    let f = fixture();
    let zero = revision_of(&f, 0);
    let one = revision_of(&f, 1);
    // Shard zero's binding, sent to shard one's revision.
    let body = padded_body(&zero, Table::Directory);
    let (status, _) = post(
        &f.state,
        &format!("/v1/shards/1/revisions/{one}/query/directory"),
        body,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// The binding names the table as well as the revision, so a page query cannot
/// be answered from the directory it shares a shard with.
#[tokio::test]
async fn a_query_built_for_one_table_is_refused_by_the_other() {
    let f = fixture();
    let revision = revision_of(&f, 0);
    let body = padded_body(&revision, Table::Pages);
    let (status, _) = post(
        &f.state,
        &format!("/v1/shards/0/revisions/{revision}/query/directory"),
        body,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_truncated_query_is_refused_rather_than_padded() {
    let f = fixture();
    let revision = revision_of(&f, 0);
    let body = query_binding(&revision, "directory").to_vec();
    let (status, _) = post(
        &f.state,
        &format!("/v1/shards/0/revisions/{revision}/query/directory"),
        body,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// An oversized body is refused from its declared length, before it is
/// buffered, queued or built for: the check the global body limit cannot make,
/// because that limit is the ceiling for the widest geometry served.
#[tokio::test]
async fn an_oversized_query_is_refused_before_any_work() {
    let f = fixture();
    let revision = revision_of(&f, 0);
    let mut body = padded_body(&revision, Table::Directory);
    body.push(0);
    let (status, raw) = post(
        &f.state,
        &format!("/v1/shards/0/revisions/{revision}/query/directory"),
        body,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(String::from_utf8_lossy(&raw).contains("must be exactly"));
    let metrics = f.state.metrics();
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&metrics.query_length_rejections),
        1
    );
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&metrics.cache_misses),
        0,
        "no runtime was built for a query that was never going to be evaluated"
    );
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&metrics.query_queue_depth),
        0
    );
}

/// A body stream that never yields and never ends: what a stalled upload looks
/// like from the server's side.
struct NeverEnds;

impl http_body::Body for NeverEnds {
    type Data = axum::body::Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        std::task::Poll::Pending
    }
}

fn streaming_request(path: &str, content_length: Option<usize>) -> Request<Body> {
    let mut builder = Request::builder().method("POST").uri(path);
    if let Some(length) = content_length {
        builder = builder.header("content-length", length.to_string());
    }
    builder.body(Body::new(NeverEnds)).unwrap()
}

/// A query whose length is unknown cannot be checked before it is read, so it
/// is not read at all.
#[tokio::test]
async fn a_query_without_a_declared_length_is_refused() {
    let f = fixture();
    let revision = revision_of(&f, 0);
    let response = router(f.state.clone())
        .oneshot(streaming_request(
            &format!("/v1/shards/0/revisions/{revision}/query/directory"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::LENGTH_REQUIRED);
}

/// A client that declares the right length and then stalls does not hold the
/// worker: the upload deadline ends it, and its waiting place comes back.
#[tokio::test]
async fn a_slow_upload_times_out_without_holding_a_place() {
    let dir = tempfile::tempdir().unwrap();
    publish(dir.path());
    let set = ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).expect("load");
    let state = ServiceState::build(
        set,
        ServiceConfig {
            upload_deadline: std::time::Duration::from_millis(100),
            ..ServiceConfig::default()
        },
    )
    .expect("state");
    let revision = state_revision(&state, dir.path(), 0);
    let expected = padded_body(&revision, Table::Directory).len();
    let response = router(state.clone())
        .oneshot(streaming_request(
            &format!("/v1/shards/0/revisions/{revision}/query/directory"),
            Some(expected),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    let metrics = state.metrics();
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&metrics.upload_timeouts),
        1
    );
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&metrics.query_queue_depth),
        0,
        "the timed-out request no longer counts as waiting"
    );
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&metrics.body_bytes_in_flight),
        0
    );
}

/// The digest the map names for `shard_id`, read from the published map on
/// disk, for states built outside `fixture()`.
fn state_revision(_: &ServiceState, dir: &Path, shard_id: u64) -> String {
    let map: ShardMap =
        serde_json::from_slice(&std::fs::read(dir.join("shards.json")).unwrap()).unwrap();
    map.shards
        .iter()
        .find(|entry| entry.shard_id == shard_id)
        .unwrap()
        .manifest_digest
        .clone()
}

/// With every slot busy and every waiting place taken, the next query is
/// refused retryably rather than queued without bound — and a waiter that gives
/// up hands its place back.
#[tokio::test(flavor = "multi_thread")]
async fn a_full_queue_refuses_retryably_and_a_cancelled_waiter_frees_its_place() {
    let dir = tempfile::tempdir().unwrap();
    publish(dir.path());
    let set = ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).expect("load");
    let state = ServiceState::build(
        set,
        ServiceConfig {
            query_slots: 1,
            max_waiters: 1,
            query_deadline: std::time::Duration::from_secs(30),
            ..ServiceConfig::default()
        },
    )
    .expect("state");
    let revision = state_revision(&state, dir.path(), 0);
    let path = format!("/v1/shards/0/revisions/{revision}/query/directory");
    let body = padded_body(&revision, Table::Directory);

    // The only slot is busy.
    let held = state.hold_query_slot().await;
    // One waiter takes the one waiting place.
    let waiting = {
        let state = state.clone();
        let path = path.clone();
        let body = body.clone();
        tokio::spawn(async move { post(&state, &path, body).await })
    };
    let metrics = state.metrics().clone();
    let depth = || transparent_shard_server::metrics::Metrics::get(&metrics.query_queue_depth);
    while depth() < 1 {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    // The next is refused, with a delay the wallet keys its retry on.
    let response = router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&path)
                .header("content-length", body.len().to_string())
                .body(Body::from(body.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .map(|v| v.to_str().unwrap()),
        Some("1")
    );
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&metrics.queue_rejections),
        1
    );

    // The waiter gives up: its place is freed and the cancellation counted.
    waiting.abort();
    let _ = waiting.await;
    while depth() > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&metrics.queries_cancelled),
        1
    );
    // With the slot released, the same request is admitted and evaluated: a
    // zero-padded body is a well-formed query that selects nothing, so it is
    // answered rather than refused, and a runtime was built to answer it.
    drop(held);
    let (status, raw) = post(&state, &path, body).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&raw));
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&metrics.cache_misses),
        1,
        "a runtime was built for the admitted query"
    );
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&metrics.queries),
        1
    );
    assert_eq!(depth(), 0);
}

/// A capacity refusal decided before the body is read still reads the body,
/// so the connection survives it. A proxy streams the upload while the worker
/// answers; closing under an unread upload broke the proxy's write, turned
/// the refusal into a proxy 502 without its delay, and counted against the
/// worker's health — which on the 2026-09-27 bench fleet ejected every
/// saturated worker at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_capacity_refusal_reads_the_upload_and_keeps_the_connection() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = tempfile::tempdir().unwrap();
    publish(dir.path());
    let set = ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).expect("load");
    let state = ServiceState::build(
        set,
        ServiceConfig {
            query_slots: 1,
            max_waiters: 0,
            ..ServiceConfig::default()
        },
    )
    .expect("state");
    let revision = state_revision(&state, dir.path(), 0);
    let path = format!("/v1/shards/0/revisions/{revision}/query/directory");
    let body = padded_body(&revision, Table::Directory);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(std::future::IntoFuture::into_future(axum::serve(
        listener,
        router(state.clone()),
    )));
    // The only slot is busy and there is no waiting place.
    let _held = state.hold_query_slot().await;

    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    // Two refused queries and a readiness check, one connection.
    for _ in 0..2 {
        let head = format!(
            "POST {path} HTTP/1.1\r\nhost: worker\r\ncontent-length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        stream
            .write_all(&body)
            .await
            .expect("the upload is read in full");
    }
    stream
        .write_all(b"GET /v1/ready HTTP/1.1\r\nhost: worker\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut replies = String::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        stream.read_to_string(&mut replies),
    )
    .await
    .expect("replies arrive")
    .unwrap();
    // Bodies carry no trailing newline, so split on the status lines.
    let statuses: Vec<&str> = replies
        .split("HTTP/1.1 ")
        .skip(1)
        .map(|reply| reply.lines().next().unwrap())
        .collect();
    assert_eq!(
        statuses,
        [
            "503 Service Unavailable",
            "503 Service Unavailable",
            "200 OK"
        ],
        "{replies}"
    );
    assert_eq!(replies.matches("retry-after: 1").count(), 2, "{replies}");
    let metrics = state.metrics();
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&metrics.queue_rejections),
        2
    );
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&metrics.refusals_unread),
        0
    );
    server.abort();
}

/// A request that waits its whole deadline for a slot is refused retryably
/// rather than kept waiting.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_that_outwaits_its_deadline_is_refused_retryably() {
    let dir = tempfile::tempdir().unwrap();
    publish(dir.path());
    let set = ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).expect("load");
    let state = ServiceState::build(
        set,
        ServiceConfig {
            query_slots: 1,
            query_deadline: std::time::Duration::from_millis(100),
            ..ServiceConfig::default()
        },
    )
    .expect("state");
    let revision = state_revision(&state, dir.path(), 0);
    let path = format!("/v1/shards/0/revisions/{revision}/query/directory");
    let body = padded_body(&revision, Table::Directory);
    let _held = state.hold_query_slot().await;
    let (status, _) = post(&state, &path, body).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&state.metrics().deadline_exceeded),
        1
    );
}

/// The manifest route serves the canonical bytes the digest names, so a wallet
/// recomputing the digest from what it received reproduces the map's.
#[tokio::test]
async fn the_manifest_route_serves_canonical_bytes_that_digest_to_the_path() {
    let f = fixture();
    for shard_id in 0..SHARDS {
        let revision = revision_of(&f, shard_id);
        let (status, raw) = get(
            &f.state,
            &format!("/v1/shards/{shard_id}/revisions/{revision}/manifest"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let manifest: ShardManifest = serde_json::from_slice(&raw).unwrap();
        assert_eq!(manifest.digest(), revision);
        assert_eq!(raw, manifest.canonical_bytes());
        assert_eq!(manifest.shard_id, shard_id);
    }
    // A revision of another shard is a routing error; an unknown digest is a
    // stale client, exactly as for setup and query.
    let zero = revision_of(&f, 0);
    let (status, _) = get(&f.state, &format!("/v1/shards/1/revisions/{zero}/manifest")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, raw) = get(
        &f.state,
        &format!("/v1/shards/0/revisions/{}/manifest", "bb".repeat(32)),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let refused: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(refused["map_sha256"], f.set.map_digest);
}

#[tokio::test]
async fn unknown_shards_and_tables_are_refused() {
    let f = fixture();
    let revision = revision_of(&f, 0);
    // A digest nobody published is a revision this worker does not hold, which
    // is a stale client rather than a malformed request.
    let (status, _) = get(
        &f.state,
        &format!(
            "/v1/shards/99/revisions/{}/setup/directory/0",
            "aa".repeat(32)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    // A real revision, but of another shard: that is a routing mistake, and the
    // service must not confuse the two.
    let (status, _) = get(
        &f.state,
        &format!("/v1/shards/1/revisions/{revision}/setup/directory/0"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = get(
        &f.state,
        &format!("/v1/shards/0/revisions/{revision}/setup/nonsense/0"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // A shard has the segments its manifest declares and no more. Asking for
    // one it does not have is a mistake, not an empty answer.
    let (status, _) = get(
        &f.state,
        &format!("/v1/shards/0/revisions/{revision}/setup/directory/7"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// Every shard shares one parameter set per table. That sharing is the reason
/// the geometry is pinned, so it is worth asserting rather than assuming.
#[tokio::test]
async fn every_shard_reports_the_same_scheme_but_its_own_setup() {
    let f = fixture();
    let (_, raw) = get(&f.state, "/v1/shards/init").await;
    let init: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(init["shards"], SHARDS);

    let mut digests = Vec::new();
    for shard_id in 0..SHARDS {
        let revision = revision_of(&f, shard_id);
        let (_, raw) = get(
            &f.state,
            &format!("/v1/shards/{shard_id}/revisions/{revision}/setup/directory/0"),
        )
        .await;
        let setup: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        digests.push(setup["public_params_sha256"].as_str().unwrap().to_string());
    }
    let unique: std::collections::BTreeSet<_> = digests.iter().collect();
    assert_eq!(
        unique.len(),
        SHARDS as usize,
        "each shard's published setup is derived from its own table"
    );
}

/// A shard set with a hole must not load. A wallet syncing a range whose shards
/// were silently missing would advance coverage over history it never fetched.
#[tokio::test]
async fn a_shard_set_missing_a_shard_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    // Remove the middle shard's directory, leaving the map naming it.
    let victim = &map.shards[1];
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let path = entry.unwrap().path();
        if !path.is_dir() {
            continue;
        }
        let raw = std::fs::read(path.join("manifest.json")).unwrap();
        let manifest: ShardManifest = serde_json::from_slice(&raw).unwrap();
        if manifest.shard_id == victim.shard_id {
            std::fs::remove_dir_all(&path).unwrap();
        }
    }
    assert!(ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).is_err());
}

/// A table edited after publication must fail at load, not be served under the
/// identity of the shard it replaced.
#[tokio::test]
async fn a_tampered_table_is_refused_at_load() {
    let dir = tempfile::tempdir().unwrap();
    publish(dir.path());
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let path = entry.unwrap().path();
        if !path.is_dir() {
            continue;
        }
        let mut bytes = std::fs::read(path.join("directory.0.bin")).unwrap();
        bytes[0] ^= 0xff;
        std::fs::write(path.join("directory.0.bin"), bytes).unwrap();
        break;
    }
    assert!(ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).is_err());
}

/// The public range filters, which this service also serves.
///
/// The bytes must be the published ones, not merely well-formed: a wallet tests
/// its own scripts against them and skips the shards that do not match, so a
/// filter that differs from what was published makes the wallet miss history
/// and then advance coverage over the gap. That is a wrong answer rather than
/// an error, which is why this compares against the set on disk.
#[tokio::test]
async fn each_shards_published_filter_is_served_verbatim() {
    let f = fixture();
    for shard_id in 0..SHARDS {
        let (status, body) = get(&f.state, &format!("/v1/filters/shards/{shard_id}/filter")).await;
        assert_eq!(status, StatusCode::OK, "shard {shard_id}");
        let published = &f.set.get(shard_id).expect("shard is in the set").filter;
        assert_eq!(&body, published, "shard {shard_id} filter differs");
        assert!(!body.is_empty(), "shard {shard_id} filter is empty");
    }
}

#[tokio::test]
async fn a_filter_for_a_shard_outside_the_set_is_refused() {
    let f = fixture();
    let (status, _) = get(&f.state, &format!("/v1/filters/shards/{SHARDS}/filter")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// The map is reachable under both names, and is the same map.
///
/// `/v1/filters/shards` exists so a wallet can point at this host or at the
/// filter service by changing only the base URL. If the two paths ever answered
/// differently, that substitution would silently stop being safe.
#[tokio::test]
async fn the_map_is_identical_under_both_paths() {
    let f = fixture();
    let (private_status, private_body) = get(&f.state, "/v1/shards").await;
    let (public_status, public_body) = get(&f.state, "/v1/filters/shards").await;
    assert_eq!(private_status, StatusCode::OK);
    assert_eq!(public_status, StatusCode::OK);
    assert_eq!(private_body, public_body);
    let map: ShardMap = serde_json::from_slice(&public_body).expect("map json");
    assert_eq!(map.shards.len() as u64, SHARDS);
}

/// A growing tail's pages queries may select over only the rows its manifest
/// says can hold data. That upload is the full one's byte prefix and is
/// answered byte for byte the same, and it decodes the published row.
#[tokio::test]
async fn a_growing_tails_prefix_pages_query_is_answered_as_its_full_one() {
    let f = fixture();
    let tail = SHARDS - 1;
    let shard = f.set.get(tail).unwrap();
    assert!(!shard.manifest.sealed);
    let rows = Table::Pages.rows(&GEOMETRY) as usize;
    let query_rows = shard.manifest.pages_query_rows() as usize;
    assert!(query_rows < rows && query_rows.is_multiple_of(transparent_native::D));
    assert!(shard.manifest.occupancy.page_rows as usize <= query_rows);

    let profile = transparent_native::TableProfile::new(
        SCHEMA,
        GEOMETRY.name,
        "pages",
        rows as u64,
        GEOMETRY.page_row_bytes as u32,
    )
    .unwrap();
    let revision = revision_of(&f, tail);
    let path = format!("/v1/shards/{tail}/revisions/{revision}/query/pages");
    let binding = query_binding(&revision, "pages");
    let (_, upload) = profile.prepare(3).unwrap();
    let mut full = binding.to_vec();
    full.extend(upload);
    let prefix = full[..8 + transparent_native::request_len(query_rows)].to_vec();
    let (status, from_full) = post(&f.state, &path, full).await;
    assert_eq!(status, StatusCode::OK);
    let (status, from_prefix) = post(&f.state, &path, prefix).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(from_prefix, from_full);

    let (status, raw) = get(
        &f.state,
        &format!("/v1/shards/{tail}/revisions/{revision}/setup/pages/0"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let setup: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let public = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        setup["public_params"].as_str().unwrap(),
    )
    .unwrap();
    let (secret, upload) = profile.prepare_prefix(query_rows, 3).unwrap();
    let mut body = binding.to_vec();
    body.extend(upload);
    assert_eq!(body.len(), 8 + transparent_native::request_len(query_rows));
    let (status, response) = post(&f.state, &path, body).await;
    assert_eq!(status, StatusCode::OK);
    let published = shard.segment(Table::Pages, 0).unwrap().load().unwrap();
    assert_eq!(
        profile.decode(&secret, &public, &response[16..]).unwrap(),
        raw_row(&published, GEOMETRY.page_row_bytes, 3)
    );
}

/// Only a growing tail's pages take the shorter length, and only the one its
/// manifest fixes: sealed shards and the directory keep one length, so their
/// uploads stay alike.
#[tokio::test]
async fn only_a_growing_tails_pages_take_a_prefix_and_only_its_own() {
    let f = fixture();
    let short = |revision: &str, table: Table, rows: usize| {
        let mut body = padded_body(revision, table);
        body.truncate(8 + transparent_native::request_len(rows));
        body
    };
    let tail = SHARDS - 1;
    let tail_revision = revision_of(&f, tail);
    let query_rows = f.set.get(tail).unwrap().manifest.pages_query_rows() as usize;
    let sealed = revision_of(&f, 0);
    for (shard_id, revision, table, rows) in [
        (0, sealed.as_str(), Table::Pages, query_rows),
        (tail, tail_revision.as_str(), Table::Directory, query_rows),
        (
            tail,
            tail_revision.as_str(),
            Table::Pages,
            query_rows + transparent_native::D,
        ),
    ] {
        let (status, raw) = post(
            &f.state,
            &format!(
                "/v1/shards/{shard_id}/revisions/{revision}/query/{}",
                table.as_str()
            ),
            short(revision, table, rows),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{shard_id} {}",
            table.as_str()
        );
        assert!(String::from_utf8_lossy(&raw).contains("must be exactly"));
    }
}

/// A segment whose rows past the ones its queries select hold data is refused
/// on verification and on load, so no runtime is built or restored from it.
#[test]
fn a_segment_with_data_past_its_selected_rows_is_refused() {
    use sha2::Digest;
    use transparent_shard_server::shardset::SegmentSource;
    let dir = tempfile::tempdir().unwrap();
    let (rows, row_bytes) = (2 * transparent_native::D as u64, 16u32);
    let mut bytes = vec![0u8; rows as usize * row_bytes as usize];
    bytes[7] = 1;
    let source = |bytes: &[u8], zero_from_row| {
        let path = dir.path().join("pages.0.bin");
        std::fs::write(&path, bytes).unwrap();
        SegmentSource {
            path,
            rows,
            row_bytes,
            sha256: hex::encode(sha2::Sha256::digest(bytes)),
            zero_from_row,
        }
    };
    let half = transparent_native::D as u64;
    assert!(source(&bytes, half).verify().is_ok());
    assert!(source(&bytes, half).load().is_ok());
    bytes[half as usize * row_bytes as usize] = 1;
    assert!(source(&bytes, half).verify().is_err());
    assert!(source(&bytes, half).load().is_err());
    assert!(source(&bytes, rows).verify().is_ok());
}
