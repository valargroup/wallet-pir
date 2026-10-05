//! Serving the public half of a published shard set.
//!
//! The failure this guards against is not a crash. A range filter that does not
//! match the digest its map publishes still parses, still answers queries, and
//! answers them *wrongly* — a wallet testing its scripts against it would
//! under-match and skip history it should have retrieved, then advance its
//! coverage over the gap. That is a silent wrong answer, so the service refuses
//! to start rather than serve it.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::path::Path;
use tower::ServiceExt;
use transparent_filter::digest::filter_hash;
use transparent_filter::wire::{SealParameters, ShardMap, ShardMapEntry};
use transparent_filter_server::shard_filters::ShardFilters;

const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;

fn entry(shard_id: u64, digest: &str, filter: &[u8]) -> ShardMapEntry {
    ShardMapEntry {
        shard_id,
        geometry: "recent-8k".to_string(),
        start_height: 100 + shard_id * 10,
        end_height: 109 + shard_id * 10,
        // The map requires a chain: each shard's parent is the block before its
        // range, which is its predecessor's terminal.
        parent_block_hash: format!("{:064x}", shard_id),
        terminal_block_hash: format!("{:064x}", shard_id + 1),
        filter_hash: filter_hash(filter).to_display_hex(),
        scripts: 1,
        page_rows: 1,
        txids: 0,
        directory_segments: 1,
        page_segments: 1,
        txid_segments: None,
        manifest_digest: digest.to_string(),
        revision: 0,
        sealed: true,
    }
}

/// Writes a two-shard set. `corrupt` flips a byte of shard 1's filter *after*
/// its digest is recorded, which is exactly the on-disk damage the load is
/// meant to catch.
fn publish(dir: &Path, corrupt: bool) {
    let mut entries = Vec::new();
    for shard_id in 0..2u64 {
        let digest = format!("{:064x}", shard_id + 1);
        let filter = vec![shard_id as u8 + 7; 64];
        let shard_dir = dir.join(&digest);
        std::fs::create_dir_all(&shard_dir).unwrap();
        entries.push(entry(shard_id, &digest, &filter));
        let mut written = filter;
        if corrupt && shard_id == 1 {
            written[0] ^= 0xff;
        }
        std::fs::write(shard_dir.join("filter.bin"), &written).unwrap();
    }
    let map = ShardMap {
        genesis_hash: GENESIS.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: 100,
        seal: std::collections::BTreeMap::from([(
            "recent-8k".to_string(),
            SealParameters {
                max_scripts: 8,
                max_page_rows: 8,
                max_txids: 0,
            },
        )]),
        shards: entries,
    };
    std::fs::write(
        dir.join("shards.json"),
        serde_json::to_vec_pretty(&map).unwrap(),
    )
    .unwrap();
}

#[test]
fn a_filter_that_does_not_match_its_published_digest_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    publish(dir.path(), true);
    let text = match ShardFilters::open(dir.path()) {
        Ok(_) => panic!("a corrupt filter must not load"),
        Err(error) => error.to_string(),
    };
    assert!(text.contains("shard 1"), "{text}");
    assert!(text.contains("digest"), "{text}");
}

#[test]
fn a_published_set_loads_and_keeps_every_filter() {
    let dir = tempfile::tempdir().unwrap();
    publish(dir.path(), false);
    let filters = ShardFilters::open(dir.path()).expect("clean set loads");
    assert_eq!(filters.shard_count(), 2);
    assert_eq!(filters.covered_through(), Some(119));
    assert_eq!(filters.filter(0).unwrap(), vec![7u8; 64]);
    assert_eq!(filters.filter(1).unwrap(), vec![8u8; 64]);
    assert!(filters.filter(2).is_none(), "the set has no shard 2");
    // Stable across calls, so a wallet can pin it.
    assert_eq!(filters.map_digest(), filters.map_digest());
    assert_eq!(filters.map_digest().len(), 64);
}

/// A missing filter file is a partial set, and a partial set must not be
/// served: a wallet would take the shards it could get and advance coverage
/// over the range whose filter never arrived.
#[test]
fn a_set_missing_a_filter_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    publish(dir.path(), false);
    std::fs::remove_file(dir.path().join(format!("{:064x}", 2u64)).join("filter.bin")).unwrap();
    assert!(ShardFilters::open(dir.path()).is_err());
}

#[tokio::test]
async fn the_routes_serve_the_map_and_each_filter() {
    let dir = tempfile::tempdir().unwrap();
    publish(dir.path(), false);
    let filters = ShardFilters::open(dir.path()).unwrap();
    let expected_digest = filters.map_digest();

    let store = transparent_filter_server::store::FilterStore::open(
        dir.path().join("filter-store"),
        transparent_filter::PROFILE,
        GENESIS,
        100,
    )
    .unwrap();
    let state =
        transparent_filter_server::service::ServiceState::new(store).with_shard_filters(filters);
    let app = transparent_filter_server::service::router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/filters/shards")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    // The digest is published as a header so a wallet can pin the revision it
    // synced against without hashing the body itself.
    assert_eq!(
        response
            .headers()
            .get("x-shard-map-sha256")
            .unwrap()
            .to_str()
            .unwrap(),
        expected_digest
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let served: ShardMap = serde_json::from_slice(&body).unwrap();
    assert_eq!(served.shards.len(), 2);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/filters/shards/1/filter")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(bytes.to_vec(), vec![8u8; 64]);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/filters/shards/9/filter")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

/// Without a set the routes must say so rather than 500 or answer emptily: the
/// service is expected to run with no published set at all.
#[tokio::test]
async fn without_a_set_the_shard_routes_are_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let store = transparent_filter_server::store::FilterStore::open(
        dir.path(),
        transparent_filter::PROFILE,
        GENESIS,
        100,
    )
    .unwrap();
    let app = transparent_filter_server::service::router(
        transparent_filter_server::service::ServiceState::new(store),
    );
    for uri in ["/v1/filters/shards", "/v1/filters/shards/0/filter"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
    }
}
