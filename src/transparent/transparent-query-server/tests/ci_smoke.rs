//! A real encrypted HTTP query matches nonzero public rows before and after restart.
#![cfg(feature = "transparent-history")]
use zakura_pir_client::transparent::native::TableClientExt as _;

use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_protocol::transparent::filter as transparent_filter;
use zakura_pir_publisher::transparent as transparent_shard;
use zakura_pir_query_server::transparent::native as transparent_native;

use axum::http::StatusCode;
use std::path::Path;
#[path = "support/revision.rs"]
mod revision;

use transparent_filter::{BlockHash, ScriptBytes, ShardMap};
use transparent_query_server::history::service::{router, ServiceConfig, ServiceState};
use transparent_shard::layout::{Geometry, DECOUPLED_2K_8K};
use transparent_shard::manifest::query_binding;
use transparent_shared::history::shardset::{ShardSet, Table, TableRef, DEFAULT_RETAIN_REVISIONS};

/// The geometry this fixture publishes at.
const GEOMETRY: Geometry = DECOUPLED_2K_8K;

const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;
const SHARDS: u64 = 1;
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

        let sealed = shard_id + 1 < SHARDS;
        let (_, entry) = revision::write(
            dir,
            &revision::Revision {
                shard_id,
                start,
                end,
                geometry: &GEOMETRY,
                sealed,
                revision: 0,
                supersedes: "",
                parent_digest: &parent_digest,
                children: 1,
                genesis: genesis(),
                hash: &hash_at,
            },
            &events,
        );
        let digest = entry.manifest_digest.clone();
        entries.push(entry);
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

struct Listening {
    origin: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Listening {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn serve(state: ServiceState) -> Listening {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", socket.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(socket, router(state)).await.unwrap() });
    Listening { origin, task }
}
fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .unwrap()
}
async fn get(origin: &str, path: &str) -> (StatusCode, Vec<u8>) {
    let response = http().get(format!("{origin}{path}")).send().await.unwrap();
    (response.status(), response.bytes().await.unwrap().to_vec())
}
async fn post(origin: &str, path: &str, body: Vec<u8>) -> (StatusCode, Vec<u8>) {
    let response = http()
        .post(format!("{origin}{path}"))
        .body(body)
        .send()
        .await
        .unwrap();
    (response.status(), response.bytes().await.unwrap().to_vec())
}
async fn retrieve(origin: &str, revision: &str, row: usize) -> Vec<u8> {
    let shard_id = 0;
    let table = TableRef::Directory;
    let (status, raw) = get(origin, "/v2/shards/init").await;
    assert_eq!(status, StatusCode::OK);
    let init: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let geometries = init["geometries"].as_array().unwrap();
    let published = geometries
        .iter()
        .find(|entry| entry["name"] == GEOMETRY.name)
        .expect("the fixture's geometry is published");
    let scheme_key = match table.kind() {
        Table::Directory => "directory_scheme",
        Table::Pages => "pages_scheme",
        Table::TxDirectory => unreachable!("history-only fixture"),
    };
    let scheme: transparent_native::NativeScheme =
        serde_json::from_value(published[scheme_key].clone()).unwrap();
    assert!(
        published.get(format!("{scheme_key}_dq44")).is_none(),
        "an 8-bit table publishes one scheme"
    );

    let (status, raw) = get(
        origin,
        &format!(
            "/v2/shards/{shard_id}/revisions/{revision}/setup/{}/0",
            table.name()
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
        table.kind().as_str(),
        table.kind().rows(&GEOMETRY),
        table.kind().row_bytes(&GEOMETRY),
    )
    .unwrap();
    assert_eq!(
        scheme, profile.scheme,
        "the served scheme must be the one a client re-derives"
    );
    assert_eq!(profile.dithered_scheme, None);
    assert_eq!(scheme.query_bits, 28);
    assert_eq!(public_params.len(), profile.scheme.public_bytes);

    let (secret, upload) = profile.prepare(row).unwrap();
    assert_eq!(upload.len(), scheme.request_bytes);
    let binding = query_binding(revision, &table.name());
    let mut body = binding.to_vec();
    body.extend(upload);

    let (status, response) = post(
        origin,
        &format!(
            "/v2/shards/{shard_id}/revisions/{revision}/query/{}",
            table.name()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypted_http_matches_published_bytes_after_cold_start_and_cached_restart() {
    use transparent_shared::runtime::disk::DiskCache;
    use transparent_shared::runtime::metrics::Metrics;
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        let root = tempfile::tempdir().unwrap();
        let map = publish(root.path());
        let revision = &map.shards[0].manifest_digest;
        let set = ShardSet::open(root.path(), DEFAULT_RETAIN_REVISIONS).unwrap();
        let bytes = set
            .get(0)
            .unwrap()
            .segment(TableRef::Directory, 0)
            .unwrap()
            .load()
            .unwrap();
        let row_bytes = GEOMETRY.directory_row_bytes;
        let row = bytes
            .chunks_exact(row_bytes)
            .position(|row| row.iter().any(|b| *b != 0))
            .unwrap();
        let expected = bytes[row * row_bytes..(row + 1) * row_bytes].to_vec();
        let cache_root = tempfile::tempdir().unwrap();
        for attempt in 0..2 {
            let restored = attempt == 1;
            let set = ShardSet::open(root.path(), DEFAULT_RETAIN_REVISIONS).unwrap();
            let disk = DiskCache::new(cache_root.path().into(), 1 << 30).unwrap();
            let state =
                ServiceState::build_with_disk(set, ServiceConfig::default(), Some(disk)).unwrap();
            let metrics = state.metrics().clone();
            let mut server = serve(state).await;
            let path = format!("/v2/shards/0/revisions/{revision}/query/directory");
            if !restored {
                assert_eq!(
                    post(&server.origin, &path, vec![0; 8]).await.0,
                    StatusCode::BAD_REQUEST
                );
                assert_eq!(
                    Metrics::get(&metrics.builds),
                    0,
                    "malformed input cannot prepare a runtime"
                );
            }
            assert_eq!(retrieve(&server.origin, revision, row).await, expected);
            assert_eq!(Metrics::get(&metrics.disk_hits), u64::from(restored));
            assert_eq!(Metrics::get(&metrics.builds), u64::from(!restored));
            while Metrics::get(&metrics.disk_save_pending) != 0 {
                tokio::task::yield_now().await;
            }
            assert_eq!(Metrics::get(&metrics.disk_write_failures), 0);
            server.task.abort();
            let _ = (&mut server.task).await;
        }
    })
    .await
    .expect("encrypted HTTP smoke exceeded its deadline");
}
