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
use transparent_shard::layout::{DIRECTORY_ROWS, DIRECTORY_ROW_BYTES, PAGE_ROWS, PAGE_ROW_BYTES};
use transparent_shard::manifest::{
    ManifestLayout, ManifestOccupancy, ManifestSeal, ShardManifest, TableGeometry, SCHEMA,
};
use transparent_shard_server::service::{router, ServiceState};
use transparent_shard_server::shardset::{ShardSet, Table};

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
            &events,
        )
        .expect("build");

        let manifest = ShardManifest {
            schema: SCHEMA.to_string(),
            profile: transparent_filter::RANGE_PROFILE.to_string(),
            network: transparent_filter::NETWORK.to_string(),
            genesis_hash: GENESIS.to_string(),
            shard_id,
            start_height: start,
            end_height: end,
            parent_block_hash: hash_at(start - 1).to_display_hex(),
            terminal_block_hash: terminal.to_display_hex(),
            parent_manifest_digest: parent_digest.clone(),
            sealed: shard_id + 1 < SHARDS,
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
                directory_choices: transparent_shard::build::DIRECTORY_CHOICES as u32,
            },
            filter_hash: filter_hash(built.filter.as_slice()).to_display_hex(),
            directory: TableGeometry {
                rows: DIRECTORY_ROWS as u64,
                row_bytes: DIRECTORY_ROW_BYTES as u32,
                sha256: hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&built.directory)),
            },
            pages: TableGeometry {
                rows: PAGE_ROWS as u64,
                row_bytes: PAGE_ROW_BYTES as u32,
                sha256: hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&built.pages)),
            },
            occupancy: ManifestOccupancy {
                scripts: built.scripts,
                page_rows: built.page_rows,
                events: built.events,
                blocks: SPAN,
                txids: 0,
                excluded_scripts: built.excluded_scripts,
            },
        };

        let digest = manifest.digest();
        let shard_dir = dir.join(&digest);
        std::fs::create_dir_all(&shard_dir).unwrap();
        std::fs::write(shard_dir.join("manifest.json"), manifest.canonical_bytes()).unwrap();
        std::fs::write(shard_dir.join("filter.bin"), built.filter.as_slice()).unwrap();
        std::fs::write(shard_dir.join("directory.bin"), &built.directory).unwrap();
        std::fs::write(shard_dir.join("pages.bin"), &built.pages).unwrap();

        entries.push(ShardMapEntry {
            shard_id,
            start_height: start,
            end_height: end,
            parent_block_hash: manifest.parent_block_hash.clone(),
            terminal_block_hash: manifest.terminal_block_hash.clone(),
            filter_hash: manifest.filter_hash.clone(),
            scripts: built.scripts,
            page_rows: built.page_rows,
            txids: 0,
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
        seal: transparent_filter::SealParameters {
            max_scripts: 8_192,
            max_page_rows: 2_048,
            max_txids: 0,
        },
        shards: entries,
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
    let set = ShardSet::open(dir.path()).expect("load");
    let served = ShardSet::open(dir.path()).expect("load");
    Fixture {
        state: ServiceState::build(served).expect("state"),
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

/// Builds a client for one table and retrieves `row` from `shard_id`.
async fn retrieve(f: &Fixture, shard_id: u64, table: Table, row: usize) -> Vec<u8> {
    let (status, raw) = get(&f.state, "/v1/shards/init").await;
    assert_eq!(status, StatusCode::OK);
    let init: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let scheme_key = match table {
        Table::Directory => "directory_scheme",
        Table::Pages => "pages_scheme",
    };
    let scheme: ipir_sp::YpirSchemeParams =
        serde_json::from_value(init[scheme_key].clone()).unwrap();

    let (status, raw) = get(
        &f.state,
        &format!("/v1/shards/{shard_id}/setup/{}", table.as_str()),
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
    let (rlwe, expected) =
        ipir_sp::params_for_simplepir(table.rows(), (table.row_bytes() as u64) * 8).unwrap();
    assert_eq!(
        scheme, expected,
        "the served scheme must be the one a client re-derives"
    );

    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&table.setup_seed().to_le_bytes());
    let client = ipir_sp::IPIRClient::new(&rlwe, &expected);
    let setup_matrix = client.generate_public_query_setup_simplepir_from_seed(seed);
    let blocks = expected.db_cols / rlwe.d;
    let published =
        ipir_sp::modulus_switch::recover_published_c1(&public_params, rlwe.d, blocks, rlwe.q);

    let (query, packing_keys, query_seed) =
        client.generate_fresh_query_simplepir(&setup_matrix, row);
    let mut body = shard_id.to_le_bytes().to_vec();
    body.extend(ipir_sp::serialize::serialize_packing_keys(&rlwe, &packing_keys).unwrap());
    body.extend(query.to_switched_bytes(rlwe.q, expected.query_bits));

    let (status, response) = post(
        &f.state,
        &format!("/v1/shards/{shard_id}/query/{}", table.as_str()),
        body,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "query failed");
    assert_eq!(&response[..8], &shard_id.to_le_bytes());

    let decoded = client.decode_response_simplepir(query_seed, &published, &response[16..]);
    decoded[..table.row_bytes() as usize].to_vec()
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
            assert_eq!(
                decoded,
                raw_row(shard.table(table), table.row_bytes() as usize, row),
                "shard {shard_id} {} row {row}",
                table.as_str()
            );
        }
    }
}

/// The shard id is in the query prefix precisely so a query cannot be answered
/// by the wrong shard. Without it a client would decode plausible nonsense.
#[tokio::test]
async fn a_query_built_for_one_shard_is_refused_by_another() {
    let f = fixture();
    let mut body = 0u64.to_le_bytes().to_vec();
    let (rlwe, scheme) = ipir_sp::params_for_simplepir(
        Table::Directory.rows(),
        (Table::Directory.row_bytes() as u64) * 8,
    )
    .unwrap();
    body.resize(
        8 + ipir_sp::serialize::serialized_packing_keys_len(&rlwe)
            + (scheme.db_rows * scheme.query_bits).div_ceil(8),
        0,
    );
    let (status, _) = post(&f.state, "/v1/shards/1/query/directory", body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_truncated_query_is_refused_rather_than_padded() {
    let f = fixture();
    let body = 0u64.to_le_bytes().to_vec();
    let (status, _) = post(&f.state, "/v1/shards/0/query/directory", body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn unknown_shards_and_tables_are_refused() {
    let f = fixture();
    let (status, _) = get(&f.state, "/v1/shards/99/setup/directory").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = get(&f.state, "/v1/shards/0/setup/nonsense").await;
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
        let (_, raw) = get(&f.state, &format!("/v1/shards/{shard_id}/setup/directory")).await;
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
    assert!(ShardSet::open(dir.path()).is_err());
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
        let mut bytes = std::fs::read(path.join("directory.bin")).unwrap();
        bytes[0] ^= 0xff;
        std::fs::write(path.join("directory.bin"), bytes).unwrap();
        break;
    }
    assert!(ShardSet::open(dir.path()).is_err());
}
