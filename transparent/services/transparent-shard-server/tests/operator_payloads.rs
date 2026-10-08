//! The operator payloads the deploy script reads, pinned as golden files.
//!
//! Two bugs shipped in one evening because nothing connected these documents to
//! the script that parses them. `.directory_scheme` moved into `geometries[]`
//! when a set became able to mix geometries; `verify_public` was migrated and
//! `verify` was not, so the deploy would have failed *after* activating. And a
//! jq program stopped compiling, failing a deploy whose every substantive check
//! had already passed.
//!
//! Neither was visible to `make check`, because CI runs the script only in
//! `validate` mode — which reaches three of its twelve jq programs, none of
//! which read a served payload.
//!
//! # Why golden files rather than assertions
//!
//! A test asserting a hand-written list of field paths drifts exactly the way
//! `verify()` drifted: two independent lists, and nothing to keep them
//! together. So this writes the payloads the server actually serializes, and
//! `ops/scripts/check-jq-contracts.sh` runs the *script's own* jq programs
//! against them. Neither side transcribes the other, so neither can go stale
//! alone.
//!
//! A change to an operator payload therefore shows up as a diff in
//! `transparent/ops/fixtures/`. That is the point: the operator contract should be
//! something a reviewer sees change. It also catches a rename arriving through
//! a dependency bump — the native `NativeScheme` is serialized wholesale into
//! `geometries[]`, and nothing else in the tree would notice.
//!
//! Accept a deliberate change with:
//!
//! ```text
//! UPDATE_OPS_FIXTURES=1 cargo test -p transparent-shard-server --test operator_payloads
//! ```
//!
//! The set published here is deliberately two-tiered. A single-geometry set
//! makes `[.geometries[].name] | sort | join(",")` — and the map-side
//! `unique | join(",")` it is compared against — pass trivially, which is the
//! comparison that catches a worker serving a different set than the one
//! shipped.
//!
//! Cheap for a reason worth knowing: `init` reads only the prepared parameters
//! and `health` only set counters and cache totals, so neither builds a
//! `TableRuntime`. The cost here is publishing two shards and hashing them.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
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
use transparent_shard_server::service::{router, ServiceConfig, ServiceState};
use transparent_shard_server::shardset::{ShardSet, DEFAULT_RETAIN_REVISIONS};

const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;
const FIRST: u64 = 3_428_143;
const SPAN: u64 = 40;

/// The two cheapest registry geometries. A wider pair would say nothing more
/// about the payload shape and would cost megabytes per table to hash.
const GEOMETRIES: [&Geometry; 2] = [&RECENT_4K, &RECENT_8K];

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../ops/fixtures/transparent-shard")
        .canonicalize()
        .unwrap_or_else(|_| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ops/fixtures/transparent-shard")
        })
}

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

/// Publishes one shard and returns its map entry.
fn write_shard(
    dir: &Path,
    shard_id: u64,
    geometry: &'static Geometry,
    sealed: bool,
    parent_manifest_digest: &str,
) -> ShardMapEntry {
    let start = FIRST + shard_id * SPAN;
    let end = start + SPAN - 1;
    let content = events(start, end);
    let built = build_shard(
        shard_id,
        start,
        end,
        genesis(),
        hash_at(end),
        transparent_filter::RANGE_PROFILE,
        geometry,
        &content,
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

/// Publishes a two-geometry set: an earlier sealed shard, then an unsealed tail.
///
/// `check_shape` refuses a sealed shard after an unsealed one, so the tail is
/// last by construction — which is also what production looks like.
fn publish(dir: &Path) -> ShardMap {
    let mut entries = Vec::new();
    let mut parent = String::new();
    for (index, geometry) in GEOMETRIES.iter().enumerate() {
        let shard_id = index as u64;
        let sealed = index + 1 < GEOMETRIES.len();
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
    map.check_shape()
        .expect("the published map must be well formed");
    std::fs::write(
        dir.join("shards.json"),
        serde_json::to_vec_pretty(&map).unwrap(),
    )
    .unwrap();
    map
}

async fn get(state: &ServiceState, path: &str) -> (StatusCode, Vec<u8>) {
    let response = router(state.clone())
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .expect("router");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 4 << 20)
        .await
        .expect("body")
        .to_vec();
    (status, body)
}

/// `payload` with the process identity (docs/serving-contract.md) checked for
/// shape and replaced by fixed values, which differ on every build and start.
fn process_identity_zeroed(payload: &serde_json::Value) -> serde_json::Value {
    let hex = |field: &str, len: usize| {
        let value = payload[field]
            .as_str()
            .unwrap_or_else(|| panic!("{field} is present"));
        assert_eq!(value.len(), len, "{field}");
        assert!(
            value.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "{field}"
        );
    };
    hex("binary_sha256", 64);
    hex("incarnation", 32);
    assert!(payload["started_unix"].as_u64().is_some_and(|t| t > 0));
    let mut fixed = payload.clone();
    fixed["binary_sha256"] = serde_json::json!("0".repeat(64));
    fixed["incarnation"] = serde_json::json!("0".repeat(32));
    fixed["started_unix"] = serde_json::json!(0);
    fixed
}

/// Writes `name` if `UPDATE_OPS_FIXTURES` is set, and otherwise compares.
fn golden(name: &str, actual: &serde_json::Value) {
    let path = fixture_dir().join(name);
    let rendered = format!("{}\n", serde_json::to_string_pretty(actual).unwrap());
    if std::env::var_os("UPDATE_OPS_FIXTURES").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &rendered).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!(
            "{}: {error}\nrun: UPDATE_OPS_FIXTURES=1 cargo test -p transparent-shard-server \
             --test operator_payloads",
            path.display()
        )
    });
    assert_eq!(
        rendered,
        expected,
        "{} is stale.\n\nops/scripts/check-jq-contracts.sh runs the deploy script's own jq \
         programs against this file, so a payload change here is a change to what the deploy \
         reads. Confirm the script still reads what it needs, then accept with:\n  \
         UPDATE_OPS_FIXTURES=1 cargo test -p transparent-shard-server --test operator_payloads",
        path.display()
    );
}

/// Serves the operator payloads and pins them against `transparent/ops/fixtures/`.
#[tokio::test(flavor = "multi_thread")]
async fn the_operator_payloads_match_what_ops_parses() {
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    let set = ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).expect("load");
    let state = ServiceState::build(set, ServiceConfig::default()).expect("state");

    let (status, body) = get(&state, "/v1/shards/init").await;
    assert_eq!(status, StatusCode::OK);
    let init: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let (status, body) = get(&state, "/v1/health").await;
    assert_eq!(status, StatusCode::OK);
    let health: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // The two `geometries` are different shapes and are easy to conflate: init
    // publishes objects a wallet validates, health publishes bare names for an
    // operator. Both are inline `json!` literals on the health side, so nothing
    // in the type system holds either still.
    assert!(
        init["geometries"][0].is_object(),
        "init geometries are objects"
    );
    assert!(
        health["geometries"][0].is_string(),
        "health geometries are bare names"
    );
    assert_eq!(init["shards"], serde_json::json!(map.shards.len()));

    // The deploy reads /v1/ready over the VPC to assert which map and
    // assignment a worker runs under, so its body is a fixture too. In
    // loaded-only mode the worker is ready as soon as the set is loaded.
    let (status, body) = get(&state, "/v1/ready").await;
    assert_eq!(status, StatusCode::OK);
    let ready: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(ready["ready"], serde_json::json!(true));
    assert_eq!(ready["mode"], serde_json::json!("loaded-only"));
    assert_eq!(ready["map_sha256"], health["map_sha256"]);

    let (status, body) = get(&state, "/metrics").await;
    assert_eq!(status, StatusCode::OK);
    let rendered = String::from_utf8(body).expect("metrics are utf-8");
    assert!(rendered.contains("transparent_shard_shards{map_sha256="));
    assert!(rendered.contains("transparent_shard_build_seconds_bucket{"));

    golden("init.json", &init);
    golden("health.json", &process_identity_zeroed(&health));
    // The prewarm timing is a measurement, not a contract.
    let mut ready_fixture = process_identity_zeroed(&ready);
    ready_fixture["prewarm_seconds"] = serde_json::json!(0.0);
    golden("ready.json", &ready_fixture);
    golden(
        "shards.json",
        &serde_json::from_slice(&std::fs::read(dir.path().join("shards.json")).unwrap()).unwrap(),
    );
}

#[test]
fn verify_only_stdout_is_machine_readable_and_checks_cache_capacity() {
    let dir = tempfile::tempdir().unwrap();
    publish(dir.path());
    let cache = tempfile::tempdir().unwrap();
    let run = |limit: &str| {
        std::process::Command::new(env!("CARGO_BIN_EXE_transparent-shard-server"))
            .arg("--shard-dir")
            .arg(dir.path())
            .arg("--verify-only")
            .arg("--runtime-cache-dir")
            .arg(cache.path())
            .args(["--runtime-cache-max-bytes", limit])
            .output()
            .unwrap()
    };
    let output = run("17179869184");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout is one JSON report");
    assert_eq!(report["verified"], true);
    assert!(report["runtime_cache"]["missing_bytes"].as_u64().unwrap() > 0);
    assert!(
        !run("1").status.success(),
        "insufficient cache capacity fails before activation"
    );
}
