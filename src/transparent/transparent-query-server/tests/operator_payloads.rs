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
//! `tools/scripts/check-jq-contracts.sh` runs the *script's own* jq programs
//! against them. Neither side transcribes the other, so neither can go stale
//! alone.
//!
//! A change to an operator payload therefore shows up as a diff in
//! `tools/transparent/fixtures/`. That is the point: the operator contract should be
//! something a reviewer sees change. It also catches a rename arriving through
//! a dependency bump — the native `NativeScheme` is serialized wholesale into
//! `geometries[]`, and nothing else in the tree would notice.
//!
//! Accept a deliberate change with:
//!
//! ```text
//! UPDATE_OPS_FIXTURES=1 cargo test -p transparent-query-server --test operator_payloads
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
#![cfg(feature = "transparent-history")]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::path::{Path, PathBuf};
use tower::ServiceExt;
#[path = "support/operator_set.rs"]
mod operator_set;
use operator_set::publish;
use transparent_query_server::history::service::{router, ServiceConfig, ServiceState};
use transparent_shared::history::shardset::{ShardSet, DEFAULT_RETAIN_REVISIONS};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tools/transparent/fixtures/transparent-shard")
        .canonicalize()
        .expect("operator payload fixtures must exist in the repository")
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
            "{}: {error}\nrun: UPDATE_OPS_FIXTURES=1 cargo test -p transparent-query-server \
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
         UPDATE_OPS_FIXTURES=1 cargo test -p transparent-query-server --test operator_payloads",
        path.display()
    );
}

/// Serves the operator payloads and pins them against `tools/transparent/fixtures/`.
#[tokio::test(flavor = "multi_thread")]
async fn the_operator_payloads_match_what_ops_parses() {
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path());
    let set = ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).expect("load");
    let state = ServiceState::build(set, ServiceConfig::default()).expect("state");

    let (status, body) = get(&state, "/v2/shards/init").await;
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
