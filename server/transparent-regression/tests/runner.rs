//! Exercise the executable, actual HTTP paths, reopened stores and report files.
#[path = "../../transparent-shard-server/tests/common/mod.rs"]
mod common;
use common::*;
use sha2::{Digest, Sha256};
use transparent_regression::{reference, Case, Checkpoint, EventRecord, Fixture, SCHEMA};
use transparent_shard::layout::RECENT_8K;
use transparent_wallet::Anchor;
#[tokio::test(flavor = "multi_thread")]
async fn executable_checks_frozen_checkpoints_and_reports_publication_drift() {
    let all = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &all);
    let (base, requests) = serve_traced(dir.path()).await;
    let temp = tempfile::tempdir().unwrap();
    let records = all
        .iter()
        .flatten()
        .filter(|(s, _)| *s == script(3))
        .map(|(s, e)| EventRecord::new(s.as_slice(), e))
        .collect::<Vec<_>>();
    let heights = [FIRST + SPAN + 10, FIRST + SPAN + 35, FIRST + SPAN + 90];
    let headers = (FIRST - 1..=map.shards.last().unwrap().end_height)
        .map(|h| (h, hash_at(h).to_display_hex()))
        .collect();
    let mut allowed = std::collections::BTreeSet::from([
        "GET /v1/filters/shards".to_string(),
        "GET /v1/shards/init".to_string(),
    ]);
    for shard in map.shards.iter().filter(|s| s.start_height <= heights[2]) {
        allowed.insert(format!("GET /v1/filters/shards/{}/filter", shard.shard_id));
        let prefix = format!(
            "/v1/shards/{}/revisions/{}",
            shard.shard_id, shard.manifest_digest
        );
        allowed.insert(format!("GET {prefix}/manifest"));
        for (table, segments) in [
            ("directory", shard.directory_segments),
            ("pages", shard.page_segments),
        ] {
            allowed.insert(format!("POST {prefix}/query/{table}"));
            for segment in 0..segments {
                allowed.insert(format!("GET {prefix}/setup/{table}/{segment}"));
            }
        }
    }
    let fixture = Fixture {
        schema: SCHEMA.into(),
        source_sha: "local-fixture".into(),
        provenance: "synthetic chain independent of the service".into(),
        map_sha256: hex::encode(Sha256::digest(serde_json::to_vec(&map).unwrap())),
        publication_file_sha256: hex::encode(Sha256::digest(
            std::fs::read(dir.path().join("shards.json")).unwrap(),
        )),
        map,
        cutoff_height: FIRST + SPAN,
        accepted_headers: headers,
        cases: vec![Case {
            id: "paged".into(),
            profile: "reused-script".into(),
            scripts: vec![hex::encode(script(3).as_slice())],
            required_from: FIRST,
            checkpoints: heights
                .into_iter()
                .map(|h| Checkpoint {
                    anchor: Anchor {
                        height: h,
                        hash: hash_at(h).to_display_hex(),
                    },
                    expected: reference(&records, h).unwrap(),
                })
                .collect(),
        }],
    };
    let input = temp.path().join("fixture.json");
    std::fs::write(&input, serde_json::to_vec_pretty(&fixture).unwrap()).unwrap();
    let output = temp.path().join("results");
    let executable = env!("CARGO_BIN_EXE_transparent-regression");
    let make = |out: &std::path::Path, origin: &str| {
        let mut c = std::process::Command::new(executable);
        c.args([
            "--fixture",
            input.to_str().unwrap(),
            "--shard-url",
            origin,
            "--filter-url",
            origin,
            "--out-dir",
            out.to_str().unwrap(),
            "--source-sha",
            "test",
            "--request-timeout-secs",
            "10",
            "--case-timeout-secs",
            "90",
        ]);
        c
    };
    let mut command = make(&output, &base);
    let result = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        format!(
            "{}\n{}",
            String::from_utf8_lossy(&result.stderr),
            std::fs::read_to_string(output.join("report.json")).unwrap_or_default()
        )
    );
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("report.json")).unwrap()).unwrap();
    assert_eq!(report["passed"], true);
    assert_eq!(
        report["cases"][0]["checkpoints"].as_array().unwrap().len(),
        5
    );
    assert!(output.join("junit.xml").exists());
    assert!(output.join("paged/wallet.sqlite").exists());
    // Recover a transient service refusal, retaining the failed attempt. Then
    // prove a persistent refusal still fails instead of manufacturing a pass.
    for (label, failures, passes, attempts) in [
        ("transient", 1, true, 2),
        ("persistent", usize::MAX, false, 3),
    ] {
        let (flaky_base, _) = serve_traced_failures(dir.path(), failures).await;
        let out = temp.path().join(label);
        let mut command = make(&out, &flaky_base);
        let result = tokio::task::spawn_blocking(move || command.output().unwrap())
            .await
            .unwrap();
        assert_eq!(
            result.status.success(),
            passes,
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(out.join("report.json")).unwrap()).unwrap();
        assert_eq!(report["passed"], passes);
        assert_eq!(report["http_policy"]["maximum_attempts"], 3);
        assert_eq!(
            report["cases"][0]["http"]["recovered_requests"],
            usize::from(passes)
        );
        let raw = std::fs::read_to_string(out.join("case-paged.http.ndjson")).unwrap();
        let init: Vec<serde_json::Value> = raw
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .filter(|e: &serde_json::Value| e["stage"] == "init")
            .collect();
        let first_id = init[0]["request_id"].clone();
        let first: Vec<_> = init
            .iter()
            .filter(|e| e["request_id"] == first_id)
            .collect();
        assert_eq!(first.len(), attempts);
        assert_eq!(first[0]["status"], 503);
        assert_eq!(first.last().unwrap()["failed"], !passes);
    }
    // A republished tail is not drift. The service now serves the same set with
    // the tail at revision 1 under a new manifest digest, which is what a
    // continuously publishing origin does between any two fetches; the fixture
    // still pins its sealed entries and the run must still pass.
    let moved = tempfile::tempdir().unwrap();
    let republished = publish_with(moved.path(), &all, |_| &RECENT_8K, 1, "", hash_at);
    assert!(
        !republished.shards.last().unwrap().sealed,
        "the synthetic set must end in an unsealed tail"
    );
    assert_ne!(
        republished.shards.last().unwrap().manifest_digest,
        fixture.map.shards.last().unwrap().manifest_digest
    );
    assert_eq!(
        republished.shards[..republished.shards.len() - 1],
        fixture.map.shards[..fixture.map.shards.len() - 1],
        "republishing the tail must leave every sealed entry untouched"
    );
    let (moved_base, _) = serve_traced(moved.path()).await;
    let advanced = temp.path().join("advanced");
    let mut command = make(&advanced, &moved_base);
    let result = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "a republished tail must not read as drift: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(advanced.join("report.json")).unwrap()).unwrap();
    assert_eq!(report["passed"], true);
    assert_eq!(report["preflight"]["tail"]["revision"], 1u64);
    assert_eq!(report["sealed_prefix_shards"], SHARDS - 1);

    // Drift is a failed run with every required case accounted for. Each of
    // these is a change the tail's block cadence cannot account for.
    for (label, break_it) in [
        (
            "sealed",
            Box::new(|f: &mut Fixture| f.map.shards[0].manifest_digest = "00".repeat(32))
                as Box<dyn Fn(&mut Fixture)>,
        ),
        (
            "shrunk-tail",
            Box::new(|f: &mut Fixture| {
                f.map.shards.last_mut().unwrap().end_height += 1;
            }),
        ),
        (
            "lineage",
            Box::new(|f: &mut Fixture| f.map.range_envelope_version += 1),
        ),
    ] {
        let mut changed = fixture.clone();
        break_it(&mut changed);
        std::fs::write(&input, serde_json::to_vec_pretty(&changed).unwrap()).unwrap();
        let drift = temp.path().join(format!("drift-{label}"));
        let mut command = make(&drift, &base);
        let result = tokio::task::spawn_blocking(move || command.output().unwrap())
            .await
            .unwrap();
        assert!(!result.status.success(), "{label} should have failed");
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(drift.join("report.json")).unwrap()).unwrap();
        assert_eq!(report["passed"], false, "{label}");
        assert_eq!(report["cases"].as_array().unwrap().len(), 1, "{label}");
    }
    // A tail that names another parent cannot be expressed as a well formed
    // map at all: `check_shape` refuses the fixture before a request is sent,
    // so there is no evidence directory to preserve.
    let mut reparented = fixture.clone();
    reparented.map.shards.last_mut().unwrap().parent_block_hash = "11".repeat(32);
    std::fs::write(&input, serde_json::to_vec_pretty(&reparented).unwrap()).unwrap();
    let refused = temp.path().join("refused");
    let mut command = make(&refused, &base);
    let result = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(!result.status.success());
    assert!(!refused.exists(), "a malformed fixture must not open a run");
    let requests = requests.lock().unwrap();
    assert!(requests.iter().any(|r| r.contains("/query/pages")));
    for request in requests.iter() {
        assert!(
            allowed.contains(request),
            "unexpected plaintext route or future-shard request: {request}"
        );
    }
}

async fn serve_traced(
    dir: &std::path::Path,
) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    serve_traced_failures(dir, 0).await
}

async fn serve_traced_failures(
    dir: &std::path::Path,
    failures: usize,
) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
    use transparent_shard_server::{
        service::{router, ServiceConfig, ServiceState},
        shardset::{ShardSet, DEFAULT_RETAIN_REVISIONS},
    };
    let state = ServiceState::build(
        ShardSet::open(dir, DEFAULT_RETAIN_REVISIONS).unwrap(),
        ServiceConfig::default(),
    )
    .unwrap();
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let capture = requests.clone();
    let remaining = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(failures));
    let app = router(state).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let capture = capture.clone();
            let remaining = remaining.clone();
            async move {
                if request.uri().path() == "/v1/shards/init"
                    && remaining
                        .fetch_update(
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::SeqCst,
                            |n| n.checked_sub(1),
                        )
                        .is_ok()
                {
                    return axum::response::IntoResponse::into_response((
                        axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        "test unavailable",
                    ));
                }
                capture
                    .lock()
                    .unwrap()
                    .push(format!("{} {}", request.method(), request.uri()));
                next.run(request).await
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), requests)
}

#[test]
fn frozen_mainnet_fixture_has_exact_prefixes_and_required_wallet_profiles() {
    let bytes = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/mainnet.json"
    ))
    .unwrap();
    let f: Fixture = serde_json::from_slice(&bytes).unwrap();
    transparent_regression::validate(&f).unwrap();
    assert_eq!(
        f.map_sha256,
        hex::encode(Sha256::digest(serde_json::to_vec(&f.map).unwrap()))
    );
    for id in [
        "unused-p2pkh",
        "unused-p2sh",
        "small-active",
        "zero-balance",
        "old-receive-recent-spend",
        "offline-receive-spend",
        "active-p2sh",
        "reused-pages",
        "multi-script-self-transfer",
        "recent-birthday",
        "coinbase",
    ] {
        assert!(
            f.cases.iter().any(|c| c.id == id),
            "missing required wallet profile {id}"
        );
    }
    assert!(
        f.cases.iter().any(|c| c.checkpoints.windows(2).any(|pair| f
            .map
            .shard_for_height(pair[0].anchor.height)
            .unwrap()
            .shard_id
            == f.map
                .shard_for_height(pair[1].anchor.height)
                .unwrap()
                .shard_id)),
        "fixture must exercise overlapping-shard continuation"
    );
    assert!(
        f.cases.iter().flat_map(|c| &c.checkpoints).any(|c| {
            let s = f.map.shard_for_height(c.anchor.height).unwrap();
            s.start_height < c.anchor.height && c.anchor.height < s.end_height
        }),
        "fixture must include arbitrary interior anchors"
    );
}
