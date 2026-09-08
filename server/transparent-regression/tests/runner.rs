//! Exercise the executable, actual HTTP paths, reopened stores and report files.
#[path = "../../transparent-shard-server/tests/common/mod.rs"]
mod common;
use common::*;
use sha2::{Digest, Sha256};
use transparent_regression::{reference, Case, Checkpoint, EventRecord, Fixture, SCHEMA};
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
    let make = |out: &std::path::Path| {
        let mut c = std::process::Command::new(executable);
        c.args([
            "--fixture",
            input.to_str().unwrap(),
            "--shard-url",
            &base,
            "--filter-url",
            &base,
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
    let mut command = make(&output);
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
    // Drift is a failed run with every required case accounted for.
    let mut changed = fixture;
    changed.map_sha256 = "00".repeat(32);
    std::fs::write(&input, serde_json::to_vec_pretty(&changed).unwrap()).unwrap();
    let drift = temp.path().join("drift");
    let mut command = make(&drift);
    let result = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(!result.status.success());
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(drift.join("report.json")).unwrap()).unwrap();
    assert_eq!(report["passed"], false);
    assert_eq!(report["cases"].as_array().unwrap().len(), 1);
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
    let app = router(state).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let capture = capture.clone();
            async move {
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
