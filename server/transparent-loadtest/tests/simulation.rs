//! Executes the shipped binary and child protocol against real HTTP shard
//! fixtures. Deadlines and shutdown are tested at the OS process boundary.
#[path = "../../transparent-shard-server/tests/common/mod.rs"]
mod common;

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use transparent_filter::ShardMap;
use transparent_shard::layout::RECENT_4K;
use transparent_shard_server::service::{router, ServiceConfig, ServiceState};
use transparent_shard_server::shardset::{ShardSet, DEFAULT_RETAIN_REVISIONS};

fn sample(path: &Path, map: &ShardMap, tags: &[u32], wrong: bool) {
    let chain = common::chain();
    let clients: Vec<_> = tags.iter().map(|tag| {
        let mut events: Vec<_> = chain.iter().flatten().filter(|(s,_)|*s==common::script(*tag)).map(|(_,e)|*e).collect();
        events.sort_by_key(|e|e.sort_key()); events.dedup();
        let mut digest=Sha256::new(); for event in &events {digest.update(event.to_bytes());}
        json!({"class":"test","scripts":[hex::encode(common::script(*tag).as_slice())],"required_from":common::FIRST,"expected_digest":if wrong {"incorrect".into()} else {hex::encode(digest.finalize())},"journal_events":events.len()})
    }).collect();
    let tip = map.shards.last().unwrap();
    fs::write(path,serde_json::to_vec(&json!({"genesis_hash":map.genesis_hash,"start_height":map.start_height,"cutoff_height":map.start_height,"anchor_height":tip.end_height,"anchor_hash":tip.terminal_block_hash,"clients":clients})).unwrap()).unwrap();
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    SlowFilter,
    RefuseFilter,
    RetryQuery,
    Drift,
    RetryMap,
    RetryPageSetup,
}
async fn service(dir: &Path, fault: Fault) -> (String, ServiceState) {
    let set = ShardSet::open(&dir.join("shards"), DEFAULT_RETAIN_REVISIONS).unwrap();
    let state = ServiceState::build(set, ServiceConfig::default()).unwrap();
    let map_calls = Arc::new(AtomicUsize::new(0));
    let app = router(state.clone()).layer(middleware::from_fn(
        move |request: axum::extract::Request, next: Next| {
            let calls = map_calls.clone();
            async move {
                let path = request.uri().path();
                if path.ends_with("/filter") {
                    match fault {
                        Fault::SlowFilter => tokio::time::sleep(Duration::from_secs(30)).await,
                        Fault::RefuseFilter => {
                            return (StatusCode::SERVICE_UNAVAILABLE, "test refusal")
                                .into_response()
                        }
                        _ => {}
                    }
                }
                if matches!(fault, Fault::RetryPageSetup)
                    && path.contains("/setup/pages")
                    && calls.fetch_add(1, Ordering::SeqCst) == 0
                {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        [("retry-after", "0")],
                        "busy",
                    )
                        .into_response();
                }
                if matches!(fault, Fault::RetryQuery)
                    && path.contains("/query/")
                    && calls.fetch_add(1, Ordering::SeqCst) < 2
                {
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        [("retry-after", "0")],
                        "busy",
                    )
                        .into_response();
                }
                if matches!(fault, Fault::RetryMap)
                    && path == "/v1/filters/shards"
                    && calls.fetch_add(1, Ordering::SeqCst) == 0
                {
                    return (StatusCode::BAD_GATEWAY, "temporary gateway outage").into_response();
                }
                if matches!(fault, Fault::Drift)
                    && path == "/v1/filters/shards"
                    && calls.fetch_add(1, Ordering::SeqCst) > 1
                {
                    return (StatusCode::SERVICE_UNAVAILABLE, "publication unavailable")
                        .into_response();
                }
                next.run(request).await
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (base, state)
}

async fn run(
    dir: &Path,
    base: &str,
    name: &str,
    mode: &str,
    slots: usize,
    deadline: u64,
    wrong: bool,
) -> Value {
    let config = json!({"schema":"transparent-scenario-v1","name":name,"mode":mode,"sample":if wrong {"wrong.json"}else{"sample.json"},"shard_url":base,"profiles":{"test":slots},"duration_seconds":1,"recovery_deadline_seconds":deadline,"request_timeout_seconds":30,"store":"sqlite","metrics_targets":{"test-worker":format!("{base}/metrics"),"missing-worker":"http://127.0.0.1:1/metrics"}});
    run_config(dir, name, config).await
}

async fn run_config(dir: &Path, name: &str, config: Value) -> Value {
    let path = dir.join(format!("{name}.json"));
    fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let out = dir.join(name);
    let output = tokio::task::spawn_blocking({
        let out = out.clone();
        move || {
            Command::new(env!("CARGO_BIN_EXE_transparent-loadtest"))
                .args([
                    "--scenario",
                    path.to_str().unwrap(),
                    "--out-dir",
                    out.to_str().unwrap(),
                ])
                .output()
                .unwrap()
        }
    })
    .await
    .unwrap();
    let report: Value = serde_json::from_slice(
        &fs::read(out.join("report.json"))
            .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&output.stderr))),
    )
    .unwrap();
    assert_eq!(
        output.status.success(),
        report["success"] == true,
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(out.join("report.html").exists());
    assert!(report["metrics"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["target"] == "missing-worker" && !m["error"].is_null()));
    assert!(report["users"]
        .as_array()
        .unwrap()
        .iter()
        .all(|u| u["outcome"] != "running"));
    report
}

fn fixture(dir: &Path, tags: &[u32]) -> ShardMap {
    fs::create_dir(dir.join("shards")).unwrap();
    let map = common::publish_with(
        &dir.join("shards"),
        &common::chain(),
        |_| &RECENT_4K,
        0,
        "",
        common::hash_at,
    );
    sample(&dir.join("sample.json"), &map, tags, false);
    sample(&dir.join("wrong.json"), &map, tags, true);
    map
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_real_pir_sqlite_recovery_and_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path(), &[1, 3, 9999, 10000]);
    let (base, state) = service(dir.path(), Fault::None).await;
    let report = run(dir.path(), &base, "real-wave", "wave", 4, 120, false).await;
    assert_eq!(report["success"], true);
    assert_eq!(report["summary"]["outcomes"]["exact"], 4);
    assert!(
        report["summary"]["stages"]["query_pages"]["calls"]
            .as_f64()
            .unwrap()
            > 0.0
    );
    assert_eq!(
        fs::read_dir(dir.path().join("real-wave/stores"))
            .unwrap()
            .count(),
        0
    );
    let starts: Vec<_> = report["users"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["started_at"].as_f64().unwrap())
        .collect();
    assert!(
        starts.iter().copied().fold(f64::MIN, f64::max)
            - starts.iter().copied().fold(f64::MAX, f64::min)
            < 2.0
    );
    assert!(state.metrics().query_seconds[0].count() > 0);
    assert!(state.metrics().evaluation_seconds.count() > 0);
    if let Ok(destination) = std::env::var("TRANSPARENT_SIMULATION_QA_DIR") {
        fs::create_dir_all(&destination).unwrap();
        for entry in fs::read_dir(dir.path().join("real-wave")).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                fs::copy(
                    entry.path(),
                    Path::new(&destination).join(entry.file_name()),
                )
                .unwrap();
            }
        }
    }
    let mut budget: Value =
        serde_json::from_slice(&fs::read(dir.path().join("real-wave.json")).unwrap()).unwrap();
    budget["max_queries"] = json!(0);
    budget["profiles"] = json!({"test":4});
    let partial = run_config(dir.path(), "budget", budget).await;
    assert_eq!(partial["summary"]["outcomes"]["incomplete"], 2);
    assert_eq!(partial["summary"]["outcomes"]["exact"], 2);
    let (retry_base, _) = service(dir.path(), Fault::RetryQuery).await;
    let retries = run(dir.path(), &retry_base, "retry", "wave", 4, 120, false).await;
    assert_eq!(retries["success"], true);
    assert_eq!(
        retries["summary"]["stages"]["query_directory"]["http_503"],
        2.0
    );
    let mismatch = run(dir.path(), &base, "mismatch", "wave", 1, 120, true).await;
    assert_eq!(mismatch["success"], false);
    assert_eq!(mismatch["summary"]["outcomes"]["mismatched"], 1);
    assert!(
        fs::read_dir(dir.path().join("mismatch/stores"))
            .unwrap()
            .count()
            > 0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twenty_users_sustained_replacement_and_drain() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path(), &(10000..10040).collect::<Vec<_>>());
    let (base, _) = service(dir.path(), Fault::None).await;
    let wave = run(dir.path(), &base, "twenty", "wave", 20, 30, false).await;
    assert_eq!(wave["summary"]["outcomes"]["exact"], 20);
    let report = run(dir.path(), &base, "sustained", "sustained", 2, 30, false).await;
    let users = report["users"].as_array().unwrap();
    assert!(users.len() > 2);
    let start = report["started_at"].as_f64().unwrap();
    for user in users {
        assert!(user["scheduled_at"].as_f64().unwrap() < start + 1.02);
    }
    for a in users {
        for b in users {
            if a["id"] == b["id"] {
                continue;
            }
            let overlap = a["scheduled_at"].as_f64().unwrap() < b["finished_at"].as_f64().unwrap()
                && b["scheduled_at"].as_f64().unwrap() < a["finished_at"].as_f64().unwrap();
            if overlap {
                assert_ne!(a["sample_index"], b["sample_index"]);
                assert_ne!(a["slot"], b["slot"]);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hard_deadline_refusal_and_publication_failure_remain_unsuccessful() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path(), &[9999, 10000]);
    let (base, _) = service(dir.path(), Fault::SlowFilter).await;
    let started = Instant::now();
    let report = run(dir.path(), &base, "timeout", "wave", 2, 1, false).await;
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(report["summary"]["outcomes"]["timed_out"], 2);
    let (base, _) = service(dir.path(), Fault::RefuseFilter).await;
    let report = run(dir.path(), &base, "refusal", "wave", 2, 10, false).await;
    assert_eq!(report["summary"]["outcomes"]["failed"], 2);
    assert_eq!(report["summary"]["stages"]["filters"]["http_503"], 2.0);
    assert!(
        report["summary"]["stages"]["filters"]["bytes_down"]
            .as_f64()
            .unwrap()
            > 0.0
    );
    let (base, _) = service(dir.path(), Fault::Drift).await;
    let report = run(dir.path(), &base, "drift", "wave", 1, 10, false).await;
    assert_eq!(report["publication_stable"], false);
    assert_eq!(report["success"], false);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_interrupt_finalizes_and_worker_crash_is_counted() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path(), &[9999]);
    let (base, _) = service(dir.path(), Fault::SlowFilter).await;
    for crash_worker in [true, false] {
        let name = if crash_worker {
            "crashed"
        } else {
            "interrupted"
        };
        let config = json!({"schema":"transparent-scenario-v1","name":name,"mode":"wave","sample":"sample.json","shard_url":base,"profiles":{"test":1},"recovery_deadline_seconds":30});
        let path = dir.path().join(format!("{name}.json"));
        fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let out = dir.path().join(name);
        let mut child = Command::new(env!("CARGO_BIN_EXE_transparent-loadtest"))
            .args([
                "--scenario",
                path.to_str().unwrap(),
                "--out-dir",
                out.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let started = Instant::now();
        loop {
            if fs::read_to_string(out.join("wallets.ndjson"))
                .unwrap_or_default()
                .contains("started")
            {
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(15));
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let pid = if crash_worker {
            let mut system = sysinfo::System::new_all();
            system.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
            system
                .processes()
                .values()
                // Linux also exposes threads with their process as parent.
                // SIGKILL to a supervisor thread would kill the supervisor,
                // not simulate the loss of its wallet child.
                .find(|p| {
                    p.parent() == Some(sysinfo::Pid::from_u32(child.id()))
                        && p.thread_kind().is_none()
                        && p.cmd().iter().any(|arg| arg == "--scenario-worker")
                })
                .unwrap()
                .pid()
                .as_u32()
        } else {
            child.id()
        };
        assert!(Command::new("kill")
            .args([
                if crash_worker { "-KILL" } else { "-TERM" },
                &pid.to_string()
            ])
            .status()
            .unwrap()
            .success());
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            assert!(started.elapsed() < Duration::from_secs(20));
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert!(
            status.code().is_some(),
            "{name}: supervisor died from a signal: {status}"
        );
        let r: Value = serde_json::from_slice(&fs::read(out.join("report.json")).unwrap()).unwrap();
        assert_eq!(r["success"], false);
        assert_eq!(
            r["summary"]["outcomes"][if crash_worker { "failed" } else { "cancelled" }],
            1
        );
    }
}

/// A receive before the catch-up window must resolve a spend inside it without
/// counting the preparatory restore as measured load or importing future events.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catch_up_seeds_prior_history_and_keeps_measurement_separate() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path(), &[1]);
    let path = dir.path().join("sample.json");
    let mut sample: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let from = common::FIRST + 2 * common::SPAN;
    let events: Vec<_> = common::chain()
        .into_iter()
        .flatten()
        .filter(|(s, e)| *s == common::script(1) && u64::from(e.height()) >= from)
        .map(|(_, e)| e)
        .collect();
    assert_eq!(events.len(), 1);
    let mut hash = Sha256::new();
    for event in &events {
        hash.update(event.to_bytes());
    }
    sample["clients"][0]["required_from"] = json!(from);
    sample["clients"][0]["expected_digest"] = json!(hex::encode(hash.finalize()));
    sample["clients"][0]["journal_events"] = json!(events.len());
    fs::write(&path, serde_json::to_vec(&sample).unwrap()).unwrap();
    let (base, _) = service(dir.path(), Fault::None).await;
    for store in ["sqlite", "memory"] {
        let config = json!({"schema":"transparent-scenario-v1","name":store,"mode":"wave","sample":"sample.json","shard_url":base,"profiles":{"test":1},"store":store,"recovery_deadline_seconds":120,"metrics_targets":{"missing-worker":"http://127.0.0.1:1/metrics"}});
        let report = run_config(dir.path(), store, config).await;
        assert_eq!(report["success"], true);
        let user = &report["users"][0];
        assert_eq!(user["seeded_events"], 1);
        assert_eq!(user["events"], 1);
        assert_eq!(user["events_exact"], true);
        assert_eq!(user["unresolved_spends"], 0);
        assert_eq!(user["outcome"], "exact");
        let totals: f64 = user["stages"]
            .as_object()
            .unwrap()
            .values()
            .map(|s| s["calls"].as_f64().unwrap())
            .sum();
        assert_eq!(user["http_totals"]["calls"], totals);
        let prep: Value = serde_json::from_slice(
            &fs::read(
                dir.path()
                    .join(store)
                    .join("preparation/batch-0/report.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(prep["success"], true);
        assert!(prep["finished_at"].as_f64().unwrap() <= report["started_at"].as_f64().unwrap());
        assert_eq!(report["summary"]["started"], 1);
        assert!(
            prep["summary"]["stages"]["filters"]["calls"]
                .as_f64()
                .unwrap()
                > report["summary"]["stages"]["filters"]["calls"]
                    .as_f64()
                    .unwrap()
        );
    }
    let (retry_base, _) = service(dir.path(), Fault::RetryMap).await;
    let retry = run(dir.path(), &retry_base, "retry-map", "wave", 1, 120, false).await;
    assert_eq!(retry["success"], true);
    assert_eq!(retry["preparation"][0]["status"], "complete");
    let requests = fs::read_to_string(
        dir.path()
            .join("retry-map/preparation/batch-0/preflight.ndjson"),
    )
    .unwrap();
    assert!(requests
        .lines()
        .any(|line| serde_json::from_str::<Value>(line).unwrap()["status"] == 502));
    // A bad oracle must stop preparation, never bless the measured wave.
    sample["clients"][0]["expected_digest"] = json!("incorrect");
    fs::write(&path, serde_json::to_vec(&sample).unwrap()).unwrap();
    let config = json!({"schema":"transparent-scenario-v1","name":"bad-seed","mode":"wave","sample":"sample.json","shard_url":base,"profiles":{"test":1},"recovery_deadline_seconds":120});
    let path = dir.path().join("bad-seed.json");
    fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let out = dir.path().join("bad-seed");
    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_transparent-loadtest"))
            .arg("--scenario")
            .arg(path)
            .arg("--out-dir")
            .arg(out)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(!output.status.success());
    let report: Value =
        serde_json::from_slice(&fs::read(dir.path().join("bad-seed/report.json")).unwrap())
            .unwrap();
    assert_eq!(report["success"], false);
    assert!(report["users"].as_array().unwrap().is_empty());
    assert_eq!(report["phase"], "preparation_failed");
    assert_eq!(report["preparation"][0]["status"], "failed");
    assert_eq!(
        report["preparation"][0]["users"][0]["outcome"],
        "mismatched"
    );
    assert_eq!(
        report["preparation"][0]["report"],
        "preparation/batch-0/report.html"
    );
    if let Ok(destination) = std::env::var("TRANSPARENT_SIMULATION_QA_DIR") {
        let target = Path::new(&destination).join("preparation-failed");
        fs::create_dir_all(&target).unwrap();
        fs::copy(
            dir.path().join("bad-seed/report.html"),
            target.join("report.html"),
        )
        .unwrap();
    }
    assert!(report["errors"][0]
        .as_str()
        .unwrap()
        .contains("preparation unsuccessful"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preparation_cap_does_not_reduce_measured_wave_concurrency() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path(), &[1, 2, 3]);
    let path = dir.path().join("sample.json");
    let mut data: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let from = common::FIRST + 2 * common::SPAN;
    for (client, tag) in data["clients"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .zip(1..=3)
    {
        let mut events: Vec<_> = common::chain()
            .into_iter()
            .flatten()
            .filter(|(s, e)| *s == common::script(tag) && u64::from(e.height()) >= from)
            .map(|(_, e)| e)
            .collect();
        events.sort_by_key(|e| e.sort_key());
        events.dedup();
        let mut hash = Sha256::new();
        for event in &events {
            hash.update(event.to_bytes());
        }
        client["required_from"] = json!(from);
        client["expected_digest"] = json!(hex::encode(hash.finalize()));
        client["journal_events"] = json!(events.len());
    }
    fs::write(&path, serde_json::to_vec(&data).unwrap()).unwrap();
    let (base, _) = service(dir.path(), Fault::None).await;
    let report = run(dir.path(), &base, "bounded-prep", "wave", 3, 120, false).await;
    assert_eq!(report["success"], true);
    assert_eq!(report["phase"], "complete");
    assert_eq!(report["summary"]["started"], 3);
    let batches = report["preparation"].as_array().unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0]["summary"]["started"], 2);
    assert_eq!(batches[1]["summary"]["started"], 1);
    for batch in batches {
        assert_eq!(batch["status"], "complete");
        for user in batch["users"].as_array().unwrap() {
            assert!(user["finished_at"].as_f64().unwrap() < report["started_at"].as_f64().unwrap());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn page_setup_retry_finishes_existing_pending_work() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path(), &[3]);
    for store in ["sqlite", "memory"] {
        let (base, _) = service(dir.path(), Fault::RetryPageSetup).await;
        let config = json!({"schema":"transparent-scenario-v1","name":store,"mode":"wave","sample":"sample.json","shard_url":base,"profiles":{"test":1},"store":store,"recovery_deadline_seconds":120,"metrics_targets":{"missing-worker":"http://127.0.0.1:1/metrics"}});
        let report = run_config(dir.path(), store, config).await;
        assert_eq!(report["summary"]["stages"]["setup_pages"]["http_503"], 1.0);
        assert_eq!(report["summary"]["stages"]["query_directory"]["calls"], 2.0);
        assert_eq!(report["users"][0]["events_exact"], true);
        assert_eq!(report["users"][0]["completion"], "Complete");
        assert_eq!(report["success"], true);
    }
}
