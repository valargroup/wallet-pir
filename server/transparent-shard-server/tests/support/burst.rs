//! Isolated worker-stage benchmark. It never contacts or mutates a fleet.
use super::*;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};
use transparent_shard::layout::RECENT_8K;
use transparent_shard_server::live::{Command, LiveService, Publication};
use transparent_shard_server::service::ReadinessMode;
use transparent_shard_server::shardset::LoadOptions;
use transparent_wallet::transport::{Overloaded, StaleRevision};

/// Drain optional writes outside the query/visibility window while retaining
/// memory sampling. Holding metrics does not pin a retired ServiceState/runtime.
async fn drain_persistence(metrics: &Arc<transparent_shard_server::metrics::Metrics>) -> bool {
    tokio::time::timeout(Duration::from_secs(60), async {
        while transparent_shard_server::metrics::Metrics::get(&metrics.disk_save_pending) != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .is_ok()
}

/// Run each configuration in its own process so allocator retention does not
/// contaminate the comparison. Timings are measurements, not normal CI asserts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual recent-8k burst benchmark; use make transparent-burst"]
async fn publication_burst_under_exact_load() {
    let _ = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter("transparent_shard_server=debug")
        .try_init();
    let slots: usize = std::env::var("TRANSPARENT_BURST_BUILD_SLOTS")
        .unwrap_or_else(|_| "1".into())
        .parse()
        .unwrap();
    assert!((1..=2).contains(&slots));
    let output = std::env::var("TRANSPARENT_BURST_REPORT").expect("report path required");
    let mut report = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .expect("report must not already exist");
    let root = tempfile::tempdir().unwrap();
    let fixture = std::env::var("TRANSPARENT_BURST_FIXTURE").ok();
    let mut publications = Vec::new();
    let (options, query_root, query_shard, final_height, assigned_count) =
        if let Some(path) = &fixture {
            let path = std::path::PathBuf::from(path).canonicalize().unwrap();
            let base = path.parent().unwrap();
            let value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            for item in value["publications"].as_array().unwrap() {
                publications.push(Publication {
                    directory: base.join(item["directory"].as_str().unwrap()),
                    assignment: Some(base.join(item["assignment"].as_str().unwrap())),
                    map_sha256: item["map_sha256"].as_str().unwrap().to_owned(),
                });
            }
            assert_eq!(publications.len(), 3);
            let assignment = transparent_shard_server::assignment::Assignment::load(
                publications[0].assignment.as_ref().unwrap(),
            )
            .unwrap();
            let worker_id = value["worker_id"].as_str().unwrap().to_owned();
            let count = assignment.worker(&worker_id).unwrap().shards.len();
            assert_eq!(
                count, 14,
                "full-residency fixture must assign fourteen shards"
            );
            let options = LoadOptions {
                scope: transparent_shard_server::shardset::LoadScope::Assigned {
                    assignment: Arc::new(assignment),
                    worker_id,
                },
                prune_excess: true,
                ..LoadOptions::whole(3)
            };
            (
                options,
                base.to_path_buf(),
                value["query_shard"].as_u64().unwrap() as usize,
                value["publications"][2]["height"].as_u64().unwrap(),
                count,
            )
        } else {
            let mut predecessor = String::new();
            for revision in 0..3 {
                let directory = root.path().join(revision.to_string());
                std::fs::create_dir(&directory).unwrap();
                let end = FIRST + 40 + u64::from(revision);
                let (digest, count) =
                    write_revision_geometry(&directory, end, revision, &predecessor, &RECENT_8K);
                write_map_geometry(&directory, &digest, end, revision, count, &RECENT_8K);
                predecessor = digest;
                let map_sha256 = ShardSet::open(&directory, 3).unwrap().map_digest;
                publications.push(Publication {
                    directory,
                    assignment: None,
                    map_sha256,
                });
            }
            (
                LoadOptions::whole(3),
                root.path().to_path_buf(),
                0,
                FIRST + 42,
                1,
            )
        };
    let config = ServiceConfig {
        cache_bytes: if fixture.is_some() { 5 << 30 } else { 1 << 30 },
        build_slots: slots,
        readiness: ReadinessMode::Warm,
        ..ServiceConfig::default()
    };
    let disk = fixture.as_ref().map(|_| {
        transparent_shard_server::runtime::disk::DiskCache::new(
            root.path().join("runtime-cache"),
            10 << 30,
        )
        .unwrap()
    });
    let set = ShardSet::open_with(&publications[0].directory, &options).unwrap();
    assert_eq!(set.map_digest, publications[0].map_sha256);
    assert_eq!(set.assigned_len(), assigned_count);
    let state = ServiceState::build_with_disk(set, config, disk).unwrap();
    let cold_started = Instant::now();
    tokio::time::timeout(Duration::from_secs(600), state.spawn_prewarm())
        .await
        .unwrap()
        .unwrap();
    let cold_prewarm_seconds = cold_started.elapsed().as_secs_f64();
    let persistence_metrics = state.metrics().clone();
    let drain_started = Instant::now();
    let cold_persistence_complete = drain_persistence(&persistence_metrics).await;
    let cold_persistence_drain_seconds = drain_started.elapsed().as_secs_f64();
    let cold_memory = kernel_memory();
    let response = router(state.clone())
        .oneshot(Request::get("/v1/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let cold_ready: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    let cold_warm = cold_persistence_complete
        && cold_ready["ready"] == true
        && cold_ready["warm_runtimes"].as_u64() == Some((assigned_count * 2) as u64)
        && cold_ready["target_runtimes"].as_u64() == Some((assigned_count * 2) as u64);
    let live = LiveService::new(
        state,
        publications[0].clone(),
        config,
        options,
        root.path().join("active.json"),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = live.router();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let stop = Arc::new(AtomicBool::new(false));
    let clock = Instant::now();
    let queries = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut clients = Vec::new();
    let external = std::env::var("TRANSPARENT_BURST_CLIENT_DIR")
        .ok()
        .map(std::path::PathBuf::from);
    if let Some(directory) = &external {
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
            - clock.elapsed().as_secs_f64();
        let config =
            serde_json::json!({"url":url,"root":query_root,"shard":query_shard,"epoch":epoch});
        std::fs::write(directory.join("config.tmp"), config.to_string()).unwrap();
        std::fs::rename(directory.join("config.tmp"), directory.join("config.json")).unwrap();
        for client in 0..2 {
            let (directory, stop, queries, ready_tx) = (
                directory.clone(),
                stop.clone(),
                queries.clone(),
                ready_tx.clone(),
            );
            clients.push(tokio::task::spawn_blocking(move || {
                read_external_records(&directory, client, &stop, &queries, &ready_tx)
            }));
        }
    } else {
        for client in 0..2 {
            let (url, directory, stop, queries, ready_tx) = (
                url.clone(),
                query_root.clone(),
                stop.clone(),
                queries.clone(),
                ready_tx.clone(),
            );
            clients.push(tokio::task::spawn_blocking(move || {
                let mut count = 0;
                while !stop.load(Ordering::Acquire) {
                    let table = if count % 2 == 0 {
                        transparent_wallet::client::Table::Directory
                    } else {
                        transparent_wallet::client::Table::Pages
                    };
                    let started = clock.elapsed().as_secs_f64();
                    let result =
                        super::burst_query::once_nonempty(&url, &directory, query_shard, table);
                    let mut record = serde_json::json!({"client":client,"started_seconds":started,
                    "finished_seconds":clock.elapsed().as_secs_f64()});
                    match result {
                        Ok(value) => {
                            record["query"] = value;
                            count += 1;
                            if count == 2 {
                                let _ = ready_tx.send(client);
                            }
                        }
                        Err(error) => {
                            let retry = StaleRevision::found_in(&error).is_some()
                                || Overloaded::found_in(&error).is_some();
                            record["retry"] = retry.into();
                            record["error"] = error.to_string().into();
                            queries.lock().unwrap().push(record);
                            if !retry {
                                return Err(error.to_string());
                            }
                            std::thread::sleep(Duration::from_millis(50));
                            continue;
                        }
                    }
                    queries.lock().unwrap().push(record);
                }
                Ok(count)
            }));
        }
    }
    drop(ready_tx);
    let sample_control = Arc::new(AtomicBool::new(false));
    let sample_stop = sample_control.clone();
    let sampler = tokio::spawn(async move {
        let mut system = sysinfo::System::new();
        let pid = sysinfo::get_current_pid().unwrap();
        let mut samples = Vec::new();
        while !sample_stop.load(Ordering::Acquire) {
            system.refresh_memory();
            system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
            samples.push(serde_json::json!({"seconds":clock.elapsed().as_secs_f64(),
                "process_rss_bytes":system.process(pid).map(|p| p.memory()),
                "host_available_bytes":system.available_memory(), "host_total_bytes":system.total_memory(),
                "cgroup":transparent_shard_server::procmem::cgroup_memory_bytes()}));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        samples
    });
    let mut activations = Vec::new();
    let result = tokio::time::timeout(Duration::from_secs(120), async {
        if !cold_warm { return Err("initial assignment did not fully warm; invalid full-residency baseline".into()); }
        let mut ready_clients = std::collections::HashSet::new();
        while ready_clients.len() < 2 { ready_clients.insert(ready_rx.recv().await.ok_or("query client failed before warmup")?); }
        let burst_start = Instant::now();
        let burst_offset = clock.elapsed().as_secs_f64();
        let mut expected = publications[0].map_sha256.clone();
        for (index, publication) in publications.iter().enumerate().skip(1) {
            let arrival = Duration::from_secs((index - 1) as u64);
            tokio::time::sleep_until(tokio::time::Instant::from_std(burst_start + arrival)).await;
            let preparation_started = burst_start.elapsed().as_secs_f64();
            let preparation = live.command(Command::Prepare { expected: expected.clone(), publication: publication.clone() })
                .await.map_err(|e| e.to_string())?;
            let prepared = burst_start.elapsed().as_secs_f64();
            live.command(Command::Activate { expected, map_sha256: publication.map_sha256.clone() })
                .await.map_err(|e| e.to_string())?;
            expected = publication.map_sha256.clone();
            let activated = burst_start.elapsed().as_secs_f64();
            let response = live.router().oneshot(Request::get("/v1/ready").body(Body::empty()).unwrap()).await.unwrap();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let ready: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            if ready["ready"] != true || ready["warm_runtimes"].as_u64() != Some((assigned_count * 2) as u64)
                || ready["target_runtimes"].as_u64() != Some((assigned_count * 2) as u64) {
                return Err("activation did not warm the entire assignment".to_string());
            }
            activations.push(serde_json::json!({"revision":index,"map_sha256":expected,
                "arrival_seconds":arrival.as_secs_f64(),"queue_seconds":preparation_started-arrival.as_secs_f64(),
                "prepare_seconds":prepared-preparation_started,"activate_seconds":activated-prepared,
                "worker_visibility_seconds":activated-arrival.as_secs_f64(),"burst_clock_offset":burst_offset,
                "ready":ready,"preparation":preparation}));
        }
        // Let the clients decode both newly activated tables before stopping.
        // This verification is outside the recorded publication cycle times.
        loop {
            let verified = {
                let records = queries.lock().unwrap();
                ["directory", "pages"].iter().all(|table| records.iter().any(|q|
                    q["query"]["height"] == final_height && q["query"]["table"] == *table
                    && q["query"]["exact"] == true))
            };
            if verified { break; }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<(), String>(())
    }).await;
    if let Some(directory) = &external {
        std::fs::write(directory.join("stop"), b"").unwrap();
    }
    stop.store(true, Ordering::Release);
    let mut client_results = Vec::new();
    for client in clients {
        client_results.push(client.await.unwrap());
    }
    let drain_started = Instant::now();
    let persistence_complete = drain_persistence(&persistence_metrics).await;
    let persistence_drain_seconds = drain_started.elapsed().as_secs_f64();
    sample_control.store(true, Ordering::Release);
    let samples = sampler.await.unwrap();
    server.abort();
    let query_records = queries.lock().unwrap();
    let exact = client_results
        .iter()
        .all(|r| r.as_ref().is_ok_and(|n| *n >= 2));
    let client_shutdown_complete = external.as_ref().is_none_or(|directory| {
        (0..2).all(|client| directory.join(format!("client-{client}.done")).exists())
    });
    let loaded = activations
        .first()
        .zip(activations.last())
        .is_some_and(|(first, last)| {
            let start = first["burst_clock_offset"].as_f64().unwrap();
            let end = start
                + last["arrival_seconds"].as_f64().unwrap()
                + last["worker_visibility_seconds"].as_f64().unwrap();
            (0..2).all(|client| {
                query_records.iter().any(|q| {
                    q["client"] == client
                        && q["query"]["exact"] == true
                        && q["started_seconds"].as_f64().unwrap() < end
                        && q["finished_seconds"].as_f64().unwrap() > start
                })
            })
        });
    let completed = persistence_complete && matches!(&result, Ok(Ok(())));
    let within_budget = activations.len() == 2
        && activations
            .iter()
            .all(|a| a["worker_visibility_seconds"].as_f64().unwrap() <= 30.0);
    let value = serde_json::json!({"schema":"transparent-worker-burst-v6","build_slots":slots,
        "client_shutdown_complete":client_shutdown_complete,
        "geometry":"recent-8k","query_clients":2,"cache_bytes":config.cache_bytes,
        "scope":"isolated worker stage; not fleet acceptance", "external_clients":external.is_some(),
        "kernel_policy":transparent_shard_server::runtime::KERNEL_POLICY,
        "cold_persistence_complete":cold_persistence_complete,"cold_persistence_drain_seconds":cold_persistence_drain_seconds,
        "persistence_complete":persistence_complete,"persistence_drain_seconds":persistence_drain_seconds,
        "cold_ready":cold_ready,"cold_warm":cold_warm,"assigned_shards":assigned_count,"fixture":fixture,"cold_prewarm_seconds":cold_prewarm_seconds,
        "kernel_memory":kernel_memory(),"cold_memory":cold_memory,
        "source_sha":std::env::var("TRANSPARENT_BURST_SOURCE_SHA").ok(),
        "os":std::env::consts::OS,"arch":std::env::consts::ARCH,
        "completed":completed,"exact_queries":exact,"load_overlapped_burst":loaded,"worker_30s_budget_passed":within_budget,
        "outcome":format!("{result:?}"),"client_results":client_results,
        "activations":activations,"queries":*query_records,"memory_samples":samples});
    use std::io::Write;
    serde_json::to_writer_pretty(&mut report, &value).unwrap();
    report.write_all(b"\n").unwrap();
    report.sync_all().unwrap();
    assert!(
        completed && exact && loaded,
        "burst or query correctness failed; see report"
    );
    // A missed performance budget is retained as data, so both configurations
    // run even when the baseline is slow. Fleet acceptance uses its own gate.
}

/// Kernel counters include cold prewarm and unsampled transient allocations.
fn kernel_memory() -> serde_json::Value {
    let mut value = serde_json::Map::new();
    if let Ok(cgroup) = std::fs::read_to_string("/proc/self/cgroup") {
        if let Some(path) = cgroup.lines().find_map(|l| l.strip_prefix("0::")) {
            value.insert("path".into(), path.into());
            let base = std::path::Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/'));
            for name in [
                "cpu.stat",
                "cpuset.cpus.effective",
                "memory.swap.max",
                "memory.peak",
                "memory.current",
                "memory.high",
                "memory.max",
                "memory.events",
                "memory.stat",
                "memory.pressure",
            ] {
                if let Ok(raw) = std::fs::read_to_string(base.join(name)) {
                    value.insert(name.into(), raw.into());
                }
            }
        }
    }
    value.into()
}

/// Runner starts this process outside the worker cgroup and on disjoint CPUs.
#[test]
#[ignore = "manual external burst clients; started by the burst runner"]
fn external_query_clients() {
    use std::io::Write;
    let directory =
        std::path::PathBuf::from(std::env::var("TRANSPARENT_BURST_CLIENT_DIR").unwrap());
    std::fs::write(directory.join("host.json"), serde_json::json!({"pid":std::process::id(),"kernel":kernel_memory(),"proc_status":std::fs::read_to_string("/proc/self/status").ok()}).to_string()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(800);
    while !directory.join("config.json").exists() {
        assert!(
            Instant::now() < deadline,
            "worker never published client configuration"
        );
        if directory.join("stop").exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.join("config.json")).unwrap()).unwrap();
    let handles: Vec<_> = (0..2).map(|client| {
        let (directory, config) = (directory.clone(), config.clone());
        std::thread::spawn(move || {
            let mut file = std::fs::OpenOptions::new().write(true).create_new(true).open(directory.join(format!("client-{client}.jsonl"))).unwrap();
            let elapsed = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64() - config["epoch"].as_f64().unwrap();
            let mut count = 0;
            while !directory.join("stop").exists() && Instant::now() < deadline {
                let table = if count % 2 == 0 { transparent_wallet::client::Table::Directory } else { transparent_wallet::client::Table::Pages };
                let started = elapsed();
                let result = super::burst_query::once_nonempty(config["url"].as_str().unwrap(), std::path::Path::new(config["root"].as_str().unwrap()), config["shard"].as_u64().unwrap() as usize, table);
                let mut record = serde_json::json!({"client":client,"started_seconds":started,"finished_seconds":elapsed()});
                let mut fatal = false;
                match result {
                    Ok(value) => { record["query"] = value; count += 1; }
                    Err(error) => {
                        let retry = StaleRevision::found_in(&error).is_some() || Overloaded::found_in(&error).is_some();
                        record["retry"] = retry.into(); record["error"] = error.to_string().into(); fatal = !retry;
                    }
                }
                writeln!(file, "{record}").unwrap(); file.flush().unwrap();
                assert!(!fatal, "external exact query failed");
                if record["retry"] == true { std::thread::sleep(Duration::from_millis(50)); }
            }
            assert!(count >= 2, "insufficient exact queries");
            drop(file);
            std::fs::write(directory.join(format!("client-{client}.done")), b"").unwrap();
        })
    }).collect();
    for handle in handles {
        handle.join().unwrap();
    }
}

fn read_external_records(
    directory: &std::path::Path,
    client: usize,
    stop: &AtomicBool,
    queries: &std::sync::Mutex<Vec<serde_json::Value>>,
    ready_tx: &tokio::sync::mpsc::UnboundedSender<usize>,
) -> Result<usize, String> {
    use std::io::BufRead;
    let path = directory.join(format!("client-{client}.jsonl"));
    while !path.exists() {
        if stop.load(Ordering::Acquire) {
            return Err("client never started".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut reader = std::io::BufReader::new(std::fs::File::open(path).unwrap());
    let mut pending = String::new();
    let mut count = 0;
    let mut done_seen = false;
    let mut drain_started = None;
    loop {
        let n = reader.read_line(&mut pending).map_err(|e| e.to_string())?;
        if pending.ends_with('\n') {
            let record: serde_json::Value =
                serde_json::from_str(&pending).map_err(|e| e.to_string())?;
            pending.clear();
            let error = record.get("error").is_some() && record["retry"] != true;
            if record["query"]["exact"] == true {
                count += 1;
            }
            queries.lock().unwrap().push(record);
            if error {
                return Err("external query failed; see client records".into());
            }
            if count == 2 {
                let _ = ready_tx.send(client);
            }
        }
        if n == 0 {
            // Observe completion, then read to EOF again: the last
            // record may have arrived between this read and the
            // completion-marker check. Never stop the server with
            // an external request still in flight.
            if done_seen {
                if !pending.is_empty() {
                    return Err("truncated final client record".into());
                }
                return Ok(count);
            }
            if directory.join(format!("client-{client}.done")).exists() {
                done_seen = true;
                continue;
            }
            if stop.load(Ordering::Acquire) {
                let started = drain_started.get_or_insert_with(Instant::now);
                if started.elapsed() > Duration::from_secs(60) {
                    return Err("external client did not finish draining".into());
                }
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[test]
fn external_records_drain_inflight_completion_after_stop() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("client-0.jsonl");
    let mut file = std::fs::File::create(path).unwrap();
    let record = "{\"query\":{\"exact\":true}}\n";
    file.write_all(record.repeat(2).as_bytes()).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let queries = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let reader = {
        let (directory, stop, queries) = (dir.path().to_owned(), stop.clone(), queries.clone());
        std::thread::spawn(move || read_external_records(&directory, 0, &stop, &queries, &tx))
    };
    assert_eq!(rx.blocking_recv(), Some(0));
    stop.store(true, Ordering::Release);
    // Model an outstanding HTTP request completing after the stop request.
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        !reader.is_finished(),
        "reader stopped before the client drained"
    );
    file.write_all(record.as_bytes()).unwrap();
    drop(file);
    std::fs::write(dir.path().join("client-0.done"), b"").unwrap();
    assert_eq!(reader.join().unwrap().unwrap(), 3);
    assert_eq!(queries.lock().unwrap().len(), 3);
}

#[test]
fn drained_external_errors_and_truncated_records_still_fail() {
    for record in [
        "{\"error\":\"request failed\",\"retry\":false}\n",
        "{\"query\":",
    ] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("client-0.jsonl"), record).unwrap();
        std::fs::write(dir.path().join("client-0.done"), b"").unwrap();
        let (tx, _) = tokio::sync::mpsc::unbounded_channel();
        assert!(read_external_records(
            dir.path(),
            0,
            &AtomicBool::new(true),
            &std::sync::Mutex::new(Vec::new()),
            &tx
        )
        .is_err());
    }
}
