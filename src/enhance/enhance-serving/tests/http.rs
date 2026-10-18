//! Real encrypted requests traverse coordinator and two worker HTTP listeners.
use enhance_pir::{client::EnhancePirClient, RECORD_BYTES};
use enhance_serving::{
    control::{Group, Ledger, Replica},
    coordinator::Coordinator,
    store::RecordJournal,
    types::{DatabaseId, ENHANCE_LAYOUT},
    worker::Worker,
};

async fn serve(router: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (url, task)
}

fn record(position: u64) -> Vec<u8> {
    let mut bytes = vec![0; RECORD_BYTES];
    bytes[..8].copy_from_slice(&position.to_le_bytes());
    bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_round_trip_retention_failover_and_restart() {
    distributed_backend_round_trip(Default::default()).await;
}

#[cfg(feature = "cuda")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires NVIDIA GPU and NVRTC"]
async fn cuda_distributed_round_trip_retention_failover_and_restart() {
    distributed_backend_round_trip(enhance_serving::matvec::MatvecConfig {
        matvec_backend: enhance_serving::matvec::Backend::Cuda,
        cuda_device: None,
    })
    .await;
}

async fn distributed_backend_round_trip(backend: enhance_serving::matvec::MatvecConfig) {
    let root = tempfile::tempdir().unwrap();
    let mut workers = Vec::new();
    let mut replicas = Vec::new();
    let mut failures = Vec::new();
    let mut commit_failures = Vec::new();
    let mut evaluations = Vec::new();
    for index in 0..2 {
        let worker = Worker::open_with_backend(
            &root.path().join(format!("worker-{index}")),
            Default::default(),
            backend,
        )
        .unwrap();
        let failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        failures.push(failed.clone());
        let commit_failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        commit_failures.push(commit_failed.clone());
        let evaluation_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        evaluations.push(evaluation_count.clone());
        let router = worker.router().layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let failed = failed.clone();
                let commit_failed = commit_failed.clone();
                let evaluation_count = evaluation_count.clone();
                async move {
                    if request.uri().path() == "/internal/evaluate" {
                        evaluation_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    if failed.load(std::sync::atomic::Ordering::SeqCst)
                        || (request.uri().path() == "/internal/commit"
                            && commit_failed.load(std::sync::atomic::Ordering::SeqCst))
                    {
                        let mut response = axum::response::IntoResponse::into_response(
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        );
                        if request.uri().path() == "/internal/evaluate" {
                            // This fixture rejected before invoking the worker.
                            response
                                .headers_mut()
                                .insert("x-enhance-evaluation", "not-accepted".parse().unwrap());
                        }
                        return response;
                    }
                    next.run(request).await
                }
            },
        ));
        let (url, task) = serve(router).await;
        workers.push(task);
        replicas.push(Replica {
            name: format!("replica-{index}"),
            url,
            incarnation: String::new(),
            ledger: Ledger::default(),
        });
    }
    let groups = vec![Group {
        placement_policy: Default::default(),
        id: "group-1".into(),
        sequence: 0,
        replicas,
    }];
    let coordinator_root = root.path().join("coordinator");
    let coordinator = Coordinator::open(&coordinator_root, groups.clone()).unwrap();
    let (origin, server) = serve(coordinator.clone().router()).await;
    let mut journal = RecordJournal::open(
        root.path().join("journal"),
        DatabaseId::Enhance,
        ENHANCE_LAYOUT,
    )
    .unwrap();
    let initial: Vec<_> = (0..67).map(record).collect();
    journal
        .append_block(3428143, "01".repeat(32), &initial)
        .unwrap();
    coordinator
        .publish(&journal, 3428143, "01".repeat(32))
        .await
        .unwrap();
    for replica in &groups[0].replicas {
        let metrics = reqwest::get(format!("{}/internal/metrics", replica.url))
            .await
            .unwrap();
        assert!(metrics.headers()[reqwest::header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .contains("version=0.0.4"));
        let text = metrics.text().await.unwrap();
        assert!(text.contains(&format!(
            "enhance_worker_live_database_bytes {}\n",
            2048 * 24576
        )));
        assert!(text.contains(&format!(
            "enhance_worker_latest_database_bytes {}\n",
            2048 * 24576
        )));
        assert!(text.contains("enhance_worker_candidate_present 0\n"));
        assert!(text.contains("enhance_worker_memory_model_available 1\n"));
        assert!(text.contains("enhance_worker_model_within_limit 1\n"));
    }
    let mut client = EnhancePirClient::connect(&origin).await.unwrap();
    for position in [0, 32, 33, 66] {
        let (got, _) = client.query_position_with_timing(position).await.unwrap();
        assert_eq!(got.as_ref(), record(position));
    }
    assert!(evaluations
        .iter()
        .all(|count| { count.load(std::sync::atomic::Ordering::Relaxed) >= 2 }));
    let old_generation = client.manifest().generation;
    for block in 1..=5u64 {
        let position = journal.tree_size();
        journal
            .append_block(
                3428143 + block,
                format!("{block:064x}"),
                &[record(position)],
            )
            .unwrap();
        if block == 1 {
            commit_failures[0].store(true, std::sync::atomic::Ordering::SeqCst);
            failures[1].store(true, std::sync::atomic::Ordering::SeqCst);
        }
        if block == 4 {
            failures[1].store(false, std::sync::atomic::Ordering::SeqCst);
        }
        let publication = coordinator
            .publish(&journal, 3428143 + block, format!("{block:064x}"))
            .await;
        if block == 1 {
            publication.unwrap(); // The decision is committed; notification retries are durable.
            let mut committed = EnhancePirClient::connect(&origin).await.unwrap();
            assert_eq!(
                committed
                    .query_position_with_timing(position)
                    .await
                    .unwrap()
                    .0
                    .as_ref(),
                record(position)
            );
            commit_failures[0].store(false, std::sync::atomic::Ordering::SeqCst);
            coordinator.reconcile().await.unwrap();
        } else {
            publication.unwrap();
        }
        let health: serde_json::Value = reqwest::get(format!("{origin}/v1/health"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let ready = if block < 4 { 1 } else { 2 };
        assert_eq!(health["published_replica_counts"]["group-1"], ready);
        let metrics = reqwest::get(format!("{origin}/metrics"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(metrics.contains(&format!(
            "enhance_shard_published_ready_replicas{{shard=\"0\"}} {ready}\n"
        )));
        if block == 4 {
            // The second replica missed three publications. It must rebuild the
            // current complete assignment and answer newly appended data without
            // needing those missing intermediate generations.
            failures[0].store(true, std::sync::atomic::Ordering::SeqCst);
            let mut recovered = EnhancePirClient::connect(&origin).await.unwrap();
            assert_eq!(
                recovered
                    .query_position_with_timing(position)
                    .await
                    .unwrap()
                    .0
                    .as_ref(),
                record(position)
            );
            failures[0].store(false, std::sync::atomic::Ordering::SeqCst);
        }
        let (got, _) = client.query_position_with_timing(33).await.unwrap();
        assert_eq!(got.as_ref(), record(33));
    }
    assert_ne!(
        client.manifest().generation,
        old_generation,
        "expired sessions refresh with fresh queries"
    );
    let expired = reqwest::get(format!("{origin}/v1/enhance/sessions/{old_generation}/0"))
        .await
        .unwrap();
    assert_eq!(expired.status(), reqwest::StatusCode::GONE);
    drop(expired);
    assert!(!coordinator_root
        .join("snapshots")
        .join(format!("{old_generation}.json"))
        .exists());
    for directory in ["snapshots", "hints"] {
        assert_eq!(
            std::fs::read_dir(coordinator_root.join(directory))
                .unwrap()
                .count(),
            5,
            "each publication changed the frontier; only the five retained artifacts survive"
        );
    }
    // Interrupted preparation can leave unreferenced files. Cleanup must ignore
    // foreign files and remove owned orphans before the next publication.
    let orphan = coordinator_root
        .join("hints")
        .join(format!("{}.bin", "ff".repeat(32)));
    let foreign = coordinator_root.join("hints/operator-notes.txt");
    std::fs::write(&orphan, b"orphan").unwrap();
    std::fs::write(&foreign, b"preserve").unwrap();
    let current_snapshot = coordinator_root.join("snapshots").join(format!(
        "{}.json",
        coordinator.manifest().await.unwrap().generation
    ));
    let saved_snapshot = std::fs::read(&current_snapshot).unwrap();
    std::fs::write(&current_snapshot, b"{}").unwrap();
    coordinator.reconcile().await.unwrap();
    assert!(
        orphan.exists(),
        "an unreadable retained snapshot must prevent deletion"
    );
    std::fs::write(&current_snapshot, saved_snapshot).unwrap();
    coordinator.reconcile().await.unwrap();
    assert!(!orphan.exists());
    assert_eq!(std::fs::read(&foreign).unwrap(), b"preserve");
    // Disable established keep-alive connections too; aborting the listener alone does not.
    failures[0].store(true, std::sync::atomic::Ordering::SeqCst);
    workers[0].abort();
    let _ = (&mut workers[0]).await;
    journal
        .append_block(3428149, "09".repeat(32), &[record(journal.tree_size())])
        .unwrap();
    coordinator
        .publish(&journal, 3428149, "09".repeat(32))
        .await
        .unwrap();
    let (got, _) = client.query_position_with_timing(66).await.unwrap();
    assert_eq!(got.as_ref(), record(66));
    drop(client);
    server.abort();
    let _ = server.await;
    drop(coordinator);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let restored = Coordinator::open(&coordinator_root, groups).unwrap();
    let (origin, server) = serve(restored.router()).await;
    // Publication health stays observable across a restart, before any new block.
    let metrics = reqwest::get(format!("{origin}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics
        .lines()
        .any(|l| l.starts_with("enhance_publication_last_advancement_unix_seconds ")));
    let mut client = EnhancePirClient::connect(&origin).await.unwrap();
    assert_eq!(
        client
            .query_position_with_timing(33)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(33)
    );
    server.abort();
    for worker in workers {
        worker.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full 32K lifecycle fixture; run on an isolated host with at least 16 GiB RAM"]
async fn full_shard_composition_confirmation_and_recovery() {
    use enhance_pir::protocol::ShardState;
    let root = tempfile::tempdir().unwrap();
    let mut tasks = Vec::new();
    let mut replicas = Vec::new();
    let hold = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let evaluated = std::sync::Arc::new(tokio::sync::Notify::new());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    for index in 0..2 {
        let worker = Worker::open(&root.path().join(format!("worker-{index}"))).unwrap();
        let hold = hold.clone();
        let evaluated = evaluated.clone();
        let release = release.clone();
        let router = worker.router().layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let hold = hold.clone();
                let evaluated = evaluated.clone();
                let release = release.clone();
                async move {
                    let evaluation = request.uri().path() == "/internal/evaluate";
                    let response = next.run(request).await;
                    if evaluation && hold.load(std::sync::atomic::Ordering::SeqCst) {
                        evaluated.notify_one();
                        release.notified().await;
                    }
                    response
                }
            },
        ));
        let (url, task) = serve(router).await;
        tasks.push(task);
        replicas.push(Replica {
            name: format!("replica-{index}"),
            url,
            incarnation: String::new(),
            ledger: Ledger::default(),
        });
    }
    let groups = vec![Group {
        placement_policy: Default::default(),
        id: "group-1".into(),
        sequence: 0,
        replicas,
    }];
    let control = root.path().join("control");
    let coordinator = Coordinator::open(&control, groups.clone()).unwrap();
    let (origin, task) = serve(coordinator.clone().router()).await;
    tasks.push(task);
    let mut journal = RecordJournal::open(
        root.path().join("journal"),
        DatabaseId::Enhance,
        ENHANCE_LAYOUT,
    )
    .unwrap();
    let span = 32768 * 33u64;
    let floor = 4096 * 33u64;
    let mut height = 3428143u64;
    let mut sealed_session = None;
    let mut rollback_anchor = None;
    // Includes exact full A, first B row, threshold at first record in row m,
    // and a second composition window. Every append is a complete block.
    for target in [
        span,
        span + 1,
        span + floor - 33 + 1,
        2 * span + 1,
        2 * span + floor - 33 + 1,
    ] {
        let records: Vec<_> = (journal.tree_size()..target).map(record).collect();
        journal
            .append_block(height, format!("{height:064x}"), &records)
            .unwrap();
        drop(records);
        coordinator
            .publish(&journal, height, format!("{height:064x}"))
            .await
            .unwrap();
        let mut client = EnhancePirClient::connect(&origin).await.unwrap();
        let manifest = client.manifest().clone();
        manifest.validate().unwrap();
        let composed = target % span == 1;
        assert_eq!(
            manifest.coverage.shards.last().unwrap().composed(),
            composed
        );
        if target == span {
            assert_eq!(manifest.coverage.shards.len(), 1);
        }
        for position in [0, span - floor, span - 1, target - 1] {
            let (got, _) = client.query_position_with_timing(position).await.unwrap();
            assert_eq!(got.as_ref(), record(position));
        }
        if composed {
            let successor = target / span;
            let (_, local, _) = manifest.coverage.locate(successor * span).unwrap();
            assert_eq!(local, 0);
            let (_, local, _) = manifest.coverage.locate(successor * span - floor).unwrap();
            assert_eq!(local, 4096);
        }
        let stale = client.session(0).await.unwrap().prepare_row(0).unwrap();
        // Confirm from the completing block, with no new record bytes.
        for _ in 0..1000 {
            height += 1;
            journal
                .append_block::<Vec<u8>>(height, format!("{height:064x}"), &[])
                .unwrap();
        }
        if target == 2 * span + 1 {
            rollback_anchor = journal.last_block().cloned();
        }
        coordinator
            .publish(&journal, height, format!("{height:064x}"))
            .await
            .unwrap();
        let confirmed = coordinator.manifest().await.unwrap();
        assert_eq!(confirmed.coverage.shards[0].state, ShardState::Sealed);
        assert_eq!(
            manifest.session_id(0).unwrap(),
            confirmed.session_id(0).unwrap()
        );
        if let Some(id) = sealed_session {
            assert_eq!(id, confirmed.session_id(0).unwrap());
        }
        sealed_session = Some(confirmed.session_id(0).unwrap());
        let response = reqwest::Client::new()
            .post(format!("{origin}/v1/enhance/query"))
            .body(stale.body().to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["code"],
            "stale_routing"
        );
        height += 1;
    }
    // Ordinary rollback of only the provisional last domain preserves seals.
    let current = coordinator.manifest().await.unwrap();
    let previous = rollback_anchor.unwrap();
    coordinator
        .revoke_after(previous.first_position + previous.action_count)
        .await
        .unwrap();
    journal.rewind_to_height(Some(previous.height)).unwrap();
    coordinator
        .publish(&journal, previous.height, previous.hash.clone())
        .await
        .unwrap();
    assert_eq!(coordinator.manifest().await.unwrap().recovery_epoch, 0);
    assert_eq!(
        coordinator.manifest().await.unwrap().session_id(0).unwrap(),
        current.session_id(0).unwrap()
    );
    // Fence an admitted request after worker evaluation but before packing/return.
    let mut client = EnhancePirClient::connect(&origin).await.unwrap();
    let query = client.session(0).await.unwrap().prepare_row(0).unwrap();
    hold.store(true, std::sync::atomic::Ordering::SeqCst);
    let url = format!("{origin}/v1/enhance/query");
    let pending = tokio::spawn(async move {
        reqwest::Client::new()
            .post(url)
            .body(query.body().to_vec())
            .send()
            .await
            .unwrap()
    });
    tokio::time::timeout(std::time::Duration::from_secs(30), evaluated.notified())
        .await
        .unwrap();
    coordinator.revoke_after(0).await.unwrap();
    coordinator.revoke_after(0).await.unwrap(); // retry must not increment twice
    hold.store(false, std::sync::atomic::Ordering::SeqCst);
    release.notify_one();
    let response = pending.await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::GONE);
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["code"],
        "noncanonical_session"
    );
    journal.rewind_to_height(None).unwrap();
    journal
        .append_block(3428143, "fe".repeat(32), &[record(0)])
        .unwrap();
    coordinator
        .publish(&journal, 3428143, "fe".repeat(32))
        .await
        .unwrap();
    let recovered = coordinator.manifest().await.unwrap();
    assert_eq!(recovered.recovery_epoch, 1);
    assert_eq!(recovered.domain_recovery_epochs[&0], "1");
    assert_ne!(recovered.session_id(0).unwrap(), sealed_session.unwrap());
    let mut client = EnhancePirClient::connect(&origin).await.unwrap();
    assert_eq!(
        client
            .query_position_with_timing(0)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(0)
    );
    // One replayed canonical block crosses two fixed storage boundaries.
    // Even byte-identical rebuilt A cannot resurrect its pre-recovery session.
    let target = 2 * span + 7;
    let records: Vec<_> = (1..target).map(record).collect();
    journal
        .append_block(3428144, "fd".repeat(32), &records)
        .unwrap();
    drop(records);
    coordinator
        .publish(&journal, 3428144, "fd".repeat(32))
        .await
        .unwrap();
    let rebuilt = coordinator.manifest().await.unwrap();
    assert_eq!(rebuilt.coverage.shards.len(), 3);
    assert_eq!(rebuilt.recovery_epoch, 1);
    assert_ne!(rebuilt.session_id(0).unwrap(), sealed_session.unwrap());
    let mut client = EnhancePirClient::connect(&origin).await.unwrap();
    for position in [span - 1, span, 2 * span - 1, 2 * span, target - 1] {
        assert_eq!(
            client
                .query_position_with_timing(position)
                .await
                .unwrap()
                .0
                .as_ref(),
            record(position)
        );
    }
    for task in tasks {
        task.abort();
    }
}

#[tokio::test]
async fn inventory_expansion_is_atomic_idempotent_and_survives_restart() {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    let root = tempfile::tempdir().unwrap();
    let mut tasks = Vec::new();
    let mut groups = Vec::new();
    let fail_last = Arc::new(AtomicBool::new(true));
    for sequence in 0..2 {
        let mut replicas = Vec::new();
        for index in 0..2 {
            let name = format!("group-{sequence}-replica-{index}");
            let worker = Worker::open(&root.path().join(&name)).unwrap();
            let fail = fail_last.clone();
            let router = worker.router().layer(axum::middleware::from_fn(
                move |request: axum::extract::Request, next: axum::middleware::Next| {
                    let fail = fail.clone();
                    async move {
                        if sequence == 1 && index == 1 && fail.load(Ordering::SeqCst) {
                            return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
                        }
                        next.run(request).await
                    }
                },
            ));
            let (url, task) = serve(router).await;
            tasks.push(task);
            replicas.push(Replica {
                name,
                url,
                incarnation: String::new(),
                ledger: Ledger::default(),
            });
        }
        groups.push(Group {
            placement_policy: Default::default(),
            id: format!("group-{sequence}"),
            sequence,
            replicas,
        });
    }
    use axum::response::IntoResponse;
    let path = root.path().join("controller");
    let coordinator = Coordinator::open(&path, groups[..1].to_vec()).unwrap();
    let read_state = || -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(path.join("controller.json")).unwrap()).unwrap()
    };
    let initial = read_state();
    let mut invalid = groups.clone();
    invalid[1].replicas[0].name.clear();
    assert!(coordinator.reconcile_inventory(invalid).await.is_err());
    let mut duplicate = groups.clone();
    duplicate[1].replicas[0].url = format!("{}/", groups[0].replicas[0].url);
    assert!(coordinator.reconcile_inventory(duplicate).await.is_err());
    assert!(coordinator
        .reconcile_inventory(groups.clone())
        .await
        .is_err());
    assert_eq!(
        read_state(),
        initial,
        "one ready replica cannot register a pair"
    );
    let mut changed = groups.clone();
    changed[0].replicas[0].url = "http://127.0.0.1:1".into();
    assert!(coordinator.reconcile_inventory(changed).await.is_err());
    assert_eq!(read_state(), initial);
    fail_last.store(false, Ordering::SeqCst);
    coordinator
        .reconcile_inventory(groups.clone())
        .await
        .unwrap();
    let registered = read_state();
    assert_eq!(registered["groups"].as_array().unwrap().len(), 2);
    assert_eq!(
        registered["revision"].as_u64().unwrap(),
        initial["revision"].as_u64().unwrap() + 1
    );
    // A replay must be a no-op even if the new worker subsequently goes down.
    fail_last.store(true, Ordering::SeqCst);
    coordinator
        .reconcile_inventory(groups.clone())
        .await
        .unwrap();
    assert_eq!(read_state(), registered);
    assert!(coordinator
        .reconcile_inventory(groups[..1].to_vec())
        .await
        .is_err());
    drop(coordinator);
    assert!(Coordinator::open(&path, groups[..1].to_vec()).is_err());
    let reopened = Coordinator::open(&path, groups.clone()).unwrap();
    reopened.reconcile_inventory(groups).await.unwrap();
    assert_eq!(read_state()["groups"], registered["groups"]);
    assert_eq!(read_state()["revision"], registered["revision"]);
    for task in tasks {
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "seven shard domains across four workers; requires at least 32 GiB RAM and 64 GiB free disk"]
async fn consolidation_reservation_failure_retries_canonical_update_then_moves_sealed_shard() {
    consolidation_campaign(6).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "eight shard domains across four workers; requires 32 GiB RAM and 64 GiB free disk"]
async fn seven_sealed_consolidation_preserves_retained_queries() {
    consolidation_campaign(7).await;
}

async fn consolidation_campaign(sealed_shards: usize) {
    let policy = enhance_serving::control::PlacementPolicy { sealed_shards };
    use axum::{
        body::{to_bytes, Body},
        response::IntoResponse,
    };
    use std::sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    };
    let root = tempfile::tempdir().unwrap();
    let fail_reservation = Arc::new(AtomicBool::new(true));
    let failed_attempt = Arc::new(AtomicU64::new(u64::MAX));
    let mut groups = Vec::new();
    let mut tasks = Vec::new();
    for sequence in 0..2 {
        let mut replicas = Vec::new();
        for index in 0..2 {
            let name = format!("g{sequence}-r{index}");
            let worker = Worker::open_with_policy(&root.path().join(&name), policy).unwrap();
            let fail = fail_reservation.clone();
            let observed_attempt = failed_attempt.clone();
            let router = worker.router().layer(axum::middleware::from_fn(
                move |request: axum::extract::Request, next: axum::middleware::Next| {
                    let fail = fail.clone();
                    let observed_attempt = observed_attempt.clone();
                    async move {
                        if sequence == 0
                            && index == 1
                            && request.uri().path() == "/internal/reserve"
                        {
                            let (parts, body) = request.into_parts();
                            let bytes = to_bytes(body, 1024 * 1024).await.unwrap();
                            let candidate: enhance_serving::worker::Candidate =
                                serde_json::from_slice(&bytes).unwrap();
                            if candidate.plans.len() == sealed_shards
                                && fail.swap(false, Ordering::SeqCst)
                            {
                                observed_attempt.store(candidate.attempt, Ordering::SeqCst);
                                return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
                            }
                            return next
                                .run(axum::extract::Request::from_parts(parts, Body::from(bytes)))
                                .await;
                        }
                        next.run(request).await
                    }
                },
            ));
            let (url, task) = serve(router).await;
            tasks.push(task);
            replicas.push(Replica {
                name,
                url,
                incarnation: String::new(),
                ledger: Ledger::default(),
            });
        }
        groups.push(Group {
            placement_policy: policy,
            id: format!("g{sequence}"),
            sequence,
            replicas,
        });
    }
    let path = root.path().join("control");
    let coordinator = Coordinator::open(&path, groups).unwrap();
    let (origin, server) = serve(coordinator.clone().router()).await;
    tasks.push(server);
    let mut journal = RecordJournal::open(
        root.path().join("journal"),
        DatabaseId::Enhance,
        ENHANCE_LAYOUT,
    )
    .unwrap();
    let mut height = 3428143;
    // Bounded fixture construction rather than keeping several GiB of records in a Vec.
    for target in (5..=sealed_shards as u64).map(|n| (n * 32768 + 4096) * 33) {
        while journal.tree_size() < target {
            let start = journal.tree_size();
            let end = (start + 8192).min(target);
            let records: Vec<_> = (start..end).map(record).collect();
            journal
                .append_block(height, format!("{height:064x}"), &records)
                .unwrap();
            height += 1;
        }
        coordinator
            .publish(&journal, height - 1, format!("{:064x}", height - 1))
            .await
            .unwrap();
    }
    assert!(
        !fail_reservation.load(Ordering::SeqCst),
        "reservation failure must have fired"
    );
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path.join("controller.json")).unwrap()).unwrap();
    assert_eq!(
        state["assignments"][(sealed_shards - 1).to_string()],
        "g1",
        "canonical update used the original placement"
    );
    assert!(state["operation"].is_null());
    assert_eq!(
        state["next_attempt"].as_u64().unwrap(),
        failed_attempt.load(Ordering::SeqCst) + 2
    );
    let mut before_move = EnhancePirClient::connect(&origin).await.unwrap();
    let position = (sealed_shards as u64 - 1) * 32768 * 33;
    assert_eq!(
        before_move
            .query_position_with_timing(position)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(position)
    );
    // Retry consolidation in a later generation after pressure has cleared.
    coordinator
        .publish(&journal, height - 1, format!("{:064x}", height - 1))
        .await
        .unwrap();
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path.join("controller.json")).unwrap()).unwrap();
    assert_eq!(state["assignments"][(sealed_shards - 1).to_string()], "g0");
    assert_eq!(
        state["assignments"]
            .as_object()
            .unwrap()
            .values()
            .filter(|v| v.as_str() == Some("g0"))
            .count(),
        sealed_shards
    );
    let mut after_move = EnhancePirClient::connect(&origin).await.unwrap();
    assert_eq!(
        after_move
            .query_position_with_timing(position)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(position)
    );
    assert_eq!(
        before_move
            .query_position_with_timing(position)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(position)
    );
    for task in tasks {
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_shard_uses_admitted_alternative_group_over_http() {
    use axum::response::IntoResponse;
    let root = tempfile::tempdir().unwrap();
    let refuse = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut tasks = Vec::new();
    let mut groups = Vec::new();
    for sequence in 0..2 {
        let mut replicas = Vec::new();
        for index in 0..2 {
            let name = format!("g{sequence}-r{index}");
            let refuse = refuse.clone();
            let router = Worker::open(&root.path().join(&name))
                .unwrap()
                .router()
                .layer(axum::middleware::from_fn(
                    move |request: axum::extract::Request, next: axum::middleware::Next| {
                        let refuse = refuse.clone();
                        async move {
                            if sequence == 0
                                && index == 1
                                && request.uri().path() == "/internal/admit"
                            {
                                return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
                            }
                            if sequence == 1
                                && index == 1
                                && request.uri().path() == "/internal/reserve"
                                && refuse.load(std::sync::atomic::Ordering::SeqCst)
                            {
                                return axum::http::StatusCode::INSUFFICIENT_STORAGE
                                    .into_response();
                            }
                            next.run(request).await
                        }
                    },
                ));
            let (url, task) = serve(router).await;
            tasks.push(task);
            replicas.push(Replica {
                name,
                url,
                incarnation: String::new(),
                ledger: Ledger::default(),
            });
        }
        groups.push(Group {
            placement_policy: Default::default(),
            id: format!("g{sequence}"),
            sequence,
            replicas,
        });
    }
    let inventory = serde_json::json!({"groups": groups.iter().map(|g| serde_json::json!({
        "name":g.id, "replicas":g.replicas.iter().map(|r| serde_json::json!({"name":r.name,"url":r.url})).collect::<Vec<_>>()
    })).collect::<Vec<_>>()});
    let inventory_path = root.path().join("inventory.json");
    let policy_path = root.path().join("policy.json");
    std::fs::write(&inventory_path, serde_json::to_vec(&inventory).unwrap()).unwrap();
    std::fs::write(&policy_path, br#"{"kind":"isolated-test-policy"}"#).unwrap();
    let coordinator = Coordinator::open(&root.path().join("control"), groups).unwrap();
    let (origin, server) = serve(coordinator.clone().router()).await;
    let mut journal = RecordJournal::open(
        root.path().join("journal"),
        DatabaseId::Enhance,
        ENHANCE_LAYOUT,
    )
    .unwrap();
    journal
        .append_block(
            3428143,
            "01".repeat(32),
            &(0..67).map(record).collect::<Vec<_>>(),
        )
        .unwrap();
    assert!(coordinator
        .publish(&journal, 3428143, "01".repeat(32))
        .await
        .is_err());
    assert!(coordinator.manifest().await.is_none());
    let blocked: serde_json::Value = reqwest::get(format!("{origin}/v1/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(blocked["capacity"]["memory_limit"]["records"], 67);
    let demand = blocked["capacity"]["requested"].as_str().unwrap();
    assert_eq!(blocked["capacity"]["requests"][demand]["target_groups"], 3);
    // Run the actual operations entrypoint against live coordinator health. This
    // freezes demand and inputs only; no provider calls or provisioning occur.
    let operations = root.path().join("operations");
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../ops/enhance/scripts/expansion-journal.py");
    let observe = || {
        let output = std::process::Command::new("python3")
            .arg(&script)
            .arg("--state-dir")
            .arg(&operations)
            .arg("--coordinator-url")
            .arg(&origin)
            .arg("--inventory")
            .arg(&inventory_path)
            .arg("--policy")
            .arg(&policy_path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["operation"], demand);
        assert_eq!(result["phase"], "requested");
        assert_eq!(result["target_groups"], 3);
    };
    observe();
    let persisted = std::fs::read(operations.join("journal.json")).unwrap();
    observe();
    assert_eq!(
        std::fs::read(operations.join("journal.json")).unwrap(),
        persisted
    );
    refuse.store(false, std::sync::atomic::Ordering::SeqCst);
    coordinator.reconcile().await.unwrap();
    coordinator
        .publish(&journal, 3428143, "01".repeat(32))
        .await
        .unwrap();
    let health: serde_json::Value = reqwest::get(format!("{origin}/v1/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    observe();
    assert_eq!(
        std::fs::read(operations.join("journal.json")).unwrap(),
        persisted,
        "successful publication must not replace unsatisfied provisioning demand"
    );
    assert_eq!(health["published_replica_counts"]["g1"], 2);
    assert!(health["published_replica_counts"].get("g0").is_none());
    let mut client = EnhancePirClient::connect(&origin).await.unwrap();
    for position in [0, 32, 33, 66] {
        assert_eq!(
            client
                .query_position_with_timing(position)
                .await
                .unwrap()
                .0
                .as_ref(),
            record(position)
        );
    }
    server.abort();
    for task in tasks {
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_refusal_moves_published_shard_with_complete_destination_pair() {
    use axum::response::IntoResponse;
    use std::sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    };
    let root = tempfile::tempdir().unwrap();
    // 0: normal; 1: source busy; 2: one source memory refusal;
    // 3: both source refusals plus unavailable destination peer; 4: move allowed.
    let mode = Arc::new(AtomicU8::new(0));
    let mut tasks = Vec::new();
    let mut groups = Vec::new();
    for sequence in 0..2 {
        let mut replicas = Vec::new();
        for index in 0..2 {
            let name = format!("g{sequence}-r{index}");
            let mode = mode.clone();
            let router = Worker::open(&root.path().join(&name))
                .unwrap()
                .router()
                .layer(axum::middleware::from_fn(
                    move |request: axum::extract::Request, next: axum::middleware::Next| {
                        let mode = mode.clone();
                        async move {
                            let current = mode.load(Ordering::SeqCst);
                            if matches!(
                                request.uri().path(),
                                "/internal/admit" | "/internal/reserve"
                            ) {
                                if sequence == 0 && current == 1 {
                                    return axum::http::StatusCode::SERVICE_UNAVAILABLE
                                        .into_response();
                                }
                                if sequence == 0 && (current >= 3 || (current == 2 && index == 0)) {
                                    return axum::http::StatusCode::INSUFFICIENT_STORAGE
                                        .into_response();
                                }
                                if sequence == 1
                                    && index == 1
                                    && (current == 3
                                        || (current == 5
                                            && request.uri().path() == "/internal/reserve"))
                                {
                                    return axum::http::StatusCode::SERVICE_UNAVAILABLE
                                        .into_response();
                                }
                            }
                            next.run(request).await
                        }
                    },
                ));
            let (url, task) = serve(router).await;
            tasks.push(task);
            replicas.push(Replica {
                name,
                url,
                incarnation: String::new(),
                ledger: Ledger::default(),
            });
        }
        groups.push(Group {
            placement_policy: Default::default(),
            id: format!("g{sequence}"),
            sequence,
            replicas,
        });
    }
    let coordinator = Coordinator::open(&root.path().join("control"), groups).unwrap();
    let (origin, server) = serve(coordinator.clone().router()).await;
    let mut journal = RecordJournal::open(
        root.path().join("journal"),
        DatabaseId::Enhance,
        ENHANCE_LAYOUT,
    )
    .unwrap();
    journal
        .append_block(
            3428143,
            "01".repeat(32),
            &(0..67).map(record).collect::<Vec<_>>(),
        )
        .unwrap();
    coordinator
        .publish(&journal, 3428143, "01".repeat(32))
        .await
        .unwrap();
    let initial = coordinator.manifest().await.unwrap();
    let mut retained = EnhancePirClient::connect(&origin).await.unwrap();
    assert_eq!(
        retained
            .query_position_with_timing(33)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(33)
    );
    journal
        .append_block(3428144, "02".repeat(32), &[record(67)])
        .unwrap();
    mode.store(1, Ordering::SeqCst);
    assert!(coordinator
        .publish(&journal, 3428144, "02".repeat(32))
        .await
        .is_err());
    assert_eq!(
        coordinator.manifest().await.unwrap(),
        initial,
        "busy sources must not trigger relocation"
    );
    coordinator.reconcile().await.unwrap();
    mode.store(2, Ordering::SeqCst);
    coordinator
        .publish(&journal, 3428144, "02".repeat(32))
        .await
        .unwrap();
    let health: serde_json::Value = reqwest::get(format!("{origin}/v1/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        health["published_replica_counts"]["g0"], 1,
        "healthy source peer keeps ordinary placement"
    );
    assert!(health["published_replica_counts"].get("g1").is_none());
    coordinator.reconcile().await.unwrap();
    journal
        .append_block(3428145, "03".repeat(32), &[record(68)])
        .unwrap();
    let before_move = coordinator.manifest().await.unwrap();
    mode.store(3, Ordering::SeqCst);
    assert!(coordinator
        .publish(&journal, 3428145, "03".repeat(32))
        .await
        .is_err());
    assert_eq!(
        coordinator.manifest().await.unwrap(),
        before_move,
        "one destination replica cannot admit the move"
    );
    coordinator.reconcile().await.unwrap();
    mode.store(5, Ordering::SeqCst);
    assert!(coordinator
        .publish(&journal, 3428145, "03".repeat(32))
        .await
        .is_err());
    assert_eq!(
        coordinator.manifest().await.unwrap(),
        before_move,
        "destination failure after preflight must preserve published placement"
    );
    coordinator.reconcile().await.unwrap();
    mode.store(4, Ordering::SeqCst);
    coordinator
        .publish(&journal, 3428145, "03".repeat(32))
        .await
        .unwrap();
    let health: serde_json::Value = reqwest::get(format!("{origin}/v1/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["published_replica_counts"]["g1"], 2);
    assert!(health["published_replica_counts"].get("g0").is_none());
    let mut current = EnhancePirClient::connect(&origin).await.unwrap();
    for position in [0, 32, 33, 66, 67, 68] {
        assert_eq!(
            current
                .query_position_with_timing(position)
                .await
                .unwrap()
                .0
                .as_ref(),
            record(position)
        );
    }
    assert_eq!(
        retained
            .query_position_with_timing(66)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(66)
    );
    server.abort();
    for task in tasks {
        task.abort();
    }
}

#[tokio::test]
async fn worker_revocation_survives_restart_and_rejects_regression() {
    use enhance_serving::worker::{Evaluate, Revocation};
    let root = tempfile::tempdir().unwrap();
    let session_id = "ab".repeat(32);
    let fence = Revocation {
        recovery_epoch: 1,
        sessions: [session_id.clone()].into(),
    };
    for restart in 0..2 {
        let worker = Worker::open(root.path()).unwrap();
        let router = worker.router().layer(axum::middleware::from_fn(
            |request: axum::extract::Request, next: axum::middleware::Next| async move {
                let mut response = next.run(request).await;
                response.headers_mut().insert(
                    axum::http::header::CONNECTION,
                    axum::http::HeaderValue::from_static("close"),
                );
                response
            },
        ));
        let (origin, server) = serve(router).await;
        let http = reqwest::Client::new();
        if restart == 0 {
            http.post(format!("{origin}/internal/revoke"))
                .json(&fence)
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap();
        }
        // The durable fence takes precedence even over generation lookup.
        let response = http
            .post(format!("{origin}/internal/evaluate"))
            .json(&Evaluate {
                binding: None,
                generation: 1,
                shard_id: 0,
                epoch: "00".repeat(8),
                session_id: session_id.clone(),
                coefficients: vec![],
            })
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::GONE);
        assert_eq!(response.text().await.unwrap(), "noncanonical_session");
        let response = http
            .post(format!("{origin}/internal/revoke"))
            .json(&Revocation::default())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        drop(response);
        drop(http);
        server.abort();
        let _ = server.await;
    }
}
