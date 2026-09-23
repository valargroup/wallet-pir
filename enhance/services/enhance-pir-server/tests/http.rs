//! Real encrypted requests traverse coordinator and two worker HTTP listeners.
use enhance_pir::{client::EnhancePirClient, RECORD_BYTES};
use enhance_pir_server::{
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
    let root = tempfile::tempdir().unwrap();
    let mut workers = Vec::new();
    let mut replicas = Vec::new();
    let mut failures = Vec::new();
    let mut commit_failures = Vec::new();
    for index in 0..2 {
        let worker = Worker::open(&root.path().join(format!("worker-{index}"))).unwrap();
        let failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        failures.push(failed.clone());
        let commit_failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        commit_failures.push(commit_failed.clone());
        let router = worker.router().layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let failed = failed.clone();
                let commit_failed = commit_failed.clone();
                async move {
                    if failed.load(std::sync::atomic::Ordering::SeqCst)
                        || (request.uri().path() == "/internal/commit"
                            && commit_failed.load(std::sync::atomic::Ordering::SeqCst))
                    {
                        return axum::response::IntoResponse::into_response(
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        );
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
        settling: false,
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
async fn full_shard_loan_return_over_http_preserves_old_queries() {
    let root = tempfile::tempdir().unwrap();
    let mut tasks = Vec::new();
    let mut replicas = Vec::new();
    for index in 0..2 {
        let worker = Worker::open(&root.path().join(format!("worker-{index}"))).unwrap();
        let (url, task) = serve(worker.router()).await;
        tasks.push(task);
        replicas.push(Replica {
            name: format!("replica-{index}"),
            url,
            incarnation: String::new(),
            ledger: Ledger::default(),
        });
    }
    let coordinator = Coordinator::open(
        &root.path().join("control"),
        vec![Group {
            placement_policy: Default::default(),
            id: "group-1".into(),
            sequence: 0,
            replicas,
            settling: false,
        }],
    )
    .unwrap();
    let (origin, task) = serve(coordinator.clone().router()).await;
    tasks.push(task);
    let mut journal = RecordJournal::open(
        root.path().join("journal"),
        DatabaseId::Enhance,
        ENHANCE_LAYOUT,
    )
    .unwrap();
    let boundary = 32768 * 33u64;
    let borrowed = (32768 - 4096) * 33u64;
    let initial: Vec<_> = (0..boundary - 1).map(record).collect();
    journal
        .append_block(3428143, "01".repeat(32), &initial)
        .unwrap();
    drop(initial);
    coordinator
        .publish(&journal, 3428143, "01".repeat(32))
        .await
        .unwrap();
    let mut before = EnhancePirClient::connect(&origin).await.unwrap();
    let session = before.session(0).await.unwrap();
    let (prepared, slot) = session.prepare_position(borrowed).unwrap();
    journal
        .append_block(3428144, "02".repeat(32), &[record(boundary - 1)])
        .unwrap();
    coordinator
        .publish(&journal, 3428144, "02".repeat(32))
        .await
        .unwrap();
    let mut during = EnhancePirClient::connect(&origin).await.unwrap();
    assert_eq!(during.manifest().coverage.locate(borrowed).unwrap().0.id, 1);
    assert_eq!(
        during
            .query_position_with_timing(borrowed)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(borrowed)
    );
    let response = reqwest::Client::new()
        .post(format!("{origin}/v1/enhance/query"))
        .body(prepared.body().to_vec())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let row = session.decode(prepared, &response).unwrap();
    assert_eq!(
        &row[slot * RECORD_BYTES..(slot + 1) * RECORD_BYTES],
        record(borrowed)
    );
    let (old_borrower_query, old_slot) = during
        .session(1)
        .await
        .unwrap()
        .prepare_position(borrowed)
        .unwrap();
    let own: Vec<_> = (boundary..boundary + 4096 * 33).map(record).collect();
    journal
        .append_block(3428145, "03".repeat(32), &own)
        .unwrap();
    drop(own);
    coordinator
        .publish(&journal, 3428145, "03".repeat(32))
        .await
        .unwrap();
    let mut after = EnhancePirClient::connect(&origin).await.unwrap();
    assert!(after.manifest().coverage.loan.is_none());
    assert_eq!(after.manifest().coverage.locate(borrowed).unwrap().0.id, 0);
    assert_eq!(after.manifest().coverage.locate(boundary).unwrap().0.id, 1);
    let positions = [
        borrowed,
        boundary - 1,
        boundary,
        boundary + 4096 * 33 - 1,
        boundary,
    ];
    for (got, position) in after
        .query_positions(&positions)
        .await
        .unwrap()
        .iter()
        .zip(positions)
    {
        assert_eq!(got.as_ref(), record(position));
    }
    let response = reqwest::Client::new()
        .post(format!("{origin}/v1/enhance/query"))
        .body(old_borrower_query.body().to_vec())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let row = during
        .session(1)
        .await
        .unwrap()
        .decode(old_borrower_query, &response)
        .unwrap();
    assert_eq!(
        &row[old_slot * RECORD_BYTES..(old_slot + 1) * RECORD_BYTES],
        record(borrowed)
    );

    // A canonical-anchor change after preparation must leave the complete old
    // generation queryable. Recovery aborts that candidate before republishing.
    let returned_manifest = after.manifest().clone();
    journal.rewind_to_height(Some(3428144)).unwrap();
    let rejected = coordinator
        .publish_checked(&journal, 3428144, "02".repeat(32), || async {
            Err("injected canonical anchor change".into())
        })
        .await;
    assert!(rejected.unwrap_err().contains("canonical anchor change"));
    assert_eq!(coordinator.manifest().await.unwrap(), returned_manifest);
    coordinator.reconcile().await.unwrap();
    coordinator
        .publish(&journal, 3428144, "02".repeat(32))
        .await
        .unwrap();
    let mut undone_return = EnhancePirClient::connect(&origin).await.unwrap();
    assert!(undone_return.manifest().coverage.loan.is_some());
    assert!(undone_return.manifest().generation > returned_manifest.generation);
    assert_eq!(
        undone_return
            .manifest()
            .coverage
            .locate(borrowed)
            .unwrap()
            .0
            .id,
        1
    );
    assert_eq!(
        undone_return
            .query_position_with_timing(borrowed)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(borrowed)
    );
    // A retained session still answers records removed by the reorg. Its anchor
    // is explicit, and the wallet decides whether to accept that anchor.
    assert_eq!(
        after
            .query_position_with_timing(boundary)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(boundary)
    );

    journal.rewind_to_height(Some(3428143)).unwrap();
    coordinator
        .publish(&journal, 3428143, "01".repeat(32))
        .await
        .unwrap();
    let mut undone_split = EnhancePirClient::connect(&origin).await.unwrap();
    assert!(undone_split.manifest().coverage.loan.is_none());
    assert_eq!(undone_split.manifest().coverage.shards.len(), 1);
    assert_eq!(
        undone_split
            .query_position_with_timing(borrowed)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(borrowed)
    );

    // Reapply the split on a different branch: stable shard identity must not
    // allow a cached runtime for the old branch to supply the changed record.
    let mut replacement = record(boundary - 1);
    replacement[enhance_pir::types::RECORD_ENC_CIPHERTEXT_SUFFIX_OFFSET] = 0x99;
    journal
        .append_block(3428144, "04".repeat(32), &[replacement.clone()])
        .unwrap();
    coordinator
        .publish(&journal, 3428144, "04".repeat(32))
        .await
        .unwrap();
    let mut alternative = EnhancePirClient::connect(&origin).await.unwrap();
    assert_eq!(
        alternative
            .manifest()
            .coverage
            .locate(boundary - 1)
            .unwrap()
            .0
            .id,
        1
    );
    assert_eq!(
        alternative
            .query_position_with_timing(boundary - 1)
            .await
            .unwrap()
            .0
            .as_ref(),
        replacement
    );
    assert_eq!(
        during
            .query_position_with_timing(boundary - 1)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(boundary - 1)
    );
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
            settling: false,
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
    let policy = enhance_pir_server::control::PlacementPolicy { sealed_shards };
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
                            let candidate: enhance_pir_server::worker::Candidate =
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
            settling: false,
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
async fn committed_offline_participant_does_not_block_and_recovers_after_expiry() {
    use axum::response::IntoResponse;
    use enhance_pir_server::control::State as ControlState;
    use std::sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    };

    // 1 rejects commit, 2 is offline, 3 loses the response after durable commit,
    // 4 reports a conflicting published digest. Close HTTP connections so a
    // stopped listener releases all worker state before reopening its directory.
    async fn start_worker(
        root: &std::path::Path,
        address: &str,
        mode: Arc<AtomicU8>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let worker = Worker::open(root).unwrap();
        let router = worker.router().layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let mode = mode.clone();
                async move {
                    let mode = mode.load(Ordering::SeqCst);
                    let path = request.uri().path().to_owned();
                    let commit = path == "/internal/commit";
                    let mut response = if mode == 2 || (mode == 1 && commit) {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                    } else {
                        let response = next.run(request).await;
                        if mode == 3 && commit {
                            assert!(response.status().is_success());
                            axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                        } else if mode == 4 && path == "/internal/health" {
                            let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
                                .await
                                .unwrap();
                            let mut value: serde_json::Value =
                                serde_json::from_slice(&body).unwrap();
                            for digest in value["published_manifest_digests"]
                                .as_object_mut()
                                .unwrap()
                                .values_mut()
                            {
                                *digest = serde_json::json!("wrong");
                            }
                            axum::Json(value).into_response()
                        } else {
                            response
                        }
                    };
                    response
                        .headers_mut()
                        .insert(axum::http::header::CONNECTION, "close".parse().unwrap());
                    response
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        (
            url,
            tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            }),
        )
    }
    let root = tempfile::tempdir().unwrap();
    let modes = [Arc::new(AtomicU8::new(0)), Arc::new(AtomicU8::new(0))];
    let mut tasks = Vec::new();
    let mut replicas = Vec::new();
    for (index, mode) in modes.iter().enumerate() {
        let (url, task) = start_worker(
            &root.path().join(format!("worker-{index}")),
            "127.0.0.1:0",
            mode.clone(),
        )
        .await;
        tasks.push(task);
        replicas.push(Replica {
            name: format!("peer-{index}"),
            url,
            incarnation: String::new(),
            ledger: Ledger::default(),
        });
    }
    let groups = vec![Group {
        placement_policy: Default::default(),
        id: "g0".into(),
        sequence: 0,
        replicas,
        settling: false,
    }];
    let coordinator_root = root.path().join("coordinator");
    let read_state = || -> ControlState {
        serde_json::from_slice(&std::fs::read(coordinator_root.join("controller.json")).unwrap())
            .unwrap()
    };
    let mut coordinator = Coordinator::open(&coordinator_root, groups.clone()).unwrap();
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
    modes[0].store(1, Ordering::SeqCst);
    journal
        .append_block(3428144, "02".repeat(32), &[record(67)])
        .unwrap();
    coordinator
        .publish(&journal, 3428144, "02".repeat(32))
        .await
        .unwrap();
    let state = read_state();
    assert!(state.operation.is_none());
    assert_eq!(state.pending_commits.len(), 1);
    assert_eq!(state.pending_commits[0].replica, "peer-0");
    let delayed_generation = state.pending_commits[0].manifest.generation;
    modes[0].store(2, Ordering::SeqCst);
    drop(coordinator);
    coordinator = Coordinator::open(&coordinator_root, groups.clone()).unwrap();
    coordinator.reconcile().await.unwrap();
    assert_eq!(read_state().pending_commits, state.pending_commits);
    let (origin, server) = serve(coordinator.clone().router()).await;
    for block in 2..=6u64 {
        let position = journal.tree_size();
        let height = 3428143 + block;
        let hash = format!("{:064x}", block + 1);
        journal
            .append_block(height, hash.clone(), &[record(position)])
            .unwrap();
        if block == 2 {
            assert!(coordinator
                .publish_checked(&journal, height, hash.clone(), || async {
                    Err("canonical anchor changed".into())
                })
                .await
                .is_err());
            coordinator.reconcile().await.unwrap();
            assert_eq!(
                read_state().pending_commits,
                state.pending_commits,
                "aborting a later attempt must preserve the older committed candidate"
            );
        }
        coordinator.publish(&journal, height, hash).await.unwrap();
        assert_eq!(read_state().pending_commits, state.pending_commits);
        let mut client = EnhancePirClient::connect(&origin).await.unwrap();
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
    assert!(!read_state()
        .published
        .iter()
        .any(|m| m.generation == delayed_generation));
    assert!(!coordinator_root
        .join("snapshots")
        .join(format!("{delayed_generation}.json"))
        .exists());
    let disk: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("worker-0/worker.json")).unwrap())
            .unwrap();
    assert_eq!(disk["candidate"]["generation"], delayed_generation);
    assert_eq!(
        disk["retention"],
        serde_json::json!([1]),
        "retention cannot advance before the delayed decision is applied"
    );
    tasks[0].abort();
    let _ = (&mut tasks[0]).await;
    modes[0].store(0, Ordering::SeqCst);
    let (url, restarted) = start_worker(
        &root.path().join("worker-0"),
        groups[0].replicas[0].url.strip_prefix("http://").unwrap(),
        modes[0].clone(),
    )
    .await;
    assert_eq!(url, groups[0].replicas[0].url);
    tasks[0] = restarted;
    coordinator.reconcile().await.unwrap();
    assert!(read_state().pending_commits.is_empty());
    let recovered: serde_json::Value = reqwest::get(format!("{url}/internal/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(recovered["candidate"].is_null());
    assert_eq!(
        recovered["published"],
        serde_json::json!([]),
        "the old decision is applied then expired, not rolled back"
    );
    let position = journal.tree_size();
    journal
        .append_block(3428150, "08".repeat(32), &[record(position)])
        .unwrap();
    coordinator
        .publish(&journal, 3428150, "08".repeat(32))
        .await
        .unwrap();
    modes[1].store(2, Ordering::SeqCst);
    let mut client = EnhancePirClient::connect(&origin).await.unwrap();
    assert_eq!(
        client
            .query_position_with_timing(position)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(position)
    );
    modes[1].store(0, Ordering::SeqCst);
    modes[0].store(3, Ordering::SeqCst);
    let position = journal.tree_size();
    journal
        .append_block(3428151, "09".repeat(32), &[record(position)])
        .unwrap();
    coordinator
        .publish(&journal, 3428151, "09".repeat(32))
        .await
        .unwrap();
    assert_eq!(read_state().pending_commits.len(), 1);
    modes[0].store(4, Ordering::SeqCst);
    coordinator.reconcile().await.unwrap();
    assert_eq!(
        read_state().pending_commits.len(),
        1,
        "a generation number without the matching digest is not a commit acknowledgement"
    );
    modes[0].store(0, Ordering::SeqCst);
    coordinator.reconcile().await.unwrap();
    assert!(read_state().pending_commits.is_empty());
    let health: serde_json::Value = reqwest::get(format!("{origin}/v1/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["pending_commit_notifications"], 0);
    server.abort();
    for task in tasks {
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abort_recovery_fences_ambiguous_reservations_without_blocking_healthy_peer() {
    use axum::response::IntoResponse;
    use enhance_pir_server::{control::State as ControlState, worker::Candidate};
    use std::sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    };

    // 1 fails preparation and abort, 2 is offline, 3 loses an applied abort's
    // response, 4 loses an accepted reservation's response and rejects abort.
    async fn start_worker(
        root: &std::path::Path,
        address: &str,
        mode: Arc<AtomicU8>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let worker = Worker::open(root).unwrap();
        let router = worker.router().layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let mode = mode.clone();
                async move {
                    let mode = mode.load(Ordering::SeqCst);
                    let path = request.uri().path().to_owned();
                    let mut response = if mode == 2
                        || (mode == 1
                            && ["/internal/prepare", "/internal/abort"].contains(&path.as_str()))
                        || (mode == 4 && path == "/internal/abort")
                    {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                    } else {
                        let response = next.run(request).await;
                        if (mode == 3 && path == "/internal/abort")
                            || (mode == 4 && path == "/internal/reserve")
                        {
                            assert!(response.status().is_success());
                            axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                        } else {
                            response
                        }
                    };
                    response
                        .headers_mut()
                        .insert(axum::http::header::CONNECTION, "close".parse().unwrap());
                    response
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        (
            url,
            tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            }),
        )
    }
    let root = tempfile::tempdir().unwrap();
    let modes = [Arc::new(AtomicU8::new(0)), Arc::new(AtomicU8::new(0))];
    let mut tasks = Vec::new();
    let mut replicas = Vec::new();
    for (index, mode) in modes.iter().enumerate() {
        let (url, task) = start_worker(
            &root.path().join(format!("worker-{index}")),
            "127.0.0.1:0",
            mode.clone(),
        )
        .await;
        tasks.push(task);
        replicas.push(Replica {
            name: format!("peer-{index}"),
            url,
            incarnation: String::new(),
            ledger: Ledger::default(),
        });
    }
    let groups = vec![Group {
        placement_policy: Default::default(),
        id: "g0".into(),
        sequence: 0,
        replicas,
        settling: false,
    }];
    let worker_url = &groups[0].replicas[0].url;
    let coordinator_root = root.path().join("coordinator");
    let read_state = || -> ControlState {
        serde_json::from_slice(&std::fs::read(coordinator_root.join("controller.json")).unwrap())
            .unwrap()
    };
    let worker_disk = || -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(root.path().join("worker-0/worker.json")).unwrap())
            .unwrap()
    };
    let mut coordinator = Coordinator::open(&coordinator_root, groups.clone()).unwrap();
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
    modes[0].store(1, Ordering::SeqCst);
    journal
        .append_block(3428144, "02".repeat(32), &[record(67)])
        .unwrap();
    assert!(coordinator
        .publish(&journal, 3428144, "02".repeat(32))
        .await
        .is_err());
    assert_eq!(coordinator.manifest().await.unwrap().generation, 1);
    let old_candidate: Candidate =
        serde_json::from_value(worker_disk()["candidate"].clone()).unwrap();
    assert_eq!(old_candidate.generation, 2);
    modes[0].store(2, Ordering::SeqCst);
    drop(coordinator);
    coordinator = Coordinator::open(&coordinator_root, groups.clone()).unwrap();
    coordinator.reconcile().await.unwrap();
    let pending = read_state().pending_aborts;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].replica, "peer-0");
    assert_eq!(pending[0].attempt, old_candidate.attempt);
    assert!(read_state().operation.is_none());
    assert_eq!(
        read_state().next_generation,
        2,
        "an abort is not a publication"
    );
    drop(coordinator);
    coordinator = Coordinator::open(&coordinator_root, groups.clone()).unwrap();
    assert_eq!(read_state().pending_aborts, pending);
    tasks[0].abort();
    let _ = (&mut tasks[0]).await;
    let (_, restarted) = start_worker(
        &root.path().join("worker-0"),
        worker_url.strip_prefix("http://").unwrap(),
        modes[0].clone(),
    )
    .await;
    tasks[0] = restarted;
    assert_eq!(worker_disk()["candidate"]["attempt"], old_candidate.attempt);
    let (origin, server) = serve(coordinator.clone().router()).await;
    coordinator
        .publish(&journal, 3428144, "02".repeat(32))
        .await
        .unwrap();
    assert_eq!(read_state().pending_aborts, pending);
    let mut client = EnhancePirClient::connect(&origin).await.unwrap();
    assert_eq!(
        client
            .query_position_with_timing(67)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(67)
    );
    journal
        .append_block(3428145, "03".repeat(32), &[record(68)])
        .unwrap();
    assert!(coordinator
        .publish_checked(&journal, 3428145, "03".repeat(32), || async {
            Err("canonical anchor changed".into())
        })
        .await
        .is_err());
    coordinator.reconcile().await.unwrap();
    assert_eq!(
        read_state().pending_aborts,
        pending,
        "later cancellation cannot overwrite the earlier abort intent"
    );
    coordinator
        .publish(&journal, 3428145, "03".repeat(32))
        .await
        .unwrap();
    assert_eq!(worker_disk()["candidate"]["attempt"], old_candidate.attempt);
    assert_eq!(worker_disk()["retention"], serde_json::json!([1]));
    modes[0].store(3, Ordering::SeqCst);
    coordinator.reconcile().await.unwrap();
    assert_eq!(
        read_state().pending_aborts,
        pending,
        "a lost response keeps the retry intent"
    );
    assert!(worker_disk()["candidate"].is_null());
    let http = reqwest::Client::new();
    assert!(
        !http
            .post(format!("{worker_url}/internal/reserve"))
            .json(&old_candidate)
            .send()
            .await
            .unwrap()
            .status()
            .is_success(),
        "late reservations must remain fenced after restart and abort"
    );
    modes[0].store(0, Ordering::SeqCst);
    coordinator.reconcile().await.unwrap();
    assert!(read_state().pending_aborts.is_empty());
    journal
        .append_block(3428146, "04".repeat(32), &[record(69)])
        .unwrap();
    coordinator
        .publish(&journal, 3428146, "04".repeat(32))
        .await
        .unwrap();
    modes[1].store(2, Ordering::SeqCst);
    let mut client = EnhancePirClient::connect(&origin).await.unwrap();
    assert_eq!(
        client
            .query_position_with_timing(69)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(69)
    );
    modes[1].store(0, Ordering::SeqCst);

    // The response is lost after the worker reserves successfully. Its peer can
    // publish, but the excluded worker must receive a durable abort, not be lost
    // from recovery accounting merely because it returned no readiness ack.
    modes[0].store(4, Ordering::SeqCst);
    journal
        .append_block(3428147, "05".repeat(32), &[record(70)])
        .unwrap();
    coordinator
        .publish(&journal, 3428147, "05".repeat(32))
        .await
        .unwrap();
    let ambiguous: Candidate = serde_json::from_value(worker_disk()["candidate"].clone()).unwrap();
    assert_eq!(
        ambiguous.generation,
        coordinator.manifest().await.unwrap().generation
    );
    assert_eq!(read_state().pending_aborts.len(), 1);
    assert_eq!(read_state().pending_aborts[0].attempt, ambiguous.attempt);
    assert!(read_state().pending_commits.is_empty());
    let mut client = EnhancePirClient::connect(&origin).await.unwrap();
    assert_eq!(
        client
            .query_position_with_timing(70)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(70)
    );
    modes[0].store(0, Ordering::SeqCst);
    coordinator.reconcile().await.unwrap();
    assert!(worker_disk()["candidate"].is_null());
    assert!(read_state().pending_aborts.is_empty());
    assert!(!http
        .post(format!("{worker_url}/internal/reserve"))
        .json(&ambiguous)
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    journal
        .append_block(3428148, "06".repeat(32), &[record(71)])
        .unwrap();
    coordinator
        .publish(&journal, 3428148, "06".repeat(32))
        .await
        .unwrap();
    let health: serde_json::Value = reqwest::get(format!("{origin}/v1/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["pending_abort_notifications"], 0);
    assert_eq!(health["published_replica_counts"]["g0"], 2);
    let metrics = reqwest::get(format!("{origin}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("enhance_pending_abort_notifications 0\n"));
    server.abort();
    for task in tasks {
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_worker_rows_repair_restores_current_and_retained_http_queries() {
    async fn start(root: &std::path::Path, address: &str) -> (String, tokio::task::JoinHandle<()>) {
        let router = Worker::open(root)
            .unwrap()
            .router()
            .layer(axum::middleware::from_fn(
                |request: axum::extract::Request, next: axum::middleware::Next| async move {
                    let mut response = next.run(request).await;
                    response.headers_mut().insert(
                        axum::http::header::CONNECTION,
                        axum::http::HeaderValue::from_static("close"),
                    );
                    response
                },
            ));
        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        (
            url,
            tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            }),
        )
    }
    let root = tempfile::tempdir().unwrap();
    let worker_root = root.path().join("worker-0");
    let peer_root = root.path().join("worker-1");
    let (first_url, first) = start(&worker_root, "127.0.0.1:0").await;
    let (peer_url, peer) = start(&peer_root, "127.0.0.1:0").await;
    let groups = vec![Group {
        placement_policy: Default::default(),
        id: "group-1".into(),
        sequence: 0,
        settling: false,
        replicas: [first_url.clone(), peer_url]
            .into_iter()
            .enumerate()
            .map(|(index, url)| Replica {
                name: format!("replica-{index}"),
                url,
                incarnation: String::new(),
                ledger: Ledger::default(),
            })
            .collect(),
    }];
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
    let mut retained = EnhancePirClient::connect(&origin).await.unwrap();
    let retained_generation = retained.manifest().generation;
    journal
        .append_block(
            3428144,
            "02".repeat(32),
            &(67..100).map(record).collect::<Vec<_>>(),
        )
        .unwrap();
    coordinator
        .publish(&journal, 3428144, "02".repeat(32))
        .await
        .unwrap();
    let durable = std::fs::read(worker_root.join("worker.json")).unwrap();
    first.abort();
    let _ = first.await;
    // No queries are in flight and worker responses close connections, releasing the directory lock.
    std::fs::remove_dir_all(worker_root.join("artifacts-9")).unwrap();
    std::fs::remove_dir_all(worker_root.join("rows")).unwrap();
    assert!(Worker::open(&worker_root).is_err());
    let mut current = EnhancePirClient::connect(&origin).await.unwrap();
    assert_eq!(
        current
            .query_position_with_timing(99)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(99)
    );
    assert_eq!(
        retained
            .query_position_with_timing(66)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(66)
    );
    for expected_restored in [2, 0] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_enhance-pir-server"))
            .arg("repair-rows")
            .arg("--data-dir")
            .arg(&worker_root)
            .arg("--source-rows")
            .arg(peer_root.join("rows"))
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(receipt["status"], "rows_verified");
        assert_eq!(receipt["restored_units"], expected_restored);
        assert_eq!(receipt["readiness"], "requires_worker_restart");
        assert_eq!(receipt["qualification"], "unqualified");
    }
    assert_eq!(
        std::fs::read(worker_root.join("worker.json")).unwrap(),
        durable
    );
    let (_, recovered) = start(&worker_root, first_url.strip_prefix("http://").unwrap()).await;
    peer.abort();
    let _ = peer.await;
    // The recovered worker is now the sole available replica; cached client sessions
    // still require live encrypted evaluations from its rebuilt retained databases.
    for position in [0, 32, 33, 66] {
        assert_eq!(
            retained
                .query_position_with_timing(position)
                .await
                .unwrap()
                .0
                .as_ref(),
            record(position)
        );
    }
    assert_eq!(retained.manifest().generation, retained_generation);
    for position in [67, 99] {
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
    assert!(current.manifest().generation > retained_generation);
    server.abort();
    recovered.abort();
    let _ = server.await;
    let _ = recovered.await;
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
            settling: false,
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
        .join("../../ops/scripts/v4-expansion-journal.py");
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
            settling: false,
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
