use super::*;
use tower::ServiceExt;

#[test]
fn device_errors_are_unavailable_without_exposing_details() {
    use super::super::runtime::EvaluationError;
    assert_eq!(
        evaluation_error(EvaluationError::InvalidQuery).0,
        StatusCode::BAD_REQUEST
    );
    let (status, body) =
        evaluation_error(EvaluationError::Unavailable("private device detail".into()));
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(!body.contains("private device detail"));
}

#[tokio::test]
async fn backend_evaluation_failure_over_http_releases_permit_and_recovers() {
    evaluation_failure_over_http(MatvecConfig::default()).await;
}

#[cfg(feature = "cuda")]
#[tokio::test]
#[ignore = "requires NVIDIA GPU and NVRTC"]
async fn cuda_backend_evaluation_failure_over_http_releases_permit_and_recovers() {
    evaluation_failure_over_http(MatvecConfig {
        matvec_backend: crate::matvec::Backend::Cuda,
        cuda_device: None,
    })
    .await;
}

async fn evaluation_failure_over_http(backend: MatvecConfig) {
    use crate::protocol::{Geometry, Lifecycle, SCHEMA_VERSION};
    use crate::{
        matvec::testing::{scoped, Faults},
        runtime::{modulus, plan, Packing},
    };
    use std::sync::atomic::Ordering::SeqCst;
    let root = tempfile::tempdir().unwrap();
    let mut worker = Worker::open_with_backend(root.path(), Default::default(), backend).unwrap();
    // With a single permit, the next successful request also proves error cleanup.
    worker.evaluation = Arc::new(Semaphore::new(1));
    let coverage = Lifecycle::default()
        .coverage(67, Geometry::default())
        .unwrap();
    let source = |_: u64, count: usize| Ok(vec![7; count * crate::RECORD_BYTES]);
    let plan = plan(coverage.shards[0].clone(), source).unwrap();
    let faults = Arc::new(Faults::default());
    let eval = scoped(&faults, || {
        worker
            .inner
            .lock()
            .unwrap()
            .engine
            .lock()
            .unwrap()
            .prepare(plan.clone(), source)
    })
    .unwrap()
    .0;
    let packing = Packing::new(
        4096,
        &eval.hint().unwrap(),
        &crate::packing_budget::PackingBudget::coordinator(),
    )
    .unwrap();
    let manifest = Manifest {
        recovery_epoch: 0,
        placement_revision: 0,
        domain_recovery_epochs: [(0, "0".into())].into(),
        schema_version: SCHEMA_VERSION,
        protocol_revision: PROTOCOL_REVISION.into(),
        network: "main".into(),
        pool: "ironwood".into(),
        generation: 1,
        anchor_height: 3428143,
        anchor_block_hash: "01".repeat(32),
        geometry: Geometry::default(),
        coverage,
        sessions: vec![packing.reference(0).unwrap()],
        unit_identities: [(0, plan.units.clone())].into(),
    };
    let query = Evaluate {
        binding: None,
        session_id: hex::encode(manifest.session_id(0).unwrap()),
        generation: 1,
        shard_id: 0,
        epoch: manifest.sessions[0].public_params_sha256[..16].into(),
        coefficients: vec![1; 4096],
    };
    let expected = eval.evaluate(&query.coefficients).unwrap();
    {
        let mut inner = worker.inner.lock().unwrap();
        inner
            .save(DiskState {
                published: [(1, (manifest, vec![plan]))].into(),
                ..DiskState::default()
            })
            .unwrap();
        inner.published.insert(1, [(0, eval)].into());
    }
    let durable = fs::read(root.path().join("worker.json")).unwrap();
    let router = worker.clone().router();
    async fn request(router: &Router, query: &Evaluate) -> (StatusCode, Vec<u8>) {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/evaluate")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(query).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        (
            response.status(),
            to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .to_vec(),
        )
    }
    faults.fail_evaluation.store(true, SeqCst);
    let calls = faults.evaluation_calls.load(SeqCst);
    let (status, body) = request(&router, &query).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, b"matrix-vector evaluation unavailable");
    assert_eq!(faults.evaluation_calls.load(SeqCst), calls + 1);
    assert_eq!(worker.evaluation.available_permits(), 1);
    // Validation must reject malformed queries before calling even a failed backend.
    for coefficients in [vec![1], vec![modulus(); 4096]] {
        let bad = Evaluate {
            coefficients,
            ..query.clone()
        };
        assert_eq!(request(&router, &bad).await.0, StatusCode::BAD_REQUEST);
    }
    assert_eq!(faults.evaluation_calls.load(SeqCst), calls + 1);
    faults.fail_evaluation.store(false, SeqCst);
    let (status, body) = request(&router, &query).await;
    assert_eq!(status, StatusCode::OK);
    let (_, worker_metrics) = metrics(State(worker.clone())).await;
    assert!(worker_metrics.contains("enhance_worker_matvec_duration_seconds_count 1"));
    let result: Intermediate = serde_json::from_slice(&body).unwrap();
    assert_eq!(result.coefficients, expected);
    assert_eq!(result.generation, query.generation);
    assert_eq!(result.epoch, query.epoch);
    assert_eq!(faults.evaluation_calls.load(SeqCst), calls + 2);
    assert_eq!(worker.evaluation.available_permits(), 1);
    assert_eq!(fs::read(root.path().join("worker.json")).unwrap(), durable);
}

#[test]
fn q46_worker_state_is_rejected_without_rewriting_it() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("worker.json");
    let mut old = serde_json::to_value(DiskState::default()).unwrap();
    old.as_object_mut().unwrap().remove("protocol_revision");
    for revision in [None, Some("ironwood-enhance-pir-v5")] {
        if let Some(revision) = revision {
            old["protocol_revision"] = revision.into();
        }
        let bytes = serde_json::to_vec(&old).unwrap();
        fs::write(&path, &bytes).unwrap();
        assert!(Worker::open(root.path()).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}

#[tokio::test]
async fn placement_policy_is_persisted_and_fences_reservations() {
    let root = tempfile::tempdir().unwrap();
    let policy = PlacementPolicy { sealed_shards: 7 };
    let worker = Worker::open_with_policy(root.path(), policy).unwrap();
    let router = worker.router();
    let mut candidate = Candidate {
        placement_policy: PlacementPolicy::default(),
        operation: "policy".into(),
        attempt: 1,
        epoch: 1,
        expected_revision: 0,
        generation: 1,
        plans: vec![],
    };
    assert_eq!(
        command(&router, "/internal/reserve", &candidate).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    candidate.placement_policy = policy;
    assert_eq!(
        command(&router, "/internal/reserve", &candidate).await,
        StatusCode::OK
    );
    drop(router);
    assert!(Worker::open(root.path())
        .err()
        .unwrap()
        .contains("placement policy"));
    assert!(Worker::open_with_policy(root.path(), policy).is_ok());
    assert!(Worker::open_with_policy(root.path(), PlacementPolicy { sealed_shards: 8 }).is_err());
}

#[test]
fn restart_rebuilds_lost_cache_only_from_verified_durable_rows() {
    use super::super::runtime::{plan, unit_rows, PublishedPacking};
    use crate::protocol::{Geometry, Lifecycle, SCHEMA_VERSION};
    let root = tempfile::tempdir().unwrap();
    let worker = Worker::open(root.path()).unwrap();
    let coverage = Lifecycle::default()
        .coverage(67, Geometry::default())
        .unwrap();
    let source = crate::test_support::records;
    let plan = plan(coverage.shards[0].clone(), source).unwrap();
    let rows = unit_rows(&plan.shard, &plan.shard.units[0], &mut { source }).unwrap();
    let path = root.path().join("rows").join(plan.units[0].digest());
    fs::write(&path, &rows).unwrap();
    crate::test_support::seed_published_fixture(root.path(), &plan);
    let eval = worker
        .inner
        .lock()
        .unwrap()
        .engine
        .lock()
        .unwrap()
        .prepare(plan.clone(), source)
        .unwrap()
        .0;
    // Workers bind the session reference but never serve packing material.
    // Real packing and decoding are exercised by the encrypted HTTP tests.
    let packing = PublishedPacking::metadata(4096, b"recovery-test-session".to_vec());
    let manifest = Manifest {
        recovery_epoch: 0,
        placement_revision: 0,
        domain_recovery_epochs: [(0, "0".into())].into(),
        schema_version: SCHEMA_VERSION,
        protocol_revision: PROTOCOL_REVISION.into(),
        network: "main".into(),
        pool: "ironwood".into(),
        generation: 1,
        anchor_height: 3428143,
        anchor_block_hash: "01".repeat(32),
        geometry: Geometry::default(),
        coverage,
        sessions: vec![packing.reference(0).unwrap()],
        unit_identities: [(0, plan.units.clone())].into(),
    };
    worker
        .inner
        .lock()
        .unwrap()
        .save(DiskState {
            epoch: 3,
            revision: 1,
            retention: vec![1],
            published: [(1, (manifest, vec![plan.clone()]))].into(),
            ..DiskState::default()
        })
        .unwrap();
    let durable = fs::read(root.path().join("worker.json")).unwrap();
    let mut coefficients = vec![0; 4096];
    coefficients[0] = 1;
    let expected = eval.evaluate(&coefficients).unwrap();
    let peer = tempfile::tempdir().unwrap();
    let peer_path = peer.path().join(plan.units[0].digest());
    fs::write(&peer_path, &rows).unwrap();
    assert!(Worker::repair_rows(root.path(), peer.path()).is_err());
    drop(eval);
    drop(worker);
    fs::remove_dir_all(root.path().join("artifacts-9")).unwrap();
    // Reject corrupt padding too, even though the live prefix is intact.
    let mut corrupt = rows.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(&path, corrupt).unwrap();
    assert!(Worker::open(root.path()).is_err());
    fs::remove_file(&path).unwrap();
    assert!(Worker::open(root.path()).is_err());
    fs::write(&peer_path, b"corrupt source").unwrap();
    assert!(Worker::repair_rows(root.path(), peer.path()).is_err());
    assert!(!path.exists());
    assert_eq!(fs::read(root.path().join("worker.json")).unwrap(), durable);
    fs::write(&peer_path, &rows).unwrap();
    fs::write(peer.path().join("unreferenced"), b"do not copy").unwrap();
    assert_eq!(Worker::repair_rows(root.path(), peer.path()).unwrap(), 1);
    assert_eq!(Worker::repair_rows(root.path(), peer.path()).unwrap(), 0);
    assert!(!root.path().join("rows/unreferenced").exists());
    let recovered = Worker::open(root.path()).unwrap();
    let inner = recovered.inner.lock().unwrap();
    assert_eq!(
        inner.published[&1][&0].evaluate(&coefficients).unwrap(),
        expected
    );
    assert_eq!(inner.disk.epoch, 3);
    assert_eq!(fs::read(root.path().join("worker.json")).unwrap(), durable);
    assert!(staged_records(root.path(), &plan, plan.shard.records, 1).is_err());
    assert!(staged_records(root.path(), &plan, 0, usize::MAX).is_err());
}

#[test]
fn offline_repair_includes_durable_candidate_and_rejects_missing_journal() {
    let root = tempfile::tempdir().unwrap();
    let source = tempfile::tempdir().unwrap();
    assert!(Worker::repair_rows(root.path(), source.path()).is_err());
    let worker = Worker::open(root.path()).unwrap();
    let coverage = crate::protocol::Lifecycle::default()
        .coverage(1, crate::protocol::Geometry::default())
        .unwrap();
    let plan = super::super::runtime::plan(coverage.shards[0].clone(), |_, count| {
        Ok(vec![0; count * crate::RECORD_BYTES])
    })
    .unwrap();
    let unit = &plan.units[0];
    fs::write(
        source.path().join(unit.digest()),
        vec![0; unit.allocated_rows as usize * crate::types::ENHANCE_LAYOUT.row_bytes()],
    )
    .unwrap();
    worker
        .inner
        .lock()
        .unwrap()
        .save(DiskState {
            candidate: Some(Candidate {
                placement_policy: Default::default(),
                operation: "pending".into(),
                attempt: 2,
                epoch: 3,
                expected_revision: 0,
                generation: 1,
                plans: vec![plan],
            }),
            ..DiskState::default()
        })
        .unwrap();
    let journal = fs::read(root.path().join("worker.json")).unwrap();
    drop(worker);
    assert_eq!(Worker::repair_rows(root.path(), source.path()).unwrap(), 1);
    assert_eq!(fs::read(root.path().join("worker.json")).unwrap(), journal);
}

async fn command(router: &Router, path: &str, value: &impl Serialize) -> StatusCode {
    router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(value).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn metrics_do_not_block_preparation_or_invent_zero_memory() {
    let root = tempfile::tempdir().unwrap();
    let worker = Worker::open(root.path()).unwrap();
    let before = fs::read(root.path().join("worker.json")).ok();
    let engine = worker.inner.lock().unwrap().engine.clone();
    let (ready_send, ready_receive) = std::sync::mpsc::channel();
    let (finish_send, finish_receive) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        let _guard = engine.lock().unwrap();
        ready_send.send(()).unwrap();
        finish_receive
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
    });
    ready_receive
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let (_, text) = metrics(State(worker.clone())).await;
    finish_send.send(()).unwrap();
    thread.join().unwrap();
    assert!(text.contains("enhance_worker_memory_sample_available 0\n"));
    assert!(!text.contains("enhance_worker_live_database_bytes "));
    let (_, text) = metrics(State(worker)).await;
    assert!(text.contains("enhance_worker_memory_sample_available 1\n"));
    assert!(text.contains("enhance_worker_live_database_bytes 0\n"));
    assert!(text.contains(&format!("enhance_worker_model_total_bytes {OVERHEAD}\n")));
    assert_eq!(fs::read(root.path().join("worker.json")).ok(), before);
}

#[tokio::test]
async fn abort_before_reserve_fences_delayed_attempt() {
    let root = tempfile::tempdir().unwrap();
    let router = Worker::open(root.path()).unwrap().router();
    let abort = Abort {
        epoch: 1,
        operation: "cancelled".into(),
        attempt: 0,
    };
    assert_eq!(
        command(&router, "/internal/abort", &abort).await,
        StatusCode::OK
    );
    let mut candidate = Candidate {
        placement_policy: Default::default(),
        epoch: 1,
        operation: "cancelled".into(),
        attempt: 0,
        expected_revision: 0,
        generation: 1,
        plans: vec![],
    };
    assert_eq!(
        command(&router, "/internal/reserve", &candidate).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    candidate.attempt = 1;
    candidate.operation = "replacement".into();
    assert_eq!(
        command(&router, "/internal/reserve", &candidate).await,
        StatusCode::OK
    );
    assert_eq!(
        command(&router, "/internal/abort", &abort).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn reservation_abort_and_commit_decisions_survive_reopening() {
    let root = tempfile::tempdir().unwrap();
    let mut candidate = Candidate {
        placement_policy: Default::default(),
        epoch: 1,
        operation: "cancelled".into(),
        attempt: 0,
        expected_revision: 0,
        generation: 1,
        plans: vec![],
    };
    let router = Worker::open(root.path()).unwrap().router();
    assert_eq!(
        command(&router, "/internal/reserve", &candidate).await,
        StatusCode::OK
    );
    drop(router);
    let router = Worker::open(root.path()).unwrap().router();
    let abort = Abort {
        epoch: 2,
        operation: candidate.operation.clone(),
        attempt: 0,
    };
    // The caller may lose either reply. Reopening must retain the reservation
    // and then the abort fence, without preparing any cryptographic rows.
    assert_eq!(
        command(&router, "/internal/abort", &abort).await,
        StatusCode::OK
    );
    drop(router);
    let router = Worker::open(root.path()).unwrap().router();
    assert_eq!(
        command(&router, "/internal/reserve", &candidate).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        command(&router, "/internal/abort", &abort).await,
        StatusCode::OK
    );
    candidate.epoch = 2;
    candidate.attempt = 1;
    candidate.operation = "replacement".into();
    assert_eq!(
        command(&router, "/internal/reserve", &candidate).await,
        StatusCode::OK
    );
    let vector: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/v7-session.json"
    )))
    .unwrap();
    let mut manifest: Manifest = serde_json::from_value(vector["manifest"].clone()).unwrap();
    manifest.generation = 1;
    manifest.protocol_revision = PROTOCOL_REVISION.into();
    for (session, shard) in manifest.sessions.iter_mut().zip(&manifest.coverage.shards) {
        session.parameter_id = crate::protocol::parameter_id(shard.logical_rows).unwrap();
    }
    for unit in manifest.unit_identities.values_mut().flatten() {
        unit.parameter_id = crate::protocol::unit_parameter_id(unit.allocated_rows).unwrap();
    }
    let activation = Activation {
        operation: candidate.operation,
        attempt: 1,
        epoch: 2,
        manifest: manifest.clone(),
    };
    assert_eq!(
        command(&router, "/internal/activate", &activation).await,
        StatusCode::OK
    );
    drop(router);
    let router = Worker::open(root.path()).unwrap().router();
    let mut commit = Commit {
        epoch: 2,
        revision: 1,
        generation: 1,
        manifest_digest: "incorrect".into(),
        retained: vec![1],
    };
    assert_eq!(
        command(&router, "/internal/commit", &commit).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    commit.manifest_digest = digest(&manifest);
    assert_eq!(
        command(&router, "/internal/commit", &commit).await,
        StatusCode::OK
    );
    drop(router);
    let worker = Worker::open(root.path()).unwrap();
    assert!(worker.inner.lock().unwrap().disk.candidate.is_none());
    assert_eq!(worker.inner.lock().unwrap().disk.retention, [1]);
    let router = worker.router();
    assert_eq!(
        command(&router, "/internal/commit", &commit).await,
        StatusCode::OK
    );
    commit.manifest_digest = "incorrect".into();
    assert_eq!(
        command(&router, "/internal/commit", &commit).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        command(
            &router,
            "/internal/retain",
            &Retain {
                epoch: 2,
                generations: vec![2],
            }
        )
        .await,
        StatusCode::OK
    );
    drop(router);
    let worker = Worker::open(root.path()).unwrap();
    assert!(worker.inner.lock().unwrap().disk.published.is_empty());
    assert_eq!(worker.inner.lock().unwrap().disk.retention, [2]);
    commit.manifest_digest = digest(&manifest);
    assert_eq!(
        command(&worker.router(), "/internal/commit", &commit).await,
        StatusCode::SERVICE_UNAVAILABLE,
        "an expired decision cannot resurrect its generation"
    );
}

#[tokio::test]
async fn aborted_attempts_conflicting_payloads_and_stale_epochs_are_fenced_over_http() {
    let root = tempfile::tempdir().unwrap();
    let router = Worker::open(root.path()).unwrap().router();
    let coverage = crate::protocol::Lifecycle::default()
        .coverage(1, crate::protocol::Geometry::default())
        .unwrap();
    let plan = super::super::runtime::plan(coverage.shards[0].clone(), |_, count| {
        Ok(vec![0; count * crate::RECORD_BYTES])
    })
    .unwrap();
    let mut candidate = Candidate {
        placement_policy: Default::default(),
        operation: "test".into(),
        attempt: 0,
        epoch: 1,
        expected_revision: 0,
        generation: 1,
        plans: vec![plan],
    };
    assert_eq!(
        command(&router, "/internal/reserve", &candidate).await,
        StatusCode::OK
    );
    assert_eq!(
        command(&router, "/internal/reserve", &candidate).await,
        StatusCode::OK
    );
    assert_eq!(
        command(
            &router,
            "/internal/abort",
            &Abort {
                epoch: 0,
                operation: "test".into(),
                attempt: 0
            }
        )
        .await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!(
                    "/internal/rows/{}",
                    candidate.plans[0].units[0].digest()
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "uploads need the reservation's command identity"
    );
    assert_eq!(
        command(
            &router,
            "/internal/abort",
            &Abort {
                epoch: 1,
                operation: "test".into(),
                attempt: 0
            }
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        command(&router, "/internal/reserve", &candidate).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    candidate.attempt = 1;
    assert_eq!(
        command(&router, "/internal/reserve", &candidate).await,
        StatusCode::OK
    );
    candidate.plans[0].units[0].content_sha256 = "ff".repeat(32);
    assert_eq!(
        command(&router, "/internal/reserve", &candidate).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        command(
            &router,
            "/internal/retain",
            &Retain {
                epoch: 0,
                generations: vec![1]
            }
        )
        .await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        command(
            &router,
            "/internal/retain",
            &Retain {
                epoch: 1,
                generations: vec![1, 1]
            }
        )
        .await,
        StatusCode::SERVICE_UNAVAILABLE
    );
}
