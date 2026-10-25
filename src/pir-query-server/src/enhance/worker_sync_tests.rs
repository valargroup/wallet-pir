//! Device failure must release admission without changing loaded authority.
use super::*;
use enhance_shared::{
    matvec::testing::{scoped, Faults},
    protocol::{Geometry, Lifecycle},
    publication::{AuthorityResponse, Current, Object},
};
use sha2::{Digest, Sha256};
use std::sync::atomic::Ordering::SeqCst;
use tower::ServiceExt;

#[tokio::test]
async fn backend_evaluation_failure_over_http_releases_permit_and_recovers() {
    evaluation_failure(Default::default()).await;
}

#[cfg(feature = "cuda")]
#[tokio::test]
#[ignore = "requires NVIDIA GPU and NVRTC"]
async fn cuda_backend_evaluation_failure_over_http_releases_permit_and_recovers() {
    evaluation_failure(MatvecConfig {
        matvec_backend: enhance_shared::matvec::Backend::Cuda,
        cuda_device: None,
    })
    .await;
}

async fn evaluation_failure(backend: MatvecConfig) {
    let root = tempfile::tempdir().unwrap();
    let mut worker = Worker::open(
        root.path(),
        Config {
            source: SourceConfig {
                publisher_origin: "https://publisher.example".into(),
                trusted_ca: None,
                download_budget_bytes: 128 * MIB,
                cache_limit_bytes: 128 * MIB,
            },
            shards: [0].into(),
        },
        Default::default(),
        backend,
    )
    .unwrap();
    worker.evaluation = Arc::new(Semaphore::new(1));
    let coverage = Lifecycle::default()
        .coverage(67, Geometry::default())
        .unwrap();
    let plan = enhance_shared::runtime::plan(
        coverage.shards[0].clone(),
        enhance_shared::test_support::records,
    )
    .unwrap();
    let rows = enhance_shared::runtime::unit_rows(
        &plan.shard,
        &plan.shard.units[0],
        &mut enhance_shared::test_support::records,
    )
    .unwrap();
    let object = Object {
        sha256: hex::encode(Sha256::digest(&rows)),
        bytes: rows.len() as u64,
    };
    fs::create_dir_all(root.path().join("objects")).unwrap();
    fs::write(object.path(root.path()).unwrap(), rows).unwrap();
    // Worker loading uses only verified rows. Public packing is never built on
    // this host, and the unused packing fields cannot grant serving authority.
    let unused = Object {
        sha256: "02".repeat(32),
        bytes: 1,
    };
    let domain = Domain {
        plan: plan.clone(),
        rows: vec![object],
        hint: unused.clone(),
        public: unused,
        packing: enhance_shared::prepared_packing::Artifact {
            name: "unused".into(),
            sha256: "03".repeat(32),
            bytes: 1,
            format: enhance_shared::prepared_packing::FORMAT,
        },
    };
    let mut checked = VerifiedObjects::default();
    for object in &domain.rows {
        checked.insert(
            pir_publication::VerifiedObjectFile::open(
                object.path(root.path()).unwrap(),
                &object.reference(),
            )
            .unwrap(),
        );
    }
    let faults = Arc::new(Faults::default());
    let loaded = scoped(&faults, || worker.load(vec![domain], &checked)).unwrap();
    let manifest = Manifest {
        recovery_epoch: 0,
        placement_revision: 1,
        domain_recovery_epochs: [(0, "0".into())].into(),
        schema_version: protocol::SCHEMA_VERSION,
        protocol_revision: protocol::PROTOCOL_REVISION.into(),
        network: "main".into(),
        pool: "ironwood".into(),
        generation: 1,
        anchor_height: 3428143,
        anchor_block_hash: "01".repeat(32),
        geometry: Geometry::default(),
        coverage,
        sessions: vec![protocol::SessionRef {
            shard_id: 0,
            public_params_sha256: "02".repeat(32),
            parameter_id: protocol::parameter_id(plan.shard.logical_rows).unwrap(),
        }],
        unit_identities: [(0, plan.units)].into(),
    };
    let id = digest(&manifest);
    let binding = QueryBinding {
        generation: 1,
        shard_id: 0,
        epoch: [2; 8],
        recovery_epoch: 0,
        session_id: manifest.session_id(0).unwrap(),
        request_id: [4; 16],
        anchor_hash: [1; 32],
    };
    let query = Evaluate {
        binding: Some(binding.encode()),
        session_id: hex::encode(binding.session_id),
        generation: 1,
        shard_id: 0,
        epoch: hex::encode(binding.epoch),
        coefficients: vec![1; 4096],
    };
    let expected = loaded[&0].evaluate(&query.coefficients).unwrap();
    let lease = Lease::from_response(
        AuthorityResponse {
            current: Current {
                version: enhance_shared::publication::VERSION,
                revision: 1,
                withdrawal_revision: 1,
                recovery_epoch: 0,
                revoked: Default::default(),
                eligible: true,
                manifest: Some(manifest.clone()),
                inventory: Some(Object {
                    sha256: "03".repeat(32),
                    bytes: 1,
                }),
            },
            authorized_manifest_sha256: id.clone(),
            source: pir_publication::SourceObservation::starting_now(),
        },
        &manifest,
        &worker.safety,
    )
    .unwrap();
    worker.inner.lock().unwrap().views.insert(
        id.clone(),
        View {
            manifest,
            domains: loaded,
            lease,
        },
    );
    worker.inner.lock().unwrap().active = Some(id);
    let safety = fs::read(root.path().join("sync.json")).unwrap();
    let router = worker.clone().router();
    async fn request(router: &Router, query: &Evaluate) -> (StatusCode, Vec<u8>) {
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/internal/evaluate")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(serde_json::to_vec(query).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        (
            response.status(),
            axum::body::to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .to_vec(),
        )
    }
    faults.fail_evaluation.store(true, SeqCst);
    let before = faults.evaluation_calls.load(SeqCst);
    let (status, body) = request(&router, &query).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, b"matrix-vector evaluation unavailable");
    assert_eq!(faults.evaluation_calls.load(SeqCst), before + 1);
    assert_eq!(worker.evaluation.available_permits(), 1);
    for coefficients in [vec![1], vec![enhance_shared::runtime::modulus(); 4096]] {
        assert_eq!(
            request(
                &router,
                &Evaluate {
                    coefficients,
                    ..query.clone()
                }
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(faults.evaluation_calls.load(SeqCst), before + 1);
    faults.fail_evaluation.store(false, SeqCst);
    let (status, body) = request(&router, &query).await;
    assert_eq!(status, StatusCode::OK);
    let result: Intermediate = serde_json::from_slice(&body).unwrap();
    assert_eq!(result.coefficients, expected);
    assert_eq!(faults.evaluation_calls.load(SeqCst), before + 2);
    assert_eq!(worker.evaluation.available_permits(), 1);
    assert_eq!(fs::read(root.path().join("sync.json")).unwrap(), safety);
}

#[cfg(not(feature = "cuda"))]
#[test]
fn cuda_without_feature_fails_before_creating_worker_state() {
    let root = tempfile::tempdir().unwrap();
    let config = enhance_shared::matvec::MatvecConfig {
        matvec_backend: enhance_shared::matvec::Backend::Cuda,
        cuda_device: None,
    };
    let result = crate::enhance::worker_sync::Worker::open(
        root.path(),
        crate::enhance::worker_sync::Config {
            source: crate::enhance::synchronization::Config {
                publisher_origin: "https://publisher.example".into(),
                trusted_ca: None,
                download_budget_bytes: 8 * 1024 * 1024,
                cache_limit_bytes: 8 * 1024 * 1024,
            },
            shards: [0].into(),
        },
        Default::default(),
        config,
    );
    assert!(matches!(result, Err(e) if e.to_string().contains("--features cuda")));
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}
