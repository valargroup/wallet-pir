//! A real wallet body cannot select a different publication, setup or anchor.
use enhance_query_server::{
    client::QuerySession,
    protocol::{QueryBinding, ShardSession, HEADER_BYTES},
};
use pir_e2e_tests::enhance_fixture::{record, System};
#[path = "support/publication.rs"]
mod publication_source;

async fn prepare_again(
    source: &publication_source::Published,
) -> Result<enhance_publisher::publication::PreparedPublication, String> {
    let root = source.scratch.path().join("publication");
    let coverage = source.inventory.coverage.clone();
    let epochs = source.manifest.domain_recovery_epochs.clone();
    tokio::task::spawn_blocking(move || {
        enhance_publisher::publication::prepare_publication(
            &root,
            coverage,
            &epochs,
            |start, count| Ok((start..start + count as u64).flat_map(record).collect()),
        )
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_object_installation_preserves_enhance_publication_and_reuse() {
    use enhance_publisher::publication::{prepare_publication, VerificationStats};
    use std::{
        fs,
        os::unix::fs::{FileExt, MetadataExt, PermissionsExt},
        time::Duration,
    };

    let source = publication_source::Published::new().await;
    let root = source.scratch.path().join("publication");
    let domain = &source.inventory.domains[0];
    let packing = domain.packing_object();
    let prepared_file = root
        .join(enhance_query_server::prepared_packing::DIRECTORY)
        .join(&domain.packing.name);
    assert_eq!(
        fs::metadata(&prepared_file).unwrap().ino(),
        fs::metadata(packing.path(&root).unwrap()).unwrap().ino(),
        "packing adoption must not copy its body"
    );
    let identities = || {
        let mut entries: Vec<_> = fs::read_dir(root.join("objects"))
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let metadata = entry.metadata().unwrap();
                (
                    entry.file_name(),
                    metadata.ino(),
                    metadata.ctime(),
                    metadata.ctime_nsec(),
                    metadata.nlink(),
                )
            })
            .collect();
        entries.sort();
        entries
    };
    let original = identities();
    let prepared = prepare_again(&source).await.unwrap();
    assert_eq!(
        serde_json::to_value(prepared.inventory()).unwrap(),
        serde_json::to_value(&source.inventory).unwrap()
    );
    assert_eq!(
        identities(),
        original,
        "references must not alter stored inode metadata"
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let prepared = prepared.recheck(&root).unwrap().recheck(&root).unwrap();
    assert_eq!(
        prepared.verification_stats(),
        VerificationStats {
            files: 0,
            bytes: 0,
            reused: original.len() as u64
        }
    );
    source
        .distribution
        .publish_prepared(source.manifest.clone(), prepared)
        .await
        .unwrap();

    let rows = &domain.rows[0];
    let downloaded = source
        .transport
        .client
        .get(format!(
            "{}/provider/enhance/v1/objects/{}",
            source.transport.origin, rows.sha256
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(
        downloaded.as_ref(),
        fs::read(rows.path(&root).unwrap()).unwrap()
    );
    let current = fs::read(root.join("current.json")).unwrap();
    let failed = prepare_publication(
        &root,
        source.inventory.coverage.clone(),
        &source.manifest.domain_recovery_epochs,
        |_, _| Err("source read interrupted".into()),
    );
    assert!(failed.is_err());
    assert_eq!(fs::read(root.join("current.json")).unwrap(), current);
    prepare_again(&source).await.unwrap();

    // A changed byte must fail even when the file still has the expected size.
    for object in [rows, &packing] {
        let path = object.path(&root).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        let file = fs::File::options()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut byte = [0];
        file.read_exact_at(&mut byte, 0).unwrap();
        file.write_all_at(&[byte[0] ^ 1], 0).unwrap();
        assert!(prepare_again(&source).await.is_err());
        let mut corrupted = [0];
        file.read_exact_at(&mut corrupted, 0).unwrap();
        assert_eq!(
            corrupted[0],
            byte[0] ^ 1,
            "preparation must not replace a conflicting object"
        );
        assert_eq!(fs::read(root.join("current.json")).unwrap(), current);
        file.write_all_at(&byte, 0).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o444)).unwrap();
    }
    let path = rows.path(&root).unwrap();
    let held = root.join("held-object");
    fs::rename(&path, &held).unwrap();
    fs::write(&path, b"wrong length").unwrap();
    assert!(prepare_again(&source).await.is_err());
    assert_eq!(fs::read(&path).unwrap(), b"wrong length");
    fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink(&held, &path).unwrap();
    assert!(prepare_again(&source).await.is_err());
    assert!(fs::symlink_metadata(&path)
        .unwrap()
        .file_type()
        .is_symlink());
    fs::remove_file(&path).unwrap();
    fs::rename(&held, &path).unwrap();
    assert_eq!(fs::read(root.join("current.json")).unwrap(), current);
    prepare_again(&source).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_publication_preparation_defers_without_transport_backoff_or_serving_authority() {
    use enhance_publisher::{chain_source::ZakuraClient, distribution::PublicationDistribution};
    use enhance_query_server::{ingress_sync, router_sync, synchronization, worker_sync};
    use pir_e2e_tests::enhance_fixture::{serve, tls};
    let root = tempfile::tempdir().unwrap();
    let cookie = root.path().join("cookie");
    std::fs::write(&cookie, "fixture:fixture").unwrap();
    let distribution = PublicationDistribution::open(
        &root.path().join("source"),
        ZakuraClient::from_cookie_file("http://127.0.0.1:1", cookie).unwrap(),
    )
    .unwrap();
    let publisher = serve(pir_e2e_tests::enhance_fixture::distribution_router(
        &distribution,
    ))
    .await;
    let transport = tls(root.path(), &publisher.url).await;
    let source = synchronization::Config {
        publisher_origin: transport.origin.clone(),
        trusted_ca: Some(root.path().join("ca.pem")),
        download_budget_bytes: 16 * 1024 * 1024,
        cache_limit_bytes: 16 * 1024 * 1024,
    };
    let worker = worker_sync::Worker::open(
        &root.path().join("worker"),
        worker_sync::Config {
            source: source.clone(),
            shards: [0].into(),
        },
        Default::default(),
        Default::default(),
    )
    .unwrap();
    let mut worker_sync = worker.synchronizer().unwrap();
    let worker_http = serve(worker.router()).await;
    let router = router_sync::PackingRouter::open(
        &root.path().join("router"),
        router_sync::Config {
            source: source.clone(),
            workers: [(0, vec![worker_http.url.clone()])].into(),
            preferred: Default::default(),
        },
        6,
        4,
    )
    .unwrap();
    let mut router_sync = router.synchronizer().unwrap();
    let control = serve(router.control_router()).await;
    let query = serve(router.public_router()).await;
    let ingress = ingress_sync::QueryIngress::open(
        &root.path().join("ingress"),
        ingress_sync::Config {
            source,
            routers: [(
                0,
                vec![ingress_sync::RouterPeer {
                    control_origin: control.url.clone(),
                    query_origin: query.url.clone(),
                }],
            )]
            .into(),
        },
        16,
        256,
    )
    .unwrap();
    let mut ingress_sync = ingress.synchronizer().unwrap();
    let public = serve(ingress.public_router()).await;
    for _ in 0..2 {
        worker_sync.poll_once().await.unwrap();
        router_sync.poll_once().await.unwrap();
        ingress_sync.poll_once().await.unwrap();
        assert_eq!(
            reqwest::get(format!("{}/v1/enhance/init", public.url))
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::SERVICE_UNAVAILABLE
        );
    }
}

async fn advance_during_authorization(
    source: &publication_source::Published,
    generation: u64,
    poll: impl std::future::Future<Output = Result<(), enhance_query_server::synchronization::Error>>,
    withdraw: bool,
) -> Result<(), enhance_query_server::synchronization::Error> {
    let fault = source.scratch.path().join("fault");
    let entered = source.scratch.path().join("fault.entered");
    if entered.exists() {
        std::fs::remove_file(&entered).unwrap();
    }
    std::fs::write(&fault, "pause-authority").unwrap();
    let change = async {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !entered.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("candidate did not request fresh authority");
        if withdraw {
            source
                .distribution
                .withdraw(&enhance_query_server::publication::Revocation {
                    recovery_epoch: 0,
                    sessions: Default::default(),
                })
                .unwrap();
        }
        let mut next = source.manifest.clone();
        next.generation = generation;
        source
            .distribution
            .publish(next, source.inventory.clone())
            .await
            .unwrap();
        std::fs::write(&fault, "").unwrap();
    };
    let (result, ()) = tokio::join!(poll, change);
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_publication_advancement_allows_retained_candidates_but_withdrawal_refuses_them() {
    use enhance_query_server::{ingress_sync, router_sync, worker_sync};
    use pir_e2e_tests::enhance_fixture::serve;
    let fixture = publication_source::Published::new().await;
    let worker = worker_sync::Worker::open(
        &fixture.scratch.path().join("worker"),
        worker_sync::Config {
            source: fixture.source(),
            shards: [0].into(),
        },
        Default::default(),
        Default::default(),
    )
    .unwrap();
    let mut worker_sync = worker.synchronizer().unwrap();
    let worker_http = serve(worker.router()).await;
    advance_during_authorization(&fixture, 2, worker_sync.poll_once(), false)
        .await
        .unwrap();
    worker_sync.poll_once().await.unwrap();
    let router = router_sync::PackingRouter::open(
        &fixture.scratch.path().join("router"),
        router_sync::Config {
            source: fixture.source(),
            workers: [(0, vec![worker_http.url.clone()])].into(),
            preferred: Default::default(),
        },
        6,
        4,
    )
    .unwrap();
    let mut router_sync = router.synchronizer().unwrap();
    let router_http = serve(router.public_router()).await;
    let router_control = serve(router.control_router()).await;
    advance_during_authorization(&fixture, 3, router_sync.poll_once(), false)
        .await
        .unwrap();
    worker_sync.poll_once().await.unwrap();
    router_sync.poll_once().await.unwrap();
    let ingress = ingress_sync::QueryIngress::open(
        &fixture.scratch.path().join("ingress"),
        ingress_sync::Config {
            source: fixture.source(),
            routers: [(
                0,
                vec![ingress_sync::RouterPeer {
                    control_origin: router_control.url.clone(),
                    query_origin: router_http.url.clone(),
                }],
            )]
            .into(),
        },
        16,
        256,
    )
    .unwrap();
    let mut ingress_sync = ingress.synchronizer().unwrap();
    advance_during_authorization(&fixture, 4, ingress_sync.poll_once(), false)
        .await
        .unwrap();
    let public = serve(ingress.public_router()).await;
    let init: serde_json::Value = reqwest::get(format!("{}/v1/enhance/init", public.url))
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(init["generation"], 3, "the retained candidate became ready");
    assert!(
        advance_during_authorization(&fixture, 5, worker_sync.poll_once(), true)
            .await
            .is_err(),
        "a newer withdrawal boundary cannot authorize the candidate"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wallet_binding_mismatches_are_refused_before_forwarding() {
    let system = System::start((0..67).map(record).collect()).await;
    let http = reqwest::Client::new();
    let session: ShardSession = http
        .get(format!(
            "{}/v1/enhance/sessions/{}/0",
            system.origin, system.manifest.generation
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let session = QuerySession::new(&system.manifest, session).unwrap();
    let (query, _) = session.prepare_position(66).unwrap();
    let initial = QueryBinding::decode(query.body()).unwrap();
    for fault in ["generation", "recovery", "anchor", "session"] {
        let mut binding = initial;
        match fault {
            "generation" => binding.generation += 1,
            "recovery" => binding.recovery_epoch += 1,
            "anchor" => binding.anchor_hash[0] ^= 1,
            _ => binding.session_id[0] ^= 1,
        }
        let mut body = binding.encode();
        body.extend_from_slice(&query.body()[HEADER_BYTES..]);
        let response = http
            .post(format!("{}/v1/enhance/query", system.origin))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if matches!(fault, "generation" | "recovery") {
                reqwest::StatusCode::CONFLICT
            } else {
                reqwest::StatusCode::GONE
            },
            "{fault}"
        );
    }
}
