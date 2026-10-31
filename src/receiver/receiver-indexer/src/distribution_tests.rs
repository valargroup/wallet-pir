use super::*;
use axum::routing::post;

/// Empty segment tables over activation and the block after it, on mainnet.
fn tables() -> zakura_pir_publisher::receiver::Segment {
    let mut coverage = crate::prepared::coverage(3);
    coverage.genesis = Network::Mainnet.genesis_hash();
    coverage.start_height = crate::blocks::ironwood_activation();
    coverage.end_height = coverage.start_height + 1;
    crate::prepared::tables(coverage, &[], 2048)
}

/// [`tables`] checked and stored in a durable directory under `root`.
fn stored(root: &std::path::Path) -> receiver_pir_server::publication::Segment {
    let tables = tables();
    let entries = tables.directory.validate().unwrap();
    let mut durable = crate::durable::Durable::open(
        root,
        Arc::new(receiver_pir_server::tables::Tables::new(4 << 30)),
    )
    .unwrap();
    durable.segment(tables, &entries).unwrap()
}
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[test]
fn directory_objects_are_bound_to_the_prepared_session() {
    use std::io::Write;
    let root = tempfile::tempdir().unwrap();
    let genesis = Network::Mainnet.genesis_hash();
    let segment = stored(root.path());
    let revision = segment
        .directory
        .directory_session()
        .unwrap()
        .manifest
        .revision()
        .unwrap();
    let rows = root
        .path()
        .join(crate::durable::TABLES_DIR)
        .join(format!("d-{}.rows", hex::encode(revision)));
    let publication = Arc::new(
        receiver_pir_server::Publication::assemble(genesis, 0, vec![(false, vec![segment], None)])
            .unwrap(),
    );
    let distribution = Distribution::open(
        root.path().into(),
        NodeClient::new("http://127.0.0.1:1".into(), None).unwrap(),
        600,
    )
    .unwrap();
    let inventory = distribution
        .material(Some(publication.clone()), None, 0, 0)
        .unwrap();
    inventory
        .validate(crate::blocks::ironwood_activation())
        .unwrap();
    let mut invalid_setup = inventory.clone();
    invalid_setup.directory.as_mut().unwrap().segments[0]
        .directory_public
        .sha256 = "00".repeat(32);
    assert!(invalid_setup
        .validate(crate::blocks::ironwood_activation())
        .is_err());
    // Damage an existing source in place: a new whole-file object digest
    // must not be accepted under the old session's rows commitment.
    std::fs::OpenOptions::new()
        .write(true)
        .open(rows)
        .unwrap()
        .write_all(&[1])
        .unwrap();
    let damaged = distribution
        .material(Some(publication), None, 0, 0)
        .unwrap();
    assert!(damaged
        .validate(crate::blocks::ironwood_activation())
        .is_err());
}

/// While a restarted process has only block filters, its exports leave the
/// restored publication and its tables in place.
#[tokio::test]
async fn an_export_without_tables_keeps_restored_tables() {
    let root = tempfile::tempdir().unwrap();
    let genesis = Network::Mainnet.genesis_hash();
    let activation = crate::blocks::ironwood_activation();
    let segment = stored(root.path());
    let publication = Arc::new(
        receiver_pir_server::Publication::assemble(genesis, 0, vec![(false, vec![segment], None)])
            .unwrap(),
    );
    // No node answers: keeping the restored tables needs no chain check.
    let distribution = Distribution::open(
        root.path().into(),
        NodeClient::new("http://127.0.0.1:1".into(), None).unwrap(),
        600,
    )
    .unwrap();
    let inventory = distribution
        .material(Some(publication), None, 0, 0)
        .unwrap();
    let object = distribution
        .objects
        .store(&serde_json::to_vec(&inventory).unwrap())
        .unwrap();
    distribution
        .authority
        .lock()
        .unwrap()
        .apply(AuthorityTransition::Publish(inventory.reference(object)))
        .unwrap();
    let restored = distribution.authority.lock().unwrap().current().clone();
    let publications = Publications::default();
    assert!(publications.publish_filters(
        receiver_pir_server::blocks::BlockWindow::new(
            genesis,
            0,
            activation,
            [0; 32],
            vec![[3; 32]],
            vec![]
        )
        .unwrap(),
        0
    ));
    distribution
        .export(&publications, &pir_pipeline::engine::PublisherEngine)
        .await
        .unwrap();
    assert_eq!(*distribution.authority.lock().unwrap().current(), restored);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn https_hints_reuse_objects_and_durable_withdrawals_fence_restarts() {
    let genesis = Network::Mainnet.genesis_hash();
    let activation = crate::blocks::ironwood_activation();
    let fork = Arc::new(AtomicBool::new(false));
    let node = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rpc = NodeClient::new(format!("http://{}", node.local_addr().unwrap()), None).unwrap();
    let changed = fork.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let task = tokio::spawn(async move {
        axum::serve(node, Router::new().route("/", post(move |Json(request): Json<serde_json::Value>| {
        let changed = changed.clone(); let counted = counted.clone(); async move {
            counted.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request["method"], "getblockhash");
            let hash = if request["params"][0] == 0 { genesis } else if changed.load(Ordering::SeqCst) { [4;32] } else { [3;32] };
            Json(serde_json::json!({"result":zakura_server_chain::hash::display(&hash),"error":null}))
        }
    }))).await.unwrap();
    });
    let root = tempfile::tempdir().unwrap();
    let distribution = Distribution::open(root.path().into(), rpc.clone(), 600).unwrap();
    let publications = Publications::default();
    distribution.attach(&publications);
    assert!(publications.publish_filters(
        receiver_pir_server::blocks::BlockWindow::new(
            genesis,
            0,
            activation,
            [0; 32],
            vec![[3; 32]],
            vec![]
        )
        .unwrap(),
        0
    ));
    let authority_path = root.path().join("distribution/authority.json");
    let before = distribution.authority.lock().unwrap().current().clone();
    std::fs::remove_file(&authority_path).unwrap();
    std::fs::create_dir(&authority_path).unwrap();
    assert!(distribution
        .export(&publications, &pir_pipeline::engine::PublisherEngine)
        .await
        .is_err());
    assert!(!distribution.authority.lock().unwrap().current().eligible);
    std::fs::remove_dir(&authority_path).unwrap();
    distribution
        .export(&publications, &pir_pipeline::engine::PublisherEngine)
        .await
        .unwrap();
    let recovered = distribution.authority.lock().unwrap().current().clone();
    assert!(recovered.eligible);
    assert!(recovered.fence.recovery_epoch > before.fence.recovery_epoch);
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", socket.local_addr().unwrap());
    let router = Router::new().nest(PREFIX.trim_end_matches('/'), distribution.router());
    let server = tokio::spawn(async move {
        axum::serve(socket, router).await.unwrap();
    });
    let tls = tls::Tls::start(&origin).await;
    let client = reqwest::Client::builder()
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&tls.certificate).unwrap()).unwrap(),
        )
        .build()
        .unwrap();
    let url = format!("{}{PREFIX}current", tls.origin);
    let current: Current = client.get(&url).send().await.unwrap().json().await.unwrap();
    assert!(current.eligible);
    assert!(current
        .publication
        .as_ref()
        .unwrap()
        .descriptor
        .directory
        .is_none());
    let reference = current.inventory().unwrap();
    let bytes = client
        .get(format!(
            "{}{PREFIX}objects/{}",
            tls.origin, reference.sha256
        ))
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    reference.verify(&bytes).unwrap();
    let inventory: Inventory = serde_json::from_slice(&bytes).unwrap();
    inventory.validate(activation).unwrap();
    assert_eq!(
        inventory.filters.as_ref().unwrap().head.tip.height,
        activation
    );
    // Unchanged observations do not rebuild objects or advance authority.
    distribution
        .export(&publications, &pir_pipeline::engine::PublisherEngine)
        .await
        .unwrap();
    let unchanged: Current = client.get(&url).send().await.unwrap().json().await.unwrap();
    assert_eq!(unchanged, current);
    let request = AuthorityCheckRequest {
        authority: current.authority().unwrap(),
        subjects: vec![inventory.anchors()],
    };
    let response: AuthorityCheckResponse<Subject> = client
        .post(format!("{}{PREFIX}authority/check", tls.origin))
        .json(&request)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(request.verdicts(response).unwrap(), [true]);
    let duplicate = AuthorityCheckRequest {
        authority: current.authority().unwrap(),
        subjects: vec![vec![(activation, [3; 32]); 4]; 3],
    };
    calls.store(0, Ordering::SeqCst);
    let response: AuthorityCheckResponse<Subject> = client
        .post(format!("{}{PREFIX}authority/check", tls.origin))
        .json(&duplicate)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(duplicate.verdicts(response).unwrap(), [true, true, true]);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "one network check and one shared anchor lookup"
    );
    let too_many = AuthorityCheckRequest {
        authority: current.authority().unwrap(),
        subjects: vec![vec![(activation, [3; 32]); MAX_CHECK_ANCHORS / 2 + 1]; 2],
    };
    calls.store(0, Ordering::SeqCst);
    let response = client
        .post(format!("{}{PREFIX}authority/check", tls.origin))
        .json(&too_many)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "reject total nested work before node RPC"
    );
    let disproven = AuthorityCheckRequest {
        authority: current.authority().unwrap(),
        subjects: vec![vec![(activation, [9; 32]), (activation + 1, [3; 32])]],
    };
    calls.store(0, Ordering::SeqCst);
    let response: AuthorityCheckResponse<Subject> = client
        .post(format!("{}{PREFIX}authority/check", tls.origin))
        .json(&disproven)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(disproven.verdicts(response).unwrap(), [false]);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "stop when a subject is disproven"
    );
    fork.store(true, Ordering::SeqCst);
    let response: AuthorityCheckResponse<Subject> = client
        .post(format!("{}{PREFIX}authority/check", tls.origin))
        .json(&request)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(request.verdicts(response).unwrap(), [false]);
    publications.revoke();
    let withdrawn: Current = client.get(&url).send().await.unwrap().json().await.unwrap();
    assert!(!withdrawn.eligible);
    assert!(withdrawn.fence.latest_withdrawal > current.sequence);
    let restarted = Distribution::open(root.path().into(), rpc, 600).unwrap();
    let reopened = restarted.authority.lock().unwrap().current().clone();
    assert!(!reopened.eligible);
    assert!(reopened.sequence > withdrawn.sequence);
    assert!(reopened.fence.recovery_epoch > withdrawn.fence.recovery_epoch);
    server.abort();
    task.abort();
}

#[tokio::test]
async fn a_restarted_publisher_republishes_the_same_tables_under_a_new_publication() {
    let genesis = Network::Mainnet.genesis_hash();
    let activation = crate::blocks::ironwood_activation();
    let node = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rpc = NodeClient::new(format!("http://{}", node.local_addr().unwrap()), None).unwrap();
    let task = tokio::spawn(async move {
        axum::serve(node, Router::new().route("/", post(move |Json(request): Json<serde_json::Value>| async move {
            let hash = if request["params"][0] == 0 { genesis } else { [3; 32] };
            Json(serde_json::json!({"result":zakura_server_chain::hash::display(&hash),"error":null}))
        }))).await.unwrap();
    });
    let root = tempfile::tempdir().unwrap();
    let export = |distribution: Distribution| async move {
        let publications = Publications::default();
        distribution.attach(&publications);
        assert!(publications.publish_filters(
            receiver_pir_server::blocks::BlockWindow::new(
                genesis,
                0,
                activation,
                [0; 32],
                vec![[3; 32]],
                vec![]
            )
            .unwrap(),
            0
        ));
        distribution
            .export(&publications, &pir_pipeline::engine::PublisherEngine)
            .await
            .unwrap();
        let current = distribution.authority.lock().unwrap().current().clone();
        assert!(current.eligible);
        current.publication.unwrap()
    };
    let first = export(Distribution::open(root.path().into(), rpc.clone(), 600).unwrap()).await;
    // A restart serves its last tables again under its own recovery epoch,
    // but only for the history revision they were built from, and once.
    let restarted = Distribution::open(root.path().into(), rpc.clone(), 600).unwrap();
    let publications = Publications::default();
    restarted.attach(&publications);
    let revision = first.descriptor.history_revision;
    assert!(!restarted
        .restore(&publications, revision + 1)
        .await
        .unwrap());
    assert!(restarted.restore(&publications, revision).await.unwrap());
    let current = restarted.authority.lock().unwrap().current().clone();
    assert!(current.eligible);
    let restored = current.publication.unwrap();
    assert!(restored.descriptor.recovery_epoch > first.descriptor.recovery_epoch);
    assert_eq!(restored.descriptor.filters, first.descriptor.filters);
    assert!(!restarted.restore(&publications, revision).await.unwrap());
    // The restart offers no authority until it exports again; a query server
    // that saw that withdrawal never serves the earlier publication again.
    let second = export(Distribution::open(root.path().into(), rpc, 600).unwrap()).await;
    assert_ne!(second.publication, first.publication);
    assert!(second.descriptor.recovery_epoch > first.descriptor.recovery_epoch);
    assert_eq!(second.descriptor.filters, first.descriptor.filters);
    task.abort();
}
