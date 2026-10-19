//! The offline repair CLI restores real current and retained encrypted HTTP queries.
use enhance_publisher::{
    control::{Group, Ledger, Replica},
    coordinator::Coordinator,
    store::RecordJournal,
};
use enhance_query_server::{client::EnhancePirClient, RECORD_BYTES};
use enhance_query_server::{
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

fn copy_control(source: &std::path::Path, destination: &std::path::Path) {
    std::fs::create_dir_all(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_control(&entry.path(), &target);
        } else if matches!(
            source.file_name().and_then(|name| name.to_str()),
            Some("hints" | "public")
        ) || source.file_name()
            == Some(std::ffi::OsStr::new(
                enhance_query_server::prepared_packing::DIRECTORY,
            ))
        {
            // Content-addressed publication bytes are immutable. Keep the old
            // inode while the live controller may unlink its reference later.
            std::fs::hard_link(entry.path(), target).unwrap();
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
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
    let coordinator = Coordinator::open(&root.path().join("control"), groups.clone()).unwrap();
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
    // Freeze the published controller snapshot, not the worker. Its real HTTP
    // router can keep querying the old generation after the live router advances.
    let retained_control = root.path().join("retained-control");
    copy_control(&root.path().join("control"), &retained_control);
    let old_coordinator = Coordinator::open(&retained_control, groups).unwrap();
    let (retained_origin, retained_server) = serve(old_coordinator.router()).await;
    let mut retained = EnhancePirClient::connect(&retained_origin).await.unwrap();
    let retained_generation = retained.manifest().generation;
    let retained_session = retained.session(0).await.unwrap();
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
    for expected_restored in [2, 0] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_enhance-pir-server-test"))
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
    let http = reqwest::Client::new();
    // Pin the old binding explicitly: a fast fixture must not depend on the
    // client's thirty-second refresh changing this into a current-generation test.
    for positions in [&[0, 32][..], &[33][..], &[66][..]] {
        let (query, _) = retained_session.prepare_position(positions[0]).unwrap();
        let binding = enhance_query_server::protocol::QueryBinding::decode(query.body()).unwrap();
        assert_eq!(binding.generation, retained_generation);
        if positions[0] == 0 {
            assert_eq!(
                http.post(format!("{origin}/v1/enhance/query"))
                    .body(query.body().to_vec())
                    .send()
                    .await
                    .unwrap()
                    .status(),
                reqwest::StatusCode::CONFLICT,
                "the current router requires its current generation"
            );
        }
        let answer = http
            .post(format!("{retained_origin}/v1/enhance/query"))
            .body(query.body().to_vec())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let row = retained_session.decode(query, &answer).unwrap();
        for position in positions {
            let slot = *position as usize % enhance_query_server::RECORDS_PER_ROW;
            assert_eq!(
                enhance_query_server::client::record_in_row(&row, slot)
                    .unwrap()
                    .as_ref(),
                record(*position)
            );
        }
    }
    let positions = [67, 99];
    for (position, answer) in positions
        .into_iter()
        .zip(current.query_positions(&positions).await.unwrap())
    {
        assert_eq!(answer.as_ref(), record(position));
    }
    assert!(current.manifest().generation > retained_generation);
    server.abort();
    retained_server.abort();
    recovered.abort();
    let _ = server.await;
    let _ = retained_server.await;
    let _ = recovered.await;
}
