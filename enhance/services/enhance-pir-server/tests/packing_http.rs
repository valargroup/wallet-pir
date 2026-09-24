//! Real wallet crypto over coordinator control + ingress + packing router + workers.
use axum::{
    extract::Request,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use enhance_pir::{client::EnhancePirClient, RECORD_BYTES};
use enhance_pir_server::{
    control::{Group, Ledger, Replica},
    coordinator::Coordinator,
    packing_router::{PackingRouter, RouterRegistration},
    query_ingress::QueryIngress,
    store::RecordJournal,
    types::{DatabaseId, ENHANCE_LAYOUT},
    worker::Worker,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind(
        std::env::var("QUALIFY_BIND").unwrap_or_else(|_| "127.0.0.1:0".into()),
    )
    .await
    .unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    (
        origin,
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
    )
}
fn record(n: u64) -> Vec<u8> {
    let mut bytes = vec![0; RECORD_BYTES];
    bytes[..8].copy_from_slice(&n.to_le_bytes());
    bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extracted_path_preserves_wallet_answers_and_pool_replication() {
    let root = tempfile::tempdir().unwrap();
    let mut tasks = Vec::new();
    let mut replicas = Vec::new();
    let mut counts = Vec::new();
    let mut modes = Vec::new();
    for index in 0..3 {
        let worker = Worker::open(&root.path().join(format!("worker-{index}"))).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let mode = Arc::new(AtomicUsize::new(0));
        counts.push(count.clone());
        modes.push(mode.clone());
        let router = worker.router().layer(axum::middleware::from_fn(
            move |request: Request, next: axum::middleware::Next| {
                let count = count.clone();
                let mode = mode.clone();
                async move {
                    if request.uri().path() == "/internal/evaluate" {
                        count.fetch_add(1, Ordering::SeqCst);
                        match mode.load(Ordering::SeqCst) {
                            1 => {
                                return (
                                    axum::http::StatusCode::TOO_MANY_REQUESTS,
                                    [("x-enhance-evaluation", "not-accepted")],
                                )
                                    .into_response()
                            }
                            2 => {
                                return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                            }
                            _ => {}
                        }
                    }
                    next.run(request).await
                }
            },
        ));
        let (url, task) = serve(router).await;
        tasks.push(task);
        replicas.push(Replica {
            name: format!("worker-{index}"),
            url,
            incarnation: String::new(),
            ledger: Ledger::default(),
        });
    }
    let groups = vec![Group {
        placement_policy: Default::default(),
        id: "legacy-pair".into(),
        sequence: 0,
        replicas: replicas[..2].to_vec(),
    }];
    // Reserve the artifact listener first so the router can pin its trusted origin.
    let artifact_listener = tokio::net::TcpListener::bind(
        std::env::var("QUALIFY_ARTIFACT_BIND").unwrap_or_else(|_| "127.0.0.1:0".into()),
    )
    .await
    .unwrap();
    let artifact_origin = format!("http://{}", artifact_listener.local_addr().unwrap());
    let (packing_control, packing_query) =
        if let Ok(control) = std::env::var("QUALIFY_ROUTER_CONTROL") {
            (control, std::env::var("QUALIFY_ROUTER_QUERY").unwrap())
        } else {
            let packing =
                PackingRouter::open(&root.path().join("packing"), &artifact_origin, 6, 4).unwrap();
            let (control, task) = serve(packing.control_router()).await;
            tasks.push(task);
            let (query, task) = serve(packing.public_router()).await;
            tasks.push(task);
            (control, query)
        };
    let ingress = QueryIngress::open(&root.path().join("ingress"), 16).unwrap();
    let (ingress_control, task) = serve(ingress.control_router()).await;
    tasks.push(task);
    let (ingress_query, task) = serve(ingress.public_router()).await;
    tasks.push(task);
    let coordinator = Coordinator::open_with_serving(
        &root.path().join("coordinator"),
        groups.clone(),
        vec![RouterRegistration {
            name: "packing".into(),
            url: packing_control.clone(),
            query_url: packing_query.clone(),
            domains: Default::default(),
        }],
        vec![ingress_control.clone()],
    )
    .unwrap();
    let artifacts = coordinator.artifact_router();
    tasks.push(tokio::spawn(async move {
        axum::serve(artifact_listener, artifacts).await.unwrap()
    }));
    let (control_origin, task) = serve(coordinator.clone().router()).await;
    tasks.push(task);
    let heart = coordinator.clone();
    let heartbeat = tokio::spawn(async move {
        loop {
            heart.refresh_routers().await;
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    });
    // Minimal Caddy equivalent: route query bodies to ingress, control GETs to coordinator.
    let query_origin = ingress_query.clone();
    let metadata_origin = control_origin.clone();
    let proxy = Router::new()
        .route(
            "/v1/enhance/query",
            post(move |request: Request| {
                let origin = query_origin.clone();
                async move {
                    let body = axum::body::to_bytes(request.into_body(), 512 * 1024)
                        .await
                        .unwrap();
                    let response = reqwest::Client::new()
                        .post(format!("{origin}/v1/enhance/query"))
                        .body(body)
                        .send()
                        .await
                        .unwrap();
                    (response.status(), response.bytes().await.unwrap())
                }
            }),
        )
        .fallback(get(move |request: Request| {
            let origin = metadata_origin.clone();
            async move {
                let response = reqwest::get(format!("{origin}{}", request.uri()))
                    .await
                    .unwrap();
                (response.status(), response.bytes().await.unwrap())
            }
        }));
    let (public, task) = serve(proxy).await;
    tasks.push(task);
    let mut journal = RecordJournal::open(
        root.path().join("journal"),
        DatabaseId::Enhance,
        ENHANCE_LAYOUT,
    )
    .unwrap();
    let records: u64 = std::env::var("QUALIFY_RECORDS")
        .ok()
        .map(|s| s.parse().unwrap())
        .unwrap_or(67);
    journal
        .append_block(
            3428143,
            "01".repeat(32),
            &(0..records).map(record).collect::<Vec<_>>(),
        )
        .unwrap();
    coordinator
        .publish(&journal, 3428143, "01".repeat(32))
        .await
        .unwrap();
    let mut client = EnhancePirClient::connect(&public).await.unwrap();
    for position in [0, 32, 33, records - 1] {
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
    assert!(counts[..2].iter().all(|c| c.load(Ordering::SeqCst) > 0));
    let http = reqwest::Client::new();
    let health: serde_json::Value = http
        .get(format!("{packing_control}/internal/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["resident_objects"], 1);
    let timing = http
        .get(format!("{packing_control}/internal/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for stage in ["worker", "packing", "total"] {
        let prefix = format!("enhance_query_stage_duration_seconds_count{{stage=\"{stage}\"}} ");
        let count: u64 = timing
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .expect("stage histogram")
            .parse()
            .unwrap();
        assert!(count > 0, "missing successful {stage} samples");
    }

    // Explicit rejection spills once; ambiguous failure never touches another worker.
    let session = client.session(0).await.unwrap();
    for mode in [1, 2] {
        for m in &modes[..2] {
            m.store(mode, Ordering::SeqCst);
        }
        let before: usize = counts.iter().map(|c| c.load(Ordering::SeqCst)).sum();
        let (query, _) = session.prepare_position(0).unwrap();
        let response = http
            .post(format!("{public}/v1/enhance/query"))
            .body(query.body().to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if mode == 1 {
                axum::http::StatusCode::TOO_MANY_REQUESTS
            } else {
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            }
        );
        let after: usize = counts.iter().map(|c| c.load(Ordering::SeqCst)).sum();
        assert_eq!(after - before, if mode == 1 { 2 } else { 1 });
    }
    for m in &modes {
        m.store(0, Ordering::SeqCst);
    }
    let mut expanded = groups.clone();
    expanded.push(Group {
        placement_policy: Default::default(),
        id: "single-worker".into(),
        sequence: 1,
        replicas: vec![replicas[2].clone()],
    });
    coordinator.reconcile_inventory(expanded).await.unwrap();
    coordinator.enable_pool(3).unwrap();
    journal
        .append_block(3428144, "02".repeat(32), &Vec::<Vec<u8>>::new())
        .unwrap();
    // Routing-only publication must reuse packing material despite extra evaluation replica.
    coordinator
        .publish(&journal, 3428144, "02".repeat(32))
        .await
        .unwrap();
    let mut client = EnhancePirClient::connect(&public).await.unwrap();
    for position in [0, 33, 66] {
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
    assert!(counts[2].load(Ordering::SeqCst) > 0);
    let health: serde_json::Value = http
        .get(format!("{packing_control}/internal/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["resident_objects"], 1);
    let health: serde_json::Value = http
        .get(format!("{control_origin}/v1/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        health["pool"]["placements"]["0"].as_array().unwrap().len(),
        3
    );
    assert_eq!(
        http.post(format!("{control_origin}/v1/enhance/query"))
            .body(vec![0; 116])
            .send()
            .await
            .unwrap()
            .status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    let publications: u64 = std::env::var("QUALIFY_PUBLICATIONS")
        .ok()
        .map(|s| s.parse().unwrap())
        .unwrap_or(0);
    for revision in 0..publications {
        let start = std::time::Instant::now();
        let mut load = tokio::task::JoinSet::new();
        for lane in 0..4 {
            let public = public.clone();
            load.spawn(async move {
                let mut client = EnhancePirClient::connect(&public).await.unwrap();
                let mut latencies = Vec::new();
                for n in 0..8 {
                    let position = (lane * 97 + n * 31) % records;
                    let now = std::time::Instant::now();
                    let result = client.query_position_with_timing(position).await.unwrap();
                    assert_eq!(result.0.as_ref(), record(position));
                    latencies.push(now.elapsed().as_secs_f64());
                }
                latencies
            });
        }
        let height = 3428145 + revision;
        let hash = format!("{:064x}", revision + 3);
        journal
            .append_block(
                height,
                hash.clone(),
                &(records + revision * 33..records + (revision + 1) * 33)
                    .map(record)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        coordinator.publish(&journal, height, hash).await.unwrap();
        let mut times = Vec::new();
        while let Some(result) = load.join_next().await {
            times.extend(result.unwrap());
        }
        times.sort_by(f64::total_cmp);
        let health: serde_json::Value = http
            .get(format!("{packing_control}/internal/health"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(health["resident_objects"].as_u64().unwrap() <= 6);
        println!(
            "QUALIFICATION {}",
            serde_json::json!({"publication":revision + 1,"records": records + (revision + 1)*33,"verified":times.len(),"elapsed_seconds":start.elapsed().as_secs_f64(),"p50_seconds":times[times.len()/2],"p95_seconds":times[times.len()*95/100],"p99_seconds":times[times.len()*99/100],"router":health})
        );
    }
    if publications > 0 {
        use tokio::io::AsyncWriteExt;
        let mut client = EnhancePirClient::connect(&public).await.unwrap();
        let session = client.session(0).await.unwrap();
        let (query, _) = session.prepare_position(0).unwrap();
        let endpoint = ingress_query.strip_prefix("http://").unwrap();
        let mut uploads = Vec::new();
        for _ in 0..20 {
            let mut stream = tokio::net::TcpStream::connect(endpoint).await.unwrap();
            let request = format!("POST /v1/enhance/query HTTP/1.1\r\nHost: {endpoint}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", query.body().len());
            stream.write_all(request.as_bytes()).await.unwrap();
            stream.write_all(&query.body()[..116]).await.unwrap();
            uploads.push(stream);
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        drop(uploads);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(40);
        loop {
            let health: serde_json::Value = http
                .get(format!("{packing_control}/internal/health"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if health["available_requests"] == 4 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "disconnected uploads leaked admission"
            );
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        assert_eq!(
            client
                .query_position_with_timing(0)
                .await
                .unwrap()
                .0
                .as_ref(),
            record(0)
        );
        println!("QUALIFICATION slow_upload_disconnect=passed clients=20");
    }
    // A lost controller must stop serving without treating its watchdog as a recovery ACK.
    heartbeat.abort();
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    let mut latest = EnhancePirClient::connect(&public).await.unwrap();
    let session = latest.session(0).await.unwrap();
    let (query, _) = session.prepare_position(0).unwrap();
    assert_eq!(
        http.post(format!("{public}/v1/enhance/query"))
            .body(query.body().to_vec())
            .send()
            .await
            .unwrap()
            .status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    coordinator.refresh_routers().await;
    // An optional physical process restart is driven externally, never by the fixture itself.
    if let Ok(marker) = std::env::var("QUALIFY_RESTART_FILE") {
        std::fs::write(&marker, b"restart router, then remove this file").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
        while std::path::Path::new(&marker).exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "physical restart not acknowledged"
            );
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        let health: serde_json::Value = http
            .get(format!("{packing_control}/internal/health"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(health["ready"], false);
        assert_eq!(health["resident_objects"], 0);
        coordinator.refresh_routers().await;
        coordinator.reconcile().await.unwrap();
    }
    let heart = coordinator.clone();
    let heartbeat = tokio::spawn(async move {
        loop {
            heart.refresh_routers().await;
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    });
    let mut latest = EnhancePirClient::connect(&public).await.unwrap();
    assert_eq!(
        latest
            .query_position_with_timing(records - 1)
            .await
            .unwrap()
            .0
            .as_ref(),
        record(records - 1)
    );
    // Revocation fences an already constructed wallet request at ingress and router.
    let session = latest.session(0).await.unwrap();
    let (revoked_query, _) = session.prepare_position(0).unwrap();
    let binding = enhance_pir::protocol::QueryBinding::decode(revoked_query.body()).unwrap();
    let fence = enhance_pir_server::worker::Revocation {
        recovery_epoch: 1,
        sessions: [hex::encode(binding.session_id)].into_iter().collect(),
    };
    for origin in [&packing_control, &ingress_control] {
        http.post(format!("{origin}/internal/revoke"))
            .json(&fence)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    for origin in [&public, &packing_query] {
        let response = http
            .post(format!("{origin}/v1/enhance/query"))
            .body(revoked_query.body().to_vec())
            .send()
            .await
            .unwrap();
        assert!(!response.status().is_success(), "revoked session served");
    }
    heartbeat.abort();
    println!("QUALIFICATION_COMPLETE records={records} publications={publications} watchdog=passed restart={} revocation=passed", std::env::var("QUALIFY_RESTART_FILE").is_ok());
    for task in tasks {
        task.abort();
    }
}
