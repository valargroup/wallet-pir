//! The independent wallet library (zakura-pir-enhance, native feature) must
//! decode real answers from the distributed native serving path: coordinator
//! control + ingress + packing router + workers, exactly as production wires them.
#![cfg(feature = "native-reinspiring")]
use axum::{
    extract::Request,
    routing::{get, post},
    Router,
};
use enhance_pir::RECORD_BYTES;
use enhance_pir_server::{
    control::{Group, Ledger, Replica},
    coordinator::Coordinator,
    packing_router::{PackingRouter, RouterRegistration},
    query_ingress::QueryIngress,
    store::RecordJournal,
    types::{DatabaseId, ENHANCE_LAYOUT},
    worker::Worker,
};
use tokio_stream::StreamExt;
use wallet::transport::{Method, Request as WalletRequest, ResponseBody, Transport};
use zakura_pir_enhance as wallet;

/// The library's own transport is HTTPS-only; the loopback fixture speaks plain
/// HTTP, so the test supplies a transport with the same bounded collection.
struct PlainHttp(reqwest::Client);
impl Transport for PlainHttp {
    async fn execute(&self, request: WalletRequest) -> Result<ResponseBody, wallet::ClientError> {
        let mut body = request.response_body();
        let mut response = self
            .0
            .request(
                match request.method {
                    Method::Get => reqwest::Method::GET,
                    Method::Post => reqwest::Method::POST,
                },
                request.url,
            )
            .header("content-type", "application/octet-stream")
            .body(request.body)
            .send()
            .await
            .map_err(|e| wallet::ClientError::Transport(e.to_string()))?;
        if !response.status().is_success() {
            return Err(wallet::ClientError::HttpStatus(response.status().as_u16()));
        }
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| wallet::ClientError::Transport(e.to_string()))?
        {
            body.extend(&chunk)?;
        }
        Ok(body.finish())
    }
}

async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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
async fn wallet_library_decodes_native_two_mask_answers_over_the_distributed_path() {
    assert_eq!(
        wallet::PROTOCOL_REVISION,
        enhance_pir::protocol::PROTOCOL_REVISION
    );
    assert_eq!(
        wallet::parameter_id(32768).unwrap(),
        enhance_pir::protocol::parameter_id(32768).unwrap()
    );
    for rows in [2048, 4096, 8192] {
        assert_eq!(
            wallet::unit_parameter_id(rows).unwrap(),
            enhance_pir::protocol::unit_parameter_id(rows).unwrap()
        );
    }
    let root = tempfile::tempdir().unwrap();
    let mut tasks = Vec::new();
    let mut replicas = Vec::new();
    for index in 0..2 {
        let worker = Worker::open(&root.path().join(format!("worker-{index}"))).unwrap();
        let (url, task) = serve(worker.router()).await;
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
        id: "pair".into(),
        sequence: 0,
        replicas,
    }];
    let artifact_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let artifact_origin = format!("http://{}", artifact_listener.local_addr().unwrap());
    let packing =
        PackingRouter::open(&root.path().join("packing"), &artifact_origin, 6, 4).unwrap();
    packing.start_worker_health_monitor();
    let (packing_control, task) = serve(packing.control_router()).await;
    tasks.push(task);
    let (packing_query, task) = serve(packing.public_router()).await;
    tasks.push(task);
    let ingress = QueryIngress::open(&root.path().join("ingress"), 16).unwrap();
    let (ingress_control, task) = serve(ingress.control_router()).await;
    tasks.push(task);
    let (ingress_query, task) = serve(ingress.public_router()).await;
    tasks.push(task);
    let coordinator = Coordinator::open_with_serving(
        &root.path().join("coordinator"),
        groups,
        vec![RouterRegistration {
            name: "packing".into(),
            url: packing_control,
            query_url: packing_query,
            domains: Default::default(),
        }],
        vec![ingress_control],
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
    // Minimal Caddy equivalent: query bodies to ingress, everything else to the coordinator.
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
                        .header("x-forwarded-for", "203.0.113.7")
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
    // Three used rows of a 4,096-row domain: the wallet selects over one
    // 2,048-row block, so this also covers its prefix upload end to end.
    let records: u64 = 67;
    let anchor_hash = "01".repeat(32);
    journal
        .append_block(
            3428143,
            anchor_hash.clone(),
            &(0..records).map(record).collect::<Vec<_>>(),
        )
        .unwrap();
    coordinator
        .publish(&journal, 3428143, anchor_hash.clone())
        .await
        .unwrap();

    // Wallet side: independent implementation, wallet-owned anchor acceptance.
    let transport = PlainHttp(reqwest::Client::new());
    let pending = wallet::transport::PendingClient::fetch(&transport, &public)
        .await
        .unwrap();
    assert_eq!(
        pending.manifest().protocol_revision,
        wallet::PROTOCOL_REVISION
    );
    let hash: [u8; 32] = hex::decode(&anchor_hash).unwrap().try_into().unwrap();
    let acceptance = wallet::GenerationAcceptance::new(
        "main",
        0,
        wallet::AcceptedAnchor::new(3428143, hash, records),
        wallet::ClientResourceLimits::new(32768),
    );
    let mut client = pending.accept(&acceptance).unwrap();
    let positions = [0u64, 1, 32, 33, 34, records - 1];
    let stream = client.query_batch(&transport, positions).unwrap();
    let mut stream = std::pin::pin!(stream);
    let mut answered = 0;
    while let Some(result) = stream.next().await {
        let got = result
            .record
            .unwrap_or_else(|e| panic!("position {}: {e}", result.position));
        assert_eq!(
            got.as_bytes().as_slice(),
            record(result.position).as_slice()
        );
        answered += 1;
    }
    assert_eq!(answered, positions.len());
    // A wrong wallet anchor is refused before any session material is fetched.
    let pending = wallet::transport::PendingClient::fetch(&transport, &public)
        .await
        .unwrap();
    let wrong = wallet::GenerationAcceptance::new(
        "main",
        0,
        wallet::AcceptedAnchor::new(3428143, [7; 32], records),
        wallet::ClientResourceLimits::new(32768),
    );
    assert!(pending.accept(&wrong).is_err());
    heartbeat.abort();
    for task in tasks {
        task.abort();
    }
}
