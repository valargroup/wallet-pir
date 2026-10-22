//! The independent wallet library (zakura-pir-enhance, native feature) must
//! decode real answers through publisher HTTPS, autonomous workers, packing
//! routers and ingress.
#![cfg(feature = "native-reinspiring")]
use axum::{
    extract::Request,
    routing::{get, post},
    Router,
};
use enhance_query_server::RECORD_BYTES;
use pir_e2e_tests::enhance_fixture::System;
use tokio_stream::StreamExt;
use wallet::transport::{Method, Request as WalletRequest, ResponseBody, Transport};
use zakura_pir_client::enhance as wallet;

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
        enhance_query_server::protocol::PROTOCOL_REVISION
    );
    assert_eq!(
        wallet::parameter_id(32768).unwrap(),
        enhance_query_server::protocol::parameter_id(32768).unwrap()
    );
    for rows in [2048, 4096, 8192] {
        assert_eq!(
            wallet::unit_parameter_id(rows).unwrap(),
            enhance_query_server::protocol::unit_parameter_id(rows).unwrap()
        );
    }
    let records: u64 = 67;
    let anchor_hash = "01".repeat(32);
    let system =
        System::with_backend((0..records).map(record).collect(), Default::default(), 1, 2).await;
    let ingress_query = system.origin.clone();
    let mut tasks = Vec::new();
    // Minimal Caddy equivalent: every wallet route goes to query ingress.
    let query_origin = ingress_query.clone();
    let metadata_origin = ingress_query.clone();
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
    for task in tasks {
        task.abort();
    }
}
