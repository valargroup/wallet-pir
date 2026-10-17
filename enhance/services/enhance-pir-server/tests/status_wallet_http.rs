//! Independent wallet client over real Status coordinator/router/worker listeners.
#![cfg(feature = "native-reinspiring")]
use enhance_pir_server::status::{fixture, now_ms, Controller, Generation};
use ipir_sp::server::MatvecBackend;
use std::sync::atomic::{AtomicBool, Ordering};
use zakura_pir_status::{self as wallet, transport::Transport};

struct Listening {
    origin: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Listening {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn serve(router: axum::Router) -> Listening {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    Listening {
        origin,
        task: tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
    }
}

/// The wallet requires an HTTPS origin. Only this test transport maps its fixed
/// logical origin to a plain HTTP loopback listener; every response is real HTTP.
struct Loopback {
    origin: String,
    http: reqwest::Client,
    corrupt_answer: AtomicBool,
}
impl Loopback {
    async fn request(
        &self,
        url: &str,
        body: Option<Vec<u8>>,
        max_bytes: usize,
    ) -> Result<Vec<u8>, wallet::Error> {
        let path = url
            .strip_prefix("https://status-wallet.invalid")
            .ok_or(wallet::Error::Malformed)?;
        let url = format!("{}{path}", self.origin);
        let request = match body {
            Some(body) => self.http.post(url).body(body),
            None => self.http.get(url),
        };
        let mut response = request
            .send()
            .await
            .map_err(|_| wallet::Error::Unavailable)?;
        if !response.status().is_success() {
            return Err(wallet::Error::Unavailable);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| wallet::Error::Unavailable)?
        {
            if bytes.len().saturating_add(chunk.len()) > max_bytes {
                return Err(wallet::Error::Malformed);
            }
            bytes.extend_from_slice(&chunk);
        }
        if path.ends_with("/query") && self.corrupt_answer.load(Ordering::SeqCst) {
            bytes[0] ^= 1;
        }
        Ok(bytes)
    }
}
impl Transport for Loopback {
    async fn get(&self, url: &str, max_bytes: usize) -> Result<Vec<u8>, wallet::Error> {
        self.request(url, None, max_bytes).await
    }
    async fn post(
        &self,
        url: &str,
        body: Vec<u8>,
        max_bytes: usize,
    ) -> Result<Vec<u8>, wallet::Error> {
        self.request(url, Some(body), max_bytes).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_status_wallet_decrypts_http_and_rejects_invalid_envelopes() {
    let snapshot = fixture::snapshot(32, false).unwrap();
    let (generation, _) =
        Generation::prepare(&snapshot, 1, 1, now_ms(), None, MatvecBackend::Cpu).unwrap();
    let controller = Controller::new(generation);
    let worker = serve(controller.worker_routes()).await;
    let router = serve(controller.router_routes(worker.origin.clone())).await;
    let coordinator = serve(controller.coordinator_routes(router.origin.clone())).await;
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap();
    let transport = Loopback {
        origin: coordinator.origin.clone(),
        http: http.clone(),
        corrupt_answer: AtomicBool::new(false),
    };
    let anchor = wallet::AcceptedAnchor {
        network: snapshot.network,
        height: snapshot.height,
        hash: snapshot.anchor,
    };
    let client = wallet::transport::PendingClient::fetch(
        &transport,
        "https://status-wallet.invalid",
        now_ms,
    )
    .await
    .unwrap()
    .accept(&transport, &anchor)
    .await
    .unwrap();
    let coverage = wallet::LocalCoverageContext {
        earliest_possible_inclusion: Some(snapshot.start),
        required_through: Some(snapshot.height),
    };
    for (txid, expected) in fixture::cases(32, false) {
        let observation = client.observe(&transport, &txid, coverage).await.unwrap();
        let expected = match expected {
            enhance_pir::status::Observation::Mined(height) => wallet::Observation::Mined(height),
            enhance_pir::status::Observation::Mempool => wallet::Observation::Mempool,
            enhance_pir::status::Observation::Forked => wallet::Observation::Forked,
            enhance_pir::status::Observation::NotFound => wallet::Observation::NotFound,
        };
        assert_eq!(observation, expected);
    }
    transport.corrupt_answer.store(true, Ordering::SeqCst);
    assert_eq!(
        client
            .observe(&transport, &fixture::txid(0), coverage)
            .await,
        Err(wallet::Error::Malformed)
    );
    let malformed = http
        .post(format!("{}/v1/status/query", coordinator.origin))
        .body(vec![0; 8])
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), reqwest::StatusCode::BAD_REQUEST);
    let mut stale = enhance_pir::status::QUERY_MAGIC.to_vec();
    stale.resize(enhance_pir::status::HEADER_BYTES, 0);
    let stale = http
        .post(format!("{}/v1/status/query", coordinator.origin))
        .body(stale)
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), reqwest::StatusCode::CONFLICT);
}
