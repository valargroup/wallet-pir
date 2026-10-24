//! The unchanged native Enhance wallet decoding a served generation, shared by
//! the Enhance-only and all-products spawned-host cases.
use enhance_shared::RECORD_BYTES;
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

pub fn record(n: u64) -> Vec<u8> {
    let mut bytes = vec![0; RECORD_BYTES];
    bytes[..8].copy_from_slice(&n.to_le_bytes());
    bytes
}

/// The unchanged wallet accepts the served generation at its own anchor,
/// decodes every queried record exactly and refuses a wrong anchor before
/// fetching any session material.
pub async fn wallet_decodes(public: &str, records: u64, anchor_hash: &str) {
    // Wallet side: independent implementation, wallet-owned anchor acceptance.
    let transport = PlainHttp(reqwest::Client::new());
    let pending = wallet::transport::PendingClient::fetch(&transport, public)
        .await
        .unwrap();
    assert_eq!(
        pending.manifest().protocol_revision,
        wallet::PROTOCOL_REVISION
    );
    let hash: [u8; 32] = hex::decode(anchor_hash).unwrap().try_into().unwrap();
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
    let pending = wallet::transport::PendingClient::fetch(&transport, public)
        .await
        .unwrap();
    let wrong = wallet::GenerationAcceptance::new(
        "main",
        0,
        wallet::AcceptedAnchor::new(3428143, [7; 32], records),
        wallet::ClientResourceLimits::new(32768),
    );
    assert!(pending.accept(&wrong).is_err());
}
