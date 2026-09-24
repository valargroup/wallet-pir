//! Opt-in deployed endpoint check using a separately verified canonical anchor.
use futures_util::StreamExt;
use zakura_pir_enhance::{
    transport::{Method, PendingClient, Request, ResponseBody, Transport},
    AcceptedAnchor, ClientError, ClientResourceLimits, GenerationAcceptance,
};
struct Http(reqwest::Client);
impl Transport for Http {
    async fn execute(&self, request: Request) -> Result<ResponseBody, ClientError> {
        let mut body = request.response_body();
        let method = match request.method {
            Method::Get => reqwest::Method::GET,
            Method::Post => reqwest::Method::POST,
        };
        let mut response = self
            .0
            .request(method, request.url)
            .body(request.body)
            .send()
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        if !response.status().is_success() {
            return Err(ClientError::HttpStatus(response.status().as_u16()));
        }
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?
        {
            body.extend(&chunk)?;
        }
        Ok(body.finish())
    }
}
#[tokio::test]
async fn public_wallet_matches_independent_canonical_records() {
    let origin = std::env::var("ENHANCE_PUBLIC_ORIGIN").unwrap();
    let anchor: serde_json::Value = serde_json::from_slice(
        &std::fs::read(std::env::var("ENHANCE_PUBLIC_ANCHOR").unwrap()).unwrap(),
    )
    .unwrap();
    let oracle: serde_json::Value = serde_json::from_slice(
        &std::fs::read(std::env::var("ENHANCE_PUBLIC_ORACLE").unwrap()).unwrap(),
    )
    .unwrap();
    let acceptance = GenerationAcceptance::new(
        "main",
        3_428_143,
        AcceptedAnchor::new(
            anchor["height"].as_u64().unwrap(),
            hex::decode(anchor["hash"].as_str().unwrap())
                .unwrap()
                .try_into()
                .unwrap(),
            anchor["records"].as_u64().unwrap(),
        ),
        ClientResourceLimits::with_cache(32768, 24),
    );
    let transport = Http(
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
    );
    let pending = PendingClient::fetch(&transport, &origin).await.unwrap();
    let mut client = pending.accept(&acceptance).unwrap();
    let samples = oracle.as_array().unwrap();
    assert!(!samples.is_empty());
    for sample in samples.iter().take(32) {
        let position = sample["position"].as_u64().unwrap();
        let stream = client.query_batch(&transport, [position]).unwrap();
        futures_util::pin_mut!(stream);
        let record = stream.next().await.unwrap().record.unwrap();
        assert_eq!(
            record.as_bytes(),
            hex::decode(sample["record_hex"].as_str().unwrap())
                .unwrap()
                .as_slice()
        );
    }
    let pending = PendingClient::fetch(&transport, &origin).await.unwrap();
    client.accept_routing(pending, &acceptance).unwrap();
    let first = &samples[0];
    let covered = client
        .query_positions_with_cover(&transport, &[first["position"].as_u64().unwrap()], 0)
        .await
        .unwrap();
    assert_eq!(
        covered[0].as_bytes(),
        hex::decode(first["record_hex"].as_str().unwrap())
            .unwrap()
            .as_slice()
    );
    println!("Public wallet verified {} canonical records, routing refresh, and cover round at anchor {}", samples.len().min(32), anchor["height"]);
}
