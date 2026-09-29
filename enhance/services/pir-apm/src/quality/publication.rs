use serde::Serialize;
use serde_json::Value;
#[derive(Clone, Debug, Default, Serialize)]
pub struct View {
    pub sampled_at: u64,
    pub public_height: Option<u64>,
    pub lag_blocks: Option<u64>,
    pub canonical: Option<bool>,
    pub origins_agree: Option<bool>,
    pub map_sha256: Option<String>,
}
async fn json(client: &reqwest::Client, url: &str) -> Option<Value> {
    serde_json::from_slice(&super::fetch(client, url).await.ok()?).ok()
}
pub async fn check(client: &reqwest::Client, metrics_url: &str, origins: &[String]) -> View {
    let mut view = View {
        sampled_at: pir_apm::incidents::unix_time(),
        ..Default::default()
    };
    let base = metrics_url.strip_suffix("/metrics").unwrap_or(metrics_url);
    let Some(first) = json(client, &format!("{base}/v1/status")).await else {
        return view;
    };
    view.public_height = first["public_height"].as_u64();
    view.map_sha256 = first["map_sha256"]
        .as_str()
        .filter(|v| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit()))
        .map(String::from);
    let reference = first["public_hash"].as_str();
    if let (Some(height), Some(reference), Ok(rpc_url), Ok(cookie_path)) = (
        view.public_height,
        reference,
        std::env::var("PIR_APM_CHAIN_RPC_URL"),
        std::env::var("PIR_APM_CHAIN_RPC_COOKIE"),
    ) {
        if let Ok(cookie) = tokio::fs::read(cookie_path).await {
            if let Ok(cookie) = std::str::from_utf8(&cookie) {
                if let Some((user, password)) = cookie.trim().split_once(':') {
                    let rpc = |method: &'static str, params: Value| {
                        client.post(&rpc_url).basic_auth(user,Some(password)).json(&serde_json::json!({"jsonrpc":"1.0","id":"quality","method":method,"params":params})).send()
                    };
                    if let Ok(response) = rpc("getblockhash", serde_json::json!([height])).await {
                        if let Ok(v) = response.json::<Value>().await {
                            if v["error"].is_null() {
                                view.canonical = v["result"].as_str().map(|s| s == reference);
                            }
                        }
                    }
                    if let Ok(response) = rpc("getblockcount", serde_json::json!([])).await {
                        if let Ok(v) = response.json::<Value>().await {
                            if v["error"].is_null() {
                                view.lag_blocks =
                                    v["result"].as_u64().and_then(|tip| tip.checked_sub(height));
                            }
                        }
                    }
                }
            }
        }
    }
    if !origins.is_empty() {
        let mut observed = Vec::new();
        for origin in origins {
            observed.push(
                json(
                    client,
                    &format!("{}/v1/shards/init", origin.trim_end_matches('/')),
                )
                .await,
            );
        }
        let after = json(client, &format!("{base}/v1/status")).await;
        // A publication between reads is an observation gap, not a mismatch.
        if after
            .as_ref()
            .is_some_and(|v| v["map_sha256"] == first["map_sha256"])
            && view.map_sha256.is_some()
        {
            view.origins_agree = observed
                .iter()
                .map(|v| {
                    v.as_ref()?
                        .get("map_sha256")?
                        .as_str()
                        .map(|s| Some(s) == view.map_sha256.as_deref())
                })
                .collect::<Option<Vec<_>>>()
                .map(|v| v.into_iter().all(|b| b));
        }
    }
    view
}
