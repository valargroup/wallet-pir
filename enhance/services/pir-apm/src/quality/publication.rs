use super::services::{Kind, Service};
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
/// The coordinator's published (height, block hash, map digest). A history
/// publisher reports them at the top level; a txid display controller under
/// `published`.
fn published(status: &Value, kind: Kind) -> (Option<u64>, Option<&str>, Option<String>) {
    let (height, hash, map) = match kind {
        Kind::History => (
            &status["public_height"],
            &status["public_hash"],
            &status["map_sha256"],
        ),
        Kind::Txid => (
            &status["published"]["tip"],
            &status["published"]["tip_hash"],
            &status["published"]["map_sha256"],
        ),
    };
    (
        height.as_u64(),
        hash.as_str(),
        map.as_str()
            .filter(|v| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit()))
            .map(String::from),
    )
}
/// Reads the coordinator status, checks the published block against the node,
/// and checks every public origin serves the same map under the service's path.
pub async fn check(client: &reqwest::Client, service: &Service) -> View {
    let mut view = View {
        sampled_at: pir_apm::incidents::unix_time(),
        ..Default::default()
    };
    let status_url = service.status_url();
    let Some(first) = json(client, &status_url).await else {
        return view;
    };
    let (height, reference, map) = published(&first, service.kind);
    view.public_height = height;
    view.map_sha256 = map;
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
    if !service.public_origins.is_empty() {
        let mut observed = Vec::new();
        for origin in &service.public_origins {
            let url = format!(
                "{}{}",
                origin.trim_end_matches('/'),
                service.public_init_path()
            );
            observed.push(json(client, &url).await);
        }
        let after = json(client, &status_url).await;
        // A publication between reads is an observation gap, not a mismatch.
        if after
            .as_ref()
            .is_some_and(|v| published(v, service.kind).2 == view.map_sha256)
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn published_block_is_read_per_kind() {
        let map = "ab".repeat(32);
        let history =
            serde_json::json!({"public_height": 7, "public_hash": "h", "map_sha256": map});
        let txid = serde_json::json!({"published": {"tip": 9, "tip_hash": "t", "map_sha256": map}});
        assert_eq!(
            published(&history, Kind::History),
            (Some(7), Some("h"), Some(map.clone()))
        );
        assert_eq!(
            published(&txid, Kind::Txid),
            (Some(9), Some("t"), Some(map))
        );
        assert_eq!(published(&history, Kind::Txid), (None, None, None));
    }
}
