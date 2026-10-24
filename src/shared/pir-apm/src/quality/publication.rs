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
    /// The published anchor hash, when it is a 64-digit hex block hash.
    pub anchor_hash: Option<String>,
    /// The coordinator map read again after the origins.
    pub map_after: Option<String>,
    /// The map published before the current one, set by the sampling loop,
    /// to recognise an origin still serving it.
    pub previous_map: Option<String>,
    /// One entry per configured origin (at most two), in configuration order.
    pub origins: Vec<Origin>,
}
/// One public origin's answer. It holds only the configured origin's scheme,
/// host and port, the HTTP status, a validated map digest and a fixed reason:
/// never a response body, path, query or credential.
#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
pub struct Origin {
    pub origin: String,
    pub observed_at: u64,
    pub http_status: Option<u16>,
    pub map_sha256: Option<String>,
    pub failure: Option<&'static str>,
}
/// Why the origin check did not find agreement, most specific first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Finding {
    /// The coordinator's anchor is not on the node's chain.
    NoncanonicalAnchor,
    /// The coordinator published between the reads: not a disagreement.
    ObservationRace,
    /// An origin could not be read or returned no valid map.
    FailedObservation,
    /// An origin still serves the previously published map.
    LaggingOrigin,
    /// An origin serves a map the coordinator has not recently published.
    Mismatch,
}
impl Finding {
    fn text(self) -> &'static str {
        match self {
            Self::NoncanonicalAnchor => "noncanonical anchor",
            Self::ObservationRace => "observation race",
            Self::FailedObservation => "failed observation",
            Self::LaggingOrigin => "lagging origin",
            Self::Mismatch => "map mismatch",
        }
    }
}
fn hex64(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|v| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit()))
        .map(String::from)
}
/// `scheme://host[:port]` of a configured origin; nothing else of the URL.
fn identity(origin: &str, index: usize) -> String {
    reqwest::Url::parse(origin)
        .ok()
        .and_then(|url| {
            let host = url.host_str()?.to_string();
            Some(match url.port() {
                Some(port) => format!("{}://{host}:{port}", url.scheme()),
                None => format!("{}://{host}", url.scheme()),
            })
        })
        .unwrap_or_else(|| format!("origin {}", index + 1))
}
async fn json(client: &reqwest::Client, url: &str) -> Option<Value> {
    serde_json::from_slice(&super::fetch(client, url).await.ok()?).ok()
}
async fn observe(client: &reqwest::Client, origin: &str, index: usize, path: &str) -> Origin {
    let url = format!("{}{path}", origin.trim_end_matches('/'));
    let (http_status, body) = super::fetch_observed(client, &url).await;
    let mut observed = Origin {
        origin: identity(origin, index),
        observed_at: pir_apm::incidents::unix_time(),
        http_status,
        ..Default::default()
    };
    match body.and_then(|bytes| serde_json::from_slice::<Value>(&bytes).map_err(|_| "invalid JSON"))
    {
        Ok(value) => match hex64(&value["map_sha256"]) {
            Some(map) => observed.map_sha256 = Some(map),
            None => observed.failure = Some("no valid map_sha256"),
        },
        Err(reason) => observed.failure = Some(reason),
    }
    observed
}
impl View {
    /// The reason origins do not agree, or `None` when they do or none are configured.
    pub fn finding(&self) -> Option<Finding> {
        if self.canonical == Some(false) {
            return Some(Finding::NoncanonicalAnchor);
        }
        if self.origins.is_empty() || self.map_sha256.is_none() {
            return None;
        }
        if self.map_after != self.map_sha256 {
            return Some(Finding::ObservationRace);
        }
        if self.origins.iter().any(|o| o.map_sha256.is_none()) {
            return Some(Finding::FailedObservation);
        }
        let differing = self
            .origins
            .iter()
            .filter(|o| o.map_sha256 != self.map_sha256)
            .collect::<Vec<_>>();
        if differing.is_empty() {
            None
        } else if self.previous_map.is_some()
            && differing.iter().all(|o| o.map_sha256 == self.previous_map)
        {
            Some(Finding::LaggingOrigin)
        } else {
            Some(Finding::Mismatch)
        }
    }
    /// Bounded incident evidence: at most two origins of fixed-size fields.
    pub fn evidence(&self) -> String {
        let map = |m: &Option<String>| m.clone().unwrap_or_else(|| "none".into());
        let mut text = format!(
            "{}; expected map {} at anchor {} {}; coordinator after {}",
            self.finding().map_or("origins agree", Finding::text),
            map(&self.map_sha256),
            self.public_height
                .map_or("unknown".into(), |h| h.to_string()),
            map(&self.anchor_hash),
            map(&self.map_after),
        );
        for origin in &self.origins {
            text.push_str(&format!(
                "; {} at {}: HTTP {}, {}",
                origin.origin,
                origin.observed_at,
                origin.http_status.map_or("none".into(), |s| s.to_string()),
                match (&origin.map_sha256, origin.failure) {
                    (Some(map), _) => format!("map {map}"),
                    (None, reason) => reason.unwrap_or("unread").into(),
                },
            ));
        }
        text
    }
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
    view.anchor_hash = reference.and_then(|r| hex64(&Value::from(r)));
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
        let path = service.public_init_path();
        for (index, origin) in service.public_origins.iter().enumerate() {
            view.origins
                .push(observe(client, origin, index, &path).await);
        }
        view.map_after = json(client, &status_url)
            .await
            .and_then(|v| published(&v, service.kind).2);
        // A publication between reads is an observation gap, not a mismatch.
        if view.map_sha256.is_some() && view.map_after == view.map_sha256 {
            view.origins_agree = view
                .origins
                .iter()
                .map(|o| {
                    o.map_sha256
                        .as_ref()
                        .map(|m| Some(m) == view.map_sha256.as_ref())
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

    fn sampled(origins: &[Option<&str>]) -> View {
        View {
            sampled_at: 1,
            public_height: Some(7),
            canonical: Some(true),
            map_sha256: Some("aa".repeat(32)),
            map_after: Some("aa".repeat(32)),
            previous_map: Some("bb".repeat(32)),
            origins: origins
                .iter()
                .enumerate()
                .map(|(i, map)| Origin {
                    origin: format!("https://o{i}.example"),
                    observed_at: 1,
                    http_status: Some(200),
                    map_sha256: map.map(|m| m.repeat(32)),
                    failure: map.is_none().then_some("HTTP failure"),
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn findings_separate_races_lag_failures_and_noncanonical_anchors() {
        assert_eq!(sampled(&[Some("aa"), Some("aa")]).finding(), None);
        assert_eq!(
            sampled(&[Some("aa"), Some("bb")]).finding(),
            Some(Finding::LaggingOrigin)
        );
        assert_eq!(
            sampled(&[Some("aa"), Some("cc")]).finding(),
            Some(Finding::Mismatch)
        );
        assert_eq!(
            sampled(&[Some("aa"), None]).finding(),
            Some(Finding::FailedObservation)
        );
        let mut race = sampled(&[Some("aa"), Some("cc")]);
        race.map_after = Some("cc".repeat(32));
        assert_eq!(race.finding(), Some(Finding::ObservationRace));
        let mut noncanonical = sampled(&[Some("aa"), Some("aa")]);
        noncanonical.canonical = Some(false);
        assert_eq!(noncanonical.finding(), Some(Finding::NoncanonicalAnchor));
        let evidence = sampled(&[Some("aa"), None]).evidence();
        assert!(evidence.starts_with("failed observation; expected map "));
        assert!(evidence.contains("https://o1.example at 1: HTTP 200, HTTP failure"));
    }

    #[test]
    fn origin_identity_keeps_only_scheme_host_and_port() {
        assert_eq!(
            identity("https://user:secret@pir.example:8443/v2/path?token=x#f", 0),
            "https://pir.example:8443"
        );
        assert_eq!(identity("https://pir.example/", 0), "https://pir.example");
        assert_eq!(identity("not a url", 1), "origin 2");
    }

    /// A coordinator whose map advances on its second read, one origin on the
    /// first map and one refusing with a secret-bearing body.
    #[tokio::test]
    async fn real_http_check_records_bounded_sanitized_per_origin_evidence() {
        use axum::{routing::get, Router};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let first = "aa".repeat(32);
        let second = "cc".repeat(32);
        let reads = Arc::new(AtomicUsize::new(0));
        let advance = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let app = {
            let origin_map = first.clone();
            let (first, second, reads, advance) = (
                first.clone(),
                second.clone(),
                reads.clone(),
                advance.clone(),
            );
            Router::new()
                .route(
                    "/v1/status",
                    get(move || {
                        let n = reads.fetch_add(1, Ordering::SeqCst);
                        let map = if n % 2 == 1 && advance.load(Ordering::SeqCst) {
                            second.clone()
                        } else {
                            first.clone()
                        };
                        async move {
                            axum::Json(serde_json::json!({
                                "public_height": 7, "public_hash": "dd".repeat(32), "map_sha256": map,
                            }))
                        }
                    }),
                )
                .route(
                    "/a/v2/shards/init",
                    get(move || {
                        let map = origin_map.clone();
                        async move { axum::Json(serde_json::json!({ "map_sha256": map })) }
                    }),
                )
                .route(
                    "/b/v2/shards/init",
                    get(|| async {
                        (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            "SECRET-BODY wallet request token=abc",
                        )
                    }),
                )
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let service: Service = serde_json::from_value(serde_json::json!({
            "service": "transparent", "kind": "history", "path_prefix": "/v2",
            "status_url": format!("{base}/v1/status"),
            "public_origins": [format!("{base}/a"), format!("{base}/b")],
        }))
        .unwrap();
        let client = reqwest::Client::new();

        let raced = check(&client, &service).await;
        assert_eq!(raced.map_sha256.as_deref(), Some(first.as_str()));
        assert_eq!(raced.map_after.as_deref(), Some(second.as_str()));
        assert_eq!(raced.origins_agree, None, "advancement is not a mismatch");
        assert_eq!(raced.finding(), Some(Finding::ObservationRace));
        assert_eq!(raced.anchor_hash, Some("dd".repeat(32)));

        advance.store(false, Ordering::SeqCst);
        let failed = check(&client, &service).await;
        assert_eq!(failed.finding(), Some(Finding::FailedObservation));
        assert_eq!(failed.origins.len(), 2);
        assert_eq!(
            failed.origins[0].map_sha256.as_deref(),
            Some(first.as_str())
        );
        assert_eq!(failed.origins[1].http_status, Some(503));
        assert_eq!(failed.origins[1].failure, Some("HTTP failure"));
        let evidence = failed.evidence();
        let serialized = serde_json::to_string(&failed).unwrap();
        for text in [&evidence, &serialized] {
            assert!(!text.contains("SECRET"), "{text}");
            assert!(!text.contains("token"), "{text}");
            assert!(!text.contains("/b/v2"), "paths are not recorded: {text}");
        }
        assert!(evidence.contains("HTTP 503, HTTP failure"), "{evidence}");
        assert!(evidence.len() < 1024, "evidence is bounded");
    }
}
