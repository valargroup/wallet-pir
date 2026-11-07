use super::services::{Kind, Service};
use serde::Serialize;
use serde_json::Value;
/// How long a worker may keep serving a replaced map, counted from when the
/// sampling loop first saw its successor. Workers poll for publications,
/// skipping any replaced meanwhile, and back off 10 s, then 20 s after a
/// refused pass, so in a burst of blocks they trail the newest map.
pub const WORKER_ADOPTION_SECONDS: u64 = 45;
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
    /// When the sampling loop first saw the current map, which bounds how long
    /// an origin serving `previous_map` counts as propagation.
    pub map_seen_at: Option<u64>,
    /// Each distinct published height and when the sampling loop first saw it,
    /// oldest first; see [`View::lag_started`].
    #[serde(skip)]
    pub heights_seen: Vec<(u64, u64)>,
    /// Maps the sampling loop saw replaced within [`WORKER_ADOPTION_SECONDS`],
    /// each with when it first saw the successor; see [`View::follows`].
    #[serde(skip)]
    pub replaced: Vec<(String, u64)>,
    /// One entry per configured origin (at most two), in configuration order.
    pub origins: Vec<Origin>,
}
/// One public origin's answer. It holds only the configured origin's scheme,
/// host and port, the HTTP status, a validated map digest, the height that map
/// covers and a fixed reason: never a response body, path, query or credential.
#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
pub struct Origin {
    pub origin: String,
    pub observed_at: u64,
    pub http_status: Option<u16>,
    pub map_sha256: Option<String>,
    pub covered_through: Option<u64>,
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
    /// An origin still serves an older publication: one below the published
    /// height, or the previous map when it reports no height.
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
        Ok(value) => {
            observed.covered_through = value["covered_through"].as_u64();
            match hex64(&value["map_sha256"]) {
                Some(map) => observed.map_sha256 = Some(map),
                None => observed.failure = Some("no valid map_sha256"),
            }
        }
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
        } else if differing.iter().all(|o| self.behind(o)) {
            Some(Finding::LaggingOrigin)
        } else {
            Some(Finding::Mismatch)
        }
    }
    /// Whether `origin` serves an older publication. Its reported height decides
    /// when known: publications can land between samples, so its map may be one
    /// the sampling loop never saw.
    fn behind(&self, origin: &Origin) -> bool {
        match (origin.covered_through, self.public_height) {
            (Some(covered), Some(height)) => covered < height,
            _ => self.previous_map.is_some() && origin.map_sha256 == self.previous_map,
        }
    }
    /// When the coordinator published past the longest-lagging origin, as the
    /// sampling loop first saw it: the first height above the one that origin
    /// covers, or the current map for an origin that reports no height. `None`
    /// when any lagging origin cannot be dated.
    pub fn lag_started(&self) -> Option<u64> {
        self.origins
            .iter()
            .filter(|o| o.map_sha256 != self.map_sha256)
            .map(|o| match (o.covered_through, self.public_height) {
                (Some(covered), Some(_)) => self
                    .heights_seen
                    .iter()
                    .filter(|&&(height, _)| height > covered)
                    .map(|&(_, seen)| seen)
                    .min(),
                _ => self.map_seen_at,
            })
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .min()
    }
    /// Whether a worker serving `map` follows the publication: `map` is the
    /// current one, or was replaced less than [`WORKER_ADOPTION_SECONDS`]
    /// before `now`.
    pub fn follows(&self, map: Option<&str>, now: u64) -> bool {
        map.is_some_and(|map| {
            self.map_sha256.as_deref() == Some(map)
                || self.replaced.iter().any(|(replaced, at)| {
                    replaced == map && now.saturating_sub(*at) < WORKER_ADOPTION_SECONDS
                })
        })
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
                    (Some(map), _) => match origin.covered_through {
                        Some(height) => format!("map {map} through {height}"),
                        None => format!("map {map}"),
                    },
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
                    covered_through: None,
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

    /// Publications that land between samples leave an origin on a map the loop
    /// never saw; its height, not the map, says it is behind.
    #[test]
    fn an_origin_below_the_published_height_lags_from_when_it_was_passed() {
        let mut view = sampled(&[Some("aa"), Some("cc")]);
        view.map_seen_at = Some(30);
        view.heights_seen = vec![(5, 10), (6, 20), (7, 30)];
        assert_eq!(view.finding(), Some(Finding::Mismatch));
        view.origins[1].covered_through = Some(5);
        assert_eq!(view.finding(), Some(Finding::LaggingOrigin));
        assert_eq!(view.lag_started(), Some(20), "height 6 passed it");
        assert!(view
            .evidence()
            .contains(&format!("map {} through 5", "cc".repeat(32))));
        // A different map at the published height is a disagreement.
        view.origins[1].covered_through = Some(7);
        assert_eq!(view.finding(), Some(Finding::Mismatch));
        // Without a height, the previous map and its successor's first sighting decide.
        let mut previous = sampled(&[Some("aa"), Some("bb")]);
        previous.map_seen_at = Some(30);
        assert_eq!(previous.finding(), Some(Finding::LaggingOrigin));
        assert_eq!(previous.lag_started(), Some(30));
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
                        async move {
                            axum::Json(serde_json::json!({ "map_sha256": map, "covered_through": 7 }))
                        }
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
        assert_eq!(failed.origins[0].covered_through, Some(7));
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
