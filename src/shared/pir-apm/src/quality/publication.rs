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
    pub node_hash: Option<String>,
    pub withdrawn: Option<bool>,
    pub revision: Option<u64>,
    /// Whether two complete operator observations named the same authority.
    pub consistent: Option<bool>,
    /// The coordinator map read again after the origins.
    pub map_after: Option<String>,
    /// The map published before the current one, set by the sampling loop,
    /// to recognise an origin still serving it.
    pub previous_map: Option<String>,
    /// When the sampling loop first saw the current map, which bounds how long
    /// an origin serving `previous_map` counts as propagation.
    pub map_seen_at: Option<u64>,
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
    /// The retained publication was deliberately fenced from serving.
    Withdrawn,
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
            Self::Withdrawn => "publication withdrawn; recovery in progress",
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
        if self.consistent == Some(false) {
            return Some(Finding::ObservationRace);
        }
        if self.consistent.is_none() {
            return Some(Finding::FailedObservation);
        }
        if self.canonical == Some(false) {
            return Some(Finding::NoncanonicalAnchor);
        }
        if self.withdrawn == Some(true) {
            return Some(Finding::Withdrawn);
        }
        if self.origins.is_empty() || self.map_sha256.is_none() {
            return None;
        }
        if self.map_after.is_some() && self.map_after != self.map_sha256 {
            return Some(Finding::ObservationRace);
        }
        if self.map_after.is_none() {
            return Some(Finding::FailedObservation);
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
            "{}; sample {}; expected map {} at anchor {} {}; node {}; withdrawn {:?}; revision {:?}; consistent {:?}; coordinator after {}",
            self.finding().map_or("origins agree", Finding::text),
            self.sampled_at,
            map(&self.map_sha256),
            self.public_height
                .map_or("unknown".into(), |h| h.to_string()),
            map(&self.anchor_hash),
            map(&self.node_hash),
            self.withdrawn,
            self.revision,
            self.consistent,
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
/// Only complete observations can establish stability. A failed read is not a
/// publication change, and a retained anchor alone does not prove it was served.
#[derive(Clone, PartialEq, Eq)]
struct Observation {
    height: u64,
    hash: String,
    map: String,
    revision: u64,
    withdrawn: bool,
}
impl Observation {
    fn parse(status: &Value) -> Option<Self> {
        let value = &status["publication_observation"];
        Some(Self {
            height: value["height"].as_u64()?,
            hash: hex64(&value["hash"])?,
            map: hex64(&value["map_sha256"])?,
            revision: value["revision"].as_u64()?,
            withdrawn: value["withdrawn"].as_bool()?,
        })
    }
}
impl View {
    fn conclude(&mut self, before: Option<Observation>, after: Option<Observation>) {
        self.canonical = None;
        self.origins_agree = None;
        self.map_after = after.as_ref().map(|o| o.map.clone());
        self.consistent = before.zip(after).map(|(a, b)| a == b);
        if self.consistent != Some(true) {
            return;
        }
        let matching = |o: &Origin| o.map_sha256.is_some() && o.map_sha256 == self.map_sha256;
        let any_match = self.origins.iter().any(matching);
        let all_match = !self.origins.is_empty() && self.origins.iter().all(matching);
        // A successful public response confirms invalid serving even when the
        // publisher says it withdrew. A refusal or transport error proves neither.
        self.canonical = match (&self.anchor_hash, &self.node_hash) {
            (Some(anchor), Some(node)) if anchor != node && any_match => Some(false),
            (Some(anchor), Some(node))
                if anchor == node && all_match && self.withdrawn == Some(false) =>
            {
                Some(true)
            }
            _ => None,
        };
        if self.withdrawn == Some(false) && !self.origins.is_empty() {
            self.origins_agree = self
                .origins
                .iter()
                .map(|o| {
                    o.map_sha256
                        .as_ref()
                        .map(|map| Some(map) == self.map_sha256.as_ref())
                })
                .collect::<Option<Vec<_>>>()
                .map(|matches| matches.into_iter().all(|v| v));
        }
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
    let before = Observation::parse(&first);
    if let Some(observation) = &before {
        view.public_height = Some(observation.height);
        view.anchor_hash = Some(observation.hash.clone());
        view.map_sha256 = Some(observation.map.clone());
        view.revision = Some(observation.revision);
        view.withdrawn = Some(observation.withdrawn);
    }
    if let (Some(height), Ok(rpc_url), Ok(cookie_path)) = (
        view.public_height,
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
                                view.node_hash = hex64(&v["result"]);
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
    }
    let after = json(client, &status_url)
        .await
        .and_then(|v| Observation::parse(&v));
    view.conclude(before, after);
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
            consistent: Some(true),
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
    fn only_stable_public_evidence_confirms_serving_or_recovery() {
        let mut view = sampled(&[Some("aa"), Some("aa")]);
        let observation = Observation {
            height: 7,
            hash: "dd".repeat(32),
            map: "aa".repeat(32),
            revision: 4,
            withdrawn: false,
        };
        view.anchor_hash = Some(observation.hash.clone());
        view.node_hash = Some(observation.hash.clone());
        view.withdrawn = Some(false);
        view.conclude(Some(observation.clone()), Some(observation.clone()));
        assert_eq!(view.canonical, Some(true));
        view.node_hash = Some("ee".repeat(32));
        view.conclude(Some(observation.clone()), Some(observation.clone()));
        assert_eq!(view.canonical, Some(false));

        let mut withdrawn = observation.clone();
        withdrawn.withdrawn = true;
        withdrawn.revision += 1;
        view.withdrawn = Some(true);
        view.conclude(Some(withdrawn.clone()), Some(withdrawn.clone()));
        assert_eq!(
            view.canonical,
            Some(false),
            "successful invalid serving overrides a withdrawal claim"
        );
        for origin in &mut view.origins {
            origin.http_status = Some(503);
            origin.map_sha256 = None;
        }
        view.conclude(Some(withdrawn.clone()), Some(withdrawn.clone()));
        assert_eq!(
            view.canonical, None,
            "withdrawal cannot recover an active invalid-serving incident"
        );
        assert_eq!(view.finding(), Some(Finding::Withdrawn));
        view.conclude(Some(observation.clone()), Some(withdrawn));
        assert_eq!(view.canonical, None);
        assert_eq!(view.finding(), Some(Finding::ObservationRace));
        view.conclude(Some(observation), None);
        assert_eq!(view.canonical, None);
        assert_eq!(view.finding(), Some(Finding::FailedObservation));
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
        assert!(evidence.starts_with("failed observation; sample 1; expected map "));
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
                                "publication_observation": {
                                    "height": 7, "hash": "dd".repeat(32), "map_sha256": map,
                                    "revision": 1, "withdrawn": false,
                                },
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
