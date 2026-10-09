//! Private sampler for `pir_observability` endpoints the public edge does not expose.
//!
//! `PIR_MONITOR_METRICS_TARGETS` is a JSON list of `{service, url}` with plain-HTTP URLs on
//! private or loopback IPv4 addresses; unset, nothing is scraped. Each target is read once a
//! minute, with the body capped at 256 KiB and the whole scrape at 5 seconds. Only the last
//! hour of samples is retained, in memory. A new process or a decreased counter is a reset:
//! that sample counts the new process's totals, never a negative increase. Status classes are
//! reported as the server counts them; `4xx` includes, but is not, 429 overload.
use anyhow::Result;
use pir_apm::incidents::unix_time;
use pir_observability::BOUNDS;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    net::Ipv4Addr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::RwLock;

const INTERVAL: Duration = Duration::from_secs(60);
const TIMEOUT: Duration = Duration::from_secs(5);
/// The receiver's seven endpoints render about 20 KB.
const MAX_BYTES: usize = 256 * 1024;
const MAX_ENDPOINTS: usize = 32;
const MAX_TARGETS: usize = 8;
/// One hour of one-minute samples.
const HISTORY: usize = 60;
/// Three missed collections.
const STALE_SECONDS: u64 = 180;
const BUCKETS: usize = BOUNDS.len() + 1;
const CLASSES: [&str; 5] = ["1xx", "2xx", "3xx", "4xx", "5xx"];

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    service: String,
    url: String,
}

/// One endpoint's values: cumulative counters and buckets, plus the `inflight` gauge.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Counters {
    arrivals: u64,
    inflight: u64,
    cancelled: u64,
    body_errors: u64,
    incomplete: u64,
    responses: [u64; 5],
    buckets: [u64; BUCKETS],
}

impl Counters {
    /// Increases since `earlier`, or `None` when any counter decreased.
    fn since(&self, earlier: &Self) -> Option<Self> {
        let mut out = Self {
            inflight: self.inflight,
            arrivals: self.arrivals.checked_sub(earlier.arrivals)?,
            cancelled: self.cancelled.checked_sub(earlier.cancelled)?,
            body_errors: self.body_errors.checked_sub(earlier.body_errors)?,
            incomplete: self.incomplete.checked_sub(earlier.incomplete)?,
            ..Self::default()
        };
        for (i, value) in self.responses.iter().enumerate() {
            out.responses[i] = value.checked_sub(earlier.responses[i])?;
        }
        for (i, value) in self.buckets.iter().enumerate() {
            out.buckets[i] = value.checked_sub(earlier.buckets[i])?;
        }
        Some(out)
    }

    /// Accumulates increases. `inflight` sums too: a total across endpoints, not over time.
    fn add(&mut self, other: &Self) {
        self.arrivals += other.arrivals;
        self.inflight += other.inflight;
        self.cancelled += other.cancelled;
        self.body_errors += other.body_errors;
        self.incomplete += other.incomplete;
        for (a, b) in self.responses.iter_mut().zip(other.responses) {
            *a += b;
        }
        for (a, b) in self.buckets.iter_mut().zip(other.buckets) {
            *a += b;
        }
    }
}

/// One scrape: the process start time and each endpoint's values.
#[derive(Clone, Debug, Default, PartialEq)]
struct Reading {
    start: f64,
    endpoints: BTreeMap<String, Counters>,
}

/// Parses the `pir_http_*` series of `HttpMetrics::render`, ignoring other families.
fn parse(text: &str) -> Result<Reading, &'static str> {
    let (mut version, mut start) = (None, None);
    let mut endpoints = BTreeMap::<String, Counters>::new();
    for line in text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        let (series, value) = line.rsplit_once(' ').ok_or("malformed")?;
        let (name, labels) = match series.split_once('{') {
            Some((name, rest)) => (name, rest.strip_suffix('}').ok_or("malformed")?),
            None => (series, ""),
        };
        let Some(name) = name.strip_prefix("pir_http_") else {
            continue;
        };
        let value: f64 = value.parse().map_err(|_| "malformed")?;
        if !value.is_finite() || value < 0. {
            return Err("malformed");
        }
        let (mut endpoint, mut code, mut le) = (None, None, None);
        for pair in labels.split(',').filter(|p| !p.is_empty()) {
            let (key, quoted) = pair.split_once('=').ok_or("malformed")?;
            let label = quoted
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .ok_or("malformed")?;
            *match key {
                "endpoint" => &mut endpoint,
                "code" => &mut code,
                "le" => &mut le,
                _ => return Err("unsupported_format"),
            } = Some(label);
        }
        match (name, endpoint) {
            ("observation_version", None) => version = Some(value),
            ("process_start_time_seconds", None) => start = Some(value),
            (_, None) => {}
            (_, Some(endpoint)) => {
                if endpoint.is_empty()
                    || endpoint.len() > 32
                    || !endpoint
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b == b'_')
                {
                    return Err("malformed");
                }
                if !endpoints.contains_key(endpoint) && endpoints.len() == MAX_ENDPOINTS {
                    return Err("too_many_series");
                }
                let c = endpoints.entry(endpoint.into()).or_default();
                let count = value as u64;
                match (name, code, le) {
                    ("arrivals_total", None, None) => c.arrivals = count,
                    ("inflight", None, None) => c.inflight = count,
                    ("cancelled_total", None, None) => c.cancelled = count,
                    ("body_errors_total", None, None) => c.body_errors = count,
                    ("incomplete_responses_total", None, None) => c.incomplete = count,
                    ("responses_total", Some(code), None) => {
                        let class = CLASSES.iter().position(|c| *c == code);
                        c.responses[class.ok_or("malformed")?] = count;
                    }
                    ("duration_seconds_bucket", None, Some(le)) => {
                        let bucket = if le == "+Inf" {
                            Some(BOUNDS.len())
                        } else {
                            let bound: f64 = le.parse().map_err(|_| "malformed")?;
                            BOUNDS.iter().position(|b| *b == bound)
                        };
                        c.buckets[bucket.ok_or("unsupported_format")?] = count;
                    }
                    // Byte totals, duration count/sum and later additions.
                    _ => {}
                }
            }
        }
    }
    match (version, start) {
        (Some(1.), Some(start)) => Ok(Reading { start, endpoints }),
        _ => Err("unsupported_format"),
    }
}

/// Per-endpoint increases from `earlier` to `later`, and whether a reset intervened.
fn delta(earlier: &Reading, later: &Reading) -> (bool, BTreeMap<String, Counters>) {
    let zero = Counters::default();
    let increases = (earlier.start == later.start)
        .then(|| {
            later
                .endpoints
                .iter()
                .map(|(e, c)| {
                    Some((
                        e.clone(),
                        c.since(earlier.endpoints.get(e).unwrap_or(&zero))?,
                    ))
                })
                .collect::<Option<BTreeMap<_, _>>>()
        })
        .flatten();
    match increases {
        Some(increases) => (false, increases),
        None => (true, later.endpoints.clone()),
    }
}

/// Increases between two successful scrapes `seconds` apart, ending at `at`.
struct Sample {
    at: u64,
    seconds: u64,
    reset: bool,
    endpoints: BTreeMap<String, Counters>,
}

/// One target's collection state and retained samples.
#[derive(Default)]
pub struct State {
    last: Option<(u64, Reading)>,
    samples: VecDeque<Sample>,
    sampled_at: u64,
    succeeded_at: u64,
    successes: u64,
    failures: u64,
    consecutive_failures: u64,
    category: &'static str,
}

impl State {
    /// Records one scrape attempt at `at`, rotating out the oldest sample past `HISTORY`.
    fn record(&mut self, at: u64, result: Result<Reading, &'static str>) {
        self.sampled_at = at;
        let reading = match result {
            Ok(reading) => reading,
            Err(category) => {
                self.failures += 1;
                self.consecutive_failures += 1;
                self.category = category;
                return;
            }
        };
        self.successes += 1;
        self.consecutive_failures = 0;
        self.category = "";
        self.succeeded_at = at;
        if let Some((earlier_at, earlier)) = &self.last {
            let (reset, endpoints) = delta(earlier, &reading);
            if self.samples.len() == HISTORY {
                self.samples.pop_front();
            }
            self.samples.push_back(Sample {
                at,
                seconds: at.saturating_sub(*earlier_at),
                reset,
                endpoints,
            });
        }
        self.last = Some((at, reading));
    }

    /// The bounded `/monitor-status` view; `stale` once no scrape succeeded for `STALE_SECONDS`.
    fn summary(&self, now: u64) -> Summary {
        let mut endpoints = BTreeMap::<String, Counters>::new();
        let history = self
            .samples
            .iter()
            .map(|s| {
                let mut all = Counters::default();
                for (endpoint, c) in &s.endpoints {
                    endpoints.entry(endpoint.clone()).or_default().add(c);
                    all.add(c);
                }
                Point {
                    at: s.at,
                    seconds: s.seconds,
                    reset: s.reset,
                    totals: (&all).into(),
                }
            })
            .collect();
        let latest = self.last.as_ref().map(|(_, r)| &r.endpoints);
        Summary {
            sampled_at: self.sampled_at,
            succeeded_at: self.succeeded_at,
            stale: self.succeeded_at == 0 || now.saturating_sub(self.succeeded_at) > STALE_SECONDS,
            category: self.category.into(),
            successes: self.successes,
            failures: self.failures,
            consecutive_failures: self.consecutive_failures,
            window_seconds: self.samples.iter().map(|s| s.seconds).sum(),
            resets: self.samples.iter().filter(|s| s.reset).count() as u64,
            endpoints: endpoints
                .into_iter()
                .map(|(endpoint, mut c)| {
                    c.inflight = latest
                        .and_then(|l| l.get(&endpoint))
                        .map_or(0, |l| l.inflight);
                    (endpoint, (&c).into())
                })
                .collect(),
            history,
        }
    }
}

/// Increases over a window; `inflight` is the latest gauge, and `duration_*_le` the upper
/// bound of the bucket holding that quantile (`None` without requests).
#[derive(Clone, Debug, Default, Serialize)]
pub struct Totals {
    arrivals: u64,
    responses: BTreeMap<&'static str, u64>,
    cancelled: u64,
    body_errors: u64,
    incomplete_responses: u64,
    inflight: u64,
    duration_p50_le: Option<String>,
    duration_p99_le: Option<String>,
}

impl From<&Counters> for Totals {
    fn from(c: &Counters) -> Self {
        let count = c.buckets[BOUNDS.len()];
        let quantile = |q: f64| {
            let rank = (q * count as f64).ceil() as u64;
            let i = c.buckets.iter().position(|b| *b >= rank.max(1))?;
            Some(BOUNDS.get(i).map_or_else(|| "+Inf".into(), f64::to_string))
        };
        Self {
            arrivals: c.arrivals,
            responses: CLASSES.into_iter().zip(c.responses).collect(),
            cancelled: c.cancelled,
            body_errors: c.body_errors,
            incomplete_responses: c.incomplete,
            inflight: c.inflight,
            duration_p50_le: (count > 0).then(|| quantile(0.5)).flatten(),
            duration_p99_le: (count > 0).then(|| quantile(0.99)).flatten(),
        }
    }
}

/// One retained sample, totalled over endpoints.
#[derive(Clone, Debug, Serialize)]
pub struct Point {
    at: u64,
    seconds: u64,
    reset: bool,
    #[serde(flatten)]
    totals: Totals,
}

/// A target's collection health, per-endpoint totals over the retained window, and its samples.
/// The target URL is omitted, as `/monitor-status` is public.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Summary {
    sampled_at: u64,
    succeeded_at: u64,
    stale: bool,
    category: String,
    successes: u64,
    failures: u64,
    consecutive_failures: u64,
    window_seconds: u64,
    resets: u64,
    endpoints: BTreeMap<String, Totals>,
    history: Vec<Point>,
}

pub type Shared = Arc<RwLock<BTreeMap<String, State>>>;

/// Every target's summary at `now`.
pub fn summaries(states: &BTreeMap<String, State>, now: u64) -> BTreeMap<String, Summary> {
    states
        .iter()
        .map(|(service, state)| (service.clone(), state.summary(now)))
        .collect()
}

/// Validates `PIR_MONITOR_METRICS_TARGETS` (see the module docs).
fn targets(text: &str) -> Result<Vec<Target>> {
    let targets: Vec<Target> = serde_json::from_str(text)?;
    let mut names = BTreeSet::new();
    anyhow::ensure!(targets.len() <= MAX_TARGETS, "too many metrics targets");
    for target in &targets {
        let url = reqwest::Url::parse(&target.url)?;
        let private = url
            .host_str()
            .and_then(|h| h.parse::<Ipv4Addr>().ok())
            .is_some_and(|ip| ip.is_private() || ip.is_loopback());
        anyhow::ensure!(
            url.scheme() == "http"
                && private
                && url.username().is_empty()
                && url.password().is_none()
                && (1..=32).contains(&target.service.len())
                && target.service.bytes().all(|b| b.is_ascii_lowercase()
                    || b.is_ascii_digit()
                    || b == b'_'
                    || b == b'-')
                && names.insert(target.service.clone()),
            "invalid metrics target"
        );
    }
    Ok(targets)
}

/// One scrape; `timeout` covers connecting, headers and the body, which is capped at `MAX_BYTES`.
async fn scrape(
    client: &reqwest::Client,
    url: &str,
    timeout: Duration,
) -> Result<Reading, &'static str> {
    let body = async {
        let mut response = client.get(url).send().await.map_err(|_| "unavailable")?;
        if !response.status().is_success() {
            return Err("http_status");
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| "unavailable")? {
            if body.len() + chunk.len() > MAX_BYTES {
                return Err("oversized");
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    };
    let body = tokio::time::timeout(timeout, body)
        .await
        .map_err(|_| "deadline")??;
    parse(std::str::from_utf8(&body).map_err(|_| "malformed")?)
}

/// Starts one sampler per configured target; every target is listed, stale, from the start.
pub fn start() -> Result<Shared> {
    let Some(text) = std::env::var("PIR_MONITOR_METRICS_TARGETS")
        .ok()
        .filter(|v| !v.trim().is_empty())
    else {
        return Ok(Shared::default());
    };
    let targets = targets(&text)?;
    let shared = Shared::new(RwLock::new(
        targets
            .iter()
            .map(|t| (t.service.clone(), State::default()))
            .collect(),
    ));
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()?;
    for target in targets {
        let (shared, client) = (shared.clone(), client.clone());
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let result = scrape(&client, &target.url, TIMEOUT).await;
                if let Some(state) = shared.write().await.get_mut(&target.service) {
                    state.record(unix_time(), result);
                }
            }
        });
    }
    Ok(shared)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{http::StatusCode, routing::get, Router};

    /// Serves `app` on an ephemeral loopback port and returns its origin.
    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });
        origin
    }

    /// A rendered body with one endpoint's arrivals and 2xx responses.
    fn rendered(start: u64, count: u64) -> String {
        format!(
            "pir_http_observation_version 1\npir_http_process_start_time_seconds {start}\n\
             pir_http_arrivals_total{{endpoint=\"receiver_init\"}} {count}\n\
             pir_http_responses_total{{endpoint=\"receiver_init\",code=\"2xx\"}} {count}\n"
        )
    }

    #[tokio::test]
    async fn scrapes_the_shared_format_and_reports_window_increases() {
        let metrics = pir_observability::HttpMetrics::default();
        metrics.initialize(&["receiver_init", "receiver_query"]);
        let rendered = metrics.clone();
        let app = Router::new()
            .route("/v1/receiver/init", get(|| async { "ok" }))
            .route(
                "/v1/receiver/query",
                get(|| async { StatusCode::TOO_MANY_REQUESTS }),
            )
            .route("/metrics", get(move || async move { rendered.render() }))
            .layer(axum::middleware::from_fn_with_state(
                metrics,
                pir_observability::observe,
            ));
        let origin = serve(app).await;
        let client = reqwest::Client::new();
        let url = format!("{origin}/metrics");
        let mut state = State::default();
        state.record(100, scrape(&client, &url, TIMEOUT).await);
        for path in ["init", "init", "query"] {
            client
                .get(format!("{origin}/v1/receiver/{path}"))
                .send()
                .await
                .unwrap();
        }
        state.record(160, scrape(&client, &url, TIMEOUT).await);
        let summary = state.summary(170);
        assert!(!summary.stale && summary.category.is_empty());
        assert_eq!((summary.window_seconds, summary.resets), (60, 0));
        let init = &summary.endpoints["receiver_init"];
        assert_eq!((init.arrivals, init.responses["2xx"]), (2, 2));
        assert_eq!(init.duration_p99_le.as_deref(), Some("0.001"));
        let query = &summary.endpoints["receiver_query"];
        assert_eq!((query.arrivals, query.responses["4xx"]), (1, 1));
        assert_eq!(summary.history.len(), 1);
        assert_eq!(summary.history[0].totals.arrivals, 3);
        let json = serde_json::to_value(&summary).unwrap();
        assert_eq!(json["history"][0]["responses"]["4xx"], 1);
        assert!(!json.to_string().contains("127.0.0.1"));
    }

    #[tokio::test]
    async fn timeout_oversized_malformed_and_error_status_fail_the_scrape() {
        let app = Router::new()
            .route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    rendered(1, 1)
                }),
            )
            .route("/large", get(|| async { "#".repeat(MAX_BYTES + 1) }))
            .route(
                "/garbage",
                get(|| async { "pir_http_observation_version one" }),
            )
            .route(
                "/unversioned",
                get(|| async { "pir_http_inflight{endpoint=\"x\"} 0" }),
            )
            .route("/error", get(|| async { StatusCode::SERVICE_UNAVAILABLE }))
            .route("/ok", get(|| async { rendered(1, 1) }));
        let origin = serve(app).await;
        let client = reqwest::Client::new();
        let short = Duration::from_millis(200);
        for (path, category) in [
            ("slow", "deadline"),
            ("large", "oversized"),
            ("garbage", "malformed"),
            ("unversioned", "unsupported_format"),
            ("error", "http_status"),
        ] {
            let result = scrape(&client, &format!("{origin}/{path}"), short).await;
            assert_eq!(result, Err(category), "{path}");
        }
        assert!(scrape(&client, &format!("{origin}/ok"), short)
            .await
            .is_ok());
        let wide = (0..=MAX_ENDPOINTS as u8)
            .map(|i| {
                let name = [b'a' + i / 26, b'a' + i % 26].map(char::from);
                format!(
                    "pir_http_inflight{{endpoint=\"{}{}\"}} 0\n",
                    name[0], name[1]
                )
            })
            .collect::<String>();
        assert_eq!(parse(&wide), Err("too_many_series"));
    }

    #[test]
    fn counter_resets_count_the_new_process_and_never_go_negative() {
        let mut state = State::default();
        for (at, start, count) in [(100, 1, 10), (160, 1, 15), (220, 2, 3), (280, 2, 1)] {
            state.record(at, parse(&rendered(start, count)));
        }
        let summary = state.summary(280);
        let resets: Vec<_> = summary
            .history
            .iter()
            .map(|p| (p.reset, p.totals.arrivals))
            .collect();
        // A new start time, then a decrease without one, are both resets.
        assert_eq!(resets, [(false, 5), (true, 3), (true, 1)]);
        assert_eq!(
            (summary.resets, summary.endpoints["receiver_init"].arrivals),
            (2, 9)
        );
    }

    #[test]
    fn failed_collection_turns_stale_and_history_rotates() {
        let mut state = State::default();
        assert!(state.summary(100).stale);
        state.record(100, parse(&rendered(1, 0)));
        for at in [160, 220, 280] {
            state.record(at, Err("deadline"));
        }
        let fresh = state.summary(280);
        assert!(!fresh.stale);
        let stale = state.summary(281);
        assert!(stale.stale);
        assert_eq!(
            (
                stale.consecutive_failures,
                stale.category.as_str(),
                stale.succeeded_at
            ),
            (3, "deadline", 100)
        );
        for i in 1..=HISTORY as u64 + 5 {
            state.record(280 + i * 60, parse(&rendered(1, i)));
        }
        let summary = state.summary(280 + (HISTORY as u64 + 5) * 60);
        assert!(!summary.stale);
        assert_eq!(summary.history.len(), HISTORY);
        assert_eq!(summary.history[0].at, 280 + 6 * 60);
        assert_eq!(summary.window_seconds, HISTORY as u64 * 60);
    }

    #[test]
    fn targets_must_be_unique_private_plain_http() {
        let valid = r#"[{"service":"receiver","url":"http://10.70.0.11:18380/metrics"}]"#;
        assert_eq!(targets(valid).unwrap()[0].service, "receiver");
        for invalid in [
            r#"[{"service":"receiver","url":"http://159.203.10.20:18380/metrics"}]"#,
            r#"[{"service":"receiver","url":"https://10.70.0.11:18380/metrics"}]"#,
            r#"[{"service":"receiver","url":"http://receiver.internal/metrics"}]"#,
            r#"[{"service":"Receiver","url":"http://10.70.0.11:18380/metrics"}]"#,
            r#"[{"service":"r","url":"http://10.0.0.1/metrics"},{"service":"r","url":"http://10.0.0.2/metrics"}]"#,
            r#"[{"service":"receiver","url":"http://10.70.0.11:18380/metrics","extra":1}]"#,
        ] {
            assert!(targets(invalid).is_err(), "{invalid}");
        }
    }
}
