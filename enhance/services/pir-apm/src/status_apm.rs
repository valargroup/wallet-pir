//! Independent scrape and presentation of live Status roles.
use crate::dashboard::SharedDashboard;
use serde_json::Value;
use std::{
    collections::VecDeque,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Default, Debug)]
pub struct View {
    pub configured: bool,
    pub topology: String,
    pub sample: Option<Value>,
    pub success: Option<SystemTime>,
    pub error: Option<String>,
    pub history: VecDeque<(SystemTime, Value)>,
}

impl View {
    pub fn fresh(&self) -> bool {
        self.error.is_none()
            && self
                .success
                .is_some_and(|s| s.elapsed().is_ok_and(|d| d.as_secs() <= 45))
    }
}

/// Five-minute HTTP completion deltas from one process incarnation. All response
/// classes contribute latency; only HTTP 5xx contributes server errors.
#[derive(Debug)]
pub struct AlertWindow {
    pub completed: u64,
    pub errors: u64,
    pub p99_seconds: Option<f64>,
}
#[derive(Clone)]
struct HttpCounters {
    instance: String,
    successes: u64,
    failures: u64,
    errors: u64,
    buckets: [u64; 12],
}
impl HttpCounters {
    fn read(value: &Value, endpoint: &str) -> Option<Self> {
        let instance = value["http_instances"][endpoint].as_str()?.to_owned();
        if instance.is_empty() {
            return None;
        }
        let op = &value["http_operations"][endpoint];
        let successes = op["successes"].as_u64()?;
        let failures = op["failures"].as_u64()?;
        let errors = op["server_errors"].as_u64()?;
        let buckets: Vec<u64> = op["buckets"]
            .as_array()?
            .iter()
            .map(Value::as_u64)
            .collect::<Option<_>>()?;
        let buckets: [u64; 12] = buckets.try_into().ok()?;
        let completed = successes.checked_add(failures)?;
        if errors > failures
            || op["arrivals"].as_u64()? < completed
            || buckets
                .iter()
                .try_fold(0u64, |sum, n| sum.checked_add(*n))?
                != completed
        {
            return None;
        }
        Some(Self {
            instance,
            successes,
            failures,
            errors,
            buckets,
        })
    }
    fn follows(&self, old: &Self) -> bool {
        self.instance == old.instance
            && self.successes >= old.successes
            && self.failures >= old.failures
            && self.errors >= old.errors
            && self.errors - old.errors <= self.failures - old.failures
            && self
                .buckets
                .iter()
                .zip(old.buckets)
                .all(|(new, old)| *new >= old)
    }
}
impl View {
    pub fn alert_window(&self, endpoint: &str, now: u64) -> Option<AlertWindow> {
        if self.error.is_some() {
            return None;
        }
        let (end_time, end) = self.history.back()?;
        if self.success != Some(*end_time) {
            return None;
        }
        let at = end_time.duration_since(UNIX_EPOCH).ok()?.as_secs();
        if at > now || now - at > 45 {
            return None;
        }
        let end = HttpCounters::read(end, endpoint)?;
        let mut start = end.clone();
        let mut previous_time = *end_time;
        let mut intervals = 0;
        for (time, value) in self.history.iter().rev().skip(1) {
            let age = end_time.duration_since(*time).ok()?.as_secs();
            let gap = previous_time.duration_since(*time).ok()?.as_secs();
            if age > 300 || gap > 45 || *time == previous_time {
                break;
            }
            let Some(old) = HttpCounters::read(value, endpoint) else {
                break;
            };
            if !start.follows(&old) {
                break;
            }
            start = old;
            previous_time = *time;
            intervals += 1;
        }
        if intervals == 0 {
            return None;
        }
        let completed =
            (end.successes - start.successes).checked_add(end.failures - start.failures)?;
        let errors = end.errors - start.errors;
        if errors > end.failures - start.failures {
            return None;
        }
        let target = (completed as f64 * 0.99).ceil() as u64;
        let mut cumulative = 0;
        let mut p99_seconds = None;
        if completed > 0 {
            for (i, (new, old)) in end.buckets.iter().zip(start.buckets).enumerate() {
                cumulative += new - old;
                if cumulative >= target {
                    p99_seconds = Some(LIMITS_MS[i] / 1000.);
                    break;
                }
            }
        }
        Some(AlertWindow {
            completed,
            errors,
            p99_seconds,
        })
    }
}

fn scalar<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    path.iter().try_fold(value, |node, key| {
        if node.is_array() {
            node.get(key.parse::<usize>().ok()?)
        } else {
            node.get(*key)
        }
    })
}
fn number(value: &Value, path: &[&str]) -> Option<f64> {
    scalar(value, path)?.as_f64()
}
fn display(value: &Value, path: &[&str]) -> String {
    number(value, path)
        .map(|n| {
            if n.fract() == 0. {
                format!("{n:.0}")
            } else {
                format!("{n:.1}")
            }
        })
        .unwrap_or_else(|| "—".into())
}
fn esc(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

const LIMITS_MS: [f64; 12] = [
    5.,
    10.,
    25.,
    50.,
    75.,
    100.,
    200.,
    500.,
    1000.,
    2000.,
    5000.,
    f64::INFINITY,
];
fn delta(newer: &Value, older: &Value, path: &[&str]) -> Option<f64> {
    let a = number(newer, path)?;
    // An operation is omitted until its first request. Its prior counters are zero.
    let b = number(older, path).unwrap_or(0.);
    (a >= b).then_some(a - b)
}
fn duration(first: SystemTime, last: SystemTime) -> Option<f64> {
    last.duration_since(first)
        .ok()
        .map(|d| d.as_secs_f64())
        .filter(|d| *d > 0.)
}
fn quantiles(view: &View, key: &str) -> Option<[f64; 3]> {
    let (end_time, end) = view.history.back()?;
    let (_, start) = view.history.iter().find(|(t, _)| {
        end_time
            .duration_since(*t)
            .is_ok_and(|d| d.as_secs() <= 300)
    })?;
    interval_quantiles(start, end, key)
}
fn interval_quantiles(start: &Value, end: &Value, key: &str) -> Option<[f64; 3]> {
    let mut counts = [0f64; 12];
    for (i, count) in counts.iter_mut().enumerate() {
        let key_i = i.to_string();
        *count = delta(end, start, &["operations", key, "buckets", &key_i])?;
    }
    let total: f64 = counts.iter().sum();
    if total == 0. {
        return None;
    }
    let mut result = [0.; 3];
    for (j, p) in [0.5, 0.9, 0.99].iter().enumerate() {
        let mut cumulative = 0.;
        for (i, count) in counts.iter().enumerate() {
            cumulative += count;
            if cumulative >= total * p {
                result[j] = LIMITS_MS[i];
                break;
            }
        }
    }
    Some(result)
}
fn rate(view: &View, key: &str, field: &str) -> Option<f64> {
    let (first_t, first) = view.history.get(view.history.len().saturating_sub(3))?;
    let (last_t, last) = view.history.back()?;
    Some(delta(last, first, &["operations", key, field])? / duration(*first_t, *last_t)?)
}
fn graph(view: &View) -> String {
    let mut out = String::from("<div class=\"mini-charts\">");
    for (key, label) in [
        ("router_query", "Router admitted processing"),
        ("worker_evaluate", "Worker admitted processing"),
        ("router_pack", "Router packing"),
    ] {
        for latency in [false, true] {
            let mut points = Vec::new();
            for i in (12..view.history.len()).step_by(12) {
                let (start_time, start) = &view.history[i - 12];
                let (end_time, end) = &view.history[i];
                let value = if latency {
                    interval_quantiles(start, end, key)
                        .map(|p| p[2])
                        .filter(|v| v.is_finite())
                } else {
                    delta(end, start, &["operations", key, "arrivals"])
                        .and_then(|n| Some(n / duration(*start_time, *end_time)?))
                };
                points.push((*end_time, value));
            }
            let title = format!(
                "{label} {} · last hour",
                if latency {
                    "p99 upper bound"
                } else {
                    "completions/s"
                }
            );
            out.push_str(&format!("<figure><figcaption>{title}</figcaption><svg viewBox=\"0 0 580 145\" role=\"img\" aria-label=\"{title}\">"));
            let max = points.iter().filter_map(|(_, v)| *v).fold(1f64, f64::max);
            let mut path = String::new();
            for (i, (_, value)) in points.iter().enumerate() {
                let x = 10. + i as f64 * 560. / points.len().max(1) as f64;
                if let Some(value) = value {
                    let y = 135. - value / max * 125.;
                    path.push_str(&format!(
                        "{} {x:.1} {y:.1} ",
                        if i == 0 || points[i - 1].1.is_none() {
                            "M"
                        } else {
                            "L"
                        }
                    ));
                }
            }
            out.push_str(&format!(
                "<path fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" d=\"{path}\"/>"
            ));
            for (i, (time, value)) in points.iter().enumerate() {
                if let Some(value) = value {
                    let x = 10. + i as f64 * 560. / points.len().max(1) as f64;
                    let y = 135. - value / max * 125.;
                    let timestamp: chrono::DateTime<chrono::Utc> = (*time).into();
                    let detail = if latency {
                        format!("{value:.1} ms")
                    } else {
                        format!("{value:.1} req/s")
                    };
                    out.push_str(&format!("<circle class=\"chart-hit\" cx=\"{x:.1}\" cy=\"{y:.1}\" r=\"4\" tabindex=\"0\" data-time=\"{}\" data-p50=\"{detail}\" data-p90=\"{detail}\" data-p99=\"{detail}\"><title>{detail}</title></circle>",timestamp.format("%Y-%m-%d %H:%M:%S UTC")));
                }
            }
            out.push_str("</svg></figure>");
        }
    }
    out.push_str("</div>");
    out
}

async fn fetch(client: &reqwest::Client, url: &str) -> Result<Value, &'static str> {
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|_| "Status scrape failed")?
        .error_for_status()
        .map_err(|_| "Status metrics unavailable")?;
    let mut raw = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| "Status scrape failed")? {
        if chunk.len() > (64 * 1024usize).saturating_sub(raw.len()) {
            return Err("Status metrics too large");
        }
        raw.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&raw).map_err(|_| "Malformed Status metrics")
}

fn merge_roles(
    mut coordinator: Value,
    router: Value,
    worker: Value,
    manifest: Option<Value>,
) -> Result<Value, &'static str> {
    for sample in [&coordinator, &router, &worker] {
        if !sample.get("operations").is_some_and(Value::is_object)
            || !sample.get("admission").is_some_and(Value::is_object)
        {
            return Err("Malformed Status role metrics");
        }
    }
    // Keep real HTTP outcomes separate from admitted-work timing. Never derive
    // error rates from admission completions or count both router and worker.
    let mut http_operations = serde_json::Map::new();
    let mut http_instances = serde_json::Map::new();
    for (endpoint, source, operation) in [
        ("init", &coordinator, "init"),
        ("query", &router, "router_query"),
    ] {
        if source["http_observation_version"].as_u64() == Some(1)
            && source["http_instance"]
                .as_str()
                .is_some_and(|s| !s.is_empty())
        {
            http_operations.insert(endpoint.into(), source["operations"][operation].clone());
            http_instances.insert(endpoint.into(), source["http_instance"].clone());
        }
    }
    coordinator["http_operations"] = Value::Object(http_operations);
    coordinator["http_instances"] = Value::Object(http_instances);
    let mut operations = serde_json::Map::new();
    let mut admission = serde_json::Map::new();
    let mut resources = serde_json::Map::new();
    for (role, sample, key) in [
        ("coordinator", &coordinator, "query"),
        ("router", &router, "router_query"),
        ("worker", &worker, "worker_evaluate"),
    ] {
        resources.insert(role.into(), sample["resources"].clone());
        let counters = &sample["admission"][role];
        if let Some(buckets) = counters["execution_buckets"].as_array() {
            if buckets.len() != 12 || buckets.iter().any(|v| !v.is_u64()) {
                return Err("Malformed admission histogram");
            }
            let count: u64 = buckets.iter().filter_map(Value::as_u64).sum();
            operations.insert(
                key.into(),
                serde_json::json!({"arrivals":count,"buckets":buckets}),
            );
        }
        if counters.is_object() {
            admission.insert(role.into(), counters.clone());
        }
    }
    if router["operations"]["router_pack"].is_object() {
        operations.insert(
            "router_pack".into(),
            router["operations"]["router_pack"].clone(),
        );
    }
    coordinator["operations"] = Value::Object(operations);
    coordinator["admission"] = Value::Object(admission);
    coordinator["role_resources"] = Value::Object(resources);
    coordinator["live_roles"] = Value::Bool(true);
    coordinator["publication_available"] = Value::Bool(manifest.is_some());
    if let Some(manifest) = manifest {
        for key in [
            "generation",
            "recovery_epoch",
            "coverage_start",
            "anchor_height",
            "observed_ms",
        ] {
            coordinator[key] = manifest[key].clone();
        }
    }
    Ok(coordinator)
}

pub async fn monitor(
    url: String,
    router_url: Option<String>,
    worker_url: Option<String>,
    topology: String,
    dashboard: SharedDashboard,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("Status metrics client");
    {
        let mut view = dashboard.write().await;
        view.status.configured = true;
        view.status.topology = topology;
        view.status.error = Some("waiting for first Status sample".into());
    }
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let result = async {
            let body = if let (Some(router_url), Some(worker_url)) = (&router_url, &worker_url) {
                let (coordinator, router, worker) = tokio::join!(
                    fetch(&client, &url),
                    fetch(&client, router_url),
                    fetch(&client, worker_url)
                );
                let manifest_url = url.replace("/internal/status-apm", "/v1/status/init");
                let manifest = fetch(&client, &manifest_url).await.ok().filter(|v| {
                    [
                        "generation",
                        "recovery_epoch",
                        "observed_ms",
                        "anchor_height",
                    ]
                    .iter()
                    .all(|k| v[*k].is_u64())
                });
                merge_roles(coordinator?, router?, worker?, manifest)?
            } else {
                fetch(&client, &url).await?
            };
            if !body.get("operations").is_some_and(Value::is_object) {
                return Err("Malformed Status metrics");
            }
            Ok::<_, &str>(body)
        }
        .await;
        let mut data = dashboard.write().await;
        match result {
            Ok(sample) => {
                let now = SystemTime::now();
                if data.status.sample.as_ref().is_some_and(|previous| {
                    [
                        "init",
                        "public_material",
                        "query",
                        "router_query",
                        "worker_evaluate",
                        "router_pack",
                    ]
                    .iter()
                    .any(|operation| {
                        let path = ["operations", *operation, "arrivals"];
                        number(&sample, &path).unwrap_or(0.) < number(previous, &path).unwrap_or(0.)
                    })
                }) {
                    data.status.history.clear();
                }
                data.status.history.push_back((now, sample.clone()));
                while data.status.history.len() > 720 {
                    data.status.history.pop_front();
                }
                data.status.sample = Some(sample);
                data.status.success = Some(now);
                data.status.error = None;
            }
            Err(error) => data.status.error = Some(error.into()),
        }
    }
}

pub fn pane(view: &View) -> String {
    if !view.configured {
        return "<section class=\"card\"><h2>Status APM</h2><p class=\"notice\">Status monitoring not configured.</p></section>".into();
    }
    let Some(sample) = &view.sample else {
        return format!(
            "<section class=\"card\"><h2>Status APM</h2><p class=\"notice\">{}</p></section>",
            esc(view.error.as_deref().unwrap_or("Awaiting first sample"))
        );
    };
    let observed = number(sample, &["observed_ms"]);
    let age = observed.and_then(|ms| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| (d.as_millis() as f64 - ms) / 1000.)
    });
    let source = if view.fresh() {
        "Reachable"
    } else {
        "Unavailable or stale"
    };
    let mut html = format!("<section class=\"card\"><h2 class=\"section-title\">Status APM</h2><p class=\"intro\">Live Status roles · {}. {}. Separate coordinator, router and worker processes. Public Status remains disabled pending qualification.</p>",source,esc(&view.topology));
    if let Some(error) = &view.error {
        html.push_str(&format!(
            "<p class=\"notice\">{}; showing the last successful sample.</p>",
            esc(error)
        ));
    }
    if age.is_some_and(|seconds| !(0. ..=20.).contains(&seconds)) {
        html.push_str("<p class=\"notice\">Status source observation is older than the twenty-second freshness gate.</p>");
    }
    html.push_str("<div class=\"wrap\"><table><thead><tr><th>Operation</th><th>Completions/s</th><th>Operation failures</th><th>p50</th><th>p90</th><th>p99</th><th>Upload/s</th><th>Download/s</th></tr></thead><tbody>");
    for (key, label) in [
        ("router_query", "Router admitted processing"),
        ("worker_evaluate", "Worker admitted processing"),
        ("router_pack", "Router packing"),
    ] {
        let percentiles = quantiles(view, key);
        let f = |n: Option<f64>| {
            n.map(|v| {
                if v.is_infinite() {
                    ">5000".into()
                } else {
                    format!("{v:.1}")
                }
            })
            .unwrap_or_else(|| "—".into())
        };
        let transfer = |field| {
            if key == "router_pack" {
                None
            } else {
                rate(view, key, field)
            }
        };
        let row = format!("<tr><th>{label}</th><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",f(rate(view,key,"arrivals")),display(sample,&["operations",key,"failures"]),f(percentiles.map(|p|p[0])),f(percentiles.map(|p|p[1])),f(percentiles.map(|p|p[2])),f(transfer("upload_bytes")),f(transfer("download_bytes")));
        html.push_str(&row);
    }
    html.push_str("</tbody></table></div><p class=\"intro\">Latency values are histogram upper bounds in milliseconds over the last five minutes. Router admitted processing includes worker transport and packing. Admission wait, request upload before admission, and client preparation/decoding are excluded. These are server measurements, not end-to-end client p99. Completions include unsuccessful admitted work; unavailable operation failure counts show a dash. Stage percentiles cannot be added.</p>");
    if sample["publication_available"] == false {
        html.push_str("<p class=\"notice\">Live publication is unavailable; monitoring reachability does not imply serving readiness.</p>");
    }
    html.push_str("<h3>Admission and rejection counters</h3><div class=\"wrap\"><table><tr><th>Role</th><th>Active</th><th>Waiting</th><th>Rejections / refused checks since start</th></tr>");
    for role in ["coordinator", "router", "worker"] {
        let reasons = sample["admission"][role]["rejections"]
            .as_object()
            .map(|r| {
                r.iter()
                    .map(|(k, v)| format!("{}: {}", esc(k), v.as_u64().unwrap_or(0)))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        html.push_str(&format!(
            "<tr><th>{role}</th><td>{}</td><td>{}</td><td>{}</td></tr>",
            display(sample, &["admission", role, "active"]),
            display(sample, &["admission", role, "waiting"]),
            if reasons.is_empty() {
                "None recorded"
            } else {
                &reasons
            }
        ));
    }
    html.push_str("</table></div>");
    html.push_str(&graph(view));
    html.push_str("<div class=\"grid\">");
    let cards: &[(&str, &[(&str, &str)])] = &[(
        "Publication",
        &[
            ("generation", "Generation"),
            ("recovery_epoch", "Recovery epoch"),
            ("coverage_start", "Coverage start"),
            ("anchor_height", "Anchor height"),
            ("observed_ms", "Observed at · Unix ms"),
        ],
    )];
    for (title, fields) in cards {
        html.push_str(&format!(
            "<section class=\"card\"><h3>{title}</h3><ul class=\"rows\">"
        ));
        for (key, label) in *fields {
            let path: Vec<_> = key.split('.').collect();
            html.push_str(&format!(
                "<li><span class=\"k\">{label}</span><span class=\"v\">{}</span></li>",
                display(sample, &path)
            ));
        }
        html.push_str("</ul></section>");
    }
    html.push_str("</div>");
    for role in ["coordinator", "router", "worker"] {
        html.push_str(&format!("<section class=\"card\"><h3>{role} resources</h3><p>Process RSS: {} bytes · host available memory: {} bytes · GPU utilization: {}%</p></section>", display(sample,&["role_resources",role,"process_rss_bytes"]), display(sample,&["role_resources",role,"host_memory_available_bytes"]), display(sample,&["role_resources",role,"gpu_utilization_percent"])));
    }
    html.push_str("<p class=\"intro\">Live publication is read from the coordinator manifest. Router and worker share the P4000 host; their host memory and GPU readings overlap and must not be summed. No client load-test percentile is inferred from these server histograms.</p></section>");
    html
}

pub fn topology(view: &View) -> String {
    if !view.configured {
        return String::new();
    }
    format!("<div class=\"trunk\" style=\"margin:auto;border-style:dashed\"></div><p class=\"summary\">Live authenticated coordinator connection</p><a class=\"node-link coord-link\" href=\"/apm/status/\"><strong>Status PIR</strong><span>{}</span><span class=\"{}\">{}</span></a>",esc(&view.topology),if view.fresh(){"ok"}else{"bad"},if view.fresh(){"Status monitoring reachable"}else{"Status monitoring unavailable"})
}

#[cfg(test)]
mod tests {
    use super::*;
    fn http_sample(instance: &str, n: u64, errors: u64, bucket: usize) -> Value {
        let mut buckets = [0u64; 12];
        buckets[bucket] = n;
        serde_json::json!({"http_instances":{"query":instance},"http_operations":{"query":{"arrivals":n,"successes":n-errors,"failures":errors,"server_errors":errors,"buckets":buckets}}})
    }
    fn append(view: &mut View, at: u64, value: Value) {
        let time = UNIX_EPOCH + Duration::from_secs(at);
        view.success = Some(time);
        view.history.push_back((time, value));
    }
    #[test]
    fn http_windows_validate_idle_overflow_restart_gaps_and_malformed_data() {
        let mut v = View::default();
        append(&mut v, 100, http_sample("a", 0, 0, 0));
        assert!(v.alert_window("query", 100).is_none());
        append(&mut v, 105, http_sample("a", 0, 0, 0));
        assert_eq!(v.alert_window("query", 105).unwrap().completed, 0);
        append(&mut v, 110, http_sample("a", 100, 10, 11));
        let w = v.alert_window("query", 110).unwrap();
        assert_eq!((w.completed, w.errors), (100, 10));
        assert!(w.p99_seconds.unwrap().is_infinite());
        assert!(v.alert_window("query", 156).is_none());
        v.error = Some("scrape failed".into());
        assert!(v.alert_window("query", 110).is_none());
        v.error = None;
        // A process restart can surpass previous totals before its first scrape.
        append(&mut v, 115, http_sample("b", 200, 20, 11));
        assert!(v.alert_window("query", 115).is_none());
        append(&mut v, 120, http_sample("b", 220, 20, 11));
        assert_eq!(v.alert_window("query", 120).unwrap().errors, 0);
        append(&mut v, 170, http_sample("b", 240, 20, 11));
        assert!(v.alert_window("query", 170).is_none());
        append(&mut v, 175, http_sample("b", 0, 0, 0));
        assert!(v.alert_window("query", 175).is_none());
        append(&mut v, 180, http_sample("b", 20, 0, 2));
        assert_eq!(v.alert_window("query", 180).unwrap().completed, 20);
        for path in ["server_errors", "successes", "buckets"] {
            let mut bad = v.clone();
            bad.history.back_mut().unwrap().1["http_operations"]["query"]
                .as_object_mut()
                .unwrap()
                .remove(path);
            assert!(bad.alert_window("query", 180).is_none());
        }
        let mut bad = v.clone();
        bad.history.back_mut().unwrap().1["http_operations"]["query"]["buckets"][2] = 21.into();
        assert!(bad.alert_window("query", 180).is_none());
    }
    #[test]
    fn merge_keeps_http_errors_separate_from_admission_and_old_roles_unknown() {
        let source = |key: &str| serde_json::json!({"operations":{key:{"arrivals":20,"successes":18,"failures":2,"server_errors":1,"buckets":[20,0,0,0,0,0,0,0,0,0,0,0]}},"admission":{},"http_observation_version":1,"http_instance":key});
        let merged = merge_roles(
            source("init"),
            source("router_query"),
            source("worker_evaluate"),
            None,
        )
        .unwrap();
        assert_eq!(merged["http_operations"]["query"]["server_errors"], 1);
        assert_eq!(merged["http_operations"]["init"]["successes"], 18);
        assert_eq!(merged["http_instances"]["query"], "router_query");
        let mut old = source("router_query");
        old.as_object_mut()
            .unwrap()
            .remove("http_observation_version");
        let merged = merge_roles(source("init"), old, source("worker_evaluate"), None).unwrap();
        assert!(merged["http_operations"]["query"].is_null());
    }
    #[test]
    fn live_roles_preserve_latency_boundaries_and_readiness() {
        let role = |name: &str, bucket: usize| {
            let mut buckets = vec![0; 12];
            buckets[bucket] = 100;
            serde_json::json!({"operations":{}, "admission":{name:{"execution_buckets":buckets,"active":0,"waiting":0,"rejections":{}}},"resources":{"process_rss_bytes":42}})
        };
        let merged = merge_roles(
            role("coordinator", 8),
            role("router", 5),
            role("worker", 2),
            Some(serde_json::json!({"generation":123,"observed_ms":1})),
        )
        .unwrap();
        assert_eq!(merged["generation"], 123);
        assert_eq!(merged["operations"]["query"]["buckets"][8], 100);
        assert_eq!(merged["operations"]["router_query"]["buckets"][5], 100);
        assert!(merged["operations"]["query"]["failures"].is_null());
        let view = View {
            configured: true,
            topology: "Enhance host → P4000 router + worker".into(),
            sample: Some(merged),
            success: Some(SystemTime::now()),
            ..Default::default()
        };
        let html = pane(&view);
        assert!(html.contains("Enhance host → P4000 router + worker"));
        assert!(html.contains("not end-to-end client p99"));
        assert!(html.contains("twenty-second freshness gate"));
        assert!(!html.contains("Synthetic"));
        let missing = merge_roles(
            role("coordinator", 8),
            role("router", 5),
            role("worker", 2),
            None,
        )
        .unwrap();
        assert_eq!(missing["publication_available"], false);
        assert!(merge_roles(
            role("coordinator", 8),
            serde_json::json!({}),
            role("worker", 2),
            None
        )
        .is_err());
    }
    #[test]
    fn overflow_latency_is_not_reported_as_ten_seconds() {
        let empty = serde_json::json!({"operations":{}});
        let full =
            serde_json::json!({"operations":{"query":{"buckets":[0,0,0,0,0,0,0,0,0,0,0,1]}}});
        assert!(interval_quantiles(&empty, &full, "query").unwrap()[2].is_infinite());
    }
    #[test]
    fn topology_uses_a_service_name_and_placement_description() {
        let view = View {
            configured: true,
            topology: "Enhance host → P4000 router + worker".into(),
            success: Some(SystemTime::now()),
            ..Default::default()
        };
        let html = topology(&view);
        assert!(html.contains("<strong>Status PIR</strong>"));
        assert!(html.contains("Enhance host → P4000 router + worker"));
        assert!(!html.contains("enhance-coordinator-and-p4000"));
    }
    #[test]
    fn first_operation_after_idle_has_latency_and_rate() {
        let now = SystemTime::now();
        let mut view = View::default();
        view.history.push_back((
            now - Duration::from_secs(10),
            serde_json::json!({"operations":{}}),
        ));
        view.history.push_back((now,serde_json::json!({"operations":{"query":{"arrivals":40,"buckets":[0,0,0,40,0,0,0,0,0,0,0,0]}}})));
        assert_eq!(quantiles(&view, "query"), Some([50., 50., 50.]));
        assert_eq!(rate(&view, "query", "arrivals"), Some(4.));
    }
    #[test]
    fn outage_preserves_sample_and_marks_it_stale() {
        let mut view = View {
            configured: true,
            topology: "Enhance host → P4000 router + worker".into(),
            sample: Some(serde_json::json!({"operations":{},"generation":1,"observed_ms":0})),
            success: Some(SystemTime::now()),
            ..Default::default()
        };
        view.error = Some("scrape failed".into());
        assert!(!view.fresh());
        assert!(pane(&view).contains("last successful sample"));
        assert!(!pane(&view).contains("http://"));
    }
}
