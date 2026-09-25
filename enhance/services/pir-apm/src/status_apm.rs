//! Independent scrape and presentation of the synthetic Status PIR service.
use crate::dashboard::SharedDashboard;
use serde_json::Value;
use std::{
    collections::VecDeque,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Default, Debug)]
pub struct View {
    pub configured: bool,
    pub host: String,
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
    5., 10., 25., 50., 75., 100., 200., 500., 1000., 2000., 5000., 10000.,
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
        ("init", "Init"),
        ("public_material", "Public material"),
        ("query", "Query"),
    ] {
        for latency in [false, true] {
            let mut points = Vec::new();
            for i in (12..view.history.len()).step_by(12) {
                let (start_time, start) = &view.history[i - 12];
                let (end_time, end) = &view.history[i];
                let value = if latency {
                    interval_quantiles(start, end, key).map(|p| p[2])
                } else {
                    delta(end, start, &["operations", key, "arrivals"])
                        .and_then(|n| Some(n / duration(*start_time, *end_time)?))
                };
                points.push((*end_time, value));
            }
            let title = format!(
                "{label} {} · last hour",
                if latency { "p99 latency" } else { "arrivals/s" }
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

pub async fn monitor(url: String, host: String, dashboard: SharedDashboard) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("Status metrics client");
    {
        let mut view = dashboard.write().await;
        view.status.configured = true;
        view.status.host = host;
        view.status.error = Some("waiting for first Status sample".into());
    }
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let result = async {
            let response = client
                .get(&url)
                .send()
                .await
                .map_err(|_| "Status scrape failed")?;
            let response = response
                .error_for_status()
                .map_err(|_| "Status metrics unavailable")?;
            if response.content_length().is_some_and(|len| len > 64 * 1024) {
                return Err("Status metrics too large");
            }
            let mut response = response;
            let mut raw = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| "Status scrape failed")? {
                if chunk.len() > (64 * 1024usize).saturating_sub(raw.len()) {
                    return Err("Status metrics too large");
                }
                raw.extend_from_slice(&chunk);
            }
            let body: Value =
                serde_json::from_slice(&raw).map_err(|_| "Malformed Status metrics")?;
            if !body.get("operations").is_some_and(Value::is_object)
                || !body
                    .get("generation")
                    .is_some_and(|v| v.is_null() || v.is_u64())
                || !body
                    .get("observed_ms")
                    .is_some_and(|v| v.is_null() || v.is_u64())
            {
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
                    ["init", "public_material", "query", "router_query", "worker_evaluate", "router_pack"]
                        .iter().any(|operation| {
                            let path = ["operations", *operation, "arrivals"];
                            matches!((number(&sample, &path), number(previous, &path)), (Some(new), Some(old)) if new < old)
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
    let mut html = format!("<section class=\"card\"><h2 class=\"section-title\">Status APM</h2><p class=\"intro\">Synthetic Status service · {} · {}. Request metrics are aggregate server observations; the server cannot see decrypted transaction states.</p>",esc(&view.host),source);
    if let Some(error) = &view.error {
        html.push_str(&format!(
            "<p class=\"notice\">{}; showing the last successful sample.</p>",
            esc(error)
        ));
    }
    if age.is_some_and(|seconds| !(0. ..=5.).contains(&seconds)) {
        html.push_str("<p class=\"notice\">Status source observation is older than the five-second target.</p>");
    }
    html.push_str("<div class=\"wrap\"><table><thead><tr><th>Operation</th><th>Requests/s</th><th>Failures</th><th>p50</th><th>p90</th><th>p99</th><th>Upload/s</th><th>Download/s</th></tr></thead><tbody>");
    for (key, label) in [
        ("init", "Init"),
        ("public_material", "Public material"),
        ("query", "Query"),
        ("router_query", "Router processing"),
        ("worker_evaluate", "Worker evaluation"),
        ("router_pack", "Router packing"),
    ] {
        let percentiles = quantiles(view, key);
        let f = |n: Option<f64>| n.map(|v| format!("{v:.1}")).unwrap_or_else(|| "—".into());
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
    html.push_str("</tbody></table></div><p class=\"intro\">Latency is in milliseconds, estimated from five-minute histogram buckets. Query is counted once at the coordinator ingress. Router processing includes worker transport and packing; Router packing measures only the final packing operation. Stage percentiles are independent and cannot be added. Failures are cumulative since process start; rates use recent successful scrapes.</p>");
    html.push_str(&graph(view));
    html.push_str("<div class=\"grid\">");
    let cards: &[(&str, &[(&str, &str)])] = &[
        (
            "Publication",
            &[
                ("generation", "Generation"),
                ("recovery_epoch", "Recovery epoch"),
                ("coverage_start", "Coverage start"),
                ("anchor_height", "Anchor height"),
                ("observed_ms", "Observed at · Unix ms"),
                ("last_activation_ms", "Last activation · Unix ms"),
            ],
        ),
        (
            "Capacity",
            &[
                ("entries", "Entries / 1,572,864 ceiling"),
                ("max_bucket_occupancy", "Largest bucket / 256 slots"),
                ("evicted_blocks", "Complete blocks evicted"),
            ],
        ),
        (
            "Preparation · last generation",
            &[
                ("index_ms", "Index ms"),
                ("database_hint_ms", "Hint ms"),
                ("packing_ms", "Packing ms"),
                ("preparation_ms", "PIR preparation ms"),
                ("rebuilt_units", "Units rebuilt"),
                ("reused_units", "Units reused"),
            ],
        ),
        (
            "Colocated host resources",
            &[
                ("resources.host_memory_total_bytes", "Host memory · bytes"),
                (
                    "resources.host_memory_available_bytes",
                    "Available memory · bytes",
                ),
                ("resources.process_rss_bytes", "Process RSS · bytes"),
                ("resources.gpu_utilization_percent", "GPU utilization %"),
                ("resources.gpu_memory_used_mib", "GPU memory used · MiB"),
                ("resources.gpu_memory_total_mib", "GPU memory total · MiB"),
            ],
        ),
    ];
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
    if number(sample, &["preparation_ms"]).is_some_and(|ms| ms > 5000.) {
        html.push_str("<p class=\"notice\">Last PIR preparation exceeded the five-second publication target. Index time is additional.</p>");
    }
    html.push_str("<p class=\"intro\">The service reaffirms an unchanged synthetic fixture. Last activation is the generation build time; it is not a live block publication measurement.</p></section>");
    html
}

pub fn topology(view: &View) -> String {
    if !view.configured {
        return String::new();
    }
    format!("<div class=\"trunk\" style=\"margin:auto;border-style:dashed\"></div><p class=\"summary\">Planned shared coordinator connection</p><a class=\"node-link coord-link\" href=\"/apm/status/\"><strong>{}</strong><span>One GPU host · Status coordinator, router, worker · one process</span><span class=\"{}\">{}</span></a>",esc(&view.host),if view.fresh(){"ok"}else{"bad"},if view.fresh(){"Status monitoring reachable"}else{"Status monitoring unavailable"})
}

#[cfg(test)]
mod tests {
    use super::*;
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
            host: "status-host".into(),
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
