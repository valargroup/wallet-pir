use super::{data::Point, store, Source};
use crate::dashboard::SharedDashboard;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Html,
    Json,
};
use pir_apm::incidents::unix_time;
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, OnceLock},
    time::Instant,
};

#[derive(Default, Deserialize)]
pub struct Selection {
    service: Option<String>,
    range: Option<String>,
}
fn selection(s: &Selection) -> Result<(&str, u64), StatusCode> {
    let service = s.service.as_deref().unwrap_or("transparent");
    if !matches!(service, "enhance" | "status" | "transparent") {
        return Err(StatusCode::BAD_REQUEST);
    }
    let seconds = match s.range.as_deref().unwrap_or("1h") {
        "1h" => 3600,
        "24h" => 86400,
        "7d" => store::RETENTION,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    Ok((service, seconds))
}
type History = BTreeMap<String, Vec<Point>>;
type Cache = BTreeMap<String, (Instant, Arc<History>)>;
async fn history(
    path: Option<std::path::PathBuf>,
    service: String,
    seconds: u64,
) -> Result<Arc<History>, StatusCode> {
    let Some(path) = path else {
        return Ok(Arc::new(BTreeMap::new()));
    };
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    let key = format!("{}:{service}:{seconds}", path.display());
    if let Some((at, value)) = cache.lock().unwrap().get(&key) {
        if at.elapsed().as_secs() < 60 {
            return Ok(value.clone());
        }
    }
    let now = unix_time();
    let h = tokio::task::spawn_blocking(move || {
        store::Store::open(&path)?.history(&service, now, seconds)
    })
    .await
    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let result = Arc::new(h);
    cache
        .lock()
        .unwrap()
        .insert(key, (Instant::now(), result.clone()));
    Ok(result)
}
pub async fn api(
    State(state): State<SharedDashboard>,
    Query(query): Query<Selection>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let (service, seconds) = selection(&query)?;
    let view = state.read().await.quality.clone();
    let data = history(view.path.clone(), service.into(), seconds).await?;
    let sources = view
        .sources
        .iter()
        .filter(|(_, s)| s.service == service)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<BTreeMap<_, _>>();
    Ok(Json(
        serde_json::json!({"schema_version":1,"service":service,"range_seconds":seconds,"retention_seconds":store::RETENTION,"collected_at":view.collected_at,"persisted_at":view.persisted_at,"storage_error":view.storage_error,"dropped_batches":view.dropped_batches,"publication":if service=="transparent"{Some(view.publication)}else{None},"sources":sources,"history":*data,"synthetic":if service=="transparent"{view.synthetic}else{None}}),
    ))
}
pub async fn page(
    State(state): State<SharedDashboard>,
    Query(query): Query<Selection>,
) -> Result<Html<String>, StatusCode> {
    render(state, query, None).await
}
pub async fn worker_page(
    State(state): State<SharedDashboard>,
    Path(name): Path<String>,
    Query(query): Query<Selection>,
) -> Result<Html<String>, StatusCode> {
    render(state, query, Some(name)).await
}
fn escape(s: &str) -> String {
    crate::dashboard::escape(s)
}
fn number(v: Option<f64>) -> String {
    v.map(|n| format!("{n:.2}")).unwrap_or_else(|| "—".into())
}
fn quantile(h: &super::data::Histogram, q: f64) -> String {
    match h.quantile(q) {
        Some(v) => format!("{:.2} ms", v * 1000.),
        None if h.counts.last().is_some_and(|n| *n > 0.) => format!(
            "> {} s",
            number(h.bounds.iter().rev().flatten().next().copied())
        ),
        None => "—".into(),
    }
}
pub fn summary(view: &super::View, service: &str) -> String {
    if !view.enabled {
        return String::new();
    }
    let now = unix_time();
    let sources: Vec<_> = view
        .sources
        .values()
        .filter(|s| s.service == service)
        .collect();
    let fresh = sources.iter().filter(|s| s.fresh(now)).count();
    format!("<section class=\"notice\"><a href=\"/apm/quality/?service={}\">Service quality · seven-day history</a> · {fresh}/{} sources fresh · history {}.</section>",escape(service),sources.len(),if view.storage_error{"unavailable"}else{"enabled"})
}
fn counters(point: &Point) -> String {
    let mut rows = String::new();
    for (name, value) in &point.values.counters {
        if name.ends_with("_sum")
            || name.contains("_sum|")
            || name.ends_with("_count")
            || name.contains("_count|")
        {
            continue;
        }
        rows.push_str(&format!(
            "<tr><th>{}</th><td>{value:.0}</td><td>{}</td></tr>",
            escape(&name.replace('_', " ").replace('|', " · ")),
            number(
                (point.observed_seconds > 0)
                    .then_some(value / point.observed_seconds.max(1) as f64)
            )
        ));
    }
    rows
}
fn source_card(name: &str, source: &Source, points: &[Point]) -> String {
    let now = unix_time();
    let window = source.window(now);
    let fresh = source.fresh(now);
    let mut out=format!("<section class=\"card\"><h2><a href=\"/apm/transparent/workers/{}/?service={}\">{}</a></h2><p class=\"{}\">{} · {} · sample age {}s{}</p>",escape(name),escape(&source.service),escape(name),if fresh{"ok"}else{"warn"},escape(&source.role),if fresh{"Fresh"}else{"Unavailable / stale"},now.saturating_sub(source.sampled_at),source.ready.map(|r|if r{" · ready"}else{" · not ready"}).unwrap_or(""));
    if source.role == "independent-probe" {
        let result = if !fresh {
            "Unavailable"
        } else if source.current.gauges.get("probe_passed") == Some(&1.) {
            "Passed"
        } else {
            "Failed"
        };
        out.push_str(&format!("<p><strong>{result}</strong> · last probe {} seconds · {} successes / {} failures since monitor start</p>",number(source.current.gauges.get("probe_duration_seconds").copied()),number(source.current.gauges.get("probe_successes").copied()),number(source.current.gauges.get("probe_failures").copied())));
    }
    if !fresh {
        out.push_str(
            "<p>Retained values below are stale. They do not establish current health.</p>",
        );
    }
    out.push_str(&format!(
        "<p>Five-minute window: {} seconds observed; {} discontinuities.</p>",
        window.observed_seconds, window.discontinuities
    ));
    out.push_str("<div class=\"table-wrap\"><table><thead><tr><th>Latency scope</th><th>Samples</th><th>p50</th><th>p90</th><th>p95</th><th>p99</th></tr></thead><tbody>");
    for (name, h) in &window.values.histograms {
        if h.counts.last().is_none_or(|n| *n == 0.) {
            continue;
        }
        out.push_str(&format!(
            "<tr><th>{}</th><td>{:.0}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape(&name.replace('_', " ").replace('|', " · ")),
            h.counts.last().copied().unwrap_or(0.),
            quantile(h, 0.5),
            quantile(h, 0.9),
            quantile(h, 0.95),
            quantile(h, 0.99)
        ));
    }
    out.push_str("</tbody></table></div>");
    // One chart per actual timing scope; empty intervals break lines.
    let hist_names = points
        .iter()
        .flat_map(|p| p.values.histograms.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>();
    for key in hist_names.iter().take(8) {
        out.push_str(&chart(points, key, "p99 seconds", false, |p| {
            p.values.histograms.get(key).and_then(|h| h.quantile(0.99))
        }));
    }
    for key in [
        "pir_http_arrivals_total",
        "caddy_http_requests_total",
        "enhance_http_arrivals_total",
        "status_http_arrivals_total",
    ] {
        if points
            .iter()
            .any(|p| p.values.counters.keys().any(|k| k.starts_with(key)))
        {
            out.push_str(&chart(points, key, "requests / second", false, |p| {
                Some(
                    p.values
                        .counters
                        .iter()
                        .filter(|(k, _)| k.starts_with(key))
                        .map(|(_, v)| v)
                        .sum::<f64>()
                        / p.observed_seconds.max(1) as f64,
                )
            }));
        }
    }
    for key in [
        "transparent_publication_freshness_seconds",
        "pir_host_memory_available_bytes",
        "pir_host_disk_available_bytes",
        "probe_duration_seconds",
    ] {
        if points.iter().any(|p| p.values.gauges.contains_key(key)) {
            out.push_str(&chart(points, key, "latest value", true, |p| {
                p.values.gauges.get(key).copied()
            }));
        }
    }
    out.push_str("<details><summary>Request outcomes, throughput and bytes · five-minute deltas</summary><div class=\"table-wrap\"><table><thead><tr><th>Metric</th><th>Count / bytes</th><th>Per observed second</th></tr></thead><tbody>");
    out.push_str(&counters(&window));
    out.push_str("</tbody></table></div></details>");
    out.push_str("<details><summary>Publication, cache, admission and resources · latest snapshot</summary><div class=\"table-wrap\"><table><tbody>");
    for (name, value) in &source.current.gauges {
        out.push_str(&format!(
            "<tr><th>{}</th><td>{value:.2}</td></tr>",
            escape(&name.replace('_', " ").replace('|', " · "))
        ));
    }
    out.push_str("</tbody></table></div></details></section>");
    out
}
fn chart(
    points: &[Point],
    name: &str,
    unit: &str,
    gauge: bool,
    value: impl Fn(&Point) -> Option<f64>,
) -> String {
    let start = points.first().map(|p| p.at).unwrap_or(0);
    let end = points
        .last()
        .map(|p| p.at)
        .unwrap_or(start + 1)
        .max(start + 1);
    let values = points
        .iter()
        .map(|p| {
            if (gauge && !p.values.gauges.is_empty())
                || (p.discontinuities == 0 && p.observed_seconds > 0)
            {
                value(p)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let max = values
        .iter()
        .flatten()
        .copied()
        .fold(0f64, f64::max)
        .max(0.001);
    let mut paths = String::new();
    let mut segment = String::new();
    let mut dots = String::new();
    for (p, value) in points.iter().zip(values) {
        if let Some(value) = value {
            let x = 20. + (p.at - start) as f64 / (end - start) as f64 * 660.;
            let y = 130. - value / max * 110.;
            segment.push_str(&format!("{x:.1},{y:.1} "));
            let utc = chrono::DateTime::from_timestamp(p.at as i64, 0)
                .map(|d| d.to_rfc3339())
                .unwrap_or_default();
            dots.push_str(&format!("<circle cx=\"{x:.1}\" cy=\"{y:.1}\" r=\"2\" tabindex=\"0\"><title>{utc} · {:.3} {} · {} observed seconds</title></circle>",value,escape(unit),p.observed_seconds));
        } else if !segment.is_empty() {
            paths.push_str(&format!("<polyline points=\"{segment}\"/>"));
            segment.clear();
        }
    }
    if !segment.is_empty() {
        paths.push_str(&format!("<polyline points=\"{segment}\"/>"));
    }
    format!("<figure><figcaption>{} · {} · upper scale {:.3}</figcaption><svg role=\"img\" aria-label=\"Metric history with gaps\" viewBox=\"0 0 700 150\"><g fill=\"none\" stroke=\"#b9915f\" stroke-width=\"2\">{paths}</g><g fill=\"#b9915f\">{dots}</g></svg></figure>",escape(&name.replace('_'," ")),escape(unit),max)
}
async fn render(
    state: SharedDashboard,
    query: Selection,
    worker: Option<String>,
) -> Result<Html<String>, StatusCode> {
    let (service, seconds) = selection(&query)?;
    let d = state.read().await.clone();
    if let Some(name) = &worker {
        if d.quality
            .sources
            .get(name)
            .is_none_or(|s| s.service != service)
        {
            return Err(StatusCode::NOT_FOUND);
        }
    }
    let h = history(d.quality.path.clone(), service.into(), seconds).await;
    let mut out=format!("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta http-equiv=\"refresh\" content=\"30\"><title>{} service quality · PIR APM</title><style>{} main{{max-width:1280px;margin:auto;padding:24px}} .stats{{display:flex;gap:32px;flex-wrap:wrap}} .stats strong{{display:block;font-size:28px;color:var(--gold)}} pre{{white-space:pre-wrap;overflow-wrap:anywhere}} a{{color:var(--gold)}} .quality-nav{{display:flex;gap:20px;flex-wrap:wrap;margin:24px 0}} .card{{margin:24px 0}} figure{{margin:24px 0}} svg{{width:100%;max-height:180px}} th{{text-align:left;overflow-wrap:anywhere}} td,th{{padding:8px}} .table-wrap{{overflow-x:auto}} details{{margin:20px 0}} h1{{font-size:36px}}</style></head><body><main><nav class=\"quality-nav\"><a href=\"/apm/?pane=enhance\">Enhance</a><a href=\"/apm/?pane=status\">Status</a><a href=\"/apm/transparent/\">Transparent</a></nav><h1>{} service quality</h1><p>Seven-day retention · history starts when collection is enabled · refreshes every 30 seconds · UTC</p><nav class=\"quality-nav\">",escape(service),crate::dashboard::STYLE,match service {"transparent"=>"Transparent", "enhance"=>"Enhance", _=>"Status"});
    for range in ["1h", "24h", "7d"] {
        out.push_str(&format!(
            "<a href=\"?service={service}&amp;range={range}\">{range}</a>"
        ));
    }
    out.push_str("</nav>");
    let now = unix_time();
    let sources: Vec<_> = d
        .quality
        .sources
        .values()
        .filter(|s| s.service == service)
        .collect();
    out.push_str(&format!(
        "<p>{}/{} sources fresh · New quality alerts: <strong>{}</strong></p>",
        sources.iter().filter(|s| s.fresh(now)).count(),
        sources.len(),
        if std::env::var("PIR_APM_QUALITY_ALERT_MODE").as_deref() == Ok("active") {
            "active"
        } else {
            "shadow"
        }
    ));
    if !d.quality.enabled {
        out.push_str("<p>Quality collection is not configured.</p>");
    }
    if d.quality.storage_error || h.is_err() {
        out.push_str("<p class=\"warn\">History storage is unavailable. Live data may still be collected.</p>");
    }
    if d.quality.inventory_error {
        out.push_str(
            "<p class=\"warn\">Fleet inventory is invalid. Retaining the last valid inventory.</p>",
        );
    }
    if service == "transparent" {
        let p = &d.quality.publication;
        let verdict = |v: Option<bool>| match v {
            Some(true) => "Verified",
            Some(false) => "Mismatch",
            None => "Unavailable",
        };
        out.push_str(&format!("<section class=\"card\"><h2>Publication and coverage</h2><p>Public height <strong>{}</strong> · Independent node lag <strong>{}</strong> blocks</p><p>Canonical anchor: <strong>{}</strong> · Public origins agree: <strong>{}</strong></p><p>Verification age: {} seconds. Idle-chain time is not publication backlog.</p></section>",p.public_height.map(|n|n.to_string()).unwrap_or_else(||"—".into()),p.lag_blocks.map(|n|n.to_string()).unwrap_or_else(||"—".into()),verdict(p.canonical),verdict(p.origins_agree),unix_time().saturating_sub(p.sampled_at)));
        if let Some(synthetic) = &d.quality.synthetic {
            out.push_str("<section class=\"card\"><h2>Continuous exact-query test</h2><p>Synthetic public-path requests from the coordinator. Exact queries test individual rows; whole-wallet completion is a separate measure.</p>");
            out.push_str(&format!(
                "<p class=\"{}\">{} · {}</p><div class=\"stats\">",
                if synthetic["fresh"] == true {
                    "ok"
                } else {
                    "warn"
                },
                if synthetic["fresh"] == true {
                    "Fresh"
                } else {
                    "Stale / unavailable"
                },
                escape(synthetic["mode"].as_str().unwrap_or("unknown"))
            ));
            for (key, label) in [
                ("completed_qps", "Completed queries / second"),
                ("exact", "Exact queries in 60s"),
                ("errors", "Raw errors in 60s"),
                ("missed_slots", "Missed slots in 60s"),
            ] {
                out.push_str(&format!(
                    "<p><strong>{}</strong>{label}</p>",
                    number(synthetic["trailing_60s"][key].as_f64())
                ));
            }
            out.push_str(
                "</div><details><summary>Latency, retries and cumulative outcomes</summary><pre>",
            );
            out.push_str(&escape(
                &serde_json::to_string_pretty(synthetic).unwrap_or_default(),
            ));
            out.push_str("</pre></details></section>");
        }
    }
    let empty = BTreeMap::new();
    let history = h.as_ref().map(|v| v.as_ref()).unwrap_or(&empty);
    for (name, source) in &d.quality.sources {
        if source.service == service && worker.as_ref().is_none_or(|n| n == name) {
            if worker.is_none()
                && matches!(
                    source.role.as_str(),
                    "recent-replica" | "archive-owner" | "worker" | "packing-router"
                )
            {
                let w = source.window(unix_time());
                let latency = w.values.histograms.iter().find(|(k, h)| {
                    (k.starts_with("transparent_shard_query_seconds")
                        || k.contains("evaluation_duration"))
                        && h.counts.last().is_some_and(|n| *n > 0.)
                });
                out.push_str(&format!("<section class=\"card\"><h2><a href=\"/apm/transparent/workers/{}/?service={}\">{}</a></h2><p>{} · {} · {}</p><p>Query/evaluation p99: {} · observed {}s of the last five minutes</p><p>Process RSS: {} MiB · cache resident: {} MiB</p></section>",escape(name),service,escape(name),escape(&source.role),if source.fresh(unix_time()){"Fresh"}else{"Stale / unavailable"},match source.ready{Some(true)=>"Ready",Some(false)=>"Not ready",None=>"Readiness unavailable"},latency.map(|(_,h)|quantile(h,0.99)).unwrap_or_else(||"—".into()),w.observed_seconds,number(source.current.gauges.get("transparent_shard_process_rss_bytes").or_else(||source.current.gauges.get("enhance_worker_process_rss_bytes")).map(|v|v/1048576.)),number(source.current.gauges.get("transparent_shard_cache_resident_bytes").map(|v|v/1048576.))));
            } else {
                out.push_str(&source_card(
                    name,
                    source,
                    history.get(name).map(Vec::as_slice).unwrap_or(&[]),
                ));
            }
        }
    }
    out.push_str("<section class=\"card\"><h2>Incidents</h2>");
    for incident in &d.monitoring.incidents {
        if incident.active {
            if let Some(c) = &incident.condition {
                if c.resource.contains(service)
                    || c.key.contains(service)
                    || c.key == "quality_storage"
                {
                    out.push_str(&format!(
                        "<p>{} · {} · {}</p>",
                        escape(&c.severity),
                        escape(&c.key),
                        escape(&c.observed)
                    ));
                }
            }
        }
    }
    out.push_str("</section><p>HTTP timing ends at response construction unless labeled as edge timing. Worker evaluation and queue timings are independent scopes. Synthetic checks include client processing. Percentiles are merged from histogram buckets; gaps and overflow remain explicit. Byte counters measure payload consumption/emission, not successful receipt by a wallet.</p></main></body></html>");
    Ok(Html(out))
}
