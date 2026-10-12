use super::{
    data::{Histogram, Point},
    store, Source,
};
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
/// Enhance, Status or a configured Transparent service; without one, the
/// `transparent` service when configured, else the first configured service.
fn selection(s: &Selection, view: &super::View) -> Result<(String, u64), StatusCode> {
    let service = match s.service.as_deref() {
        Some(name) => name.to_owned(),
        None => view
            .service("transparent")
            .or(view.services.first())
            .map(|s| s.name.clone())
            .unwrap_or_else(|| "enhance".into()),
    };
    if !matches!(service.as_str(), "enhance" | "status") && view.service(&service).is_none() {
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
    let view = state.read().await.quality.clone();
    let (service, seconds) = selection(&query, &view)?;
    let data = history(view.path.clone(), service.clone(), seconds).await?;
    let sources = view
        .sources
        .iter()
        .filter(|(_, s)| s.service == service)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<BTreeMap<_, _>>();
    let configured = view.service(&service);
    Ok(Json(
        serde_json::json!({"schema_version":2,"service":service,"kind":configured.map(|s|s.kind),"alert_mode":configured.map(|s|if s.shadow{"shadow"}else{"active"}),"range_seconds":seconds,"retention_seconds":store::RETENTION,"collected_at":view.collected_at,"persisted_at":view.persisted_at,"storage_error":view.storage_error,"dropped_batches":view.dropped_batches,"publication":configured.map(|s|&s.publication),"sources":sources,"history":*data,"synthetic":configured.and_then(|s|s.synthetic.as_ref())}),
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
fn metric_label(name: &str) -> String {
    let (base, labels) = name.split_once('|').unwrap_or((name, ""));
    let title = match base {
        "caddy_http_request_duration_seconds" => "Edge response completion".into(),
        "caddy_http_response_duration_seconds" => "Edge time to first byte".into(),
        "transparent_publication_cycle_seconds" => "Successful publication cycle".into(),
        "transparent_publication_freshness_seconds" => "Successful publication visibility".into(),
        _ => base.replace('_', " "),
    };
    if labels.is_empty() {
        title
    } else {
        format!("{title} · {labels}")
    }
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
            escape(&metric_label(name)),
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
    if source.role == "independent-probe" {
        out.push_str(&chart(
            points,
            "probe_duration_seconds",
            "latest value",
            true,
            |p| p.values.gauges.get("probe_duration_seconds").copied(),
        ));
        out.push_str("<p>One exact query per minute. Probe duration includes client preparation and canonical checks.</p></section>");
        return out;
    }
    out.push_str(&format!(
        "<p>Five-minute window: {} seconds observed; {} discontinuities.</p>",
        window.observed_seconds, window.discontinuities
    ));
    out.push_str("<div class=\"table-wrap\"><table><thead><tr><th>Latency scope</th><th>Samples</th><th>p50</th><th>p90</th><th>p95</th><th>p99</th></tr></thead><tbody>");
    for (name, h) in &window.values.histograms {
        if !name
            .split('|')
            .next()
            .is_some_and(|n| n.ends_with("_seconds"))
        {
            continue;
        }
        if h.counts.last().is_none_or(|n| *n == 0.) {
            continue;
        }
        out.push_str(&format!(
            "<tr><th>{}</th><td>{:.0}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape(&metric_label(name)),
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
        .filter(|k| k.split('|').next().is_some_and(|n| n.ends_with("_seconds")))
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
            escape(&metric_label(name))
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
    format!("<figure><figcaption>{} · {} · upper scale {:.3}</figcaption><svg role=\"img\" aria-label=\"Metric history with gaps\" viewBox=\"0 0 700 150\"><g fill=\"none\" stroke=\"#b9915f\" stroke-width=\"2\">{paths}</g><g fill=\"#b9915f\">{dots}</g></svg></figure>",escape(&metric_label(name)),escape(unit),max)
}
/// A service's display name: its configured name with a capital first letter.
pub fn title(name: &str) -> String {
    let mut chars = name.chars();
    chars
        .next()
        .map(|c| c.to_uppercase().chain(chars).collect())
        .unwrap_or_default()
}
/// The `/apm/` tabs after Enhance and Status: one per configured service, then capacity.
pub fn tabs(view: &super::View) -> String {
    let mut out = String::new();
    for s in &view.services {
        out.push_str(&format!(
            "<a href=\"/apm/quality/?service={}\">{} APM</a>",
            escape(&s.name),
            escape(&title(&s.name))
        ));
    }
    if view.enabled {
        out.push_str("<a href=\"/apm/capacity/\">Capacity</a>");
    }
    out
}
/// Links to every pane: Enhance, Status, each configured service and capacity.
pub(super) fn nav(view: &super::View) -> String {
    let mut out = String::from("<nav class=\"quality-nav\"><a href=\"/apm/?pane=enhance\">Enhance</a><a href=\"/apm/?pane=status\">Status</a>");
    for s in &view.services {
        out.push_str(&format!(
            "<a href=\"/apm/quality/?service={}\">{}</a>",
            escape(&s.name),
            escape(&title(&s.name))
        ));
    }
    if view.enabled {
        out.push_str("<a href=\"/apm/capacity/\">Capacity</a>");
    }
    out.push_str("</nav>");
    out
}
/// One histogram merged over every stored bin; None without data or when its
/// buckets changed within the range.
fn merged(points: &[Point], key: &str) -> Option<Histogram> {
    let mut out: Option<Histogram> = None;
    for p in points {
        if p.invalid_histograms.contains(key) {
            return None;
        }
        if let Some(h) = p.values.histograms.get(key) {
            match &mut out {
                Some(o) => {
                    if !o.merge(h) {
                        return None;
                    }
                }
                None => out = Some(h.clone()),
            }
        }
    }
    out.filter(|h| h.counts.last().is_some_and(|n| *n > 0.))
}
fn seconds(h: Option<&Histogram>, q: f64) -> String {
    match h.map(|h| (h.quantile(q), h)) {
        Some((Some(v), _)) => format!("{v:.2} s"),
        Some((None, h)) => format!(
            "> {} s",
            number(h.bounds.iter().rev().flatten().next().copied())
        ),
        None => "—".into(),
    }
}
fn timing_row(label: &str, h: Option<&Histogram>, p99: bool) -> String {
    format!(
        "<tr><th>{}</th><td>{:.0}</td><td>{}</td><td>{}</td>{}</tr>",
        escape(label),
        h.and_then(|h| h.counts.last().copied()).unwrap_or(0.),
        seconds(h, 0.5),
        seconds(h, 0.95),
        if p99 {
            format!("<td>{}</td>", seconds(h, 0.99))
        } else {
            String::new()
        }
    )
}
fn age(now: u64, at: Option<f64>) -> String {
    at.map(|t| format!("{:.0} s", (now as f64 - t).max(0.)))
        .unwrap_or_else(|| "—".into())
}

/// Node and published heights, backlog, last success, and publication cycle
/// and visibility percentiles over the selected range.
fn publication_block(
    svc: &super::ServiceView,
    sources: &[(&String, &Source)],
    history: &History,
    range: &str,
) -> String {
    let now = unix_time();
    let c = super::rules::coordinator(svc.kind);
    let coordinator = sources.iter().find(|(_, s)| s.role == c.role);
    let gauge = |key: &str| {
        coordinator.and_then(|(_, s)| s.current.gauges.get(key).copied().filter(|_| s.fresh(now)))
    };
    let p = &svc.publication;
    let verdict = |v: Option<bool>| match v {
        Some(true) => "Verified",
        Some(false) => "Mismatch",
        None => "Unavailable",
    };
    let points = coordinator
        .and_then(|(name, _)| history.get(name.as_str()))
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let mut out = format!("<section class=\"card\"><h2>Publication freshness</h2><div class=\"stats\"><p><strong>{}</strong>Node height</p><p><strong>{}</strong>Published height</p><p><strong>{}</strong>Lag (blocks)</p><p><strong>{}</strong>Oldest unpublished</p><p><strong>{}</strong>Since last success</p></div><p>Canonical anchor: <strong>{}</strong> · Public origins agree on <code>{}</code>: <strong>{}</strong> · verification age {} s · independent node lag {} blocks. Idle-chain time is not publication backlog.</p><div class=\"table-wrap\"><table><thead><tr><th>Over {}</th><th>Samples</th><th>p50</th><th>p95</th><th>p99</th></tr></thead><tbody>",
        number(gauge(c.node_height)),
        number(gauge(c.published_height)),
        number(gauge(c.lag)),
        gauge(c.oldest_unpublished).map(|v| format!("{v:.0} s")).unwrap_or_else(|| "—".into()),
        age(now, gauge(c.last_success)),
        verdict(p.canonical),
        escape(&format!("{}{}", svc.path_prefix, match svc.kind { super::services::Kind::History => "/shards/init", super::services::Kind::Txid => "/txid/init" })),
        if svc.public_origins.is_empty() { "not configured" } else { verdict(p.origins_agree) },
        now.saturating_sub(p.sampled_at),
        p.lag_blocks.map(|n| n.to_string()).unwrap_or_else(|| "—".into()),
        escape(range),
    );
    out.push_str(&timing_row(
        "Publication cycle",
        merged(points, c.cycle).as_ref(),
        true,
    ));
    out.push_str(&timing_row(
        "Publication visibility",
        merged(points, c.freshness).as_ref(),
        true,
    ));
    out.push_str("</tbody></table></div></section>");
    out
}

/// How long rebuilding takes: per-stage timings of the txid display cycle or
/// the history publication cycle, and each worker's revision prepare.
fn rebuild_block(
    svc: &super::ServiceView,
    sources: &[(&String, &Source)],
    history: &History,
    range: &str,
) -> String {
    let c = super::rules::coordinator(svc.kind);
    let (roles, prepare) = super::rules::workers(svc.kind);
    let mut out = format!("<section class=\"card\"><h2>Recent rebuild</h2><div class=\"table-wrap\"><table><thead><tr><th>Over {}</th><th>Samples</th><th>p50</th><th>p95</th></tr></thead><tbody>", escape(range));
    if let Some((name, _)) = sources.iter().find(|(_, s)| s.role == c.role) {
        let points = history.get(name.as_str()).map(Vec::as_slice).unwrap_or(&[]);
        out.push_str(&timing_row(
            "Whole cycle",
            merged(points, c.cycle).as_ref(),
            false,
        ));
        if svc.kind == super::services::Kind::Txid {
            for stage in [
                "build", "verify", "digest", "write", "prebuild", "ship", "prepare", "activate",
            ] {
                let key = format!("txid_display_cycle_stage_seconds|stage={stage}");
                out.push_str(&timing_row(
                    &format!("Stage: {stage}"),
                    merged(points, &key).as_ref(),
                    false,
                ));
            }
        }
    }
    for (name, s) in sources {
        if roles.contains(&s.role.as_str()) {
            let points = history.get(name.as_str()).map(Vec::as_slice).unwrap_or(&[]);
            out.push_str(&timing_row(
                &format!("Prepare · {name}"),
                merged(points, prepare).as_ref(),
                false,
            ));
        }
    }
    out.push_str("</tbody></table></div><p>Fifteen-minute p95s of the same timings drive the rebuild and prepare alerts.</p></section>");
    out
}

/// Memory in use on each host behind the service, one line per host.
fn memory_block(sources: &[(&String, &Source)], history: &History) -> String {
    let mut seen = std::collections::BTreeSet::new();
    let mut out = String::new();
    for (name, s) in sources {
        let host = s.host.clone().unwrap_or_else(|| name.to_string());
        let Some(points) = history.get(name.as_str()) else {
            continue;
        };
        if !points.iter().any(|p| {
            p.values
                .gauges
                .contains_key("pir_host_memory_available_bytes")
        }) || !seen.insert(host.clone())
        {
            continue;
        }
        out.push_str(&chart(
            points,
            &format!("Memory in use · {host}"),
            "% of host memory",
            true,
            |p| {
                let g = &p.values.gauges;
                let total = g.get("pir_host_memory_total_bytes").filter(|t| **t > 0.)?;
                Some((1. - g.get("pir_host_memory_available_bytes")? / total) * 100.)
            },
        ));
    }
    if out.is_empty() {
        return String::new();
    }
    format!("<section class=\"card\"><h2>Host memory</h2>{out}<p><a href=\"/apm/capacity/\">Capacity forecasts and next sizes</a></p></section>")
}

async fn render(
    state: SharedDashboard,
    query: Selection,
    worker: Option<String>,
) -> Result<Html<String>, StatusCode> {
    let d = state.read().await.clone();
    let (service, seconds) = selection(&query, &d.quality)?;
    let service = service.as_str();
    let configured = d.quality.service(service);
    if let Some(name) = &worker {
        if d.quality
            .sources
            .get(name)
            .is_none_or(|s| s.service != service)
        {
            return Err(StatusCode::NOT_FOUND);
        }
    }
    let range = query.range.clone().unwrap_or_else(|| "1h".into());
    let h = history(d.quality.path.clone(), service.into(), seconds).await;
    let heading = match service {
        "enhance" => "Enhance".into(),
        "status" => "Status".into(),
        name => title(name),
    };
    let mut out=format!("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta http-equiv=\"refresh\" content=\"30\"><title>{} service quality · PIR APM</title><style>{} main{{max-width:1280px;margin:auto;padding:24px}} .stats{{display:flex;gap:32px;flex-wrap:wrap}} .stats strong{{display:block;font-size:28px;color:var(--gold)}} pre{{white-space:pre-wrap;overflow-wrap:anywhere}} a{{color:var(--gold)}} .quality-nav{{display:flex;gap:20px;flex-wrap:wrap;margin:24px 0}} .card{{margin:24px 0}} figure{{margin:24px 0}} svg{{width:100%;max-height:180px}} th{{text-align:left;overflow-wrap:anywhere}} td,th{{padding:8px}} .table-wrap{{overflow-x:auto}} details{{margin:20px 0}} h1{{font-size:36px}}</style></head><body><main>{}<h1>{} service quality</h1><p>Seven-day retention · history starts when collection is enabled · refreshes every 30 seconds · UTC</p><nav class=\"quality-nav\">",escape(&heading),crate::dashboard::STYLE,nav(&d.quality),escape(&heading));
    for range in ["1h", "24h", "7d"] {
        out.push_str(&format!(
            "<a href=\"?service={}&amp;range={range}\">{range}</a>",
            escape(service)
        ));
    }
    out.push_str("</nav>");
    let now = unix_time();
    let sources: Vec<_> = d
        .quality
        .sources
        .iter()
        .filter(|(_, s)| s.service == service)
        .collect();
    let shadow = match configured {
        Some(s) => s.shadow,
        None => std::env::var("PIR_APM_QUALITY_ALERT_MODE").as_deref() != Ok("active"),
    };
    out.push_str(&format!(
        "<p>{}/{} sources fresh · New quality alerts: <strong>{}</strong></p>",
        sources.iter().filter(|(_, s)| s.fresh(now)).count(),
        sources.len(),
        if shadow { "shadow" } else { "active" }
    ));
    if !d.quality.enabled {
        out.push_str("<p>Quality collection is not configured.</p>");
    }
    if d.quality.storage_error || h.is_err() {
        out.push_str("<p class=\"warn\">History storage is unavailable. Live data may still be collected.</p>");
    }
    if configured.map_or(d.quality.inventory_error, |s| s.inventory_error) {
        out.push_str(
            "<p class=\"warn\">Fleet inventory is invalid. Retaining the last valid inventory.</p>",
        );
    }
    let empty = BTreeMap::new();
    let history = h.as_ref().map(|v| v.as_ref()).unwrap_or(&empty);
    if let (Some(svc), None) = (configured, &worker) {
        out.push_str(&publication_block(svc, &sources, history, &range));
        if let Some(synthetic) = &svc.synthetic {
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
        out.push_str(&rebuild_block(svc, &sources, history, &range));
        out.push_str(&memory_block(&sources, history));
    }
    for (name, source) in &sources {
        if worker.as_ref().is_none_or(|n| n == *name) {
            if worker.is_none()
                && matches!(
                    source.role.as_str(),
                    "recent-replica"
                        | "archive-owner"
                        | "display-worker"
                        | "worker"
                        | "packing-router"
                )
            {
                let w = source.window(unix_time());
                let latency = w.values.histograms.iter().find(|(k, h)| {
                    (k.starts_with("transparent_shard_query_seconds")
                        || k.contains("evaluation_duration"))
                        && h.counts.last().is_some_and(|n| *n > 0.)
                });
                out.push_str(&format!("<section class=\"card\"><h2><a href=\"/apm/transparent/workers/{}/?service={}\">{}</a></h2><p>{} · {} · {}</p><p>Query/evaluation p99: {} · observed {}s of the last five minutes</p><p>Process RSS: {} MiB · cache resident: {} MiB</p></section>",escape(name),escape(service),escape(name),escape(&source.role),if source.fresh(unix_time()){"Fresh"}else{"Stale / unavailable"},match source.ready{Some(true)=>"Ready",Some(false)=>"Not ready",None=>"Readiness unavailable"},latency.map(|(_,h)|quantile(h,0.99)).unwrap_or_else(||"—".into()),w.observed_seconds,number(source.current.gauges.get("transparent_shard_process_rss_bytes").or_else(||source.current.gauges.get("enhance_worker_process_rss_bytes")).map(|v|v/1048576.)),number(source.current.gauges.get("transparent_shard_cache_resident_bytes").map(|v|v/1048576.))));
            } else {
                out.push_str(&source_card(
                    name,
                    source,
                    history.get(name.as_str()).map(Vec::as_slice).unwrap_or(&[]),
                ));
            }
        }
    }
    out.push_str("<section class=\"card\"><h2>Incidents</h2>");
    let prefix = super::rules::service_prefix(service);
    for incident in &d.monitoring.incidents {
        if incident.active {
            if let Some(c) = &incident.condition {
                let ours = if configured.is_some() {
                    c.key.starts_with(&prefix)
                } else {
                    c.key.starts_with("quality_")
                        && (c.resource.contains(service) || c.key.contains(service))
                };
                if ours || c.key == "quality_storage" {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn range_percentiles_merge_buckets_and_break_on_changed_bounds() {
        let point = |bounds: Vec<Option<f64>>, counts: Vec<f64>| {
            let mut p = Point::default();
            p.values
                .histograms
                .insert("cycle".into(), Histogram { bounds, counts });
            p
        };
        let a = point(vec![Some(1.), Some(10.), None], vec![5., 10., 10.]);
        let h = merged(&[a.clone(), a.clone()], "cycle").unwrap();
        assert_eq!(h.counts, vec![10., 20., 20.]);
        assert_eq!(seconds(Some(&h), 0.25), "0.50 s");
        assert_eq!(seconds(None, 0.5), "—");
        let b = point(vec![Some(2.), Some(10.), None], vec![1., 1., 1.]);
        assert!(merged(&[a, b], "cycle").is_none());
        assert!(merged(&[], "cycle").is_none());
        assert_eq!(title("txid"), "Txid");
    }
}
