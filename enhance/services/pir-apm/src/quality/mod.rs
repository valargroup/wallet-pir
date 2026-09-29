//! Independent aggregate collectors and seven-day service-quality history.
mod data;
mod publication;
mod store;
mod view;
use crate::{config::Config, dashboard::SharedDashboard};
use anyhow::{Context, Result};
use data::{Histogram, Point, Reading};
use pir_apm::incidents::{unix_time, Condition};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
pub use view::{api, page, summary, worker_page};

#[derive(Clone, Debug, Default, Serialize)]
pub struct Source {
    pub publication: Option<String>,
    pub service: String,
    pub role: String,
    pub sampled_at: u64,
    pub error: Option<String>,
    pub ready: Option<bool>,
    pub current: data::Values,
    #[serde(skip)]
    pub recent: VecDeque<Point>,
}
impl Source {
    pub fn fresh(&self, now: u64) -> bool {
        self.sampled_at > 0
            && self.sampled_at <= now
            && now - self.sampled_at
                <= if self.role == "independent-probe" {
                    90
                } else {
                    45
                }
            && self.error.is_none()
    }
    pub fn window(&self, now: u64) -> Point {
        let mut p = Point::default();
        for point in &self.recent {
            if point.at > now.saturating_sub(300) {
                p.merge(point);
            }
        }
        p
    }
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct View {
    pub publication: publication::View,
    pub enabled: bool,
    pub collected_at: u64,
    pub persisted_at: u64,
    pub storage_error: bool,
    pub dropped_batches: u64,
    pub last_dropped_at: u64,
    pub inventory_error: bool,
    pub sources: BTreeMap<String, Source>,
    pub synthetic: Option<serde_json::Value>,
    #[serde(skip)]
    pub path: Option<PathBuf>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransparentConfig {
    publisher_url: String,
    roster: PathBuf,
    router_metrics_url: Option<String>,
    synthetic_status: Option<PathBuf>,
    host_snapshot: Option<PathBuf>,
    #[serde(default)]
    public_origins: Vec<String>,
}
#[derive(Deserialize)]
struct Worker {
    id: String,
    role: String,
    upstream: String,
}
#[derive(Clone)]
struct Target {
    id: String,
    service: &'static str,
    role: String,
    url: String,
    ready: Option<String>,
    status_operation: Option<&'static str>,
    endpoint: Option<&'static str>,
}

fn validate_url(value: &str) -> Result<()> {
    let u = reqwest::Url::parse(value)?;
    if u.scheme() == "file" {
        anyhow::ensure!(
            u.host_str().is_none()
                && u.to_file_path().is_ok()
                && u.query().is_none()
                && u.fragment().is_none(),
            "invalid local metrics snapshot"
        );
        return Ok(());
    }
    anyhow::ensure!(
        matches!(u.scheme(), "http" | "https")
            && u.host_str().is_some()
            && u.username().is_empty()
            && u.password().is_none()
            && u.query().is_none()
            && u.fragment().is_none(),
        "metrics URL must be credential-free HTTP(S)"
    );
    Ok(())
}
fn transparent_targets(config: &TransparentConfig) -> Result<Vec<Target>> {
    let bytes = std::fs::read(&config.roster)?;
    anyhow::ensure!(bytes.len() <= 64 * 1024, "roster too large");
    let roster: Vec<Worker> = serde_json::from_slice(&bytes)?;
    anyhow::ensure!(
        !roster.is_empty() && roster.len() <= 32,
        "roster must contain 1..32 workers"
    );
    let mut ids = std::collections::BTreeSet::new();
    let mut targets = vec![Target {
        id: "transparent-publisher".into(),
        service: "transparent",
        role: "publisher".into(),
        url: config.publisher_url.clone(),
        ready: None,
        status_operation: None,
        endpoint: None,
    }];
    for w in roster {
        anyhow::ensure!(
            w.id.len() <= 80
                && w.id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                && ids.insert(w.id.clone())
                && matches!(w.role.as_str(), "archive-owner" | "recent-replica"),
            "invalid worker identity or role"
        );
        let origin = format!("http://{}", w.upstream);
        let u = reqwest::Url::parse(&origin)?;
        anyhow::ensure!(u.path() == "/", "worker upstream must be host:port");
        targets.push(Target {
            id: w.id,
            service: "transparent",
            role: w.role,
            url: format!("{origin}/metrics"),
            ready: Some(format!("{origin}/v1/ready")),
            status_operation: None,
            endpoint: None,
        });
    }
    if let Some(url) = &config.router_metrics_url {
        targets.push(Target {
            id: "transparent-edge".into(),
            service: "transparent",
            role: "edge".into(),
            url: url.clone(),
            ready: None,
            status_operation: None,
            endpoint: None,
        });
    }
    for target in &targets {
        validate_url(&target.url)?;
    }
    Ok(targets)
}
async fn fetch(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, &'static str> {
    if url.starts_with("file:") {
        let path = reqwest::Url::parse(url)
            .map_err(|_| "invalid snapshot URL")?
            .to_file_path()
            .map_err(|_| "invalid snapshot path")?;
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|_| "snapshot unavailable")?;
        if bytes.len() > 2 * 1024 * 1024 {
            return Err("telemetry exceeds limit");
        }
        return Ok(bytes);
    }
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|_| "transport unavailable")?
        .error_for_status()
        .map_err(|_| "HTTP failure")?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| "truncated response")? {
        if bytes.len() + chunk.len() > 2 * 1024 * 1024 {
            return Err("telemetry exceeds limit");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
fn status_read(bytes: &[u8], operation: &str, at: u64) -> Result<Reading, &'static str> {
    let v: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| "invalid Status JSON")?;
    if v["http_observation_version"].as_u64() != Some(1) {
        return Err("unsupported Status observation version");
    }
    let incarnation = v["http_instance"]
        .as_str()
        .filter(|v| !v.is_empty())
        .ok_or("missing Status incarnation")?;
    let op = &v["operations"][operation];
    let mut r = Reading {
        at,
        incarnation: Some(incarnation.into()),
        ..Default::default()
    };
    for key in [
        "arrivals",
        "successes",
        "failures",
        "server_errors",
        "upload_bytes",
        "download_bytes",
    ] {
        r.values.counters.insert(
            format!("status_http_{key}_total"),
            op[key].as_u64().ok_or("missing Status HTTP counters")? as f64,
        );
    }
    let successes = op["successes"].as_u64().unwrap();
    let failures = op["failures"].as_u64().unwrap();
    let completed = successes
        .checked_add(failures)
        .ok_or("invalid Status counters")?;
    if op["server_errors"].as_u64().unwrap() > failures
        || op["arrivals"].as_u64().unwrap() < completed
    {
        return Err("inconsistent Status counters");
    }
    let bins = op["buckets"]
        .as_array()
        .filter(|v| v.len() == 12)
        .ok_or("missing Status histogram")?;
    let mut count = 0u64;
    let mut counts = Vec::new();
    for n in bins {
        count = count
            .checked_add(n.as_u64().ok_or("invalid Status bucket")?)
            .ok_or("Status histogram overflow")?;
        counts.push(count as f64);
    }
    if count != completed {
        return Err("inconsistent Status histogram");
    }
    r.values.histograms.insert(
        "status_http_duration_seconds".into(),
        Histogram {
            bounds: vec![
                Some(0.005),
                Some(0.01),
                Some(0.025),
                Some(0.05),
                Some(0.075),
                Some(0.1),
                Some(0.2),
                Some(0.5),
                Some(1.),
                Some(2.),
                Some(5.),
                None,
            ],
            counts,
        },
    );
    for key in [
        "host_memory_total_bytes",
        "host_memory_available_bytes",
        "process_rss_bytes",
        "gpu_utilization_percent",
        "gpu_memory_used_mib",
        "gpu_memory_total_mib",
    ] {
        if let Some(value) = v["resources"][key].as_u64() {
            r.values.gauges.insert(key.into(), value as f64);
            if let Some(name) = key.strip_prefix("host_") {
                r.values
                    .gauges
                    .insert(format!("pir_host_{name}"), value as f64);
            }
        }
    }
    Ok(r)
}

fn probe_read(bytes: &[u8], service: &str, now: u64) -> Result<Reading, &'static str> {
    let v: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| "invalid probe snapshot")?;
    let p = &v["services"][service];
    let at = p["sampled_at"].as_u64().ok_or("probe unavailable")?;
    if at == 0 || at > now || now - at > 90 {
        return Err("stale probe");
    }
    let mut r = Reading {
        at,
        ..Default::default()
    };
    for key in [
        "duration_seconds",
        "successes",
        "failures",
        "consecutive_failures",
        "consecutive_slow",
    ] {
        if let Some(n) = p[key].as_f64().filter(|n| n.is_finite() && *n >= 0.) {
            r.values.gauges.insert(format!("probe_{key}"), n);
        }
    }
    let category = p["category"].as_str().ok_or("invalid probe result")?;
    r.values
        .gauges
        .insert("probe_passed".into(), f64::from(category.is_empty()));
    r.values.gauges.insert(
        "probe_answer_mismatch".into(),
        f64::from(category == "answer_mismatch"),
    );
    r.values.gauges.insert(
        "probe_oracle_invalid".into(),
        f64::from(category == "oracle_invalid"),
    );
    Ok(r)
}

pub fn start(dashboard: SharedDashboard, config: &Config) -> Result<()> {
    let path = std::env::var_os("PIR_APM_HISTORY_PATH")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("PIR_APM_STATE_PATH")
                .map(PathBuf::from)
                .map(|p| p.with_file_name("quality.sqlite"))
        });
    let Some(path) = path else {
        return Ok(());
    };
    let transparent = std::env::var_os("PIR_APM_TRANSPARENT_CONFIG")
        .map(|p| -> Result<TransparentConfig> { Ok(serde_json::from_slice(&std::fs::read(p)?)?) })
        .transpose()?;
    let mut transparent_inventory = transparent
        .as_ref()
        .map(transparent_targets)
        .transpose()?
        .unwrap_or_default();
    if let Some(t) = &transparent {
        anyhow::ensure!(
            t.public_origins.len() <= 2,
            "at most two public map origins are supported"
        );
        for origin in &t.public_origins {
            validate_url(origin)?;
        }
    }
    let mut targets = vec![Target {
        id: "enhance-init".into(),
        service: "enhance",
        role: "init".into(),
        url: format!("{}{}", config.scrape_url, config.metrics_path),
        ready: None,
        status_operation: None,
        endpoint: Some("init"),
    }];
    let queries = if config.query_scrape_urls.is_empty() {
        vec![format!("{}{}", config.scrape_url, config.metrics_path)]
    } else {
        config.query_scrape_urls.clone()
    };
    for (i, url) in queries.into_iter().enumerate() {
        targets.push(Target {
            id: format!("enhance-query-{i}"),
            service: "enhance",
            role: "query".into(),
            url,
            ready: None,
            status_operation: None,
            endpoint: Some("query"),
        });
    }
    for (id, url, operation) in [
        ("status-init", &config.status_apm_url, "init"),
        ("status-query", &config.status_router_url, "router_query"),
    ] {
        if let Some(url) = url {
            targets.push(Target {
                id: id.into(),
                service: "status",
                role: if operation == "init" { "init" } else { "query" }.into(),
                url: url.clone(),
                ready: None,
                status_operation: Some(operation),
                endpoint: None,
            });
        }
    }
    for t in &targets {
        validate_url(&t.url)?;
    }
    if let Ok(url) = std::env::var("PIR_APM_SERVICE_MONITOR_URL") {
        validate_url(&url)?;
        for service in ["status", "transparent"] {
            targets.push(Target {
                id: format!("{service}-independent-probe"),
                service,
                role: "independent-probe".into(),
                url: url.clone(),
                ready: None,
                status_operation: Some(service),
                endpoint: None,
            });
        }
    }
    let worker_config = config.worker_config.clone();
    let packing_config = config.packing_router_config.clone();
    let mut db = store::Store::open(&path).context("opening service-quality database")?;
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Vec<(String, String, Point)>>(128);
    let failed = Arc::new(AtomicBool::new(false));
    let persisted = Arc::new(AtomicU64::new(0));
    let write_failed = failed.clone();
    let written = persisted.clone();
    std::thread::Builder::new()
        .name("pir-quality-store".into())
        .spawn(move || {
            let mut pruned = 0;
            while let Some(batch) = receiver.blocking_recv() {
                let now = unix_time();
                let outcome = (|| -> Result<()> {
                    for (service, source, point) in batch {
                        db.record(&service, &source, &point)?;
                    }
                    if now / 60 != pruned {
                        db.prune(now)?;
                        pruned = now / 60;
                    }
                    Ok(())
                })();
                write_failed.store(outcome.is_err(), Ordering::Relaxed);
                if outcome.is_ok() {
                    written.store(now, Ordering::Relaxed);
                }
            }
        })?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    if let Some(t) = transparent.clone() {
        let dashboard = dashboard.clone();
        let client = client.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(15));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let result = publication::check(&client, &t.publisher_url, &t.public_origins).await;
                dashboard.write().await.quality.publication = result;
            }
        });
    }
    tokio::spawn(async move {
        {
            let mut d = dashboard.write().await;
            d.quality.enabled = true;
            d.quality.path = Some(path);
        }
        let mut previous: BTreeMap<String, Reading> = BTreeMap::new();
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut inventory_at = 0;
        let mut fleet_targets = Vec::new();
        let mut last_hosts: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        loop {
            tick.tick().await;
            let now = unix_time();
            if now.saturating_sub(inventory_at) >= 15 {
                let discovered = (|| -> Result<Vec<Target>> {
                    let mut fleet = Vec::new();
                    if let Some(path) = &worker_config {
                        for (id, w) in crate::fleet::inventory(&std::fs::read_to_string(path)?)
                            .map_err(anyhow::Error::msg)?
                        {
                            fleet.push(Target {
                                id,
                                service: "enhance",
                                role: "worker".into(),
                                url: format!("{}/internal/metrics", w.url),
                                ready: None,
                                status_operation: None,
                                endpoint: None,
                            });
                        }
                    }
                    if let Some(path) = &packing_config {
                        for (id, r) in
                            crate::packing_fleet::inventory(&std::fs::read_to_string(path)?)
                                .map_err(anyhow::Error::msg)?
                        {
                            fleet.push(Target {
                                id,
                                service: "enhance",
                                role: "packing-router".into(),
                                url: format!("{}/internal/metrics", r.sample.url),
                                ready: None,
                                status_operation: None,
                                endpoint: None,
                            });
                        }
                    }
                    Ok(fleet)
                })();
                let mut inventory_error = discovered.is_err();
                if let Ok(fleet) = discovered {
                    fleet_targets = fleet;
                }
                if let Some(t) = &transparent {
                    match transparent_targets(t) {
                        Ok(v) => {
                            transparent_inventory = v;
                        }
                        Err(_) => inventory_error = true,
                    }
                }
                dashboard.write().await.quality.inventory_error = inventory_error;
                inventory_at = now;
            }
            let all = targets
                .iter()
                .chain(&transparent_inventory)
                .chain(&fleet_targets)
                .cloned()
                .collect::<Vec<_>>();
            let hosts: BTreeMap<String, serde_json::Value> =
                if let Some(path) = transparent.as_ref().and_then(|t| t.host_snapshot.as_ref()) {
                    tokio::fs::read(path)
                        .await
                        .ok()
                        .filter(|b| b.len() <= 262144)
                        .and_then(|b| serde_json::from_slice(&b).ok())
                        .unwrap_or_else(|| last_hosts.clone())
                } else {
                    BTreeMap::new()
                };
            last_hosts = hosts.clone();
            let sem = Arc::new(tokio::sync::Semaphore::new(8));
            let mut tasks = tokio::task::JoinSet::new();
            for target in all.clone() {
                let client = client.clone();
                let sem = sem.clone();
                let host = hosts.get(&target.id).cloned();
                tasks.spawn(async move {
                    let _permit = sem.acquire_owned().await.unwrap();
                    let result = async {
                        let bytes = fetch(&client, &target.url).await?;
                        let mut reading = if let Some(operation) = target.status_operation {
                            if matches!(operation, "transparent" | "status") {
                                probe_read(&bytes, operation, now)?
                            } else {
                                status_read(&bytes, operation, now)?
                            }
                        } else {
                            data::parse(
                                std::str::from_utf8(&bytes).map_err(|_| "invalid metrics text")?,
                                now,
                            )?
                        };
                        if let Some(endpoint) = target.endpoint {
                            let retain = |k: &String| {
                                !k.contains("endpoint=")
                                    || k.split('|').nth(1).is_some_and(|s| {
                                        s.split(',').any(|x| x == format!("endpoint={endpoint}"))
                                    })
                            };
                            reading.values.counters.retain(|k, _| retain(k));
                            reading.values.histograms.retain(|k, _| retain(k));
                            reading.values.gauges.retain(|k, _| retain(k));
                        }
                        if let Some(host) = &host {
                            let fresh = host["at"]
                                .as_u64()
                                .is_some_and(|at| at <= now && now - at <= 45);
                            reading
                                .values
                                .gauges
                                .insert("pir_host_observation_available".into(), f64::from(fresh));
                            if fresh {
                                for key in [
                                    "cpu_seconds",
                                    "memory_available_bytes",
                                    "memory_total_bytes",
                                    "cgroup_memory_bytes",
                                    "oom_kills",
                                    "restarts",
                                    "disk_available_bytes",
                                    "disk_total_bytes",
                                ] {
                                    if let Some(n) = host[key].as_u64() {
                                        reading
                                            .values
                                            .gauges
                                            .insert(format!("pir_host_{key}"), n as f64);
                                    }
                                }
                            }
                        }
                        Ok::<_, &'static str>(reading)
                    }
                    .await;
                    let ready = if let Some(url) = &target.ready {
                        match fetch(&client, url).await {
                            Ok(b) => serde_json::from_slice::<serde_json::Value>(&b).ok(),
                            Err(_) => None,
                        }
                    } else {
                        None
                    };
                    (target, result, ready)
                });
            }
            let mut batch = Vec::new();
            while let Some(result) = tasks.join_next().await {
                let Ok((target, result, ready)) = result else {
                    continue;
                };
                let mut d = dashboard.write().await;
                let source = d.quality.sources.entry(target.id.clone()).or_default();
                source.service = target.service.into();
                source.role = target.role;
                source.ready = ready.as_ref().and_then(|v| v["ready"].as_bool());
                source.publication = ready
                    .as_ref()
                    .and_then(|v| v["map_sha256"].as_str())
                    .filter(|v| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit()))
                    .map(String::from);
                match result {
                    Ok(reading) => {
                        if previous.get(&target.id).is_some_and(|p| p.at == reading.at) {
                            source.error = None;
                            continue;
                        }
                        let point = reading.delta(previous.get(&target.id));
                        source.sampled_at = reading.at;
                        source.error = None;
                        source.current = reading.values.clone();
                        source.recent.push_back(point.clone());
                        while source
                            .recent
                            .front()
                            .is_some_and(|p| p.at < now.saturating_sub(300))
                        {
                            source.recent.pop_front();
                        }
                        batch.push((target.service.into(), target.id.clone(), point));
                        previous.insert(target.id, reading);
                    }
                    Err(error) => {
                        source.error = Some(error.into());
                        previous.remove(&target.id);
                        batch.push((
                            target.service.into(),
                            target.id,
                            Point {
                                at: now,
                                seconds: 5,
                                discontinuities: 1,
                                ..Default::default()
                            },
                        ));
                    }
                }
            }
            if let Some(t) = &transparent {
                if let Some(path) = &t.synthetic_status {
                    let value = read_synthetic(path);
                    if let Some(v) = value.as_ref().filter(|v| v["fresh"] == true) {
                        let at =
                            chrono::DateTime::parse_from_rfc3339(v["utc"].as_str().unwrap_or(""))
                                .map(|d| d.timestamp().max(0) as u64)
                                .unwrap_or(0);
                        let mut reading = Reading {
                            at,
                            incarnation: v["started_unix"].as_f64().map(|n| n.to_string()),
                            ..Default::default()
                        };
                        for group in ["counts", "trailing_60s"] {
                            if let Some(fields) = v[group].as_object() {
                                for (key, value) in fields {
                                    if let Some(n) = value.as_f64() {
                                        if group == "counts" {
                                            reading
                                                .values
                                                .counters
                                                .insert(format!("synthetic_{key}_total"), n);
                                        } else {
                                            reading
                                                .values
                                                .gauges
                                                .insert(format!("synthetic_{key}"), n);
                                        }
                                    }
                                }
                            }
                        }
                        if previous
                            .get("transparent-synthetic")
                            .is_none_or(|p| p.at != at)
                        {
                            let point = reading.delta(previous.get("transparent-synthetic"));
                            batch.push((
                                "transparent".into(),
                                "transparent-synthetic".into(),
                                point,
                            ));
                            previous.insert("transparent-synthetic".into(), reading);
                        }
                    } else {
                        previous.remove("transparent-synthetic");
                        batch.push((
                            "transparent".into(),
                            "transparent-synthetic".into(),
                            Point {
                                at: now,
                                seconds: 5,
                                discontinuities: 1,
                                ..Default::default()
                            },
                        ));
                    }
                    dashboard.write().await.quality.synthetic = value;
                }
            }
            let mut d = dashboard.write().await;
            if sender.try_send(batch).is_err() {
                d.quality.dropped_batches += 1;
                d.quality.last_dropped_at = now;
            }
            d.quality.persisted_at = persisted.load(Ordering::Relaxed);
            d.quality.storage_error = failed.load(Ordering::Relaxed);
            d.quality.collected_at = now;
            d.quality
                .sources
                .retain(|name, _| all.iter().any(|t| t.id == *name));
            previous.retain(|name, _| {
                name == "transparent-synthetic" || all.iter().any(|t| t.id == *name)
            });
        }
    });
    Ok(())
}

fn read_synthetic(path: &std::path::Path) -> Option<serde_json::Value> {
    let bytes = std::fs::read(path).ok()?;
    if bytes.len() > 256 * 1024 {
        return None;
    }
    let raw: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    let mut output = serde_json::Map::new();
    if let Some(n) = raw["started_unix"]
        .as_f64()
        .filter(|n| n.is_finite() && *n >= 0.)
    {
        output.insert("started_unix".into(), serde_json::json!(n));
    }
    if let Some(utc) = raw["utc"].as_str().filter(|s| s.len() < 40) {
        output.insert("utc".into(), utc.into());
    }
    if let Some(mode) = raw["mode"].as_str().filter(|s| {
        matches!(
            *s,
            "running" | "paused" | "starting" | "latched" | "stopped"
        )
    }) {
        output.insert("mode".into(), mode.into());
    }
    for group in ["counts", "trailing_60s"] {
        let mut fields = serde_json::Map::new();
        for key in [
            "query",
            "error",
            "missed_slot",
            "cancelled_slot",
            "http_attempts",
            "recovered",
            "paused",
            "exact",
            "errors",
            "missed_slots",
            "completed_qps",
            "http_p50_seconds",
            "http_p95_seconds",
            "http_p99_seconds",
            "max_schedule_lag_seconds",
        ] {
            if let Some(n) = raw[group][key]
                .as_f64()
                .filter(|n| n.is_finite() && *n >= 0.)
            {
                fields.insert(key.into(), serde_json::json!(n));
            }
        }
        output.insert(group.into(), fields.into());
    }
    let at = raw["utc"]
        .as_str()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.timestamp().max(0) as u64);
    let now = unix_time();
    output.insert(
        "fresh".into(),
        serde_json::json!(at.is_some_and(|at| at <= now && now - at <= 45)),
    );
    Some(output.into())
}

pub fn conditions(view: &View, now: u64) -> Vec<Condition> {
    if !view.enabled {
        return vec![];
    }
    let mut conditions = Vec::new();
    let mut add = |key: String,
                   resource: String,
                   firing: Option<bool>,
                   severity: &str,
                   hold: u64,
                   sample: u64,
                   observed: String,
                   threshold: &str| {
        conditions.push(Condition {
            retired: false,
            key: format!("quality_{key}"),
            resource,
            severity: severity.into(),
            firing,
            observed,
            threshold: threshold.into(),
            hold_seconds: hold,
            sample,
        })
    };
    add(
        "storage".into(),
        "history".into(),
        Some(
            view.storage_error
                || (view.collected_at > 0
                    && (view.persisted_at == 0 || now.saturating_sub(view.persisted_at) > 60)),
        ),
        "critical",
        60,
        now,
        "Seven-day metrics persistence".into(),
        "history writes unavailable for 60s",
    );
    add(
        "dropped_history".into(),
        "history".into(),
        Some(view.last_dropped_at > 0 && now.saturating_sub(view.last_dropped_at) <= 60),
        "warning",
        0,
        now,
        format!("{} batches dropped", view.dropped_batches),
        "history writer queue dropped a batch in the last minute",
    );
    add(
        "inventory".into(),
        "transparent".into(),
        Some(view.inventory_error),
        "warning",
        45,
        now,
        "Service fleet inventory".into(),
        "invalid inventory for 45s",
    );
    let publication_fresh =
        view.publication.sampled_at > 0 && now.saturating_sub(view.publication.sampled_at) <= 45;
    if view.sources.values().any(|s| s.service == "transparent") {
        for (key, verdict) in [
            ("canonical", view.publication.canonical),
            ("origins", view.publication.origins_agree),
        ] {
            add(
                format!("publication_{key}"),
                "transparent".into(),
                verdict.filter(|_| publication_fresh).map(|v| !v),
                "critical",
                if key == "canonical" { 0 } else { 30 },
                view.publication.sampled_at,
                "Independent public publication check".into(),
                "public anchor must be canonical; both origins must agree",
            );
            add(
                format!("publication_{key}_coverage"),
                "transparent".into(),
                Some(!publication_fresh || verdict.is_none()),
                "warning",
                45,
                now,
                "Publication verification coverage".into(),
                "verification unavailable for 45s",
            );
        }
    }
    for (name, s) in &view.sources {
        let fresh = s.fresh(now);
        add(
            format!("coverage_{name}"),
            name.clone(),
            Some(!fresh),
            "warning",
            45,
            now,
            if fresh { "fresh" } else { "unavailable" }.into(),
            "source unavailable for 45s",
        );
        let gauge = |key: &str| s.current.gauges.get(key).copied().filter(|_| fresh);
        if s.current
            .gauges
            .contains_key("pir_host_observation_available")
        {
            add(
                format!("host_coverage_{name}"),
                name.clone(),
                Some(gauge("pir_host_observation_available") != Some(1.)),
                "warning",
                45,
                now,
                "Host resource observation coverage".into(),
                "host observation unavailable for 45s",
            );
        }
        for (kind, available, total, limit) in [
            (
                "memory",
                "pir_host_memory_available_bytes",
                "pir_host_memory_total_bytes",
                0.15,
            ),
            (
                "disk",
                "pir_host_disk_available_bytes",
                "pir_host_disk_total_bytes",
                0.10,
            ),
        ] {
            let ratio = gauge(available)
                .zip(gauge(total))
                .filter(|(_, t)| *t > 0.)
                .map(|(a, t)| a / t);
            add(
                format!("{kind}_{name}"),
                name.clone(),
                ratio.map(|r| r < limit),
                "warning",
                120,
                s.sampled_at,
                ratio
                    .map(|r| format!("{:.1}% available", r * 100.))
                    .unwrap_or_else(|| "unavailable".into()),
                "memory <15% or disk <10% available for 120s",
            );
        }
        let oom = s
            .recent
            .front()
            .and_then(|p| p.values.gauges.get("pir_host_oom_kills"))
            .copied()
            .zip(gauge("pir_host_oom_kills"))
            .map(|(old, new)| new > old);
        add(
            format!("oom_{name}"),
            name.clone(),
            oom,
            "critical",
            0,
            s.sampled_at,
            "OOM kill counter change".into(),
            "new OOM kills in the observation window",
        );
        if s.service != "transparent" {
            continue;
        }
        if matches!(
            s.role.as_str(),
            "publisher" | "recent-replica" | "archive-owner"
        ) {
            add(
                format!("http_coverage_{name}"),
                name.clone(),
                Some(!fresh || gauge("pir_http_observation_version") != Some(1.)),
                "warning",
                45,
                now,
                "Outer HTTP observation capability".into(),
                "HTTP observation version 1 unavailable for 45s",
            );
        }
        let window = s.window(now);
        let outcomes: Vec<_> = window
            .values
            .counters
            .iter()
            .filter(|(k, _)| k.starts_with("pir_http_responses_total|"))
            .collect();
        let mut completed = outcomes.iter().map(|(_, v)| **v).sum::<f64>();
        let mut errors = outcomes
            .iter()
            .filter(|(k, _)| k.contains("code=5xx"))
            .map(|(_, v)| **v)
            .sum::<f64>();
        let mut known = fresh && window.observed_seconds > 0 && !outcomes.is_empty();
        if s.role == "edge" {
            let requests = window
                .values
                .counters
                .iter()
                .filter(|(k, _)| k.starts_with("caddy_http_requests_total|"))
                .collect::<Vec<_>>();
            completed = requests.iter().map(|(_, v)| **v).sum();
            errors = window
                .values
                .counters
                .iter()
                .filter(|(k, _)| {
                    k.starts_with("caddy_http_request_errors_total|")
                        || (k.starts_with("caddy_http_request_duration_seconds_count|")
                            && k.contains("code=5"))
                })
                .map(|(_, v)| *v)
                .sum();
            known =
                fresh && window.observed_seconds > 0 && !requests.is_empty() && errors <= completed;
        }
        add(
            format!("http_errors_{name}"),
            name.clone(),
            known.then_some(completed >= 10. && errors / completed > 0.05),
            "warning",
            0,
            s.sampled_at,
            format!("{errors:.0} server errors / {completed:.0} responses"),
            "5xx >5% with at least 10 responses in 5m",
        );
        for (key, h) in &window.values.histograms {
            if !key.starts_with("pir_http_duration_seconds|") {
                continue;
            }
            let endpoint = key.split("endpoint=").nth(1).unwrap_or("unknown");
            let budget = if endpoint.starts_with("query") || endpoint == "setup" {
                5.
            } else {
                2.
            };
            let count = h.counts.last().copied().unwrap_or(0.);
            let exceeds = if count < 20. {
                false
            } else {
                h.quantile(0.99).is_none_or(|p| p > budget)
            };
            add(
                format!("latency_{name}_{endpoint}"),
                name.clone(),
                known.then_some(exceeds),
                "warning",
                120,
                s.sampled_at,
                format!(
                    "{endpoint} p99 {}s",
                    h.quantile(0.99)
                        .map(|n| format!("{n:.3}"))
                        .unwrap_or_else(|| "overflow/unavailable".into())
                ),
                "p99 metadata >2s or setup/query >5s, >=20 samples, 120s",
            );
        }
        if s.role == "archive-owner" {
            add(
                format!("ready_{name}"),
                name.clone(),
                (fresh && publication_fresh && view.publication.map_sha256.is_some()).then_some(
                    s.ready != Some(true) || s.publication != view.publication.map_sha256,
                ),
                "critical",
                60,
                s.sampled_at,
                "Archive owner readiness".into(),
                "archive coverage unavailable for 60s",
            );
        }
        if s.role == "publisher" {
            let age = s
                .current
                .gauges
                .get("transparent_publication_oldest_unpublished_seconds")
                .copied()
                .filter(|_| fresh);
            for (level, budget, hold) in [("warning", 30., 0), ("critical", 60., 60)] {
                add(
                    format!("publication_{level}"),
                    name.clone(),
                    age.map(|v| v > budget),
                    level,
                    hold,
                    s.sampled_at,
                    age.map(|v| format!("{v:.1}s unpublished"))
                        .unwrap_or_else(|| "unknown".into()),
                    if budget == 30. {
                        "unpublished age >30s"
                    } else {
                        "unpublished age >60s for 60s"
                    },
                );
            }
            let withdrawn = s
                .current
                .gauges
                .get("transparent_publication_withdrawn")
                .copied()
                .filter(|_| fresh);
            add(
                "withdrawn".into(),
                name.clone(),
                withdrawn.map(|v| v == 1.),
                "critical",
                60,
                s.sampled_at,
                "Public coverage withdrawn".into(),
                "withdrawn for 60s",
            );
        }
    }
    let recent: Vec<_> = view
        .sources
        .values()
        .filter(|s| s.role == "recent-replica")
        .collect();
    if !recent.is_empty() {
        let known = publication_fresh
            && view.publication.map_sha256.is_some()
            && recent.iter().all(|s| s.fresh(now));
        let ready = recent
            .iter()
            .filter(|s| s.ready == Some(true) && s.publication == view.publication.map_sha256)
            .count();
        add(
            "recent_availability".into(),
            "transparent".into(),
            known.then_some(ready == 0),
            "critical",
            60,
            now,
            format!("{ready} ready replicas"),
            "no recent replica for 60s",
        );
        add(
            "recent_redundancy".into(),
            "transparent".into(),
            known.then_some(ready < 2),
            "warning",
            120,
            now,
            format!("{ready} ready replicas"),
            "fewer than two recent replicas for 120s",
        );
    }
    conditions
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_http_capability_is_unknown_and_has_coverage_alert() {
        let mut view = View {
            enabled: true,
            ..Default::default()
        };
        view.sources.insert(
            "worker".into(),
            Source {
                service: "transparent".into(),
                role: "recent-replica".into(),
                sampled_at: 100,
                ..Default::default()
            },
        );
        let c = conditions(&view, 110);
        assert_eq!(
            c.iter()
                .find(|c| c.key == "quality_http_errors_worker")
                .unwrap()
                .firing,
            None
        );
        assert_eq!(
            c.iter()
                .find(|c| c.key == "quality_http_coverage_worker")
                .unwrap()
                .firing,
            Some(true)
        );
    }
    #[test]
    fn http_ratio_counts_server_errors_and_stale_samples_cannot_recover() {
        let mut source = Source {
            service: "transparent".into(),
            role: "recent-replica".into(),
            sampled_at: 100,
            ..Default::default()
        };
        let mut p = Point {
            at: 100,
            observed_seconds: 5,
            ..Default::default()
        };
        p.values.counters.insert(
            "pir_http_responses_total|code=2xx,endpoint=query_pages".into(),
            18.,
        );
        p.values.counters.insert(
            "pir_http_responses_total|code=5xx,endpoint=query_pages".into(),
            2.,
        );
        source.recent.push_back(p);
        let view = View {
            enabled: true,
            sources: BTreeMap::from([("worker".into(), source)]),
            ..Default::default()
        };
        assert_eq!(
            conditions(&view, 110)
                .iter()
                .find(|c| c.key == "quality_http_errors_worker")
                .unwrap()
                .firing,
            Some(true)
        );
        assert_eq!(
            conditions(&view, 160)
                .iter()
                .find(|c| c.key == "quality_http_errors_worker")
                .unwrap()
                .firing,
            None
        );
    }
    #[test]
    fn independent_probe_rejects_stale_and_does_not_expose_raw_output() {
        let bytes=br#"{"services":{"transparent":{"sampled_at":100,"duration_seconds":0.5,"category":"answer_mismatch","private_row":"never retain"}}}"#;
        let r = probe_read(bytes, "transparent", 110).unwrap();
        assert_eq!(r.values.gauges["probe_answer_mismatch"], 1.);
        assert!(!serde_json::to_string(&r.values)
            .unwrap()
            .contains("private_row"));
        assert!(probe_read(bytes, "transparent", 200).is_err());
    }
}
