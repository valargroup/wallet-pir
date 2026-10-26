//! Independent aggregate collectors and seven-day service-quality history.
mod capacity;
mod data;
mod publication;
mod rules;
mod services;
mod store;
mod view;
use crate::{config::Config, dashboard::SharedDashboard};
use anyhow::{Context, Result};
pub use capacity::page as capacity_page;
use data::{Histogram, Point, Reading};
use pir_apm::incidents::unix_time;
pub use rules::{conditions, retire, Policy};
use serde::Serialize;
use services::validate_url;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
pub use view::{api, page, summary, tabs, worker_page};

#[derive(Clone, Debug, Default, Serialize)]
pub struct Source {
    pub publication: Option<String>,
    pub service: String,
    pub role: String,
    /// Host id from the host snapshot; several sources can share one host.
    pub host: Option<String>,
    pub sampled_at: u64,
    pub error: Option<String>,
    pub ready: Option<bool>,
    pub current: data::Values,
    #[serde(skip)]
    pub recent: VecDeque<Point>,
    /// Rebuild and prepare timings only, kept for the fifteen-minute rules.
    #[serde(skip)]
    pub long: VecDeque<(u64, BTreeMap<String, Histogram>)>,
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
    /// Rebuild and prepare histograms merged over the last fifteen minutes.
    /// A histogram whose buckets changed inside the window is dropped.
    pub fn long_window(&self, now: u64) -> BTreeMap<String, Histogram> {
        let mut merged: BTreeMap<String, Histogram> = BTreeMap::new();
        let mut invalid = BTreeSet::new();
        for (at, histograms) in &self.long {
            if *at <= now.saturating_sub(rules::LONG_WINDOW) {
                continue;
            }
            for (key, h) in histograms {
                if invalid.contains(key) {
                    continue;
                }
                match merged.get_mut(key) {
                    Some(old) => {
                        if !old.merge(h) {
                            merged.remove(key);
                            invalid.insert(key.clone());
                        }
                    }
                    None => {
                        merged.insert(key.clone(), h.clone());
                    }
                }
            }
        }
        merged
    }
}

/// One configured Transparent service as the rules and pages see it.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ServiceView {
    pub name: String,
    pub kind: services::Kind,
    pub path_prefix: String,
    pub public_origins: Vec<String>,
    /// Ready recent replicas a history service needs before redundancy is met.
    pub min_recent_replicas: usize,
    pub shadow: bool,
    pub inventory_error: bool,
    pub publication: publication::View,
    pub synthetic: Option<serde_json::Value>,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct View {
    pub services: Vec<ServiceView>,
    pub enabled: bool,
    pub collected_at: u64,
    pub persisted_at: u64,
    pub storage_error: bool,
    pub dropped_batches: u64,
    pub last_dropped_at: u64,
    /// The Enhance worker and packing-router inventories.
    pub inventory_error: bool,
    /// `PIR_APM_TRANSPARENT_CONFIG` failed to read, parse or validate at start,
    /// so Transparent monitoring is off while Enhance and Status continue.
    pub transparent_config_error: bool,
    pub sources: BTreeMap<String, Source>,
    pub capacity: capacity::View,
    #[serde(skip)]
    pub path: Option<PathBuf>,
}
impl View {
    pub fn service(&self, name: &str) -> Option<&ServiceView> {
        self.services.iter().find(|s| s.name == name)
    }
    fn service_mut(&mut self, name: &str) -> Option<&mut ServiceView> {
        self.services.iter_mut().find(|s| s.name == name)
    }
}

#[derive(Clone)]
struct Target {
    id: String,
    service: String,
    role: String,
    url: String,
    ready: Option<String>,
    status_operation: Option<&'static str>,
    endpoint: Option<&'static str>,
}

/// Histograms the fifteen-minute rules read; everything else stays in the
/// five-minute window and the minute history.
const LONG_HISTOGRAMS: [&str; 4] = [
    "transparent_publication_cycle_seconds",
    "txid_display_cycle_seconds",
    "transparent_shard_prepare_seconds",
    "transparent_display_prepare_seconds",
];

async fn fetch(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, &'static str> {
    fetch_observed(client, url).await.1
}
/// [`fetch`], also returning the HTTP status of any response received. The
/// error is a fixed reason; no response body or URL is ever included.
async fn fetch_observed(
    client: &reqwest::Client,
    url: &str,
) -> (Option<u16>, Result<Vec<u8>, &'static str>) {
    if url.starts_with("file:") {
        return (None, fetch_file(url).await);
    }
    let response = match client.get(url).send().await {
        Ok(response) => response,
        Err(_) => return (None, Err("transport unavailable")),
    };
    let status = response.status().as_u16();
    match response.error_for_status() {
        Ok(response) => (Some(status), read_limited(response).await),
        Err(_) => (Some(status), Err("HTTP failure")),
    }
}
async fn fetch_file(url: &str) -> Result<Vec<u8>, &'static str> {
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
    Ok(bytes)
}
async fn read_limited(mut response: reqwest::Response) -> Result<Vec<u8>, &'static str> {
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

/// Host snapshot fields copied onto a source as `pir_host_*` gauges.
const HOST_GAUGES: [&str; 10] = [
    "cpu_seconds",
    "memory_available_bytes",
    "memory_total_bytes",
    "cgroup_memory_bytes",
    "oom_kills",
    "restarts",
    "disk_available_bytes",
    "disk_total_bytes",
    "swap_total_bytes",
    "swap_free_bytes",
];

/// Current targets of every configured service. A service whose roster cannot be
/// read, or whose worker ids collide with an earlier service's, keeps its last
/// valid inventory and is marked.
fn refresh(
    configured: &[services::Service],
    inventory: &mut BTreeMap<String, Vec<Target>>,
) -> BTreeMap<String, bool> {
    let mut errors = BTreeMap::new();
    let mut ids = BTreeSet::new();
    for s in configured {
        let fresh = s.targets().ok().filter(|targets| {
            targets.iter().all(|t| {
                !ids.contains(&t.id) && targets.iter().filter(|o| o.id == t.id).count() == 1
            })
        });
        errors.insert(s.service.clone(), fresh.is_none());
        if let Some(targets) = fresh {
            inventory.insert(s.service.clone(), targets);
        }
        ids.extend(
            inventory
                .get(&s.service)
                .into_iter()
                .flatten()
                .map(|t| t.id.clone()),
        );
    }
    errors
}

type Inventory = BTreeMap<String, Vec<Target>>;

/// The Transparent config at `path` and each service's first inventory; None
/// without a path. Any failure to read, parse or validate is an error.
fn load_transparent(
    path: Option<std::ffi::OsString>,
) -> Result<Option<(services::Config, Inventory)>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let config = services::Config::parse(&std::fs::read(path)?)?;
    let mut inventory = BTreeMap::new();
    for s in &config.services {
        inventory.insert(s.service.clone(), s.targets()?);
    }
    anyhow::ensure!(
        !refresh(&config.services, &mut inventory)
            .values()
            .any(|e| *e),
        "Transparent services must not share worker ids"
    );
    Ok(Some((config, inventory)))
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
    // A bad Transparent config disables only Transparent monitoring: Enhance and
    // Status keep their alerts, and the shared family raises a warning.
    let (transparent, mut service_inventory, transparent_config_error) =
        match load_transparent(std::env::var_os("PIR_APM_TRANSPARENT_CONFIG")) {
            Ok(Some((c, inventory))) => (Some(c), inventory, false),
            Ok(None) => (None, BTreeMap::new(), false),
            Err(error) => {
                // Messages name fields and services, never file contents or URLs.
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "event": "transparent_config_invalid",
                        "variable": "PIR_APM_TRANSPARENT_CONFIG",
                        "error": format!("{error:#}"),
                        "effect": "Transparent monitoring disabled; Enhance and Status continue",
                    })
                );
                (None, BTreeMap::new(), true)
            }
        };
    let configured = transparent
        .as_ref()
        .map(|c| c.services.clone())
        .unwrap_or_default();
    let host_snapshot = transparent.as_ref().and_then(|c| c.host_snapshot.clone());
    let capacity_shadow = transparent
        .as_ref()
        .is_none_or(|c| c.capacity_alert_mode == services::Mode::Shadow);
    let policy = crate::monitoring::Policy::load()?.quality;
    let mut targets = Vec::new();
    // Query ingress serves both wallet entrypoints.
    let entrypoints = if config.query_scrape_urls.is_empty() {
        vec![format!("{}{}", config.scrape_url, config.metrics_path)]
    } else {
        config.query_scrape_urls.clone()
    };
    for (i, url) in entrypoints.into_iter().enumerate() {
        for endpoint in ["init", "query"] {
            targets.push(Target {
                id: format!("enhance-{endpoint}-{i}"),
                service: "enhance".into(),
                role: endpoint.into(),
                url: url.clone(),
                ready: None,
                status_operation: None,
                endpoint: Some(endpoint),
            });
        }
    }
    for (id, url, operation) in [
        ("status-init", &config.status_router_url, "init"),
        ("status-query", &config.status_router_url, "router_query"),
    ] {
        if let Some(url) = url {
            targets.push(Target {
                id: id.into(),
                service: "status".into(),
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
                service: service.into(),
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
    type Batch = (Vec<(String, String, Point)>, Vec<capacity::Sample>);
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Batch>(128);
    let failed = Arc::new(AtomicBool::new(false));
    let persisted = Arc::new(AtomicU64::new(0));
    let write_failed = failed.clone();
    let written = persisted.clone();
    std::thread::Builder::new()
        .name("pir-quality-store".into())
        .spawn(move || {
            let mut pruned = 0;
            while let Some((points, samples)) = receiver.blocking_recv() {
                let now = unix_time();
                let outcome = (|| -> Result<()> {
                    for (service, source, point) in points {
                        db.record(&service, &source, &point)?;
                    }
                    for sample in &samples {
                        db.record_capacity(sample)?;
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
    for service in configured.clone() {
        let dashboard = dashboard.clone();
        let client = client.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(15));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // The last two distinct published maps; an origin serving the
            // older one is lagging rather than disagreeing.
            let (mut last, mut before, mut seen_at) = (None, None, None);
            loop {
                tick.tick().await;
                let mut result = publication::check(&client, &service).await;
                if result.map_sha256.is_some() && result.map_sha256 != last {
                    before = std::mem::replace(&mut last, result.map_sha256.clone());
                    seen_at = Some(result.sampled_at);
                }
                result.previous_map = before.clone();
                result.map_seen_at = seen_at;
                if let Some(s) = dashboard
                    .write()
                    .await
                    .quality
                    .service_mut(&service.service)
                {
                    s.publication = result;
                }
            }
        });
    }
    let synthetic_ids: BTreeSet<String> = configured
        .iter()
        .filter(|s| s.synthetic_status.is_some())
        .map(|s| format!("{}-synthetic", s.service))
        .collect();
    tokio::spawn(async move {
        {
            let mut d = dashboard.write().await;
            d.quality.enabled = true;
            d.quality.transparent_config_error = transparent_config_error;
            d.quality.path = Some(path.clone());
            d.quality.capacity.shadow = capacity_shadow;
            d.quality.services = configured
                .iter()
                .map(|s| ServiceView {
                    name: s.service.clone(),
                    kind: s.kind,
                    path_prefix: s.path_prefix.clone(),
                    public_origins: s.public_origins.clone(),
                    min_recent_replicas: s.min_recent_replicas.unwrap_or(2) as usize,
                    shadow: s.alert_mode == services::Mode::Shadow,
                    ..Default::default()
                })
                .collect();
        }
        let mut previous: BTreeMap<String, Reading> = BTreeMap::new();
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut inventory_at = 0;
        let mut fleet_targets = Vec::new();
        let mut last_hosts: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        let mut host_seen: BTreeMap<String, u64> = BTreeMap::new();
        let mut recent_capacity = capacity::Recent::default();
        let mut capacity_at = 0;
        let mut capacity_rows = Vec::new();
        let mut capacity_error = false;
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
                                service: "enhance".into(),
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
                                service: "enhance".into(),
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
                let inventory_error = discovered.is_err();
                if let Ok(fleet) = discovered {
                    fleet_targets = fleet;
                }
                let service_errors = refresh(&configured, &mut service_inventory);
                let mut d = dashboard.write().await;
                d.quality.inventory_error = inventory_error;
                for s in &mut d.quality.services {
                    s.inventory_error = service_errors.get(&s.name).copied().unwrap_or(true);
                }
                inventory_at = now;
            }
            let all = targets
                .iter()
                .chain(service_inventory.values().flatten())
                .chain(&fleet_targets)
                .cloned()
                .collect::<Vec<_>>();
            let hosts: BTreeMap<String, serde_json::Value> = if let Some(path) = &host_snapshot {
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
                                for key in HOST_GAUGES {
                                    if let Some(n) = host[key].as_u64() {
                                        reading
                                            .values
                                            .gauges
                                            .insert(format!("pir_host_{key}"), n as f64);
                                    }
                                }
                                reading.values.gauges.extend(capacity::unit_ooms(host));
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
                    (target, result, ready, host)
                });
            }
            let mut batch = Vec::new();
            while let Some(result) = tasks.join_next().await {
                let Ok((target, result, ready, host)) = result else {
                    continue;
                };
                let mut d = dashboard.write().await;
                let source = d.quality.sources.entry(target.id.clone()).or_default();
                source.service = target.service.clone();
                source.role = target.role;
                source.host = host
                    .as_ref()
                    .map(|h| capacity::host_id(h, &target.id).to_owned());
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
                        let long: BTreeMap<_, _> = point
                            .values
                            .histograms
                            .iter()
                            .filter(|(k, _)| {
                                LONG_HISTOGRAMS.contains(&k.split('|').next().unwrap_or(""))
                            })
                            .map(|(k, h)| (k.clone(), h.clone()))
                            .collect();
                        if !long.is_empty() {
                            source.long.push_back((point.at, long));
                        }
                        while source
                            .long
                            .front()
                            .is_some_and(|(at, _)| *at <= now.saturating_sub(rules::LONG_WINDOW))
                        {
                            source.long.pop_front();
                        }
                        batch.push((target.service.clone(), target.id.clone(), point));
                        previous.insert(target.id, reading);
                    }
                    Err(error) => {
                        source.error = Some(error.into());
                        previous.remove(&target.id);
                        batch.push((
                            target.service.clone(),
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
            for s in &configured {
                let Some(path) = &s.synthetic_status else {
                    continue;
                };
                let id = format!("{}-synthetic", s.service);
                let value = read_synthetic(path);
                if let Some(v) = value.as_ref().filter(|v| v["fresh"] == true) {
                    let at = chrono::DateTime::parse_from_rfc3339(v["utc"].as_str().unwrap_or(""))
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
                                        reading.values.gauges.insert(format!("synthetic_{key}"), n);
                                    }
                                }
                            }
                        }
                    }
                    if previous.get(&id).is_none_or(|p| p.at != at) {
                        let point = reading.delta(previous.get(&id));
                        batch.push((s.service.clone(), id.clone(), point));
                        previous.insert(id, reading);
                    }
                } else {
                    previous.remove(&id);
                    batch.push((
                        s.service.clone(),
                        id,
                        Point {
                            at: now,
                            seconds: 5,
                            discontinuities: 1,
                            ..Default::default()
                        },
                    ));
                }
                if let Some(view) = dashboard.write().await.quality.service_mut(&s.service) {
                    view.synthetic = value;
                }
            }
            let samples = {
                let d = dashboard.read().await;
                capacity::samples(
                    &hosts,
                    |name| {
                        d.quality
                            .sources
                            .get(name)
                            .filter(|s| s.fresh(now))
                            .and_then(|s| {
                                s.current
                                    .gauges
                                    .get("transparent_shard_cache_resident_bytes")
                            })
                            .copied()
                    },
                    now,
                )
            };
            // Each host observation is folded in once, however often it is reread.
            let samples: Vec<_> = samples
                .into_iter()
                .filter(|s| {
                    let key = format!("{}\u{0}{}", s.host, s.unit);
                    let new = host_seen.get(&key).is_none_or(|at| *at < s.at);
                    if new {
                        host_seen.insert(key, s.at);
                    }
                    new
                })
                .collect();
            host_seen.retain(|_, at| now.saturating_sub(*at) <= 86400);
            for s in &samples {
                recent_capacity.observe(s);
            }
            recent_capacity.prune(now);
            let mut d = dashboard.write().await;
            if sender.try_send((batch, samples)).is_err() {
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
                synthetic_ids.contains(name) || all.iter().any(|t| t.id == *name)
            });
            drop(d);
            if host_snapshot.is_some() {
                // Daily rows change slowly; live use and 24-hour peaks every tick.
                if now.saturating_sub(capacity_at) >= 60 {
                    capacity_at = now;
                    let path = path.clone();
                    let rows = tokio::task::spawn_blocking(move || {
                        store::Store::open(&path)?
                            .capacity((now / capacity::DAY).saturating_sub(31))
                    })
                    .await;
                    capacity_error = !matches!(rows, Ok(Ok(_)));
                    if let Ok(Ok(rows)) = rows {
                        capacity_rows = rows;
                    }
                }
                let mut view = capacity::build(&capacity_rows, &recent_capacity, now, &policy);
                view.shadow = capacity_shadow;
                view.error = capacity_error;
                dashboard.write().await.quality.capacity = view;
            }
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

#[cfg(test)]
mod tests {
    use super::*;
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
    #[test]
    fn a_bad_transparent_config_is_an_error_start_reports_and_survives() {
        assert!(load_transparent(None).unwrap().is_none());
        let path = std::env::temp_dir().join(format!(
            "pir-apm-bad-transparent-{}.json",
            std::process::id()
        ));
        for bad in [&b"{not json"[..], br#"{"services": []}"#] {
            std::fs::write(&path, bad).unwrap();
            assert!(load_transparent(Some(path.clone().into())).is_err());
        }
        std::fs::remove_file(&path).unwrap();
        assert!(load_transparent(Some(path.into())).is_err());
        // What start does with the error: Transparent off, a visible warning.
        let view = View {
            enabled: true,
            transparent_config_error: true,
            ..Default::default()
        };
        assert!(summary(&view, "enhance").contains("Transparent services are not monitored"));
    }
    #[test]
    fn long_window_merges_fifteen_minutes_and_drops_changed_buckets() {
        let h = |n: f64| Histogram {
            bounds: vec![Some(10.), Some(60.), None],
            counts: vec![n, n, n],
        };
        let mut s = Source::default();
        for at in [200, 500, 1000] {
            s.long
                .push_back((at, BTreeMap::from([("cycle".to_string(), h(1.))])));
        }
        assert_eq!(s.long_window(1000)["cycle"].counts, vec![3., 3., 3.]);
        assert_eq!(s.long_window(1150)["cycle"].counts, vec![2., 2., 2.]);
        let mut other = h(1.);
        other.bounds[0] = Some(5.);
        s.long
            .push_back((1001, BTreeMap::from([("cycle".to_string(), other)])));
        assert!(!s.long_window(1001).contains_key("cycle"));
    }
}
