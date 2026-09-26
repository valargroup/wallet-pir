//! Current distributed serving-path observations and independent alert evaluation.
use crate::{
    config::Config,
    dashboard::{DashboardData, SharedDashboard},
};
use anyhow::{Context, Result};
use axum::{extract::State, http::StatusCode, Json};
use pir_apm::incidents::{self, Condition, DeliveryHealth, Incident, Store};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct View {
    pub loops: BTreeMap<String, u64>,
    pub router_outcomes: BTreeMap<String, (f64, f64)>,
    pub evaluated_at: u64,
    pub shadow: bool,
    pub storage_error: bool,
    pub incidents: Vec<Incident>,
    pub delivery: DeliveryHealth,
    pub chain: Chain,
    pub peer: Peer,
    pub publication: Publication,
}
/// Fresh measurements used by publication rules, also retained by the observer.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Publication {
    pub sampled_at: u64,
    pub anchor_height: Option<u64>,
    pub lag_blocks: Option<u64>,
    pub behind_since: Option<u64>,
    pub advancement_age_seconds: Option<u64>,
    pub consecutive_failures: Option<u64>,
    pub blocked: Option<bool>,
}
fn publication(d: &DashboardData, now: u64) -> Publication {
    let sample = epoch(d.last_scrape);
    let coord = fresh(sample, now) && d.scrape_error.is_none();
    let gauge = |name: &str| {
        d.snapshot_gauges
            .get(name)
            .copied()
            .filter(|v| coord && v.is_finite() && *v >= 0. && v.fract() == 0.)
            .map(|v| v as u64)
    };
    let anchor = gauge("enhance_published_anchor_height");
    let chain = &d.monitoring.chain;
    let lag_blocks = if fresh(chain.sampled_at, now) && !chain.error {
        anchor.map(|a| chain.height.saturating_sub(a))
    } else {
        None
    };
    let previous = &d.monitoring.publication;
    let behind_since = lag_blocks.filter(|n| *n > 0).map(|_| {
        previous
            .behind_since
            .filter(|at| *at <= now && fresh(previous.sampled_at, now))
            .unwrap_or(now)
    });
    Publication {
        sampled_at: sample.min(chain.sampled_at),
        anchor_height: anchor,
        lag_blocks,
        behind_since,
        advancement_age_seconds: gauge("enhance_publication_last_advancement_unix_seconds")
            .filter(|at| *at > 0 && *at <= now)
            .map(|at| now - at),
        consecutive_failures: gauge("enhance_publication_consecutive_failures"),
        blocked: gauge("enhance_publication_blocked")
            .filter(|v| *v <= 1)
            .map(|v| v == 1),
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Chain {
    pub oracle_anchor_height: Option<u64>,
    pub oracle_anchor_hash: Option<String>,
    pub oracle_valid: Option<bool>,
    pub sampled_at: u64,
    pub height: u64,
    pub hash: String,
    pub advanced_at: u64,
    pub error: bool,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Peer {
    pub checked_at: u64,
    pub progress_ok: bool,
    pub delivery_backlog_age: u64,
    pub delivery_configured: bool,
}

/// Explicit opt-in preserves existing production notifications during shadow rollout.
pub fn enabled() -> bool {
    std::env::var("PIR_APM_ALERT_MODE").as_deref() == Ok("active")
}
fn epoch(t: Option<SystemTime>) -> u64 {
    t.and_then(|v| v.duration_since(UNIX_EPOCH).ok())
        .map(|v| v.as_secs())
        .unwrap_or(0)
}
fn fresh(t: u64, now: u64) -> bool {
    t > 0 && t <= now && now - t <= 45
}

pub fn start(dashboard: SharedDashboard, config: Config) -> Result<()> {
    let Some(path) = std::env::var_os("PIR_APM_STATE_PATH").map(PathBuf::from) else {
        anyhow::ensure!(!enabled(), "active alerting requires PIR_APM_STATE_PATH");
        return Ok(());
    };
    let mode = std::env::var("PIR_APM_ALERT_MODE").unwrap_or_else(|_| "shadow".into());
    anyhow::ensure!(
        matches!(mode.as_str(), "shadow" | "active"),
        "PIR_APM_ALERT_MODE must be shadow or active"
    );
    let shadow = mode == "shadow";
    if !shadow {
        for key in [
            "PIR_APM_CHAIN_RPC_URL",
            "PIR_APM_CHAIN_RPC_COOKIE",
            "PIR_APM_PEER_STATUS_URL",
        ] {
            anyhow::ensure!(
                std::env::var(key).is_ok(),
                "{key} is required for active alerts"
            );
        }
    }
    let store = Store::open(&path)?;
    let policy = Policy::load()?;
    let url = std::env::var("PIR_APM_PUBLIC_URL")
        .unwrap_or_else(|_| "https://enhance-pir.valargroup.dev/apm/".into());
    let notifier_path = path.clone();
    let webhook = config.slack_webhook_url.clone();
    let configured = webhook.is_some();
    tokio::spawn(async move {
        if incidents::deliver(notifier_path, webhook).await.is_err() {
            eprintln!("durable Slack worker stopped; check state directory");
        }
    });
    let eval_dashboard = dashboard.clone();
    tokio::spawn(async move {
        let mut store = store;
        let mut router_history = BTreeMap::new();
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let now = incidents::unix_time();
            let mut snapshot = eval_dashboard.read().await.clone();
            // Read delivery configuration before evaluating, including the first tick.
            // Otherwise the default view can report a missing webhook at startup.
            match store.health(configured, now) {
                Ok(delivery) => snapshot.monitoring.delivery = delivery,
                Err(_) => {
                    eval_dashboard.write().await.monitoring.storage_error = true;
                    continue;
                }
            }
            snapshot.monitoring.publication = publication(&snapshot, now);
            update_router_rates(&mut snapshot, &mut router_history, now);
            let mut conditions = conditions(&snapshot, now, &policy);
            if snapshot.placement.is_some()
                && !snapshot.placement_error
                && fresh(epoch(snapshot.placement_success), now)
                && snapshot.inventory_error.is_none()
                && snapshot.packing_inventory_error.is_none()
            {
                for incident in &snapshot.monitoring.incidents {
                    if let Some(c) = &incident.condition {
                        if (c.key.starts_with("domain_")
                            || c.key.starts_with("coverage_worker_")
                            || c.key.starts_with("coverage_router_")
                            || c.key.starts_with("router_"))
                            && !conditions.iter().any(|n| n.key == c.key)
                        {
                            let mut retired = c.clone();
                            retired.retired = true;
                            retired.sample = now;
                            conditions.push(retired);
                        }
                    }
                }
            }
            let result = (|| -> Result<(Vec<Incident>, DeliveryHealth)> {
                store.evaluate(&conditions, now, shadow, &config.environment, &url)?;
                Ok((store.incidents()?, store.health(configured, now)?))
            })();
            let mut view = eval_dashboard.write().await;
            view.monitoring.publication = snapshot.monitoring.publication;
            view.monitoring.router_outcomes = snapshot.monitoring.router_outcomes;
            view.monitoring.shadow = shadow;
            view.monitoring.evaluated_at = now;
            view.monitoring.storage_error = result.is_err();
            if let Ok((incidents, delivery)) = result {
                view.monitoring.incidents = incidents;
                view.monitoring.delivery = delivery;
            }
        }
    });
    if let (Ok(url), Ok(cookie)) = (
        std::env::var("PIR_APM_CHAIN_RPC_URL"),
        std::env::var("PIR_APM_CHAIN_RPC_COOKIE"),
    ) {
        tokio::spawn(chain_loop(dashboard.clone(), url, PathBuf::from(cookie)));
    }
    if let Ok(url) = std::env::var("PIR_APM_PEER_STATUS_URL") {
        tokio::spawn(peer_loop(dashboard, url));
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub error_ratio: f64,
    pub error_min_requests: f64,
    pub latency_min_samples: f64,
    pub init_latency_seconds: f64,
    pub query_latency_seconds: f64,
    pub latency_hold_seconds: u64,
    pub unavailable_seconds: u64,
    pub redundancy_seconds: u64,
    pub publication_warning_seconds: u64,
    pub publication_critical_seconds: u64,
    pub publication_warning_blocks: u64,
    pub publication_critical_blocks: u64,
    pub publication_failure_hold_seconds: u64,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            error_ratio: 0.05,
            error_min_requests: 10.,
            latency_min_samples: 20.,
            init_latency_seconds: 2.,
            query_latency_seconds: 5.,
            latency_hold_seconds: 120,
            unavailable_seconds: 30,
            redundancy_seconds: 120,
            publication_warning_seconds: 120,
            publication_critical_seconds: 300,
            publication_warning_blocks: 8,
            publication_critical_blocks: 16,
            publication_failure_hold_seconds: 120,
        }
    }
}
impl Policy {
    fn load() -> Result<Self> {
        let p: Self = match std::env::var("PIR_APM_ALERT_POLICY") {
            Ok(path) => serde_json::from_slice(&std::fs::read(path)?)?,
            Err(_) => Self::default(),
        };
        anyhow::ensure!(
            (0.0..1.0).contains(&p.error_ratio)
                && [
                    p.error_min_requests,
                    p.latency_min_samples,
                    p.init_latency_seconds,
                    p.query_latency_seconds
                ]
                .iter()
                .all(|v| v.is_finite() && *v > 0.)
                && p.publication_warning_seconds > 0
                && p.publication_critical_seconds >= p.publication_warning_seconds
                && p.publication_warning_blocks > 0
                && p.publication_critical_blocks > p.publication_warning_blocks
                && p.publication_failure_hold_seconds > 0,
            "invalid alert policy"
        );
        Ok(p)
    }
}

pub fn conditions(d: &DashboardData, now: u64, p: &Policy) -> Vec<Condition> {
    let mut out = Vec::new();
    let mut add = |key: String,
                   resource: String,
                   severity: &str,
                   firing: Option<bool>,
                   observed: String,
                   threshold: String,
                   hold: u64,
                   sample: u64| {
        out.push(Condition {
            retired: false,
            key,
            resource,
            severity: severity.into(),
            firing,
            observed,
            threshold,
            hold_seconds: hold,
            sample,
        });
    };
    let coordinator_sample = epoch(d.last_scrape);
    let coord = fresh(coordinator_sample, now) && d.scrape_error.is_none();
    let mut sources = BTreeMap::from([("coordinator".to_string(), coord)]);
    for endpoint in ["init", "query"] {
        let e = d.entrypoints.get(endpoint);
        let valid = e.is_some_and(|e| e.outcomes_available && fresh(epoch(e.alert_sample), now));
        sources.insert(format!("{endpoint}_ingress"), valid);
        let sample = e.map(|e| epoch(e.alert_sample)).unwrap_or(0);
        let w = e.map(|e| &e.alert_window);
        add(
            format!("{endpoint}_5xx"),
            endpoint.into(),
            "critical",
            valid.then(|| {
                w.is_some_and(|w| {
                    w.requests >= p.error_min_requests && w.error_ratio > p.error_ratio
                })
            }),
            w.map(|w| format!("{} failures / {} requests", w.errors_5xx, w.requests))
                .unwrap_or_else(|| "unavailable".into()),
            format!(
                "> {}% over 5m; minimum {} requests",
                p.error_ratio * 100.,
                p.error_min_requests
            ),
            0,
            sample,
        );
        let budget = if endpoint == "init" {
            p.init_latency_seconds
        } else {
            p.query_latency_seconds
        };
        let latency = w
            .filter(|w| valid && w.processing_available)
            .map(|w| &w.processing);
        add(
            format!("{endpoint}_latency"),
            endpoint.into(),
            "warning",
            latency
                .map(|l| l.samples >= p.latency_min_samples && l.p99.is_some_and(|v| v > budget)),
            latency
                .and_then(|l| l.p99)
                .map(|v| format!("p99 {v:.3}s"))
                .unwrap_or_else(|| "unavailable".into()),
            format!("p99 > {budget}s, minimum {} samples", p.latency_min_samples),
            p.latency_hold_seconds,
            sample,
        );
    }
    if d.fleet_enabled {
        sources.insert("worker_inventory".into(), d.inventory_error.is_none());
    }
    if d.packing_enabled {
        sources.insert(
            "router_inventory".into(),
            d.packing_inventory_error.is_none(),
        );
    }
    for (name, w) in &d.fleet {
        sources.insert(format!("worker_{name}"), w.status() == "reachable");
    }
    for (name, r) in &d.packing_routers {
        let valid = fresh(epoch(r.sample.success), now) && r.sample.error.is_none();
        sources.insert(format!("router_{name}"), valid);
        let counts = d.monitoring.router_outcomes.get(name).filter(|_| valid);
        add(
            format!("router_{name}_failures"),
            format!("router_{name}"),
            "warning",
            counts.map(|(total, failed)| {
                *total >= p.error_min_requests && failed / total > p.error_ratio
            }),
            format!("five-minute outcomes {counts:?}"),
            ">5% failed, minimum 10 terminal outcomes".into(),
            0,
            epoch(r.sample.success),
        );
    }
    let topology = fresh(epoch(d.placement_success), now)
        && !d.placement_error
        && d.placement.as_ref().is_some_and(|p| !p.domains.is_empty());
    sources.insert("placement".into(), topology);
    if let Some(placement) = &d.placement {
        for (domain, workers) in &placement.domains {
            let sample = coordinator_sample.min(epoch(d.placement_success));
            let replicas = d
                .domain_ready_replicas
                .get(domain)
                .copied()
                .filter(|_| coord && topology);
            let routers: Vec<_> = d
                .packing_routers
                .values()
                .filter(|r| r.serves(*domain))
                .collect();
            let router_known = topology
                && d.packing_inventory_error.is_none()
                && routers
                    .iter()
                    .all(|r| fresh(epoch(r.sample.success), now) && r.sample.error.is_none());
            let ready = routers
                .iter()
                .filter(|r| {
                    r.status() == "ready"
                        && r.sample
                            .values
                            .get(&format!("domain_eligible_{domain}"))
                            .is_some_and(|n| *n > 0.)
                })
                .count();
            let router_known = router_known
                && routers.iter().all(|r| {
                    r.sample
                        .values
                        .contains_key(&format!("domain_eligible_{domain}"))
                });
            let unavailable = match (replicas, router_known) {
                (Some(0.), _) => Some(true),
                (_, true) if ready == 0 => Some(true),
                (Some(_), true) => Some(false),
                _ => None,
            };
            sources.insert(
                format!("domain_{domain}_eligibility"),
                replicas.is_some() && router_known,
            );
            let resource = format!("domain_{domain}");
            add(
                format!("{resource}_unavailable"),
                resource.clone(),
                "critical",
                unavailable,
                format!("replicas {replicas:?}, ready routers {ready}"),
                "at least one replica and router".into(),
                p.unavailable_seconds,
                sample,
            );
            add(
                format!("{resource}_redundancy"),
                resource,
                "warning",
                replicas.map(|n| n < workers.len() as f64),
                format!("{replicas:?}/{} replicas", workers.len()),
                "all assigned replicas eligible".into(),
                p.redundancy_seconds,
                sample,
            );
        }
    }
    add(
        "coordinator_ready".into(),
        "coordinator".into(),
        "critical",
        coord.then_some(d.ready_status != Some(200)),
        format!("readiness {:?}", d.ready_status),
        "non-200 for 300s".into(),
        300,
        coordinator_sample,
    );
    let publication = publication(d, now);
    sources.insert(
        "publication_metrics".into(),
        coord
            && [
                "enhance_published_anchor_height",
                "enhance_publication_last_advancement_unix_seconds",
                "enhance_publication_blocked",
                "enhance_publication_consecutive_failures",
                "enhance_pending_commit_oldest_seconds",
                "enhance_pending_abort_oldest_seconds",
            ]
            .iter()
            .all(|name| {
                d.snapshot_gauges
                    .get(*name)
                    .is_some_and(|v| v.is_finite() && *v >= 0.)
            })
            && publication.advancement_age_seconds.is_some()
            && publication.anchor_height.is_some()
            && publication.blocked.is_some()
            && publication.consecutive_failures.is_some(),
    );
    let chain = &d.monitoring.chain;
    let chain_ok = fresh(chain.sampled_at, now) && !chain.error;
    sources.insert("chain_rpc".into(), chain_ok);
    for (severity, hold, blocks) in [
        (
            "warning",
            p.publication_warning_seconds,
            p.publication_warning_blocks,
        ),
        (
            "critical",
            p.publication_critical_seconds,
            p.publication_critical_blocks,
        ),
    ] {
        let stalled = match (publication.lag_blocks, publication.advancement_age_seconds) {
            (Some(0), _) => Some(false),
            (Some(_), Some(age)) => Some(
                age >= hold
                    && publication
                        .behind_since
                        .is_some_and(|since| now.saturating_sub(since) >= hold),
            ),
            _ => None,
        };
        add(
            format!("publication_lag_{severity}"),
            "publication".into(),
            severity,
            stalled,
            format!(
                "tip {}, anchor {:?}, lag {:?} blocks, last advancement {:?}s ago",
                chain.height,
                publication.anchor_height,
                publication.lag_blocks,
                publication.advancement_age_seconds
            ),
            format!("behind tip for {hold}s and no successful advancement for {hold}s"),
            0,
            publication.sampled_at,
        );
        add(
            format!("publication_backlog_{severity}"),
            "publication".into(),
            severity,
            publication.lag_blocks.map(|n| n >= blocks),
            format!("lag {:?} blocks", publication.lag_blocks),
            format!(">= {blocks} blocks behind continuously for {hold}s"),
            hold,
            publication.sampled_at,
        );
        add(
            format!("publication_failure_{severity}"),
            "publication".into(),
            severity,
            match (publication.consecutive_failures, publication.blocked) {
                (Some(n), Some(blocked)) => Some(n >= 3 || blocked),
                _ => None,
            },
            format!(
                "consecutive failures {:?}, blocked {:?}",
                publication.consecutive_failures, publication.blocked
            ),
            format!(
                "3 consecutive failures or blocked continuously for {}s",
                if severity == "warning" {
                    p.publication_failure_hold_seconds
                } else {
                    hold
                }
            ),
            if severity == "warning" {
                p.publication_failure_hold_seconds
            } else {
                hold
            },
            coordinator_sample,
        );
        for kind in ["commit", "abort"] {
            let value = d
                .snapshot_gauges
                .get(&format!("enhance_pending_{kind}_oldest_seconds"))
                .copied();
            add(
                format!("pending_{kind}_{severity}"),
                format!("pending_{kind}"),
                severity,
                if coord {
                    value.map(|v| v >= hold as f64)
                } else {
                    None
                },
                format!("oldest age {value:?}"),
                format!(">= {hold}s"),
                0,
                coordinator_sample,
            );
        }
    }
    add(
        "chain_inactive".into(),
        "chain_rpc".into(),
        "warning",
        chain_ok.then_some(now.saturating_sub(chain.advanced_at) >= 600),
        format!("last tip change {}", chain.advanced_at),
        "no tip change for 600s".into(),
        0,
        chain.sampled_at,
    );
    let peer = &d.monitoring.peer;
    add(
        "external_monitor".into(),
        "external_monitor".into(),
        "critical",
        Some(!fresh(peer.checked_at, now) || !peer.progress_ok),
        "external monitor progress".into(),
        "no healthy progress for 60s".into(),
        60,
        if fresh(peer.checked_at, now) && peer.progress_ok {
            peer.checked_at
        } else {
            now
        },
    );
    for (severity, seconds) in [("warning", 60), ("critical", 300)] {
        add(
            format!("delivery_backlog_{severity}"),
            "delivery".into(),
            severity,
            if d.monitoring.delivery.oldest_pending_age >= seconds
                || !d.monitoring.delivery.configured
            {
                Some(true)
            } else if fresh(peer.checked_at, now) && peer.progress_ok {
                Some(peer.delivery_backlog_age >= seconds || !peer.delivery_configured)
            } else {
                None
            },
            format!(
                "local {}s, peer {}s",
                d.monitoring.delivery.oldest_pending_age, peer.delivery_backlog_age
            ),
            format!("oldest pending >= {seconds}s"),
            0,
            now,
        );
    }
    for (source, ok) in sources {
        for (severity, hold) in [("warning", 45), ("critical", 120)] {
            add(
                format!("coverage_{source}_{severity}"),
                source.clone(),
                severity,
                Some(!ok),
                if ok { "fresh" } else { "unavailable" }.into(),
                format!("unavailable for {hold}s"),
                hold,
                now,
            );
        }
    }
    // Existing local host protection continues under the durable evaluator.
    add(
        "disk_usage".into(),
        "coordinator_host".into(),
        "critical",
        (d.host.disk_total_bytes > 0).then_some(d.host.disk_used_ratio > 0.9),
        format!("{}% used", d.host.disk_used_ratio * 100.),
        ">90%".into(),
        0,
        coordinator_sample,
    );
    add(
        "memory_available".into(),
        "coordinator_host".into(),
        "critical",
        (d.host.total_memory_bytes > 0)
            .then_some(d.host.available_memory_bytes < 512 * 1024 * 1024),
        format!("{} bytes available", d.host.available_memory_bytes),
        "<512 MiB".into(),
        0,
        coordinator_sample,
    );
    out
}

async fn chain_loop(d: SharedDashboard, url: String, cookie: PathBuf) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .expect("RPC client");
    loop {
        let result=async {
            let auth=tokio::fs::read_to_string(&cookie).await?;
            let (user,password)=auth.trim().split_once(':').context("invalid RPC cookie")?;
            let value:serde_json::Value=client.post(&url).basic_auth(user,Some(password)).json(&serde_json::json!({"jsonrpc":"1.0","id":"apm","method":"getblockchaininfo","params":[]})).send().await?.error_for_status()?.json().await?;
            anyhow::ensure!(value["error"].is_null(),"RPC error");
            let oracle_height=std::env::var("PIR_APM_ORACLE_ANCHOR_HEIGHT").ok().and_then(|v|v.parse::<u64>().ok());
            let oracle_hash=std::env::var("PIR_APM_ORACLE_ANCHOR_HASH").ok();
            let oracle_valid=if let (Some(height),Some(hash))=(oracle_height,oracle_hash.as_ref()) {
                let reference:serde_json::Value=client.post(&url).basic_auth(user,Some(password)).json(&serde_json::json!({"jsonrpc":"1.0","id":"apm-oracle","method":"getblockhash","params":[height]})).send().await?.error_for_status()?.json().await?;
                anyhow::ensure!(reference["error"].is_null(),"oracle RPC error");
                Some(reference["result"].as_str()==Some(hash.as_str()))
            } else {None};
            Ok::<_,anyhow::Error>((value["result"]["blocks"].as_u64().context("missing height")?,value["result"]["bestblockhash"].as_str().context("missing hash")?.to_string(),oracle_height,oracle_hash,oracle_valid))
        }.await;
        let mut v = d.write().await;
        let c = &mut v.monitoring.chain;
        match result {
            Ok((height, hash, oracle_height, oracle_hash, oracle_valid)) => {
                let now = incidents::unix_time();
                if c.hash != hash {
                    c.advanced_at = now;
                }
                c.oracle_anchor_height = oracle_height;
                c.oracle_anchor_hash = oracle_hash;
                c.oracle_valid = oracle_valid;
                c.height = height;
                c.hash = hash;
                c.sampled_at = now;
                c.error = false;
            }
            Err(_) => c.error = true,
        }
        drop(v);
        tokio::time::sleep(Duration::from_secs(15)).await;
    }
}
async fn peer_loop(d: SharedDashboard, url: String) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .expect("peer client");
    loop {
        let result = async {
            client
                .get(&url)
                .send()
                .await?
                .error_for_status()?
                .json::<serde_json::Value>()
                .await
        }
        .await;
        let now = incidents::unix_time();
        let mut v = d.write().await;
        v.monitoring.peer = match result {
            Ok(body) => Peer {
                checked_at: now,
                progress_ok: body["progress_ok"].as_bool() == Some(true),
                delivery_backlog_age: body["delivery"]["oldest_pending_age"].as_u64().unwrap_or(0),
                delivery_configured: body["delivery"]["configured"].as_bool() == Some(true),
            },
            Err(_) => Peer {
                checked_at: now,
                progress_ok: false,
                delivery_backlog_age: 0,
                delivery_configured: false,
            },
        };
        drop(v);
        tokio::time::sleep(Duration::from_secs(15)).await;
    }
}

fn progress_ok(d: &DashboardData, now: u64) -> bool {
    let mut required = vec!["coordinator", "entrypoints"];
    if d.fleet_enabled {
        required.push("workers");
    }
    if d.packing_enabled {
        required.push("routers");
    }
    fresh(d.monitoring.evaluated_at, now)
        && fresh(d.monitoring.delivery.worker_at, now)
        && !d.monitoring.storage_error
        && required.iter().all(|name| {
            d.monitoring
                .loops
                .get(*name)
                .is_some_and(|t| fresh(*t, now))
        })
}
pub async fn status(State(d): State<SharedDashboard>) -> Json<serde_json::Value> {
    let d = d.read().await;
    let now = incidents::unix_time();
    let progress = progress_ok(&d, now);
    Json(
        serde_json::json!({"progress_ok":progress,"evaluated_at":d.monitoring.evaluated_at,"shadow":d.monitoring.shadow,"delivery":d.monitoring.delivery,"incidents":d.monitoring.incidents,"chain":d.monitoring.chain,"peer":d.monitoring.peer,"publication":d.monitoring.publication}),
    )
}
pub async fn readyz(State(d): State<SharedDashboard>) -> (StatusCode, Json<serde_json::Value>) {
    let d = d.read().await;
    let now = incidents::unix_time();
    let ok = progress_ok(&d, now)
        && d.monitoring.delivery.configured
        && d.monitoring.delivery.oldest_pending_age < 300
        && d.monitoring.incidents.iter().all(|i| {
            i.condition
                .as_ref()
                .is_none_or(|c| !c.key.starts_with("coverage_") || c.firing == Some(false))
        });
    (
        if ok {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(serde_json::json!({"ready":ok})),
    )
}

// Per-source counter resets cannot create negative deltas or cross-process rates.
type RouterHistory = BTreeMap<String, std::collections::VecDeque<(u64, f64, f64)>>;
fn update_router_rates(d: &mut DashboardData, history: &mut RouterHistory, now: u64) {
    history.retain(|name, _| d.packing_routers.contains_key(name));
    d.monitoring.router_outcomes.clear();
    for (name, r) in &d.packing_routers {
        let at = epoch(r.sample.success);
        if !fresh(at, now) || r.sample.error.is_some() {
            continue;
        }
        let (Some(success), Some(failure)) = (
            r.sample.values.get("successful_queries_total"),
            r.sample.values.get("failed_queries_total"),
        ) else {
            continue;
        };
        let h = history.entry(name.clone()).or_default();
        if h.back()
            .is_some_and(|(t, s, f)| at > *t && (success < s || failure < f || at - t > 45))
        {
            h.clear();
        }
        if h.back().is_none_or(|(t, _, _)| at > *t) {
            h.push_back((at, *success, *failure));
        }
        while h.len() > 2
            && h.get(1)
                .is_some_and(|(t, _, _)| now.saturating_sub(*t) >= 300)
        {
            h.pop_front();
        }
        if let (Some((_, s0, f0)), Some((_, s1, f1))) = (h.front(), h.back()) {
            if h.len() > 1 {
                d.monitoring
                    .router_outcomes
                    .insert(name.clone(), (s1 - s0 + f1 - f0, f1 - f0));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn view(now: u64) -> DashboardData {
        let mut d = DashboardData::new(
            "test".into(),
            crate::schema::Schema::enhance_default(),
            "test".into(),
            "host".into(),
            Default::default(),
        );
        d.last_scrape = Some(UNIX_EPOCH + Duration::from_secs(now));
        d.scrape_error = None;
        d
    }
    #[test]
    fn ingress_failures_alert_without_coordinator_query_or_bandwidth() {
        let mut d = view(100);
        d.entrypoints.insert(
            "query".into(),
            crate::dashboard::EntrypointData {
                outcomes_available: true,
                alert_sample: d.last_scrape,
                alert_window: crate::metrics::EndpointWindow {
                    requests: 20.,
                    errors_5xx: 2.,
                    error_ratio: 0.1,
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let get = |d: &DashboardData| {
            conditions(d, 100, &Policy::default())
                .into_iter()
                .find(|c| c.key == "query_5xx")
                .unwrap()
        };
        assert_eq!(get(&d).firing, Some(true));
        d.entrypoints.get_mut("query").unwrap().outcomes_available = false;
        assert_eq!(get(&d).firing, None);
    }
    #[test]
    fn publication_uses_independent_tip_and_stale_rpc_is_unknown() {
        let mut d = view(100);
        d.snapshot_gauges
            .insert("enhance_published_anchor_height".into(), 50.);
        d.snapshot_gauges.insert(
            "enhance_publication_last_advancement_unix_seconds".into(),
            1.,
        );
        d.monitoring.chain = Chain {
            sampled_at: 100,
            height: 51,
            advanced_at: 100,
            ..Default::default()
        };
        let get = |d: &DashboardData| {
            conditions(d, 100, &Policy::default())
                .into_iter()
                .find(|c| c.key == "publication_lag_warning")
                .unwrap()
        };
        assert_eq!(get(&d).firing, Some(false));
        d.monitoring.chain.height = 49;
        assert_eq!(get(&d).firing, Some(false));
        d.monitoring.chain.error = true;
        assert_eq!(get(&d).firing, None);
    }
    fn publication_view(now: u64, age: u64, lag: u64) -> DashboardData {
        let mut d = view(now);
        d.monitoring.publication = Publication {
            sampled_at: now,
            behind_since: Some(1),
            ..Default::default()
        };
        d.monitoring.chain = Chain {
            sampled_at: now,
            height: 100 + lag,
            ..Default::default()
        };
        for (key, value) in [
            ("enhance_published_anchor_height", 100.),
            (
                "enhance_publication_last_advancement_unix_seconds",
                (now - age) as f64,
            ),
            ("enhance_publication_consecutive_failures", 0.),
            ("enhance_publication_blocked", 0.),
        ] {
            d.snapshot_gauges.insert(key.into(), value);
        }
        d
    }
    fn check(d: &DashboardData, now: u64, key: &str) -> Condition {
        conditions(d, now, &Policy::default())
            .into_iter()
            .find(|c| c.key == key)
            .unwrap()
    }
    #[test]
    fn advancing_behind_tip_is_not_a_stall_but_stalls_and_backlogs_are_detected() {
        let now = 1000;
        let d = publication_view(now, 60, 4);
        assert_eq!(
            check(&d, now, "publication_lag_warning").firing,
            Some(false)
        );
        assert_eq!(
            check(&d, now, "publication_backlog_warning").firing,
            Some(false)
        );
        let d = publication_view(now, 121, 1);
        assert_eq!(check(&d, now, "publication_lag_warning").firing, Some(true));
        assert_eq!(
            check(&d, now, "publication_lag_critical").firing,
            Some(false)
        );
        let d = publication_view(now, 301, 1);
        assert_eq!(
            check(&d, now, "publication_lag_critical").firing,
            Some(true)
        );
        let d = publication_view(now, 30, 16);
        assert_eq!(
            check(&d, now, "publication_lag_critical").firing,
            Some(false)
        );
        let c = check(&d, now, "publication_backlog_critical");
        assert_eq!(c.firing, Some(true));
        assert_eq!(c.hold_seconds, 300);
        let d = publication_view(now, 301, 0);
        assert_eq!(
            check(&d, now, "publication_lag_critical").firing,
            Some(false)
        );
    }
    #[test]
    fn first_new_block_after_idle_chain_gets_full_publication_grace() {
        let mut d = publication_view(1000, 900, 1);
        d.monitoring.publication = Publication::default();
        assert_eq!(
            check(&d, 1000, "publication_lag_critical").firing,
            Some(false)
        );
        d.monitoring.publication = publication(&d, 1000);
        d.last_scrape = Some(UNIX_EPOCH + Duration::from_secs(1030));
        d.monitoring.chain.sampled_at = 1030;
        assert_eq!(
            check(&d, 1030, "publication_lag_warning").firing,
            Some(false)
        );
    }
    #[test]
    fn missing_or_future_advancement_and_missing_blocked_are_unknown() {
        let mut d = publication_view(1000, 30, 1);
        for value in [f64::NAN, 1001., 0.] {
            d.snapshot_gauges.insert(
                "enhance_publication_last_advancement_unix_seconds".into(),
                value,
            );
            assert_eq!(check(&d, 1000, "publication_lag_warning").firing, None);
        }
        d.snapshot_gauges.remove("enhance_publication_blocked");
        assert_eq!(check(&d, 1000, "publication_failure_warning").firing, None);
        assert_eq!(
            check(&d, 1000, "coverage_publication_metrics_warning").firing,
            Some(true)
        );
    }
    #[test]
    fn transient_blocked_retry_is_suppressed_but_persistent_failure_fires() {
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        for t in (1000..=1080).step_by(5) {
            let mut d = publication_view(t, 30, 1);
            d.snapshot_gauges
                .insert("enhance_publication_blocked".into(), 1.);
            let c = check(&d, t, "publication_failure_warning");
            store.evaluate(&[c], t, true, "test", "").unwrap();
        }
        assert!(!store.incidents().unwrap()[0].active);
        store
            .evaluate(
                &[check(
                    &publication_view(1085, 10, 0),
                    1085,
                    "publication_failure_warning",
                )],
                1085,
                true,
                "test",
                "",
            )
            .unwrap();
        for t in (1090..=1210).step_by(5) {
            let mut d = publication_view(t, 30, 1);
            d.snapshot_gauges
                .insert("enhance_publication_blocked".into(), 1.);
            store
                .evaluate(
                    &[check(&d, t, "publication_failure_warning")],
                    t,
                    true,
                    "test",
                    "",
                )
                .unwrap();
        }
        assert!(store.incidents().unwrap()[0].active);
    }
    #[test]
    fn stale_replica_counts_never_recover_unavailability() {
        let mut d = view(100);
        d.placement = Some(crate::placement::Placement {
            domains: BTreeMap::from([(0, vec!["w".into()])]),
            revision: Some(1),
        });
        d.placement_success = d.last_scrape;
        d.placement_error = false;
        d.domain_ready_replicas.insert(0, 0.);
        let c = conditions(&d, 100, &Policy::default())
            .into_iter()
            .find(|c| c.key == "domain_0_unavailable")
            .unwrap();
        assert_eq!(c.firing, Some(true));
        d.scrape_error = Some("failed".into());
        d.packing_inventory_error = Some("failed".into());
        let c = conditions(&d, 100, &Policy::default())
            .into_iter()
            .find(|c| c.key == "domain_0_unavailable")
            .unwrap();
        assert_eq!(c.firing, None);
    }
}
