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
    pub receiver: ReceiverAudit,
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
/// The Receiver payout audit as the publisher's health route reports it.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReceiverAudit {
    pub sampled_at: u64,
    /// The newer of the last successful NEAR read and the stored feed cursor, so
    /// a publisher restart does not look like a stalled audit. Kept across
    /// reports that carry neither, such as before a restarted publisher's first
    /// read.
    pub last_read: Option<u64>,
    /// When this monitor first read the health route. Staleness counts from
    /// here until the route reports a read.
    pub since: u64,
    pub error: bool,
    /// Over the cumulative `payouts_checked`.
    pub payouts: Progress,
    /// `payouts_uncheckable` in the latest report.
    pub payouts_uncheckable: Option<u64>,
    /// The REFUNDED swap feed, while the publisher reports it.
    pub refunds: Option<RefundFeed>,
    /// The excess refund feed, while the publisher reports it.
    pub excess_refunds: Option<RefundFeed>,
    /// The latest report's refund counts, or `None` when it has none.
    pub refund_counts: Option<RefundCounts>,
    /// The latest response carried both `near.reads` and the indexer report,
    /// so a refund feed missing from it was removed by the publisher.
    pub complete: bool,
}
/// A NEAR cross-check's progress as this monitor has seen it. The value is a
/// cumulative count or a watermark, so it only moves forward in normal operation.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Progress {
    pub value: Option<u64>,
    /// The last available report had the field present and null, such as a
    /// watermark before its first check, so its first value is progress.
    #[serde(default)]
    pub empty: bool,
    /// When this monitor first sampled the feed.
    pub first_sample: u64,
    /// When this monitor last saw the value increase.
    pub progressed_at: Option<u64>,
}
impl Progress {
    /// Records one successful sample of a field read by [`reported`]. Unavailable
    /// data keeps the baseline. A known-empty field is an empty baseline, so its
    /// first value is progress. A decrease, or a value going back to empty (a
    /// reset provider store), rebaselines. Only an increase is progress.
    fn observe(&mut self, value: Option<Option<u64>>, now: u64) {
        if self.first_sample == 0 {
            self.first_sample = now;
        }
        match value {
            None => {}
            Some(None) => {
                self.value = None;
                self.empty = true;
            }
            Some(Some(value)) => {
                if self.empty || self.value.is_some_and(|old| value > old) {
                    self.progressed_at = Some(now);
                }
                self.value = Some(value);
                self.empty = false;
            }
        }
    }
    /// Seconds since the last increase, or since the first sample before one.
    fn idle_for(&self, now: u64) -> u64 {
        now.saturating_sub(self.progressed_at.unwrap_or(self.first_sample))
    }
    /// Whether the value has gone `threshold` seconds without an increase.
    /// Before the first increase this is unknown until the threshold passes,
    /// so an APM restart cannot recover an idle incident.
    fn idle(&self, now: u64, threshold: u64) -> Option<bool> {
        let idle = self.idle_for(now) >= threshold;
        (idle || self.progressed_at.is_some()).then_some(idle)
    }
}
/// One NEAR refund feed, tracked from the first sample whose `near.reads`
/// listed it, even with a null value.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RefundFeed {
    /// Like [`ReceiverAudit::last_read`], for this feed.
    pub last_read: Option<u64>,
    /// Over the feed's checked-through watermark. Its first sample stands in
    /// for [`ReceiverAudit::since`] when the feed has no read yet.
    pub progress: Progress,
}
/// Refund discrepancy counts from one health report.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RefundCounts {
    pub missing_new: u64,
    pub uncheckable_new: u64,
    pub unconfirmed_new: u64,
    pub missing_day: u64,
    /// Deposit addresses of the missing refunds due in the last day.
    pub missing_deposits: Vec<String>,
}
const PAYOUT_FEED: &str = "near-payout-audit";
const REFUND_FEED: &str = "near-refund-audit";
const EXCESS_REFUND_FEED: &str = "near-excess-refund-audit";
const REFUND_IDLE: &str = "receiver_refund_crosscheck_idle";
const EXCESS_REFUND_IDLE: &str = "receiver_excess_refund_crosscheck_idle";
const REFUND_NOT_FOUND: &str = "receiver_refund_not_found";
const REFUND_PATTERN: &str = "receiver_refunds_not_found_repeatedly";
impl ReceiverAudit {
    /// Records one health route request, `None` when it failed. A refund feed
    /// is tracked while `near.reads` lists it. A complete response without it
    /// drops the feed's tracking; a failed or partial one keeps it.
    fn observe(&mut self, body: Option<&serde_json::Value>, now: u64) {
        self.sampled_at = now;
        self.error = body.is_none();
        let Some(body) = body else { return };
        if self.since == 0 {
            self.since = now;
        }
        self.last_read = self.last_read.max(audit_read(body, PAYOUT_FEED));
        let report = &body["indexer"];
        self.payouts
            .observe(reported(report, "payouts_checked"), now);
        self.payouts_uncheckable = report["payouts_uncheckable"].as_u64();
        let reads = body["near"]["reads"].as_object();
        self.complete = reads.is_some() && report.is_object();
        for (feed, watermark, state) in [
            (REFUND_FEED, "refunds_checked_through", &mut self.refunds),
            (
                EXCESS_REFUND_FEED,
                "excess_refunds_checked_through",
                &mut self.excess_refunds,
            ),
        ] {
            if reads.is_some_and(|reads| reads.contains_key(feed)) {
                let state = state.get_or_insert_with(RefundFeed::default);
                state.last_read = state.last_read.max(audit_read(body, feed));
                state.progress.observe(reported(report, watermark), now);
            } else if self.complete {
                *state = None;
            }
        }
        let count = |field: &str| report[field].as_u64();
        self.refund_counts = (|| {
            Some(RefundCounts {
                missing_new: count("refunds_missing_new")?,
                uncheckable_new: count("refunds_uncheckable_new")?,
                unconfirmed_new: count("refunds_unconfirmed_new")?,
                missing_day: count("refunds_missing_day")?,
                missing_deposits: deposits(&report["refunds_missing_deposits"]),
            })
        })();
    }
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
/// Product phases from a shared publisher's management `/health` report, or None
/// when the report is malformed. Names become alert keys, so they stay simple.
pub fn publisher_products(body: &str) -> Option<BTreeMap<String, String>> {
    #[derive(Deserialize)]
    struct Product {
        phase: String,
    }
    #[derive(Deserialize)]
    struct Report {
        products: BTreeMap<String, Product>,
    }
    let report: Report = serde_json::from_str(body).ok()?;
    let simple = |s: &str| {
        (1..=64).contains(&s.len())
            && s.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    };
    (report.products.len() <= 16
        && report
            .products
            .iter()
            .all(|(name, p)| simple(name) && simple(&p.phase)))
    .then(|| {
        report
            .products
            .into_iter()
            .map(|(name, p)| (name, p.phase))
            .collect()
    })
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
    let scaling = crate::scaling::Settings::from_env()?;
    let Some(path) = std::env::var_os("PIR_APM_STATE_PATH").map(PathBuf::from) else {
        anyhow::ensure!(!enabled(), "active alerting requires PIR_APM_STATE_PATH");
        anyhow::ensure!(
            scaling.is_none(),
            "{} requires PIR_APM_STATE_PATH",
            crate::scaling::STATUS_VAR
        );
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
    let mut quality_store = Store::open(&path)?.with_family("quality");
    let quality_mode =
        std::env::var("PIR_APM_QUALITY_ALERT_MODE").unwrap_or_else(|_| "shadow".into());
    anyhow::ensure!(
        matches!(quality_mode.as_str(), "shadow" | "active"),
        "PIR_APM_QUALITY_ALERT_MODE must be shadow or active"
    );
    let mut capacity_store = Store::open(&path)?.with_family("capacity");
    let capacity_mode =
        std::env::var(crate::capacity_alerts::MODE_VAR).unwrap_or_else(|_| "shadow".into());
    anyhow::ensure!(
        matches!(capacity_mode.as_str(), "shadow" | "active"),
        "{} must be shadow or active",
        crate::capacity_alerts::MODE_VAR
    );
    // Optional and independent: absent PIR_APM_SCALER_STATUS opens nothing.
    let mut scaling = scaling
        .map(|settings| crate::scaling::Family::open(settings, &path))
        .transpose()?;
    // A disabled family retires the incidents it left in the shared database.
    let mut retired_scaling = match scaling {
        Some(_) => None,
        None => Some(Store::open(&path)?.with_family("scaling")),
    };
    let policy = Policy::load()?;
    let url = std::env::var("PIR_APM_PUBLIC_URL")
        .unwrap_or_else(|_| "https://wallet-pir.valargroup.dev/apm/".into());
    let transparent_url = format!("{}/transparent/", url.trim_end_matches('/'));
    let notifier_path = path.clone();
    let webhook = config.slack_webhook_url.clone();
    let configured = webhook.is_some();
    tokio::spawn(async move {
        if incidents::deliver(notifier_path, webhook).await.is_err() {
            eprintln!("durable Slack worker stopped; check state directory");
        }
    });
    let eval_dashboard = dashboard.clone();
    let state_path = path.clone();
    let mut service_stores: BTreeMap<String, Store> = BTreeMap::new();
    // Named apart from the Enhance `capacity` family, with its mode in the
    // Transparent config (`capacity_alert_mode`).
    let mut transparent_capacity_store = Store::open(&path)?.with_family("transparent_capacity");
    tokio::spawn(async move {
        let mut store = store;
        let mut router_history = BTreeMap::new();
        let mut capacity_history = crate::capacity_alerts::CounterHistory::new();
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
            let mut capacity_conditions =
                crate::capacity_alerts::conditions(&snapshot, now, &mut capacity_history);
            if snapshot.placement.is_some()
                && !snapshot.placement_error
                && fresh(epoch(snapshot.placement_success), now)
                && snapshot.inventory_error.is_none()
                && snapshot.packing_inventory_error.is_none()
            {
                for incident in &snapshot.monitoring.incidents {
                    if let Some(c) = &incident.condition {
                        if (c.key.starts_with("pending_commit_")
                            || c.key.starts_with("pending_abort_")
                            || c.key.starts_with("domain_")
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
                        // A worker or router removed from the inventories ends its memory incidents.
                        if c.key.starts_with("capacity_")
                            && (c.resource.starts_with("worker_")
                                || c.resource.starts_with("router_"))
                            && !capacity_conditions.iter().any(|n| n.key == c.key)
                        {
                            let mut retired = c.clone();
                            retired.retired = true;
                            retired.sample = now;
                            capacity_conditions.push(retired);
                        }
                    }
                }
            }
            // A product removed from a freshly reported publisher ends its incident.
            if snapshot.publisher_configured && fresh(epoch(snapshot.publisher_sample), now) {
                for incident in &snapshot.monitoring.incidents {
                    if let Some(c) = &incident.condition {
                        if c.key.starts_with("publisher_product_")
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
                let mut quality =
                    crate::quality::conditions(&snapshot.quality, now, &policy.quality);
                crate::quality::retire(
                    &snapshot.quality,
                    &snapshot.monitoring.incidents,
                    now,
                    &mut quality,
                );
                quality_store.evaluate(
                    &quality.shared,
                    now,
                    quality_mode != "active",
                    &config.environment,
                    &transparent_url,
                )?;
                // Each Transparent service is its own family, in its configured mode.
                for (service, conditions) in &quality.services {
                    let shadow = snapshot.quality.service(service).is_none_or(|s| s.shadow);
                    let store = match service_stores.entry(service.clone()) {
                        std::collections::btree_map::Entry::Occupied(e) => e.into_mut(),
                        std::collections::btree_map::Entry::Vacant(e) => e.insert(
                            Store::open(&state_path)?.with_family(&format!("quality-{service}")),
                        ),
                    };
                    store.evaluate(
                        conditions,
                        now,
                        shadow,
                        &config.environment,
                        &format!("{}/quality/?service={service}", url.trim_end_matches('/')),
                    )?;
                }
                transparent_capacity_store.evaluate(
                    &quality.capacity,
                    now,
                    snapshot.quality.capacity.shadow,
                    &config.environment,
                    &format!("{}/capacity/", url.trim_end_matches('/')),
                )?;
                capacity_store.evaluate(
                    &capacity_conditions,
                    now,
                    capacity_mode != "active",
                    &config.environment,
                    &url,
                )?;
                if let Some(family) = scaling.as_mut() {
                    family.evaluate(now, &config.environment, &transparent_url)?;
                }
                if let Some(store) = retired_scaling.as_mut() {
                    crate::scaling::retire_disabled(
                        store,
                        now,
                        &config.environment,
                        &transparent_url,
                    )?;
                }
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
    if let Ok(url) = std::env::var("PIR_APM_RECEIVER_HEALTH_URL") {
        tokio::spawn(receiver_loop(dashboard.clone(), url));
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
    pub status_init_latency_seconds: f64,
    pub status_query_latency_seconds: f64,
    pub latency_hold_seconds: u64,
    pub unavailable_seconds: u64,
    pub redundancy_seconds: u64,
    pub publication_warning_seconds: u64,
    pub publication_critical_seconds: u64,
    pub publication_warning_blocks: u64,
    pub publication_critical_blocks: u64,
    pub publication_failure_hold_seconds: u64,
    /// How long the Receiver payout audit may go without a successful NEAR read.
    pub receiver_audit_stale_seconds: u64,
    /// How long the payout cross-check may go without a newly checked payout.
    pub receiver_audit_idle_seconds: u64,
    /// How long the excess refund cross-check watermark may stay put.
    pub receiver_excess_refund_idle_seconds: u64,
    /// How long the REFUNDED swap cross-check watermark may stay put.
    pub receiver_refund_idle_seconds: u64,
    /// Hold for both refund not-found conditions.
    pub receiver_refund_missing_hold_seconds: u64,
    /// Refunded swaps due in one day and not found that make a pattern.
    pub receiver_refund_pattern_count: u64,
    /// Transparent service-quality and capacity thresholds.
    pub quality: crate::quality::Policy,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            error_ratio: 0.05,
            error_min_requests: 10.,
            latency_min_samples: 20.,
            init_latency_seconds: 2.,
            query_latency_seconds: 5.,
            status_init_latency_seconds: 2.,
            status_query_latency_seconds: 5.,
            latency_hold_seconds: 120,
            unavailable_seconds: 30,
            redundancy_seconds: 120,
            publication_warning_seconds: 120,
            publication_critical_seconds: 300,
            publication_warning_blocks: 8,
            publication_critical_blocks: 16,
            publication_failure_hold_seconds: 120,
            receiver_audit_stale_seconds: 1800,
            receiver_audit_idle_seconds: 21600,
            receiver_excess_refund_idle_seconds: 21600,
            receiver_refund_idle_seconds: 172800,
            receiver_refund_missing_hold_seconds: 3600,
            receiver_refund_pattern_count: 3,
            quality: Default::default(),
        }
    }
}
impl Policy {
    pub(crate) fn load() -> Result<Self> {
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
                    p.query_latency_seconds,
                    p.status_init_latency_seconds,
                    p.status_query_latency_seconds
                ]
                .iter()
                .all(|v| v.is_finite() && *v > 0.)
                && p.status_init_latency_seconds <= 5.
                && p.status_query_latency_seconds <= 5.
                && p.publication_warning_seconds > 0
                && p.publication_critical_seconds >= p.publication_warning_seconds
                && p.publication_warning_blocks > 0
                && p.publication_critical_blocks > p.publication_warning_blocks
                && p.publication_failure_hold_seconds > 0
                && [
                    p.receiver_audit_stale_seconds,
                    p.receiver_audit_idle_seconds,
                    p.receiver_excess_refund_idle_seconds,
                    p.receiver_refund_idle_seconds,
                    p.receiver_refund_missing_hold_seconds,
                    p.receiver_refund_pattern_count,
                ]
                .iter()
                .all(|v| *v > 0)
                && p.quality.valid(),
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
    if d.status.configured {
        for endpoint in ["init", "query"] {
            let window = d.status.alert_window(endpoint, now);
            sources.insert(format!("status_{endpoint}"), window.is_some());
            let sample = epoch(d.status.success);
            let resource = format!("status_{endpoint}");
            add(
                format!("status_{endpoint}_5xx"),
                resource.clone(),
                "critical",
                window.as_ref().map(|w| {
                    w.completed as f64 >= p.error_min_requests
                        && w.errors as f64 / (w.completed.max(1) as f64) > p.error_ratio
                }),
                window
                    .as_ref()
                    .map(|w| format!("{} HTTP 5xx / {} completed requests", w.errors, w.completed))
                    .unwrap_or_else(|| "unavailable".into()),
                format!(
                    "> {}% over 5m; minimum {} completions",
                    p.error_ratio * 100.,
                    p.error_min_requests
                ),
                0,
                sample,
            );
            let budget = if endpoint == "init" {
                p.status_init_latency_seconds
            } else {
                p.status_query_latency_seconds
            };
            add(
                format!("status_{endpoint}_latency"),
                resource,
                "warning",
                window.as_ref().map(|w| {
                    w.completed as f64 >= p.latency_min_samples
                        && w.p99_seconds.is_some_and(|v| v > budget)
                }),
                window
                    .as_ref()
                    .and_then(|w| w.p99_seconds)
                    .map(|v| format!("HTTP processing p99 {v:.3}s"))
                    .unwrap_or_else(|| {
                        if window.is_some() {
                            "no completed requests".into()
                        } else {
                            "unavailable".into()
                        }
                    }),
                format!(
                    "p99 > {budget}s, minimum {} completions",
                    p.latency_min_samples
                ),
                p.latency_hold_seconds,
                sample,
            );
        }
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
    for (severity, hold) in [
        ("warning", p.publication_warning_seconds),
        ("critical", p.publication_critical_seconds),
    ] {
        let generations = d
            .placement
            .as_ref()
            .and_then(|placement| Some((placement.revision?, placement.published_revision?)));
        add(
            format!("publication_handover_{severity}"),
            "placement".into(),
            severity,
            generations
                .filter(|_| topology)
                .map(|(active, published)| active < published),
            generations
                .map(|(active, published)| {
                    format!("serving generation {active}; publisher generation {published}")
                })
                .unwrap_or_else(|| "unavailable".into()),
            format!("serving generation behind publisher for {hold}s"),
            hold,
            coordinator_sample.min(epoch(d.placement_success)),
        );
    }
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
    // A shared publisher keeps serving the products that still run, so a failed
    // product is visible only in its management report, not in Enhance readiness.
    if d.publisher_configured {
        let sample = epoch(d.publisher_sample);
        let valid = fresh(sample, now);
        sources.insert("publisher".into(), valid);
        for (product, phase) in &d.publisher_products {
            add(
                format!("publisher_product_{product}"),
                format!("publisher_{product}"),
                "critical",
                valid.then_some(phase != "running"),
                format!("phase {phase}"),
                "not running for 60s".into(),
                60,
                sample,
            );
        }
    }
    let publication = publication(d, now);
    sources.insert(
        "publication_metrics".into(),
        coord
            && [
                "enhance_published_anchor_height",
                "enhance_publication_last_advancement_unix_seconds",
                "enhance_publication_blocked",
                "enhance_publication_consecutive_failures",
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
    let receiver = &d.monitoring.receiver;
    let mut retired = Vec::new();
    if receiver.sampled_at > 0 {
        let sampled = fresh(receiver.sampled_at, now) && !receiver.error;
        let refund_feeds = [
            ("refund", &receiver.refunds),
            ("excess refund", &receiver.excess_refunds),
        ];
        let mut feeds = vec![("payout", receiver.last_read, receiver.since)];
        for (label, feed) in refund_feeds {
            if let Some(feed) = feed {
                feeds.push((label, feed.last_read, feed.progress.first_sample));
            }
        }
        // The stalest feed decides, firing over unknown over healthy. Before any
        // read the grace is unknown, not healthy, so an APM restart cannot
        // recover an incident that is still failing.
        let (label, last_read, stale) = feeds
            .iter()
            .map(|&(label, read, since)| {
                let stale = sampled
                    .then(|| match read {
                        Some(read) => {
                            Some(now.saturating_sub(read) >= p.receiver_audit_stale_seconds)
                        }
                        None => (now.saturating_sub(since) >= p.receiver_audit_stale_seconds)
                            .then_some(true),
                    })
                    .flatten();
                let rank = match stale {
                    Some(true) => 2,
                    None => 1,
                    Some(false) => 0,
                };
                (
                    rank,
                    now.saturating_sub(read.unwrap_or(since)),
                    label,
                    read,
                    stale,
                )
            })
            .max_by_key(|&(rank, age, ..)| (rank, age))
            .map(|(.., label, read, stale)| (label, read, stale))
            .expect("the payout feed is always listed");
        add(
            "receiver_payout_crosscheck_paused".into(),
            "receiver".into(),
            "warning",
            stale,
            // A NEAR outage or bad key stops only the cross-check, so say so.
            // Recovery reuses this text, so the pause note follows the verdict.
            format!(
                "{}{}",
                last_read.map_or(format!("No NEAR {label} read yet"), |read| {
                    format!(
                        "Last NEAR {label} read {} min ago",
                        now.saturating_sub(read) / 60
                    )
                }),
                if stale == Some(true) {
                    format!(
                        ". Receiver lookups are unaffected, but new {label}s are not \
                         cross-checked against NEAR until reads resume."
                    )
                } else {
                    String::new()
                }
            ),
            format!(
                "no NEAR {} read for {} min (key or NEAR API; see `near feed deferred` in the publisher journal)",
                if feeds.len() > 1 { "payout or refund" } else { "payout" },
                p.receiver_audit_stale_seconds / 60
            ),
            0,
            receiver.sampled_at,
        );
        let uncheckable = receiver
            .payouts_uncheckable
            .map(|n| format!(" ({n} uncheckable in the last day)"))
            .unwrap_or_default();
        let refund_uncheckable = receiver
            .refund_counts
            .as_ref()
            .map(|c| format!(" ({} uncheckable in the last 3 h)", c.uncheckable_new))
            .unwrap_or_default();
        for (key, what, progress, threshold, detail, causes) in [
            (
                "receiver_payout_crosscheck_idle",
                "payouts",
                Some(&receiver.payouts),
                p.receiver_audit_idle_seconds,
                &uncheckable,
                "quiet swap traffic, a paused chain, the explorer not reporting payout \
                 txids, or reads returning no usable swaps",
            ),
            (
                EXCESS_REFUND_IDLE,
                "excess refunds",
                receiver.excess_refunds.as_ref().map(|f| &f.progress),
                p.receiver_excess_refund_idle_seconds,
                &refund_uncheckable,
                "quiet swap traffic, a paused chain, the explorer not reporting refund \
                 txids, 1Click status being unavailable, or reads returning no usable swaps",
            ),
            (
                REFUND_IDLE,
                "refunds",
                receiver.refunds.as_ref().map(|f| &f.progress),
                p.receiver_refund_idle_seconds,
                &refund_uncheckable,
                "few refunded swaps, a paused chain, the explorer not reporting refund \
                 txids, or reads returning no usable swaps",
            ),
        ] {
            let Some(progress) = progress else { continue };
            let firing = sampled.then(|| progress.idle(now, threshold)).flatten();
            add(
                key.into(),
                "receiver".into(),
                "warning",
                firing,
                idle_observed(what, progress, now, firing, detail, causes),
                format!("no new NEAR {what} checked for {}", span(threshold)),
                0,
                receiver.sampled_at,
            );
        }
        if receiver.refunds.is_some() || receiver.excess_refunds.is_some() {
            let counts = receiver.refund_counts.as_ref().filter(|_| sampled);
            let hold = p.receiver_refund_missing_hold_seconds;
            add(
                REFUND_NOT_FOUND.into(),
                "receiver".into(),
                "warning",
                counts.map(|c| {
                    c.missing_new
                        .saturating_add(c.uncheckable_new)
                        .saturating_add(c.unconfirmed_new)
                        > 0
                }),
                counts.map_or("Refund counts unavailable".into(), not_found_observed),
                format!(
                    "a refund not found, without a refund transaction, or unconfirmed in \
                     the last 3 h, for {}",
                    span(hold)
                ),
                hold,
                receiver.sampled_at,
            );
            let firing = counts.map(|c| c.missing_day >= p.receiver_refund_pattern_count);
            add(
                REFUND_PATTERN.into(),
                "receiver".into(),
                // Promoted to critical once shadow data on candidate delays is in.
                "warning",
                firing,
                counts.map_or("Refund counts unavailable".into(), |c| {
                    pattern_observed(c, firing == Some(true))
                }),
                format!(
                    "at least {} refunded swaps due in the last day not found, for {}",
                    p.receiver_refund_pattern_count,
                    span(hold)
                ),
                hold,
                receiver.sampled_at,
            );
        }
        // A complete response without a refund feed means the publisher dropped
        // it (a rollback), so its incidents end rather than stay unknown.
        if sampled && receiver.complete {
            let mut gone = Vec::new();
            if receiver.refunds.is_none() {
                gone.push(REFUND_IDLE);
            }
            if receiver.excess_refunds.is_none() {
                gone.push(EXCESS_REFUND_IDLE);
            }
            if receiver.refunds.is_none() && receiver.excess_refunds.is_none() {
                gone.extend([REFUND_NOT_FOUND, REFUND_PATTERN]);
            }
            for incident in &d.monitoring.incidents {
                if let Some(c) = &incident.condition {
                    if gone.contains(&c.key.as_str()) {
                        let mut c = c.clone();
                        c.retired = true;
                        c.sample = now;
                        c.observed = "The publisher no longer reports this NEAR refund feed".into();
                        retired.push(c);
                    }
                }
            }
        }
        add(
            "receiver_health".into(),
            "receiver".into(),
            "warning",
            Some(!sampled),
            "Receiver health route".into(),
            "health unavailable for 120s".into(),
            120,
            if sampled { receiver.sampled_at } else { now },
        );
    }
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
        // An unavailable source is observed on each evaluator tick. Recovery
        // needs new source observations, not repeated ticks over one sample.
        let sample = if ok {
            match source.as_str() {
                "placement" => epoch(d.placement_success).min(coordinator_sample),
                "publication_metrics" => coordinator_sample,
                _ => now,
            }
        } else {
            now
        };
        for (severity, hold) in [("warning", 45), ("critical", 120)] {
            add(
                format!("coverage_{source}_{severity}"),
                source.clone(),
                severity,
                Some(!ok),
                if ok { "fresh" } else { "unavailable" }.into(),
                format!("unavailable for {hold}s"),
                hold,
                sample,
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
    out.extend(retired);
    out
}

/// Whole minutes, or whole hours from two hours up.
fn span(seconds: u64) -> String {
    if seconds < 7200 {
        format!("{} min", seconds / 60)
    } else {
        format!("{} h", seconds / 3600)
    }
}

/// Observed text for an idle NEAR cross-check. Recovery reuses it, so the
/// impact and possible causes follow only a firing verdict.
fn idle_observed(
    what: &str,
    progress: &Progress,
    now: u64,
    firing: Option<bool>,
    detail: &str,
    causes: &str,
) -> String {
    let age = span(progress.idle_for(now));
    if firing == Some(true) {
        format!(
            "No new NEAR {what} checked for {age}{detail}. Receiver lookups are unaffected. \
             Possible causes: {causes}."
        )
    } else if progress.progressed_at.is_some() {
        format!("New NEAR {what} last checked {age} ago{detail}")
    } else {
        format!("No new NEAR {what} checked in {age} of monitoring{detail}")
    }
}

/// Observed text for `receiver_refund_not_found`, one sentence per kind of
/// discrepancy. It gives counts, not a cause, since one deposit-only
/// candidate list looks the same as an OVK change.
fn not_found_observed(c: &RefundCounts) -> String {
    let swaps = |n: u64| match n {
        1 => "1 refunded swap".to_string(),
        n => format!("{n} refunded swaps"),
    };
    let mut text = Vec::new();
    if c.missing_new > 0 {
        text.push(format!(
            "None of NEAR's reported transactions for {} pays the refund address in a way \
             Receiver PIR can find (zero OVK, Ironwood).",
            swaps(c.missing_new)
        ));
    }
    if c.uncheckable_new > 0 {
        text.push(format!(
            "{} {} no refund transaction from NEAR.",
            swaps(c.uncheckable_new),
            if c.uncheckable_new == 1 {
                "has"
            } else {
                "have"
            }
        ));
    }
    if c.unconfirmed_new > 0 {
        text.push(match c.unconfirmed_new {
            1 => "1 refund could not be confirmed because 1Click status is unavailable.".into(),
            n => {
                format!("{n} refunds could not be confirmed because 1Click status is unavailable.")
            }
        });
    }
    if text.is_empty() {
        "No refund discrepancies in the last 3 h".into()
    } else {
        text.join(" ")
    }
}

/// Observed text for `receiver_refunds_not_found_repeatedly`. Recovery reuses
/// it, so the possible causes follow only a firing verdict.
fn pattern_observed(c: &RefundCounts, firing: bool) -> String {
    let n = c.missing_day;
    if !firing {
        return format!("{n} refunded swaps due in the last day not found");
    }
    let mut text = format!(
        "{n} refunded swaps due in the last day have no refund Receiver PIR can find. NEAR \
         may have changed how it sends refunds (OVK or pool), or its explorer may be late \
         listing refund txids."
    );
    if !c.missing_deposits.is_empty() {
        text.push_str(&format!(
            " Deposit addresses: {}.",
            c.missing_deposits.join(", ")
        ));
    }
    text
}

/// Up to 10 deposit addresses from the report's `refunds_missing_deposits`,
/// keeping only short printable ASCII strings since they reach Slack.
fn deposits(value: &serde_json::Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str())
        .filter(|s| (1..=128).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_graphic()))
        .take(10)
        .map(String::from)
        .collect()
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

/// Samples the Receiver health route every 30 seconds for the NEAR audit's
/// reads and progress (see [`ReceiverAudit::observe`]).
async fn receiver_loop(d: SharedDashboard, url: String) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .expect("receiver health client");
    let mut audit = ReceiverAudit::default();
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
        audit.observe(result.as_ref().ok(), incidents::unix_time());
        d.write().await.monitoring.receiver = audit.clone();
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

/// An indexer report field for [`Progress::observe`]: `Some(None)` when it is
/// present and null, and `None` when the report or field is missing or malformed.
fn reported(report: &serde_json::Value, field: &str) -> Option<Option<u64>> {
    match report.get(field)? {
        serde_json::Value::Null => Some(None),
        value => value.as_u64().map(Some),
    }
}

/// The newer of the health report's in-memory NEAR read and its stored feed
/// cursor for `feed`, or `None` when neither has one.
fn audit_read(health: &serde_json::Value, feed: &str) -> Option<u64> {
    let read = health["near"]["reads"][feed].as_u64();
    let stored = health["indexer"]["feeds"][feed].as_u64();
    read.max(stored)
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
        && (!d.quality.enabled
            || (fresh(d.quality.collected_at, now)
                && fresh(d.quality.persisted_at, now)
                && !d.quality.storage_error))
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
    let serving = serde_json::json!({
        "placement": d.placement,
        "sampled_at": epoch(d.placement_success),
        "observation_available": fresh(epoch(d.placement_success),now) && !d.placement_error,
        "routers": d.packing_routers.iter().map(|(name, router)| {
            serde_json::json!({
                "name": name,
                "sampled_at": epoch(router.sample.success),
                "observation_available": fresh(epoch(router.sample.success),now) && router.sample.error.is_none(),
                "handover": router.placement.as_ref().and_then(|report|report.handover.as_ref()),
            })
        }).collect::<Vec<_>>(),
    });
    Json(
        serde_json::json!({"progress_ok":progress,"evaluated_at":d.monitoring.evaluated_at,"shadow":d.monitoring.shadow,"delivery":d.monitoring.delivery,"incidents":d.monitoring.incidents,"chain":d.monitoring.chain,"peer":d.monitoring.peer,"publication":d.monitoring.publication,"serving":serving,"quality":{"enabled":d.quality.enabled,"collected_at":d.quality.collected_at,"persisted_at":d.quality.persisted_at,"storage_error":d.quality.storage_error,"dropped_batches":d.quality.dropped_batches,"inventory_error":d.quality.inventory_error}}),
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
    fn status_view(now: u64, errors: u64, slow: bool) -> DashboardData {
        let mut d = view(now);
        d.status.configured = true;
        for (at, count) in [(now - 5, 0), (now, 100)] {
            let mut buckets = [0u64; 12];
            buckets[if slow { 11 } else { 2 }] = count;
            let failures = if count == 0 { 0 } else { errors };
            let op = serde_json::json!({"arrivals":count,"successes":count-failures,"failures":failures,"server_errors":failures,"buckets":buckets});
            let sample = serde_json::json!({"http_instances":{"init":"c","query":"r"},"http_operations":{"init":op,"query":op}});
            let time = UNIX_EPOCH + Duration::from_secs(at);
            d.status.success = Some(time);
            d.status.history.push_back((time, sample));
        }
        d
    }
    #[test]
    fn status_alerts_fire_hold_and_require_two_fresh_samples_to_recover() {
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        for now in (1000..=1120).step_by(5) {
            let d = status_view(now, 10, true);
            let cs: Vec<_> = conditions(&d, now, &Policy::default())
                .into_iter()
                .filter(|c| c.key.starts_with("status_"))
                .collect();
            assert_eq!(cs.len(), 4);
            assert!(cs.iter().all(|c| c.firing == Some(true)));
            store.evaluate(&cs, now, true, "test", "").unwrap();
            let latency = store
                .incidents()
                .unwrap()
                .into_iter()
                .find(|i| {
                    i.condition
                        .as_ref()
                        .is_some_and(|c| c.key == "status_query_latency")
                })
                .unwrap();
            assert_eq!(latency.active, now == 1120);
        }
        let mut missing = status_view(1125, 0, false);
        missing.status.error = Some("offline".into());
        let cs: Vec<_> = conditions(&missing, 1125, &Policy::default())
            .into_iter()
            .filter(|c| c.key.starts_with("status_"))
            .collect();
        assert!(cs.iter().all(|c| c.firing.is_none()));
        store.evaluate(&cs, 1125, true, "test", "").unwrap();
        assert!(store.incidents().unwrap().iter().all(|i| i.active));
        for now in [1130, 1130, 1135] {
            let cs: Vec<_> = conditions(&status_view(now, 0, false), now, &Policy::default())
                .into_iter()
                .filter(|c| c.key.starts_with("status_"))
                .collect();
            store.evaluate(&cs, now, true, "test", "").unwrap();
            assert!(store
                .incidents()
                .unwrap()
                .iter()
                .all(|i| i.active == (now != 1135)));
        }
    }
    #[test]
    fn status_rules_are_opt_in_and_missing_http_metrics_raise_coverage() {
        assert!(!conditions(&view(100), 100, &Policy::default())
            .iter()
            .any(|c| c.key.contains("status_")));
        let mut d = view(100);
        d.status.configured = true;
        let cs = conditions(&d, 100, &Policy::default());
        assert_eq!(
            cs.iter()
                .find(|c| c.key == "status_query_5xx")
                .unwrap()
                .firing,
            None
        );
        assert_eq!(
            cs.iter()
                .find(|c| c.key == "coverage_status_query_warning")
                .unwrap()
                .firing,
            Some(true)
        );
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
    fn receiver_audit_staleness_and_health_follow_the_health_route() {
        let now = 10_000;
        let mut d = view(now);
        assert!(
            conditions(&d, now, &Policy::default())
                .iter()
                .all(|c| c.resource != "receiver"),
            "an unconfigured health route adds no Receiver conditions"
        );
        d.monitoring.receiver = ReceiverAudit {
            sampled_at: now,
            last_read: Some(now - 60),
            since: now - 3600,
            error: false,
            ..Default::default()
        };
        assert_eq!(
            check(&d, now, "receiver_payout_crosscheck_paused").firing,
            Some(false)
        );
        assert_eq!(check(&d, now, "receiver_health").firing, Some(false));
        // Recovery reuses the observed text, so only a stale read mentions the pause.
        let fresh_text = check(&d, now, "receiver_payout_crosscheck_paused").observed;
        assert_eq!(fresh_text, "Last NEAR payout read 1 min ago");
        d.monitoring.receiver.last_read = Some(now - 1800);
        let paused = check(&d, now, "receiver_payout_crosscheck_paused");
        assert_eq!(paused.firing, Some(true));
        assert!(paused
            .observed
            .starts_with("Last NEAR payout read 30 min ago. Receiver lookups are unaffected"));
        // Without any reported read, staleness counts from the first sample, so
        // a publisher that has not read yet gets the full threshold, and the
        // grace leaves an incident from before an APM restart open.
        d.monitoring.receiver.last_read = None;
        d.monitoring.receiver.since = now - 60;
        assert_eq!(
            check(&d, now, "receiver_payout_crosscheck_paused").firing,
            None
        );
        d.monitoring.receiver.since = now - 1800;
        assert_eq!(
            check(&d, now, "receiver_payout_crosscheck_paused").firing,
            Some(true)
        );
        // An unreachable route leaves staleness unknown and fails health instead.
        d.monitoring.receiver = ReceiverAudit {
            sampled_at: now,
            last_read: None,
            since: 0,
            error: true,
            ..Default::default()
        };
        assert_eq!(
            check(&d, now, "receiver_payout_crosscheck_paused").firing,
            None
        );
        let health = check(&d, now, "receiver_health");
        assert_eq!((health.firing, health.hold_seconds), (Some(true), 120));
        d.monitoring.receiver = ReceiverAudit {
            sampled_at: now - 100,
            last_read: Some(now - 60),
            since: now - 3600,
            error: false,
            ..Default::default()
        };
        assert_eq!(check(&d, now, "receiver_health").firing, Some(true));
    }
    #[test]
    fn audit_read_prefers_the_newer_of_read_and_stored_cursor() {
        let health = |read: Option<u64>, stored: Option<u64>| {
            serde_json::json!({
                "near": {"reads": {"near-payout-audit": read}},
                "indexer": {"feeds": {"near-payout-audit": stored}},
            })
        };
        assert_eq!(audit_read(&health(Some(5), Some(9)), PAYOUT_FEED), Some(9));
        assert_eq!(audit_read(&health(Some(9), None), PAYOUT_FEED), Some(9));
        // A restarted publisher has no read yet but keeps its stored cursor.
        assert_eq!(audit_read(&health(None, Some(7)), PAYOUT_FEED), Some(7));
        assert_eq!(audit_read(&health(None, None), PAYOUT_FEED), None);
    }
    /// A health body at `now` from a publisher with both refund feeds, every
    /// feed read at `now` and every count zero, overlaid with `indexer` fields.
    fn refund_health(now: u64, indexer: serde_json::Value) -> serde_json::Value {
        let mut body = serde_json::json!({
            "near": {"reads": {
                "near-payout-audit": now,
                "near-refund-audit": now,
                "near-excess-refund-audit": now,
            }},
            "indexer": {
                "feeds": {
                    "near-payout-audit": now,
                    "near-refund-audit": now,
                    "near-excess-refund-audit": now,
                },
                "payouts_checked": 0,
                "payouts_missing": 0,
                "payouts_uncheckable": 0,
                "refunds_checked": 0,
                "refunds_missing": 0,
                "refunds_checked_through": 0,
                "excess_refunds_checked_through": 0,
                "refunds_missing_new": 0,
                "refunds_uncheckable_new": 0,
                "refunds_unconfirmed_new": 0,
                "refunds_missing_day": 0,
                "refunds_missing_deposits": [],
                "refunds_pending": 0,
            },
        });
        for (field, value) in indexer.as_object().unwrap() {
            body["indexer"][field] = value.clone();
        }
        body
    }
    /// A health body from a publisher that predates the refund feeds.
    fn payout_health(now: u64, checked: u64) -> serde_json::Value {
        serde_json::json!({
            "near": {"reads": {"near-payout-audit": now}},
            "indexer": {
                "feeds": {"near-payout-audit": now},
                "payouts_checked": checked,
                "payouts_missing": 0,
                "payouts_uncheckable": 0,
            },
        })
    }
    /// Records one health response at `now` and evaluates the Receiver
    /// conditions durably, as `receiver_loop` and the evaluator loop do.
    fn sample(
        store: &mut Store,
        d: &mut DashboardData,
        body: Option<&serde_json::Value>,
        now: u64,
    ) -> Vec<Condition> {
        d.monitoring.receiver.observe(body, now);
        let cs: Vec<_> = conditions(d, now, &Policy::default())
            .into_iter()
            .filter(|c| c.resource == "receiver")
            .collect();
        store.evaluate(&cs, now, false, "test", "").unwrap();
        d.monitoring.incidents = store.incidents().unwrap();
        cs
    }
    /// The id of the active incident for `key`, if any.
    fn active_id(store: &Store, key: &str) -> Option<String> {
        store
            .incidents()
            .unwrap()
            .into_iter()
            .find(|i| i.active && i.condition.as_ref().is_some_and(|c| c.key == key))
            .map(|i| i.id)
    }
    #[test]
    fn progress_counts_increases_only_and_stays_unknown_after_a_restart() {
        let mut p = Progress::default();
        p.observe(Some(Some(10)), 1000);
        assert_eq!(
            (p.value, p.first_sample, p.progressed_at),
            (Some(10), 1000, None)
        );
        p.observe(Some(Some(12)), 1030);
        assert_eq!(p.progressed_at, Some(1030));
        // A null report keeps the baseline, and a decrease rebaselines.
        p.observe(None, 1060);
        assert_eq!((p.value, p.progressed_at), (Some(12), Some(1030)));
        p.observe(Some(Some(3)), 1090);
        assert_eq!((p.value, p.progressed_at), (Some(3), Some(1030)));
        p.observe(Some(Some(4)), 1120);
        assert_eq!(p.progressed_at, Some(1120));
        assert_eq!(p.idle(1120 + 99, 100), Some(false));
        assert_eq!(p.idle(1120 + 100, 100), Some(true));
        // A restarted APM knows nothing until a real increase or the threshold.
        let mut p = Progress::default();
        p.observe(Some(Some(4)), 5000);
        p.observe(Some(Some(4)), 5050);
        assert_eq!(p.idle(5099, 100), None);
        assert_eq!(p.idle(5100, 100), Some(true));
        p.observe(Some(Some(5)), 5130);
        assert_eq!(p.idle(5130, 100), Some(false));
    }
    #[test]
    fn receiver_idle_and_not_found_texts_follow_the_verdict() {
        let t = 100_000;
        let mut d = view(t);
        let audit = &mut d.monitoring.receiver;
        let body = |now, checked| {
            refund_health(
                now,
                serde_json::json!({"payouts_checked": checked, "payouts_uncheckable": 2}),
            )
        };
        audit.observe(Some(&body(t, 10)), t);
        audit.observe(Some(&body(t + 60, 11)), t + 60);
        let later = t + 60 + 21600;
        d.monitoring.receiver.observe(Some(&body(later, 11)), later);
        let idle = check(&d, later, "receiver_payout_crosscheck_idle");
        assert_eq!(idle.firing, Some(true));
        assert_eq!(
            idle.observed,
            "No new NEAR payouts checked for 6 h (2 uncheckable in the last day). Receiver \
             lookups are unaffected. Possible causes: quiet swap traffic, a paused chain, the \
             explorer not reporting payout txids, or reads returning no usable swaps."
        );
        assert_eq!(idle.threshold, "no new NEAR payouts checked for 6 h");
        d.monitoring
            .receiver
            .observe(Some(&body(later + 30, 12)), later + 30);
        let recovered = check(&d, later + 30, "receiver_payout_crosscheck_idle");
        assert_eq!(recovered.firing, Some(false));
        assert_eq!(
            recovered.observed,
            "New NEAR payouts last checked 0 min ago (2 uncheckable in the last day)"
        );

        let counts = |missing, uncheckable, unconfirmed| RefundCounts {
            missing_new: missing,
            uncheckable_new: uncheckable,
            unconfirmed_new: unconfirmed,
            missing_day: missing,
            missing_deposits: Vec::new(),
        };
        assert_eq!(
            not_found_observed(&counts(2, 1, 0)),
            "None of NEAR's reported transactions for 2 refunded swaps pays the refund address \
             in a way Receiver PIR can find (zero OVK, Ironwood). 1 refunded swap has no refund \
             transaction from NEAR."
        );
        assert_eq!(
            not_found_observed(&counts(0, 0, 0)),
            "No refund discrepancies in the last 3 h"
        );
        // A 1Click outage alone warns, with its own sentence.
        let now = later + 60;
        let outage = refund_health(now, serde_json::json!({"refunds_unconfirmed_new": 4}));
        d.monitoring.receiver.observe(Some(&outage), now);
        let warning = check(&d, now, REFUND_NOT_FOUND);
        assert_eq!((warning.firing, warning.hold_seconds), (Some(true), 3600));
        assert_eq!(
            warning.observed,
            "4 refunds could not be confirmed because 1Click status is unavailable."
        );
        let pattern = check(&d, now, REFUND_PATTERN);
        assert_eq!(
            (pattern.firing, pattern.severity.as_str()),
            (Some(false), "warning")
        );
        assert_eq!(
            pattern.observed,
            "0 refunded swaps due in the last day not found"
        );
        let now = now + 30;
        let pattern = refund_health(
            now,
            serde_json::json!({
                "refunds_missing_day": 3,
                "refunds_missing_deposits": ["t1aaa", "t1bbb", "bad address", 7, "t1ccc"],
            }),
        );
        d.monitoring.receiver.observe(Some(&pattern), now);
        assert_eq!(
            check(&d, now, REFUND_PATTERN).observed,
            "3 refunded swaps due in the last day have no refund Receiver PIR can find. NEAR may \
             have changed how it sends refunds (OVK or pool), or its explorer may be late listing \
             refund txids. Deposit addresses: t1aaa, t1bbb, t1ccc."
        );
    }
    #[test]
    fn refund_conditions_need_a_supporting_publisher_and_count_from_its_first_sample() {
        let start = 100_000;
        let mut d = view(start);
        d.monitoring
            .receiver
            .observe(Some(&payout_health(start, 5)), start);
        assert!(conditions(&d, start, &Policy::default())
            .iter()
            .all(
                |c| !c.key.starts_with("receiver_refund") && !c.key.starts_with("receiver_excess")
            ));
        assert_eq!(
            check(&d, start, "receiver_payout_crosscheck_paused").threshold,
            "no NEAR payout read for 30 min (key or NEAR API; see `near feed deferred` in the \
             publisher journal)"
        );
        // Change A lands three days later. Its refund idle counts from then.
        let a = start + 3 * 86400;
        let body = |now| {
            refund_health(
                now,
                serde_json::json!({
                    "payouts_checked": 6,
                    "refunds_checked_through": 50,
                    "excess_refunds_checked_through": 50,
                }),
            )
        };
        d.monitoring.receiver.observe(Some(&body(a)), a);
        assert_eq!(check(&d, a, REFUND_IDLE).firing, None);
        assert_eq!(check(&d, a, EXCESS_REFUND_IDLE).firing, None);
        assert_eq!(check(&d, a, REFUND_NOT_FOUND).firing, Some(false));
        assert_eq!(
            check(&d, a, "receiver_payout_crosscheck_paused").firing,
            Some(false)
        );
        let at = a + 21600;
        d.monitoring.receiver.observe(Some(&body(at)), at);
        assert_eq!(check(&d, at, EXCESS_REFUND_IDLE).firing, Some(true));
        assert_eq!(check(&d, at, REFUND_IDLE).firing, None);
        let at = a + 172800;
        d.monitoring.receiver.observe(Some(&body(at)), at);
        assert_eq!(check(&d, at, REFUND_IDLE).firing, Some(true));
    }
    #[test]
    fn a_null_report_from_a_supporting_publisher_is_no_progress_and_unknown_not_found() {
        let t = 100_000;
        let mut d = view(t);
        let body = |now| {
            serde_json::json!({
                "near": {"reads": {
                    "near-payout-audit": now,
                    "near-refund-audit": null,
                    "near-excess-refund-audit": null,
                }},
                "indexer": null,
            })
        };
        d.monitoring.receiver.observe(Some(&body(t)), t);
        assert!(d.monitoring.receiver.refunds.is_some());
        assert_eq!(check(&d, t, REFUND_IDLE).firing, None);
        let not_found = check(&d, t, REFUND_NOT_FOUND);
        assert_eq!(not_found.firing, None);
        assert_eq!(not_found.observed, "Refund counts unavailable");
        let later = t + 172800;
        d.monitoring.receiver.observe(Some(&body(later)), later);
        assert_eq!(check(&d, later, REFUND_IDLE).firing, Some(true));
        assert_eq!(check(&d, later, REFUND_NOT_FOUND).firing, None);
        assert_eq!(check(&d, later, REFUND_PATTERN).firing, None);
    }
    #[test]
    fn a_stale_or_never_read_refund_feed_pauses_the_cross_check() {
        let t = 100_000;
        let mut d = view(t);
        // A refund feed that never read counts from its first supported sample.
        let unread = |now| {
            let mut body = refund_health(now, serde_json::json!({}));
            body["near"]["reads"]["near-refund-audit"] = serde_json::Value::Null;
            body["indexer"]["feeds"]["near-refund-audit"] = serde_json::Value::Null;
            body
        };
        d.monitoring.receiver.observe(Some(&unread(t)), t);
        let paused = check(&d, t, "receiver_payout_crosscheck_paused");
        assert_eq!(paused.firing, None);
        assert_eq!(paused.observed, "No NEAR refund read yet");
        let at = t + 1800;
        d.monitoring.receiver.observe(Some(&unread(at)), at);
        let paused = check(&d, at, "receiver_payout_crosscheck_paused");
        assert_eq!(paused.firing, Some(true));
        assert_eq!(
            paused.observed,
            "No NEAR refund read yet. Receiver lookups are unaffected, but new refunds are not \
             cross-checked against NEAR until reads resume."
        );
        assert!(paused
            .threshold
            .starts_with("no NEAR payout or refund read for 30 min"));
        // Payouts reading on time does not hide a refund-only stall.
        let mut d = view(t);
        let mut body = refund_health(t, serde_json::json!({}));
        body["near"]["reads"]["near-excess-refund-audit"] = (t - 1860).into();
        body["indexer"]["feeds"]["near-excess-refund-audit"] = (t - 1860).into();
        d.monitoring.receiver.observe(Some(&body), t);
        let paused = check(&d, t, "receiver_payout_crosscheck_paused");
        assert_eq!(paused.firing, Some(true));
        assert!(paused.observed.starts_with(
            "Last NEAR excess refund read 31 min ago. Receiver lookups are unaffected, but new \
             excess refunds"
        ));
    }
    #[test]
    fn refund_watermark_progresses_while_old_rows_leave_the_cohort() {
        let t = 100_000;
        let mut audit = ReceiverAudit::default();
        for (now, checked, through) in [(t, 5, 1000), (t + 30, 4, 1000), (t + 60, 4, 1300)] {
            let body = refund_health(
                now,
                serde_json::json!({"refunds_checked": checked, "refunds_checked_through": through}),
            );
            audit.observe(Some(&body), now);
        }
        assert_eq!(
            audit.refunds.unwrap().progress.progressed_at,
            Some(t + 60),
            "only the new due swap is progress, not the expiring row"
        );
    }
    #[test]
    fn a_watermark_leaving_null_is_progress_but_not_after_a_restart() {
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let (start, hour) = (100_000, 3600);
        let mut d = view(start);
        let body = |now: u64, through: serde_json::Value| {
            refund_health(
                now,
                serde_json::json!({
                    "payouts_checked": now,
                    "refunds_checked_through": through,
                    "excess_refunds_checked_through": now,
                }),
            )
        };
        // The feed reports a null watermark for 48 h, so its idle warning fires.
        let mut now = start;
        while now <= start + 48 * hour {
            sample(
                &mut store,
                &mut d,
                Some(&body(now, serde_json::Value::Null)),
                now,
            );
            now += 60;
        }
        assert!(active_id(&store, REFUND_IDLE).is_some());
        // Its first check is an advance from the empty baseline, so the
        // incident recovers on two fresh samples without a second advance.
        let through = serde_json::json!(now - 600);
        for _ in 0..2 {
            let cs = sample(&mut store, &mut d, Some(&body(now, through.clone())), now);
            let idle = cs.iter().find(|c| c.key == REFUND_IDLE).unwrap();
            assert_eq!(idle.firing, Some(false));
            now += 60;
        }
        assert!(active_id(&store, REFUND_IDLE).is_none());

        // Null until a check at hour 47 is not "no new refunds for 48 h" at 48.
        let mut audit = ReceiverAudit::default();
        let null = body(start, serde_json::Value::Null);
        audit.observe(Some(&null), start);
        let at = start + 47 * hour;
        audit.observe(Some(&body(at, at.into())), at);
        let progress = &audit.refunds.as_ref().unwrap().progress;
        assert_eq!(progress.progressed_at, Some(at));
        assert_eq!(progress.idle(start + 48 * hour, 48 * hour), Some(false));
        // A null report is unavailable, not empty, so it keeps the baseline.
        let mut audit = ReceiverAudit::default();
        let unavailable = serde_json::json!({
            "near": {"reads": {"near-payout-audit": start, "near-refund-audit": null}},
            "indexer": null,
        });
        audit.observe(Some(&unavailable), start);
        // So does a restarted APM's first sample: a value is only a baseline.
        audit.observe(Some(&body(at, at.into())), at);
        let progress = &audit.refunds.as_ref().unwrap().progress;
        assert_eq!(progress.progressed_at, None);
        assert_eq!(progress.idle(start + 48 * hour, 48 * hour), Some(true));
    }
    #[test]
    fn an_isolated_refund_discrepancy_and_a_later_ovk_change_notify_separately() {
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let (start, hour) = (100_000, 3600);
        let mut d = view(start);
        // One deposit-only swap due at 1 h, then from 13 h a refund every 20 min
        // that Receiver PIR cannot find.
        let end = start + 16 * hour;
        let due: Vec<u64> = std::iter::once(start + hour)
            .chain((start + 13 * hour..=end).step_by(1200))
            .collect();
        let mut fired: BTreeMap<&str, std::collections::BTreeSet<String>> = BTreeMap::new();
        for now in (start..=end).step_by(60) {
            let within = |window: u64| {
                due.iter()
                    .filter(|t| **t <= now && now - **t < window)
                    .count()
            };
            let body = refund_health(
                now,
                serde_json::json!({
                    "payouts_checked": now,
                    "refunds_checked_through": now,
                    "excess_refunds_checked_through": now,
                    "refunds_missing_new": within(3 * hour),
                    "refunds_missing_day": within(24 * hour),
                }),
            );
            sample(&mut store, &mut d, Some(&body), now);
            for key in [REFUND_NOT_FOUND, REFUND_PATTERN] {
                if let Some(id) = active_id(&store, key) {
                    fired.entry(key).or_default().insert(id);
                }
            }
            if now == start + 12 * hour {
                assert_eq!(fired[REFUND_NOT_FOUND].len(), 1);
                assert!(active_id(&store, REFUND_NOT_FOUND).is_none(), "cleared");
                assert!(!fired.contains_key(REFUND_PATTERN));
            }
        }
        assert_eq!(
            fired[REFUND_NOT_FOUND].len(),
            2,
            "the later stream notifies again"
        );
        assert_eq!(fired[REFUND_PATTERN].len(), 1);
        assert!(active_id(&store, REFUND_PATTERN).is_some());
    }
    #[test]
    fn a_reorg_restored_within_the_hold_raises_no_refund_alert() {
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let (start, hour) = (100_000, 3600);
        let mut d = view(start);
        let mut fired = false;
        for now in (start..=start + 4 * hour).step_by(60) {
            // A rewind drops three matched refunds for 20 minutes.
            let dropped = (start + 2 * hour..start + 2 * hour + 1200).contains(&now);
            let n = if dropped { 3 } else { 0 };
            let body = refund_health(
                now,
                serde_json::json!({
                    "refunds_checked_through": now,
                    "excess_refunds_checked_through": now,
                    "refunds_missing_new": n,
                    "refunds_missing_day": n,
                }),
            );
            let cs = sample(&mut store, &mut d, Some(&body), now);
            fired |= cs
                .iter()
                .any(|c| c.key == REFUND_PATTERN && c.firing == Some(true));
            assert!(active_id(&store, REFUND_PATTERN).is_none());
            assert!(active_id(&store, REFUND_NOT_FOUND).is_none());
        }
        assert!(fired, "the dropped refunds were reported");
    }
    #[test]
    fn a_publisher_rollback_retires_refund_incidents_once() {
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let (start, hour) = (100_000, 3600);
        let mut d = view(start);
        let mut now = start;
        while now <= start + hour + 60 {
            let body = refund_health(
                now,
                serde_json::json!({
                    "refunds_checked_through": now,
                    "excess_refunds_checked_through": now,
                    "refunds_missing_new": 1,
                    "refunds_missing_day": 1,
                }),
            );
            sample(&mut store, &mut d, Some(&body), now);
            now += 60;
        }
        assert!(active_id(&store, REFUND_NOT_FOUND).is_some());
        let delivered = |store: &Store| store.health(true, now).unwrap().pending;
        let before = delivered(&store);
        // Neither a failed request nor a null report looks like a rollback.
        let partial = serde_json::json!({
            "near": {"reads": {"near-payout-audit": now + 60}},
            "indexer": null,
        });
        for (at, body) in [(now, None), (now + 60, Some(&partial))] {
            let cs = sample(&mut store, &mut d, body, at);
            assert!(cs.iter().all(|c| !c.retired));
            assert!(active_id(&store, REFUND_NOT_FOUND).is_some());
            assert!(d.monitoring.receiver.refunds.is_some());
        }
        let at = now + 120;
        let cs = sample(&mut store, &mut d, Some(&payout_health(at, 1)), at);
        assert!(cs.iter().any(|c| c.retired && c.key == REFUND_NOT_FOUND));
        assert_eq!(delivered(&store), before + 1, "one RETIRED message");
        assert!(d.monitoring.receiver.refunds.is_none());
        assert!(d.monitoring.incidents.iter().all(|i| i
            .condition
            .as_ref()
            .is_none_or(|c| !c.key.contains("refund"))));
        let at = at + 60;
        let cs = sample(&mut store, &mut d, Some(&payout_health(at, 1)), at);
        assert!(cs.iter().all(|c| !c.key.contains("refund")));
        assert_eq!(delivered(&store), before + 1);
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
    fn publisher_view(now: u64, sampled: u64, body: &str) -> DashboardData {
        let mut d = view(now);
        d.publisher_configured = true;
        d.publisher_products = publisher_products(body).unwrap();
        d.publisher_sample = Some(UNIX_EPOCH + Duration::from_secs(sampled));
        d
    }
    #[test]
    fn failed_product_in_running_shared_publisher_pages() {
        // The 2026-10-23 report: Enhance kept /ready at 200 while txid failed.
        let body = r#"{"ready":false,"products":{
            "enhance":{"phase":"running","ready":true},
            "status":{"phase":"running","ready":true},
            "transaction_details":{"phase":"failed","ready":false},
            "transparent_history":{"phase":"running","ready":true}}}"#;
        let mut d = publisher_view(1000, 1000, body);
        d.ready_status = Some(200);
        assert_eq!(check(&d, 1000, "coordinator_ready").firing, Some(false));
        let failed = check(&d, 1000, "publisher_product_transaction_details");
        assert_eq!(failed.firing, Some(true));
        assert_eq!(failed.severity, "critical");
        assert_eq!(failed.observed, "phase failed");
        for product in ["enhance", "status", "transparent_history"] {
            let key = format!("publisher_product_{product}");
            assert_eq!(check(&d, 1000, &key).firing, Some(false));
        }
        assert_eq!(
            check(&d, 1000, "coverage_publisher_critical").firing,
            Some(false)
        );

        // The durable evaluator pages once the hold passes, even in a running host.
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        for t in (1000..=1060).step_by(15) {
            let d = publisher_view(t, t, body);
            let c = check(&d, t, "publisher_product_transaction_details");
            store.evaluate(&[c], t, false, "test", "").unwrap();
        }
        let incidents = store.incidents().unwrap();
        assert!(incidents[0].active && incidents[0].notified);
    }
    #[test]
    fn stale_or_malformed_publisher_reports_are_unknown_with_coverage() {
        let body = r#"{"products":{"transaction_details":{"phase":"failed","ready":false}}}"#;
        let d = publisher_view(1000, 900, body);
        assert_eq!(
            check(&d, 1000, "publisher_product_transaction_details").firing,
            None
        );
        assert_eq!(
            check(&d, 1000, "coverage_publisher_critical").firing,
            Some(true)
        );
        assert!(publisher_products("not json").is_none());
        assert!(publisher_products(r#"{"products":{"Bad Name":{"phase":"failed"}}}"#).is_none());
        // Without a configured publisher there is neither a rule nor coverage.
        let d = view(1000);
        assert!(!conditions(&d, 1000, &Policy::default())
            .iter()
            .any(|c| c.key.starts_with("publisher_") || c.key.contains("_publisher_")));
    }
    #[test]
    fn stale_replica_counts_never_recover_unavailability() {
        let mut d = view(100);
        d.placement = Some(crate::placement::Placement {
            domains: BTreeMap::from([(0, vec!["w".into()])]),
            revision: Some(1),
            published_revision: Some(1),
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
