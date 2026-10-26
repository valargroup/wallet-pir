//! Service-quality alert rules.
//!
//! Conditions split into families with independent rollout modes: the shared
//! `quality` family (history writes, the Enhance inventory, and Enhance and
//! Status sources), one family per configured Transparent service (keys
//! `quality.<service>.<rule>`), and the Transparent capacity family. Rules pick
//! by service kind and source role, never by service name.
use super::{publication::Finding, services::Kind, ServiceView, Source, View};
use pir_apm::incidents::{Condition, Incident};
use serde::Deserialize;
use std::collections::BTreeMap;

/// Rebuild and prepare timings are judged over this window.
pub const LONG_WINDOW: u64 = 900;

/// Transparent thresholds, read from the `quality` object of `PIR_APM_ALERT_POLICY`.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub history_unpublished_warning_seconds: f64,
    pub history_unpublished_critical_seconds: f64,
    pub txid_unpublished_critical_seconds: f64,
    pub lag_warning_blocks: f64,
    pub lag_hold_seconds: u64,
    pub last_success_warning_seconds: f64,
    pub last_success_critical_seconds: f64,
    pub last_success_hold_seconds: u64,
    pub history_rebuild_warning_seconds: f64,
    pub txid_rebuild_warning_seconds: f64,
    pub rebuild_critical_seconds: f64,
    pub prepare_warning_seconds: f64,
    pub prepare_critical_seconds: f64,
    pub capacity_warning_days: f64,
    pub capacity_critical_days: f64,
    pub host_floor_ratio: f64,
    pub unit_forecast_ratio: f64,
    pub unit_memory_ratio: f64,
    pub unit_memory_hold_seconds: u64,
    /// How long after a publication a public origin may keep serving the
    /// previous map before the origin check fires (see
    /// [`Finding::LaggingOrigin`]). Counted from the publication, not from the
    /// disagreement, so unknown samples cannot extend it. Otherwise 30 s.
    pub origin_lag_hold_seconds: u64,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            history_unpublished_warning_seconds: 30.,
            history_unpublished_critical_seconds: 60.,
            txid_unpublished_critical_seconds: 180.,
            lag_warning_blocks: 2.,
            lag_hold_seconds: 300,
            last_success_warning_seconds: 300.,
            last_success_critical_seconds: 900.,
            last_success_hold_seconds: 120,
            // Bucket edges of the cycle (…10, 30, 60, 120, 600 s) and prepare
            // (…16, 32, 64, 128, 300 s) histograms, so a limit means what it says.
            history_rebuild_warning_seconds: 60.,
            txid_rebuild_warning_seconds: 30.,
            rebuild_critical_seconds: 120.,
            prepare_warning_seconds: 32.,
            prepare_critical_seconds: 64.,
            capacity_warning_days: 30.,
            capacity_critical_days: 7.,
            host_floor_ratio: 0.20,
            unit_forecast_ratio: 0.90,
            unit_memory_ratio: 0.85,
            unit_memory_hold_seconds: 600,
            origin_lag_hold_seconds: 180,
        }
    }
}
impl Policy {
    pub fn valid(&self) -> bool {
        let positive = |v: f64| v.is_finite() && v > 0.;
        let ratio = |v: f64| v.is_finite() && v > 0. && v < 1.;
        [
            self.history_unpublished_warning_seconds,
            self.txid_unpublished_critical_seconds,
            self.lag_warning_blocks,
            self.last_success_warning_seconds,
            self.history_rebuild_warning_seconds,
            self.txid_rebuild_warning_seconds,
            self.prepare_warning_seconds,
            self.capacity_critical_days,
        ]
        .into_iter()
        .all(positive)
            && self.history_unpublished_critical_seconds >= self.history_unpublished_warning_seconds
            && self.last_success_critical_seconds >= self.last_success_warning_seconds
            && self.rebuild_critical_seconds >= self.history_rebuild_warning_seconds
            && self.rebuild_critical_seconds >= self.txid_rebuild_warning_seconds
            && self.prepare_critical_seconds >= self.prepare_warning_seconds
            && self.capacity_warning_days >= self.capacity_critical_days
            && self.origin_lag_hold_seconds > 0
            && [
                self.host_floor_ratio,
                self.unit_forecast_ratio,
                self.unit_memory_ratio,
            ]
            .into_iter()
            .all(ratio)
    }
}

/// Where a kind's coordinator reports publication progress.
pub struct Coordinator {
    pub role: &'static str,
    pub lag: &'static str,
    pub oldest_unpublished: &'static str,
    pub last_success: &'static str,
    pub node_height: &'static str,
    pub published_height: &'static str,
    pub cycle: &'static str,
    pub freshness: &'static str,
}
pub fn coordinator(kind: Kind) -> Coordinator {
    match kind {
        Kind::History => Coordinator {
            role: "publisher",
            lag: "transparent_publication_lag_blocks",
            oldest_unpublished: "transparent_publication_oldest_unpublished_seconds",
            last_success: "transparent_publication_last_success_timestamp_seconds",
            node_height: "transparent_publication_node_height",
            published_height: "transparent_publication_public_height",
            cycle: "transparent_publication_cycle_seconds",
            freshness: "transparent_publication_freshness_seconds",
        },
        Kind::Txid => Coordinator {
            role: "display-controller",
            lag: "txid_display_lag_blocks",
            oldest_unpublished: "txid_display_oldest_unpublished_seconds",
            last_success: "txid_display_last_success_timestamp_seconds",
            node_height: "txid_display_node_height",
            published_height: "txid_display_published_height",
            cycle: "txid_display_cycle_seconds",
            freshness: "txid_display_freshness_seconds",
        },
    }
}
/// Worker roles of a kind and the histogram that times preparing a revision.
pub fn workers(kind: Kind) -> (&'static [&'static str], &'static str) {
    match kind {
        Kind::History => (
            &["archive-owner", "recent-replica"],
            "transparent_shard_prepare_seconds|phase=total",
        ),
        Kind::Txid => (&["display-worker"], "transparent_display_prepare_seconds"),
    }
}

pub fn service_prefix(service: &str) -> String {
    format!("quality.{service}.")
}

#[derive(Default)]
pub struct Families {
    pub shared: Vec<Condition>,
    pub services: BTreeMap<String, Vec<Condition>>,
    pub capacity: Vec<Condition>,
}

struct Adder<'a> {
    out: &'a mut Vec<Condition>,
    prefix: String,
}
impl Adder<'_> {
    #[allow(clippy::too_many_arguments)]
    fn add(
        &mut self,
        key: impl std::fmt::Display,
        resource: impl Into<String>,
        firing: Option<bool>,
        severity: &str,
        hold: u64,
        sample: u64,
        observed: impl Into<String>,
        threshold: impl Into<String>,
    ) {
        self.out.push(Condition {
            retired: false,
            key: format!("{}{key}", self.prefix),
            resource: resource.into(),
            severity: severity.into(),
            firing,
            observed: observed.into(),
            threshold: threshold.into(),
            hold_seconds: hold,
            sample,
        })
    }
}

pub fn conditions(view: &View, now: u64, p: &Policy) -> Families {
    let mut families = Families::default();
    if !view.enabled {
        return families;
    }
    let mut shared = Adder {
        out: &mut families.shared,
        prefix: "quality_".into(),
    };
    shared.add(
        "storage",
        "history",
        Some(
            view.storage_error
                || (view.collected_at > 0
                    && (view.persisted_at == 0 || now.saturating_sub(view.persisted_at) > 60)),
        ),
        "critical",
        60,
        now,
        "Seven-day metrics persistence",
        "history writes unavailable for 60s",
    );
    shared.add(
        "dropped_history",
        "history",
        Some(view.last_dropped_at > 0 && now.saturating_sub(view.last_dropped_at) <= 60),
        "warning",
        0,
        now,
        format!("{} batches dropped", view.dropped_batches),
        "history writer queue dropped a batch in the last minute",
    );
    shared.add(
        "inventory",
        "inventory",
        Some(view.inventory_error),
        "warning",
        45,
        now,
        "Enhance fleet inventory",
        "invalid inventory for 45s",
    );
    shared.add(
        "transparent_config",
        "transparent-config",
        Some(view.transparent_config_error),
        "warning",
        0,
        now,
        if view.transparent_config_error {
            "PIR_APM_TRANSPARENT_CONFIG invalid; Transparent monitoring disabled"
        } else {
            "valid or not configured"
        },
        "the Transparent config must read, parse and validate at start",
    );
    for (name, s) in &view.sources {
        if view.service(&s.service).is_none() {
            source_rules(&mut shared, name, s, now);
        }
    }
    for service in &view.services {
        let mut out = Vec::new();
        service_rules(
            &mut Adder {
                out: &mut out,
                prefix: service_prefix(&service.name),
            },
            service,
            view,
            now,
            p,
        );
        families.services.insert(service.name.clone(), out);
    }
    families.capacity = super::capacity::conditions(&view.capacity, now, p);
    families
}

/// Per-unit OOM kill gauges a source carries from the host snapshot.
pub const UNIT_OOM: &str = "pir_host_unit_oom_kills|unit=";

/// Coverage, host resources and OOM kills: every source, whatever its service.
fn source_rules(a: &mut Adder, name: &str, s: &Source, now: u64) {
    let fresh = s.fresh(now);
    a.add(
        format!("coverage_{name}"),
        name,
        Some(!fresh),
        "warning",
        45,
        now,
        if fresh { "fresh" } else { "unavailable" },
        "source unavailable for 45s",
    );
    let gauge = |key: &str| s.current.gauges.get(key).copied().filter(|_| fresh);
    if s.current
        .gauges
        .contains_key("pir_host_observation_available")
    {
        a.add(
            format!("host_coverage_{name}"),
            name,
            Some(gauge("pir_host_observation_available") != Some(1.)),
            "warning",
            45,
            now,
            "Host resource observation coverage",
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
        a.add(
            format!("{kind}_{name}"),
            name,
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
    a.add(
        format!("oom_{name}"),
        name,
        oom,
        "critical",
        0,
        s.sampled_at,
        "OOM kill counter change",
        "new OOM kills in the observation window",
    );
    // Every other unit the host sampler reports beside the main one.
    for (key, new) in &s.current.gauges {
        let Some(unit) = key.strip_prefix(UNIT_OOM) else {
            continue;
        };
        let oom = s
            .recent
            .front()
            .and_then(|p| p.values.gauges.get(key))
            .copied()
            .zip(fresh.then_some(*new))
            .map(|(old, new)| new > old);
        a.add(
            format!("oom_{name}_{unit}"),
            name,
            oom,
            "critical",
            0,
            s.sampled_at,
            format!("OOM kill counter change of {unit}"),
            "new OOM kills in the observation window",
        );
    }
}

/// Share of a fifteen-minute window's samples that may lie above a limit.
const EXCEEDANCE: f64 = 0.05;

/// The fifteen-minute histogram; None (unknown) without a fresh sample.
fn long_histogram(s: &Source, key: &str, now: u64) -> Option<super::data::Histogram> {
    let h = s.long_window(now).remove(key)?;
    (s.fresh(now) && h.counts.last().is_some_and(|n| *n > 0.)).then_some(h)
}

/// A limit fires when more than 5% of the fifteen-minute window's samples lie
/// above the smallest bucket edge at or above it; the overflow bucket counts as
/// above. Interpolating a percentile inside a bucket would fire far below the
/// limit, since the buckets double.
#[allow(clippy::too_many_arguments)]
fn exceedance_rules(
    a: &mut Adder,
    key: &str,
    resource: &str,
    s: &Source,
    histogram: &str,
    what: &str,
    limits: [f64; 2],
    now: u64,
) {
    let h = long_histogram(s, histogram, now);
    for (level, limit) in [("warning", limits[0]), ("critical", limits[1])] {
        let verdict = h.as_ref().map(|h| {
            let total = h.counts.last().copied().unwrap_or(0.);
            match h.above(limit) {
                Some((edge, above)) => (
                    above / total > EXCEEDANCE,
                    format!("{what}: {above:.0} of {total:.0} samples above {edge}s over 15m"),
                ),
                None => (
                    true,
                    format!("{what}: {total:.0} samples beyond the last bucket over 15m"),
                ),
            }
        });
        a.add(
            format!("{key}_{level}"),
            resource,
            verdict.as_ref().map(|(firing, _)| *firing),
            level,
            60,
            s.sampled_at,
            verdict
                .map(|(_, observed)| observed)
                .unwrap_or_else(|| "no samples in 15m".into()),
            format!("{what}: more than 5% of 15m samples above {limit}s"),
        );
    }
}

fn service_rules(a: &mut Adder, svc: &ServiceView, view: &View, now: u64, p: &Policy) {
    let name = svc.name.as_str();
    a.add(
        "inventory",
        name,
        Some(svc.inventory_error),
        "warning",
        45,
        now,
        "Service inventory",
        "invalid inventory for 45s",
    );
    let sources: Vec<_> = view
        .sources
        .iter()
        .filter(|(_, s)| s.service == name)
        .collect();
    let publication = &svc.publication;
    let publication_fresh =
        publication.sampled_at > 0 && now.saturating_sub(publication.sampled_at) <= 45;
    if !sources.is_empty() {
        let mut verdicts = vec![("canonical", publication.canonical)];
        if !svc.public_origins.is_empty() {
            verdicts.push(("origins", publication.origins_agree));
        }
        for (key, verdict) in verdicts {
            // Origin incidents retain the bounded, sanitized per-origin evidence.
            let (observed, coverage) = if key == "origins" {
                (publication.evidence(), publication.evidence())
            } else {
                (
                    "Independent public publication check".into(),
                    "Publication verification coverage".into(),
                )
            };
            a.add(
                format!("publication_{key}"),
                name,
                verdict.filter(|_| publication_fresh).map(|v| !v),
                "critical",
                match key {
                    "canonical" => 0,
                    // Origins pick up a new publication within about a minute.
                    _ if publication.finding() == Some(Finding::LaggingOrigin)
                        && publication.map_seen_at.is_some_and(|seen| {
                            now.saturating_sub(seen) < p.origin_lag_hold_seconds
                        }) =>
                    {
                        p.origin_lag_hold_seconds
                    }
                    _ => 30,
                },
                publication.sampled_at,
                observed,
                "public anchor must be canonical; every origin must agree",
            );
            a.add(
                format!("publication_{key}_coverage"),
                name,
                Some(!publication_fresh || verdict.is_none()),
                "warning",
                45,
                now,
                coverage,
                "verification unavailable for 45s",
            );
        }
    }
    let coordinator = coordinator(svc.kind);
    let (worker_roles, prepare) = workers(svc.kind);
    for (source_name, s) in &sources {
        let source_name = source_name.as_str();
        source_rules(a, source_name, s, now);
        let fresh = s.fresh(now);
        let gauge = |key: &str| s.current.gauges.get(key).copied().filter(|_| fresh);
        if matches!(
            s.role.as_str(),
            "publisher" | "recent-replica" | "archive-owner"
        ) {
            a.add(
                format!("http_coverage_{source_name}"),
                source_name,
                Some(!fresh || gauge("pir_http_observation_version") != Some(1.)),
                "warning",
                45,
                now,
                "Outer HTTP observation capability",
                "HTTP observation version 1 unavailable for 45s",
            );
        }
        http_rules(a, source_name, s, now);
        if matches!(s.role.as_str(), "archive-owner" | "display-worker") {
            a.add(
                format!("ready_{source_name}"),
                source_name,
                (fresh && publication_fresh && publication.map_sha256.is_some())
                    .then_some(s.ready != Some(true) || s.publication != publication.map_sha256),
                "critical",
                60,
                s.sampled_at,
                "Worker readiness on the published map",
                "not ready on the published map for 60s",
            );
        }
        if worker_roles.contains(&s.role.as_str()) {
            exceedance_rules(
                a,
                &format!("prepare_{source_name}"),
                source_name,
                s,
                prepare,
                "Revision prepare",
                [p.prepare_warning_seconds, p.prepare_critical_seconds],
                now,
            );
        }
        if s.role == coordinator.role {
            publication_rules(a, svc.kind, source_name, s, now, p);
        }
    }
    if svc.kind == Kind::History {
        let recent: Vec<_> = sources
            .iter()
            .map(|(_, s)| *s)
            .filter(|s| s.role == "recent-replica")
            .collect();
        if !recent.is_empty() {
            let known = publication_fresh
                && publication.map_sha256.is_some()
                && recent.iter().all(|s| s.fresh(now));
            let ready = recent
                .iter()
                .filter(|s| s.ready == Some(true) && s.publication == publication.map_sha256)
                .count();
            a.add(
                "recent_availability",
                name,
                known.then_some(ready == 0),
                "critical",
                60,
                now,
                format!("{ready} ready replicas"),
                "no recent replica for 60s",
            );
            // With a one-replica floor the critical above already covers it.
            let floor = svc.min_recent_replicas;
            a.add(
                "recent_redundancy",
                name,
                known.then_some(floor > 1 && ready < floor),
                "warning",
                120,
                now,
                format!("{ready} ready replicas"),
                format!("fewer than {floor} ready recent replicas for 120s"),
            );
        }
    }
}

/// Server errors and latency of the outer HTTP layer (or the edge's Caddy).
fn http_rules(a: &mut Adder, name: &str, s: &Source, now: u64) {
    let fresh = s.fresh(now);
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
        known = fresh && window.observed_seconds > 0 && !requests.is_empty() && errors <= completed;
    }
    a.add(
        format!("http_errors_{name}"),
        name,
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
        a.add(
            format!("latency_{name}_{endpoint}"),
            name,
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
}

/// Publication freshness on a service's coordinator.
fn publication_rules(a: &mut Adder, kind: Kind, name: &str, s: &Source, now: u64, p: &Policy) {
    let c = coordinator(kind);
    let fresh = s.fresh(now);
    let gauge = |key: &str| {
        s.current
            .gauges
            .get(key)
            .copied()
            .filter(|v| fresh && v.is_finite())
    };
    let sample = s.sampled_at;
    // Unpublished age: history warns at 30 s and pages at 60 s held 60 s; the
    // txid display seals every block, so it pages at 180 s and relies on the
    // lag rule for its warning.
    let age = gauge(c.oldest_unpublished);
    let ages: Vec<(&str, f64, u64)> = match kind {
        Kind::History => vec![
            ("warning", p.history_unpublished_warning_seconds, 0),
            ("critical", p.history_unpublished_critical_seconds, 60),
        ],
        Kind::Txid => vec![("critical", p.txid_unpublished_critical_seconds, 0)],
    };
    for (level, budget, hold) in ages {
        a.add(
            format!("publication_{level}"),
            name,
            age.map(|v| v > budget),
            level,
            hold,
            sample,
            age.map(|v| format!("{v:.1}s unpublished"))
                .unwrap_or_else(|| "unknown".into()),
            if hold > 0 {
                format!("unpublished age >{budget}s for {hold}s")
            } else {
                format!("unpublished age >{budget}s")
            },
        );
    }
    let lag = gauge(c.lag);
    a.add(
        "publication_lag",
        name,
        lag.map(|v| v >= p.lag_warning_blocks),
        "warning",
        p.lag_hold_seconds,
        sample,
        lag.map(|v| format!("{v:.0} blocks behind the node"))
            .unwrap_or_else(|| "unknown".into()),
        format!(
            "lag >={} blocks for {}s",
            p.lag_warning_blocks, p.lag_hold_seconds
        ),
    );
    // An idle chain publishes nothing, so age alone is not a failure: only
    // count it while the node is ahead of what was last published.
    // A coordinator reports 0 until its first success after a restart: unknown,
    // so the lag and unpublished-age rules cover that window. After a quiet
    // chain the first new block is already "late", so both levels hold until
    // the coordinator has had time to publish it.
    let since = gauge(c.last_success)
        .filter(|t| *t > 0.)
        .map(|t| (now as f64 - t).max(0.));
    let behind = gauge(c.node_height)
        .zip(gauge(c.published_height))
        .map(|(node, published)| node > published);
    for (level, budget) in [
        ("warning", p.last_success_warning_seconds),
        ("critical", p.last_success_critical_seconds),
    ] {
        a.add(
            format!("last_success_{level}"),
            name,
            since.and_then(|age| if age <= budget { Some(false) } else { behind }),
            level,
            p.last_success_hold_seconds,
            sample,
            since
                .map(|v| format!("last successful publication {v:.0}s ago"))
                .unwrap_or_else(|| "unknown".into()),
            format!(
                "no successful publication for {budget}s while the node is ahead for {}s",
                p.last_success_hold_seconds
            ),
        );
    }
    let rebuild_warning = match kind {
        Kind::History => p.history_rebuild_warning_seconds,
        Kind::Txid => p.txid_rebuild_warning_seconds,
    };
    exceedance_rules(
        a,
        "rebuild",
        name,
        s,
        c.cycle,
        "Publication cycle",
        [rebuild_warning, p.rebuild_critical_seconds],
        now,
    );
    match kind {
        Kind::History => {
            let withdrawn = gauge("transparent_publication_withdrawn");
            a.add(
                "withdrawn",
                name,
                withdrawn.map(|v| v == 1.),
                "critical",
                60,
                sample,
                "Public coverage withdrawn",
                "withdrawn for 60s",
            );
        }
        Kind::Txid => {
            for (key, metric, text) in [
                ("stalled", "txid_display_stalled", "controller stalled"),
                ("halted", "txid_display_halted", "controller halted"),
            ] {
                let v = gauge(metric);
                a.add(
                    key,
                    name,
                    v.map(|v| v == 1.),
                    "critical",
                    0,
                    sample,
                    v.map(|v| if v == 1. { text } else { "running" })
                        .unwrap_or("unknown"),
                    text,
                );
            }
        }
    }
}

/// Ends incidents whose resource is gone: a source dropped from its service,
/// a service no longer configured, or a host or unit the capacity alerts no
/// longer evaluate (gone, or unobserved for an hour). While the Transparent
/// config is invalid nothing of the Transparent services is retired: they are
/// unmonitored, not gone.
pub fn retire(view: &View, incidents: &[Incident], now: u64, f: &mut Families) {
    if !view.enabled || now.saturating_sub(view.collected_at) > 45 {
        return;
    }
    let capacity_fresh =
        !view.capacity.error && now.saturating_sub(view.capacity.computed_at) <= 900;
    for incident in incidents {
        let Some(c) = &incident.condition else {
            continue;
        };
        if view.transparent_config_error && c.key.starts_with("quality.") {
            continue;
        }
        let mut retired = c.clone();
        retired.retired = true;
        retired.sample = now;
        if let Some(svc) = view
            .services
            .iter()
            .find(|s| c.key.starts_with(&service_prefix(&s.name)))
        {
            let current = c.resource == svc.name
                || view
                    .sources
                    .get(&c.resource)
                    .is_some_and(|s| s.service == svc.name);
            let out = f.services.entry(svc.name.clone()).or_default();
            if !svc.inventory_error && !current && !out.iter().any(|n| n.key == c.key) {
                out.push(retired);
            }
        } else if c.key.starts_with("quality_") || c.key.starts_with("quality.") {
            let shared_source = view
                .sources
                .get(&c.resource)
                .is_some_and(|s| view.service(&s.service).is_none());
            if !view.inventory_error
                && !matches!(c.resource.as_str(), "history" | "inventory")
                && !shared_source
                && !f.shared.iter().any(|n| n.key == c.key)
            {
                f.shared.push(retired);
            }
        } else if c.key.starts_with(super::capacity::KEY_PREFIX)
            && capacity_fresh
            && !f.capacity.iter().any(|n| n.key == c.key)
        {
            f.capacity.push(retired);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{data::Histogram, data::Point, publication};
    use super::*;

    fn service(name: &str, kind: Kind) -> ServiceView {
        ServiceView {
            name: name.into(),
            kind,
            path_prefix: "/v2".into(),
            min_recent_replicas: 2,
            shadow: true,
            ..Default::default()
        }
    }
    fn view(services: Vec<ServiceView>, sources: Vec<(&str, Source)>) -> View {
        View {
            enabled: true,
            services,
            sources: sources
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            ..Default::default()
        }
    }
    fn source(service: &str, role: &str, at: u64, gauges: &[(&str, f64)]) -> Source {
        let mut s = Source {
            service: service.into(),
            role: role.into(),
            sampled_at: at,
            ..Default::default()
        };
        for (k, v) in gauges {
            s.current.gauges.insert(k.to_string(), *v);
        }
        s
    }
    fn firing(f: &Families, service: &str, key: &str) -> Option<bool> {
        f.services[service]
            .iter()
            .find(|c| c.key == format!("quality.{service}.{key}"))
            .unwrap_or_else(|| panic!("missing {key}"))
            .firing
    }

    #[test]
    fn missing_http_capability_is_unknown_and_has_coverage_alert() {
        let v = view(
            vec![service("transparent", Kind::History)],
            vec![("worker", source("transparent", "recent-replica", 100, &[]))],
        );
        let f = conditions(&v, 110, &Policy::default());
        assert_eq!(firing(&f, "transparent", "http_errors_worker"), None);
        assert_eq!(
            firing(&f, "transparent", "http_coverage_worker"),
            Some(true)
        );
    }

    #[test]
    fn http_ratio_counts_server_errors_and_stale_samples_cannot_recover() {
        let mut s = source("transparent", "recent-replica", 100, &[]);
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
        s.recent.push_back(p);
        let v = view(
            vec![service("transparent", Kind::History)],
            vec![("worker", s)],
        );
        let f = conditions(&v, 110, &Policy::default());
        assert_eq!(firing(&f, "transparent", "http_errors_worker"), Some(true));
        let f = conditions(&v, 160, &Policy::default());
        assert_eq!(firing(&f, "transparent", "http_errors_worker"), None);
    }

    #[test]
    fn services_have_their_own_families_and_shared_sources_stay_shared() {
        let v = view(
            vec![
                service("transparent", Kind::History),
                service("txid", Kind::Txid),
            ],
            vec![
                ("enhance-init", source("enhance", "init", 100, &[])),
                (
                    "txid-controller",
                    source("txid", "display-controller", 100, &[]),
                ),
                ("k8-01", source("txid", "display-worker", 100, &[])),
            ],
        );
        let f = conditions(&v, 110, &Policy::default());
        assert!(f
            .shared
            .iter()
            .any(|c| c.key == "quality_coverage_enhance-init"));
        assert!(f.shared.iter().all(|c| c.key.starts_with("quality_")));
        assert!(f.services["txid"]
            .iter()
            .all(|c| c.key.starts_with("quality.txid.")));
        assert!(f.services["txid"]
            .iter()
            .any(|c| c.key == "quality.txid.coverage_k8-01"));
        // Display workers have no outer HTTP layer to require.
        assert!(!f.services["txid"]
            .iter()
            .any(|c| c.key.contains("http_coverage")));
        // History-only rules never apply to the txid kind.
        assert!(!f.services["txid"]
            .iter()
            .any(|c| c.key.contains("withdrawn")));
        assert!(f.services["transparent"]
            .iter()
            .any(|c| c.key == "quality.transparent.inventory"));
    }

    #[test]
    fn txid_publication_rules_fire_and_recover() {
        let p = Policy::default();
        let svc = || vec![service("txid", Kind::Txid)];
        let controller = |at, gauges: &[(&str, f64)]| {
            view(
                svc(),
                vec![(
                    "txid-controller",
                    source("txid", "display-controller", at, gauges),
                )],
            )
        };
        // Lag of 2 blocks warns after its hold; the engine holds, the rule reports.
        let f = conditions(
            &controller(100, &[("txid_display_lag_blocks", 2.)]),
            105,
            &p,
        );
        assert_eq!(firing(&f, "txid", "publication_lag"), Some(true));
        let lag = f.services["txid"]
            .iter()
            .find(|c| c.key == "quality.txid.publication_lag")
            .unwrap();
        assert_eq!(lag.hold_seconds, 300);
        let f = conditions(
            &controller(100, &[("txid_display_lag_blocks", 1.)]),
            105,
            &p,
        );
        assert_eq!(firing(&f, "txid", "publication_lag"), Some(false));
        // Unpublished age pages at 180 s; there is no 30 s warning for txid.
        let f = conditions(
            &controller(100, &[("txid_display_oldest_unpublished_seconds", 181.)]),
            105,
            &p,
        );
        assert_eq!(firing(&f, "txid", "publication_critical"), Some(true));
        assert!(!f.services["txid"]
            .iter()
            .any(|c| c.key == "quality.txid.publication_warning"));
        let f = conditions(
            &controller(100, &[("txid_display_oldest_unpublished_seconds", 40.)]),
            105,
            &p,
        );
        assert_eq!(firing(&f, "txid", "publication_critical"), Some(false));
        // Stalled and halted page immediately.
        let f = conditions(
            &controller(
                100,
                &[("txid_display_stalled", 1.), ("txid_display_halted", 0.)],
            ),
            105,
            &p,
        );
        assert_eq!(firing(&f, "txid", "stalled"), Some(true));
        assert_eq!(firing(&f, "txid", "halted"), Some(false));
        // Missing gauges are unknown, never recovery.
        let f = conditions(&controller(100, &[]), 105, &p);
        assert_eq!(firing(&f, "txid", "stalled"), None);
        assert_eq!(firing(&f, "txid", "publication_lag"), None);
        // Stale samples are unknown too.
        let f = conditions(&controller(100, &[("txid_display_stalled", 0.)]), 200, &p);
        assert_eq!(firing(&f, "txid", "stalled"), None);
    }

    #[test]
    fn history_keeps_its_30_and_60_second_age_rules() {
        let v = view(
            vec![service("transparent", Kind::History)],
            vec![(
                "transparent-publisher",
                source(
                    "transparent",
                    "publisher",
                    100,
                    &[("transparent_publication_oldest_unpublished_seconds", 45.)],
                ),
            )],
        );
        let f = conditions(&v, 105, &Policy::default());
        assert_eq!(firing(&f, "transparent", "publication_warning"), Some(true));
        assert_eq!(
            firing(&f, "transparent", "publication_critical"),
            Some(false)
        );
    }

    #[test]
    fn last_success_age_fires_only_while_the_node_is_ahead() {
        let p = Policy::default();
        let at = 10_000;
        let controller = |last: f64, node: f64, published: f64| {
            view(
                vec![service("txid", Kind::Txid)],
                vec![(
                    "txid-controller",
                    source(
                        "txid",
                        "display-controller",
                        at,
                        &[
                            ("txid_display_last_success_timestamp_seconds", last),
                            ("txid_display_node_height", node),
                            ("txid_display_published_height", published),
                        ],
                    ),
                )],
            )
        };
        let f = conditions(&controller((at - 400) as f64, 11., 10.), at, &p);
        assert_eq!(firing(&f, "txid", "last_success_warning"), Some(true));
        assert_eq!(firing(&f, "txid", "last_success_critical"), Some(false));
        let f = conditions(&controller((at - 1000) as f64, 11., 10.), at, &p);
        assert_eq!(firing(&f, "txid", "last_success_critical"), Some(true));
        // Idle chain: old success, nothing new to publish.
        let f = conditions(&controller((at - 1000) as f64, 10., 10.), at, &p);
        assert_eq!(firing(&f, "txid", "last_success_critical"), Some(false));
        // Recovery once a publication lands.
        let f = conditions(&controller((at - 5) as f64, 11., 11.), at, &p);
        assert_eq!(firing(&f, "txid", "last_success_warning"), Some(false));
        // 0 after a controller restart is unknown, not 10 000 s old.
        let f = conditions(&controller(0., 11., 10.), at, &p);
        assert_eq!(firing(&f, "txid", "last_success_critical"), None);
    }

    #[test]
    fn last_success_holds_past_the_first_block_after_a_quiet_chain() {
        let p = Policy::default();
        let start = 10_000u64;
        // Height 10 published at `start`; the chain is quiet for six minutes.
        let run = |publish_at: Option<u64>| {
            let mut store =
                pir_apm::incidents::Store::open(std::path::Path::new(":memory:")).unwrap();
            for t in (start + 360..=start + 600).step_by(5) {
                let published = publish_at.filter(|at| t >= *at);
                let v = view(
                    vec![service("txid", Kind::Txid)],
                    vec![(
                        "txid-controller",
                        source(
                            "txid",
                            "display-controller",
                            t,
                            &[
                                (
                                    "txid_display_last_success_timestamp_seconds",
                                    published.unwrap_or(start) as f64,
                                ),
                                ("txid_display_node_height", 11.),
                                (
                                    "txid_display_published_height",
                                    if published.is_some() { 11. } else { 10. },
                                ),
                            ],
                        ),
                    )],
                );
                store
                    .evaluate(
                        &conditions(&v, t, &p).services["txid"],
                        t,
                        false,
                        "test",
                        "",
                    )
                    .unwrap();
            }
            store
                .incidents()
                .unwrap()
                .into_iter()
                .any(|i| i.active && i.id.starts_with("quality.txid.last_success_warning"))
        };
        // Block 11 arrives after the gap and is published 60 s later: no incident.
        assert!(!run(Some(start + 420)));
        // Nothing is published: a real stall fires once the hold has passed.
        assert!(run(None));
    }

    #[test]
    fn invalid_transparent_config_warns_and_keeps_transparent_incidents() {
        let mut v = view(
            vec![],
            vec![("enhance-init", source("enhance", "init", 100, &[]))],
        );
        v.collected_at = 100;
        let shared = |f: &Families| {
            f.shared
                .iter()
                .find(|c| c.key == "quality_transparent_config")
                .unwrap()
                .firing
        };
        assert_eq!(
            shared(&conditions(&v, 100, &Policy::default())),
            Some(false)
        );
        v.transparent_config_error = true;
        let mut f = conditions(&v, 100, &Policy::default());
        assert_eq!(shared(&f), Some(true));
        // Enhance rules still run.
        assert!(f
            .shared
            .iter()
            .any(|c| c.key == "quality_coverage_enhance-init"));
        // Unmonitored, not gone: the Transparent services' incidents stay open.
        let incident = Incident {
            active: true,
            condition: Some(Condition {
                retired: false,
                key: "quality.txid.stalled".into(),
                resource: "txid".into(),
                severity: "critical".into(),
                firing: Some(true),
                observed: String::new(),
                threshold: String::new(),
                hold_seconds: 0,
                sample: 90,
            }),
            ..Default::default()
        };
        retire(&v, std::slice::from_ref(&incident), 100, &mut f);
        assert!(f.shared.iter().all(|c| !c.retired));
        v.transparent_config_error = false;
        let mut f = conditions(&v, 100, &Policy::default());
        retire(&v, &[incident], 100, &mut f);
        assert!(f.shared.iter().any(|c| c.retired));
    }

    #[test]
    fn every_reported_unit_has_its_own_oom_rule() {
        let key = format!("{UNIT_OOM}pir-apm.service");
        let mut s = source("transparent", "publisher", 100, &[(&key, 2.)]);
        let mut old = Point {
            at: 50,
            ..Default::default()
        };
        old.values.gauges.insert(key.clone(), 1.);
        s.recent.push_back(old);
        let v = view(
            vec![service("transparent", Kind::History)],
            vec![("transparent-publisher", s)],
        );
        let f = conditions(&v, 105, &Policy::default());
        assert_eq!(
            firing(
                &f,
                "transparent",
                "oom_transparent-publisher_pir-apm.service"
            ),
            Some(true)
        );
        // Stale: unknown.
        let f = conditions(&v, 200, &Policy::default());
        assert_eq!(
            firing(
                &f,
                "transparent",
                "oom_transparent-publisher_pir-apm.service"
            ),
            None
        );
    }

    /// The deployed bucket edges: coordinator cycles use the shared
    /// observability bounds, worker prepare the shard server's.
    fn bounds(key: &str) -> Vec<Option<f64>> {
        let finite: &[f64] = if key.contains("prepare") {
            &[0.1, 0.25, 0.5, 1., 2., 4., 8., 16., 32., 64., 128., 300.]
        } else {
            &[
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1., 2., 5., 10., 30., 60., 120.,
                600.,
            ]
        };
        finite.iter().copied().map(Some).chain([None]).collect()
    }
    /// `n` samples of each listed duration in seconds, as one fresh source.
    fn timed_mix(role: &str, service: &str, key: &str, samples: &[(f64, f64)], at: u64) -> Source {
        let mut s = source(service, role, at, &[]);
        let bounds = bounds(key);
        let counts = bounds
            .iter()
            .map(|b| {
                samples
                    .iter()
                    .filter(|(seconds, _)| b.is_none_or(|b| b >= *seconds))
                    .map(|(_, n)| n)
                    .sum()
            })
            .collect();
        s.long.push_back((
            at,
            BTreeMap::from([(key.to_string(), Histogram { bounds, counts })]),
        ));
        s
    }
    fn timed(role: &str, service: &str, key: &str, seconds: f64, n: f64, at: u64) -> Source {
        timed_mix(role, service, key, &[(seconds, n)], at)
    }

    #[test]
    fn rebuild_and_prepare_fire_on_exceedance_at_bucket_edges() {
        let p = Policy::default();
        let at = 5_000;
        let cycle = |kind, name: &str, seconds| {
            let (role, key) = match kind {
                Kind::History => ("publisher", "transparent_publication_cycle_seconds"),
                Kind::Txid => ("display-controller", "txid_display_cycle_seconds"),
            };
            view(
                vec![service(name, kind)],
                vec![("c", timed(role, name, key, seconds, 10., at))],
            )
        };
        // A 31 s cycle is past the txid 30 s edge but well inside the history
        // 60 s warning; interpolating in the 30–60 s bucket used to fire it.
        let f = conditions(&cycle(Kind::History, "transparent", 31.), at, &p);
        assert_eq!(firing(&f, "transparent", "rebuild_warning"), Some(false));
        let f = conditions(&cycle(Kind::Txid, "txid", 31.), at, &p);
        assert_eq!(firing(&f, "txid", "rebuild_warning"), Some(true));
        assert_eq!(firing(&f, "txid", "rebuild_critical"), Some(false));
        let f = conditions(&cycle(Kind::Txid, "txid", 25.), at, &p);
        assert_eq!(firing(&f, "txid", "rebuild_warning"), Some(false));
        // Past 120 s pages for both kinds.
        let f = conditions(&cycle(Kind::History, "transparent", 130.), at, &p);
        assert_eq!(firing(&f, "transparent", "rebuild_critical"), Some(true));
        let f = conditions(&cycle(Kind::History, "transparent", 80.), at, &p);
        assert_eq!(firing(&f, "transparent", "rebuild_critical"), Some(false));
        // Worker prepare warns past 32 s and pages past 64 s; history reads the
        // total phase. A 34 s prepare used to page at the old 60 s limit.
        let worker = |samples: &[(f64, f64)]| {
            view(
                vec![service("transparent", Kind::History)],
                vec![(
                    "w",
                    timed_mix(
                        "recent-replica",
                        "transparent",
                        "transparent_shard_prepare_seconds|phase=total",
                        samples,
                        at,
                    ),
                )],
            )
        };
        let f = conditions(&worker(&[(34., 3.)]), at, &p);
        assert_eq!(firing(&f, "transparent", "prepare_w_warning"), Some(true));
        assert_eq!(firing(&f, "transparent", "prepare_w_critical"), Some(false));
        let f = conditions(&worker(&[(70., 3.)]), at, &p);
        assert_eq!(firing(&f, "transparent", "prepare_w_critical"), Some(true));
        let f = conditions(&worker(&[(10., 3.)]), at, &p);
        assert_eq!(firing(&f, "transparent", "prepare_w_warning"), Some(false));
        // Up to 5% of the window may exceed; more fires.
        let f = conditions(&worker(&[(10., 95.), (70., 5.)]), at, &p);
        assert_eq!(firing(&f, "transparent", "prepare_w_critical"), Some(false));
        let f = conditions(&worker(&[(10., 94.), (70., 6.)]), at, &p);
        assert_eq!(firing(&f, "transparent", "prepare_w_critical"), Some(true));
        // Overflow beyond the last bucket exceeds every limit.
        let f = conditions(&worker(&[(500., 3.)]), at, &p);
        assert_eq!(firing(&f, "transparent", "prepare_w_critical"), Some(true));
        // After fifteen minutes without a prepare it is unknown, never recovered.
        let f = conditions(&worker(&[(70., 3.)]), at + LONG_WINDOW + 1, &p);
        assert_eq!(firing(&f, "transparent", "prepare_w_critical"), None);
        let display = view(
            vec![service("txid", Kind::Txid)],
            vec![(
                "d",
                timed(
                    "display-worker",
                    "txid",
                    "transparent_display_prepare_seconds",
                    35.,
                    2.,
                    at,
                ),
            )],
        );
        assert_eq!(
            firing(&conditions(&display, at, &p), "txid", "prepare_d_warning"),
            Some(true)
        );
    }

    #[test]
    fn origin_disagreement_incident_retains_per_origin_evidence() {
        let mut svc = service("transparent", Kind::History);
        svc.public_origins = vec!["https://a.example".into(), "https://b.example".into()];
        svc.publication = publication::View {
            sampled_at: 100,
            canonical: Some(true),
            origins_agree: Some(false),
            map_sha256: Some("aa".repeat(32)),
            map_after: Some("aa".repeat(32)),
            previous_map: Some("bb".repeat(32)),
            map_seen_at: Some(90),
            origins: vec![
                publication::Origin {
                    origin: "https://a.example".into(),
                    observed_at: 99,
                    http_status: Some(200),
                    map_sha256: Some("aa".repeat(32)),
                    failure: None,
                },
                publication::Origin {
                    origin: "https://b.example".into(),
                    observed_at: 99,
                    http_status: Some(200),
                    map_sha256: Some("bb".repeat(32)),
                    failure: None,
                },
            ],
            ..Default::default()
        };
        let v = view(
            vec![svc],
            vec![("r", source("transparent", "recent-replica", 100, &[]))],
        );
        let f = conditions(&v, 105, &Policy::default());
        let origins = f.services["transparent"]
            .iter()
            .find(|c| c.key == "quality.transparent.publication_origins")
            .unwrap();
        assert_eq!(origins.firing, Some(true));
        assert_eq!(origins.hold_seconds, 180, "a lagging origin is propagation");
        assert!(origins
            .observed
            .starts_with("lagging origin; expected map aaaa"));
        assert!(origins.observed.contains(&format!(
            "https://b.example at 99: HTTP 200, map {}",
            "bb".repeat(32)
        )));
        // Past the window the origin is stuck, not propagating, so failed reads
        // that reset the pending dwell cannot keep postponing the alert.
        let mut v = v;
        v.services[0].publication.sampled_at = 270;
        let f = conditions(&v, 275, &Policy::default());
        let origins = f.services["transparent"]
            .iter()
            .find(|c| c.key == "quality.transparent.publication_origins")
            .unwrap();
        assert_eq!(
            (origins.firing, origins.hold_seconds),
            (Some(true), 30),
            "the window counts from publication"
        );
    }

    #[test]
    fn origin_serving_an_unknown_map_keeps_the_short_hold() {
        let mut svc = service("transparent", Kind::History);
        svc.public_origins = vec!["https://a.example".into()];
        svc.publication = publication::View {
            sampled_at: 100,
            canonical: Some(true),
            origins_agree: Some(false),
            map_sha256: Some("aa".repeat(32)),
            map_after: Some("aa".repeat(32)),
            previous_map: Some("bb".repeat(32)),
            origins: vec![publication::Origin {
                origin: "https://a.example".into(),
                observed_at: 99,
                http_status: Some(200),
                map_sha256: Some("cc".repeat(32)),
                failure: None,
            }],
            ..Default::default()
        };
        let v = view(
            vec![svc],
            vec![("r", source("transparent", "recent-replica", 100, &[]))],
        );
        let f = conditions(&v, 105, &Policy::default());
        let origins = f.services["transparent"]
            .iter()
            .find(|c| c.key == "quality.transparent.publication_origins")
            .unwrap();
        assert_eq!((origins.firing, origins.hold_seconds), (Some(true), 30));
        assert!(origins.observed.starts_with("map mismatch"));
    }

    #[test]
    fn display_worker_must_be_ready_on_the_published_map() {
        let map = "ab".repeat(32);
        let mut svc = service("txid", Kind::Txid);
        svc.publication = publication::View {
            sampled_at: 100,
            map_sha256: Some(map.clone()),
            ..Default::default()
        };
        let mut worker = source("txid", "display-worker", 100, &[]);
        worker.ready = Some(true);
        worker.publication = Some("cd".repeat(32));
        let mut v = view(vec![svc], vec![("k8", worker)]);
        let f = conditions(&v, 105, &Policy::default());
        assert_eq!(firing(&f, "txid", "ready_k8"), Some(true));
        v.sources.get_mut("k8").unwrap().publication = Some(map);
        let f = conditions(&v, 105, &Policy::default());
        assert_eq!(firing(&f, "txid", "ready_k8"), Some(false));
    }

    #[test]
    fn one_replica_fleet_sets_its_own_redundancy_floor() {
        let map = "ab".repeat(32);
        let mut svc = service("transparent", Kind::History);
        svc.publication = publication::View {
            sampled_at: 100,
            map_sha256: Some(map.clone()),
            ..Default::default()
        };
        let mut recent = source("transparent", "recent-replica", 100, &[]);
        recent.ready = Some(true);
        recent.publication = Some(map);
        let mut v = view(vec![svc], vec![("r", recent)]);
        let f = conditions(&v, 105, &Policy::default());
        assert_eq!(firing(&f, "transparent", "recent_redundancy"), Some(true));
        assert_eq!(
            firing(&f, "transparent", "recent_availability"),
            Some(false)
        );
        v.services[0].min_recent_replicas = 1;
        let f = conditions(&v, 105, &Policy::default());
        assert_eq!(firing(&f, "transparent", "recent_redundancy"), Some(false));
    }

    #[test]
    fn retired_resources_end_in_their_own_family() {
        let mut v = view(
            vec![service("txid", Kind::Txid)],
            vec![("k8", source("txid", "display-worker", 100, &[]))],
        );
        v.collected_at = 100;
        let incident = |key: &str, resource: &str| Incident {
            active: true,
            condition: Some(Condition {
                retired: false,
                key: key.into(),
                resource: resource.into(),
                severity: "warning".into(),
                firing: Some(true),
                observed: String::new(),
                threshold: String::new(),
                hold_seconds: 0,
                sample: 90,
            }),
            ..Default::default()
        };
        let incidents = [
            incident("quality.txid.coverage_gone", "gone"),
            incident("quality.txid.coverage_k8", "k8"),
            incident("quality.txid.stalled", "txid"),
            // Keys of the single-service shape are retired by the shared family.
            incident(
                "quality_coverage_transparent-publisher",
                "transparent-publisher",
            ),
            incident("quality.removed.stalled", "removed"),
            incident("quality_storage", "history"),
        ];
        let mut f = conditions(&v, 100, &Policy::default());
        retire(&v, &incidents, 100, &mut f);
        let retired = |list: &[Condition]| {
            list.iter()
                .filter(|c| c.retired)
                .map(|c| c.key.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(retired(&f.services["txid"]), ["quality.txid.coverage_gone"]);
        assert_eq!(
            retired(&f.shared),
            [
                "quality_coverage_transparent-publisher",
                "quality.removed.stalled"
            ]
        );
        // An invalid service inventory retires nothing of that service.
        v.services[0].inventory_error = true;
        let mut f = conditions(&v, 100, &Policy::default());
        retire(&v, &incidents, 100, &mut f);
        assert!(retired(&f.services["txid"]).is_empty());
    }

    #[test]
    fn capacity_incidents_retire_once_their_row_leaves_the_alerts() {
        let p = Policy::default();
        let now = 100 * super::super::capacity::DAY;
        let mut v = view(vec![], vec![]);
        v.collected_at = now;
        // One host seen a minute ago, one last seen two hours ago (still listed).
        let daily = |host: &str, at: u64| super::super::capacity::Daily {
            host: host.into(),
            day: at / super::super::capacity::DAY,
            at,
            memory_total: Some(100.),
            min_available: Some(50.),
            ..Default::default()
        };
        v.capacity = super::super::capacity::build(
            &[daily("gone", now - 7200), daily("here", now - 60)],
            &Default::default(),
            now,
            &p,
        );
        assert_eq!(v.capacity.rows.len(), 2);
        let incident = |host: &str| Incident {
            active: true,
            condition: Some(Condition {
                retired: false,
                key: format!("transparent_capacity_floor_warning_{host}"),
                resource: host.into(),
                severity: "warning".into(),
                firing: Some(true),
                observed: String::new(),
                threshold: String::new(),
                hold_seconds: 0,
                sample: now - 7200,
            }),
            ..Default::default()
        };
        let mut f = conditions(&v, now, &p);
        retire(&v, &[incident("gone"), incident("here")], now, &mut f);
        let retired: Vec<_> = f
            .capacity
            .iter()
            .filter(|c| c.retired)
            .map(|c| c.key.as_str())
            .collect();
        assert_eq!(retired, ["transparent_capacity_floor_warning_gone"]);
    }

    #[test]
    fn policy_defaults_are_valid_and_reject_inverted_limits() {
        assert!(Policy::default().valid());
        let p: Policy = serde_json::from_str(r#"{"txid_rebuild_warning_seconds": 130}"#).unwrap();
        assert!(!p.valid());
        assert!(serde_json::from_str::<Policy>(r#"{"unknown": 1}"#).is_err());
    }
}
