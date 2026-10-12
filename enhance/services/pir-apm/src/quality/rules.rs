//! Service-quality alert rules.
//!
//! Conditions split into families with independent rollout modes: the shared
//! `quality` family (history writes, the Enhance inventory, and Enhance and
//! Status sources), one family per configured Transparent service (keys
//! `quality.<service>.<rule>`), and the Transparent capacity family. Rules pick
//! by service kind and source role, never by service name.
use super::{services::Kind, ServiceView, Source, View};
use pir_apm::incidents::{Condition, Incident};
use serde::Deserialize;
use std::collections::BTreeMap;

/// Rebuild and prepare percentiles are taken over this window.
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
            history_rebuild_warning_seconds: 45.,
            txid_rebuild_warning_seconds: 20.,
            rebuild_critical_seconds: 75.,
            prepare_warning_seconds: 30.,
            prepare_critical_seconds: 60.,
            capacity_warning_days: 30.,
            capacity_critical_days: 7.,
            host_floor_ratio: 0.20,
            unit_forecast_ratio: 0.90,
            unit_memory_ratio: 0.85,
            unit_memory_hold_seconds: 600,
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
}

/// p95 over the fifteen-minute window: unknown without a sample, and an
/// overflow beyond the last finite bucket counts as over every limit.
fn p95(s: &Source, key: &str, now: u64) -> Option<(Option<f64>, f64)> {
    let h = s.long_window(now).remove(key)?;
    let count = h.counts.last().copied().unwrap_or(0.);
    (s.fresh(now) && count > 0.).then(|| (h.quantile(0.95), count))
}

#[allow(clippy::too_many_arguments)]
fn p95_rules(
    a: &mut Adder,
    key: &str,
    resource: &str,
    s: &Source,
    histogram: &str,
    what: &str,
    limits: [f64; 2],
    now: u64,
) {
    let observed = p95(s, histogram, now);
    for (level, limit) in [("warning", limits[0]), ("critical", limits[1])] {
        a.add(
            format!("{key}_{level}"),
            resource,
            observed.map(|(q, _)| q.is_none_or(|q| q > limit)),
            level,
            60,
            s.sampled_at,
            observed
                .map(|(q, n)| match q {
                    Some(q) => format!("{what} p95 {q:.1}s over 15m ({n:.0} samples)"),
                    None => format!("{what} p95 beyond the last bucket ({n:.0} samples)"),
                })
                .unwrap_or_else(|| "no samples in 15m".into()),
            format!("{what} p95 over 15m >{limit}s"),
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
            a.add(
                format!("publication_{key}"),
                name,
                verdict.filter(|_| publication_fresh).map(|v| !v),
                "critical",
                if key == "canonical" { 0 } else { 30 },
                publication.sampled_at,
                "Independent public publication check",
                "public anchor must be canonical; every origin must agree",
            );
            a.add(
                format!("publication_{key}_coverage"),
                name,
                Some(!publication_fresh || verdict.is_none()),
                "warning",
                45,
                now,
                "Publication verification coverage",
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
            p95_rules(
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
            a.add(
                "recent_redundancy",
                name,
                known.then_some(ready < 2),
                "warning",
                120,
                now,
                format!("{ready} ready replicas"),
                "fewer than two recent replicas for 120s",
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
    // so the lag and unpublished-age rules cover that window.
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
            0,
            sample,
            since
                .map(|v| format!("last successful publication {v:.0}s ago"))
                .unwrap_or_else(|| "unknown".into()),
            format!("no successful publication for {budget}s while the node is ahead"),
        );
    }
    let rebuild_warning = match kind {
        Kind::History => p.history_rebuild_warning_seconds,
        Kind::Txid => p.txid_rebuild_warning_seconds,
    };
    p95_rules(
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
/// a service no longer configured, or a host that left the capacity view.
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
            && !view.capacity.rows.iter().any(|r| r.host == c.resource)
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

    fn timed(role: &str, service: &str, key: &str, seconds: f64, n: f64, at: u64) -> Source {
        let mut s = source(service, role, at, &[]);
        let bounds = vec![
            Some(10.),
            Some(20.),
            Some(30.),
            Some(45.),
            Some(60.),
            Some(75.),
            Some(120.),
            None,
        ];
        let counts = bounds
            .iter()
            .map(|b| {
                if b.is_none_or(|b| b >= seconds) {
                    n
                } else {
                    0.
                }
            })
            .collect();
        s.long.push_back((
            at,
            BTreeMap::from([(key.to_string(), Histogram { bounds, counts })]),
        ));
        s
    }

    #[test]
    fn rebuild_and_prepare_percentiles_use_kind_thresholds() {
        let p = Policy::default();
        let at = 5_000;
        // A 25 s txid cycle warns (limit 20 s); the same history cycle does not (45 s).
        let txid = view(
            vec![service("txid", Kind::Txid)],
            vec![(
                "c",
                timed(
                    "display-controller",
                    "txid",
                    "txid_display_cycle_seconds",
                    25.,
                    10.,
                    at,
                ),
            )],
        );
        let f = conditions(&txid, at, &p);
        assert_eq!(firing(&f, "txid", "rebuild_warning"), Some(true));
        assert_eq!(firing(&f, "txid", "rebuild_critical"), Some(false));
        let history = view(
            vec![service("transparent", Kind::History)],
            vec![(
                "c",
                timed(
                    "publisher",
                    "transparent",
                    "transparent_publication_cycle_seconds",
                    25.,
                    10.,
                    at,
                ),
            )],
        );
        let f = conditions(&history, at, &p);
        assert_eq!(firing(&f, "transparent", "rebuild_warning"), Some(false));
        // 80 s pages for both kinds.
        let history = view(
            vec![service("transparent", Kind::History)],
            vec![(
                "c",
                timed(
                    "publisher",
                    "transparent",
                    "transparent_publication_cycle_seconds",
                    80.,
                    10.,
                    at,
                ),
            )],
        );
        assert_eq!(
            firing(
                &conditions(&history, at, &p),
                "transparent",
                "rebuild_critical"
            ),
            Some(true)
        );
        // Worker prepare: 40 s warns, 70 s pages; history reads the total phase.
        let worker = |seconds| {
            view(
                vec![service("transparent", Kind::History)],
                vec![(
                    "w",
                    timed(
                        "recent-replica",
                        "transparent",
                        "transparent_shard_prepare_seconds|phase=total",
                        seconds,
                        3.,
                        at,
                    ),
                )],
            )
        };
        let f = conditions(&worker(40.), at, &p);
        assert_eq!(firing(&f, "transparent", "prepare_w_warning"), Some(true));
        assert_eq!(firing(&f, "transparent", "prepare_w_critical"), Some(false));
        let f = conditions(&worker(70.), at, &p);
        assert_eq!(firing(&f, "transparent", "prepare_w_critical"), Some(true));
        let f = conditions(&worker(10.), at, &p);
        assert_eq!(firing(&f, "transparent", "prepare_w_warning"), Some(false));
        // Overflow beyond the last bucket exceeds every limit.
        let f = conditions(&worker(500.), at, &p);
        assert_eq!(firing(&f, "transparent", "prepare_w_critical"), Some(true));
        // After fifteen minutes without a prepare it is unknown, never recovered.
        let f = conditions(&worker(70.), at + LONG_WINDOW + 1, &p);
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
    fn policy_defaults_are_valid_and_reject_inverted_limits() {
        assert!(Policy::default().valid());
        let p: Policy = serde_json::from_str(r#"{"txid_rebuild_warning_seconds": 80}"#).unwrap();
        assert!(!p.valid());
        assert!(serde_json::from_str::<Policy>(r#"{"unknown": 1}"#).is_err());
    }
}
