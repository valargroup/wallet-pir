//! Enhance memory and growth alerts, a separate family with its own rollout mode
//! (`PIR_APM_CAPACITY_ALERT_MODE`, default `shadow`).
//!
//! Workers and packing routers report their host memory and their own cgroup's
//! memory, swap, reclaim and OOM counters. The worker also reports how much of
//! its resident limit its current assignment is modelled to need. The
//! coordinator reports placement rows left and rows left before the growing
//! shard fills. Thresholds are early warnings for operators, not a qualification.
use crate::{dashboard::DashboardData, fleet::Worker};
use pir_apm::incidents::Condition;
use std::{
    collections::{BTreeMap, VecDeque},
    time::{SystemTime, UNIX_EPOCH},
};

pub const MODE_VAR: &str = "PIR_APM_CAPACITY_ALERT_MODE";

/// Counter changes are judged over this window, so an increase stays visible
/// for five minutes and then recovers.
const COUNTER_WINDOW_SECONDS: u64 = 300;
const HOST_AVAILABLE_RATIO: f64 = 0.15;
const SWAP_WARNING_BYTES: f64 = 64. * 1024. * 1024.;
const SWAP_CRITICAL_BYTES: f64 = 512. * 1024. * 1024.;
const MODEL_WARNING_RATIO: f64 = 0.85;
/// One storage shard of rows: about five months of growth at the October 2026 rate.
const PLACEMENT_WARNING_ROWS: f64 = 32_768.;
const PLACEMENT_CRITICAL_ROWS: f64 = 8_192.;
/// About three weeks before the growing shard fills at the October 2026 rate.
const ROLLOVER_WARNING_ROWS: f64 = 4_096.;

/// Recent (sample time, reclaim events, OOM kills) per worker or router.
pub type CounterHistory = BTreeMap<String, VecDeque<(u64, f64, f64)>>;

fn epoch(t: Option<SystemTime>) -> u64 {
    t.and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn fresh(at: u64, now: u64) -> bool {
    at > 0 && at <= now && now - at <= 45
}

pub fn conditions(d: &DashboardData, now: u64, history: &mut CounterHistory) -> Vec<Condition> {
    let mut out = Vec::new();
    let mut add = |key: String,
                   resource: String,
                   severity: &str,
                   firing: Option<bool>,
                   observed: String,
                   threshold: &str,
                   hold: u64,
                   sample: u64| {
        out.push(Condition {
            retired: false,
            key: format!("capacity_{key}"),
            resource,
            severity: severity.into(),
            firing,
            observed,
            threshold: threshold.into(),
            hold_seconds: hold,
            window_seconds: 0,
            sample,
        })
    };
    let roles = d
        .fleet
        .iter()
        .map(|(name, w)| (format!("worker_{name}"), w, true))
        .chain(
            d.packing_routers
                .iter()
                .map(|(name, r)| (format!("router_{name}"), &r.sample, false)),
        );
    let mut seen = Vec::new();
    for (resource, sample, worker) in roles {
        seen.push(resource.clone());
        evaluate_role(&mut add, &resource, sample, worker, now, history);
    }
    history.retain(|name, _| seen.contains(name));

    let at = epoch(d.last_scrape);
    let coordinator = fresh(at, now) && d.scrape_error.is_none();
    let gauge = |name: &str| {
        d.snapshot_gauges
            .get(name)
            .copied()
            .filter(|v| coordinator && v.is_finite() && *v >= 0.)
    };
    let remaining = gauge("enhance_capacity_remaining_rows");
    for (severity, limit, threshold) in [
        (
            "warning",
            PLACEMENT_WARNING_ROWS,
            "fewer than 32,768 placement rows (one shard) left",
        ),
        (
            "critical",
            PLACEMENT_CRITICAL_ROWS,
            "fewer than 8,192 placement rows left",
        ),
    ] {
        add(
            format!("placement_{severity}"),
            "placement".into(),
            severity,
            remaining.map(|rows| rows < limit),
            remaining
                .map(|rows| format!("{rows:.0} rows of worker placement left"))
                .unwrap_or_else(|| "unavailable".into()),
            threshold,
            0,
            at,
        );
    }
    let rollover = gauge("enhance_rows_before_next_carveout");
    add(
        "shard_rollover".into(),
        "placement".into(),
        "warning",
        rollover.map(|rows| rows < ROLLOVER_WARNING_ROWS),
        rollover
            .map(|rows| format!("{rows:.0} rows before the growing shard fills"))
            .unwrap_or_else(|| "unavailable".into()),
        "fewer than 4,096 rows before the growing shard fills",
        0,
        at,
    );
    out
}

#[allow(clippy::type_complexity)]
fn evaluate_role(
    add: &mut impl FnMut(String, String, &str, Option<bool>, String, &str, u64, u64),
    resource: &str,
    sample: &Worker,
    worker: bool,
    now: u64,
    history: &mut CounterHistory,
) {
    let at = epoch(sample.success);
    let ok = fresh(at, now) && sample.error.is_none();
    let value = |key: &str| {
        sample
            .values
            .get(key)
            .copied()
            .filter(|v| ok && v.is_finite() && *v >= 0.)
    };
    let available = value("host_memory_available_bytes");
    let total = value("host_memory_total_bytes").filter(|t| *t > 0.);
    let swap = value("cgroup_swap_bytes");
    let reclaim = value("cgroup_memory_high_events_total");
    let oom = value("cgroup_oom_kills_total");
    // A reachable role that stops reporting memory must not look healthy.
    add(
        format!("coverage_{resource}"),
        resource.into(),
        "warning",
        Some(!ok || available.is_none() || total.is_none() || swap.is_none() || oom.is_none()),
        if ok {
            "memory fields"
        } else {
            "role unavailable"
        }
        .into(),
        "memory observation unavailable for 120s",
        120,
        now,
    );
    let ratio = available.zip(total).map(|(a, t)| a / t);
    add(
        format!("memory_{resource}"),
        resource.into(),
        "warning",
        ratio.map(|r| r < HOST_AVAILABLE_RATIO),
        ratio
            .map(|r| format!("{:.1}% of host memory available", r * 100.))
            .unwrap_or_else(|| "unavailable".into()),
        "host memory <15% available for 120s",
        120,
        at,
    );
    for (severity, limit, hold, threshold) in [
        (
            "warning",
            SWAP_WARNING_BYTES,
            300,
            "cgroup swap >64 MiB for 300s",
        ),
        (
            "critical",
            SWAP_CRITICAL_BYTES,
            120,
            "cgroup swap >512 MiB for 120s",
        ),
    ] {
        add(
            format!("swap_{severity}_{resource}"),
            resource.into(),
            severity,
            swap.map(|s| s > limit),
            swap.map(|s| format!("{:.0} MiB swapped", s / 1048576.))
                .unwrap_or_else(|| "unavailable".into()),
            threshold,
            hold,
            at,
        );
    }
    let h = history.entry(resource.into()).or_default();
    let (mut reclaim_grew, mut oom_grew) = (None, None);
    if let (Some(reclaim), Some(oom)) = (reclaim, oom) {
        // Counters restart with the service's cgroup; a decrease is a restart, not a recovery.
        if h.back()
            .is_some_and(|(t, r, o)| at > *t && (reclaim < *r || oom < *o))
        {
            h.clear();
        }
        if h.back().is_none_or(|(t, _, _)| at > *t) {
            h.push_back((at, reclaim, oom));
        }
        while h
            .front()
            .is_some_and(|(t, _, _)| now.saturating_sub(*t) > COUNTER_WINDOW_SECONDS)
        {
            h.pop_front();
        }
        if let Some((_, r0, o0)) = h.front() {
            reclaim_grew = Some(reclaim > *r0);
            oom_grew = Some(oom > *o0);
        }
    }
    add(
        format!("reclaim_{resource}"),
        resource.into(),
        "warning",
        reclaim_grew,
        reclaim
            .map(|r| format!("{r:.0} memory.high events since service start"))
            .unwrap_or_else(|| "unavailable".into()),
        "cgroup crossed memory.high in the last 5 minutes",
        0,
        at,
    );
    add(
        format!("oom_{resource}"),
        resource.into(),
        "critical",
        oom_grew,
        oom.map(|o| format!("{o:.0} OOM kills since service start"))
            .unwrap_or_else(|| "unavailable".into()),
        "new OOM kill in the last 5 minutes",
        0,
        at,
    );
    if !worker {
        return;
    }
    let model = value("model_total_bytes").zip(value("model_limit_bytes").filter(|l| *l > 0.));
    add(
        format!("model_{resource}"),
        resource.into(),
        "warning",
        model.map(|(t, l)| t / l >= MODEL_WARNING_RATIO),
        model
            .map(|(t, l)| {
                format!(
                    "assignment modelled at {:.0}% of the resident limit",
                    t / l * 100.
                )
            })
            .unwrap_or_else(|| "unavailable".into()),
        "assignment modelled at >=85% of the resident limit for 300s",
        300,
        at,
    );
    let within = value("model_within_limit");
    add(
        format!("model_limit_{resource}"),
        resource.into(),
        "critical",
        within.map(|w| w < 1.),
        within
            .map(|w| if w < 1. { "over" } else { "within" }.to_string() + " the resident limit")
            .unwrap_or_else(|| "unavailable".into()),
        "assignment modelled over the resident limit for 120s",
        120,
        at,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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

    fn worker(now: u64, values: &[(&str, f64)]) -> Worker {
        Worker {
            name: "w1".into(),
            success: Some(UNIX_EPOCH + Duration::from_secs(now)),
            values: values.iter().map(|(k, v)| ((*k).into(), *v)).collect(),
            ..Default::default()
        }
    }

    fn healthy(reclaim: f64, oom: f64) -> Vec<(&'static str, f64)> {
        vec![
            ("host_memory_available_bytes", 6e9),
            ("host_memory_total_bytes", 8e9),
            ("cgroup_swap_bytes", 0.),
            ("cgroup_memory_high_events_total", reclaim),
            ("cgroup_oom_kills_total", oom),
            ("model_total_bytes", 2.7e9),
            ("model_limit_bytes", 7e9),
            ("model_within_limit", 1.),
        ]
    }

    fn find(c: &[Condition], key: &str) -> Option<bool> {
        c.iter().find(|c| c.key == key).unwrap().firing
    }

    #[test]
    fn healthy_worker_is_quiet_and_swap_and_model_pressure_fire() {
        let now = 1_000;
        let mut d = view(now);
        d.fleet.insert("w1".into(), worker(now, &healthy(0., 0.)));
        let c = conditions(&d, now, &mut CounterHistory::new());
        assert!(c
            .iter()
            .filter(|c| c.resource == "worker_w1")
            .all(|c| c.firing == Some(false)));

        let mut values = healthy(0., 0.);
        values.retain(|(k, _)| !matches!(*k, "cgroup_swap_bytes" | "model_total_bytes"));
        values.push(("cgroup_swap_bytes", 120. * 1048576.));
        values.push(("model_total_bytes", 6.2e9));
        d.fleet.insert("w1".into(), worker(now, &values));
        let c = conditions(&d, now, &mut CounterHistory::new());
        assert_eq!(find(&c, "capacity_swap_warning_worker_w1"), Some(true));
        assert_eq!(find(&c, "capacity_swap_critical_worker_w1"), Some(false));
        assert_eq!(find(&c, "capacity_model_worker_w1"), Some(true));
    }

    #[test]
    fn missing_memory_fields_raise_coverage_and_never_recover_rules() {
        let now = 1_000;
        let mut d = view(now);
        // A worker running a binary from before memory reporting.
        d.fleet
            .insert("w1".into(), worker(now, &[("model_within_limit", 1.)]));
        let c = conditions(&d, now, &mut CounterHistory::new());
        assert_eq!(find(&c, "capacity_coverage_worker_w1"), Some(true));
        assert_eq!(find(&c, "capacity_swap_warning_worker_w1"), None);
        assert_eq!(find(&c, "capacity_oom_worker_w1"), None);
        // A stale sample is unknown, not healthy.
        d.fleet
            .insert("w1".into(), worker(now - 100, &healthy(0., 0.)));
        let c = conditions(&d, now, &mut CounterHistory::new());
        assert_eq!(find(&c, "capacity_memory_worker_w1"), None);
        assert_eq!(find(&c, "capacity_coverage_worker_w1"), Some(true));
    }

    #[test]
    fn counter_increases_fire_for_five_minutes_and_restarts_do_not() {
        let mut d = view(0);
        let mut h = CounterHistory::new();
        let at = |d: &mut DashboardData, h: &mut CounterHistory, t: u64, r: f64, o: f64| {
            d.last_scrape = Some(UNIX_EPOCH + Duration::from_secs(t));
            d.fleet.insert("w1".into(), worker(t, &healthy(r, o)));
            conditions(d, t, h)
        };
        let c = at(&mut d, &mut h, 1_000, 3., 0.);
        assert_eq!(find(&c, "capacity_reclaim_worker_w1"), Some(false));
        let c = at(&mut d, &mut h, 1_010, 5., 1.);
        assert_eq!(find(&c, "capacity_reclaim_worker_w1"), Some(true));
        assert_eq!(find(&c, "capacity_oom_worker_w1"), Some(true));
        let c = at(&mut d, &mut h, 1_400, 5., 1.);
        assert_eq!(find(&c, "capacity_oom_worker_w1"), Some(false));
        // Service restart: counters return to zero, which is not an increase.
        let c = at(&mut d, &mut h, 1_410, 0., 0.);
        assert_eq!(find(&c, "capacity_oom_worker_w1"), Some(false));
    }

    #[test]
    fn placement_and_rollover_use_coordinator_rows_and_stale_scrapes_are_unknown() {
        let now = 1_000;
        let mut d = view(now);
        d.snapshot_gauges
            .insert("enhance_capacity_remaining_rows".into(), 141_747.);
        d.snapshot_gauges
            .insert("enhance_rows_before_next_carveout".into(), 3_000.);
        let c = conditions(&d, now, &mut CounterHistory::new());
        assert_eq!(find(&c, "capacity_placement_warning"), Some(false));
        assert_eq!(find(&c, "capacity_shard_rollover"), Some(true));
        d.snapshot_gauges
            .insert("enhance_capacity_remaining_rows".into(), 20_000.);
        let c = conditions(&d, now, &mut CounterHistory::new());
        assert_eq!(find(&c, "capacity_placement_warning"), Some(true));
        assert_eq!(find(&c, "capacity_placement_critical"), Some(false));
        d.last_scrape = Some(UNIX_EPOCH + Duration::from_secs(now - 100));
        let c = conditions(&d, now, &mut CounterHistory::new());
        assert_eq!(find(&c, "capacity_placement_warning"), None);
    }
}
