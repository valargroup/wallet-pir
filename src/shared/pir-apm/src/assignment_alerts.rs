//! Enhance domain assignment alerts, a separate family with its own rollout mode
//! (`PIR_APM_ASSIGNMENT_ALERT_MODE`, default `shadow`).
//!
//! A new domain opens the moment the newest one fills, and no role serves a
//! domain it was not configured for, so assignment must precede growth. See
//! [`crate::placement::rollover`] for how routers and workers prove it.
use crate::dashboard::DashboardData;
use pir_apm::incidents::Condition;

pub const MODE_VAR: &str = "PIR_APM_ASSIGNMENT_ALERT_MODE";

/// About 34,000 records before the next domain opens: a few hours at the
/// 2026-11-09 mainnet peak, weeks at the October 2026 rate.
const CRITICAL_ROWS: f64 = 1_024.;

pub fn conditions(d: &DashboardData, now: u64) -> Vec<Condition> {
    let mut out = Vec::new();
    let mut add = |key: &str,
                   severity: &str,
                   firing: Option<bool>,
                   observed: String,
                   threshold: &str,
                   hold: u64,
                   sample: u64| {
        out.push(Condition {
            retired: false,
            key: format!("assignment_{key}"),
            resource: "placement".into(),
            severity: severity.into(),
            firing,
            observed,
            threshold: threshold.into(),
            hold_seconds: hold,
            window_seconds: 0,
            sample,
        })
    };
    let rollover = crate::placement::rollover(d, now);
    let unassigned = rollover.as_ref().and_then(|r| r.unassigned.clone());
    add(
        "unassigned",
        "critical",
        unassigned.as_ref().map(|u| !u.is_empty()),
        match &unassigned {
            Some(u) if !u.is_empty() => {
                format!("published domains {u:?} have no configured router and worker")
            }
            Some(_) => "every published domain has a configured router and worker".into(),
            None => "unavailable".into(),
        },
        "a published domain has no configured router and worker for 60s",
        60,
        rollover.as_ref().map_or(now, |r| r.unassigned_sample),
    );
    let next = rollover.as_ref().map(|r| (r, &r.next_domain));
    let describe = |rows: Option<f64>| {
        rollover
            .as_ref()
            .map(|r| {
                format!(
                    "{} rows before domain {} opens; {}",
                    rows.map_or("unknown".into(), |n| format!("{n:.0}")),
                    r.next,
                    match (r.exhausted, r.next_domain.assigned) {
                        (true, _) => "it would exceed the protocol's 24-domain limit",
                        (_, Some(true)) => "the publication it creates can be served",
                        (_, Some(false)) => "the publication it creates cannot be served",
                        (_, None) => "router or worker configuration unknown",
                    }
                )
            })
            .unwrap_or_else(|| "unavailable".into())
    };
    // The warning gives a full domain of lead time: it does not wait for rows
    // to run low, since adding workers or a router takes longer than that.
    add(
        "rollover_warning",
        "warning",
        next.and_then(|(_, n)| n.assigned.map(|a| !a)),
        describe(rollover.as_ref().and_then(|r| r.rows_left)),
        "the publication the next domain creates cannot be served",
        0,
        next.map_or(now, |(_, n)| n.sample),
    );
    let critical = next.map(|(r, n)| match (r.rows_left, n.assigned) {
        (_, Some(true)) => (Some(false), n.sample),
        (Some(rows), _) if rows >= CRITICAL_ROWS => (Some(false), r.rows_sample),
        (Some(_), Some(false)) => (Some(true), n.sample.min(r.rows_sample)),
        _ => (None, now),
    });
    add(
        "rollover_critical",
        "critical",
        critical.and_then(|c| c.0),
        describe(rollover.as_ref().and_then(|r| r.rows_left)),
        "fewer than 1,024 rows before a domain no one can serve opens",
        0,
        critical.map_or(now, |c| c.1),
    );
    // An unknown answer must not look healthy: missing evidence pages on its
    // own. Rows matter only when the next domain cannot be served. A healthy
    // answer carries the oldest evidence it rests on, so one scrape cannot
    // count as two recovery samples.
    let evidence = rollover.as_ref().and_then(|r| {
        let next = r.next_domain.assigned?;
        let critical = critical?.0;
        r.unassigned.as_ref()?;
        critical?;
        if r.incomplete {
            return None;
        }
        let mut sample = r.unassigned_sample.min(r.next_domain.sample);
        if !next {
            sample = sample.min(r.rows_sample);
        }
        Some(sample)
    });
    for (severity, hold) in [("warning", 45), ("critical", 120)] {
        add(
            &format!("coverage_{severity}"),
            severity,
            Some(evidence.is_none()),
            if evidence.is_some() {
                "fresh"
            } else {
                "unavailable"
            }
            .into(),
            &format!("router, worker or publisher assignment evidence unavailable for {hold}s"),
            hold,
            evidence.unwrap_or(now),
        );
    }
    out
}
