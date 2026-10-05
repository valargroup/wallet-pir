//! Optional `scaling` alert family over the Transparent scaler's `scaler/status.json`
//! (format in `transparent/docs/elastic-recent.md`).
//!
//! Enabled only when `PIR_APM_SCALER_STATUS` names that file on this host. The family
//! has its own rollout mode (`PIR_APM_SCALING_ALERT_MODE`, default `shadow`) and shares
//! the incident database and Slack outbox with the other families. A missing,
//! oversized, unparseable or stale file makes every rule unknown, never healthy.
use anyhow::Result;
use pir_apm::incidents::{Condition, Store};
use serde::Deserialize;
use std::{
    collections::{BTreeSet, HashMap},
    io::Read,
    path::{Path, PathBuf},
};

pub const STATUS_VAR: &str = "PIR_APM_SCALER_STATUS";
pub const MODE_VAR: &str = "PIR_APM_SCALING_ALERT_MODE";
const RESOURCE: &str = "transparent-scaler";
const OPERATION_PREFIX: &str = "scaling_operation_";
const MAX_STATUS_BYTES: u64 = 1024 * 1024;
/// Contents older than this are unknown; only the heartbeat rule still reads them.
const STALE_SECONDS: u64 = 90;
const HEARTBEAT_SECONDS: u64 = 180;
const COVERAGE_HOLD_SECONDS: u64 = 180;
const DEMAND_HOLD_SECONDS: u64 = 1800;
const SERVING_HOLD_SECONDS: u64 = 60;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    pub status: PathBuf,
    pub shadow: bool,
}

impl Settings {
    pub fn from_env() -> Result<Option<Self>> {
        let vars: HashMap<String, String> = std::env::vars()
            .filter(|(name, _)| name == STATUS_VAR || name == MODE_VAR)
            .collect();
        Self::from_map(&vars)
    }

    /// Absent `PIR_APM_SCALER_STATUS` disables the family; no other state changes.
    pub fn from_map(vars: &HashMap<String, String>) -> Result<Option<Self>> {
        let get = |name: &str| {
            vars.get(name)
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
        };
        let mode = get(MODE_VAR).unwrap_or("shadow");
        anyhow::ensure!(
            matches!(mode, "shadow" | "active"),
            "{MODE_VAR} must be shadow or active"
        );
        let Some(status) = get(STATUS_VAR) else {
            anyhow::ensure!(mode != "active", "{MODE_VAR}=active requires {STATUS_VAR}");
            return Ok(None);
        };
        let status = PathBuf::from(status);
        anyhow::ensure!(
            status.is_absolute(),
            "{STATUS_VAR} must be an absolute path"
        );
        Ok(Some(Self {
            status,
            shadow: mode == "shadow",
        }))
    }
}

/// The fields of `scaler/status.json` schema 1 this family reads. Other fields are
/// ignored; an absent or `null` field makes only the rules that need it unknown.
#[derive(Debug, Deserialize)]
struct Status {
    schema: u64,
    updated_unix: f64,
    heartbeat_unix: Option<f64>,
    desired_recent: Option<u64>,
    serving_recent: Option<u64>,
    holds: Option<Vec<String>>,
    operation: Option<Operation>,
    budget: Option<Budget>,
    forecast: Option<Forecast>,
    awaiting_operator: Option<Vec<serde_json::Value>>,
    orphans: Option<Vec<serde_json::Value>>,
}
#[derive(Debug, Deserialize)]
struct Operation {
    id: Option<String>,
    phase: Option<String>,
    age_seconds: Option<f64>,
    deadline_exceeded: Option<bool>,
    fenced: Option<bool>,
}
#[derive(Debug, Deserialize)]
struct Budget {
    actions_left: Option<i64>,
    destroys_left: Option<i64>,
}
#[derive(Debug, Deserialize)]
struct Forecast {
    days_to_recent_budget: Option<f64>,
}

/// One read of the status file at `now`.
enum Observation {
    /// Missing, oversized, unparseable, unsupported schema or future timestamp.
    Invalid(&'static str),
    /// `sample` is `updated_unix`; `fresh` means it is at most `STALE_SECONDS` old.
    Valid {
        status: Box<Status>,
        sample: u64,
        fresh: bool,
    },
}

fn unix(value: f64) -> Option<u64> {
    (value.is_finite() && value > 0. && value < 1e12).then(|| value.floor() as u64)
}

fn parse(bytes: &[u8], now: u64) -> Observation {
    if bytes.len() as u64 > MAX_STATUS_BYTES {
        return Observation::Invalid("status file exceeds 1 MiB");
    }
    let Ok(status) = serde_json::from_slice::<Status>(bytes) else {
        return Observation::Invalid("invalid status JSON");
    };
    if status.schema != 1 {
        return Observation::Invalid("unsupported status schema");
    }
    let Some(sample) = unix(status.updated_unix).filter(|t| *t <= now) else {
        return Observation::Invalid("invalid updated_unix");
    };
    Observation::Valid {
        status: Box::new(status),
        sample,
        fresh: now - sample <= STALE_SECONDS,
    }
}

fn read(path: &Path, now: u64) -> Observation {
    let mut bytes = Vec::new();
    let Ok(file) = std::fs::File::open(path) else {
        return Observation::Invalid("status file unavailable");
    };
    if file
        .take(MAX_STATUS_BYTES + 1)
        .read_to_end(&mut bytes)
        .is_err()
    {
        return Observation::Invalid("status file unreadable");
    }
    parse(&bytes, now)
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.into()
    } else {
        format!("{}…", text.chars().take(max).collect::<String>())
    }
}

/// Count and a bounded preview; the file is local but its strings reach Slack.
fn summary(items: Vec<String>) -> String {
    if items.is_empty() {
        return "none".into();
    }
    let shown: Vec<_> = items.iter().take(3).map(|s| truncate(s, 80)).collect();
    let more = items.len().saturating_sub(shown.len());
    format!(
        "{} ({}){}",
        items.len(),
        shown.join("; "),
        if more > 0 {
            format!("; +{more} more")
        } else {
            String::new()
        }
    )
}

fn values(items: &[serde_json::Value]) -> Vec<String> {
    items
        .iter()
        .map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            v => v.to_string(),
        })
        .collect()
}

fn known<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map_or_else(|| "unknown".into(), |v| v.to_string())
}

/// Incident keys carry the operation id so one stuck operation is one incident.
fn operation_id(id: Option<&str>) -> String {
    let id: String = id
        .unwrap_or("")
        .chars()
        .take(64)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if id.is_empty() {
        "unidentified".into()
    } else {
        id
    }
}

/// `open_operations` are the stored conditions of active per-operation incidents.
/// One the fresh status no longer names is healthy (recovers after two fresh
/// samples); while the status is unknown it stays unknown.
fn conditions(
    observation: &Observation,
    now: u64,
    open_operations: &[Condition],
) -> Vec<Condition> {
    let mut out = Vec::new();
    let mut add = |key: String,
                   severity: &str,
                   firing: Option<bool>,
                   observed: String,
                   threshold: String,
                   hold_seconds: u64,
                   sample: u64| {
        out.push(Condition {
            retired: false,
            key,
            resource: RESOURCE.into(),
            severity: severity.into(),
            firing,
            observed,
            threshold,
            hold_seconds,
            sample,
        })
    };
    let (valid, current, sample) = match observation {
        Observation::Invalid(_) => (None, None, 0),
        Observation::Valid {
            status,
            sample,
            fresh,
        } => (
            Some(status.as_ref()),
            fresh.then_some(status.as_ref()),
            *sample,
        ),
    };
    // Reads stale contents too: a stopped scaler leaves an old heartbeat behind.
    let heartbeat = valid
        .and_then(|s| s.heartbeat_unix)
        .and_then(unix)
        .filter(|t| *t <= now);
    // Staleness is the heartbeat rule's; coverage reports what leaves it unknown.
    add(
        "scaling_status_coverage".into(),
        "warning",
        Some(heartbeat.is_none()),
        match observation {
            Observation::Invalid(reason) => (*reason).into(),
            Observation::Valid { .. } if heartbeat.is_none() => {
                "heartbeat_unix missing or invalid".into()
            }
            Observation::Valid { fresh: true, .. } => "fresh".into(),
            Observation::Valid { .. } => format!("stale: updated {}s ago", now - sample),
        },
        format!(
            "status file or its heartbeat missing, unreadable or invalid for {COVERAGE_HOLD_SECONDS}s"
        ),
        COVERAGE_HOLD_SECONDS,
        now,
    );
    let stopped = heartbeat.map(|t| now - t > HEARTBEAT_SECONDS);
    add(
        "scaling_heartbeat".into(),
        "warning",
        stopped,
        heartbeat.map_or_else(
            || "heartbeat unavailable".into(),
            |t| format!("last heartbeat {}s ago", now - t),
        ),
        format!("no scaler heartbeat for more than {HEARTBEAT_SECONDS}s"),
        0,
        // Staleness is observed now; recovery needs two distinct heartbeats.
        if stopped == Some(true) {
            now
        } else {
            heartbeat.unwrap_or(0)
        },
    );
    let budget = current.and_then(|s| s.budget.as_ref());
    let left = budget.map(|b| [b.actions_left, b.destroys_left]);
    add(
        "scaling_budget".into(),
        "warning",
        left.and_then(|left| {
            if left.iter().any(|v| v.is_some_and(|n| n <= 0)) {
                Some(true)
            } else if left.iter().all(Option::is_some) {
                Some(false)
            } else {
                None
            }
        }),
        format!(
            "actions left {}, destroys left {}",
            known(budget.and_then(|b| b.actions_left)),
            known(budget.and_then(|b| b.destroys_left))
        ),
        "daily action or destroy budget exhausted".into(),
        0,
        sample,
    );
    let desired = current.and_then(|s| s.desired_recent);
    let serving = current.and_then(|s| s.serving_recent);
    let holds = current.and_then(|s| s.holds.as_ref());
    add(
        "scaling_demand_held".into(),
        "warning",
        current.and_then(|_| Some(desired? > serving? && !holds?.is_empty())),
        format!(
            "desired {}, serving {}, holds {}",
            known(desired),
            known(serving),
            holds.map_or_else(|| "unknown".into(), |h| summary(h.clone()))
        ),
        format!(
            "desired above serving recent replicas with holds for {}m",
            DEMAND_HOLD_SECONDS / 60
        ),
        DEMAND_HOLD_SECONDS,
        sample,
    );
    let days = current
        .and_then(|s| s.forecast.as_ref())
        .and_then(|f| f.days_to_recent_budget)
        .filter(|d| d.is_finite());
    for (severity, limit) in [("warning", 30.), ("critical", 7.)] {
        add(
            format!("scaling_forecast_{severity}"),
            severity,
            days.map(|d| d < limit),
            days.map_or_else(
                || "forecast unavailable".into(),
                |d| format!("{d:.1} days to the recent-tier budget"),
            ),
            format!("recent-tier budget reached in under {limit} days"),
            0,
            sample,
        );
    }
    add(
        "scaling_recent_serving".into(),
        "critical",
        serving.map(|n| n == 0),
        format!("{} serving recent replicas", known(serving)),
        format!("no serving recent replica for {SERVING_HOLD_SECONDS}s"),
        SERVING_HOLD_SECONDS,
        sample,
    );
    for (key, list, threshold) in [
        (
            "scaling_awaiting_operator",
            current.and_then(|s| s.awaiting_operator.as_deref()),
            "scaler or actuator waiting for an operator",
        ),
        (
            "scaling_orphan",
            current.and_then(|s| s.orphans.as_deref()),
            "tagged elastic droplets absent from the inventory",
        ),
    ] {
        add(
            key.into(),
            "warning",
            list.map(|l| !l.is_empty()),
            list.map_or_else(|| "unknown".into(), |l| summary(values(l))),
            threshold.into(),
            0,
            sample,
        );
    }
    let mut named = BTreeSet::new();
    if let Some(op) = current.and_then(|s| s.operation.as_ref()) {
        let id = operation_id(op.id.as_deref());
        let context = format!(
            "operation {id} phase {}, age {}",
            op.phase.as_deref().unwrap_or("unknown"),
            op.age_seconds
                .filter(|a| a.is_finite())
                .map_or_else(|| "unknown".into(), |a| format!("{a:.0}s"))
        );
        for (kind, severity, value, threshold) in [
            (
                "deadline",
                "warning",
                op.deadline_exceeded,
                "operation deadline exceeded",
            ),
            (
                "fenced",
                "critical",
                op.fenced,
                "interrupted apply fenced; nothing proceeds until resolve-apply",
            ),
        ] {
            let key = format!("{OPERATION_PREFIX}{kind}_{id}");
            named.insert(key.clone());
            add(
                key,
                severity,
                value,
                format!("{context}; {kind} {}", known(value)),
                threshold.into(),
                0,
                sample,
            );
        }
    }
    for previous in open_operations {
        if previous.key.starts_with(OPERATION_PREFIX) && !named.contains(&previous.key) {
            let mut c = previous.clone();
            c.retired = false;
            c.firing = current.map(|_| false);
            c.observed = if current.is_some() {
                "operation no longer open".into()
            } else {
                "status unavailable".into()
            };
            c.sample = sample;
            out.push(c);
        }
    }
    out
}

pub struct Family {
    settings: Settings,
    store: Store,
}

impl Family {
    pub fn open(settings: Settings, database: &Path) -> Result<Self> {
        Ok(Self {
            store: Store::open(database)?.with_family("scaling"),
            settings,
        })
    }

    /// Read the status file once and commit this family's transitions.
    pub fn evaluate(&mut self, now: u64, environment: &str, dashboard: &str) -> Result<()> {
        let open: Vec<Condition> = self
            .store
            .incidents()?
            .into_iter()
            .filter(|i| i.active)
            .filter_map(|i| i.condition)
            .filter(|c| c.key.starts_with(OPERATION_PREFIX))
            .collect();
        let observation = read(&self.settings.status, now);
        let conditions = conditions(&observation, now, &open);
        self.store.evaluate(
            &conditions,
            now,
            self.settings.shadow,
            environment,
            dashboard,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The `scaler/status.json` example from `transparent/docs/elastic-recent.md`.
    const SPEC_EXAMPLE: &str = r#"{"schema": 1, "updated_unix": 1790680000.0, "mode": "observe",
 "heartbeat_unix": 1790680000.0, "decision": {"action": "hold", "reason": "…"},
 "desired_recent": 2, "serving_recent": 2, "offered_qps": 3.1,
 "holds": ["stale signal: transparent-pir-recent-02"],
 "operation": {"id": "…", "phase": "bootstrapping", "age_seconds": 120,
               "deadline_exceeded": false, "fenced": false},
 "budget": {"actions_left": 5, "destroys_left": 2, "monthly_cost_usd": 168.0},
 "forecast": {"days_to_recent_budget": 240.0},
 "awaiting_operator": [], "orphans": []}"#;

    fn healthy(at: u64) -> serde_json::Value {
        json!({"schema": 1, "updated_unix": at as f64 + 0.25, "mode": "act",
            "heartbeat_unix": at as f64 + 0.25, "decision": {"action": "hold", "reason": "ok"},
            "desired_recent": 2, "serving_recent": 2, "offered_qps": 3.1, "holds": [],
            "operation": null,
            "budget": {"actions_left": 5, "destroys_left": 2, "monthly_cost_usd": 168.0},
            "forecast": {"days_to_recent_budget": 240.0},
            "awaiting_operator": [], "orphans": []})
    }
    fn observe(v: &serde_json::Value, now: u64) -> Observation {
        parse(&serde_json::to_vec(v).unwrap(), now)
    }
    fn eval(v: &serde_json::Value, now: u64) -> Vec<Condition> {
        conditions(&observe(v, now), now, &[])
    }
    fn get<'a>(cs: &'a [Condition], key: &str) -> &'a Condition {
        cs.iter()
            .find(|c| c.key == key)
            .unwrap_or_else(|| panic!("missing {key}"))
    }
    fn firing(cs: &[Condition], key: &str) -> Option<bool> {
        get(cs, key).firing
    }
    const STATUS_RULES: [&str; 7] = [
        "scaling_budget",
        "scaling_demand_held",
        "scaling_forecast_warning",
        "scaling_forecast_critical",
        "scaling_recent_serving",
        "scaling_awaiting_operator",
        "scaling_orphan",
    ];
    fn temp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("pir-scaling-{name}-{}", rand::random::<u64>()))
    }
    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn fresh_healthy_status_is_healthy_and_keyed_to_the_scaler() {
        let cs = eval(&healthy(1000), 1003);
        assert!(cs.iter().all(|c| c.firing == Some(false)), "{cs:#?}");
        assert!(cs
            .iter()
            .all(|c| c.key.starts_with("scaling_") && c.resource == "transparent-scaler"));
        assert_eq!(get(&cs, "scaling_budget").sample, 1000);
        assert_eq!(get(&cs, "scaling_heartbeat").sample, 1000);
        assert_eq!(get(&cs, "scaling_status_coverage").sample, 1003);
        // No operation, no per-operation rules.
        assert!(!cs.iter().any(|c| c.key.starts_with(OPERATION_PREFIX)));
    }

    #[test]
    fn spec_example_parses_and_names_its_operation() {
        let cs = conditions(&parse(SPEC_EXAMPLE.as_bytes(), 1790680010), 1790680010, &[]);
        assert!(cs.iter().all(|c| c.firing == Some(false)), "{cs:#?}");
        // Holds alone, with desired == serving, are not held demand.
        assert!(get(&cs, "scaling_demand_held")
            .observed
            .contains("stale signal: transparent-pir-recent-02"));
        // Unsafe id characters are replaced; a missing id is "unidentified".
        let ops: Vec<_> = cs
            .iter()
            .filter(|c| c.key.starts_with(OPERATION_PREFIX))
            .map(|c| c.key.as_str())
            .collect();
        assert_eq!(
            ops,
            ["scaling_operation_deadline__", "scaling_operation_fenced__"]
        );
        assert_eq!(operation_id(None), "unidentified");
        assert_eq!(operation_id(Some("op 1/ä")), "op_1__");
    }

    #[test]
    fn stale_status_is_unknown_except_heartbeat() {
        let v = healthy(1000);
        let cs = eval(&v, 1000 + STALE_SECONDS);
        assert!(STATUS_RULES.iter().all(|k| firing(&cs, k) == Some(false)));
        let cs = eval(&v, 1000 + STALE_SECONDS + 1);
        for key in STATUS_RULES {
            assert_eq!(firing(&cs, key), None, "{key}");
        }
        assert_eq!(firing(&cs, "scaling_status_coverage"), Some(false));
        assert_eq!(firing(&cs, "scaling_heartbeat"), Some(false));
        let cs = eval(&v, 1000 + HEARTBEAT_SECONDS + 1);
        let heartbeat = get(&cs, "scaling_heartbeat");
        assert_eq!(heartbeat.firing, Some(true));
        assert_eq!(heartbeat.sample, 1000 + HEARTBEAT_SECONDS + 1);
        assert_eq!(heartbeat.severity, "warning");
    }

    #[test]
    fn missing_oversized_and_invalid_files_are_unknown_with_coverage() {
        let missing = temp("missing");
        let cs = conditions(&read(&missing, 1000), 1000, &[]);
        let unknown = |cs: &[Condition]| {
            cs.iter()
                .filter(|c| c.key != "scaling_status_coverage")
                .all(|c| c.firing.is_none())
        };
        assert!(unknown(&cs));
        let coverage = get(&cs, "scaling_status_coverage");
        assert_eq!(coverage.firing, Some(true));
        assert_eq!(coverage.hold_seconds, COVERAGE_HOLD_SECONDS);
        assert_eq!(coverage.observed, "status file unavailable");

        let big = temp("big");
        let mut padded = serde_json::to_vec(&healthy(1000)).unwrap();
        padded.resize(MAX_STATUS_BYTES as usize + 1, b' ');
        std::fs::write(&big, &padded).unwrap();
        let cs = conditions(&read(&big, 1000), 1000, &[]);
        assert!(unknown(&cs));
        assert_eq!(
            get(&cs, "scaling_status_coverage").observed,
            "status file exceeds 1 MiB"
        );
        padded.truncate(MAX_STATUS_BYTES as usize);
        std::fs::write(&big, &padded).unwrap();
        assert!(matches!(read(&big, 1000), Observation::Valid { .. }));
        std::fs::remove_file(big).unwrap();

        let mut wrong_schema = healthy(1000);
        wrong_schema["schema"] = json!(2);
        let mut future = healthy(1000);
        future["updated_unix"] = json!(2000.0);
        let mut bad_type = healthy(1000);
        bad_type["serving_recent"] = json!("two");
        for (bytes, reason) in [
            (b"{not json".to_vec(), "invalid status JSON"),
            (
                serde_json::to_vec(&bad_type).unwrap(),
                "invalid status JSON",
            ),
            (
                serde_json::to_vec(&wrong_schema).unwrap(),
                "unsupported status schema",
            ),
            (serde_json::to_vec(&future).unwrap(), "invalid updated_unix"),
        ] {
            let cs = conditions(&parse(&bytes, 1000), 1000, &[]);
            assert!(unknown(&cs));
            assert_eq!(get(&cs, "scaling_status_coverage").observed, reason);
        }
    }

    #[test]
    fn absent_fields_are_unknown_not_healthy() {
        let mut v = healthy(1000);
        for key in [
            "heartbeat_unix",
            "desired_recent",
            "holds",
            "budget",
            "forecast",
            "awaiting_operator",
            "orphans",
        ] {
            v.as_object_mut().unwrap().remove(key);
        }
        v["serving_recent"] = json!(null);
        let cs = eval(&v, 1000);
        for key in STATUS_RULES.iter().chain(&["scaling_heartbeat"]) {
            assert_eq!(firing(&cs, key), None, "{key}");
        }
        // Without a heartbeat a stopped scaler would go unnoticed: coverage says so.
        let coverage = get(&cs, "scaling_status_coverage");
        assert_eq!(coverage.firing, Some(true));
        assert_eq!(coverage.observed, "heartbeat_unix missing or invalid");
        let mut v = healthy(1000);
        v["forecast"]["days_to_recent_budget"] = json!(null);
        v["budget"] = json!({"actions_left": 3});
        let cs = eval(&v, 1000);
        assert_eq!(firing(&cs, "scaling_forecast_warning"), None);
        assert_eq!(firing(&cs, "scaling_budget"), None);
        // One exhausted budget is enough to know it fires.
        v["budget"] = json!({"destroys_left": 0});
        assert_eq!(firing(&eval(&v, 1000), "scaling_budget"), Some(true));
    }

    #[test]
    fn each_rule_fires_on_its_condition() {
        let at = 1000;
        let fires = |edit: &dyn Fn(&mut serde_json::Value), key: &str, severity: &str| {
            let mut v = healthy(at);
            edit(&mut v);
            let cs = eval(&v, at);
            let c = get(&cs, key);
            assert_eq!(c.firing, Some(true), "{key}: {c:#?}");
            assert_eq!(c.severity, severity, "{key}");
            let others: Vec<_> = cs
                .iter()
                .filter(|c| c.key != key && c.firing != Some(false))
                .collect();
            (others.len(), c.clone())
        };
        let (_, c) = fires(
            &|v| v["heartbeat_unix"] = json!(at as f64 - 181.),
            "scaling_heartbeat",
            "warning",
        );
        assert_eq!(c.sample, at);
        let mut v = healthy(at);
        v["heartbeat_unix"] = json!(at as f64 - 180.);
        assert_eq!(firing(&eval(&v, at), "scaling_heartbeat"), Some(false));

        assert_eq!(
            fires(
                &|v| v["budget"]["actions_left"] = json!(0),
                "scaling_budget",
                "warning"
            )
            .0,
            0
        );
        fires(
            &|v| v["budget"]["destroys_left"] = json!(0),
            "scaling_budget",
            "warning",
        );

        let (_, c) = fires(
            &|v| {
                v["desired_recent"] = json!(3);
                v["holds"] = json!(["stale signal: transparent-pir-recent-02"]);
            },
            "scaling_demand_held",
            "warning",
        );
        assert_eq!(c.hold_seconds, DEMAND_HOLD_SECONDS);
        assert!(c.observed.contains("desired 3, serving 2"));
        let mut v = healthy(at);
        v["desired_recent"] = json!(3);
        assert_eq!(firing(&eval(&v, at), "scaling_demand_held"), Some(false));

        let (_, c) = fires(
            &|v| v["forecast"]["days_to_recent_budget"] = json!(29.5),
            "scaling_forecast_warning",
            "warning",
        );
        assert_eq!(c.observed, "29.5 days to the recent-tier budget");
        let mut v = healthy(at);
        v["forecast"]["days_to_recent_budget"] = json!(29.5);
        assert_eq!(
            firing(&eval(&v, at), "scaling_forecast_critical"),
            Some(false)
        );
        let (others, _) = fires(
            &|v| v["forecast"]["days_to_recent_budget"] = json!(6.9),
            "scaling_forecast_critical",
            "critical",
        );
        // The warning fires alongside the critical.
        assert_eq!(others, 1);

        let (_, c) = fires(
            &|v| v["serving_recent"] = json!(0),
            "scaling_recent_serving",
            "critical",
        );
        assert_eq!(c.hold_seconds, SERVING_HOLD_SECONDS);

        let (_, c) = fires(
            &|v| v["awaiting_operator"] = json!(["resolve-apply", {"id": "op-7"}, "c", "d"]),
            "scaling_awaiting_operator",
            "warning",
        );
        assert_eq!(
            c.observed,
            r#"4 (resolve-apply; {"id":"op-7"}; c); +1 more"#
        );
        let (_, c) = fires(
            &|v| v["orphans"] = json!(["512345999"]),
            "scaling_orphan",
            "warning",
        );
        assert_eq!(c.observed, "1 (512345999)");

        let op = |deadline: bool, fenced: bool| {
            move |v: &mut serde_json::Value| {
                v["operation"] = json!({"id": "7f3c-a", "phase": "applying",
                    "age_seconds": 1900.0, "deadline_exceeded": deadline, "fenced": fenced})
            }
        };
        let (_, c) = fires(
            &op(true, false),
            "scaling_operation_deadline_7f3c-a",
            "warning",
        );
        assert!(c
            .observed
            .starts_with("operation 7f3c-a phase applying, age 1900s"));
        let (others, _) = fires(
            &op(false, true),
            "scaling_operation_fenced_7f3c-a",
            "critical",
        );
        assert_eq!(others, 0);
    }

    fn family(dir: &Path, shadow: bool) -> Family {
        Family::open(
            Settings {
                status: dir.join("status.json"),
                shadow,
            },
            &dir.join("incidents.sqlite"),
        )
        .unwrap()
    }
    fn write(dir: &Path, v: &serde_json::Value) {
        std::fs::write(dir.join("status.json"), serde_json::to_vec(v).unwrap()).unwrap();
    }
    fn active(f: &Family, key: &str) -> bool {
        f.store
            .incidents()
            .unwrap()
            .iter()
            .any(|i| i.active && i.condition.as_ref().is_some_and(|c| c.key == key))
    }
    fn fenced(at: u64, id: &str) -> serde_json::Value {
        let mut v = healthy(at);
        v["operation"] = json!({"id": id, "phase": "applying", "age_seconds": 60.0,
            "deadline_exceeded": false, "fenced": true});
        v
    }

    #[test]
    fn recovery_requires_two_fresh_healthy_samples_and_unknown_never_recovers() {
        let dir = temp("recovery");
        std::fs::create_dir(&dir).unwrap();
        let mut f = family(&dir, false);
        let key = "scaling_operation_fenced_op1";
        write(&dir, &fenced(1000, "op1"));
        f.evaluate(1000, "test", "").unwrap();
        assert!(active(&f, key));
        assert_eq!(f.store.health(true, 1000).unwrap().pending, 1);
        // Still fenced: one incident, no repeat.
        write(&dir, &fenced(1005, "op1"));
        f.evaluate(1005, "test", "").unwrap();
        assert_eq!(f.store.health(true, 1005).unwrap().pending, 1);
        // The operation closes: one healthy sample, then the same sample again.
        write(&dir, &healthy(1010));
        for now in [1010, 1015] {
            f.evaluate(now, "test", "").unwrap();
            assert!(active(&f, key));
        }
        // Missing file: unknown resets recovery progress.
        std::fs::remove_file(dir.join("status.json")).unwrap();
        f.evaluate(1020, "test", "").unwrap();
        assert!(active(&f, key));
        write(&dir, &healthy(1025));
        f.evaluate(1025, "test", "").unwrap();
        assert!(active(&f, key));
        // Stale contents are unknown too.
        f.evaluate(1025 + STALE_SECONDS + 1, "test", "").unwrap();
        assert!(active(&f, key));
        for at in [1120, 1125] {
            write(&dir, &healthy(at));
            f.evaluate(at, "test", "").unwrap();
        }
        assert!(!active(&f, key));
        assert_eq!(f.store.health(true, 1125).unwrap().pending, 2);
        // A later operation is a new incident; the closed one stays closed.
        write(&dir, &fenced(1130, "op2"));
        f.evaluate(1130, "test", "").unwrap();
        assert!(active(&f, "scaling_operation_fenced_op2"));
        assert!(!active(&f, key));
        assert_eq!(f.store.health(true, 1130).unwrap().pending, 3);
        drop(f);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn demand_must_be_held_continuously_for_thirty_minutes() {
        let mut store = Store::open(Path::new(":memory:"))
            .unwrap()
            .with_family("scaling");
        let held = |at: u64| {
            let mut v = healthy(at);
            v["desired_recent"] = json!(3);
            v["holds"] = json!(["cooldown"]);
            v
        };
        let key = "scaling_demand_held";
        let step = |store: &mut Store, v: Option<serde_json::Value>, now: u64| {
            let o = v.map_or(Observation::Invalid("status file unavailable"), |v| {
                observe(&v, now)
            });
            let cs: Vec<_> = conditions(&o, now, &[])
                .into_iter()
                .filter(|c| c.key == key)
                .collect();
            store.evaluate(&cs, now, true, "test", "").unwrap();
            store.incidents().unwrap()[0].active
        };
        // An unknown sample mid-way restarts the dwell.
        for now in (1000..1500).step_by(5) {
            assert!(!step(&mut store, Some(held(now)), now));
        }
        assert!(!step(&mut store, None, 1500));
        for now in (1505..1505 + DEMAND_HOLD_SECONDS).step_by(5) {
            assert!(!step(&mut store, Some(held(now)), now));
        }
        assert!(step(
            &mut store,
            Some(held(1505 + DEMAND_HOLD_SECONDS)),
            1505 + DEMAND_HOLD_SECONDS
        ));
    }

    #[test]
    fn shadow_is_the_default_and_records_without_delivery() {
        let settings = Settings::from_map(&vars(&[(STATUS_VAR, "/opt/s/status.json")]))
            .unwrap()
            .unwrap();
        assert!(settings.shadow);
        assert_eq!(settings.status, Path::new("/opt/s/status.json"));
        let promoted = Settings::from_map(&vars(&[
            (STATUS_VAR, " /opt/s/status.json "),
            (MODE_VAR, "active"),
        ]))
        .unwrap()
        .unwrap();
        assert!(!promoted.shadow);
        assert_eq!(promoted.status, Path::new("/opt/s/status.json"));
        for bad in [
            vars(&[(STATUS_VAR, "/s.json"), (MODE_VAR, "loud")]),
            vars(&[(STATUS_VAR, "relative/status.json")]),
            vars(&[(MODE_VAR, "active")]),
        ] {
            assert!(Settings::from_map(&bad).is_err(), "{bad:?}");
        }

        let dir = temp("shadow");
        std::fs::create_dir(&dir).unwrap();
        let mut f = family(&dir, settings.shadow);
        write(&dir, &fenced(1000, "op1"));
        f.evaluate(1000, "test", "").unwrap();
        assert!(active(&f, "scaling_operation_fenced_op1"));
        assert_eq!(f.store.health(true, 1000).unwrap().pending, 0);
        let db = rusqlite::Connection::open(dir.join("incidents.sqlite")).unwrap();
        let events: i64 = db
            .query_row("SELECT COUNT(*) FROM events WHERE shadow=1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(events, 1);
        drop(f);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn absent_variable_disables_family_and_enabled_family_leaves_others_unchanged() {
        // Disabled: nothing is opened, read or written.
        assert_eq!(Settings::from_map(&HashMap::new()).unwrap(), None);
        assert_eq!(
            Settings::from_map(&vars(&[(STATUS_VAR, "  "), (MODE_VAR, "shadow")])).unwrap(),
            None
        );
        assert_eq!(
            Settings::from_map(&vars(&[("PIR_APM_QUALITY_ALERT_MODE", "active")])).unwrap(),
            None
        );

        // Enabled beside active Enhance and shadow quality families in the same
        // database, it touches only its own rows and mode.
        let dir = temp("families");
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("incidents.sqlite");
        let mut enhance = Store::open(&path).unwrap();
        let mut quality = Store::open(&path).unwrap().with_family("quality");
        let condition = |key: &str, at: u64| Condition {
            retired: false,
            key: key.into(),
            resource: "query".into(),
            severity: "critical".into(),
            firing: Some(true),
            observed: "x".into(),
            threshold: "y".into(),
            hold_seconds: 0,
            sample: at,
        };
        enhance
            .evaluate(&[condition("query_5xx", 1000)], 1000, false, "test", "")
            .unwrap();
        quality
            .evaluate(
                &[condition("quality_storage", 1000)],
                1000,
                true,
                "test",
                "",
            )
            .unwrap();
        let db = rusqlite::Connection::open(&path).unwrap();
        let rows = |sql: &str| -> Vec<String> {
            db.prepare(sql)
                .unwrap()
                .query_map([], |r| r.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        let others = "SELECT key||value FROM incidents WHERE key NOT LIKE 'scaling\\_%' ESCAPE '\\' ORDER BY key";
        let outbox = "SELECT incident||body FROM outbox ORDER BY id";
        let modes = "SELECT key||value FROM metadata WHERE key LIKE 'mode%' ORDER BY key";
        let before = (rows(others), rows(outbox), rows(modes));
        assert_eq!(before.1.len(), 1);

        let mut f = family(&dir, false);
        let mut v = fenced(1005, "op1");
        v["serving_recent"] = json!(0);
        write(&dir, &v);
        f.evaluate(1005, "test", "").unwrap();
        assert_eq!(rows(others), before.0);
        let outbox_after = rows(outbox);
        assert_eq!(outbox_after.len(), 2);
        assert_eq!(outbox_after[..1], before.1[..]);
        assert!(outbox_after[1..]
            .iter()
            .all(|r| r.starts_with("scaling_operation_fenced_op1-")));
        assert_eq!(
            rows(modes),
            vec!["modeactive", "mode:qualityshadow", "mode:scalingactive"]
        );
        drop((enhance, quality, f, db));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
