//! Memory, swap, systemd units, the serving certificate and Caddy aggregates.
//!
//! Moved rules keep their thresholds and dwell: available memory below 15% for
//! 120s warns, a new OOM kill is critical and a new automatic restart warns,
//! each for five minutes after the counter grew. Counter baselines and the
//! memory dwell start survive sidecar restarts in a small state file; there is
//! no history database or notification queue here.
use pir_host_observations::{Check, Status, Unit, Value};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};

pub const MEMORY_WARN_RATIO: f64 = 0.15;
pub const MEMORY_DWELL_SECONDS: u64 = 120;
pub const COUNTER_WINDOW_SECONDS: u64 = 300;
pub const CERT_WARN_SECONDS: i64 = 30 * 86_400;
pub const CERT_CRITICAL_SECONDS: i64 = 7 * 86_400;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct State {
    pub memory_low_since: Option<u64>,
    pub counters: BTreeMap<String, Counter>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Counter {
    pub value: u64,
    pub grew_at: Option<u64>,
}

impl State {
    pub fn load(path: &Path) -> Self {
        std::fs::read(path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default()
    }

    pub fn store(&self, path: &Path) -> std::io::Result<()> {
        let next = path.with_extension("next");
        std::fs::write(&next, serde_json::to_vec(self)?)?;
        std::fs::rename(next, path)
    }

    /// Whether `key` grew within the window. A decrease is a reset (a new
    /// cgroup or a manual restart), never a recovery or an event.
    fn grew(&mut self, key: &str, value: u64, now: u64) -> bool {
        let entry = self.counters.entry(key.to_string());
        let c = match entry {
            std::collections::btree_map::Entry::Vacant(v) => v.insert(Counter {
                value,
                grew_at: None,
            }),
            std::collections::btree_map::Entry::Occupied(o) => {
                let c = o.into_mut();
                if value > c.value {
                    c.grew_at = Some(now);
                }
                c.value = value;
                c
            }
        };
        c.grew_at
            .is_some_and(|at| now.saturating_sub(at) < COUNTER_WINDOW_SECONDS)
    }
}

fn value(name: &str, value: f64, unit: Unit) -> Value {
    Value {
        name: name.into(),
        value,
        unit,
    }
}

pub fn meminfo(text: &str) -> BTreeMap<String, u64> {
    text.lines()
        .filter_map(|line| {
            let (key, rest) = line.split_once(':')?;
            let kib: u64 = rest.split_whitespace().next()?.parse().ok()?;
            Some((key.to_string(), kib * 1024))
        })
        .collect()
}

pub fn memory(info: &BTreeMap<String, u64>, state: &mut State, now: u64) -> [Check; 2] {
    let (available, total) = (info.get("MemAvailable"), info.get("MemTotal"));
    let memory = match (available, total) {
        (Some(&a), Some(&t)) if t > 0 => {
            let ratio = a as f64 / t as f64;
            let since = if ratio < MEMORY_WARN_RATIO {
                Some(*state.memory_low_since.get_or_insert(now))
            } else {
                state.memory_low_since = None;
                None
            };
            let low = since.is_some_and(|s| now.saturating_sub(s) >= MEMORY_DWELL_SECONDS);
            Check {
                name: "memory".into(),
                status: if low {
                    Status::Warning
                } else {
                    Status::Healthy
                },
                reason: format!("{:.1}% available (warn below 15% for 120s)", ratio * 100.),
                observed_at: now,
                values: vec![
                    value("available_bytes", a as f64, Unit::Bytes),
                    value("total_bytes", t as f64, Unit::Bytes),
                ],
            }
        }
        _ => Check {
            name: "memory".into(),
            status: Status::Unknown,
            reason: "memory statistics unavailable".into(),
            observed_at: now,
            values: vec![],
        },
    };
    let swap = match (info.get("SwapTotal"), info.get("SwapFree")) {
        (Some(&t), Some(&f)) => Check {
            name: "swap".into(),
            status: Status::Healthy,
            reason: format!("{} MiB of {} MiB swap used", (t - f.min(t)) >> 20, t >> 20),
            observed_at: now,
            values: vec![
                value("total_bytes", t as f64, Unit::Bytes),
                value("free_bytes", f as f64, Unit::Bytes),
            ],
        },
        _ => Check {
            name: "swap".into(),
            status: Status::Unknown,
            reason: "swap statistics unavailable".into(),
            observed_at: now,
            values: vec![],
        },
    };
    [memory, swap]
}

/// `systemctl show` properties and cgroup files of one unit.
#[derive(Clone, Debug, Default)]
pub struct UnitSample {
    pub properties: BTreeMap<String, String>,
    pub memory_events: Option<String>,
    pub memory_stat: Option<String>,
}

pub const UNIT_PROPERTIES: &str =
    "LoadState,ActiveState,NRestarts,ControlGroup,MemoryCurrent,MemoryPeak,MemoryMax";

/// Unit state applies to every unit; restart and cgroup counters belong to services.
pub(crate) fn unit_check_kinds(unit: &str) -> &'static [&'static str] {
    if unit.ends_with(".service") {
        &["active", "restarts", "oom"]
    } else {
        &["active"]
    }
}

pub fn unit_checks(
    unit: &str,
    sample: Option<&UnitSample>,
    state: &mut State,
    now: u64,
) -> Vec<Check> {
    let base = format!("unit:{}", unit.to_lowercase());
    let check = |kind: &str, status, reason: String, values| Check {
        name: format!("{base}:{kind}"),
        status,
        reason,
        observed_at: now,
        values,
    };
    let Some(sample) = sample else {
        return unit_check_kinds(unit)
            .iter()
            .map(|k| check(k, Status::Unknown, "unit state unavailable".into(), vec![]))
            .collect();
    };
    let p = &sample.properties;
    let get = |k: &str| p.get(k).map(String::as_str).unwrap_or("");
    let number = |k: &str| get(k).parse::<u64>().ok();
    let mut out = Vec::new();
    let active = match (get("LoadState"), get("ActiveState")) {
        ("loaded", "active") => (Status::Healthy, "active"),
        ("loaded", "failed") => (Status::Critical, "failed"),
        ("loaded", _) => (Status::Warning, "not active"),
        _ => (Status::Critical, "unit is not loaded"),
    };
    let mut memory = Vec::new();
    for (name, key) in [
        ("memory_current_bytes", "MemoryCurrent"),
        ("memory_peak_bytes", "MemoryPeak"),
        ("memory_max_bytes", "MemoryMax"),
    ] {
        if let Some(v) = number(key) {
            memory.push(value(name, v as f64, Unit::Bytes));
        }
    }
    if let Some(stat) = &sample.memory_stat {
        let stat: BTreeMap<&str, u64> = stat
            .lines()
            .filter_map(|l| {
                let (k, v) = l.split_once(' ')?;
                Some((k, v.trim().parse().ok()?))
            })
            .collect();
        if let Some(anon) = stat.get("anon") {
            let anon = anon + stat.get("shmem").copied().unwrap_or(0);
            memory.push(value("memory_anon_bytes", anon as f64, Unit::Bytes));
        }
    }
    out.push(check("active", active.0, active.1.into(), memory));
    if !unit.ends_with(".service") {
        return out;
    }
    out.push(match number("NRestarts") {
        Some(n) => {
            let grew = state.grew(&format!("{base}:restarts"), n, now);
            check(
                "restarts",
                if grew {
                    Status::Warning
                } else {
                    Status::Healthy
                },
                if grew {
                    "automatic restart within 5m".into()
                } else {
                    "no automatic restart within 5m".into()
                },
                vec![value("restarts", n as f64, Unit::Count)],
            )
        }
        None => check(
            "restarts",
            Status::Unknown,
            "restart counter unavailable".into(),
            vec![],
        ),
    });
    let oom = sample.memory_events.as_deref().and_then(|events| {
        events.lines().find_map(|l| {
            l.strip_prefix("oom_kill ")
                .and_then(|v| v.trim().parse::<u64>().ok())
        })
    });
    out.push(match oom {
        Some(n) => {
            let grew = state.grew(&format!("{base}:oom"), n, now);
            check(
                "oom",
                if grew {
                    Status::Critical
                } else {
                    Status::Healthy
                },
                if grew {
                    "new OOM kill within 5m".into()
                } else {
                    "no OOM kill within 5m".into()
                },
                vec![value("oom_kills", n as f64, Unit::Count)],
            )
        }
        None => check(
            "oom",
            Status::Unknown,
            "OOM counter unavailable".into(),
            vec![],
        ),
    });
    out
}

pub fn parse_properties(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|l| l.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Read one unit through `systemctl show` and its cgroup v2 directory.
pub async fn sample_unit(unit: &str) -> Option<UnitSample> {
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::process::Command::new("systemctl")
            .args([
                "show",
                "-p",
                if unit.ends_with(".service") {
                    UNIT_PROPERTIES
                } else {
                    "LoadState,ActiveState"
                },
                "--",
                unit,
            ])
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .ok()?
    .ok()?;
    if !output.status.success() {
        return None;
    }
    let properties = parse_properties(&String::from_utf8_lossy(&output.stdout));
    let group = properties.get("ControlGroup").cloned().unwrap_or_default();
    let read = |name: &str| {
        (group.starts_with('/') && !group.contains(".."))
            .then(|| std::fs::read_to_string(format!("/sys/fs/cgroup{group}/{name}")).ok())
            .flatten()
    };
    Some(UnitSample {
        memory_events: read("memory.events"),
        memory_stat: read("memory.stat"),
        properties,
    })
}

/// Remaining validity of the serving certificate.
pub fn certificate(not_after: Option<i64>, now: u64) -> Check {
    let Some(not_after) = not_after else {
        return Check {
            name: "certificate".into(),
            status: Status::Unknown,
            reason: "serving certificate expiry unreadable".into(),
            observed_at: now,
            values: vec![],
        };
    };
    let remaining = not_after - now as i64;
    let status = if remaining < CERT_CRITICAL_SECONDS {
        Status::Critical
    } else if remaining < CERT_WARN_SECONDS {
        Status::Warning
    } else {
        Status::Healthy
    };
    Check {
        name: "certificate".into(),
        status,
        reason: format!(
            "serving certificate expires in {} days (warn 30, critical 7)",
            remaining.div_euclid(86_400)
        ),
        observed_at: now,
        values: vec![value("remaining_seconds", remaining as f64, Unit::Seconds)],
    }
}

/// Aggregate Caddy series the edge quality rules consume, as the SSH sampler kept them.
pub fn edge_metrics(text: &str, now: u64) -> String {
    let mut out: String = text
        .lines()
        .filter(|l| {
            l.starts_with("caddy_http_")
                || l.starts_with("caddy_config_last_reload_")
                || l.starts_with("process_start_time_seconds")
        })
        .flat_map(|l| [l, "\n"])
        .collect();
    out.push_str(&format!("pir_observation_timestamp_seconds {now}\n"));
    out
}

/// Plain HTTP/1.0 GET of a loopback metrics endpoint, bounded to 2 MiB.
pub async fn fetch_loopback(url: &str) -> Option<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let rest = url.strip_prefix("http://")?;
    let (authority, path) = rest
        .split_once('/')
        .map(|(a, p)| (a, format!("/{p}")))
        .unwrap_or((rest, "/".into()));
    let work = async {
        let mut stream = tokio::net::TcpStream::connect(authority).await.ok()?;
        stream
            .write_all(format!("GET {path} HTTP/1.0\r\nHost: {authority}\r\n\r\n").as_bytes())
            .await
            .ok()?;
        let mut body = Vec::new();
        (&mut stream)
            .take(2 * 1024 * 1024 + 1)
            .read_to_end(&mut body)
            .await
            .ok()?;
        (body.len() <= 2 * 1024 * 1024).then_some(body)
    };
    let body = tokio::time::timeout(std::time::Duration::from_secs(3), work)
        .await
        .ok()??;
    let text = String::from_utf8(body).ok()?;
    let (head, body) = text.split_once("\r\n\r\n")?;
    let status = head.lines().next()?;
    status
        .split_whitespace()
        .nth(1)
        .filter(|c| *c == "200")
        .map(|_| body.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(restarts: u64, oom: u64, active: &str) -> UnitSample {
        UnitSample {
            properties: parse_properties(&format!(
                "LoadState=loaded\nActiveState={active}\nNRestarts={restarts}\nMemoryCurrent=100\nMemoryMax=infinity\n"
            )),
            memory_events: Some(format!("low 0\noom 1\noom_kill {oom}\n")),
            memory_stat: Some("anon 40\nfile 10\nshmem 2\n".into()),
        }
    }

    fn status(checks: &[Check], name: &str) -> Status {
        checks.iter().find(|c| c.name == name).unwrap().status
    }

    #[test]
    fn non_service_units_collect_and_expect_only_active_state() {
        for unit in ["healthy.socket", "healthy.timer"] {
            let sample = UnitSample {
                properties: parse_properties("LoadState=loaded\nActiveState=active\n"),
                ..UnitSample::default()
            };
            let mut state = State::default();
            let checks = unit_checks(unit, Some(&sample), &mut state, 100);
            assert_eq!(
                checks.len(),
                1,
                "non-service units have no service counters"
            );
            assert_eq!(checks[0].status, Status::Healthy);
            let config: crate::config::Config = serde_json::from_value(serde_json::json!({
                "host_id":"test", "tls":{"cert":"c", "key":"k", "client_ca":"a", "collector_identity":"collector"},
                "units":[unit], "state_path":"/tmp/state.json"
            })).unwrap();
            config.validate().unwrap();
            let expected: Vec<_> = config
                .expected_checks()
                .into_iter()
                .filter(|c| c.starts_with("unit:"))
                .collect();
            assert_eq!(
                expected,
                checks.iter().map(|c| c.name.clone()).collect::<Vec<_>>()
            );
            let missing = unit_checks(unit, None, &mut state, 100);
            assert_eq!(missing.len(), 1);
            assert_eq!(missing[0].status, Status::Unknown);
            let inactive = UnitSample {
                properties: parse_properties("LoadState=loaded\nActiveState=inactive\n"),
                ..UnitSample::default()
            };
            assert_eq!(
                unit_checks(unit, Some(&inactive), &mut state, 100)[0].status,
                Status::Warning
            );
        }
    }

    #[test]
    fn counters_fire_on_growth_survive_restart_and_rebase_on_reset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        let mut state = State::default();
        let c = unit_checks(
            "Pir.service",
            Some(&sample(2, 0, "active")),
            &mut state,
            100,
        );
        assert_eq!(status(&c, "unit:pir.service:restarts"), Status::Healthy);
        let anon = &c[0].values.iter().find(|v| v.name == "memory_anon_bytes");
        assert_eq!(anon.unwrap().value, 42.);
        let c = unit_checks(
            "Pir.service",
            Some(&sample(3, 1, "active")),
            &mut state,
            115,
        );
        assert_eq!(status(&c, "unit:pir.service:restarts"), Status::Warning);
        assert_eq!(status(&c, "unit:pir.service:oom"), Status::Critical);
        // The sidecar restarts: the baseline and the growth time are retained.
        state.store(&path).unwrap();
        let mut state = State::load(&path);
        let c = unit_checks(
            "Pir.service",
            Some(&sample(3, 1, "active")),
            &mut state,
            130,
        );
        assert_eq!(status(&c, "unit:pir.service:oom"), Status::Critical);
        let c = unit_checks(
            "Pir.service",
            Some(&sample(3, 1, "active")),
            &mut state,
            415,
        );
        assert_eq!(status(&c, "unit:pir.service:oom"), Status::Healthy);
        // A new cgroup resets the counter to zero: no event.
        let c = unit_checks(
            "Pir.service",
            Some(&sample(0, 0, "failed")),
            &mut state,
            430,
        );
        assert_eq!(status(&c, "unit:pir.service:oom"), Status::Healthy);
        assert_eq!(status(&c, "unit:pir.service:restarts"), Status::Healthy);
        assert_eq!(status(&c, "unit:pir.service:active"), Status::Critical);
        let c = unit_checks("Pir.service", None, &mut state, 445);
        assert!(c.iter().all(|c| c.status == Status::Unknown));
    }

    #[test]
    fn memory_keeps_its_dwell_and_certificate_its_thresholds() {
        let mut state = State::default();
        let info =
            meminfo("MemTotal: 1000 kB\nMemAvailable: 100 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n");
        assert_eq!(memory(&info, &mut state, 0)[0].status, Status::Healthy);
        assert_eq!(memory(&info, &mut state, 119)[0].status, Status::Healthy);
        assert_eq!(memory(&info, &mut state, 120)[0].status, Status::Warning);
        let ok = meminfo("MemTotal: 1000 kB\nMemAvailable: 150 kB\n");
        let [m, swap] = memory(&ok, &mut state, 121);
        assert_eq!(m.status, Status::Healthy);
        assert_eq!(swap.status, Status::Unknown);
        assert_eq!(state.memory_low_since, None);
        let day = 86_400;
        assert_eq!(certificate(Some(31 * day), 0).status, Status::Healthy);
        assert_eq!(certificate(Some(30 * day - 1), 0).status, Status::Warning);
        assert_eq!(certificate(Some(7 * day - 1), 0).status, Status::Critical);
        assert_eq!(certificate(None, 0).status, Status::Unknown);
    }

    #[test]
    fn edge_metrics_keep_only_aggregate_caddy_series() {
        let text = "# HELP x\ncaddy_http_requests_total{server=\"a\"} 3\ngo_goroutines 9\nprocess_start_time_seconds 5\n";
        assert_eq!(
            edge_metrics(text, 7),
            "caddy_http_requests_total{server=\"a\"} 3\nprocess_start_time_seconds 5\npir_observation_timestamp_seconds 7\n"
        );
    }
}
