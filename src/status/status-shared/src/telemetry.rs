//! Aggregate Status telemetry. No query identifiers enter labels. Hosts mount
//! [`snapshot`] and [`prometheus`] on loopback management routes.
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
};

#[derive(Clone, Default, Serialize)]
pub struct Resources {
    pub host_memory_total_bytes: Option<u64>,
    pub host_memory_available_bytes: Option<u64>,
    pub process_rss_bytes: Option<u64>,
    pub gpu_utilization_percent: Option<u64>,
    pub gpu_memory_used_mib: Option<u64>,
    pub gpu_memory_total_mib: Option<u64>,
}

#[derive(Clone, Default, Serialize)]
pub struct Operation {
    pub arrivals: u64,
    pub successes: u64,
    pub failures: u64,
    pub server_errors: u64,
    pub upload_bytes: u64,
    pub download_bytes: u64,
    pub buckets: [u64; 12],
}

#[derive(Clone, Default, Serialize)]
pub struct Telemetry {
    pub http_observation_version: u32,
    pub http_instance: String,
    pub admission: BTreeMap<String, AdmissionMetrics>,
    pub operations: BTreeMap<String, Operation>,
    pub index_ms: Option<f64>,
    pub database_hint_ms: Option<f64>,
    pub packing_ms: Option<f64>,
    pub preparation_ms: Option<f64>,
    pub rebuilt_units: Option<usize>,
    pub reused_units: Option<usize>,
    pub evicted_blocks: Option<usize>,
    pub last_activation_ms: Option<u64>,
    pub generation: Option<u64>,
    pub recovery_epoch: Option<u64>,
    pub coverage_start: Option<u32>,
    pub anchor_height: Option<u32>,
    pub observed_ms: Option<u64>,
    pub entries: Option<usize>,
    pub max_bucket_occupancy: Option<usize>,
    pub resources: Resources,
}

#[derive(Clone, Default, Serialize)]
pub struct AdmissionMetrics {
    pub active: u64,
    pub waiting: u64,
    pub rejections: BTreeMap<String, u64>,
    pub wait_buckets: [u64; 12],
    pub execution_buckets: [u64; 12],
}
pub fn admission_rejected(role: &'static str, reason: &'static str) {
    let mut t = shared().lock().unwrap();
    *t.admission
        .entry(role.into())
        .or_default()
        .rejections
        .entry(reason.into())
        .or_default() += 1;
}
pub fn admission_gauge(role: &'static str, waiting: bool, increment: bool) {
    let mut t = shared().lock().unwrap();
    let m = t.admission.entry(role.into()).or_default();
    let value = if waiting {
        &mut m.waiting
    } else {
        &mut m.active
    };
    if increment {
        *value += 1;
    } else {
        *value -= 1;
    }
}
pub fn admission_timing(role: &'static str, waiting: bool, elapsed: std::time::Duration) {
    let mut t = shared().lock().unwrap();
    let m = t.admission.entry(role.into()).or_default();
    let buckets = if waiting {
        &mut m.wait_buckets
    } else {
        &mut m.execution_buckets
    };
    let index = LIMITS
        .iter()
        .position(|limit| elapsed.as_secs_f64() <= *limit)
        .unwrap_or(LIMITS.len());
    buckets[index] += 1;
}

static TELEMETRY: OnceLock<Mutex<Telemetry>> = OnceLock::new();
fn shared() -> &'static Mutex<Telemetry> {
    TELEMETRY.get_or_init(|| Mutex::new(Telemetry::default()))
}

pub fn preparation(
    index_ms: f64,
    stats: &super::Preparation,
    snapshot: &super::index::Snapshot,
    manifest: &crate::protocol::Manifest,
) {
    let mut t = shared().lock().unwrap();
    t.index_ms = Some(index_ms);
    t.database_hint_ms = Some(stats.database_hint_ms);
    t.packing_ms = Some(stats.packing_ms);
    t.preparation_ms = Some(stats.total_ms);
    t.rebuilt_units = Some(stats.rebuilt_units);
    t.reused_units = Some(stats.reused_units);
    t.evicted_blocks = Some(snapshot.evicted_blocks);
    t.last_activation_ms = Some(manifest.observed_ms);
    t.generation = Some(manifest.generation);
    t.recovery_epoch = Some(manifest.recovery_epoch);
    t.coverage_start = Some(snapshot.start);
    t.anchor_height = Some(manifest.anchor_height);
    t.observed_ms = Some(manifest.observed_ms);
    t.entries = Some(snapshot.entries);
    t.max_bucket_occupancy = Some(
        snapshot
            .rows
            .chunks_exact(crate::protocol::ROW_BYTES)
            .map(|row| {
                row.chunks_exact(crate::protocol::SLOT_BYTES)
                    .take_while(|slot| slot.iter().any(|b| *b != 0))
                    .count()
            })
            .max()
            .unwrap_or(0),
    );
}

const LIMITS: [f64; 11] = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.2, 0.5, 1., 2., 5.];

pub fn stage(operation: &'static str, elapsed: std::time::Duration, success: bool) {
    let mut data = shared().lock().unwrap();
    let item = data.operations.entry(operation.into()).or_default();
    item.arrivals += 1;
    if success {
        item.successes += 1;
    } else {
        item.failures += 1;
    }
    let index = LIMITS
        .iter()
        .position(|limit| elapsed.as_secs_f64() <= *limit)
        .unwrap_or(LIMITS.len());
    item.buckets[index] += 1;
}

/// The wallet-facing or role operation a request path counts as, if any.
pub fn operation(role: &str, path: &str) -> Option<&'static str> {
    match (role, path) {
        ("router", "/v1/status/init") => Some("init"),
        ("router", p) if p.starts_with("/v1/status/session/") => Some("public_material"),
        ("router", "/v1/status/query") => Some("router_query"),
        ("worker", "/evaluate") => Some("worker_evaluate"),
        _ => None,
    }
}
/// Count one arrival before the request runs, so in-flight work is visible.
pub fn arrival(operation: &'static str) {
    shared()
        .lock()
        .unwrap()
        .operations
        .entry(operation.into())
        .or_default()
        .arrivals += 1;
}
/// Record the HTTP outcome of a request already counted by [`arrival`].
pub fn outcome(
    operation: &'static str,
    status: u16,
    upload: u64,
    download: u64,
    elapsed: std::time::Duration,
) {
    let mut guard = shared().lock().unwrap();
    let counter = guard.operations.entry(operation.into()).or_default();
    if (200..300).contains(&status) {
        counter.successes += 1;
    } else {
        counter.failures += 1;
        if status >= 500 {
            counter.server_errors += 1;
        }
    }
    counter.upload_bytes += upload;
    counter.download_bytes += download;
    let bucket = LIMITS
        .iter()
        .position(|limit| elapsed.as_secs_f64() <= *limit)
        .unwrap_or(LIMITS.len());
    counter.buckets[bucket] += 1;
}

fn process_resources() -> Resources {
    let mut resources = Resources::default();
    if let Ok(memory) = std::fs::read_to_string("/proc/meminfo") {
        for line in memory.lines() {
            let value = line
                .split_whitespace()
                .nth(1)
                .and_then(|v| v.parse::<u64>().ok())
                .and_then(|v| v.checked_mul(1024));
            if line.starts_with("MemTotal:") {
                resources.host_memory_total_bytes = value;
            }
            if line.starts_with("MemAvailable:") {
                resources.host_memory_available_bytes = value;
            }
        }
    }
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        resources.process_rss_bytes = status
            .lines()
            .find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<u64>().ok())
            .and_then(|v| v.checked_mul(1024));
    }
    if let Ok(output) = std::process::Command::new("/usr/bin/nvidia-smi")
        .args([
            "--query-gpu=utilization.gpu,memory.used,memory.total",
            "--format=csv,noheader,nounits",
        ])
        .output()
    {
        if output.status.success() {
            if let Some(line) = String::from_utf8_lossy(&output.stdout).lines().next() {
                let values: Vec<_> = line.split(',').map(str::trim).collect();
                if values.len() == 3 {
                    resources.gpu_utilization_percent = values[0].parse().ok();
                    resources.gpu_memory_used_mib = values[1].parse().ok();
                    resources.gpu_memory_total_mib = values[2].parse().ok();
                }
            }
        }
    }
    resources
}
/// Current counters with process resources. Reads `/proc` and may run
/// `nvidia-smi`, so call it off any async runtime thread.
pub fn snapshot() -> Telemetry {
    let mut data = shared().lock().unwrap().clone();
    static INSTANCE: OnceLock<String> = OnceLock::new();
    data.http_observation_version = 1;
    data.http_instance = INSTANCE
        .get_or_init(|| {
            format!(
                "{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )
        })
        .clone();
    // Explicit zero counters distinguish an idle observed route from old telemetry.
    for operation in ["init", "public_material", "router_query", "worker_evaluate"] {
        data.operations.entry(operation.into()).or_default();
    }
    data.resources = process_resources();
    data
}
/// Prometheus text exposition (`text/plain; version=0.0.4`).
pub fn prometheus() -> String {
    let data = shared().lock().unwrap().clone();
    let mut body = String::from("# TYPE status_pir_requests_total counter\n# TYPE status_pir_request_duration_seconds_bucket counter\n");
    for (role, metrics) in &data.admission {
        body.push_str(&format!("status_pir_admission_active{{role=\"{role}\"}} {}\nstatus_pir_admission_waiting{{role=\"{role}\"}} {}\n", metrics.active, metrics.waiting));
        for (reason, count) in &metrics.rejections {
            body.push_str(&format!("status_pir_admission_rejections_total{{role=\"{role}\",reason=\"{reason}\"}} {count}\n"));
        }
        for (phase, buckets) in [
            ("wait", &metrics.wait_buckets),
            ("permit_hold", &metrics.execution_buckets),
        ] {
            let mut cumulative = 0;
            for (i, count) in buckets.iter().enumerate() {
                cumulative += count;
                let limit = if i == LIMITS.len() {
                    "+Inf".into()
                } else {
                    LIMITS[i].to_string()
                };
                body.push_str(&format!("status_pir_admission_duration_seconds_bucket{{role=\"{role}\",phase=\"{phase}\",le=\"{limit}\"}} {cumulative}\n"));
            }
        }
    }
    for operation in [
        "init",
        "public_material",
        "router_query",
        "worker_evaluate",
        "router_pack",
    ] {
        let Some(item) = data.operations.get(operation) else {
            continue;
        };
        for (outcome, count) in [
            ("arrival", item.arrivals),
            ("success", item.successes),
            ("failure", item.failures),
            ("server_error", item.server_errors),
        ] {
            body.push_str(&format!("status_pir_requests_total{{operation=\"{operation}\",outcome=\"{outcome}\"}} {count}\n"));
        }
        let mut cumulative = 0;
        for (i, count) in item.buckets.iter().enumerate() {
            cumulative += count;
            let limit = if i == LIMITS.len() {
                "+Inf".into()
            } else {
                LIMITS[i].to_string()
            };
            body.push_str(&format!("status_pir_request_duration_seconds_bucket{{operation=\"{operation}\",le=\"{limit}\"}} {cumulative}\n"));
        }
    }
    for (name, value) in [
        ("status_pir_entries", data.entries.map(|v| v as f64)),
        ("status_pir_generation", data.generation.map(|v| v as f64)),
        (
            "status_pir_coverage_start",
            data.coverage_start.map(|v| v as f64),
        ),
        (
            "status_pir_anchor_height",
            data.anchor_height.map(|v| v as f64),
        ),
        ("status_pir_preparation_milliseconds", data.preparation_ms),
    ] {
        if let Some(v) = value {
            body.push_str(&format!("{name} {v}\n"));
        }
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcomes_distinguish_client_errors_and_export_no_request_identity() {
        stage("router_pack", std::time::Duration::from_millis(4), true);
        let before = snapshot().operations["public_material"].clone();
        for status in [200, 400, 503] {
            arrival("public_material");
            outcome(
                "public_material",
                status,
                2,
                3,
                std::time::Duration::from_millis(1),
            );
        }
        let data = snapshot();
        let after = &data.operations["public_material"];
        assert_eq!(after.arrivals - before.arrivals, 3);
        assert_eq!(after.successes - before.successes, 1);
        assert_eq!(after.failures - before.failures, 2);
        assert_eq!(after.server_errors - before.server_errors, 1);
        assert_eq!(after.upload_bytes - before.upload_bytes, 6);
        assert_eq!(
            after.buckets.iter().sum::<u64>() - before.buckets.iter().sum::<u64>(),
            3
        );
        assert!(data.operations["router_pack"].buckets[0] >= 1);
        assert_eq!(data.http_observation_version, 1);
        assert!(prometheus().contains("status_pir_requests_total{operation=\"public_material\""));
    }

    #[test]
    fn only_wallet_and_role_paths_are_counted() {
        assert_eq!(operation("router", "/v1/status/init"), Some("init"));
        assert_eq!(
            operation("router", "/v1/status/session/00"),
            Some("public_material")
        );
        assert_eq!(
            operation("router", "/v1/status/query"),
            Some("router_query")
        );
        assert_eq!(operation("worker", "/evaluate"), Some("worker_evaluate"));
        assert_eq!(operation("worker", "/v1/status/query"), None);
        assert_eq!(operation("router", "/control/health"), None);
    }
}
