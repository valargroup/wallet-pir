//! Per-host observer for the central PIR APM.
//!
//! The sidecar measures this host every 15 seconds, decides each check's
//! status locally and serves the cached result at `GET /v1/observations` over
//! mutually authenticated HTTPS. It holds no notification credentials; the
//! central APM owns incidents, recovery confirmation and delivery.
pub mod config;
pub mod fs;
pub mod hooks;
pub mod resources;
pub mod server;

use pir_host_observations::{Check, Observations, SCHEMA_VERSION};
use std::time::{SystemTime, UNIX_EPOCH};

pub const INTERVAL_SECONDS: u64 = 15;

pub fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Random identity of this process, distinguishing restarts in the APM.
pub fn incarnation() -> String {
    use rand::Rng;
    hex::encode(rand::thread_rng().gen::<[u8; 16]>())
}

/// One complete collection from the host.
pub async fn collect(
    config: &config::Config,
    state: &mut resources::State,
    cert_not_after: Option<i64>,
) -> (Vec<Check>, Option<String>) {
    let now = unix_time();
    let mut checks = Vec::new();
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let mounts = fs::parse_mountinfo(&mountinfo);
    checks.extend(fs::filesystems(&mounts, &fs::statvfs, now));
    checks.extend(fs::volumes(
        &config.required_volumes,
        &mounts,
        &fs::statvfs,
        &fs::uuid_device,
        now,
    ));
    let meminfo = resources::meminfo(&std::fs::read_to_string("/proc/meminfo").unwrap_or_default());
    checks.extend(resources::memory(&meminfo, state, now));
    for unit in &config.units {
        let sample = resources::sample_unit(unit).await;
        checks.extend(resources::unit_checks(unit, sample.as_ref(), state, now));
    }
    checks.push(resources::certificate(cert_not_after, now));
    let hooks = futures_join(config, now).await;
    checks.extend(hooks.into_iter().flatten());
    let mut edge = None;
    if let Some(url) = &config.edge_metrics_url {
        let text = resources::fetch_loopback(url).await;
        checks.push(Check {
            name: "edge".into(),
            status: if text.is_some() {
                pir_host_observations::Status::Healthy
            } else {
                pir_host_observations::Status::Unknown
            },
            reason: if text.is_some() {
                "edge metrics read".into()
            } else {
                "edge metrics unavailable".into()
            },
            observed_at: now,
            values: vec![],
        });
        edge = text.map(|t| resources::edge_metrics(&t, now));
    }
    (checks, edge)
}

async fn futures_join(config: &config::Config, now: u64) -> Vec<Vec<Check>> {
    let mut set = tokio::task::JoinSet::new();
    for (i, hook) in config.hooks.iter().cloned().enumerate() {
        set.spawn(async move { (i, hooks::run(&hook, now).await) });
    }
    let mut out = vec![Vec::new(); config.hooks.len()];
    while let Some(Ok((i, checks))) = set.join_next().await {
        out[i] = checks;
    }
    out
}

pub fn document(
    config: &config::Config,
    digest: &str,
    incarnation: &str,
    checks: Vec<Check>,
) -> Observations {
    Observations {
        schema_version: SCHEMA_VERSION,
        host_id: config.host_id.clone(),
        incarnation: incarnation.into(),
        config_digest: digest.into(),
        generated_at: unix_time(),
        checks,
    }
}
