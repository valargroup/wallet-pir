//! Separate native binaries avoid unifying Enhance and Transparent PIR backends.
use anyhow::Result;
use pir_apm::incidents::{unix_time, Condition};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub service: String,
    pub command: Vec<String>,
    pub timeout_seconds: u64,
}
#[derive(Clone, Default, Serialize)]
pub struct Probe {
    pub sampled_at: u64,
    pub duration_seconds: f64,
    pub successes: u64,
    pub failures: u64,
    pub consecutive_failures: u64,
    pub consecutive_slow: u64,
    pub category: String,
}
pub type Shared = Arc<RwLock<BTreeMap<String, Probe>>>;
pub fn start() -> Result<Shared> {
    let shared = Shared::default();
    let Some(path) = std::env::var_os("PIR_MONITOR_SERVICE_PROBES_CONFIG") else {
        return Ok(shared);
    };
    let configs: Vec<Config> = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut names = std::collections::BTreeSet::new();
    for config in &configs {
        anyhow::ensure!(
            matches!(config.service.as_str(), "status" | "transparent")
                && names.insert(config.service.clone())
                && !config.command.is_empty()
                && PathBuf::from(&config.command[0]).is_absolute()
                && (1..=45).contains(&config.timeout_seconds),
            "invalid service probe config"
        );
    }
    for config in configs {
        let output = shared.clone();
        tokio::spawn(async move {
            output
                .write()
                .await
                .insert(config.service.clone(), Probe::default());
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let began = Instant::now();
                let result =
                    tokio::time::timeout(Duration::from_secs(config.timeout_seconds), run(&config))
                        .await;
                let category = match result {
                    Ok(Ok(c)) => c,
                    Ok(Err(_)) => "probe_unavailable".into(),
                    Err(_) => "deadline".into(),
                };
                let mut data = output.write().await;
                let value = data.get_mut(&config.service).unwrap();
                value.sampled_at = unix_time();
                value.duration_seconds = began.elapsed().as_secs_f64();
                if category.is_empty() {
                    value.successes += 1;
                    value.consecutive_failures = 0;
                } else {
                    value.failures += 1;
                    value.consecutive_failures += 1;
                }
                value.consecutive_slow = if category.is_empty() && value.duration_seconds > 5. {
                    value.consecutive_slow + 1
                } else {
                    0
                };
                value.category = category;
            }
        });
    }
    Ok(shared)
}
async fn run(config: &Config) -> Result<String> {
    use tokio::io::AsyncReadExt;
    let mut child = tokio::process::Command::new(&config.command[0])
        .args(&config.command[1..])
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let read = |stream: Box<dyn tokio::io::AsyncRead + Unpin + Send>| async move {
        let mut data = Vec::new();
        stream.take(65537).read_to_end(&mut data).await?;
        Ok::<_, std::io::Error>(data)
    };
    let (out, err) = tokio::try_join!(read(Box::new(stdout)), read(Box::new(stderr)))?;
    if out.len() > 65536 || err.len() > 65536 {
        child.kill().await?;
        return Ok("invalid_probe_output".into());
    }
    let status = child.wait().await?;
    let text = String::from_utf8_lossy(&out);
    let value = text
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<serde_json::Value>(line).ok());
    if let Some(v) = value {
        if v["category"].as_str() == Some("answer_mismatch") {
            return Ok("answer_mismatch".into());
        }
        if v["category"].as_str() == Some("oracle_invalid") {
            return Ok("oracle_invalid".into());
        }
        if status.success()
            && (v["passed"].as_bool() == Some(true)
                || (v["phase"].as_str() == Some("live_encrypted_probe")
                    && v["correct"].as_u64() == v["queries"].as_u64()
                    && v["correct"].as_u64().is_some_and(|n| n > 0)))
        {
            return Ok(String::new());
        }
    }
    let err = String::from_utf8_lossy(&err);
    Ok(if err.contains("differs from canonical block oracle") {
        "answer_mismatch"
    } else if err.contains("not canonical") || err.contains("raw block hash differs") {
        "oracle_invalid"
    } else {
        "request_failed"
    }
    .into())
}
pub fn conditions(probes: &BTreeMap<String, Probe>, now: u64) -> Vec<Condition> {
    let mut result = Vec::new();
    for (service, p) in probes {
        let fresh = p.sampled_at > 0 && p.sampled_at <= now && now - p.sampled_at <= 90;
        for (name, firing, severity, hold, sample, threshold) in [
            (
                "progress",
                Some(!fresh),
                "critical",
                60,
                now,
                "probe sample unavailable for 90s, held 60s",
            ),
            (
                "latency",
                fresh.then_some(p.consecutive_slow >= 5),
                "warning",
                0,
                p.sampled_at,
                "five consecutive successful probes above 5 seconds",
            ),
            (
                "availability",
                fresh.then_some(p.consecutive_failures >= 3),
                "critical",
                0,
                p.sampled_at,
                "three consecutive failed probes",
            ),
            (
                "correctness",
                (fresh && (p.category.is_empty() || p.category == "answer_mismatch"))
                    .then_some(p.category == "answer_mismatch"),
                "critical",
                0,
                p.sampled_at,
                "decoded answer must match independent oracle",
            ),
            (
                "oracle",
                fresh.then_some(p.category == "oracle_invalid"),
                "warning",
                0,
                p.sampled_at,
                "oracle must remain canonical",
            ),
        ] {
            result.push(Condition {
                retired: false,
                key: format!("service_canary_{service}_{name}"),
                resource: service.clone(),
                severity: severity.into(),
                firing,
                observed: format!(
                    "{} consecutive failures; {}",
                    p.consecutive_failures,
                    if p.category.is_empty() {
                        "exact"
                    } else {
                        &p.category
                    }
                ),
                threshold: threshold.into(),
                hold_seconds: hold,
                sample,
            });
        }
    }
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unavailable_oracle_cannot_recover_correctness_incident() {
        let data = BTreeMap::from([(
            "transparent".into(),
            Probe {
                sampled_at: 100,
                category: "oracle_invalid".into(),
                ..Default::default()
            },
        )]);
        let c = conditions(&data, 110);
        assert_eq!(
            c.iter()
                .find(|c| c.key.ends_with("correctness"))
                .unwrap()
                .firing,
            None
        );
    }
}
