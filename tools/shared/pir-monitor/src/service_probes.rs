//! Independent canaries for Status, Transparent history (v12), the receiver directory and
//! the one-query txid display. Separate native binaries avoid unifying Enhance and
//! Transparent PIR backends.
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
    /// Of `duration_seconds`, the time the probe reported waiting for the service to
    /// catch up with a new publication before the run it reports. Not query latency.
    pub waited_seconds: f64,
}
impl Probe {
    /// Count one run. A successful run is slow when it took over 5 s beyond the wait
    /// it reported.
    fn record(&mut self, category: String, duration_seconds: f64, waited_seconds: f64, now: u64) {
        self.sampled_at = now;
        self.duration_seconds = duration_seconds;
        self.waited_seconds = waited_seconds.clamp(0., duration_seconds);
        if category.is_empty() {
            self.successes += 1;
            self.consecutive_failures = 0;
        } else {
            self.failures += 1;
            self.consecutive_failures += 1;
        }
        self.consecutive_slow =
            if category.is_empty() && duration_seconds - self.waited_seconds > 5. {
                self.consecutive_slow + 1
            } else {
                0
            };
        self.category = category;
    }
}
/// What one probe run reported: its failure category, empty when it passed, and the
/// catch-up wait it reported.
struct Outcome {
    category: String,
    waited_seconds: f64,
}
pub type Shared = Arc<RwLock<BTreeMap<String, Probe>>>;
/// Services a probe may report: Status, the v12 Transparent history, the receiver
/// directory and the one-query txid display (`txid`).
pub const SERVICES: [&str; 4] = ["status", "transparent", "receiver", "txid"];
/// Parses the probe config: each service at most once, an absolute command, a
/// 1–45 second timeout.
fn parse(bytes: &[u8]) -> Result<Vec<Config>> {
    let configs: Vec<Config> = serde_json::from_slice(bytes)?;
    let mut names = std::collections::BTreeSet::new();
    for config in &configs {
        anyhow::ensure!(
            SERVICES.contains(&config.service.as_str())
                && names.insert(config.service.clone())
                && !config.command.is_empty()
                && PathBuf::from(&config.command[0]).is_absolute()
                && (1..=45).contains(&config.timeout_seconds),
            "invalid service probe config"
        );
    }
    Ok(configs)
}
pub fn start() -> Result<Shared> {
    let shared = Shared::default();
    let Some(path) = std::env::var_os("PIR_MONITOR_SERVICE_PROBES_CONFIG") else {
        return Ok(shared);
    };
    let configs = parse(&std::fs::read(path)?)?;
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
                let (category, waited) = match result {
                    Ok(Ok(outcome)) => (outcome.category, outcome.waited_seconds),
                    Ok(Err(_)) => ("probe_unavailable".into(), 0.),
                    Err(_) => ("deadline".into(), 0.),
                };
                let mut data = output.write().await;
                data.get_mut(&config.service).unwrap().record(
                    category,
                    began.elapsed().as_secs_f64(),
                    waited,
                    unix_time(),
                );
            }
        });
    }
    Ok(shared)
}
async fn run(config: &Config) -> Result<Outcome> {
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
        return Ok(Outcome {
            category: "invalid_probe_output".into(),
            waited_seconds: 0.,
        });
    }
    let status = child.wait().await?;
    let text = String::from_utf8_lossy(&out);
    let value = text
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<serde_json::Value>(line).ok());
    let waited_seconds = value
        .as_ref()
        .and_then(|v| v["waited_seconds"].as_f64())
        .filter(|w| w.is_finite())
        .unwrap_or(0.);
    let outcome = |category: &str| {
        Ok(Outcome {
            category: category.into(),
            waited_seconds,
        })
    };
    if let Some(v) = value {
        if v["category"].as_str() == Some("answer_mismatch") {
            return outcome("answer_mismatch");
        }
        if v["category"].as_str() == Some("oracle_invalid") {
            return outcome("oracle_invalid");
        }
        if status.success()
            && (v["passed"].as_bool() == Some(true)
                || (v["phase"].as_str() == Some("live_encrypted_probe")
                    && v["correct"].as_u64() == v["queries"].as_u64()
                    && v["correct"].as_u64().is_some_and(|n| n > 0)))
        {
            return outcome("");
        }
    }
    let err = String::from_utf8_lossy(&err);
    let category = if err.contains("differs from canonical block oracle") {
        "answer_mismatch"
    } else if err.contains("not canonical") || err.contains("raw block hash differs") {
        "oracle_invalid"
    } else {
        "request_failed"
    };
    outcome(category)
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
                window_seconds: 0,
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
    fn config_accepts_each_known_service_once() {
        let probe = |service: &str| serde_json::json!({"service": service, "command": ["/usr/local/bin/canary"], "timeout_seconds": 30});
        let all: Vec<_> = SERVICES.iter().map(|s| probe(s)).collect();
        let parsed = parse(&serde_json::to_vec(&all).unwrap()).unwrap();
        assert_eq!(
            parsed
                .iter()
                .map(|c| c.service.as_str())
                .collect::<Vec<_>>(),
            SERVICES
        );
        for bad in [
            vec![probe("enhance")],
            vec![probe("txid"), probe("txid")],
            vec![
                serde_json::json!({"service": "txid", "command": ["canary"], "timeout_seconds": 30}),
            ],
            vec![
                serde_json::json!({"service": "txid", "command": ["/bin/c"], "timeout_seconds": 46}),
            ],
        ] {
            assert!(parse(&serde_json::to_vec(&bad).unwrap()).is_err());
        }
    }
    #[test]
    fn txid_probe_conditions_are_keyed_by_service() {
        let data = BTreeMap::from([(
            "txid".to_string(),
            Probe {
                sampled_at: 100,
                category: "answer_mismatch".into(),
                ..Default::default()
            },
        )]);
        let c = conditions(&data, 110);
        let correctness = c
            .iter()
            .find(|c| c.key == "service_canary_txid_correctness")
            .unwrap();
        assert_eq!(correctness.firing, Some(true));
        assert_eq!(correctness.resource, "txid");
    }
    /// A run counts as slow by the time it took beyond the catch-up wait it reported,
    /// and a failure or a fast run ends a slow streak.
    #[test]
    fn a_reported_catch_up_wait_is_not_latency() {
        let mut probe = Probe::default();
        probe.record(String::new(), 14., 10., 1);
        assert_eq!((probe.consecutive_slow, probe.waited_seconds), (0, 10.));
        probe.record(String::new(), 14., 8., 2);
        probe.record(String::new(), 6., 0., 3);
        assert_eq!(probe.consecutive_slow, 2);
        // A reported wait never exceeds the run it is part of.
        probe.record(String::new(), 6., 60., 4);
        assert_eq!((probe.consecutive_slow, probe.waited_seconds), (0, 6.));
        probe.record(String::new(), 6., 0., 5);
        probe.record("request_failed".into(), 6., 0., 6);
        assert_eq!((probe.consecutive_slow, probe.consecutive_failures), (0, 1));
        assert_eq!((probe.successes, probe.failures), (5, 1));
    }
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
