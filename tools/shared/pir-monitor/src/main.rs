//! Independent, low-rate public PIR correctness and monitoring-progress checker.
mod service_probes;
use anyhow::{Context, Result};
use axum::{extract::State, routing::get, Json, Router};
use enhance_shared::client::EnhancePirClient;
use pir_apm::incidents::{self, Condition, DeliveryHealth, Incident, Store};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Oracle {
    anchor_height: u64,
    anchor_hash: String,
    records: Vec<Record>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    position: u64,
    record_hex: String,
}
/// One canary sample of an origin.
#[derive(Clone, Default, Serialize)]
struct Canary {
    at: u64,
    ok: bool,
    duration_seconds: f64,
    failure_category: String,
}
#[derive(Clone, Default, Serialize)]
struct View {
    services: std::collections::BTreeMap<String, service_probes::Probe>,
    evaluated_at: u64,
    canary_at: u64,
    canary_ok: bool,
    canary_duration_seconds: f64,
    failure_category: String,
    /// The latest sample of each `PIR_MONITOR_CANARY_ORIGINS` origin, by name.
    origins: std::collections::BTreeMap<String, Canary>,
    shadow: bool,
    storage_error: bool,
    delivery: DeliveryHealth,
    incidents: Vec<Incident>,
}
/// Where service incidents link unless `PIR_MONITOR_PUBLIC_STATUS_URL` overrides it; the
/// public route of `tools/enhance/deploy/pir-monitor.Caddyfile`.
const PUBLIC_STATUS_URL: &str = "https://monitor-pir.valargroup.dev/monitor-status";
fn env(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("{key} is required"))
}
fn condition(
    key: &str,
    severity: &str,
    firing: Option<bool>,
    sample: u64,
    hold: u64,
    observed: String,
) -> Condition {
    Condition {
        retired: false,
        key: key.into(),
        resource: if key.starts_with("canary") {
            "public_pir"
        } else {
            "apm"
        }
        .into(),
        severity: severity.into(),
        firing,
        observed,
        // An extra origin's keys are the primary's with `.<name>` appended.
        threshold: match key.split_once('.').map_or(key, |(base, _)| base) {
            "apm_progress" => "APM progress unavailable for 60 seconds",
            "oracle_validity" => "pinned oracle block must remain canonical",
            "oracle_monitoring" => "oracle reference unavailable for 60 seconds",
            "canary_progress" => "canary sample older than 90 seconds for 60 seconds",
            "canary_failures" => "three consecutive failed one-minute probes",
            "canary_correctness" => "decrypted bytes must match the validated oracle",
            "canary_latency" => "five consecutive successful probes above 10 seconds",
            "apm_delivery_warning" => "Slack unconfigured or oldest pending event >=60 seconds",
            "apm_delivery_critical" => "Slack unconfigured or oldest pending event >=300 seconds",
            _ => key,
        }
        .into(),
        hold_seconds: hold,
        window_seconds: 0,
        sample,
    }
}
/// Evaluates the service probes' conditions. They cover Status, Transparent, receiver and txid, not
/// one APM, so their incidents link to this monitor's public status at `status_url`.
fn evaluate_services(
    store: &mut Store,
    probes: &std::collections::BTreeMap<String, service_probes::Probe>,
    now: u64,
    shadow: bool,
    status_url: &str,
) -> Result<()> {
    store.evaluate(
        &service_probes::conditions(probes, now),
        now,
        shadow,
        "production",
        status_url,
    )
}
/// Parses `PIR_MONITOR_CANARY_ORIGINS`: space-separated `<name>=<origin>` pairs,
/// each an extra origin that serves the same publication as `PIR_MONITOR_ORIGIN`,
/// such as a unified query node wallets reach directly.
fn canary_origins(text: &str) -> Result<Vec<(String, String)>> {
    let mut names = std::collections::BTreeSet::new();
    text.split_whitespace()
        .map(|pair| {
            let (name, origin) = pair
                .split_once('=')
                .context("canary origins are <name>=<origin> pairs")?;
            anyhow::ensure!(
                !name.is_empty()
                    && name.len() <= 64
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
                    && names.insert(name.to_string()),
                "invalid or duplicate canary origin name {name:?}"
            );
            let url = reqwest::Url::parse(origin).context("invalid canary origin")?;
            anyhow::ensure!(
                matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
                "invalid canary origin {origin:?}"
            );
            Ok((name.into(), origin.into()))
        })
        .collect()
}

/// Runs one wallet-shaped sample against `origin`: a fresh connection and
/// init, then one query whose decrypted record must match the oracle.
async fn sample(origin: &str, oracle: &Oracle, record: &Record) -> Canary {
    let began = Instant::now();
    let probe = async {
        let mut client = EnhancePirClient::connect(origin)
            .await
            .map_err(|_| "init_failed")?;
        if client.manifest().anchor_height < oracle.anchor_height {
            return Err("coverage_behind_oracle");
        }
        let (answer, _) = client
            .query_position_with_timing(record.position)
            .await
            .map_err(|_| "query_failed")?;
        if hex::encode(answer.as_bytes()) != record.record_hex.to_ascii_lowercase() {
            return Err("answer_mismatch");
        }
        Ok(())
    };
    let category = match tokio::time::timeout(Duration::from_secs(20), probe).await {
        Ok(Ok(())) => "",
        Ok(Err(category)) => category,
        Err(_) => "deadline",
    };
    Canary {
        at: incidents::unix_time(),
        ok: category.is_empty(),
        duration_seconds: began.elapsed().as_secs_f64(),
        failure_category: category.into(),
    }
}

/// Consecutive failed and slow samples of one origin, counted once per sample.
#[derive(Default)]
struct Streak {
    last: u64,
    failures: u32,
    slow: u32,
}
impl Streak {
    /// Counts `canary` if it is newer than the last sample counted.
    fn update(&mut self, canary: &Canary) {
        if canary.at <= self.last {
            return;
        }
        self.last = canary.at;
        self.failures = if canary.ok {
            0
        } else {
            self.failures.saturating_add(1)
        };
        self.slow = if canary.ok && canary.duration_seconds > 10. {
            self.slow.saturating_add(1)
        } else {
            0
        };
    }
}

/// The failure, correctness and latency conditions of one origin's canary.
/// `suffix` is empty for `PIR_MONITOR_ORIGIN`, whose keys predate extra origins,
/// and `.<name>` for an extra one.
fn canary_conditions(
    suffix: &str,
    canary: &Canary,
    streak: &Streak,
    oracle_valid: Option<bool>,
    origin: &str,
) -> Vec<Condition> {
    let sample = canary.at;
    let (failures, slow) = (streak.failures, streak.slow);
    let at = if suffix.is_empty() {
        String::new()
    } else {
        format!(" at {origin}")
    };
    vec![
        condition(
            &format!("canary_failures{suffix}"),
            "critical",
            if sample == 0 || (failures > 0 && failures < 3) {
                None
            } else {
                Some(failures >= 3)
            },
            sample,
            0,
            format!(
                "{failures} consecutive failures; {}{at}",
                canary.failure_category
            ),
        ),
        condition(
            &format!("canary_correctness{suffix}"),
            "critical",
            if oracle_valid == Some(true)
                && sample > 0
                && (canary.ok || canary.failure_category == "answer_mismatch")
            {
                Some(canary.failure_category == "answer_mismatch")
            } else {
                None
            },
            sample,
            0,
            format!("decrypted bytes compared with pinned oracle{at}"),
        ),
        condition(
            &format!("canary_latency{suffix}"),
            "warning",
            if sample == 0 || (slow > 0 && slow < 5) {
                None
            } else {
                Some(slow >= 5)
            },
            sample,
            0,
            format!("{slow} consecutive samples above 10s{at}"),
        ),
    ]
}

fn load_oracle(bytes: &[u8], checksum: &str) -> Result<Oracle> {
    anyhow::ensure!(
        hex::encode(Sha256::digest(bytes)) == checksum,
        "oracle checksum mismatch"
    );
    let oracle: Oracle = serde_json::from_slice(bytes)?;
    anyhow::ensure!(
        !oracle.records.is_empty()
            && oracle.anchor_hash.len() == 64
            && hex::decode(&oracle.anchor_hash).is_ok(),
        "invalid oracle provenance"
    );
    let mut positions = std::collections::BTreeSet::new();
    for r in &oracle.records {
        anyhow::ensure!(
            positions.insert(r.position)
                && hex::decode(&r.record_hex)?.len() == enhance_shared::RECORD_BYTES,
            "invalid oracle record"
        );
    }
    Ok(oracle)
}

#[tokio::main]
async fn main() -> Result<()> {
    let origin = env("PIR_MONITOR_ORIGIN")?;
    let extra_origins =
        canary_origins(&std::env::var("PIR_MONITOR_CANARY_ORIGINS").unwrap_or_default())?;
    let status_url = env("PIR_MONITOR_APM_STATUS_URL")?;
    // Incidents link to the dashboard that serves the APM status this monitor reads.
    let dashboard_url = match status_url.rsplit_once('/') {
        Some((parent, _)) => format!("{parent}/"),
        None => anyhow::bail!("PIR_MONITOR_APM_STATUS_URL has no path"),
    };
    let oracle = load_oracle(
        &std::fs::read(env("PIR_MONITOR_ORACLE")?)?,
        &env("PIR_MONITOR_ORACLE_SHA256")?,
    )?;
    let path = PathBuf::from(env("PIR_MONITOR_STATE_PATH")?);
    let webhook = std::env::var("PIR_APM_SLACK_WEBHOOK_URL")
        .ok()
        .filter(|v| !v.trim().is_empty());
    let configured = webhook.is_some();
    let mode = std::env::var("PIR_MONITOR_ALERT_MODE").unwrap_or_else(|_| "shadow".into());
    anyhow::ensure!(mode == "shadow" || mode == "active", "invalid alert mode");
    let shadow = mode == "shadow";
    let listen = std::env::var("PIR_MONITOR_LISTEN").unwrap_or_else(|_| "127.0.0.1:3003".into());
    let public_status = std::env::var("PIR_MONITOR_PUBLIC_STATUS_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| PUBLIC_STATUS_URL.into());
    let mut store = Store::open(&path)?;
    let mut service_store = Store::open(&path)?.with_family("service-quality");
    let service_mode =
        std::env::var("PIR_MONITOR_SERVICE_ALERT_MODE").unwrap_or_else(|_| "shadow".into());
    anyhow::ensure!(
        matches!(service_mode.as_str(), "shadow" | "active"),
        "invalid service alert mode"
    );
    let probes = service_probes::start()?;
    tokio::spawn(async move {
        if incidents::deliver(path, webhook).await.is_err() {
            eprintln!("notification worker stopped");
        }
    });
    let shared = Arc::new(RwLock::new(View {
        shadow,
        ..Default::default()
    }));
    let oracle_check = oracle.clone();
    // The primary origin and each extra one sample once a minute on their own
    // task, so a slow origin never delays another.
    let mut origins = vec![(None, origin.clone())];
    origins.extend(extra_origins.iter().cloned().map(|(n, o)| (Some(n), o)));
    for (name, probe_origin) in origins {
        let canary = shared.clone();
        let oracle = oracle.clone();
        tokio::spawn(async move {
            let mut cursor = 0;
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let r = &oracle.records[cursor % oracle.records.len()];
                cursor += 1;
                // Full fresh connection/init on every sample; no cached success path.
                let result = sample(&probe_origin, &oracle, r).await;
                let mut v = canary.write().await;
                match &name {
                    None => {
                        v.canary_at = result.at;
                        v.canary_ok = result.ok;
                        v.failure_category = result.failure_category;
                        v.canary_duration_seconds = result.duration_seconds;
                    }
                    Some(name) => {
                        v.origins.insert(name.clone(), result);
                    }
                }
            }
        });
    }
    let eval = shared.clone();
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(4))
            .build()
            .expect("status client");
        let mut tick = tokio::time::interval(Duration::from_secs(15));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut primary = Streak::default();
        let mut streaks: std::collections::BTreeMap<String, Streak> = Default::default();
        loop {
            tick.tick().await;
            let status = async {
                client
                    .get(&status_url)
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<serde_json::Value>()
                    .await
            }
            .await;
            let now = incidents::unix_time();
            let snapshot = eval.read().await.clone();
            let body = status.as_ref().ok();
            let progress = body.is_some_and(|v| {
                v["progress_ok"].as_bool() == Some(true)
                    && v["evaluated_at"]
                        .as_u64()
                        .is_some_and(|at| now.saturating_sub(at) <= 45 && at <= now)
            });
            let oracle_valid = body.and_then(|v| {
                let chain = &v["chain"];
                if chain["oracle_anchor_height"].as_u64() != Some(oracle_check.anchor_height)
                    || chain["oracle_anchor_hash"].as_str()
                        != Some(oracle_check.anchor_hash.as_str())
                    || chain["error"].as_bool() != Some(false)
                    || !chain["sampled_at"]
                        .as_u64()
                        .is_some_and(|at| now.saturating_sub(at) <= 45 && at <= now)
                {
                    return None;
                }
                chain["oracle_valid"].as_bool()
            });
            let primary_canary = Canary {
                at: snapshot.canary_at,
                ok: snapshot.canary_ok,
                duration_seconds: snapshot.canary_duration_seconds,
                failure_category: snapshot.failure_category.clone(),
            };
            primary.update(&primary_canary);
            let sample = snapshot.canary_at;
            let mut checks = vec![
                condition(
                    "apm_progress",
                    "critical",
                    Some(!progress),
                    now,
                    60,
                    "APM collector/evaluator/delivery progress".into(),
                ),
                condition(
                    "oracle_validity",
                    "warning",
                    oracle_valid.map(|valid| !valid),
                    now,
                    0,
                    "canonical oracle anchor validation".into(),
                ),
                condition(
                    "oracle_monitoring",
                    "warning",
                    Some(oracle_valid.is_none()),
                    now,
                    60,
                    "oracle reference unavailable".into(),
                ),
                condition(
                    "canary_progress",
                    "critical",
                    Some(sample == 0 || now.saturating_sub(sample) > 90),
                    now,
                    60,
                    "canary task progress".into(),
                ),
            ];
            checks.extend(canary_conditions(
                "",
                &primary_canary,
                &primary,
                oracle_valid,
                &origin,
            ));
            for (name, extra) in &extra_origins {
                let canary = snapshot.origins.get(name).cloned().unwrap_or_default();
                let streak = streaks.entry(name.clone()).or_default();
                streak.update(&canary);
                checks.extend(canary_conditions(
                    &format!(".{name}"),
                    &canary,
                    streak,
                    oracle_valid,
                    extra,
                ));
            }
            for (severity, age) in [("warning", 60), ("critical", 300)] {
                checks.push(condition(
                    &format!("apm_delivery_{severity}"),
                    severity,
                    body.map(|v| {
                        v["delivery"]["oldest_pending_age"].as_u64().unwrap_or(0) >= age
                            || v["delivery"]["configured"].as_bool() != Some(true)
                    }),
                    now,
                    0,
                    "APM Slack delivery health".into(),
                ));
            }
            let probe_snapshot = probes.read().await.clone();
            // Pause and resume with APM's deployment window; an unreachable APM
            // leaves the window as it is, and it still expires on its own.
            let owner = body.map(|v| {
                serde_json::from_value::<Option<pir_apm::deployment::Deployment>>(
                    v["deployment"].clone(),
                )
                .ok()
                .flatten()
            });
            let result = (|| -> Result<_> {
                if let Some(owner) = &owner {
                    store.mirror_deployment(owner.as_ref(), now)?;
                }
                evaluate_services(
                    &mut service_store,
                    &probe_snapshot,
                    now,
                    service_mode != "active",
                    &public_status,
                )?;
                store.evaluate(&checks, now, shadow, "production", &dashboard_url)?;
                Ok((store.incidents()?, store.health(configured, now)?))
            })();
            let mut v = eval.write().await;
            v.services = probe_snapshot;
            v.evaluated_at = now;
            v.storage_error = result.is_err();
            if let Ok((incidents, delivery)) = result {
                v.incidents = incidents;
                v.delivery = delivery;
            }
        }
    });
    async fn status(State(v): State<Arc<RwLock<View>>>) -> Json<serde_json::Value> {
        let v = v.read().await;
        let now = incidents::unix_time();
        let progress = v.evaluated_at > 0
            && now.saturating_sub(v.evaluated_at) <= 45
            && v.canary_at > 0
            && now.saturating_sub(v.canary_at) <= 90
            && v.delivery.worker_at > 0
            && now.saturating_sub(v.delivery.worker_at) <= 45
            && !v.storage_error;
        let mut body = serde_json::to_value(&*v).expect("view serialization");
        body["progress_ok"] = progress.into();
        Json(body)
    }
    let app = Router::new()
        .route("/monitor-status", get(status))
        .with_state(shared);
    axum::serve(tokio::net::TcpListener::bind(listen).await?, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn oracle_is_pinned_and_rejects_duplicate_positions() {
        let bytes=serde_json::to_vec(&serde_json::json!({"anchor_height":10,"anchor_hash":"00".repeat(32),"records":[{"position":1,"record_hex":"00".repeat(enhance_shared::RECORD_BYTES)}]})).unwrap();
        let checksum = hex::encode(Sha256::digest(&bytes));
        assert!(load_oracle(&bytes, &checksum).is_ok());
        assert!(load_oracle(&bytes, "invalid").is_err());
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let r = value["records"][0].clone();
        value["records"].as_array_mut().unwrap().push(r);
        let bytes = serde_json::to_vec(&value).unwrap();
        assert!(load_oracle(&bytes, &hex::encode(Sha256::digest(&bytes))).is_err());
    }

    #[test]
    fn canary_origins_are_named_unique_http_origins() {
        assert!(canary_origins("").unwrap().is_empty());
        let parsed = canary_origins(
            "query-02=https://wallet-pir-query-02.valargroup.dev  b_2=http://10.0.0.1:8090",
        )
        .unwrap();
        assert_eq!(parsed[0].0, "query-02");
        assert_eq!(parsed[1].1, "http://10.0.0.1:8090");
        for bad in [
            "https://no-name.example",
            "=https://a.example",
            "a.b=https://a.example",
            "a=ftp://a.example",
            "a=https://a.example a=https://b.example",
        ] {
            assert!(canary_origins(bad).is_err(), "{bad}");
        }
    }

    /// An extra origin pages under its own keys with the primary's thresholds,
    /// and the primary's keys are unchanged.
    #[test]
    fn each_origin_pages_under_its_own_keys() {
        let failed = Canary {
            at: 100,
            failure_category: "init_failed".into(),
            ..Default::default()
        };
        let mut streak = Streak::default();
        for at in [100, 100, 160, 220] {
            streak.update(&Canary {
                at,
                ..failed.clone()
            });
        }
        assert_eq!(streak.failures, 3, "a sample counts once");
        let failed = Canary { at: 220, ..failed };
        let extra = canary_conditions(
            ".query-02",
            &failed,
            &streak,
            Some(true),
            "https://q.example",
        );
        let keys: Vec<_> = extra.iter().map(|c| c.key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "canary_failures.query-02",
                "canary_correctness.query-02",
                "canary_latency.query-02"
            ]
        );
        assert_eq!(extra[0].firing, Some(true));
        assert_eq!(
            extra[0].threshold,
            "three consecutive failed one-minute probes"
        );
        assert!(extra[0]
            .observed
            .ends_with("init_failed at https://q.example"));
        assert_eq!(extra[0].resource, "public_pir");
        let primary = canary_conditions("", &failed, &streak, Some(true), "https://p.example");
        assert_eq!(primary[0].key, "canary_failures");
        assert!(primary[0].observed.ends_with("init_failed"));
    }

    #[test]
    fn service_incidents_link_to_the_public_monitor_status() {
        let caddy = include_str!("../../../../tools/enhance/deploy/pir-monitor.Caddyfile");
        let (host, path) = PUBLIC_STATUS_URL
            .strip_prefix("https://")
            .unwrap()
            .split_once('/')
            .unwrap();
        assert!(caddy.starts_with(&format!("{host} {{")));
        assert!(caddy.contains(&format!("handle /{path} {{")));
        let services = ["receiver", "status", "transparent", "txid"];
        let probes: std::collections::BTreeMap<_, _> = services
            .iter()
            .map(|service| {
                let probe = service_probes::Probe {
                    sampled_at: 100,
                    consecutive_failures: 3,
                    category: "request_failed".into(),
                    ..Default::default()
                };
                (service.to_string(), probe)
            })
            .collect();
        // The default and a `PIR_MONITOR_PUBLIC_STATUS_URL` override.
        for url in [PUBLIC_STATUS_URL, "https://status.example/monitor-status"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("incidents.sqlite");
            let mut store = Store::open(&path).unwrap().with_family("service-quality");
            evaluate_services(&mut store, &probes, 100, false, url).unwrap();
            let db = rusqlite::Connection::open(&path).unwrap();
            let bodies: Vec<String> = db
                .prepare("SELECT body FROM outbox")
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert_eq!(bodies.len(), services.len());
            for service in services {
                let key = format!("service_canary_{service}_availability");
                let body = bodies.iter().find(|body| body.contains(&key)).unwrap();
                assert!(body.contains(&format!("<{url}|Dashboard>")), "{body}");
            }
        }
    }
}
