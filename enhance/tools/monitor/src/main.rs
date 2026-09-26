//! Independent, low-rate public PIR correctness and monitoring-progress checker.
use anyhow::{Context, Result};
use axum::{extract::State, routing::get, Json, Router};
use enhance_pir::client::EnhancePirClient;
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
#[derive(Clone, Default, Serialize)]
struct View {
    evaluated_at: u64,
    canary_at: u64,
    canary_ok: bool,
    canary_duration_seconds: f64,
    failure_category: String,
    shadow: bool,
    storage_error: bool,
    delivery: DeliveryHealth,
    incidents: Vec<Incident>,
}
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
        threshold: match key {
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
        sample,
    }
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
                && hex::decode(&r.record_hex)?.len() == enhance_pir::RECORD_BYTES,
            "invalid oracle record"
        );
    }
    Ok(oracle)
}

#[tokio::main]
async fn main() -> Result<()> {
    let origin = env("PIR_MONITOR_ORIGIN")?;
    let status_url = env("PIR_MONITOR_APM_STATUS_URL")?;
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
    let mut store = Store::open(&path)?;
    tokio::spawn(async move {
        if incidents::deliver(path, webhook).await.is_err() {
            eprintln!("notification worker stopped");
        }
    });
    let shared = Arc::new(RwLock::new(View {
        shadow,
        ..Default::default()
    }));
    let canary = shared.clone();
    let oracle_check = oracle.clone();
    let probe_origin = origin.clone();
    tokio::spawn(async move {
        let mut cursor = 0;
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let r = &oracle.records[cursor % oracle.records.len()];
            cursor += 1;
            let began = Instant::now();
            // Full fresh connection/init on every sample; no cached success path.
            let probe = async {
                let mut client = EnhancePirClient::connect(&probe_origin)
                    .await
                    .map_err(|_| "init_failed")?;
                if client.manifest().anchor_height < oracle.anchor_height {
                    return Err("coverage_behind_oracle");
                }
                let (answer, _) = client
                    .query_position_with_timing(r.position)
                    .await
                    .map_err(|_| "query_failed")?;
                if hex::encode(answer.as_bytes()) != r.record_hex.to_ascii_lowercase() {
                    return Err("answer_mismatch");
                }
                Ok(())
            };
            let result = tokio::time::timeout(Duration::from_secs(20), probe).await;
            let category = match result {
                Ok(Ok(())) => "",
                Ok(Err(category)) => category,
                Err(_) => "deadline",
            };
            let mut v = canary.write().await;
            v.canary_at = incidents::unix_time();
            v.canary_ok = category.is_empty();
            v.failure_category = category.into();
            v.canary_duration_seconds = began.elapsed().as_secs_f64();
        }
    });
    let eval = shared.clone();
    tokio::spawn(async move {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(4))
            .build()
            .expect("status client");
        let mut tick = tokio::time::interval(Duration::from_secs(15));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let (mut last_canary, mut failures, mut slow) = (0, 0u32, 0u32);
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
            if snapshot.canary_at > last_canary {
                last_canary = snapshot.canary_at;
                failures = if snapshot.canary_ok {
                    0
                } else {
                    failures.saturating_add(1)
                };
                slow = if snapshot.canary_ok && snapshot.canary_duration_seconds > 10. {
                    slow.saturating_add(1)
                } else {
                    0
                };
            }
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
                condition(
                    "canary_failures",
                    "critical",
                    if sample == 0 || (failures > 0 && failures < 3) {
                        None
                    } else {
                        Some(failures >= 3)
                    },
                    sample,
                    0,
                    format!(
                        "{failures} consecutive failures; {}",
                        snapshot.failure_category
                    ),
                ),
                condition(
                    "canary_correctness",
                    "critical",
                    if oracle_valid == Some(true)
                        && sample > 0
                        && (snapshot.canary_ok || snapshot.failure_category == "answer_mismatch")
                    {
                        Some(snapshot.failure_category == "answer_mismatch")
                    } else {
                        None
                    },
                    sample,
                    0,
                    "decrypted bytes compared with pinned oracle".into(),
                ),
                condition(
                    "canary_latency",
                    "warning",
                    if sample == 0 || (slow > 0 && slow < 5) {
                        None
                    } else {
                        Some(slow >= 5)
                    },
                    sample,
                    0,
                    format!("{slow} consecutive samples above 10s"),
                ),
            ];
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
            let result = (|| -> Result<_> {
                store.evaluate(
                    &checks,
                    now,
                    shadow,
                    "production",
                    &format!("{origin}/apm/"),
                )?;
                Ok((store.incidents()?, store.health(configured, now)?))
            })();
            let mut v = eval.write().await;
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
        let bytes=serde_json::to_vec(&serde_json::json!({"anchor_height":10,"anchor_hash":"00".repeat(32),"records":[{"position":1,"record_hex":"00".repeat(enhance_pir::RECORD_BYTES)}]})).unwrap();
        let checksum = hex::encode(Sha256::digest(&bytes));
        assert!(load_oracle(&bytes, &checksum).is_ok());
        assert!(load_oracle(&bytes, "invalid").is_err());
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let r = value["records"][0].clone();
        value["records"].as_array_mut().unwrap().push(r);
        let bytes = serde_json::to_vec(&value).unwrap();
        assert!(load_oracle(&bytes, &hex::encode(Sha256::digest(&bytes))).is_err());
    }
}
