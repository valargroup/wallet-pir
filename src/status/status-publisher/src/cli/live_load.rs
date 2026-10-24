//! `probe-live-load`: the open-loop live mined-answer load that the deploy exact
//! check runs against a published Status origin.
use super::{require, AnyError};
use crate::{status, zakura::ZakuraClient};
use serde_json::json;
use status_shared::protocol::*;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

/// A session can expire after initialization but before its answer arrives.
/// Other unavailable responses remain load failures, including a 503 while
/// the selected manifest is still fresh.
fn expired_session(
    error: &AnyError,
    manifest: &Manifest,
    now_ms: u64,
    session_status: Option<u16>,
) -> bool {
    matches!(error.downcast_ref::<Error>(), Some(Error::Stale))
        || (error
            .downcast_ref::<reqwest::Error>()
            .and_then(reqwest::Error::status)
            == Some(reqwest::StatusCode::SERVICE_UNAVAILABLE)
            && session_status == Some(503)
            && matches!(manifest.fresh(now_ms), Err(Error::Stale)))
}

fn record_stage(
    stages: &mut Vec<serde_json::Value>,
    start: Instant,
    stage: &str,
    attempt: usize,
    manifest: Option<&Manifest>,
    http_status: Option<u16>,
) {
    let now_ms = status::now_ms();
    stages.push(
        json!({"stage":stage,"elapsed_ms":start.elapsed().as_secs_f64()*1000.,
        "attempt":attempt,"http_status":http_status,
        "generation":manifest.map(|m|m.generation),
        "manifest_id":manifest.map(|m|hex::encode(m.id())),
        "observed_ms":manifest.map(|m|m.observed_ms),
        "observation_age_ms":manifest.map(|m|now_ms.saturating_sub(m.observed_ms)),
        "remaining_lifetime_ms":manifest.map(|m|m.observed_ms.saturating_add(MAX_AGE_MS).saturating_sub(now_ms))}),
    );
}

/// After expiry, wait for a newly observed identity within the caller's
/// original lookup deadline. A cached copy of the expired identity never
/// authorizes another query. The caller independently checks its anchor.
async fn next_manifest(
    http: &reqwest::Client,
    origin: &str,
    expired: Option<&Manifest>,
    start: Instant,
    attempt: usize,
    stages: &mut Vec<serde_json::Value>,
) -> Result<Manifest, AnyError> {
    loop {
        let response = http.get(format!("{origin}/v1/status/init")).send().await?;
        let code = response.status().as_u16();
        if !response.status().is_success() {
            record_stage(stages, start, "init", attempt, None, Some(code));
        }
        let manifest: Manifest = response.error_for_status()?.json().await?;
        record_stage(stages, start, "init", attempt, Some(&manifest), Some(code));
        if let Some(old) = expired {
            if manifest.id() == old.id() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            require(
                manifest.network == old.network
                    && manifest.salt == old.salt
                    && manifest.recovery_epoch >= old.recovery_epoch
                    && manifest.generation >= old.generation
                    && manifest.observed_ms > old.observed_ms,
                "replacement Status session has no newer compatible source observation",
            )?;
            manifest.fresh(status::now_ms())?;
        }
        return Ok(manifest);
    }
}

pub async fn probe_live_load(
    origin: String,
    rpc_url: String,
    cookie: PathBuf,
    seconds: u64,
    report_dir: PathBuf,
) -> Result<(), AnyError> {
    live_load(
        origin,
        ZakuraClient::from_cookie_file(rpc_url, cookie)?,
        seconds,
        report_dir,
    )
    .await
}

async fn live_load(
    origin: String,
    rpc: ZakuraClient,
    seconds: u64,
    report_dir: PathBuf,
) -> Result<(), AnyError> {
    require(
        seconds > 0 && seconds <= 21_660,
        "live load duration must be 1..=21660 seconds",
    )?;
    std::fs::create_dir_all(&report_dir)?;
    let mut report = BufWriter::new(
        File::options()
            .create_new(true)
            .write(true)
            .open(report_dir.join("requests.jsonl"))?,
    );
    let mut summary = File::options()
        .create_new(true)
        .write(true)
        .open(report_dir.join("summary.jsonl"))?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let network = rpc.block_hash(0).await?;
    let permits = Arc::new(tokio::sync::Semaphore::new(16));
    let cache = Arc::new(tokio::sync::Mutex::new(std::collections::BTreeMap::<
        Hash,
        Hash,
    >::new()));
    let started_ms = status::now_ms();
    let began = Instant::now();
    let offered = seconds * 20;
    let mut jobs = tokio::task::JoinSet::new();
    let mut latencies = Vec::new();
    let mut failed = 0u64;
    let mut unstarted = 0u64;
    for arrival in 0..offered {
        tokio::time::sleep_until(tokio::time::Instant::from_std(
            began + Duration::from_millis(arrival * 50),
        ))
        .await;
        let scheduled_ms = started_ms + arrival * 50;
        let deadline =
            tokio::time::Instant::from_std(began + Duration::from_millis(arrival * 50 + 5_000));
        match permits.clone().try_acquire_owned() {
            Err(_) => {
                unstarted += 1;
                writeln!(
                    report,
                    "{}",
                    json!({"arrival":arrival,"scheduled_ms":scheduled_ms,"completed_ms":status::now_ms(),"result":"unstarted"})
                )?;
            }
            Ok(permit) => {
                let (http, rpc, origin, cache) =
                    (http.clone(), rpc.clone(), origin.clone(), cache.clone());
                jobs.spawn(async move {
                    // Blocking wallet work keeps the admission permit even if the
                    // async deadline expires, bounding outstanding CPU jobs too.
                    let permit = Arc::new(permit);
                    let start = Instant::now();
                    let started_ms = status::now_ms();
                    let mut stage_completions = Vec::new();
                    let mut selected = None::<Manifest>;
                    let result = tokio::time::timeout_at(deadline, async {
                        let mut expired = None::<Manifest>;
                        for retry in 0..2 {
                            let mut session_status = None;
                            let attempt = async {
                            let manifest = next_manifest(&http, &origin, expired.as_ref(), start, retry, &mut stage_completions).await?;
                            selected = Some(manifest.clone());
                            manifest.fresh(status::now_ms())?;
                            let hash = rpc.block_hash(u64::from(manifest.anchor_height)).await?;
                            require(manifest.network == network && manifest.anchor_hash == hash, "oracle anchor mismatch")?;
                            let txid = {
                                let mut cache = cache.lock().await;
                                if let Some(txid) = cache.get(&hash) { *txid } else {
                                    let block = crate::source::status_block(&rpc, u64::from(manifest.anchor_height)).await?;
                                    require(block.hash == hash, "oracle canonical block changed")?;
                                    let txid = *block.txids.first().ok_or("oracle block empty")?;
                                    if cache.len() >= 4 { cache.clear(); }
                                    cache.insert(hash, txid);
                                    txid
                                }
                            };
                            record_stage(&mut stage_completions, start, "oracle", retry, Some(&manifest), None);
                            let public = http.get(format!("{origin}/v1/status/session/{}", hex::encode(manifest.id()))).send().await?;
                            session_status = Some(public.status().as_u16());
                            record_stage(&mut stage_completions, start, "session", retry, Some(&manifest), Some(public.status().as_u16()));
                            let public = public.error_for_status()?.bytes().await?;
                            let job_manifest = manifest.clone();
                            let cpu_permit = permit.clone();
                            let (client, query) = tokio::task::spawn_blocking(move || {
                                let _permit = cpu_permit;
                                let client = Client::new(job_manifest.clone(), &public, &AcceptedAnchor { network, height: job_manifest.anchor_height, hash })?;
                                let query = client.prepare(&txid, Some(job_manifest.anchor_height), status::now_ms())?;
                                Ok::<_, AnyError>((client, query))
                            }).await??;
                            record_stage(&mut stage_completions, start, "prepare", retry, Some(&manifest), None);
                            let response = http.post(format!("{origin}/v1/status/query")).body(query.body.clone()).send().await?;
                            session_status = Some(response.status().as_u16());
                            record_stage(&mut stage_completions, start, "query", retry, Some(&manifest), Some(response.status().as_u16()));
                            let bytes = response.error_for_status()?.bytes().await?;
                            let cpu_permit = permit.clone();
                            let observed = tokio::task::spawn_blocking(move || {
                                let _permit = cpu_permit;
                                client.decode(query, &bytes, status::now_ms())
                            }).await??;
                            record_stage(&mut stage_completions, start, "decode", retry, Some(&manifest), None);
                            require(observed == Observation::Mined(manifest.anchor_height), "independent mined oracle mismatch")?;
                            Ok::<_, AnyError>((manifest.generation, status::now_ms().saturating_sub(manifest.observed_ms)))
                            }.await;
                            if retry == 0 {
                                if let Err(error) = &attempt {
                                    let code = error.downcast_ref::<reqwest::Error>().and_then(reqwest::Error::status);
                                    if let Some(manifest) = &selected {
                                        if expired_session(error, manifest, status::now_ms(), session_status) {
                                            record_stage(&mut stage_completions, start, "expired_session", retry, Some(manifest), code.map(|c|c.as_u16()));
                                            expired = Some(manifest.clone());
                                            continue;
                                        }
                                    }
                                    if matches!(session_status, Some(409 | 410)) {
                                        continue;
                                    }
                                }
                            }
                            return attempt;
                        }
                        Err("live load exhausted session retry".into())
                    }).await;
                    let (generation, age, error) = match result {
                        Ok(Ok((g, a))) => (Some(g), Some(a), None),
                        Ok(Err(e)) => (selected.as_ref().map(|m|m.generation), selected.as_ref().map(|m|status::now_ms().saturating_sub(m.observed_ms)), Some(e.to_string())),
                        Err(_) => (selected.as_ref().map(|m|m.generation), selected.as_ref().map(|m|status::now_ms().saturating_sub(m.observed_ms)), Some("overall lookup timeout".into())),
                    };
                    let service_ms = start.elapsed().as_secs_f64() * 1000.;
                    let completed_ms = status::now_ms();
                    let ms = completed_ms.saturating_sub(scheduled_ms) as f64;
                    (ms, error.is_none(), json!({"arrival":arrival,"scheduled_ms":scheduled_ms,"started_ms":started_ms,"completed_ms":completed_ms,
                        "latency_basis":"scheduled_to_completed","start_lag_ms":started_ms.saturating_sub(scheduled_ms),"service_ms":service_ms,"stage_completions":stage_completions,"duration_ms":ms,"generation":generation,"observation_age_ms":age,"result":if error.is_none() {"correct"} else {"error"},"error":error}))
                });
            }
        }
        while let Some(result) = jobs.try_join_next() {
            let (ms, correct, row) = result?;
            if correct {
                latencies.push(ms);
            } else {
                failed += 1;
            }
            writeln!(report, "{row}")?;
        }
        if arrival % 20 == 0 {
            report.flush()?;
        }
    }
    while let Some(result) = jobs.join_next().await {
        let (ms, correct, row) = result?;
        if correct {
            latencies.push(ms);
        } else {
            failed += 1;
        }
        writeln!(report, "{row}")?;
    }
    tokio::time::sleep_until(tokio::time::Instant::from_std(
        began + Duration::from_secs(seconds),
    ))
    .await;
    report.flush()?;
    latencies.sort_by(f64::total_cmp);
    let p99 = latencies
        .get((latencies.len() as f64 * 0.99).ceil().max(1.) as usize - 1)
        .copied();
    let result = json!({"phase":"load","source":"live","oracle_source":"independent","oracle_coverage":"canonical_mined","latency_basis":"scheduled_to_completed",
        "seconds":seconds,"qps":20,"offered":offered,"correct":latencies.len(),"failed":failed,"unstarted":unstarted,
        "p99_ms":p99,"run_started_ms":started_ms,"run_ended_ms":status::now_ms(),"protocol":PROTOCOL,"rows":ROWS,"slots":SLOTS,"slot_bytes":SLOT_BYTES,"columns":ROW_BYTES/2,"row_bytes":ROW_BYTES,"database_bytes":ROWS*ROW_BYTES,"production_qualified":false});
    writeln!(summary, "{result}")?;
    summary.sync_all()?;
    println!("{result}");
    require(
        failed == 0 && unstarted == 0 && p99.is_some_and(|ms| ms <= 1000.),
        "live load gate failed",
    )
}

#[cfg(all(test, feature = "fixtures", feature = "native-reinspiring"))]
#[path = "live_load_tests.rs"]
mod tests;
