//! `probe-live-load`: the open-loop live mined-answer load that the deploy exact
//! check runs against a published Status origin.
use super::{require, AnyError};
use crate::{status, zakura::ZakuraClient};
use enhance_pir::status::*;
use serde_json::json;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

pub async fn probe_live_load(
    origin: String,
    query_origin: Option<String>,
    rpc_url: String,
    cookie: PathBuf,
    seconds: u64,
    report_dir: PathBuf,
) -> Result<(), AnyError> {
    live_load(
        query_origin.unwrap_or_else(|| origin.clone()),
        origin,
        ZakuraClient::from_cookie_file(rpc_url, cookie)?,
        seconds,
        report_dir,
    )
    .await
}

async fn live_load(
    query_origin: String,
    origin: String,
    rpc: ZakuraClient,
    seconds: u64,
    report_dir: PathBuf,
) -> Result<(), AnyError> {
    require(
        seconds > 0 && seconds <= 21_600,
        "live load duration must be 1..=21600 seconds",
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
                let (http, rpc, origin, query_origin, cache) = (
                    http.clone(),
                    rpc.clone(),
                    origin.clone(),
                    query_origin.clone(),
                    cache.clone(),
                );
                jobs.spawn(async move {
                    // Blocking wallet work keeps the admission permit even if the
                    // async deadline expires, bounding outstanding CPU jobs too.
                    let permit = Arc::new(permit);
                    let start = Instant::now();
                    let started_ms = status::now_ms();
                    let mut stage_completions = Vec::new();
                    let result = tokio::time::timeout_at(deadline, async {
                        for retry in 0..2 {
                            let manifest: Manifest = http.get(format!("{origin}/v1/status/init")).send().await?.error_for_status()?.json().await?;
                            manifest.fresh(status::now_ms())?;
                            stage_completions.push(json!({"stage":"init","elapsed_ms":start.elapsed().as_secs_f64()*1000.,"attempt":retry}));
                            let hash = rpc.block_hash(u64::from(manifest.anchor_height)).await?;
                            require(manifest.network == network && manifest.anchor_hash == hash, "oracle anchor mismatch")?;
                            let txid = {
                                let mut cache = cache.lock().await;
                                if let Some(txid) = cache.get(&hash) { *txid } else {
                                    let block = crate::status::source::status_block(&rpc, u64::from(manifest.anchor_height)).await?;
                                    require(block.hash == hash, "oracle canonical block changed")?;
                                    let txid = *block.txids.first().ok_or("oracle block empty")?;
                                    if cache.len() >= 4 { cache.clear(); }
                                    cache.insert(hash, txid);
                                    txid
                                }
                            };
                            stage_completions.push(json!({"stage":"oracle","elapsed_ms":start.elapsed().as_secs_f64()*1000.,"attempt":retry}));
                            let public = http.get(format!("{origin}/v1/status/session/{}", hex::encode(manifest.id()))).send().await?;
                            if retry == 0 && matches!(public.status().as_u16(), 409 | 410) { continue; }
                            let public = public.error_for_status()?.bytes().await?;
                            stage_completions.push(json!({"stage":"session","elapsed_ms":start.elapsed().as_secs_f64()*1000.,"attempt":retry}));
                            let job_manifest = manifest.clone();
                            let cpu_permit = permit.clone();
                            let (client, query) = tokio::task::spawn_blocking(move || {
                                let _permit = cpu_permit;
                                let client = Client::new(job_manifest.clone(), &public, &AcceptedAnchor { network, height: job_manifest.anchor_height, hash })?;
                                let query = client.prepare(&txid, Some(job_manifest.anchor_height), status::now_ms())?;
                                Ok::<_, AnyError>((client, query))
                            }).await??;
                            stage_completions.push(json!({"stage":"prepare","elapsed_ms":start.elapsed().as_secs_f64()*1000.,"attempt":retry}));
                            let response = http.post(format!("{query_origin}/v1/status/query")).body(query.body.clone()).send().await?;
                            if retry == 0 && matches!(response.status().as_u16(), 409 | 410) { continue; }
                            let bytes = response.error_for_status()?.bytes().await?;
                            stage_completions.push(json!({"stage":"query","elapsed_ms":start.elapsed().as_secs_f64()*1000.,"attempt":retry}));
                            let cpu_permit = permit.clone();
                            let observed = tokio::task::spawn_blocking(move || {
                                let _permit = cpu_permit;
                                client.decode(query, &bytes, status::now_ms())
                            }).await??;
                            stage_completions.push(json!({"stage":"decode","elapsed_ms":start.elapsed().as_secs_f64()*1000.,"attempt":retry}));
                            require(observed == Observation::Mined(manifest.anchor_height), "independent mined oracle mismatch")?;
                            return Ok::<_, AnyError>((manifest.generation, status::now_ms().saturating_sub(manifest.observed_ms)));
                        }
                        Err("live load exhausted session retry".into())
                    }).await;
                    let (generation, age, error) = match result {
                        Ok(Ok((g, a))) => (Some(g), Some(a), None),
                        Ok(Err(e)) => (None, None, Some(e.to_string())),
                        Err(_) => (None, None, Some("overall lookup timeout".into())),
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
        "query_path":if query_origin == origin {"same_origin"} else {"separate_query_origin"},"p99_ms":p99,"run_started_ms":started_ms,"run_ended_ms":status::now_ms(),"protocol":PROTOCOL,"rows":ROWS,"slots":SLOTS,"slot_bytes":SLOT_BYTES,"columns":ROW_BYTES/2,"row_bytes":ROW_BYTES,"database_bytes":ROWS*ROW_BYTES,"production_qualified":false});
    writeln!(summary, "{result}")?;
    summary.sync_all()?;
    println!("{result}");
    require(
        failed == 0 && unstarted == 0 && p99.is_some_and(|ms| ms <= 1000.),
        "live load gate failed",
    )
}
