//! Append-only per-attempt evidence, separate from logical wallet-call totals.
use crate::Args;
use anyhow::Result;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use transparent_wallet::http::HttpObserver;

fn path(args: &Args) -> PathBuf {
    let name = match &args.worker_case {
        Some(id) => format!("case-{id}.http.ndjson"),
        None => "preflight.http.ndjson".into(),
    };
    args.out_dir.join(name)
}

pub fn observer(args: &Args) -> Result<HttpObserver> {
    let file = Arc::new(Mutex::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path(args))?,
    ));
    Ok(Arc::new(move |event| {
        // Failure to retain evidence must fail this worker, not silently pass.
        let line = json!({"request_id":event.request_id,"attempt":event.attempt,
            "stage":event.stage,"status":event.status,"seconds":event.elapsed.as_secs_f64(),
            "submitted_payload_bytes":event.bytes_up,"completed_body_bytes":event.bytes_down,
            "failed":event.failed,"transport_error":event.transport_error});
        writeln!(file.lock().expect("HTTP evidence lock"), "{line}").expect("write HTTP evidence");
    }))
}

pub fn summary(args: &Args) -> Result<Value> {
    let text = std::fs::read_to_string(path(args))?;
    let events: Vec<Value> = text
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    let mut last = BTreeMap::new();
    for event in &events {
        last.insert(
            event["request_id"].as_u64().expect("recorded request id"),
            event,
        );
    }
    Ok(
        json!({"log":path(args).file_name().unwrap().to_string_lossy(),
        "attempts":events.len(),"requests":last.len(),
        "failed_attempts":events.iter().filter(|e| e["failed"] == true).count(),
        "recovered_requests":last.values().filter(|e| e["failed"] == false && e["attempt"].as_u64().unwrap() > 1).count(),
        "failed_requests":last.values().filter(|e| e["failed"] == true).count()}),
    )
}
