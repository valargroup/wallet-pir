//! Fixed-rate operator queries with fresh secrets and an independent row-hash oracle.
//! Input contains only sealed revisions. This measures PIR serving, not wallet syncs.
use clap::Parser;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{mpsc, Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use transparent_wallet::{
    client::{Table, TableClient},
    http::{HttpOptions, HttpShardTransport},
    transport::{BoxError, ShardTransport},
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    url: String,
    #[arg(long)]
    fixture: PathBuf,
    #[arg(long, default_value_t = 5)]
    qps: u32,
    #[arg(long, default_value_t = 8)]
    workers: usize,
    /// Zero runs until stopped. A supervisor must refresh the permit file.
    #[arg(long, default_value_t = 0)]
    seconds: u64,
    #[arg(long)]
    permit: PathBuf,
}
#[derive(Deserialize)]
struct Fixture {
    schema: String,
    tables: Vec<FixtureTable>,
}
#[derive(Deserialize)]
struct FixtureTable {
    shard_id: u64,
    revision: String,
    geometry: String,
    table: String,
    rows: u64,
    row_bytes: u32,
    segments: u32,
    samples: Vec<Sample>,
}
#[derive(Deserialize)]
struct Sample {
    row: usize,
    sha256: Vec<String>,
    nonempty: bool,
}
#[derive(Deserialize)]
struct Setup {
    public_params: String,
    public_params_sha256: String,
}
struct Target {
    fixture: FixtureTable,
    client: Arc<TableClient>,
    table: Table,
}
fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}
fn emit(v: serde_json::Value) {
    println!("{v}");
}
fn allowed(path: &std::path::Path) -> bool {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.elapsed().ok())
        .is_some_and(|age| age < Duration::from_secs(45))
        && std::fs::read_to_string(path).is_ok_and(|s| s.trim() == "allow")
}
fn bucket(sequence: u64) -> usize {
    match sequence % 10 {
        0..=3 => 0,
        4..=7 => 1,
        8 => 2,
        _ => 3,
    }
}
fn retryable_transport(error: &BoxError) -> bool {
    error.downcast_ref::<reqwest::Error>().is_some_and(|e| {
        e.is_timeout() || e.is_connect() || e.is_request() || e.is_body() || e.is_decode()
    })
}
fn error_chain(error: &(dyn std::error::Error + 'static)) -> Vec<String> {
    let mut values = vec![error.to_string()];
    let mut source = error.source();
    while let Some(error) = source {
        values.push(error.to_string());
        source = error.source();
    }
    values
}
fn main() -> Result<(), BoxError> {
    let args = Args::parse();
    if args.qps == 0 || args.qps > 100 || args.workers == 0 || args.workers > 64 {
        return Err("invalid rate or worker count".into());
    }
    let raw = std::fs::read(&args.fixture)?;
    let fixture: Fixture = serde_json::from_slice(&raw)?;
    let options = HttpOptions {
        timeout: Duration::from_secs(15),
        user_agent: "transparent-operator-rate-query".into(),
    };
    let mut transport = HttpShardTransport::new(&args.url, &options)?;
    let geometry = transport.geometry()?;
    if fixture.schema != geometry.schema {
        return Err("fixture schema differs from service".into());
    }
    let mut clients = BTreeMap::new();
    for f in &fixture.tables {
        let table = match f.table.as_str() {
            "directory" => Table::Directory,
            "pages" => Table::Pages,
            _ => return Err("unknown table".into()),
        };
        let key = (f.geometry.clone(), table);
        if !clients.contains_key(&key) {
            let g = geometry
                .geometries
                .iter()
                .find(|g| g.name == f.geometry)
                .ok_or("missing geometry")?;
            let (rows, width, scheme, dithered) = match table {
                Table::Directory => (
                    g.directory_rows,
                    g.directory_row_bytes,
                    &g.directory_scheme,
                    g.directory_scheme_dq44.as_ref(),
                ),
                Table::Pages => (
                    g.page_rows,
                    g.page_row_bytes,
                    &g.pages_scheme,
                    g.pages_scheme_dq44.as_ref(),
                ),
            };
            if rows != f.rows || width != f.row_bytes {
                return Err("fixture geometry mismatch".into());
            }
            clients.insert(
                key.clone(),
                TableClient::new(table, &f.geometry, rows, width, scheme, dithered)?,
            );
        }
        let client = clients.get_mut(&key).unwrap();
        if f.rows != client.rows() as u64
            || f.row_bytes != 4096
            || f.segments == 0
            || f.samples.is_empty()
            || !f.samples.iter().any(|s| s.nonempty)
        {
            return Err("invalid fixture dimensions or no occupied sample".into());
        }
        for sample in &f.samples {
            if sample.row >= client.rows()
                || sample.sha256.len() != f.segments as usize
                || sample
                    .sha256
                    .iter()
                    .any(|h| h.len() != 64 || hex::decode(h).is_err())
            {
                return Err("invalid fixture row".into());
            }
        }
        for segment in 0..f.segments {
            let (bytes, _) = transport.setup(f.shard_id, &f.revision, table, segment)?;
            let setup: Setup = serde_json::from_slice(&bytes)?;
            client.open_segment(
                &f.revision,
                segment,
                &setup.public_params,
                &setup.public_params_sha256,
            )?;
        }
    }
    let clients: BTreeMap<_, _> = clients.into_iter().map(|(k, v)| (k, Arc::new(v))).collect();
    let mut groups: Vec<Vec<Arc<Target>>> = (0..4).map(|_| Vec::new()).collect();
    for f in fixture.tables {
        let table = if f.table == "directory" {
            Table::Directory
        } else {
            Table::Pages
        };
        let index = match (f.geometry.as_str(), table) {
            ("recent-4k-8k", Table::Directory) => 0,
            ("recent-4k-8k", Table::Pages) => 1,
            ("archive-wide", Table::Directory) => 2,
            ("archive-wide", Table::Pages) => 3,
            _ => return Err("unsupported load geometry".into()),
        };
        let client = clients[&(f.geometry.clone(), table)].clone();
        groups[index].push(Arc::new(Target {
            fixture: f,
            client,
            table,
        }));
    }
    if groups.iter().any(|g| g.is_empty()) {
        return Err("all four traffic classes required".into());
    }
    emit(
        serde_json::json!({"event":"ready","unix":now(),"qps":args.qps,"workers":args.workers,"targets":groups.iter().map(Vec::len).collect::<Vec<_>>(),"fixture_sha256":hex::encode(Sha256::digest(&raw)),"http_attempts_per_call":1,"maximum_attempts_per_logical_query":3,"retry_budget":"retries consume scheduled slots; every attempt has a fresh key","mix":"40% recent directory, 40% recent pages, 10% archive directory, 10% archive pages"}),
    );
    let (tx, rx) = mpsc::sync_channel::<(u64, Instant, Arc<Target>, usize, u64, u8)>(args.workers);
    let (retry_tx, retry_rx) = mpsc::sync_channel::<(Arc<Target>, usize, u64, u8)>(args.workers);
    let (started_tx, started_rx) = mpsc::channel::<Instant>();
    let rx = Arc::new(Mutex::new(rx));
    let interval = Duration::from_secs_f64(1.0 / f64::from(args.qps));
    let send_gate = Arc::new(Mutex::new(Instant::now()));
    let mut handles = Vec::new();
    for _ in 0..args.workers {
        let rx = rx.clone();
        let url = args.url.clone();
        let options = options.clone();
        let send_gate = send_gate.clone();
        let permit = args.permit.clone();
        let retry_tx = retry_tx.clone();
        let started_tx = started_tx.clone();
        handles.push(std::thread::spawn(move || {
            let mut transport = HttpShardTransport::new(&url, &options).expect("transport already validated");
            loop {
                let Ok((sequence, due, target, sample_index, logical_id, attempt)) = rx.lock().unwrap().recv() else { break; };
                let f = &target.fixture; let sample = &f.samples[sample_index];
                let prep = Instant::now();
                let query = match target.client.prepare(&f.revision, sample.row) {
                    Ok(q) => q,
                    Err(e) => { let _ = started_tx.send(Instant::now()); emit(serde_json::json!({"event":"error","unix":now(),"sequence":sequence,"kind":"prepare","error":e.to_string()})); continue; }
                };
                let preparation = prep.elapsed().as_secs_f64();
                if let Some(wait) = due.checked_duration_since(Instant::now()) { std::thread::sleep(wait); }
                // Bound actual POST starts too: queued work must never catch up in a burst.
                {
                    let mut next = send_gate.lock().unwrap();
                    if let Some(wait) = next.checked_duration_since(Instant::now()) { std::thread::sleep(wait); }
                    *next = Instant::now() + interval;
                }
                if !allowed(&permit) {
                    let _ = started_tx.send(Instant::now());
                    emit(serde_json::json!({"event":"cancelled_slot","unix":now(),"sequence":sequence,"reason":"permit withdrawn before POST"}));
                    continue;
                }
                let started = Instant::now(); let unix = now(); let upload = query.body.len();
                let _ = started_tx.send(started);
                let response = transport.query(f.shard_id, &f.revision, target.table, &query.body);
                let http_seconds = started.elapsed().as_secs_f64();
                let retryable = response.as_ref().err().is_some_and(retryable_transport);
                let result = response.and_then(|bytes| {
                    let size = bytes.len();
                    let decoded = target.client.decode(&f.revision, f.segments, query, &bytes)?;
                    let hashes: Vec<_> = decoded.iter().map(|b| hex::encode(Sha256::digest(b))).collect();
                    if hashes != sample.sha256 { return Err("decoded row differs from independent plaintext oracle".into()); }
                    Ok(size)
                });
                match result {
                    Ok(download) => emit(serde_json::json!({"event":"query","unix":unix,"sequence":sequence,"shard":f.shard_id,"table":f.table,"geometry":f.geometry,"exact":true,"logical_id":logical_id,"attempt":attempt,"recovered":attempt>1,"nonempty":sample.nonempty,"http_seconds":http_seconds,"total_seconds":started.elapsed().as_secs_f64(),"prepare_seconds":preparation,"schedule_lag_seconds":started.saturating_duration_since(due).as_secs_f64(),"up":upload,"down":download})),
                    Err(e) => {
                        let retry_scheduled = retryable && attempt < 3 && retry_tx.try_send((target.clone(), sample_index, logical_id, attempt+1)).is_ok();
                        emit(serde_json::json!({"event":"error","unix":unix,"sequence":sequence,"logical_id":logical_id,"attempt":attempt,"retry_scheduled":retry_scheduled,"shard":f.shard_id,"table":f.table,"kind":"query_or_decode","seconds":started.elapsed().as_secs_f64(),"error":e.to_string(),"error_chain":error_chain(e.as_ref())}));
                    },
                }
            }
        }));
    }
    drop(started_tx);
    drop(retry_tx);
    drop(rx);
    let start = Instant::now();
    let mut due = Instant::now() + Duration::from_secs(1);
    let mut sequence = 0;
    let mut counters = [0usize; 4];
    while args.seconds == 0 || start.elapsed() < Duration::from_secs(args.seconds) {
        if !allowed(&args.permit) {
            emit(
                serde_json::json!({"event":"paused","unix":now(),"reason":"health permit missing, stale, or denied"}),
            );
            std::thread::sleep(Duration::from_secs(5));
            due = Instant::now() + interval;
            continue;
        }
        let dispatch = due.checked_sub(Duration::from_millis(100)).unwrap_or(due);
        if let Some(wait) = dispatch.checked_duration_since(Instant::now()) {
            std::thread::sleep(wait);
        }
        let (target, row, logical_id, attempt) = match retry_rx.try_recv() {
            Ok(retry) => retry,
            Err(_) => {
                let b = bucket(counters.iter().sum::<usize>() as u64);
                let ordinal = counters[b];
                counters[b] += 1;
                let target = groups[b][ordinal % groups[b].len()].clone();
                let row = (ordinal / groups[b].len()) % target.fixture.samples.len();
                (target, row, sequence, 1)
            }
        };
        let enqueued = match tx.try_send((sequence, due, target, row, logical_id, attempt)) {
            Ok(()) => true,
            Err(mpsc::TrySendError::Full(_)) => {
                emit(
                    serde_json::json!({"event":"missed_slot","unix":now(),"sequence":sequence,"reason":"client worker queue full"}),
                );
                false
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                return Err("all query workers exited".into())
            }
        };
        sequence += 1;
        // Advance from actual admission, not an ideal clock that drifts ahead of
        // the send gate. HTTP requests remain concurrent: only their starts are
        // paced. At most one prepared job waits for its admission acknowledgement.
        if enqueued {
            loop {
                match started_rx.recv_timeout(interval) {
                    Ok(actual) => {
                        due = actual + interval;
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => emit(
                        serde_json::json!({"event":"missed_slot","unix":now(),"sequence":sequence,"reason":"client admission delayed beyond one interval"}),
                    ),
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        return Err("query admission channel closed".into())
                    }
                }
            }
        } else {
            due = Instant::now() + interval;
        }
    }
    drop(tx);
    for handle in handles {
        handle.join().map_err(|_| "query worker panicked")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn application_errors_are_never_retried() {
        let wrong: BoxError = "decoded row differs from independent plaintext oracle".into();
        assert!(!retryable_transport(&wrong));
        let protocol: BoxError = Box::new(transparent_wallet::client::ClientError::Response(
            "bad epoch".into(),
        ));
        assert!(!retryable_transport(&protocol));
    }
    #[test]
    fn closed_connection_is_retryable() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let error: BoxError = Box::new(
            reqwest::blocking::Client::new()
                .get(format!("http://{addr}"))
                .timeout(Duration::from_secs(1))
                .send()
                .unwrap_err(),
        );
        assert!(retryable_transport(&error));
        assert!(error_chain(error.as_ref()).len() > 1);
    }
    #[test]
    fn mix_is_exact_over_every_ten_slots() {
        let mut count = [0; 4];
        for n in 0..1000 {
            count[bucket(n)] += 1;
        }
        assert_eq!(count, [400, 400, 100, 100]);
    }
    #[test]
    fn missing_or_denied_permit_stops_admission() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("permit");
        assert!(!allowed(&p));
        std::fs::write(&p, "deny").unwrap();
        assert!(!allowed(&p));
        std::fs::write(&p, "allow").unwrap();
        assert!(allowed(&p));
    }
}
