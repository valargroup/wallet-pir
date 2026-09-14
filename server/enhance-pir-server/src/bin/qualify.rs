//! Exact-answer remote-worker qualification. Use only isolated/new workers:
//! this command activates fixture generations and replaces their assignments.
use clap::Parser;
use enhance_pir::client::{record_in_row, QuerySession};
use enhance_pir_server::{
    coordinator::{CoordinatorState, TableSetup, WorkerGroup, WorkerTarget},
    store::RecordJournal,
    types::{DatabaseId, EnhanceRecord, EnhanceRecordParts, ENHANCE_LAYOUT, SHARD_POSITIONS},
};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

#[derive(Parser)]
struct Cli {
    #[arg(long, required = true)]
    worker_url: Vec<String>,
    #[arg(long, default_value_t = 1)]
    shards: u64,
    #[arg(long, default_value_t = 10)]
    seconds: u64,
    #[arg(long, default_value_t = 1)]
    min_publications: u64,
    #[arg(long)]
    work_dir: PathBuf,
    #[arg(long)]
    output: PathBuf,
    /// Required acknowledgement: target workers must not serve production.
    #[arg(long)]
    isolated_workers: bool,
}

fn record(position: u64) -> EnhanceRecord {
    let mut key = [0u8; 32];
    key[..8].copy_from_slice(&position.to_le_bytes());
    EnhanceRecord::from_parts(EnhanceRecordParts {
        ephemeral_key: key,
        enc_ciphertext: [17; 580],
        cv_net: [23; 32],
        out_ciphertext: [41; 80],
        has_transparent_inputs: false,
        has_transparent_outputs: false,
    })
}

async fn query(state: &CoordinatorState, position: u64) -> Result<(), String> {
    let session = QuerySession::from_session(state.session().ok_or("no session")?)
        .map_err(|e| e.to_string())?;
    let (query, slot) = session
        .prepare_position(position)
        .map_err(|e| e.to_string())?;
    let response = state
        .answer_query(DatabaseId::Enhance, query.body())
        .await?;
    let row = session
        .decode(query, &response)
        .map_err(|e| e.to_string())?;
    if record_in_row(&row, slot) != record(position) {
        return Err("incorrect PIR answer".into());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if !cli.isolated_workers
        || !(1..=16).contains(&cli.shards)
        || cli.worker_url.len() > 2
        || cli.min_publications == 0
        || cli.min_publications >= cli.shards * SHARD_POSITIONS as u64
        || cli.seconds == 0
    {
        return Err("requires --isolated-workers, 1..16 shards and one or two workers".into());
    }
    if cli.work_dir.exists() {
        return Err("qualification work directory must be new".into());
    }
    std::fs::create_dir_all(&cli.work_dir)?;
    let state = CoordinatorState::new(vec![TableSetup {
        table: DatabaseId::Enhance,
        groups: vec![WorkerGroup {
            name: "qualification".into(),
            replicas: cli
                .worker_url
                .iter()
                .enumerate()
                .map(|(i, url)| WorkerTarget::Remote {
                    name: format!("qualify-{i}"),
                    base_url: url.clone(),
                })
                .collect(),
        }],
    }])?;
    let mut store = RecordJournal::open(&cli.work_dir, DatabaseId::Enhance, ENHANCE_LAYOUT)?;
    let count = cli.shards * SHARD_POSITIONS as u64 - cli.min_publications.max(1);
    let mut height = 3_428_143;
    let records: Vec<_> = (0..count).map(record).collect();
    store.append_block(height, format!("{height:064x}"), &records)?;
    drop(records);
    let started = Instant::now();
    state
        .publish_from_store(&store, height, format!("{height:064x}"))
        .await?;
    let serving_started = Instant::now();
    let mut publications = 1;
    let mut queries = 0u64;
    let mut durations = Vec::new();
    let mut position_count = count;
    let mut last_publish = Instant::now();
    let interval = Duration::from_secs((cli.seconds / cli.min_publications.max(1)).max(1));
    while serving_started.elapsed().as_secs() < cli.seconds || publications < cli.min_publications {
        let before = Instant::now();
        let a = queries.wrapping_mul(7919) % position_count;
        let b = position_count - 1;
        let (first, last) = tokio::join!(query(&state, a), query(&state, b));
        first?;
        last?;
        queries += 2;
        durations.push(before.elapsed().as_secs_f64());
        if last_publish.elapsed() >= interval
            && position_count < cli.shards * SHARD_POSITIONS as u64
        {
            height += 1;
            store.append_block(height, format!("{height:064x}"), &[record(position_count)])?;
            position_count += 1;
            state
                .publish_from_store(&store, height, format!("{height:064x}"))
                .await?;
            publications += 1;
            last_publish = Instant::now();
        }
    }
    durations.sort_by(f64::total_cmp);
    let p99 = durations
        .get(durations.len().saturating_sub(1) * 99 / 100)
        .copied()
        .unwrap_or(0.0);
    let report = serde_json::json!({"passed": p99 <= 5.0, "shards": cli.shards, "queries": queries,
        "mismatches": 0, "publications": publications, "seconds": started.elapsed().as_secs_f64(),
        "serving_seconds": serving_started.elapsed().as_secs_f64(),
        "paired_client_p99_seconds": p99, "scope": "remote exact-answer fixture; memory, failover and online-append gates require separate evidence"});
    std::fs::write(&cli.output, serde_json::to_vec_pretty(&report)?)?;
    if p99 > 5.0 {
        return Err("qualification p99 exceeded five seconds".into());
    }
    println!("{report}");
    Ok(())
}
