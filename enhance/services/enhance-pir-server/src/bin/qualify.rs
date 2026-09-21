//! Exact-answer remote-worker qualification. Use only isolated/new workers:
//! this command activates fixture generations and replaces their assignments.
use clap::Parser;
use enhance_pir::client::{record_in_row, QuerySession};
use enhance_pir_server::{
    coordinator::{CoordinatorState, TableSetup, WorkerGroup, WorkerTarget},
    store::RecordJournal,
    topology::{Append, Group, Replica, TopologyStore},
    types::{
        DatabaseId, EnhanceRecord, EnhanceRecordParts, ENHANCE_LAYOUT, SHARDS_PER_GROUP,
        SHARD_POSITIONS,
    },
};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Parser)]
struct Cli {
    #[arg(long, required = true)]
    worker_url: Vec<String>,
    /// Optional isolated second pair for a group-boundary online append rehearsal.
    #[arg(long)]
    append_worker_url: Vec<String>,
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
        metadata: enhance_pir::EnhanceTransactionMetadata::new(0, Some(0)).unwrap(),
    })
}

async fn query(
    state: &CoordinatorState,
    session: &QuerySession,
    position: u64,
) -> Result<(), String> {
    let (query, slot) = session
        .prepare_position(position)
        .map_err(|e| e.to_string())?;
    let response = state
        .answer_query(DatabaseId::Enhance, query.body())
        .await?;
    let row = session
        .decode(query, &response)
        .map_err(|e| e.to_string())?;
    if record_in_row(&row, slot).map_err(|e| e.to_string())? != record(position) {
        return Err("incorrect PIR answer".into());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    if !cli.isolated_workers
        || !(1..=SHARDS_PER_GROUP).contains(&cli.shards)
        || cli.worker_url.len() > 2
        || cli.min_publications == 0
        || cli.min_publications >= cli.shards * SHARD_POSITIONS as u64
        || cli.seconds == 0
    {
        return Err(format!(
            "requires --isolated-workers, 1..={SHARDS_PER_GROUP} shards and one or two workers"
        )
        .into());
    }
    if !cli.append_worker_url.is_empty()
        && (cli.shards != SHARDS_PER_GROUP
            || cli.worker_url.len() != 2
            || cli.append_worker_url.len() != 2)
    {
        return Err(format!(
            "online append requires {SHARDS_PER_GROUP} shards and two distinct worker pairs"
        )
        .into());
    }
    if cli.work_dir.exists() {
        return Err("qualification work directory must be new".into());
    }
    std::fs::create_dir_all(&cli.work_dir)?;
    let mut state = CoordinatorState::new(vec![TableSetup {
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
    let groups: Vec<_> = [&cli.worker_url, &cli.append_worker_url]
        .into_iter()
        .enumerate()
        .map(|(group, urls)| Group {
            name: format!("qualification-{group}"),
            replicas: urls
                .iter()
                .enumerate()
                .map(|(replica, url)| Replica {
                    name: format!("qualify-{group}-{replica}"),
                    url: url.clone(),
                })
                .collect(),
        })
        .collect();
    let topology = if cli.append_worker_url.is_empty() {
        None
    } else {
        enhance_pir_server::topology::validate(&groups)?;
        let topology = Arc::new(TopologyStore::open(
            cli.work_dir.join("topology.json"),
            vec![groups[0].clone()],
        )?);
        state = CoordinatorState::new(vec![TableSetup {
            table: DatabaseId::Enhance,
            groups: vec![groups[0].target()],
        }])?
        .with_topology(topology.clone());
        Some(topology)
    };
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
    let cold_prepare_seconds = started.elapsed().as_secs_f64();
    let serving_started = Instant::now();
    let mut retained = std::collections::VecDeque::from([(
        QuerySession::from_session(state.session().ok_or("no session")?)?,
        count,
    )]);
    let mut max_publication_seconds = 0.0f64;
    let mut publications = 1;
    let mut queries = 0u64;
    let mut durations = Vec::new();
    let mut position_count = count;
    let mut last_publish = Instant::now();
    let interval = Duration::from_secs((cli.seconds / cli.min_publications.max(1)).max(1));
    while serving_started.elapsed().as_secs() < cli.seconds || publications < cli.min_publications {
        let before = Instant::now();
        let a = if queries.is_multiple_of(4) {
            position_count - 1
        } else {
            queries.wrapping_mul(7919) % position_count
        };
        let (old_session, old_count) = retained.front().ok_or("no retained session")?;
        let (current_session, _) = retained.back().ok_or("no current session")?;
        let (first, last) = tokio::join!(
            query(&state, current_session, a),
            query(&state, old_session, old_count - 1)
        );
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
            let publication_started = Instant::now();
            state
                .publish_from_store(&store, height, format!("{height:064x}"))
                .await?;
            let publication_seconds = publication_started.elapsed().as_secs_f64();
            max_publication_seconds = max_publication_seconds.max(publication_seconds);
            retained.push_back((
                QuerySession::from_session(state.session().ok_or("no session")?)?,
                position_count,
            ));
            while retained.len() > enhance_pir_server::worker::RETAINED_GENERATIONS {
                retained.pop_front();
            }
            publications += 1;
            println!(
                "{}",
                serde_json::json!({"event": "publication", "publications": publications,
                "queries": queries, "serving_seconds": serving_started.elapsed().as_secs_f64(),
                "publication_seconds": publication_seconds})
            );
            last_publish = Instant::now();
        }
    }
    let mut online_append = None;
    if let Some(topology) = topology {
        // Fill the last row before crossing both ownership and query-domain boundaries.
        let boundary = SHARDS_PER_GROUP * SHARD_POSITIONS as u64;
        if position_count < boundary {
            height += 1;
            let tail: Vec<_> = (position_count..boundary).map(record).collect();
            store.append_block(height, format!("{height:064x}"), &tail)?;
            state
                .publish_from_store(&store, height, format!("{height:064x}"))
                .await?;
            publications += 1;
        }
        let old = QuerySession::from_session(state.session().ok_or("no boundary session")?)?;
        topology.append(Append {
            operation_id: "qualification-append".into(),
            expected_revision: 0,
            groups,
        })?;
        height += 1;
        store.append_block(height, format!("{height:064x}"), &[record(boundary)])?;
        let append_started = Instant::now();
        let publication = state.publish_from_store(&store, height, format!("{height:064x}"));
        tokio::pin!(publication);
        let mut during_append_queries = 0;
        loop {
            let before = Instant::now();
            tokio::select! {
                result = &mut publication => { result?; break; }
                result = query(&state, &old, boundary - 1) => {
                    result?;
                    durations.push(before.elapsed().as_secs_f64());
                    queries += 1;
                    during_append_queries += 1;
                }
            }
        }
        publications += 1;
        let current = QuerySession::from_session(state.session().ok_or("no expanded session")?)?;
        query(&state, &old, boundary - 1).await?;
        query(&state, &current, boundary - 1).await?;
        query(&state, &current, boundary).await?;
        queries += 3;
        if topology.status().revision != 1 || during_append_queries == 0 {
            return Err(
                "online append did not commit with concurrent retained-session queries".into(),
            );
        }
        online_append = Some(serde_json::json!({
            "passed": true, "from_shards": 16, "to_shards": 17,
            "seconds": append_started.elapsed().as_secs_f64(),
            "during_append_queries": during_append_queries,
            "old_and_new_session_answers_verified": true,
        }));
    }
    durations.sort_by(f64::total_cmp);
    let p99 = durations
        .get(durations.len().saturating_sub(1) * 99 / 100)
        .copied()
        .unwrap_or(0.0);
    let report = serde_json::json!({"passed": p99 <= 5.0, "shards": cli.shards, "queries": queries,
        "mismatches": 0, "online_append": online_append, "publications": publications, "seconds": started.elapsed().as_secs_f64(),
        "serving_seconds": serving_started.elapsed().as_secs_f64(),
        "paired_client_p99_seconds": p99, "cold_prepare_seconds": cold_prepare_seconds,
        "max_publication_seconds": max_publication_seconds, "retained_session_queries": true, "scope": "remote exact-answer fixture; memory, failover, process uptime and cloud lifecycle require separate evidence"});
    std::fs::write(&cli.output, serde_json::to_vec_pretty(&report)?)?;
    if p99 > 5.0 {
        return Err("qualification p99 exceeded five seconds".into());
    }
    println!("{report}");
    Ok(())
}
