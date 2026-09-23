//! Synthetic off-worker workloads. Results are evidence, never qualification receipts.
use super::{
    control::{Group, Role, State},
    coordinator::Coordinator,
};
use crate::{
    store::RecordJournal,
    types::{DatabaseId, ENHANCE_LAYOUT},
};
use enhance_pir::{
    v4::{Manifest, RETAINED_GENERATIONS},
    v4_client::EnhancePirClient,
    RECORDS_PER_ROW, RECORD_BYTES,
};
use hdrhistogram::Histogram;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, VecDeque},
    fs::File,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const SPAN: u64 = 32768 * RECORDS_PER_ROW as u64;
const LOAN: u64 = 4096 * RECORDS_PER_ROW as u64;

#[derive(Clone, Copy, Debug, clap::ValueEnum, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    Smoke,
    Active,
    Sealed,
}
impl Profile {
    fn seeds(self) -> Vec<u64> {
        match self {
            Self::Smoke => vec![67],
            Self::Active => vec![4 * SPAN - RECORDS_PER_ROW as u64, 4 * SPAN],
            Self::Sealed => vec![5 * SPAN + LOAN, 6 * SPAN + LOAN],
        }
    }
    fn target(self, index: u64, records: u64) -> (u64, &'static str) {
        match self {
            Self::Smoke => (records + RECORDS_PER_ROW as u64, "append"),
            Self::Sealed => (
                records + RECORDS_PER_ROW as u64,
                "sealed_with_external_append",
            ),
            Self::Active => {
                let (offset, stage) = [
                    (LOAN / 2, "loan_growth"),
                    (LOAN, "return"),
                    (2 * LOAN, "owned_8k"),
                    (4 * LOAN, "owned_16k"),
                    (SPAN - RECORDS_PER_ROW as u64, "owned_near_32k"),
                    (0, "rewind_return"),
                ][index as usize % 6];
                (4 * SPAN + offset, stage)
            }
        }
    }
}

pub struct Config {
    pub profile: Profile,
    pub root: PathBuf,
    pub listen: SocketAddr,
    pub seconds: u64,
    pub min_publications: u64,
    pub interval: Duration,
    pub concurrency: usize,
}

fn now_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
// Match the observer's digest of the raw Linux machine-id file, including its
// trailing newline. Unsupported hosts remain explicitly unidentified.
fn host_id(root: &Path) -> Option<String> {
    std::fs::read(root.join("etc/machine-id"))
        .ok()
        .filter(|bytes| {
            let value = bytes.strip_suffix(b"\n").unwrap_or(bytes);
            value.len() == 32 && value.iter().all(u8::is_ascii_hexdigit)
        })
        .map(|bytes| hex::encode(Sha256::digest(bytes)))
}
fn record(position: u64) -> Vec<u8> {
    let mut bytes = vec![0; RECORD_BYTES];
    bytes[..8].copy_from_slice(&position.to_le_bytes());
    bytes
}
fn append(journal: &mut RecordJournal, target: u64, branch: u64) -> Result<()> {
    while journal.tree_size() < target {
        let start = journal.tree_size();
        let end = (start + 8192).min(target);
        let rows: Vec<_> = (start..end).map(record).collect();
        let height = journal
            .last_block()
            .map_or(enhance_pir::ACTIVATION_HEIGHT, |b| b.height + 1);
        journal.append_block(height, format!("{branch:032x}{height:032x}"), &rows)?;
    }
    Ok(())
}
fn report(root: &Path, value: &Value) -> Result<()> {
    crate::artifact::write_atomic(root, "exercise.json", |f| {
        serde_json::to_writer_pretty(f, value).map_err(std::io::Error::other)
    })?;
    File::open(root)?.sync_all()?;
    Ok(())
}
async fn publish(c: &Coordinator, journal: &RecordJournal) -> Result<Manifest> {
    let last = journal.last_block().ok_or("empty workload journal")?;
    c.publish(journal, last.height, last.hash.clone()).await?;
    c.manifest()
        .await
        .ok_or_else(|| "publication produced no manifest".into())
}
async fn exact(
    client: &mut EnhancePirClient,
    position: u64,
    queries: &tokio::sync::Semaphore,
) -> Result<()> {
    let _permit = queries.acquire().await?;
    let (answer, _) = client.query_position_with_timing(position).await?;
    if answer.as_ref() != record(position) {
        return Err("incorrect workload PIR answer".into());
    }
    Ok(())
}
fn probe_positions(coverage: &enhance_pir::v4::Coverage) -> BTreeSet<u64> {
    let mut positions = BTreeSet::new();
    for shard in &coverage.shards {
        let first = shard.global_row_start * RECORDS_PER_ROW as u64;
        positions.insert(first);
        positions.insert(first + shard.records - 1);
        for unit in &shard.units {
            let offset = unit.local_row_start * RECORDS_PER_ROW as u64;
            if offset < shard.records {
                positions.insert(first + offset);
            }
            if offset > 0 && offset <= shard.records {
                positions.insert(first + offset - 1);
            }
        }
    }
    if let Some(loan) = &coverage.loan {
        let owned_start = loan.row_end * RECORDS_PER_ROW as u64;
        positions.insert(owned_start - 1);
        if owned_start < coverage.records {
            positions.insert(owned_start);
        }
    }
    positions
}
fn assignments(root: &Path, manifest: &Manifest, profile: Profile) -> Result<Value> {
    let state: State =
        serde_json::from_slice(&std::fs::read(root.join("control/controller-v4.json"))?)?;
    let mut groups = Vec::new();
    for group in &state.groups {
        let role = group.role(&manifest.coverage, &state.assignments)?;
        let count = state
            .assignments
            .values()
            .filter(|id| *id == &group.id)
            .count();
        groups.push(json!({"id":group.id,"role":role,"shards":count}));
    }
    let first = &state.groups[0];
    let count = state
        .assignments
        .values()
        .filter(|id| *id == &first.id)
        .count();
    let role = first.role(&manifest.coverage, &state.assignments)?;
    if (profile == Profile::Active && (count != 5 || role != Role::Active))
        || (profile == Profile::Sealed && (count != 6 || role != Role::SealedFull))
    {
        return Err("published assignment does not match requested capacity profile".into());
    }
    Ok(
        json!({"groups":groups,"retained_generations":state.published.iter().map(|m|m.generation).collect::<Vec<_>>(),
              "draining_operations":state.draining.len(),"placement_revision":state.revision}),
    )
}

struct HttpTask(tokio::task::JoinHandle<()>);
impl Drop for HttpTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct Traffic {
    latencies: Histogram<u64>,
    correct: u64,
    errors: u64,
    reselected: u64,
}

pub async fn run(config: Config, groups: Vec<Group>) -> Result<()> {
    if config.seconds == 0
        || config.seconds > 86400
        || config.min_publications == 0
        || config.min_publications > 10000
        || config.interval.is_zero()
        || config.concurrency == 0
        || config.concurrency > 16
        || groups.len()
            != if config.profile == Profile::Sealed {
                2
            } else {
                1
            }
    {
        return Err(
            "invalid isolated workload duration, concurrency or replica-group count".into(),
        );
    }
    // A new directory and fresh replicas are mandatory; never adopt serving state.
    std::fs::create_dir(&config.root)?;
    std::fs::write(config.root.join("v4-source-mode"), "synthetic-fixture")?;
    let mut summary = json!({"kind":"enhance-v4-workload","qualification":"unqualified","status":"building",
        "profile":config.profile,"seconds_requested":config.seconds,"min_publications":config.min_publications,
        "workload_host_id_sha256":host_id(Path::new("/")),
        "concurrency":config.concurrency,"started_wall_ns":now_ns(),"publications":0,
        "binary_sha256":hex::encode(Sha256::digest(std::fs::read(std::env::current_exe()?)?))});
    report(&config.root, &summary)?;
    let result = execute(&config, groups, &mut summary).await;
    if let Err(error) = &result {
        summary["status"] = json!("failed");
        summary["error"] = json!(error.to_string());
    }
    summary["finished_wall_ns"] = json!(now_ns());
    report(&config.root, &summary)?;
    result
}

async fn execute(config: &Config, groups: Vec<Group>, summary: &mut Value) -> Result<()> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let mut identities = BTreeSet::new();
    for replica in groups.iter().flat_map(|g| &g.replicas) {
        let health: Value = http
            .get(format!("{}/internal/v4/health", replica.url))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let incarnation = health["incarnation"]
            .as_str()
            .ok_or("missing worker identity")?;
        if health["protocol"] != enhance_pir::v4::PROTOCOL_REVISION
            || health["epoch"] != 0
            || health["revision"] != 0
            || health["candidate"] != Value::Null
            || health["published"] != json!([])
            || !identities.insert(incarnation.to_owned())
        {
            return Err("workload requires distinct fresh idle v4 workers".into());
        }
    }
    let worker_targets: Vec<_> = groups
        .iter()
        .flat_map(|g| {
            g.replicas
                .iter()
                .map(|r| (g.id.clone(), r.name.clone(), r.url.clone()))
        })
        .collect();
    let coordinator = Coordinator::open(&config.root.join("control"), groups)?;
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    let address = listener.local_addr()?;
    let connect_address = if address.ip().is_unspecified() {
        SocketAddr::new(
            if address.is_ipv4() {
                std::net::Ipv4Addr::LOCALHOST.into()
            } else {
                std::net::Ipv6Addr::LOCALHOST.into()
            },
            address.port(),
        )
    } else {
        address
    };
    let origin = format!("http://{connect_address}");
    summary["origin"] = json!(origin);
    let router = coordinator.clone().router();
    let _server = HttpTask(tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    }));
    let mut journal = RecordJournal::open(
        config.root.join("journal"),
        DatabaseId::Enhance,
        ENHANCE_LAYOUT,
    )?;
    let mut manifest = None;
    for target in config.profile.seeds() {
        append(&mut journal, target, 0)?;
        manifest = Some(publish(&coordinator, &journal).await?);
    }
    let initial = manifest.unwrap();
    assignments(&config.root, &initial, config.profile)?;
    let rewind_height = journal.last_block().unwrap().height;
    let initial_client = EnhancePirClient::connect(&origin).await?;
    let stop = Arc::new(AtomicBool::new(false));
    // Boundary/retention probes share the declared concurrency budget with load.
    let queries = Arc::new(tokio::sync::Semaphore::new(config.concurrency));
    let mut traffic = tokio::task::JoinSet::new();
    for index in 0..config.concurrency {
        let origin = origin.clone();
        let stop = stop.clone();
        let queries = queries.clone();
        traffic.spawn(async move {
            let mut client = EnhancePirClient::connect(&origin).await?;
            let mut sample = Traffic {
                latencies: Histogram::new(3)?,
                correct: 0,
                errors: 0,
                reselected: 0,
            };
            let mut sequence = index as u64;
            while !stop.load(Ordering::Relaxed) {
                let mut position =
                    sequence.wrapping_mul(104729) % client.manifest().coverage.records;
                let requested_generation = client.manifest().generation;
                sequence = sequence.wrapping_add(1);
                let began = Instant::now();
                let permit = queries.acquire().await?;
                let mut result = client.query_position_with_timing(position).await;
                // A retained snapshot may expire after rollback removed its tail.
                // Reselect only when refresh actually changed the generation and
                // the original target is now outside the new canonical coverage.
                if matches!(
                    &result,
                    Err(enhance_pir::client::ClientError::OutsideCoverage(_))
                ) && client.manifest().generation != requested_generation
                    && position >= client.manifest().coverage.records
                    && client.manifest().coverage.records > 0
                {
                    position %= client.manifest().coverage.records;
                    sample.reselected += 1;
                    result = client.query_position_with_timing(position).await;
                }
                let mut failed = false;
                match result {
                    Ok((answer, _)) if answer.as_ref() == record(position) => sample.correct += 1,
                    Ok(_) => return Err("incorrect background PIR answer".into()),
                    Err(_) => {
                        sample.errors += 1;
                        failed = true;
                    }
                }
                drop(permit);
                sample
                    .latencies
                    .record(began.elapsed().as_micros().max(1) as u64)?;
                if failed {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(sample)
        });
    }
    let mut retained = VecDeque::from([initial_client]);
    let mut trace = File::create(config.root.join("publications.jsonl"))?;
    let started = Instant::now();
    summary["status"] = json!("running");
    summary["measurement_started_wall_ns"] = json!(now_ns());
    report(&config.root, summary)?;
    let mut publications = 0;
    let mut branch = 0;
    let mut probes = 0;
    let mut expired_refreshes = 0;
    let mut max_publication_ms = 0u128;
    while started.elapsed() < Duration::from_secs(config.seconds)
        || publications < config.min_publications
    {
        if let Some(done) = traffic.try_join_next() {
            done??;
            return Err("background traffic ended early".into());
        }
        let began = Instant::now();
        let (target, stage) = config.profile.target(publications, journal.tree_size());
        if target < journal.tree_size() {
            journal.rewind_to_height(Some(rewind_height))?;
            branch += 1;
        }
        append(&mut journal, target, branch)?;
        let manifest = publish(&coordinator, &journal).await?;
        let publication_ms = began.elapsed().as_millis();
        max_publication_ms = max_publication_ms.max(publication_ms);
        let mut placement = assignments(&config.root, &manifest, config.profile)?;
        let health: Value = http
            .get(format!("{origin}/v1/health"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let replicas = health["published_replica_counts"]
            .as_object()
            .ok_or("missing published readiness")?;
        if replicas.is_empty() || replicas.values().any(|count| count.as_u64() != Some(2)) {
            return Err("capacity workload requires both published-ready replicas".into());
        }
        placement["published_replica_counts"] = json!(replicas);
        let mut client = EnhancePirClient::connect(&origin).await?;
        for position in probe_positions(&manifest.coverage) {
            exact(&mut client, position, &queries).await?;
            probes += 1;
        }
        retained.push_back(client);
        if retained.len() > RETAINED_GENERATIONS {
            let mut expired = retained.pop_front().unwrap();
            exact(&mut expired, 0, &queries).await?;
            if expired.manifest().generation != manifest.generation {
                return Err("expired session did not refresh".into());
            }
            expired_refreshes += 1;
        }
        for client in &mut retained {
            let generation = client.manifest().generation;
            let last = client.manifest().coverage.records - 1;
            exact(client, last, &queries).await?;
            if client.manifest().generation != generation {
                return Err("retained session refreshed prematurely".into());
            }
            probes += 1;
        }
        publications += 1;
        writeln!(
            trace,
            "{}",
            json!({"wall_time_ns":now_ns(),"elapsed_seconds":started.elapsed().as_secs_f64(),
            "stage":stage,"branch":branch,"records":target,"manifest":manifest,"placement":placement,"publication_ms":publication_ms})
        )?;
        trace.sync_all()?;
        summary["publications"] = json!(publications);
        summary["exact_boundary_and_retained_probes"] = json!(probes);
        summary["expired_refreshes"] = json!(expired_refreshes);
        summary["max_fixture_publication_ms"] = json!(max_publication_ms);
        report(&config.root, summary)?;
        tokio::time::sleep(config.interval.saturating_sub(began.elapsed())).await;
    }
    stop.store(true, Ordering::Relaxed);
    let mut histogram = Histogram::<u64>::new(3)?;
    let (mut correct, mut errors, mut reselected) = (0, 0, 0);
    while let Some(done) = traffic.join_next().await {
        let sample = done??;
        histogram.add(sample.latencies)?;
        correct += sample.correct;
        errors += sample.errors;
        reselected += sample.reselected;
    }
    summary["measurement_seconds"] = json!(started.elapsed().as_secs_f64());
    summary["background_correct_answers"] = json!(correct);
    summary["background_errors"] = json!(errors);
    summary["expired_reorg_target_reselections"] = json!(reselected);
    summary["publications_sha256"] = json!(hex::encode(Sha256::digest(std::fs::read(
        config.root.join("publications.jsonl")
    )?)));
    summary["background_p99_ms"] = json!(histogram.value_at_quantile(0.99) as f64 / 1000.);
    summary["six_hour_publication_gate"] = json!(
        config.profile != Profile::Smoke
            && started.elapsed().as_secs() >= 21600
            && publications >= 300
    );
    // Final snapshots complement, but cannot replace, time-series host sampling.
    let metrics_root = config.root.join("metrics");
    std::fs::create_dir(&metrics_root)?;
    let mut targets = vec![(
        "coordinator.prom".to_owned(),
        json!({"role":"coordinator"}),
        format!("{origin}/metrics"),
    )];
    targets.extend(
        worker_targets
            .into_iter()
            .enumerate()
            .map(|(index, (group, replica, url))| {
                (
                    format!("worker-{index}.prom"),
                    json!({"role":"worker","group":group,"replica":replica}),
                    format!("{url}/internal/v4/metrics"),
                )
            }),
    );
    let mut scrapes = Vec::new();
    for (name, target, url) in targets {
        let text = http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        crate::artifact::write_atomic(&metrics_root, &name, |f| f.write_all(text.as_bytes()))?;
        scrapes.push(json!({"file":name,"target":target,"sha256":hex::encode(Sha256::digest(text.as_bytes()))}));
    }
    File::open(metrics_root)?.sync_all()?;
    summary["final_metrics"] = json!(scrapes);
    summary["status"] = json!("recorded");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v4::control::{assign, consolidate};
    use enhance_pir::v4::{Geometry, Lifecycle};
    use std::collections::BTreeMap;

    #[test]
    fn host_identity_matches_raw_linux_sampler_digest() {
        let root = tempfile::tempdir().unwrap();
        assert_eq!(host_id(root.path()), None);
        std::fs::create_dir(root.path().join("etc")).unwrap();
        let path = root.path().join("etc/machine-id");
        std::fs::write(&path, b"not-an-identity\n").unwrap();
        assert_eq!(host_id(root.path()), None);
        let bytes = b"0123456789abcdef0123456789abcdef\n";
        std::fs::write(path, bytes).unwrap();
        assert_eq!(
            host_id(root.path()),
            Some(hex::encode(Sha256::digest(bytes)))
        );
    }

    #[test]
    fn full_profiles_preserve_assignment_limits_across_cycles() {
        for profile in [Profile::Active, Profile::Sealed] {
            let groups: Vec<_> = (0..if profile == Profile::Sealed { 2 } else { 1 })
                .map(|i| Group {
                    id: format!("g{i}"),
                    sequence: i,
                    replicas: vec![],
                    settling: false,
                })
                .collect();
            let mut lifecycle = Lifecycle::default();
            let mut placement = BTreeMap::new();
            let mut records = 0;
            for target in profile.seeds() {
                records = target;
                let coverage = lifecycle.coverage(records, Geometry::default()).unwrap();
                placement = assign(&coverage, &groups, &placement).unwrap();
                consolidate(&coverage, &groups, &mut placement).unwrap();
            }
            for index in 0..24 {
                records = profile.target(index, records).0;
                let coverage = lifecycle.coverage(records, Geometry::default()).unwrap();
                placement = assign(&coverage, &groups, &placement).unwrap();
                consolidate(&coverage, &groups, &mut placement).unwrap();
                assert_eq!(
                    placement.values().filter(|g| **g == groups[0].id).count(),
                    if profile == Profile::Active { 5 } else { 6 }
                );
                assert_eq!(
                    groups[0].role(&coverage, &placement).unwrap(),
                    if profile == Profile::Active {
                        Role::Active
                    } else {
                        Role::SealedFull
                    }
                );
            }
        }
    }

    #[test]
    fn exact_probes_include_borrowed_owned_and_unit_boundaries() {
        let coverage = Lifecycle::default()
            .coverage(4 * SPAN + LOAN / 2 + 1, Geometry::default())
            .unwrap();
        let positions = probe_positions(&coverage);
        assert!(positions.contains(&(4 * SPAN)));
        assert!(positions
            .contains(&(coverage.loan.as_ref().unwrap().row_start * RECORDS_PER_ROW as u64)));
        assert!(positions.contains(&(coverage.records - 1)));
        for position in positions {
            assert!(coverage.locate(position).is_some());
        }
    }
}
