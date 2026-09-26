//! Isolated synthetic backend. Uses a declared synthetic source; never enables wallet traffic.
use clap::{Parser, Subcommand};
use enhance_pir::status::*;
use enhance_pir_server::{
    control::{Group, Ledger, Replica},
    coordinator::Coordinator,
    status::{self, fixture, Controller, Generation},
    zakura::ZakuraClient,
};
use ipir_sp::server::MatvecBackend;
use serde_json::json;
use std::{
    fs::File,
    io::{BufWriter, Write},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::net::TcpListener;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Open-loop live mined-answer smoke load; publication qualification is assessed separately.
    ProbeLiveLoad {
        #[arg(long)]
        origin: String,
        /// Send encrypted queries directly here; initialization still uses origin.
        #[arg(long)]
        query_origin: Option<String>,
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        rpc_url: String,
        #[arg(long)]
        cookie: PathBuf,
        #[arg(long, default_value_t = 60)]
        seconds: u64,
        #[arg(long)]
        report_dir: PathBuf,
    },
    /// Qualification controller using the same publisher integrated into Enhance.
    ServeDistributed {
        #[arg(long)]
        config: PathBuf,
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        rpc_url: String,
        #[arg(long)]
        cookie: PathBuf,
        /// Loopback listener for the public `/v1/status/*` routes that the
        /// HTTPS ingress proxies to. Required when the config enables public
        /// routes; refused otherwise.
        #[arg(long)]
        public_listen: Option<SocketAddr>,
    },
    /// Encrypted oracle validation across real router and worker child processes.
    ValidateDistributed {
        #[arg(long, default_value_t = 32)]
        entries: usize,
        #[arg(long)]
        cuda: bool,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Independent private role. Loopback endpoints require authenticated SSH forwarding.
    Role {
        #[arg(long, value_enum)]
        role: status::distributed::Role,
        #[arg(long)]
        network_hex: String,
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        listen: SocketAddr,
        /// Router only: serve `/v1/status/query` on this separate loopback
        /// listener and keep control, artifact, public, evaluate and telemetry
        /// on `--listen`. Omitted: one merged listener.
        #[arg(long)]
        query_listen: Option<SocketAddr>,
        #[arg(long)]
        artifact_origin: String,
        #[arg(long, default_value = "http://127.0.0.1:8381")]
        worker_origin: String,
        #[arg(long)]
        cuda: bool,
    },
    /// Read a consistent live block/mempool window without publishing it.
    ObserveLive {
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        rpc_url: String,
        #[arg(long)]
        cookie: PathBuf,
        /// Public index salt, exactly 32 bytes of hex.
        #[arg(long)]
        salt_hex: String,
        #[arg(long, default_value_t = 64)]
        window_blocks: u32,
        #[arg(long, default_value_t = 1)]
        samples: usize,
        #[arg(long, default_value_t = 1000)]
        interval_ms: u64,
        /// Persist canonical block and fork cache; every restart still reobserves the source.
        #[arg(long)]
        state_dir: Option<PathBuf>,
    },
    /// Run the synthetic backend on loopback; use an SSH tunnel for remote probes.
    Serve {
        #[arg(long,default_value_t=MAX_ENTRIES)]
        entries: usize,
        #[arg(long)]
        cuda: bool,
        #[arg(long, default_value = "127.0.0.1:8380")]
        listen: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8381")]
        worker: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8382")]
        router: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8383")]
        monitoring: SocketAddr,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Experimental live Status serving on loopback. Roles still share this process.
    ServeLive {
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        rpc_url: String,
        #[arg(long)]
        cookie: PathBuf,
        #[arg(long)]
        salt_hex: String,
        #[arg(long, default_value_t = 64)]
        window_blocks: u32,
        #[arg(long)]
        cuda: bool,
        #[arg(long, default_value = "127.0.0.1:8380")]
        listen: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8381")]
        worker: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8382")]
        router: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8383")]
        monitoring: SocketAddr,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Full encrypted HTTP correctness, update/rebuild differential, and failure checks.
    Validate {
        #[arg(long,default_value_t=MAX_ENTRIES)]
        entries: usize,
        #[arg(long)]
        cuda: bool,
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Probe or load an already running synthetic backend, validating all answers.
    Probe {
        #[arg(long, default_value = "http://127.0.0.1:8380")]
        origin: String,
        #[arg(long,default_value_t=MAX_ENTRIES)]
        entries: usize,
        #[arg(long, default_value_t = 4)]
        queries: usize,
        #[arg(long, default_value_t = 1)]
        concurrency: usize,
        #[arg(long, default_value_t = 0)]
        qps: u64,
        /// Schedule qps * seconds arrivals; intended for long open-loop runs.
        #[arg(long)]
        seconds: Option<u64>,
        /// Optional per-arrival JSONL evidence. Contains no txids or query material.
        #[arg(long)]
        report_jsonl: Option<PathBuf>,
        /// Fail the probe if complete-lookup p99 exceeds this bound.
        #[arg(long)]
        max_p99_ms: Option<f64>,
    },
    /// Check a canonical mined txid through a live encrypted query on loopback.
    ProbeLive {
        #[arg(long, default_value = "http://127.0.0.1:8380")]
        origin: String,
        #[arg(long, default_value = "http://127.0.0.1:8232")]
        rpc_url: String,
        #[arg(long)]
        cookie: PathBuf,
        #[arg(long, default_value_t = 4)]
        queries: usize,
    },
}

type AnyError = Box<dyn std::error::Error + Send + Sync>;
fn require(ok: bool, message: &str) -> Result<(), AnyError> {
    if ok {
        Ok(())
    } else {
        Err(message.into())
    }
}
fn backend(cuda: bool) -> MatvecBackend {
    if cuda {
        MatvecBackend::Cuda { device: 0 }
    } else {
        MatvecBackend::Cpu
    }
}
fn generation(
    entries: usize,
    advanced: bool,
    previous: Option<&Generation>,
    cuda: bool,
) -> Result<(Generation, status::index::Snapshot), AnyError> {
    let began = Instant::now();
    let snapshot = fixture::snapshot(entries, advanced)?;
    let index_ms = began.elapsed().as_secs_f64() * 1000.;
    println!(
        "{}",
        json!({"phase":"index","ms":index_ms,"entries":snapshot.entries,"evicted_blocks":snapshot.evicted_blocks,"coverage_start":snapshot.start})
    );
    let (mut generation, stats) = Generation::prepare(
        &snapshot,
        if advanced { 2 } else { 1 },
        0,
        status::now_ms(),
        previous,
        backend(cuda),
    )?;
    println!(
        "{}",
        json!({"phase":"preparation","advanced":advanced,"stats":stats,"twenty_second_preparation_gate":stats.total_ms<=20_000.})
    );
    // A fresh check of the immutable synthetic oracle, not a live node observation.
    generation.manifest.observed_ms = status::now_ms();
    status::telemetry::preparation(index_ms, &stats, &snapshot, &generation.manifest);
    Ok((generation, snapshot))
}

// Fixture shutdown must close accepted keepalive connections, not only its listener.
struct FixtureServer {
    shutdown: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl FixtureServer {
    fn spawn(listener: TcpListener, routes: axum::Router) -> Self {
        let (shutdown, receive) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, routes)
                .with_graceful_shutdown(async {
                    let _ = receive.await;
                })
                .await
                .unwrap();
        });
        Self { shutdown, task }
    }
    async fn stop(self) -> Result<(), AnyError> {
        let _ = self.shutdown.send(());
        tokio::time::timeout(Duration::from_secs(5), self.task).await??;
        Ok(())
    }
    fn abort(self) {
        let _ = self.shutdown.send(());
        self.task.abort();
    }
}

async fn listeners(
    controller: &Controller,
    state_dir: PathBuf,
    listen: SocketAddr,
    worker: SocketAddr,
    router: SocketAddr,
    monitoring: SocketAddr,
    live: bool,
) -> Result<(String, Vec<FixtureServer>), AnyError> {
    require(
        listen.ip().is_loopback()
            && worker.ip().is_loopback()
            && router.ip().is_loopback()
            && monitoring.ip().is_loopback(),
        "Status listeners must be loopback",
    )?;
    let w = TcpListener::bind(worker).await?;
    let waddr = w.local_addr()?;
    let r = TcpListener::bind(router).await?;
    let raddr = r.local_addr()?;
    let c = TcpListener::bind(listen).await?;
    let caddr = c.local_addr()?;
    let m = TcpListener::bind(monitoring).await?;
    // Exercise route coexistence with a real, deliberately unready Enhance
    // coordinator. No Enhance ingestion/placement loop runs in this service.
    let inventory = vec![Group {
        placement_policy: Default::default(),
        id: "isolated-unready-enhance".into(),
        sequence: 0,
        replicas: (0..2)
            .map(|i| Replica {
                name: format!("unready-{i}"),
                url: format!("http://127.0.0.1:{}", 9 + i),
                incarnation: String::new(),
                ledger: Ledger::default(),
            })
            .collect(),
    }];
    let status_routes = controller.coordinator_routes(format!("http://{raddr}"));
    let routes = if live {
        status_routes
    } else {
        let coordinator = Coordinator::open(&state_dir, inventory)?;
        coordinator.router().merge(status_routes)
    };
    let worker_routes = controller.worker_routes();
    let router_routes = controller.router_routes(format!("http://{waddr}"));
    let jobs = vec![
        FixtureServer::spawn(w, worker_routes),
        FixtureServer::spawn(r, router_routes),
        FixtureServer::spawn(c, routes),
        FixtureServer::spawn(m, status::telemetry::routes()),
    ];
    println!(
        "{}",
        json!({"phase":"listening","coordinator":caddr.to_string(),"worker":waddr.to_string(),"router":raddr.to_string(),"source":if live {"live canonical RPC"} else {"synthetic immutable fixture"},"single_process":true})
    );
    Ok((format!("http://{caddr}"), jobs))
}

fn reaffirm(controller: Controller, digest: Hash) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            // This fixture's source is immutable. Failed/contended refreshes do not renew age.
            if controller.reaffirm_fixture(digest).is_ok() {
                status::telemetry::reaffirmed(status::now_ms());
            }
        }
    })
}

async fn connect(http: &reqwest::Client, origin: &str, advanced: bool) -> Result<Client, AnyError> {
    let m: Manifest = http
        .get(format!("{origin}/v1/status/init"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let height = fixture::START + fixture::BLOCKS as u32 - 1 + u32::from(advanced);
    // Anchor comes from the independent fixture definition, not the response.
    let anchor = AcceptedAnchor {
        network: fixture::NETWORK,
        height,
        hash: fixture::block_hash(height),
    };
    let public = http
        .get(format!(
            "{origin}/v1/status/session/{}",
            hex::encode(m.id())
        ))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    Ok(Client::new(m, &public, &anchor)?)
}

async fn lookup(
    http: &reqwest::Client,
    origin: &str,
    client: &Client,
    txid: Hash,
    earliest: Option<u32>,
) -> Result<Observation, AnyError> {
    let q = client.prepare(&txid, earliest, status::now_ms())?;
    let response = http
        .post(format!("{origin}/v1/status/query"))
        .body(q.body.clone())
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    Ok(client.decode(q, &response, status::now_ms())?)
}

async fn check_cases(
    http: &reqwest::Client,
    origin: &str,
    controller: &Controller,
    entries: usize,
    advanced: bool,
) -> Result<(), AnyError> {
    controller.reaffirm_fixture(controller.current().manifest.rows_digest)?;
    let client = connect(http, origin, advanced).await?;
    for (id, expected) in fixture::cases(entries, advanced) {
        let observed = lookup(
            http,
            origin,
            &client,
            id,
            Some(client.manifest.anchor_height),
        )
        .await?;
        require(observed == expected, "observation oracle mismatch")?;
    }
    // Exercise different buckets rather than repeatedly testing only four txids.
    for ordinal in 3..19 {
        controller.reaffirm_fixture(controller.current().manifest.rows_digest)?;
        let sampled = connect(http, origin, advanced).await?;
        let (id, expected) =
            fixture::case_at(entries, advanced, sampled.manifest.coverage_start, ordinal);
        require(
            lookup(
                http,
                origin,
                &sampled,
                id,
                Some(sampled.manifest.anchor_height),
            )
            .await?
                == expected,
            "sampled oracle mismatch",
        )?;
    }
    controller.reaffirm_fixture(controller.current().manifest.rows_digest)?;
    let client = connect(http, origin, advanced).await?;
    let error = lookup(http, origin, &client, fixture::txid(u64::MAX), None)
        .await
        .unwrap_err();
    require(
        error.downcast_ref::<Error>() == Some(&Error::CoverageIncomplete),
        "unknown coverage became absence",
    )?;
    let a = client.prepare(&fixture::txid(0), None, status::now_ms())?;
    let b = client.prepare(&fixture::txid(0), None, status::now_ms())?;
    require(a.body != b.body, "queries reused randomness")?;
    require(
        client.prepare(&[0; 31], None, status::now_ms()).is_err(),
        "short txid accepted",
    )?;
    let mut wrong = a.body.clone();
    wrong[4] ^= 1;
    require(
        http.post(format!("{origin}/v1/status/query"))
            .body(wrong)
            .send()
            .await?
            .status()
            == 409,
        "wrong binding accepted",
    )?;
    let mut response = http
        .post(format!("{origin}/v1/status/query"))
        .body(a.body.clone())
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?
        .to_vec();
    response[36] ^= 1;
    require(
        client.decode(a, &response, status::now_ms()) == Err(Error::Malformed),
        "wrong response request id accepted",
    )?;
    require(
        http.post(format!(
            "{origin}/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetTransaction"
        ))
        .send()
        .await?
        .status()
            == 404,
        "unexpected payload endpoint",
    )?;
    println!(
        "{}",
        json!({"phase":"http_cases","advanced":advanced,"passed":true,"cases":["mined","mempool_or_mined_transition","forked","not_found","coverage_incomplete","fresh_randomness","short_txid","wrong_binding","wrong_response","no_payload_route"]})
    );
    Ok(())
}

struct ProbeConfig {
    origin: String,
    entries: usize,
    queries: usize,
    concurrency: usize,
    qps: u64,
    seconds: Option<u64>,
    report_jsonl: Option<PathBuf>,
    max_p99_ms: Option<f64>,
}

async fn probe(config: ProbeConfig) -> Result<(), AnyError> {
    let ProbeConfig {
        origin,
        entries,
        queries,
        concurrency,
        qps,
        seconds,
        report_jsonl,
        max_p99_ms,
    } = config;
    let queries = match seconds {
        Some(seconds) if qps > 0 && seconds > 0 => usize::try_from(
            seconds
                .checked_mul(qps)
                .ok_or("load arrival count overflow")?,
        )?,
        Some(_) => return Err("--seconds requires positive --qps and duration".into()),
        None => queries,
    };
    require(
        (4..=MAX_ENTRIES).contains(&entries),
        "entries must be 4..=75% ceiling",
    )?;
    require(
        concurrency > 0 && concurrency <= 16 && queries > 0,
        "invalid load configuration",
    )?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let permits = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut jobs = tokio::task::JoinSet::new();
    let began = Instant::now();
    let mut unstarted = 0;
    let mut latencies = Vec::new();
    let mut errors = std::collections::BTreeMap::new();
    let mut report = report_jsonl
        .map(|path| File::options().write(true).create_new(true).open(path))
        .transpose()?
        .map(BufWriter::new);
    for i in 0..queries {
        if qps > 0 {
            tokio::time::sleep_until(tokio::time::Instant::from_std(
                began + Duration::from_secs_f64(i as f64 / qps as f64),
            ))
            .await;
        }
        let permit = if qps > 0 {
            match permits.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    unstarted += 1;
                    if let Some(report) = &mut report {
                        writeln!(report, "{}", json!({"arrival":i,"result":"unstarted"}))?;
                    }
                    continue;
                }
            }
        } else {
            permits.clone().acquire_owned().await?
        };
        let http = http.clone();
        let origin = origin.clone();
        jobs.spawn(async move {
            let _permit = permit;
            let start = Instant::now();
            let mut generation = None;
            let mut observation_age_ms = None;
            // Include init, material download, setup and query in every measured lookup.
            let result = async {
                let client = connect(&http, &origin, false).await?;
                generation = Some(client.manifest.generation);
                observation_age_ms = status::now_ms().checked_sub(client.manifest.observed_ms);
                let (txid, expected) =
                    fixture::case_at(entries, false, client.manifest.coverage_start, i);
                let actual = lookup(
                    &http,
                    &origin,
                    &client,
                    txid,
                    Some(client.manifest.anchor_height),
                )
                .await?;
                observation_age_ms = status::now_ms().checked_sub(client.manifest.observed_ms);
                require(actual == expected, "oracle mismatch")?;
                Ok::<_, AnyError>(())
            }
            .await;
            (
                i,
                start.elapsed().as_secs_f64() * 1000.,
                generation,
                observation_age_ms,
                result.map_err(|e| e.to_string()),
            )
        });
        // Drain completed tasks while scheduling. A six-hour run must not retain
        // hundreds of thousands of completed JoinSet entries in memory.
        while let Some(result) = jobs.try_join_next() {
            let (arrival, ms, generation, age_ms, result) = result?;
            if let Some(report) = &mut report {
                writeln!(
                    report,
                    "{}",
                    json!({"arrival":arrival,"duration_ms":ms,
                    "generation":generation,"observation_age_ms":age_ms,
                    "result":result.as_ref().map(|_| "correct").unwrap_or("error"),
                    "error":result.as_ref().err()})
                )?;
            }
            match result {
                Ok(()) => latencies.push(ms),
                Err(e) => *errors.entry(e).or_insert(0usize) += 1,
            }
        }
        if i % 1000 == 0 {
            if let Some(report) = &mut report {
                report.flush()?;
            }
        }
    }
    while let Some(result) = jobs.join_next().await {
        let (arrival, ms, generation, age_ms, result) = result?;
        if let Some(report) = &mut report {
            writeln!(
                report,
                "{}",
                json!({"arrival":arrival,"duration_ms":ms,
                "generation":generation,"observation_age_ms":age_ms,
                "result":result.as_ref().map(|_| "correct").unwrap_or("error"),
                "error":result.as_ref().err()})
            )?;
        }
        match result {
            Ok(()) => latencies.push(ms),
            Err(e) => *errors.entry(e).or_insert(0usize) += 1,
        }
    }
    if let Some(report) = &mut report {
        report.flush()?;
    }
    let seconds = began.elapsed().as_secs_f64();
    latencies.sort_by(f64::total_cmp);
    let pct = |p: f64| {
        latencies
            .get(((latencies.len().saturating_sub(1)) as f64 * p) as usize)
            .copied()
    };
    let p99 = pct(0.99);
    println!(
        "{}",
        json!({"phase":"load","offered":queries,"unstarted":unstarted,"correct":latencies.len(),"errors":errors,
        "seconds":seconds,"correct_qps":latencies.len() as f64/seconds,"p50_ms":pct(0.5),"p95_ms":pct(0.95),"p99_ms":p99,"includes_material_refresh":true,"source":"synthetic_fixture","protocol":PROTOCOL,"rows":ROWS,"slots":SLOTS,"slot_bytes":SLOT_BYTES,"columns":ROW_BYTES/2,"row_bytes":ROW_BYTES,"database_bytes":ROWS*ROW_BYTES,"production_qualified":false})
    );
    require(
        latencies.len() == queries,
        "load had errors or unstarted arrivals",
    )?;
    if let Some(limit) = max_p99_ms {
        require(
            limit.is_finite() && limit > 0. && p99.is_some_and(|value| value <= limit),
            "complete lookup p99 exceeded limit",
        )?;
    }
    Ok(())
}

async fn probe_live(
    origin: String,
    rpc_url: String,
    cookie: PathBuf,
    queries: usize,
) -> Result<(), AnyError> {
    require((1..=100).contains(&queries), "queries must be 1..=100")?;
    let rpc = ZakuraClient::from_cookie_file(rpc_url, cookie)?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let manifest: Manifest = http
        .get(format!("{origin}/v1/status/init"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    manifest.fresh(status::now_ms())?;
    let network: zakura_chain::block::Hash = rpc.block_hash(0).await?.parse()?;
    let anchor: zakura_chain::block::Hash = rpc
        .block_hash(u64::from(manifest.anchor_height))
        .await?
        .parse()?;
    require(
        manifest.network == network.0 && manifest.anchor_hash == anchor.0,
        "live manifest anchor is not canonical",
    )?;
    let block = rpc.status_block(u64::from(manifest.anchor_height)).await?;
    require(
        block.hash == anchor.0,
        "raw block hash differs from canonical anchor",
    )?;
    let txid = *block
        .txids
        .first()
        .ok_or("anchor block has no transactions")?;
    let mut wallet = enhance_pir::status::HttpClient::new(&origin, http);
    let began = Instant::now();
    for _ in 0..queries {
        let answer = wallet
            .lookup(txid, Some(block.height), |_| {
                Ok(AcceptedAnchor {
                    network: network.0,
                    height: block.height,
                    hash: anchor.0,
                })
            })
            .await?;
        require(
            answer == Observation::Mined(block.height),
            "live encrypted answer differs from canonical block oracle",
        )?;
    }
    println!(
        "{}",
        json!({"phase":"live_encrypted_probe","queries":queries,"correct":queries,
            "anchor_height":block.height,"elapsed_ms":began.elapsed().as_secs_f64()*1000.,
            "source":"canonical_rpc","txid_logged":false})
    );
    Ok(())
}

fn matches_manifest(snapshot: &status::index::Snapshot, manifest: &Manifest) -> bool {
    snapshot.network == manifest.network
        && snapshot.salt == manifest.salt
        && snapshot.start == manifest.coverage_start
        && snapshot.height == manifest.anchor_height
        && snapshot.anchor == manifest.anchor_hash
        && snapshot.entries == manifest.entries
        && snapshot.digest == manifest.rows_digest
}

const UNCHANGED_REAFFIRM_MS: u64 = 5_000;

async fn revalidate_candidate(
    rpc: &ZakuraClient,
    verified: &status::source::Observation,
    candidate: &mut Generation,
) -> Result<bool, AnyError> {
    // A newer mempool observation does not invalidate a complete earlier
    // snapshot. Keep its original timestamp so freshness is never renewed by
    // rows that were not prepared from the newer observation.
    let canonical: zakura_chain::block::Hash = rpc
        .block_hash(u64::from(candidate.manifest.anchor_height))
        .await?
        .parse()?;
    Ok(accept_candidate_observation(
        &mut candidate.manifest,
        verified,
        canonical.0,
        status::now_ms(),
    )?)
}

fn accept_candidate_observation(
    manifest: &mut Manifest,
    verified: &status::source::Observation,
    canonical: Hash,
    now_ms: u64,
) -> Result<bool, Error> {
    if canonical != manifest.anchor_hash || verified.snapshot.network != manifest.network {
        return Err(Error::Malformed);
    }
    let unchanged = matches_manifest(&verified.snapshot, manifest);
    if unchanged {
        manifest.observed_ms = verified.observed_ms;
    }
    manifest.fresh(now_ms)?;
    Ok(unchanged)
}

struct LiveConfig {
    rpc_url: String,
    cookie: PathBuf,
    salt_hex: String,
    window_blocks: u32,
    cuda: bool,
    listen: SocketAddr,
    worker: SocketAddr,
    router: SocketAddr,
    monitoring: SocketAddr,
    state_dir: PathBuf,
}

async fn serve_live(config: LiveConfig) -> Result<(), AnyError> {
    let LiveConfig {
        rpc_url,
        cookie,
        salt_hex,
        window_blocks,
        cuda,
        listen,
        worker,
        router,
        monitoring,
        state_dir,
    } = config;
    let salt: Hash = hex::decode(salt_hex)?
        .try_into()
        .map_err(|_| "salt must be exactly 32 bytes")?;
    let rpc = ZakuraClient::from_cookie_file(rpc_url, cookie)?;
    let mut source =
        status::source::RollingWindow::open(state_dir.join("source"), salt, window_blocks)?;
    let first = source.observe(&rpc).await?;
    let mut authority = status::authority::Authority::open(
        state_dir.join("authority"),
        first.snapshot.network,
        salt,
    )?;
    let controller = loop {
        let observed = source.observe(&rpc).await?;
        let (number, epoch) = authority.next_identity()?;
        let build = tokio::task::spawn_blocking(move || {
            Generation::prepare(
                &observed.snapshot,
                number,
                epoch,
                observed.observed_ms,
                None,
                backend(cuda),
            )
        })
        .await??;
        let (mut candidate, stats) = build;
        let verified = source.observe(&rpc).await?;
        let unchanged = match revalidate_candidate(&rpc, &verified, &mut candidate).await {
            Ok(value) => value,
            Err(error) => {
                println!(
                    "{}",
                    json!({"phase":"live_candidate_discarded","reason":error.to_string()})
                );
                continue;
            }
        };
        authority.commit(&candidate.manifest)?;
        println!(
            "{}",
            json!({"phase":"live_initial_publication","generation":number,"recovery_epoch":epoch,"preparation":stats,"anchor_height":candidate.manifest.anchor_height,"source_unchanged":unchanged,"source_observed_ms":candidate.manifest.observed_ms,"queryable_ms":status::now_ms()})
        );
        break Controller::new(candidate);
    };
    let (_origin, jobs) = listeners(
        &controller,
        state_dir,
        listen,
        worker,
        router,
        monitoring,
        true,
    )
    .await?;
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = interval.tick() => {}
        }
        let observed = match source.observe(&rpc).await {
            Ok(value) => value,
            Err(error) => {
                println!(
                    "{}",
                    json!({"phase":"live_observation_failed","error":error.to_string()})
                );
                continue;
            }
        };
        let previous = controller.current();
        if matches_manifest(&observed.snapshot, &previous.manifest)
            && observed
                .observed_ms
                .saturating_sub(previous.manifest.observed_ms)
                < UNCHANGED_REAFFIRM_MS
        {
            continue;
        }
        let (number, epoch) = authority.next_identity()?;
        let candidate = if matches_manifest(&observed.snapshot, &previous.manifest) {
            let mut candidate = (*previous).clone();
            candidate.manifest.generation = number;
            candidate.manifest.recovery_epoch = epoch;
            candidate.manifest.observed_ms = observed.observed_ms;
            candidate
        } else {
            let snapshot = observed.snapshot;
            let previous = previous.clone();
            let (mut candidate, stats) = match tokio::task::spawn_blocking(move || {
                Generation::prepare(
                    &snapshot,
                    number,
                    epoch,
                    observed.observed_ms,
                    Some(&previous),
                    backend(cuda),
                )
            })
            .await?
            {
                Ok(value) => value,
                Err(error) => {
                    println!(
                        "{}",
                        json!({"phase":"live_preparation_failed","error":error.to_string()})
                    );
                    continue;
                }
            };
            let verified = match source.observe(&rpc).await {
                Ok(value) => value,
                Err(error) => {
                    println!(
                        "{}",
                        json!({"phase":"live_revalidation_failed","error":error.to_string()})
                    );
                    continue;
                }
            };
            let unchanged = match revalidate_candidate(&rpc, &verified, &mut candidate).await {
                Ok(value) => value,
                Err(error) => {
                    println!(
                        "{}",
                        json!({"phase":"live_candidate_discarded","reason":error.to_string()})
                    );
                    continue;
                }
            };
            println!(
                "{}",
                json!({"phase":"live_prepared","stats":stats,"generation":number,"source_unchanged":unchanged})
            );
            candidate
        };
        if candidate.manifest.fresh(status::now_ms()).is_err() {
            continue;
        }
        authority.commit(&candidate.manifest)?;
        controller.activate(candidate)?;
        println!(
            "{}",
            json!({"phase":"live_publication","generation":number,"recovery_epoch":epoch,"source_observed_ms":controller.current().manifest.observed_ms,"queryable_ms":status::now_ms()})
        );
    }
    for job in jobs {
        job.abort();
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    match Args::parse().command {
        Command::ProbeLiveLoad {
            origin,
            query_origin,
            rpc_url,
            cookie,
            seconds,
            report_dir,
        } => {
            live_load(
                query_origin.unwrap_or_else(|| origin.clone()),
                origin,
                ZakuraClient::from_cookie_file(rpc_url, cookie)?,
                seconds,
                report_dir,
            )
            .await?;
        }
        Command::ServeDistributed {
            config,
            rpc_url,
            cookie,
            public_listen,
        } => {
            let config: status::publisher::Config =
                serde_json::from_slice(&std::fs::read(config)?)?;
            require(
                !config.public_enabled || public_listen.is_some(),
                "public_enabled requires --public-listen",
            )?;
            require(
                config.public_enabled || public_listen.is_none(),
                "--public-listen requires public_enabled in the controller config",
            )?;
            require(
                public_listen.is_none_or(|l| l.ip().is_loopback()),
                "--public-listen must be loopback; the HTTPS ingress proxies to it",
            )?;
            let publisher = status::publisher::Publisher::new(config)?;
            // Bind before starting the controller so a port conflict fails fast.
            let public = match public_listen {
                Some(listen) => Some(TcpListener::bind(listen).await?),
                None => None,
            };
            publisher.start(ZakuraClient::from_cookie_file(rpc_url, cookie)?)?;
            match public {
                // Public admission still depends on the publisher's healthy flag.
                Some(listener) => {
                    let routes = publisher.routes();
                    tokio::select! {
                        result = axum::serve(listener, routes) => result?,
                        result = tokio::signal::ctrl_c() => result?,
                    }
                }
                None => tokio::signal::ctrl_c().await?,
            }
        }
        Command::ValidateDistributed {
            entries,
            cuda,
            state_dir,
        } => {
            validate_distributed(entries, cuda, state_dir).await?;
        }
        Command::Role {
            role,
            network_hex,
            state_dir,
            listen,
            query_listen,
            artifact_origin,
            worker_origin,
            cuda,
        } => {
            let network: Hash = hex::decode(network_hex)?
                .try_into()
                .map_err(|_| "network must be 32 bytes")?;
            status::distributed::Service::open(
                role,
                network,
                state_dir,
                artifact_origin,
                worker_origin,
                cuda,
            )?
            .serve(listen, query_listen)
            .await?;
        }
        Command::ObserveLive {
            rpc_url,
            cookie,
            salt_hex,
            window_blocks,
            samples,
            interval_ms,
            state_dir,
        } => {
            require(
                samples > 0 && interval_ms > 0,
                "invalid observation schedule",
            )?;
            let salt: Hash = hex::decode(salt_hex)?
                .try_into()
                .map_err(|_| "salt must be exactly 32 bytes")?;
            let rpc = ZakuraClient::from_cookie_file(rpc_url, cookie)?;
            let mut window = match state_dir {
                Some(path) => status::source::RollingWindow::open(path, salt, window_blocks)?,
                None => status::source::RollingWindow::new(salt, window_blocks)?,
            };
            let mut previous = None;
            for sample in 0..samples {
                if sample > 0 {
                    tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                }
                let observed = window.observe(&rpc).await?;
                let changed_rows = previous
                    .as_ref()
                    .map(|old| observed.snapshot.changed_rows(old));
                println!(
                    "{}",
                    json!({"phase":"live_observation","published":false,"sample":sample,
                        "observed_ms":observed.observed_ms,"collect_ms":observed.collect_ms,
                        "source_blocks":observed.blocks,"source_mempool":observed.mempool,
                        "retained_forks":observed.retained_forks,"changed_rows":changed_rows,
                        "anchor_height":observed.snapshot.height,
                        "coverage_start":observed.snapshot.start,
                        "indexed_entries":observed.snapshot.entries,
                        "evicted_blocks":observed.snapshot.evicted_blocks,
                        "rows_digest":hex::encode(observed.snapshot.digest)})
                );
                previous = Some(observed.snapshot);
            }
        }
        Command::Probe {
            origin,
            entries,
            queries,
            concurrency,
            qps,
            seconds,
            report_jsonl,
            max_p99_ms,
        } => {
            probe(ProbeConfig {
                origin,
                entries,
                queries,
                concurrency,
                qps,
                seconds,
                report_jsonl,
                max_p99_ms,
            })
            .await?
        }
        Command::ProbeLive {
            origin,
            rpc_url,
            cookie,
            queries,
        } => probe_live(origin, rpc_url, cookie, queries).await?,
        Command::Serve {
            entries,
            cuda,
            listen,
            worker,
            router,
            monitoring,
            state_dir,
        } => {
            require(
                (4..=MAX_ENTRIES).contains(&entries),
                "entries must be 4..=75% ceiling",
            )?;
            let (g, snapshot) = generation(entries, false, None, cuda)?;
            let controller = Controller::new(g);
            let (_origin, _jobs) = listeners(
                &controller,
                state_dir,
                listen,
                worker,
                router,
                monitoring,
                false,
            )
            .await?;
            let _fresh = reaffirm(controller, snapshot.digest);
            tokio::signal::ctrl_c().await?;
        }
        Command::ServeLive {
            rpc_url,
            cookie,
            salt_hex,
            window_blocks,
            cuda,
            listen,
            worker,
            router,
            monitoring,
            state_dir,
        } => {
            serve_live(LiveConfig {
                rpc_url,
                cookie,
                salt_hex,
                window_blocks,
                cuda,
                listen,
                worker,
                router,
                monitoring,
                state_dir,
            })
            .await?;
        }
        Command::Validate {
            entries,
            cuda,
            state_dir,
        } => {
            require(
                (4..=MAX_ENTRIES).contains(&entries),
                "entries must be 4..=75% ceiling",
            )?;
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()?;
            let (g, snapshot) = generation(entries, false, None, cuda)?;
            g.verify_full_hint(&snapshot)?;
            println!(
                "{}",
                json!({"phase":"initial_full_hint_differential","passed":true})
            );
            let controller = Controller::new(g);
            controller.reaffirm_fixture(snapshot.digest)?;
            let zero = "127.0.0.1:0".parse()?;
            let (origin, mut jobs) =
                listeners(&controller, state_dir, zero, zero, zero, zero, false).await?;
            check_cases(&http, &origin, &controller, entries, false).await?;
            let previous = controller.current();
            let old_client = connect(&http, &origin, false).await?;
            let (next, next_snapshot) = generation(entries, true, Some(&previous), cuda)?;
            println!(
                "{}",
                json!({"phase":"update","changed_rows":next_snapshot.changed_rows(&snapshot)})
            );
            next.verify_full_hint(&next_snapshot)?;
            let rebuilt_router =
                Generation::prepare_router(next.manifest.clone(), &next.hint_bytes()?)?;
            require(
                rebuilt_router.public == next.public,
                "reused packing differs from full reconstruction",
            )?;
            drop(rebuilt_router);
            controller.activate(next)?;
            drop(previous);
            check_cases(&http, &origin, &controller, entries, true).await?;
            let mut wallet = enhance_pir::status::HttpClient::new(&origin, http.clone());
            let (retry_txid, retry_expected) = fixture::case_at(
                entries,
                true,
                controller.current().manifest.coverage_start,
                7,
            );
            let accepted = |manifest: &Manifest| {
                Ok(AcceptedAnchor {
                    network: fixture::NETWORK,
                    height: manifest.anchor_height,
                    hash: fixture::block_hash(manifest.anchor_height),
                })
            };
            require(
                wallet
                    .lookup(
                        retry_txid,
                        Some(controller.current().manifest.anchor_height),
                        accepted,
                    )
                    .await?
                    == retry_expected,
                "initial wallet lookup failed",
            )?;
            for _ in 0..2 {
                let mut replacement = (*controller.current()).clone();
                replacement.manifest.generation += 1;
                replacement.manifest.observed_ms = status::now_ms();
                controller.activate(replacement)?;
            }
            require(
                wallet
                    .lookup(
                        retry_txid,
                        Some(controller.current().manifest.anchor_height),
                        accepted,
                    )
                    .await?
                    == retry_expected,
                "wallet did not recover from evicted-session conflict",
            )?;
            let mut recovered = (*controller.current()).clone();
            recovered.manifest.generation += 1;
            recovered.manifest.recovery_epoch += 1;
            recovered.manifest.observed_ms = status::now_ms();
            controller.activate(recovered)?;
            require(
                wallet
                    .lookup(
                        retry_txid,
                        Some(controller.current().manifest.anchor_height),
                        accepted,
                    )
                    .await?
                    == retry_expected,
                "wallet did not recover from recovery-epoch conflict",
            )?;
            println!(
                "{}",
                json!({"phase":"wallet_session_retry","passed":true,"cases":["evicted_session_409","recovery_epoch_410"]})
            );
            controller.reaffirm_fixture(controller.current().manifest.rows_digest)?;
            let failure_client = connect(&http, &origin, true).await?;
            let failure_query =
                failure_client.prepare(&fixture::txid(u64::MAX - 1), None, status::now_ms())?;
            jobs.remove(0).stop().await?;
            let failed = http
                .post(format!("{origin}/v1/status/query"))
                .body(failure_query.body)
                .send()
                .await?;
            require(
                failed.status() == 502 || failed.status() == 503,
                "worker loss did not fail closed",
            )?;
            println!(
                "{}",
                json!({"phase":"worker_loss","passed":true,"http_status":failed.status().as_u16()})
            );
            require(
                old_client.manifest.id() != controller.current().manifest.id(),
                "update identity unchanged",
            )?;
            let stale_id = hex::encode(old_client.manifest.id());
            tokio::time::sleep(Duration::from_millis(MAX_AGE_MS + 20)).await;
            require(
                !http
                    .get(format!("{origin}/v1/status/session/{stale_id}"))
                    .send()
                    .await?
                    .status()
                    .is_success(),
                "stale session accepted",
            )?;
            require(
                http.get(format!("{origin}/v1/status/init"))
                    .send()
                    .await?
                    .status()
                    == 503,
                "stale source served",
            )?;
            let mut recovery = (*controller.current()).clone();
            let revoked_id = hex::encode(recovery.manifest.id());
            recovery.manifest.generation += 1;
            recovery.manifest.recovery_epoch += 1;
            recovery.manifest.observed_ms = status::now_ms();
            controller.activate(recovery)?;
            require(
                http.get(format!("{origin}/v1/status/session/{revoked_id}"))
                    .send()
                    .await?
                    .status()
                    == 410,
                "recovery fence did not revoke old view",
            )?;
            println!("{}", json!({"phase":"recovery_epoch_fence","passed":true}));
            for job in jobs {
                job.abort();
            }
            println!(
                "{}",
                json!({"phase":"validation_complete","passed":true,"cuda":cuda,"wallet_changes":false,"incremental_granularity":"bounded coefficient deltas, dense-unit fallback; unchanged packing block reuse","protocol":PROTOCOL,"rows":ROWS,"slots":SLOTS,"slot_bytes":SLOT_BYTES,"columns":ROW_BYTES/2,"row_bytes":ROW_BYTES,"database_bytes":ROWS*ROW_BYTES,"production_qualified":false})
            );
        }
    }
    Ok(())
}

async fn validate_distributed(
    entries: usize,
    cuda: bool,
    state_dir: PathBuf,
) -> Result<(), AnyError> {
    use status::distributed::{Activate, Binding, Health, Prepare, Ready};
    struct Children(Vec<std::process::Child>);
    impl Drop for Children {
        fn drop(&mut self) {
            for child in &mut self.0 {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
    let snapshot = fixture::snapshot(entries, false)?;
    let rows = Arc::new(snapshot.rows.clone());
    let digest = snapshot.digest;
    let artifact_listener = TcpListener::bind("127.0.0.1:0").await?;
    let artifact_origin = format!("http://{}", artifact_listener.local_addr()?);
    let artifacts = axum::Router::new().route(
        "/artifact/:id",
        axum::routing::get(
            move |axum::extract::Path(id): axum::extract::Path<String>| {
                let rows = rows.clone();
                async move {
                    if id == hex::encode(digest) {
                        Ok(rows.as_ref().clone())
                    } else {
                        Err(axum::http::StatusCode::NOT_FOUND)
                    }
                }
            },
        ),
    );
    let artifact_job = tokio::spawn(async move { axum::serve(artifact_listener, artifacts).await });
    let w = TcpListener::bind("127.0.0.1:0").await?;
    let r = TcpListener::bind("127.0.0.1:0").await?;
    let wa = w.local_addr()?;
    let ra = r.local_addr()?;
    drop(w);
    drop(r);
    let worker = format!("http://{wa}");
    let router = format!("http://{ra}");
    let mut children = Children(Vec::new());
    for (role, addr, origin) in [
        ("worker", wa, artifact_origin),
        ("router", ra, worker.clone()),
    ] {
        let mut command = std::process::Command::new(std::env::current_exe()?);
        command
            .args([
                "role",
                "--role",
                role,
                "--network-hex",
                &hex::encode(snapshot.network),
                "--listen",
                &addr.to_string(),
                "--artifact-origin",
                &origin,
                "--worker-origin",
                &worker,
                "--state-dir",
            ])
            .arg(state_dir.join(role));
        if cuda && role == "worker" {
            command.arg("--cuda");
        }
        children.0.push(command.spawn()?);
    }
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut bindings = Vec::new();
    let mut healths = Vec::new();
    for origin in [&worker, &router] {
        let mut health = None;
        for _ in 0..100 {
            if let Ok(response) = http.get(format!("{origin}/control/health")).send().await {
                health = Some(response.error_for_status()?.json::<Health>().await?);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let health = health.ok_or("role failed to start")?;
        healths.push(health);
    }
    let epoch = healths.iter().map(|h| h.binding.epoch).max().unwrap() + 1;
    for (origin, health) in [&worker, &router].into_iter().zip(healths) {
        let b = Binding {
            epoch,
            incarnation: health.binding.incarnation,
        };
        let ack: Binding = http
            .post(format!("{origin}/control/fence"))
            .json(&b)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        require(ack == b, "fence mismatch")?;
        bindings.push(b);
    }
    require(
        bindings[0].epoch == bindings[1].epoch,
        "fixture role epochs disagree",
    )?;
    let m = Manifest {
        protocol: PROTOCOL.into(),
        network: snapshot.network,
        salt: snapshot.salt,
        generation: status::now_ms(),
        recovery_epoch: bindings[0].epoch,
        coverage_start: snapshot.start,
        anchor_height: snapshot.height,
        anchor_hash: snapshot.anchor,
        observed_ms: status::now_ms(),
        entries: snapshot.entries,
        rows_digest: snapshot.digest,
        public_digest: [0; 32],
    };
    let w: Ready = http
        .post(format!("{worker}/control/prepare"))
        .json(&Prepare {
            binding: bindings[0].clone(),
            manifest: m.clone(),
            artifact_digest: snapshot.digest,
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let r: Ready = http
        .post(format!("{router}/control/prepare"))
        .json(&Prepare {
            binding: bindings[1].clone(),
            manifest: m.clone(),
            artifact_digest: w.artifact_digest,
        })
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let public = http
        .get(format!(
            "{router}/public/{}",
            hex::encode(r.manifest.public_digest)
        ))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?
        .to_vec();
    for (origin, binding) in [(&worker, &bindings[0]), (&router, &bindings[1])] {
        let _: Ready = http
            .post(format!("{origin}/control/activate"))
            .json(&Activate {
                binding: binding.clone(),
                manifest: r.manifest.clone(),
            })
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
    }
    let manifest = r.manifest.clone();
    let heartbeat_http = http.clone();
    let peers = [
        (worker.clone(), bindings[0].clone()),
        (router.clone(), bindings[1].clone()),
    ];
    let heartbeat = tokio::spawn(async move {
        loop {
            for (origin, binding) in &peers {
                let _ = heartbeat_http
                    .post(format!("{origin}/control/heartbeat"))
                    .json(&Activate {
                        binding: binding.clone(),
                        manifest: manifest.clone(),
                    })
                    .send()
                    .await;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    let anchor = AcceptedAnchor {
        network: snapshot.network,
        height: snapshot.height,
        hash: snapshot.anchor,
    };
    let client = Client::new(r.manifest.clone(), &public, &anchor)?;
    for (txid, expected) in fixture::cases(entries, false) {
        let actual = lookup(&http, &router, &client, txid, Some(snapshot.height)).await?;
        require(actual == expected, "distributed encrypted oracle mismatch")?;
    }
    // An authenticated fence must invalidate both new admission and already-held identities.
    let fence = Binding {
        epoch: bindings[1].epoch + 1,
        incarnation: bindings[1].incarnation.clone(),
    };
    http.post(format!("{router}/control/fence"))
        .json(&fence)
        .send()
        .await?
        .error_for_status()?;
    let query = client.prepare(&fixture::txid(0), Some(snapshot.height), status::now_ms())?;
    let response = http
        .post(format!("{router}/v1/status/query"))
        .body(query.body)
        .send()
        .await?;
    require(response.status() == 503, "fenced router still serves")?;
    heartbeat.abort();
    artifact_job.abort();
    println!(
        "{}",
        json!({"phase":"distributed_validation","passed":true,"processes":3,"cuda":cuda,
        "entries":entries,"cases":["mined","mempool","forked","not_found","remote_fence"],"protocol":PROTOCOL,"rows":ROWS,"slots":SLOTS,"slot_bytes":SLOT_BYTES,"columns":ROW_BYTES/2,"row_bytes":ROW_BYTES,"database_bytes":ROWS*ROW_BYTES,"production_qualified":false})
    );
    Ok(())
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
    let network: zakura_chain::block::Hash = rpc.block_hash(0).await?.parse()?;
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
                            let hash: zakura_chain::block::Hash = rpc.block_hash(u64::from(manifest.anchor_height)).await?.parse()?;
                            require(manifest.network == network.0 && manifest.anchor_hash == hash.0, "oracle anchor mismatch")?;
                            let txid = {
                                let mut cache = cache.lock().await;
                                if let Some(txid) = cache.get(&hash.0) { *txid } else {
                                    let block = rpc.status_block(u64::from(manifest.anchor_height)).await?;
                                    require(block.hash == hash.0, "oracle canonical block changed")?;
                                    let txid = *block.txids.first().ok_or("oracle block empty")?;
                                    if cache.len() >= 4 { cache.clear(); }
                                    cache.insert(hash.0, txid);
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
                                let client = Client::new(job_manifest.clone(), &public, &AcceptedAnchor { network: network.0, height: job_manifest.anchor_height, hash: hash.0 })?;
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

#[cfg(test)]
mod live_candidate_tests {
    use super::*;

    fn observation(now_ms: u64) -> (Manifest, status::source::Observation) {
        let snapshot = status::index::Snapshot {
            network: [1; 32],
            salt: [2; 32],
            start: 1,
            height: 1,
            anchor: [3; 32],
            entries: 0,
            rows: Vec::new(),
            digest: [4; 32],
            evicted_blocks: 0,
        };
        let manifest = Manifest {
            protocol: PROTOCOL.into(),
            network: snapshot.network,
            salt: snapshot.salt,
            generation: 1,
            recovery_epoch: 1,
            coverage_start: snapshot.start,
            anchor_height: snapshot.height,
            anchor_hash: snapshot.anchor,
            observed_ms: now_ms - 6_000,
            entries: snapshot.entries,
            rows_digest: snapshot.digest,
            public_digest: [5; 32],
        };
        (
            manifest,
            status::source::Observation {
                snapshot,
                observed_ms: now_ms,
                collect_ms: 0.,
                blocks: 1,
                mempool: 0,
                retained_forks: 0,
            },
        )
    }

    #[test]
    fn changed_source_keeps_original_age_and_reorg_rejects_candidate() {
        let now = status::now_ms();
        let (mut manifest, mut verified) = observation(now);
        verified.snapshot.digest = [6; 32];
        assert_eq!(
            accept_candidate_observation(&mut manifest, &verified, [3; 32], now),
            Ok(false)
        );
        assert_eq!(manifest.observed_ms, now - 6_000);
        assert_eq!(
            accept_candidate_observation(&mut manifest, &verified, [7; 32], now),
            Err(Error::Malformed)
        );
        assert_eq!(manifest.observed_ms, now - 6_000);
    }

    #[test]
    fn identical_source_can_refresh_timestamp_but_stale_candidate_cannot_publish() {
        let now = status::now_ms();
        let (mut manifest, verified) = observation(now);
        assert_eq!(
            accept_candidate_observation(&mut manifest, &verified, [3; 32], now),
            Ok(true)
        );
        assert_eq!(manifest.observed_ms, now);
        let (mut stale, mut changed) = observation(now);
        changed.snapshot.digest = [6; 32];
        stale.observed_ms = now - MAX_AGE_MS - 1;
        assert_eq!(
            accept_candidate_observation(&mut stale, &changed, [3; 32], now),
            Err(Error::Stale)
        );
    }
}
