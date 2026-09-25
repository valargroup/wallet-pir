//! Isolated synthetic backend. Uses a declared synthetic source; never enables wallet traffic.
use clap::{Parser, Subcommand};
use enhance_pir::status::*;
use enhance_pir_server::{
    control::{Group, Ledger, Replica},
    coordinator::Coordinator,
    status::{self, fixture, Controller, Generation},
};
use ipir_sp::server::MatvecBackend;
use serde_json::json;
use std::{
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
        json!({"phase":"preparation","advanced":advanced,"stats":stats,"five_second_preparation_gate":stats.total_ms<=5000.})
    );
    // A fresh check of the immutable synthetic oracle, not a live node observation.
    generation.manifest.observed_ms = status::now_ms();
    status::telemetry::preparation(index_ms, &stats, &snapshot, &generation.manifest);
    Ok((generation, snapshot))
}

async fn listeners(
    controller: &Controller,
    state_dir: PathBuf,
    listen: SocketAddr,
    worker: SocketAddr,
    router: SocketAddr,
    monitoring: SocketAddr,
) -> Result<(String, Vec<tokio::task::JoinHandle<()>>), AnyError> {
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
    let coordinator = Coordinator::open(&state_dir, inventory)?;
    let routes = coordinator
        .router()
        .merge(controller.coordinator_routes(format!("http://{raddr}")));
    let worker_routes = controller.worker_routes();
    let router_routes = controller.router_routes(format!("http://{waddr}"));
    let jobs = vec![
        tokio::spawn(async move {
            axum::serve(w, worker_routes).await.unwrap();
        }),
        tokio::spawn(async move {
            axum::serve(r, router_routes).await.unwrap();
        }),
        tokio::spawn(async move {
            axum::serve(c, routes).await.unwrap();
        }),
        tokio::spawn(async move {
            axum::serve(m, status::telemetry::routes()).await.unwrap();
        }),
    ];
    println!(
        "{}",
        json!({"phase":"listening","coordinator":caddr.to_string(),"worker":waddr.to_string(),"router":raddr.to_string(),"source":"synthetic immutable fixture"})
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

async fn probe(
    origin: String,
    entries: usize,
    queries: usize,
    concurrency: usize,
    qps: u64,
) -> Result<(), AnyError> {
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
            // Include init, material download, setup and query in every measured lookup.
            let result = async {
                let client = connect(&http, &origin, false).await?;
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
                require(actual == expected, "oracle mismatch")?;
                Ok::<_, AnyError>(())
            }
            .await;
            (
                start.elapsed().as_secs_f64() * 1000.,
                result.map_err(|e| e.to_string()),
            )
        });
    }
    let mut latencies = Vec::new();
    let mut errors = std::collections::BTreeMap::new();
    while let Some(result) = jobs.join_next().await {
        let (ms, result) = result?;
        match result {
            Ok(()) => latencies.push(ms),
            Err(e) => *errors.entry(e).or_insert(0usize) += 1,
        }
    }
    let seconds = began.elapsed().as_secs_f64();
    latencies.sort_by(f64::total_cmp);
    let pct = |p: f64| {
        latencies
            .get(((latencies.len().saturating_sub(1)) as f64 * p) as usize)
            .copied()
    };
    println!(
        "{}",
        json!({"phase":"load","offered":queries,"unstarted":unstarted,"correct":latencies.len(),"errors":errors,
        "seconds":seconds,"correct_qps":latencies.len() as f64/seconds,"p50_ms":pct(0.5),"p95_ms":pct(0.95),"p99_ms":pct(0.99),"includes_material_refresh":true})
    );
    require(
        latencies.len() == queries,
        "load had errors or unstarted arrivals",
    )
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    match Args::parse().command {
        Command::Probe {
            origin,
            entries,
            queries,
            concurrency,
            qps,
        } => probe(origin, entries, queries, concurrency, qps).await?,
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
            let (_origin, _jobs) =
                listeners(&controller, state_dir, listen, worker, router, monitoring).await?;
            let _fresh = reaffirm(controller, snapshot.digest);
            tokio::signal::ctrl_c().await?;
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
            let (origin, jobs) = listeners(&controller, state_dir, zero, zero, zero, zero).await?;
            check_cases(&http, &origin, &controller, entries, false).await?;
            let previous = controller.current();
            let old_client = connect(&http, &origin, false).await?;
            let (next, next_snapshot) = generation(entries, true, Some(&previous), cuda)?;
            println!(
                "{}",
                json!({"phase":"update","changed_rows":next_snapshot.changed_rows(&snapshot)})
            );
            next.verify_full_hint(&next_snapshot)?;
            controller.activate(next)?;
            drop(previous);
            check_cases(&http, &origin, &controller, entries, true).await?;
            controller.reaffirm_fixture(controller.current().manifest.rows_digest)?;
            let failure_client = connect(&http, &origin, true).await?;
            let failure_query =
                failure_client.prepare(&fixture::txid(u64::MAX - 1), None, status::now_ms())?;
            jobs[0].abort();
            tokio::time::sleep(Duration::from_millis(50)).await;
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
                json!({"phase":"validation_complete","passed":true,"cuda":cuda,"wallet_changes":false,"incremental_granularity":"2048-row units; full packing rebuild","production_qualified":false})
            );
        }
    }
    Ok(())
}
