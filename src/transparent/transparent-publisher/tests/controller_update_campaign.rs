//! Explicit controller-update campaign: the real publish-controller process,
//! real TLS termination and two independent packaged provider processes.
//!
//! This is qualification-shaped evidence, not a fast correctness case: it is
//! `#[ignore]`d and run by `tools/transparent/scripts/https-update-campaign.py`,
//! which supplies the packaged provider binary and an output root. Every query,
//! stage timing and failure is appended to that root as raw JSON lines; the
//! test never summarizes a pass it did not observe. The oracle decodes rows
//! from the controller's own published tables, independent of either provider.
//! Fork/invalidation, replay and write-failure fencing stay owned by the
//! in-process controller distribution/manager tests rather than repeated here.
use std::{
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::io::{AsyncBufReadExt, BufReader};
use transparent_events::TransparentEvent;
use transparent_filter::{BlockHash, ScriptBytes};
use transparent_publisher::history::publication::{self, PublishOptions};
use transparent_shared::history::shardset::{ShardSet, TableRef};
use transparent_shared::journal::events::EventStore;

const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;
const RECENT_FROM: u64 = 2;

fn hash(h: u64) -> BlockHash {
    if h == 0 {
        BlockHash::from_display_hex(GENESIS).unwrap()
    } else {
        BlockHash::from_internal_bytes([h as u8; 32])
    }
}
fn block(journal: &mut EventStore, h: u64) {
    let script = vec![
        0x76, 0xa9, 0x14, h as u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x88,
        0xac,
    ];
    let events = if h >= RECENT_FROM {
        vec![(
            ScriptBytes::new(script),
            TransparentEvent::Receive(transparent_events::ReceiveEvent {
                metadata: Some(transparent_events::TransactionMetadata {
                    fee: transparent_events::FeeState::Exact(1000),
                    transparent_input_count: 1,
                    has_shielded_components: false,
                }),
                height: h as u32,
                txid: transparent_events::Txid([h as u8 + 5; 32]),
                transaction_index: 0,
                output_index: 0,
                value: 50_000 + h,
                coinbase: false,
            }),
        )]
    } else {
        Vec::new()
    };
    journal.append_block(h, hash(h), &events).unwrap();
}

/// Raw, append-only campaign observations. Nothing here is a summary.
struct Ledger {
    file: Mutex<std::fs::File>,
    start: Instant,
}
impl Ledger {
    fn record(&self, kind: &str, mut value: serde_json::Value) {
        value["kind"] = kind.into();
        value["t_ms"] = (self.start.elapsed().as_secs_f64() * 1e3).into();
        let mut file = self.file.lock().unwrap();
        writeln!(file, "{value}").unwrap();
        file.flush().unwrap();
    }
}

async fn rpc_server(tip: Arc<AtomicU64>, ledger: Arc<Ledger>) -> String {
    let app = axum::Router::new().route(
        "/",
        axum::routing::post(move |axum::Json(r): axum::Json<serde_json::Value>| {
            let tip = tip.load(Ordering::Acquire);
            let result = match r["method"].as_str() {
                Some("getblockcount") => Some(serde_json::json!(tip)),
                Some("getblockchaininfo") => Some(serde_json::json!({"blocks": tip})),
                Some("getblockhash") => r["params"][0]
                    .as_u64()
                    .filter(|h| *h <= tip)
                    .map(|h| hash(h).to_display_hex().into()),
                _ => None,
            };
            if result.is_none() && r["method"] != "getblockhash" {
                ledger.record("rpc_unsupported", r.clone());
            }
            async move {
                axum::Json(match result {
                    Some(v) => serde_json::json!({"id":r["id"],"result":v,"error":null}),
                    None => {
                        serde_json::json!({"id":r["id"],"result":null,"error":{"code":-8,"message":"unavailable"}})
                    }
                })
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn free_port() -> std::net::SocketAddr {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap()
}

async fn tls(root: &Path, upstream: &str) -> (String, tokio::process::Child) {
    let control = root.join("tls-control");
    std::fs::write(&control, "").unwrap();
    let mut child = tokio::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/distribution_tls.py"
        ))
        .arg(upstream.strip_prefix("http://").unwrap())
        .arg(root.join("ca.pem"))
        .arg(root.join("key.pem"))
        .arg(&control)
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .await
        .unwrap();
    let port: u16 = line.trim().parse().unwrap();
    (format!("https://localhost:{port}"), child)
}

struct Provider {
    name: &'static str,
    origin: String,
    args: Vec<String>,
    cwd: PathBuf,
    child: Option<tokio::process::Child>,
}
impl Provider {
    async fn start(&mut self, binary: &Path, ledger: &Ledger) {
        let started = Instant::now();
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.cwd.join("provider.log"))
            .unwrap();
        let mut child = tokio::process::Command::new(binary)
            .args(&self.args)
            .current_dir(&self.cwd)
            .kill_on_drop(true)
            .stderr(log)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if reqwest::get(format!("{}/v1/ready", self.origin))
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                break;
            }
            if let Some(status) = child.try_wait().unwrap() {
                ledger.record(
                    "provider_exit",
                    serde_json::json!({"provider":self.name,"status":status.to_string()}),
                );
                panic!(
                    "{} exited: {}",
                    self.name,
                    std::fs::read_to_string(self.cwd.join("provider.log")).unwrap()
                );
            }
            assert!(Instant::now() < deadline, "{} never became ready", self.name);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        ledger.record(
            "provider_ready",
            serde_json::json!({"provider":self.name,"pid":child.id(),
                "ready_ms":started.elapsed().as_secs_f64()*1e3}),
        );
        self.child = Some(child);
    }
    async fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().await.unwrap();
            child.wait().await.unwrap();
        }
    }
}

async fn status(origin: &str) -> serde_json::Value {
    match reqwest::get(format!("{origin}/v1/provider-status")).await {
        Ok(r) => r.json().await.unwrap_or_default(),
        Err(_) => serde_json::Value::Null,
    }
}
async fn current(publisher: &str, client: &reqwest::Client) -> serde_json::Value {
    // A starting or withdrawn controller refuses; that is not yet an offer.
    match client.get(format!("{publisher}/provider/v1/current")).send().await {
        Ok(r) if r.status().is_success() => r.json().await.unwrap_or_default(),
        _ => serde_json::Value::Null,
    }
}

/// The controller's own active map and tables: the provider-independent oracle.
fn controller_set(publications: &Path) -> ShardSet {
    let active: serde_json::Value =
        serde_json::from_slice(&std::fs::read(publications.join("active.json")).unwrap()).unwrap();
    ShardSet::open(Path::new(active["directory"].as_str().unwrap()), 0).unwrap()
}

/// One encrypted directory query, decoded against the oracle's exact row.
async fn row_query(origin: &str, set: &ShardSet, id: u64) -> Result<(), String> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    let shard = set.get(id).ok_or("oracle shard missing")?;
    let source = shard
        .segment(TableRef::Directory, 0)
        .unwrap()
        .load()
        .unwrap();
    let width = shard.geometry.directory_row_bytes;
    let row = source
        .chunks_exact(width)
        .position(|r| r.iter().any(|b| *b != 0))
        .ok_or("no occupied row")?;
    let profile = transparent_native::TableProfile::new(
        transparent_shard::manifest::SCHEMA,
        shard.geometry.name,
        "directory",
        shard.geometry.directory_rows,
        width as u32,
    )
    .unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let setup = client
        .get(format!(
            "{origin}/v2/shards/{id}/revisions/{}/setup/directory/0",
            shard.digest
        ))
        .send()
        .await
        .map_err(|e| format!("setup transport: {e}"))?;
    if !setup.status().is_success() {
        return Err(format!("setup status {}", setup.status()));
    }
    let setup: serde_json::Value = setup.json().await.map_err(|e| e.to_string())?;
    let params = STANDARD
        .decode(setup["public_params"].as_str().ok_or("no params")?)
        .unwrap();
    let (secret, upload) = profile.prepare(row).unwrap();
    let binding = transparent_shard::manifest::query_binding(&shard.digest, "directory");
    let mut body = binding.to_vec();
    body.extend(upload);
    let response = client
        .post(format!(
            "{origin}/v2/shards/{id}/revisions/{}/query/directory",
            shard.digest
        ))
        .body(body)
        .send()
        .await
        .map_err(|e| format!("query transport: {e}"))?;
    if response.status() != 200 {
        return Err(format!("query status {}", response.status()));
    }
    let answer = response.bytes().await.map_err(|e| e.to_string())?;
    if answer[..8] != binding
        || profile.decode(&secret, &params, &answer[16..]).unwrap()
            != source[row * width..(row + 1) * width]
    {
        return Err("decoded row differs from controller table".into());
    }
    Ok(())
}

/// Wait until both the controller and every named provider report `map`.
async fn converge(
    ledger: &Ledger,
    phase: &str,
    publisher: &str,
    https: &reqwest::Client,
    providers: &[&Provider],
    publications: &Path,
    timeout: Duration,
) -> serde_json::Value {
    let started = Instant::now();
    let mut offered = None;
    let mut seen = vec![None; providers.len()];
    while started.elapsed() < timeout {
        if offered.is_none() {
            let record = current(publisher, https).await;
            if record["eligibility"] == "canonical" && publications.join("active.json").exists() {
                let map = controller_set(publications).map_digest;
                ledger.record(
                    "controller_offered",
                    serde_json::json!({"phase":phase,"revision":record["revision"],
                        "inventory_sha256":record["inventory_sha256"],"map_sha256":map,
                        "ms":started.elapsed().as_secs_f64()*1e3}),
                );
                offered = Some((record, map));
            }
        }
        if let Some((_, map)) = &offered {
            for (i, provider) in providers.iter().enumerate() {
                if seen[i].is_none() {
                    let observed = status(&provider.origin).await;
                    if observed["verified_publication"]["map_sha256"] == *map.as_str() {
                        ledger.record(
                            "provider_activated",
                            serde_json::json!({"phase":phase,"provider":provider.name,
                                "ms":started.elapsed().as_secs_f64()*1e3,"status":observed}),
                        );
                        seen[i] = Some(());
                    }
                }
            }
            if seen.iter().all(Option::is_some) {
                return offered.unwrap().0;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    ledger.record(
        "convergence_timeout",
        serde_json::json!({"phase":phase,"offered":offered.is_some(),
            "activated":seen.iter().filter(|s| s.is_some()).count()}),
    );
    panic!("{phase}: convergence exceeded {timeout:?}");
}

/// Encrypted queries against every provider until `stop`, each one recorded.
async fn query_load(
    ledger: Arc<Ledger>,
    phase: String,
    origins: Vec<(&'static str, String)>,
    publications: PathBuf,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> (u64, u64) {
    let (mut ok, mut failed) = (0, 0);
    while !stop.load(Ordering::Acquire) {
        // The oracle set is re-read so that queries after activation bind the
        // new revision; a query naming a just-superseded revision may still be
        // answered from retained revisions, which is recorded either way.
        let set = controller_set(&publications);
        for (name, origin) in &origins {
            let started = Instant::now();
            let outcome = row_query(origin, &set, 1).await;
            ledger.record(
                "query",
                serde_json::json!({"phase":phase,"provider":name,"map_sha256":set.map_digest,
                    "ms":started.elapsed().as_secs_f64()*1e3,"ok":outcome.is_ok(),
                    "error":outcome.as_ref().err()}),
            );
            if outcome.is_ok() {
                ok += 1
            } else {
                failed += 1
            }
        }
    }
    (ok, failed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit campaign; run tools/transparent/scripts/https-update-campaign.py"]
async fn controller_updates_reach_independent_https_providers_under_queries() {
    let binary = PathBuf::from(
        std::env::var_os("WALLET_PIR_PROVIDER_BIN")
            .expect("WALLET_PIR_PROVIDER_BIN names the packaged transparent-query-server"),
    );
    let out = PathBuf::from(
        std::env::var_os("WALLET_PIR_CAMPAIGN_OUT").expect("WALLET_PIR_CAMPAIGN_OUT names the run"),
    );
    let updates: u64 = std::env::var("WALLET_PIR_CAMPAIGN_UPDATES")
        .ok()
        .map_or(3, |v| v.parse().unwrap());
    let root = out.join("work");
    std::fs::create_dir_all(&root).unwrap();
    let ledger = Arc::new(Ledger {
        file: Mutex::new(
            std::fs::OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(out.join("observations.jsonl"))
                .unwrap(),
        ),
        start: Instant::now(),
    });

    // Journal owned by this "ingest" writer; the controller follows it.
    let journal_dir = root.join("journal");
    let mut journal = EventStore::open(&journal_dir, GENESIS, 0).unwrap();
    let mut tip_height = 4;
    for h in 0..=tip_height {
        block(&mut journal, h);
    }
    journal.commit().unwrap();
    let initial = root.join("initial");
    publication::publish(
        &PublishOptions {
            data_dir: journal_dir.clone(),
            output: initial.clone(),
            previous: None,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: Some("decoupled-32k-32k".into()),
            recent_from: Some(RECENT_FROM),
            zakura_rpc_url: String::new(),
            zakura_cookie: root.join("unused"),
            through: None,
            record: None,
            source_sha: None,
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: transparent_filter::RANGE_PROFILE.to_string(),
        },
        &journal,
        BlockHash::from_internal_bytes([0; 32]),
    )
    .unwrap();

    let tip = Arc::new(AtomicU64::new(tip_height));
    let rpc = rpc_server(tip.clone(), ledger.clone()).await;
    let cookie = root.join("node-cookie");
    std::fs::write(&cookie, "fixture:fixture").unwrap();
    let hook = root.join("fleet.py");
    std::fs::write(
        &hook,
        "#!/usr/bin/env python3\nimport json,sys\nr=json.load(sys.stdin)\n\
         open(__file__+'.requests','a').write(json.dumps(r)+'\\n')\n\
         print(json.dumps({'ok':True,'upstreams':[]}))\n",
    )
    .unwrap();
    std::fs::set_permissions(
        &hook,
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .unwrap();
    let publications = root.join("publications");
    let listen = free_port();
    let config = serde_json::json!({
        "data_dir": journal_dir, "publication_root": publications, "initial_publication": initial,
        "recent_from": RECENT_FROM, "recent_geometry": "decoupled-2k-8k",
        "archive_geometry": "decoupled-32k-32k", "rpc_url": rpc, "rpc_cookie": cookie,
        "fleet_command": hook, "fleet_config": root.join("fleet-config.json"),
        "listen": listen.to_string(), "source_sha": std::env::var("WALLET_PIR_SOURCE_SHA").unwrap_or_default(),
        "follow_journal": true, "ship_runtimes": true, "distribute_prepared": true,
    });
    std::fs::write(out.join("controller-config.json"), config.to_string()).unwrap();
    let controller_log = std::fs::File::create(out.join("controller.log")).unwrap();
    let mut controller = tokio::process::Command::new(env!(
        "CARGO_BIN_EXE_transparent-publish-controller"
    ))
    .arg("--config")
    .arg(out.join("controller-config.json"))
    .kill_on_drop(true)
    .stdout(Stdio::null())
    .stderr(controller_log)
    .spawn()
    .unwrap();

    let openssl = std::process::Command::new("openssl")
        .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1"])
        .args(["-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost"])
        .args(["-addext", "basicConstraints=critical,CA:FALSE", "-keyout"])
        .arg(root.join("key.pem"))
        .arg("-out")
        .arg(root.join("ca.pem"))
        .output()
        .unwrap();
    assert!(openssl.status.success());
    let (publisher, mut publisher_tls) = tls(&root, &format!("http://{listen}")).await;
    let ca = reqwest::Certificate::from_pem(&std::fs::read(root.join("ca.pem")).unwrap()).unwrap();
    let https = reqwest::Client::builder()
        .add_root_certificate(ca)
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    // Bootstrap: the controller's own first revision, then two cold providers.
    let first = converge(&ledger, "controller-bootstrap", &publisher, &https, &[], &publications,
        Duration::from_secs(120)).await;
    let mut providers = Vec::new();
    for name in ["provider-a", "provider-b"] {
        let cwd = root.join(name);
        std::fs::create_dir(&cwd).unwrap();
        let addr = free_port();
        let args = [
            "--publisher", &publisher, "--publisher-network", "main", "--publisher-genesis", GENESIS,
            "--publisher-ca", root.join("ca.pem").to_str().unwrap(),
            "--listen", &addr.to_string(), "--shard-dir", cwd.join("state").to_str().unwrap(),
            "--cache-bytes", &(1u64 << 30).to_string(),
        ]
        .map(String::from)
        .to_vec();
        providers.push(Provider { name, origin: format!("http://{addr}"), args, cwd, child: None });
    }
    let started = Instant::now();
    let (a, b) = providers.split_at_mut(1);
    tokio::join!(a[0].start(&binary, &ledger), b[0].start(&binary, &ledger));
    ledger.record("phase", serde_json::json!({"phase":"bootstrap","revision":first["revision"],
        "ms":started.elapsed().as_secs_f64()*1e3}));
    let mut revision = first["revision"].as_u64().unwrap();

    let origins: Vec<_> = providers.iter().map(|p| (p.name, p.origin.clone())).collect();
    let mut failed_queries = 0;
    // Updates under continuous encrypted queries against both providers.
    for update in 0..updates {
        let phase = format!("update-{update}");
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let load = tokio::spawn(query_load(ledger.clone(), phase.clone(), origins.clone(),
            publications.clone(), stop.clone()));
        tip_height += 1;
        block(&mut journal, tip_height);
        journal.commit().unwrap();
        tip.store(tip_height, Ordering::Release);
        ledger.record("journal_extended", serde_json::json!({"phase":phase,"height":tip_height}));
        let record = loop {
            let record = converge(&ledger, &phase, &publisher, &https,
                &providers.iter().collect::<Vec<_>>(), &publications, Duration::from_secs(120)).await;
            if record["revision"].as_u64().unwrap() > revision
                && record["anchor_height"].as_u64() == Some(tip_height)
            {
                break record;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        revision = record["revision"].as_u64().unwrap();
        stop.store(true, Ordering::Release);
        let (ok, failed) = load.await.unwrap();
        ledger.record("phase", serde_json::json!({"phase":phase,"revision":revision,
            "queries_ok":ok,"queries_failed":failed}));
        assert!(ok > 0, "{phase}: no successful query during update");
        failed_queries += failed;
    }

    // Unchanged source: no new revision, conditional polls keep both serving.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let unchanged = current(&publisher, &https).await;
    ledger.record("phase", serde_json::json!({"phase":"unchanged","revision":unchanged["revision"]}));
    assert_eq!(unchanged["revision"].as_u64(), Some(revision), "unchanged source advanced revision");
    let set = controller_set(&publications);
    for p in &providers {
        row_query(&p.origin, &set, 1).await.unwrap();
    }

    // Missed revisions: provider-b is down across two publications.
    providers[1].stop().await;
    for _ in 0..2 {
        tip_height += 1;
        block(&mut journal, tip_height);
        journal.commit().unwrap();
        tip.store(tip_height, Ordering::Release);
        loop {
            let record = converge(&ledger, "missed", &publisher, &https, &[&providers[0]],
                &publications, Duration::from_secs(120)).await;
            if record["anchor_height"].as_u64() == Some(tip_height) {
                revision = record["revision"].as_u64().unwrap();
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    providers[1].start(&binary, &ledger).await;
    converge(&ledger, "missed-recovery", &publisher, &https, &providers.iter().collect::<Vec<_>>(),
        &publications, Duration::from_secs(60)).await;
    ledger.record("phase", serde_json::json!({"phase":"missed-recovery","revision":revision}));

    // Source outage: both keep serving the active view, then recover.
    publisher_tls.kill().await.unwrap();
    publisher_tls.wait().await.unwrap();
    for p in &providers {
        let deadline = Instant::now() + Duration::from_secs(30);
        while status(&p.origin).await["synchronization"]["state"] != "refused" {
            assert!(Instant::now() < deadline, "{} never observed the outage", p.name);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        row_query(&p.origin, &controller_set(&publications), 1).await.unwrap();
    }
    ledger.record("phase", serde_json::json!({"phase":"source-outage","serving":true}));

    stop_all(&mut providers).await;
    controller.kill().await.unwrap();
    controller.wait().await.unwrap();
    ledger.record("campaign_end", serde_json::json!({"revision":revision,"failed_queries":failed_queries}));
    // Retryable refusals during activation are recorded, never hidden; any
    // wrong decoded answer would have been recorded as a failed query too.
    let wrong = std::fs::read_to_string(out.join("observations.jsonl"))
        .unwrap()
        .lines()
        .filter(|l| l.contains("decoded row differs"))
        .count();
    assert_eq!(wrong, 0, "a provider returned a wrong decoded row");
}

async fn stop_all(providers: &mut [Provider]) {
    for p in providers {
        p.stop().await;
    }
}
