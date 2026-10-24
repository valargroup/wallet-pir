//! Explicit local campaign for the actual unified publisher and two independent
//! `pir-query-server` providers, each serving history and display in one process.
//! Runs only through unified-http-sync-campaign.py; all observations are retained.
use std::{
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::io::{AsyncBufReadExt, BufReader};
use transparent_publisher::{
    history::publication::{self, PublishOptions},
    tx_detail::{
        offline::synthetic_sources,
        publisher::{self, DisplayRoot},
    },
};
use transparent_shared::journal::{display_journal::implied_events, events::EventStore};
use zakura_pir_protocol::transparent::{
    events::{FeeState, ReceiveEvent, TransactionMetadata, TransparentEvent, Txid},
    filter::{BlockHash, ScriptBytes},
};
use zakura_pir_publisher::transparent::{
    display::{DisplaySealParams, TXID_2K8},
    filter,
    seal::DecoupledPolicy,
};

#[path = "../../../src/pir-query-server/tests/transparent/support/display_world.rs"]
mod display_world;

const GENESIS: &str = filter::MAINNET_GENESIS_DISPLAY;
fn hash(height: u64) -> BlockHash {
    if height == 0 {
        BlockHash::from_display_hex(GENESIS).unwrap()
    } else {
        BlockHash::from_internal_bytes([height as u8; 32])
    }
}
fn wallet() -> ScriptBytes {
    let mut s = vec![0x76, 0xa9, 0x14];
    s.extend([0xaa; 20]);
    s.extend([0x88, 0xac]);
    ScriptBytes::new(s)
}
fn append(history: &Path, display: &Path, height: u64) {
    let mut h = EventStore::open(history, GENESIS, 0).unwrap();
    let mut events = if height == 0 {
        vec![]
    } else {
        vec![(
            wallet(),
            TransparentEvent::Receive(ReceiveEvent {
                metadata: Some(TransactionMetadata {
                    fee: FeeState::Exact(1000),
                    transparent_input_count: 1,
                    has_shielded_components: false,
                }),
                height: height as u32,
                txid: Txid([height as u8 + 5; 32]),
                transaction_index: 0,
                output_index: 0,
                value: 50_000 + height,
                coinbase: false,
            }),
        )]
    };
    if height == 1 {
        let mut other = events[0].clone();
        other.0 = ScriptBytes::new(vec![0x51]);
        if let TransparentEvent::Receive(event) = &mut other.1 {
            event.output_index = 1;
        }
        events.push(other);
    }
    h.append_block(height, hash(height), &events).unwrap();
    h.commit().unwrap();
    drop(h);
    let mut d = EventStore::open(display, GENESIS, 0).unwrap();
    let sources = if height == 0 {
        vec![]
    } else {
        synthetic_sources(2, 0x5eed + height)
    };
    let events = implied_events(height, &sources).unwrap();
    d.append_block_with_display(height, hash(height), &events, &sources)
        .unwrap();
    d.commit().unwrap();
}
fn address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}
struct Process {
    name: String,
    command: PathBuf,
    args: Vec<String>,
    log: PathBuf,
    child: Option<tokio::process::Child>,
}
impl Process {
    fn new(name: &str, command: PathBuf, args: Vec<String>, out: &Path) -> Self {
        Self {
            name: name.into(),
            command,
            args,
            log: out.join(format!("{name}.log")),
            child: None,
        }
    }
    fn start(&mut self) {
        assert!(self.child.is_none());
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .unwrap();
        self.child = Some(
            tokio::process::Command::new(&self.command)
                .args(&self.args)
                // Retained logs keep info-level diagnostics unless overridden.
                .env(
                    "RUST_LOG",
                    std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
                )
                .stdout(Stdio::from(log.try_clone().unwrap()))
                .stderr(Stdio::from(log))
                .kill_on_drop(true)
                .spawn()
                .unwrap(),
        );
    }
    async fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if let Some(pid) = child.id() {
                let _ = tokio::process::Command::new("kill")
                    .args(["-TERM", &pid.to_string()])
                    .status()
                    .await;
            }
            if tokio::time::timeout(Duration::from_secs(30), child.wait())
                .await
                .is_err()
            {
                child.kill().await.unwrap();
                child.wait().await.unwrap();
            }
        }
    }
    fn assert_running(&mut self) {
        if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
            panic!(
                "{} exited {status}: {}",
                self.name,
                std::fs::read_to_string(&self.log).unwrap()
            );
        }
    }
}
struct Ledger(Mutex<std::fs::File>);
impl Ledger {
    fn record(&self, kind: &str, value: serde_json::Value) {
        let mut file = self.0.lock().unwrap();
        writeln!(file,"{}",serde_json::json!({"kind":kind,"at_unix":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64(),"value":value})).unwrap();
        file.flush().unwrap();
    }
}
async fn tls(
    label: &str,
    upstream: &str,
    discovery: &str,
    cert: &Path,
    key: &Path,
    out: &Path,
) -> (String, tokio::process::Child) {
    let control = out.join(format!("{label}-fault"));
    std::fs::write(&control, "").unwrap();
    let mut child = tokio::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../src/pir-publisher/tests/support/distribution_tls.py"
        ))
        .args([
            upstream,
            cert.to_str().unwrap(),
            key.to_str().unwrap(),
            control.to_str().unwrap(),
        ])
        // Discovery is served by the publisher's management listener; the edge
        // proxies that exact path there.
        .env("DISCOVERY_UPSTREAM", discovery)
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .await
        .unwrap();
    let port: u16 = line
        .trim()
        .parse()
        .expect("TLS fixture must report its bound port");
    (format!("https://localhost:{port}"), child)
}
async fn json(origin: &str, path: &str) -> Option<serde_json::Value> {
    reqwest::Client::new()
        .get(format!("{origin}{path}"))
        .timeout(Duration::from_secs(3))
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()
}
async fn wait_through(process: &mut Process, origin: &str, path: &str, height: u64) {
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        process.assert_running();
        if json(origin, path)
            .await
            .is_some_and(|s| s["covered_through"] == height)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} did not reach {height}: {}",
            process.name,
            std::fs::read_to_string(&process.log).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
/// One `pir-query-server` provider: a history and a display section in a
/// single host process, each following its own HTTPS publisher origin.
struct Provider {
    process: Process,
    management: String,
}
#[allow(clippy::too_many_arguments)]
fn provider(
    name: &str,
    history_publisher: &str,
    display_publisher: &str,
    history_listen: &str,
    display_listen: &str,
    cert: &Path,
    state: &Path,
    out: &Path,
) -> Provider {
    let management = address();
    let common = serde_json::json!({
        "publisher_network": "main",
        "publisher_genesis": GENESIS,
        "publisher_ca": cert,
        "cache_bytes": 16u64 << 30,
        "bootstrap_max_bytes": 16u64 << 30,
    });
    let mut history = common.clone();
    history["publisher"] = history_publisher.into();
    history["listen"] = history_listen.into();
    history["shard_dir"] = state.join("history").display().to_string().into();
    history["full_holdings"] = true.into();
    let mut display = common;
    display["publisher"] = display_publisher.into();
    display["listen"] = display_listen.into();
    display["role"] = "combined".into();
    display["state_dir"] = state.join("display").display().to_string().into();
    let config = out.join(format!("{name}.json"));
    std::fs::write(
        &config,
        serde_json::to_vec_pretty(&serde_json::json!({
            "management_listen": management,
            "transparent_history": {"enabled": true, "settings": history},
            "transaction_details": {"enabled": true, "settings": display},
        }))
        .unwrap(),
    )
    .unwrap();
    let bin = std::env::var("WALLET_PIR_QUERY_SERVER_BIN").unwrap();
    Provider {
        process: Process::new(
            name,
            bin.into(),
            vec!["--config".into(), config.display().to_string()],
            out,
        ),
        management,
    }
}
impl Provider {
    async fn wait_through(&mut self, history: &str, display: &str, height: u64) {
        wait_through(&mut self.process, history, "/v2/shards/init", height).await;
        wait_through(&mut self.process, display, "/v2/txid/init", height).await;
    }
    /// Without a reachable publisher the host refuses to start: service
    /// discovery fails before any listener is bound, and the process exits.
    async fn wait_refused(&mut self) {
        let mut child = self.process.child.take().unwrap();
        let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
            .await
            .expect("host started without a reachable publisher")
            .unwrap();
        let log = std::fs::read_to_string(&self.process.log).unwrap();
        assert!(!status.success(), "{}: {log}", self.process.name);
        assert!(log.contains("service discovery failed"), "{log}");
        assert!(
            json(&format!("http://{}", self.management), "/health")
                .await
                .is_none(),
            "management served after a refused start"
        );
    }
}
async fn recovery(client: &Path, origin: &str, journal: &Path, ledger: &Ledger) {
    let started = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new(client)
            .args([origin, &hex::encode(wallet().as_slice()), "0"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        result.status.success(),
        "released client refused: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let recovered: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    let through = recovered["covered_through"].as_u64().unwrap();
    let source = EventStore::open_existing(journal).unwrap();
    let mut expected = Vec::new();
    for height in 0..=through {
        for (script, event) in source.events_at(height).unwrap().unwrap() {
            if script == wallet() {
                if let TransparentEvent::Receive(e) = event {
                    expected.push(serde_json::json!({"txid":e.txid.to_display_hex(),"index":e.output_index,"value":e.value,"script":hex::encode(script.as_slice()),"height":e.height,"coinbase":e.coinbase}));
                }
            }
        }
    }
    let mut actual = recovered["utxos"].as_array().unwrap().clone();
    expected.sort_by_key(|v| v.to_string());
    actual.sort_by_key(|v| v.to_string());
    assert_eq!(
        actual, expected,
        "released client ledger differs from committed journal events"
    );
    assert_eq!(recovered["unresolved"], 0);
    assert_eq!(recovered["spends"].as_array().unwrap().len(), 0);
    assert_eq!(
        recovered["history"].as_array().unwrap().len(),
        through as usize
    );
    ledger.record("exact_history_recovery",serde_json::json!({"origin":origin,"through":through,"utxos":actual.len(),"seconds":started.elapsed().as_secs_f64()}));
}
async fn display_lookup(origin: &str, journal: &Path, height: u64, ledger: &Ledger) {
    let source = EventStore::open_existing(journal).unwrap();
    let record = source.display_at(height).unwrap().remove(0);
    let mut wallet = display_world::Wallet::at(origin);
    let (_, transcript) = wallet.found(&record, height);
    assert!(transcript
        .iter()
        .any(|sent| sent.method == zakura_pir_client::transparent::txid_client::Method::Post));
    let frozen = std::env::var_os("WALLET_PIR_FROZEN_CLIENT").unwrap();
    let result = tokio::process::Command::new(frozen)
        .args([
            origin,
            "--display",
            &hex::encode(record.txid.0),
            &height.to_string(),
        ])
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "released display client refused: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let original: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(
        original["entry_hex"],
        hex::encode(record.entry.encode().unwrap()),
        "released client entry differs from committed journal source"
    );
    ledger.record(
        "exact_display_lookup",
        serde_json::json!({"origin":origin,"height":height,"txid":record.txid.to_display_hex()}),
    );
}
async fn no_builds(origins: &[String], out: &Path) {
    for (index, origin) in origins.iter().enumerate() {
        let text = reqwest::get(format!("{origin}/metrics"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        std::fs::write(out.join(format!("origin-{index}-metrics.txt")), &text).unwrap();
        let value = text
            .lines()
            .find(|line| line.starts_with("transparent_shard_builds_total"))
            .and_then(|line| line.split_whitespace().last())
            .expect("runtime build counter is required");
        assert_eq!(value, "0", "query servers may not build native runtimes");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "explicit real-process local qualification; run unified-http-sync-campaign.py"]
async fn unified_publisher_drives_independent_full_history_and_display_consumers() {
    let out = PathBuf::from(
        std::env::var_os("WALLET_PIR_CAMPAIGN_OUT")
            .expect("explicit retained output directory required"),
    );
    std::fs::create_dir_all(&out).unwrap();
    let ledger = Ledger(Mutex::new(
        std::fs::File::create(out.join("observations.jsonl")).unwrap(),
    ));
    let history = out.join("history-journal");
    let display = out.join("display-journal");
    for height in 0..=8 {
        append(&history, &display, height);
    }
    let history_bootstrap = out.join("history-bootstrap");
    let store = EventStore::open_existing(&history).unwrap();
    publication::publish_with(
        &PublishOptions {
            data_dir: history.clone(),
            output: history_bootstrap.clone(),
            previous: None,
            recent_geometry: "decoupled-2k-8k".into(),
            archive_geometry: Some("decoupled-32k-32k".into()),
            recent_from: Some(2),
            zakura_rpc_url: String::new(),
            zakura_cookie: out.join("unused"),
            through: None,
            record: None,
            source_sha: None,
            directory_choice: publication::DirectoryChoice::Off,
            range_profile: filter::RANGE_PROFILE.into(),
        },
        &store,
        hash(0),
        &DecoupledPolicy::for_geometry,
    )
    .unwrap();
    drop(store);
    let display_root = out.join("display-root");
    let store = EventStore::open_existing(&display).unwrap();
    publisher::bootstrap(
        &store,
        &display_root,
        &DisplayRoot {
            geometry: TXID_2K8.name.into(),
            seal: DisplaySealParams {
                n_archive: 1,
                n_recent: 1,
                archive_target: 4,
                recent_window: 3,
                reorg_margin: 1,
            },
            max_archive_shards: 64,
            network: "main".into(),
            genesis_hash: GENESIS.into(),
            start_height: 1,
            base_parent: hash(0).to_display_hex(),
        },
        8,
    )
    .unwrap();
    drop(store);
    let tip = Arc::new(AtomicU64::new(8));
    let canonical = Arc::new(AtomicBool::new(true));
    let state = (tip.clone(), canonical.clone());
    let node = axum::Router::new().route(
        "/",
        axum::routing::post(move |axum::Json(request): axum::Json<serde_json::Value>| {
            let (tip, canonical) = state.clone();
            async move {
                let result = match request["method"].as_str().unwrap() {
                    "getblockcount" => serde_json::json!(tip.load(Ordering::Acquire)),
                    "getblockchaininfo" => {
                        serde_json::json!({"blocks":tip.load(Ordering::Acquire)})
                    }
                    "getblockhash" => {
                        let height = request["params"][0].as_u64().unwrap();
                        serde_json::json!(if height == 0 || canonical.load(Ordering::Acquire) {
                            hash(height).to_display_hex()
                        } else {
                            "77".repeat(32)
                        })
                    }
                    other => panic!("campaign must not fetch historical chain data: {other}"),
                };
                axum::Json(serde_json::json!({"id":request["id"],"result":result,"error":null}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_origin = format!("http://{}", listener.local_addr().unwrap());
    let node_task = tokio::spawn(async move {
        axum::serve(listener, node).await.unwrap();
    });
    let cookie = out.join("node-cookie");
    std::fs::write(&cookie, "fixture:fixture").unwrap();
    let history_listen = address();
    let display_listen = address();
    let management = address();
    let configuration = out.join("publisher.json");
    let settings = serde_json::json!({
        "management_listen": management,
        "chain": {"rpc_url": node_origin, "rpc_cookie": cookie, "poll_ms": 100},
        "transparent_history": {"enabled": true, "settings": {
            "data_dir": history,
            "publication_root": out.join("history-publisher"),
            "initial_publication": history_bootstrap,
            "recent_from": 2,
            "recent_geometry": "decoupled-2k-8k",
            "archive_geometry": "decoupled-32k-32k",
            "listen": history_listen,
            "source_sha": std::env::var("WALLET_PIR_SOURCE_SHA").unwrap(),
            "follow_journal": true
        }},
        "transaction_details": {"enabled": true, "settings": {
            "root": display_root,
            "journal": display,
            "status_listen": display_listen
        }}
    });
    std::fs::write(
        &configuration,
        serde_json::to_vec_pretty(&settings).unwrap(),
    )
    .unwrap();
    let cert = out.join("ca.pem");
    let key = out.join("key.pem");
    assert!(std::process::Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-keyout"
        ])
        .arg(&key)
        .arg("-out")
        .arg(&cert)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success());
    let (history_origin, _history_tls) =
        tls("history", &history_listen, &management, &cert, &key, &out).await;
    let (display_origin, _display_tls) =
        tls("display", &display_listen, &management, &cert, &key, &out).await;
    let mut publisher = Process::new(
        "unified-publisher",
        std::env::var_os("WALLET_PIR_PUBLISHER_BIN").unwrap().into(),
        vec!["--config".into(), configuration.display().to_string()],
        &out,
    );
    publisher.start();
    let addresses: Vec<_> = (0..4).map(|_| address()).collect();
    let origins: Vec<_> = addresses.iter().map(|a| format!("http://{a}")).collect();
    // Two independent providers; provider k serves history at origins[2k]
    // and display at origins[2k + 1] from one host process and state root.
    let mut providers: Vec<_> = (0..2)
        .map(|k| {
            provider(
                &format!("provider-{k}"),
                &history_origin,
                &display_origin,
                &addresses[2 * k],
                &addresses[2 * k + 1],
                &cert,
                &out.join(format!("provider-{k}")),
                &out,
            )
        })
        .collect();
    let frozen = PathBuf::from(std::env::var_os("WALLET_PIR_FROZEN_CLIENT").unwrap());
    // Consumers start after the publisher finalized both products, with no prior worker registrations.
    let https = reqwest::Client::builder()
        .add_root_certificate(
            reqwest::Certificate::from_pem(&std::fs::read(&cert).unwrap()).unwrap(),
        )
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        publisher.assert_running();
        let mut ready = true;
        for (origin, prefix) in [
            (&history_origin, "/provider/v1/current"),
            (&display_origin, "/provider/txid/v1/current"),
        ] {
            ready &= https
                .get(format!("{origin}{prefix}"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success());
        }
        if ready {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "publisher did not finalize without consumers"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    ledger.record(
        "publisher_without_consumers",
        serde_json::json!({"height":8}),
    );
    for (k, provider) in providers.iter_mut().enumerate() {
        provider.process.start();
        provider
            .wait_through(&origins[2 * k], &origins[2 * k + 1], 8)
            .await;
    }
    for i in [0, 2] {
        recovery(&frozen, &origins[i], &history, &ledger).await;
        display_lookup(&origins[i + 1], &display, 1, &ledger).await;
        display_lookup(&origins[i + 1], &display, 8, &ledger).await;
    }
    providers[0].process.stop().await;
    for height in [9, 10] {
        publisher.stop().await;
        recovery(&frozen, &origins[2], &history, &ledger).await;
        display_lookup(&origins[3], &display, 1, &ledger).await;
        append(&history, &display, height);
        tip.store(height, Ordering::Release);
        publisher.start();
        let deadline = Instant::now() + Duration::from_secs(300);
        while json(&origins[2], "/v2/shards/init")
            .await
            .is_none_or(|s| s["covered_through"] != height)
        {
            publisher.assert_running();
            recovery(&frozen, &origins[2], &history, &ledger).await;
            display_lookup(&origins[3], &display, 1, &ledger).await;
            assert!(
                Instant::now() < deadline,
                "history consumer did not advance"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        wait_through(
            &mut providers[1].process,
            &origins[3],
            "/v2/txid/init",
            height,
        )
        .await;
        recovery(&frozen, &origins[2], &history, &ledger).await;
        display_lookup(&origins[3], &display, height, &ledger).await;
        ledger.record("live_update", serde_json::json!({"height":height}));
    }
    providers[0].process.start();
    providers[0]
        .wait_through(&origins[0], &origins[1], 10)
        .await;
    no_builds(&origins, &out).await;
    publisher.stop().await;
    for i in [0, 2] {
        recovery(&frozen, &origins[i], &history, &ledger).await;
        display_lookup(&origins[i + 1], &display, 10, &ledger).await;
    }
    providers[0].process.stop().await;
    // Restart requires live authority, even when all bytes and the old active pointer exist.
    providers[0].process.start();
    providers[0].wait_refused().await;
    ledger.record(
        "restart_without_authority_refused",
        serde_json::json!({"provider": 0}),
    );
    providers[0].process.stop().await;
    publisher.start();
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        publisher.assert_running();
        let mut ready = true;
        for (origin, prefix) in [
            (&history_origin, "/provider/v1/current"),
            (&display_origin, "/provider/txid/v1/current"),
        ] {
            ready &= https
                .get(format!("{origin}{prefix}"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success());
        }
        if ready {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "restarted publisher did not restore authority"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    providers[0].process.start();
    providers[0]
        .wait_through(&origins[0], &origins[1], 10)
        .await;
    no_builds(&origins, &out).await;
    canonical.store(false, Ordering::Release);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let error = tokio::process::Command::new(&frozen)
            .args([origins[0].as_str(), &hex::encode(wallet().as_slice()), "0"])
            .output()
            .await
            .unwrap();
        if !error.status.success() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "confirmed noncanonical history still serves"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    ledger.record("noncanonical_refused", serde_json::json!({"height":10}));
    for provider in &mut providers {
        provider.process.stop().await;
    }
    publisher.stop().await;
    node_task.abort();
    ledger.record("completed",serde_json::json!({"scope":"synthetic real-process correctness; production snapshots and original application not executed"}));
}
