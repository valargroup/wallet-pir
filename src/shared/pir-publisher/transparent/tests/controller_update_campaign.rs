//! Explicit controller-update campaign: the real publish-controller process,
//! real TLS termination, a fixed archive owner process and two independent
//! packaged provider processes, each with its own state root.
//!
//! This is qualification-shaped evidence, not a fast correctness case: it is
//! `#[ignore]`d and run by `tools/transparent/scripts/https-update-campaign.py`,
//! which supplies the packaged provider, the original released391 client and
//! an output root. Every recovery, provider observation, stage timing and
//! failure is appended to that root as raw JSON lines; the test never
//! summarizes a pass it did not observe.
//!
//! The correctness oracle is independent of the controller and of both
//! providers: the frozen released client recovers one wallet through each
//! provider, and its complete ledger is compared with the ledger read back from
//! the committed journal events at the anchor the client reports. Controller
//! tables and `active.json` are never consulted. The in-process controller
//! distribution/manager tests still own fork/invalidation, replay, durable
//! fencing and write-failure cases; the runner executes them beside this
//! campaign instead of repeating them here.
use std::{
    collections::BTreeMap,
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
use transparent_events::TransparentEvent;
use transparent_filter::{BlockHash, ScriptBytes};
use transparent_publisher::history::publication::{self, PublishOptions};
use transparent_query_server::routing::router::{plan, RosterEntry};
use transparent_shared::history::assignment::{GeneratedBy, WorkerRole};
use transparent_shared::history::shardset::ShardSet;
use transparent_shared::journal::events::EventStore;
use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_publisher::transparent::filter as transparent_filter;

const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;
const RECENT_FROM: u64 = 2;
/// Whole-process bound for one frozen recovery, above the client's own
/// 15-second per-request deadline, so a hung recovery is a recorded timeout.
const RECOVERY_DEADLINE: Duration = Duration::from_secs(60);
const CLIENT_REQUEST_DEADLINE: Duration = Duration::from_secs(15);

fn hash(h: u64) -> BlockHash {
    if h == 0 {
        BlockHash::from_display_hex(GENESIS).unwrap()
    } else {
        BlockHash::from_internal_bytes([h as u8; 32])
    }
}
/// The one wallet the frozen client recovers; it is paid in every block from
/// height 1, so its history spans the fixed archive and the recent tier.
fn wallet() -> ScriptBytes {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend([0xaa; 20]);
    script.extend([0x88, 0xac]);
    ScriptBytes::new(script)
}
fn block(journal: &mut EventStore, h: u64) {
    let events = if h >= 1 {
        vec![(
            wallet(),
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

/// The phase label every concurrent observation carries.
#[derive(Clone)]
struct Phase(Arc<Mutex<String>>);
impl Phase {
    fn get(&self) -> String {
        self.0.lock().unwrap().clone()
    }
    fn set(&self, ledger: &Ledger, phase: &str) {
        *self.0.lock().unwrap() = phase.to_string();
        ledger.record("phase_start", serde_json::json!({"phase":phase}));
    }
}

/// Source ledgers by anchor height, read back from the committed journal.
type Oracle = Arc<Mutex<BTreeMap<u64, serde_json::Value>>>;

/// The ledger the frozen client must reproduce at `through`, derived from the
/// committed journal's events rather than from any publication or provider.
fn source_ledger(journal: &Path, through: u64) -> serde_json::Value {
    let store = EventStore::open_existing(journal).unwrap();
    let wallet = wallet();
    let (mut utxos, mut history) = (Vec::new(), Vec::new());
    for h in 0..=through {
        for (script, event) in store.events_at(h).unwrap().expect("committed block") {
            if script != wallet {
                continue;
            }
            let TransparentEvent::Receive(receive) = event else {
                panic!("the campaign journal holds only receives");
            };
            let txid = receive.txid.to_display_hex();
            utxos.push(serde_json::json!({"txid":txid,"index":receive.output_index,
                "value":receive.value,"script":hex::encode(script.as_slice()),
                "height":receive.height,"coinbase":receive.coinbase}));
            history.push(serde_json::json!({"txid":txid,"height":receive.height,
                "received":receive.value,"spent":0}));
        }
    }
    serde_json::json!({"utxos":utxos,"spends":[],"history":history,"unresolved":0})
}
fn same_entries(actual: &serde_json::Value, expected: &serde_json::Value) -> bool {
    match (actual.as_array(), expected.as_array()) {
        (Some(a), Some(e)) => a.len() == e.len() && e.iter().all(|x| a.contains(x)),
        _ => false,
    }
}

/// Raw bytes of every journal file: the dataset identity of one commit.
fn journal_identity(journal: &Path) -> serde_json::Value {
    let mut files = BTreeMap::new();
    let mut entries: Vec<_> = std::fs::read_dir(journal)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_file() {
            let bytes = std::fs::read(&path).unwrap();
            files.insert(
                path.file_name().unwrap().to_string_lossy().into_owned(),
                serde_json::json!({"bytes":bytes.len(),
                    "sha256":transparent_shared::publication::digest(&bytes)}),
            );
        }
    }
    let digest = transparent_shared::publication::digest(&serde_json::to_vec(&files).unwrap());
    serde_json::json!({"files_sha256":digest,"files":files})
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

/// One TLS terminator in front of the real controller, with its own fault
/// control file. A restart binds the same port so configured origins hold.
struct Terminator {
    name: &'static str,
    upstream: String,
    port: u16,
    control: PathBuf,
    root: PathBuf,
    child: Option<tokio::process::Child>,
}
impl Terminator {
    async fn start(root: &Path, name: &'static str, upstream: &str) -> Self {
        let control = root.join(format!("{name}-control"));
        std::fs::write(&control, "").unwrap();
        let mut terminator = Terminator {
            name,
            upstream: upstream.strip_prefix("http://").unwrap().to_string(),
            port: 0,
            control,
            root: root.to_path_buf(),
            child: None,
        };
        terminator.spawn().await;
        terminator
    }
    async fn spawn(&mut self) {
        let mut child = tokio::process::Command::new("python3")
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/support/distribution_tls.py"
            ))
            .arg(&self.upstream)
            .arg(self.root.join("ca.pem"))
            .arg(self.root.join("key.pem"))
            .arg(&self.control)
            .arg(self.port.to_string())
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .await
            .unwrap();
        self.port = line.trim().parse().unwrap();
        self.child = Some(child);
    }
    fn origin(&self) -> String {
        format!("https://localhost:{}", self.port)
    }
    fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(|c| c.id())
    }
    fn fault(&self, fault: &str) {
        let _ = std::fs::remove_file(self.control.with_extension("entered"));
        std::fs::write(&self.control, fault).unwrap();
    }
    async fn entered(&self, ledger: &Ledger, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while !self.control.with_extension("entered").exists() {
            if Instant::now() > deadline {
                ledger.record(
                    "fault_not_entered",
                    serde_json::json!({"terminator":self.name}),
                );
                panic!("{}: fault point never entered", self.name);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    async fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().await.unwrap();
            child.wait().await.unwrap();
        }
    }
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
        let deadline = Instant::now() + Duration::from_secs(300);
        loop {
            if reqwest::get(format!("{}/v1/ready", self.origin))
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                break;
            }
            if let Some(status) = child.try_wait().unwrap() {
                ledger.record(
                    "process_exit",
                    serde_json::json!({"process":self.name,"status":status.to_string()}),
                );
                panic!(
                    "{} exited: {}",
                    self.name,
                    std::fs::read_to_string(self.cwd.join("provider.log")).unwrap()
                );
            }
            assert!(
                Instant::now() < deadline,
                "{} never became ready",
                self.name
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        ledger.record(
            "process_ready",
            serde_json::json!({"process":self.name,"pid":child.id(),
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
    match client
        .get(format!("{publisher}/provider/v1/current"))
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r.json().await.unwrap_or_default(),
        _ => serde_json::Value::Null,
    }
}
/// The canonical offer and its map digest, read only over the publisher's
/// HTTPS distribution surface.
async fn offered(publisher: &str, client: &reqwest::Client) -> Option<(serde_json::Value, String)> {
    let record = current(publisher, client).await;
    if record["eligibility"] != "canonical" {
        return None;
    }
    let inventory: serde_json::Value = client
        .get(format!(
            "{publisher}/provider/v1/inventories/{}",
            record["inventory_sha256"].as_str()?
        ))
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    let map = inventory["map_sha256"].as_str()?.to_string();
    Some((record, map))
}

/// One fresh frozen-client process recovering the campaign wallet.
async fn recover(
    client: &Path,
    origin: &str,
    oracle: &Oracle,
) -> (&'static str, serde_json::Value) {
    let started = Instant::now();
    let child = tokio::process::Command::new(client)
        .args([origin, &hex::encode(wallet().as_slice()), "0"])
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let output = tokio::time::timeout(RECOVERY_DEADLINE, child.wait_with_output()).await;
    let ms = started.elapsed().as_secs_f64() * 1e3;
    let Ok(output) = output else {
        return (
            "timeout",
            serde_json::json!({"ms":ms,"error":"whole-recovery deadline"}),
        );
    };
    let output = output.unwrap();
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let error: String = stderr.chars().take(2048).collect();
        let lower = error.to_ascii_lowercase();
        // The client maps transport failures, including its own request
        // deadline, to one variant; elapsed time separates a deadline.
        let outcome = if started.elapsed() >= CLIENT_REQUEST_DEADLINE || lower.contains("timed out")
        {
            "timeout"
        } else if lower.contains("unreachable") {
            // Includes a provider the campaign stopped on purpose; the phase
            // and provider fields say which.
            "unreachable"
        } else if [
            "409",
            "429",
            "503",
            "busy",
            "retry",
            "refus",
            "unavailable",
            "conflict",
        ]
        .iter()
        .any(|s| lower.contains(s))
        {
            "refusal"
        } else {
            "error"
        };
        return (
            outcome,
            serde_json::json!({"ms":ms,"status":output.status.to_string(),"error":error}),
        );
    }
    let recovered: serde_json::Value = match serde_json::from_slice(&output.stdout) {
        Ok(v) => v,
        Err(e) => {
            return (
                "error",
                serde_json::json!({"ms":ms,"error":format!("client output: {e}")}),
            )
        }
    };
    let through = recovered["covered_through"].as_u64();
    let expected = through.and_then(|h| oracle.lock().unwrap().get(&h).cloned());
    let detail = serde_json::json!({"ms":ms,"covered_through":through,
        "matched_shards":recovered["matched_shards"]});
    let Some(expected) = expected else {
        return (
            "wrong",
            serde_json::json!({"ms":ms,"error":"anchor is not a committed journal tip",
            "recovered":recovered}),
        );
    };
    let exact = ["utxos", "spends", "history"]
        .iter()
        .all(|k| same_entries(&recovered[*k], &expected[*k]))
        && recovered["unresolved"] == expected["unresolved"];
    if exact {
        ("ok", detail)
    } else {
        (
            "wrong",
            serde_json::json!({"ms":ms,"error":"ledger differs from journal events",
            "recovered":recovered,"expected":expected}),
        )
    }
}

/// Continuous frozen-client recoveries against one provider until `stop`.
async fn recoveries(
    ledger: Arc<Ledger>,
    phase: Phase,
    client: PathBuf,
    provider: (&'static str, String),
    oracle: Oracle,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Acquire) {
        let label = phase.get();
        let (outcome, mut detail) = recover(&client, &provider.1, &oracle).await;
        detail["phase"] = label.into();
        detail["provider"] = provider.0.into();
        detail["outcome"] = outcome.into();
        ledger.record("recovery", detail);
        tokio::time::sleep(Duration::from_millis(if outcome == "ok" {
            50
        } else {
            250
        }))
        .await;
    }
}

/// Records every change of a provider's synchronization, verified map and
/// archive reachability, including each activation's stage timings.
async fn watch(
    ledger: Arc<Ledger>,
    phase: Phase,
    provider: (&'static str, String),
    stop: Arc<AtomicBool>,
) {
    let mut last = serde_json::Value::Null;
    let mut last_change = serde_json::Value::Null;
    while !stop.load(Ordering::Acquire) {
        let observed = status(&provider.1).await;
        let sync = &observed["synchronization"];
        let key = serde_json::json!({"state":sync["state"],"error":sync["error"],
            "in_progress":sync["in_progress"],
            "map_sha256":observed["verified_publication"]["map_sha256"],
            "archive":observed["archive_reachability"]["state"],
            "reachable":!observed.is_null()});
        if key != last {
            let mut record = key.clone();
            record["phase"] = phase.get().into();
            record["provider"] = provider.0.into();
            record["progress"] = sync["progress"].clone();
            ledger.record("provider_sync", record);
            last = key;
        }
        if !sync["last_change"].is_null() && sync["last_change"] != last_change {
            ledger.record(
                "provider_stages",
                serde_json::json!({"phase":phase.get(),"provider":provider.0,
                    "map_sha256":observed["verified_publication"]["map_sha256"],
                    "change":sync["last_change"]}),
            );
            last_change = sync["last_change"].clone();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

struct Campaign {
    ledger: Arc<Ledger>,
    phase: Phase,
    https: reqwest::Client,
    /// The terminator the campaign itself and the distribution consumers use.
    publisher: String,
    revisions: Mutex<Vec<serde_json::Value>>,
}
impl Campaign {
    /// Wait until the controller offers an anchor at or above `min_anchor` and
    /// every named provider verifies that offer's map.
    async fn converge(
        &self,
        providers: &[&Provider],
        min_anchor: u64,
        timeout: Duration,
    ) -> (serde_json::Value, String) {
        let phase = self.phase.get();
        let started = Instant::now();
        let mut offer: Option<(serde_json::Value, String)> = None;
        let mut seen = vec![false; providers.len()];
        while started.elapsed() < timeout {
            if offer.is_none() {
                if let Some((record, map)) = offered(&self.publisher, &self.https).await {
                    if record["anchor_height"].as_u64() >= Some(min_anchor) {
                        let identity = serde_json::json!({"revision":record["revision"],
                            "anchor_height":record["anchor_height"],"anchor_hash":record["anchor_hash"],
                            "inventory_sha256":record["inventory_sha256"],"map_sha256":map});
                        self.ledger.record(
                            "controller_offered",
                            serde_json::json!({"phase":phase,"publication":identity,
                                "ms":started.elapsed().as_secs_f64()*1e3}),
                        );
                        let mut revisions = self.revisions.lock().unwrap();
                        if !revisions.contains(&identity) {
                            revisions.push(identity);
                        }
                        offer = Some((record, map));
                    }
                }
            }
            if let Some((record, map)) = &offer {
                for (i, provider) in providers.iter().enumerate() {
                    if !seen[i] {
                        let observed = status(&provider.origin).await;
                        if observed["verified_publication"]["map_sha256"] == *map.as_str() {
                            self.ledger.record(
                                "provider_activated",
                                serde_json::json!({"phase":phase,"provider":provider.name,
                                    "revision":record["revision"],
                                    "ms":started.elapsed().as_secs_f64()*1e3,
                                    "stages":observed["synchronization"]["last_change"]}),
                            );
                            seen[i] = true;
                        }
                    }
                }
                if seen.iter().all(|s| *s) {
                    return offer.unwrap();
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.ledger.record(
            "convergence_timeout",
            serde_json::json!({"phase":phase,"offered":offer.is_some(),
                "activated":seen.iter().filter(|s| **s).count()}),
        );
        panic!("{phase}: convergence exceeded {timeout:?}");
    }
}

/// CPU ticks, resident bytes and character I/O of one live process.
fn process_sample(pid: u32) -> Option<serde_json::Value> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    let ticks: u64 = fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?;
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let rss_kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next())
        .and_then(|v| v.parse().ok())?;
    let io = std::fs::read_to_string(format!("/proc/{pid}/io")).unwrap_or_default();
    let field = |name: &str| {
        io.lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|v| v.trim().parse::<u64>().ok())
    };
    Some(
        serde_json::json!({"cpu_ticks":ticks,"rss_bytes":rss_kb*1024,
        "rchar":field("rchar:"),"wchar":field("wchar:")}),
    )
}

/// One distribution wave of the corrected PR #201 driver against the real
/// controller's terminator, with publisher CPU, memory and I/O samples.
async fn wave(
    campaign: &Campaign,
    out: &Path,
    consumers: u32,
    phase: &str,
    processes: &[(&'static str, u32)],
) {
    let output = out.join(format!("distribution-{consumers}"));
    let budget = std::env::var("WALLET_PIR_CAMPAIGN_DISTRIBUTION_BYTES")
        .unwrap_or_else(|_| (8u64 << 30).to_string());
    let sample = || {
        processes
            .iter()
            .map(|(name, pid)| (name.to_string(), process_sample(*pid).unwrap_or_default()))
            .collect::<serde_json::Map<_, _>>()
    };
    let before = sample();
    let started = Instant::now();
    let mut child = tokio::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../../tools/transparent/loadtest/distribution.py"
        ))
        .args(["--publisher", &campaign.publisher, "--ca"])
        .arg(out.join("work/ca.pem"))
        .arg("--output")
        .arg(&output)
        .args(["--consumers", &consumers.to_string(), "--phase", phase])
        .args(["--cache-budget-bytes", &budget])
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut peak = serde_json::Map::new();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        for (name, value) in sample() {
            let rss = value["rss_bytes"].as_u64().unwrap_or(0);
            if peak.get(&name).and_then(|v| v.as_u64()).unwrap_or(0) < rss {
                peak.insert(name, rss.into());
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let seconds = started.elapsed().as_secs_f64();
    let after = sample();
    let output = child.wait_with_output().await.unwrap();
    let report = String::from_utf8_lossy(&output.stdout)
        .lines()
        .last()
        .and_then(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .unwrap_or_default();
    let ticks = |m: &serde_json::Map<String, serde_json::Value>, n: &str| {
        m.get(n).and_then(|v| v["cpu_ticks"].as_u64()).unwrap_or(0)
    };
    let cpu: serde_json::Map<_, _> = processes
        .iter()
        .map(|(n, _)| (n.to_string(), (ticks(&after, n) - ticks(&before, n)).into()))
        .collect();
    campaign.ledger.record(
        "distribution_wave",
        serde_json::json!({"phase":campaign.phase.get(),"consumers":consumers,"wave":phase,
            "exit":status.code(),"seconds":seconds,"report":report,
            "output":output_dir_name(consumers),"cpu_ticks_delta":cpu,
            "peak_rss_bytes":peak,"before":before,"after":after,
            "stderr":String::from_utf8_lossy(&output.stderr).chars().take(2048).collect::<String>()}),
    );
}
fn output_dir_name(consumers: u32) -> String {
    format!("distribution-{consumers}")
}

/// Every convergence must name a strictly newer controller revision.
fn advanced(previous: u64, record: &serde_json::Value) -> u64 {
    let next = record["revision"].as_u64().unwrap();
    assert!(
        next > previous,
        "revision {next} does not follow {previous}"
    );
    next
}

fn copy_tree(source: &Path, target: &Path) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let path = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &path);
        } else {
            std::fs::copy(entry.path(), path).unwrap();
        }
    }
}

/// A terminal archive owner serving the fixed initial publication's archive
/// tier from its own copy, behind its own TLS terminator.
async fn archive(
    root: &Path,
    binary: &Path,
    initial: &Path,
    ledger: &Ledger,
) -> (String, Provider, tokio::process::Child, serde_json::Value) {
    let cwd = root.join("archive");
    let state = cwd.join("state");
    copy_tree(initial, &state);
    let set = ShardSet::open(&state, usize::MAX).unwrap();
    let boundary = set
        .map
        .shards
        .iter()
        .position(|s| s.start_height >= RECENT_FROM)
        .unwrap() as u64;
    let roster = [
        ("archive-01", WorkerRole::ArchiveOwner, None),
        (
            "recent-01",
            WorkerRole::RecentReplica,
            Some("recent".to_string()),
        ),
    ]
    .map(|(id, role, replica_group)| RosterEntry {
        id: id.into(),
        role,
        replica_group,
        ssh_host: "127.0.0.1".into(),
        upstream: "127.0.0.1:1".into(),
        cache_bytes: 4 << 30,
        archive_range: None,
    });
    let assignment = plan(
        &set.map,
        &set.map_digest,
        &roster,
        boundary,
        0.1,
        GeneratedBy {
            tool: "controller update campaign".into(),
            source_sha: None,
            generated_at: "campaign".into(),
        },
    )
    .unwrap();
    let assignment_bytes = serde_json::to_vec(&assignment).unwrap();
    std::fs::write(cwd.join("assignment.json"), &assignment_bytes).unwrap();
    let identity = serde_json::json!({"map_sha256":set.map_digest,"recent_from_shard":boundary,
        "assignment_sha256":transparent_shared::publication::digest(&assignment_bytes),
        "archive_shards":set.map.shards.iter().take(boundary as usize)
            .map(|s| serde_json::json!({"shard_id":s.shard_id,"manifest_digest":s.manifest_digest}))
            .collect::<Vec<_>>()});
    drop(set);
    let addr = free_port();
    let mut owner = Provider {
        name: "archive",
        origin: format!("http://{addr}"),
        args: [
            "--shard-dir",
            state.to_str().unwrap(),
            "--assignment",
            cwd.join("assignment.json").to_str().unwrap(),
            "--worker-id",
            "archive-01",
            "--listen",
            &addr.to_string(),
            "--cache-bytes",
            &(2u64 << 30).to_string(),
        ]
        .map(String::from)
        .to_vec(),
        cwd,
        child: None,
    };
    owner.start(binary, ledger).await;
    let mut tls = tokio::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../transparent/transparent-query-server/tests/support/provider_tls.py"
        ))
        .arg(addr.to_string())
        .arg(root.join("ca.pem"))
        .arg(root.join("key.pem"))
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(tls.stdout.take().unwrap())
        .read_line(&mut line)
        .await
        .unwrap();
    let origin = format!("https://localhost:{}", line.trim());
    (origin, owner, tls, identity)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit campaign; run tools/transparent/scripts/https-update-campaign.py"]
async fn controller_updates_reach_independent_https_providers_under_queries() {
    let binary = PathBuf::from(
        std::env::var_os("WALLET_PIR_PROVIDER_BIN")
            .expect("WALLET_PIR_PROVIDER_BIN names the packaged transparent-query-server"),
    );
    let client = PathBuf::from(
        std::env::var_os("WALLET_PIR_FROZEN_CLIENT")
            .expect("WALLET_PIR_FROZEN_CLIENT names the isolated released391 client"),
    );
    let out = PathBuf::from(
        std::env::var_os("WALLET_PIR_CAMPAIGN_OUT").expect("WALLET_PIR_CAMPAIGN_OUT names the run"),
    );
    let updates: u64 = std::env::var("WALLET_PIR_CAMPAIGN_UPDATES")
        .ok()
        .map_or(3, |v| v.parse().unwrap());
    let consumers: Vec<u32> = std::env::var("WALLET_PIR_CAMPAIGN_CONSUMERS")
        .unwrap_or_else(|_| "10".into())
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().unwrap())
        .collect();
    assert!(
        updates as usize >= consumers.len(),
        "each consumer count needs its own update"
    );
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
    let phase = Phase(Arc::new(Mutex::new("setup".into())));
    let oracle: Oracle = Arc::default();
    let journals = Mutex::new(Vec::new());

    // Journal owned by this "ingest" writer; the controller follows it.
    let journal_dir = root.join("journal");
    let mut journal = EventStore::open(&journal_dir, GENESIS, 0).unwrap();
    let mut tip_height = 4;
    for h in 0..=tip_height {
        block(&mut journal, h);
    }
    journal.commit().unwrap();
    let commit = |journal: &mut EventStore, height: u64| {
        journal.commit().unwrap();
        // The oracle and dataset identity are taken from committed bytes.
        oracle
            .lock()
            .unwrap()
            .insert(height, source_ledger(&journal_dir, height));
        let identity = journal_identity(&journal_dir);
        ledger.record(
            "journal_committed",
            serde_json::json!({"height":height,"files_sha256":identity["files_sha256"]}),
        );
        journals
            .lock()
            .unwrap()
            .push(serde_json::json!({"height":height,"identity":identity}));
    };
    commit(&mut journal, tip_height);
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
    let initial_map = ShardSet::open(&initial, usize::MAX).unwrap().map_digest;

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
    std::fs::set_permissions(&hook, std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
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
    let mut controller =
        tokio::process::Command::new(env!("CARGO_BIN_EXE_transparent-publish-controller"))
            .arg("--config")
            .arg(out.join("controller-config.json"))
            .kill_on_drop(true)
            .stdout(Stdio::null())
            .stderr(controller_log)
            .spawn()
            .unwrap();
    let controller_pid = controller.id().unwrap();

    let openssl = std::process::Command::new("openssl")
        .args([
            "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        ])
        .args([
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
        ])
        .args(["-addext", "basicConstraints=critical,CA:FALSE", "-keyout"])
        .arg(root.join("key.pem"))
        .arg("-out")
        .arg(root.join("ca.pem"))
        .output()
        .unwrap();
    assert!(openssl.status.success());
    // One terminator per consumer class, all forwarding to the one controller:
    // faults then target one provider, and distribution CPU is attributable.
    let upstream = format!("http://{listen}");
    let mut terminators = Vec::new();
    for name in ["publisher-a", "publisher-b", "publisher-campaign"] {
        terminators.push(Terminator::start(&root, name, &upstream).await);
    }
    let ca = reqwest::Certificate::from_pem(&std::fs::read(root.join("ca.pem")).unwrap()).unwrap();
    let https = reqwest::Client::builder()
        .add_root_certificate(ca)
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let campaign = Campaign {
        ledger: ledger.clone(),
        phase: phase.clone(),
        https,
        publisher: terminators[2].origin(),
        revisions: Mutex::new(Vec::new()),
    };

    // Bootstrap: the controller's own first revision, the fixed archive owner
    // over its own copy of the initial publication, then two cold providers.
    phase.set(&ledger, "controller-bootstrap");
    let (first, _) = campaign
        .converge(&[], tip_height, Duration::from_secs(180))
        .await;
    phase.set(&ledger, "archive-bootstrap");
    let (archive_origin, mut archive_owner, mut archive_tls, archive_identity) =
        archive(&root, &binary, &initial, &ledger).await;
    let mut providers = Vec::new();
    for (name, terminator) in [
        ("provider-a", &terminators[0]),
        ("provider-b", &terminators[1]),
    ] {
        let cwd = root.join(name);
        std::fs::create_dir(&cwd).unwrap();
        let addr = free_port();
        let args = [
            "--publisher",
            &terminator.origin(),
            "--publisher-network",
            "main",
            "--publisher-genesis",
            GENESIS,
            "--publisher-ca",
            root.join("ca.pem").to_str().unwrap(),
            "--archive-upstream",
            &archive_origin,
            "--archive-ca",
            root.join("ca.pem").to_str().unwrap(),
            "--listen",
            &addr.to_string(),
            "--shard-dir",
            cwd.join("state").to_str().unwrap(),
            "--cache-bytes",
            &(1u64 << 30).to_string(),
        ]
        .map(String::from)
        .to_vec();
        providers.push(Provider {
            name,
            origin: format!("http://{addr}"),
            args,
            cwd,
            child: None,
        });
    }
    phase.set(&ledger, "provider-bootstrap");
    let started = Instant::now();
    let (a, b) = providers.split_at_mut(1);
    tokio::join!(a[0].start(&binary, &ledger), b[0].start(&binary, &ledger));
    ledger.record(
        "phase",
        serde_json::json!({"phase":"bootstrap","revision":first["revision"],
        "ms":started.elapsed().as_secs_f64()*1e3}),
    );

    let stop = Arc::new(AtomicBool::new(false));
    let mut background = Vec::new();
    for p in &providers {
        let target = (p.name, p.origin.clone());
        background.push(tokio::spawn(watch(
            ledger.clone(),
            phase.clone(),
            target.clone(),
            stop.clone(),
        )));
        background.push(tokio::spawn(recoveries(
            ledger.clone(),
            phase.clone(),
            client.clone(),
            target,
            oracle.clone(),
            stop.clone(),
        )));
    }
    // A recovery reaches shard 0 through the archive; both providers must have
    // identity-checked the fixed owner before updates begin.
    phase.set(&ledger, "archive-check");
    for p in &providers {
        let deadline = Instant::now() + Duration::from_secs(120);
        while status(&p.origin).await["archive_reachability"]["state"] != "identity-checked" {
            assert!(
                Instant::now() < deadline,
                "{} never identity-checked the archive",
                p.name
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let publisher_processes = [
        ("controller", controller_pid),
        ("publisher-campaign-tls", terminators[2].pid().unwrap()),
    ];
    if let Some(n) = consumers.first() {
        phase.set(&ledger, &format!("distribution-{n}-bootstrap"));
        wave(&campaign, &out, *n, "bootstrap", &publisher_processes).await;
    }
    phase.set(&ledger, "steady");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut revision = first["revision"].as_u64().unwrap();
    let append = |journal: &mut EventStore, tip_height: &mut u64| {
        *tip_height += 1;
        block(journal, *tip_height);
        commit(journal, *tip_height);
        tip.store(*tip_height, Ordering::Release);
    };
    // Updates under continuous frozen-client recoveries against both providers.
    for update in 0..updates {
        phase.set(&ledger, &format!("update-{update}"));
        append(&mut journal, &mut tip_height);
        let (record, _) = campaign
            .converge(
                &providers.iter().collect::<Vec<_>>(),
                tip_height,
                Duration::from_secs(180),
            )
            .await;
        assert!(record["revision"].as_u64().unwrap() > revision);
        revision = advanced(revision, &record);
        let index = update as usize;
        if let Some(n) = consumers.get(index) {
            phase.set(&ledger, &format!("distribution-{n}-update"));
            wave(&campaign, &out, *n, "update", &publisher_processes).await;
        }
        if let Some(n) = consumers.get(index + 1) {
            phase.set(&ledger, &format!("distribution-{n}-bootstrap"));
            wave(&campaign, &out, *n, "bootstrap", &publisher_processes).await;
        }
    }

    // Unchanged source: no new revision; this window is the no-update baseline.
    phase.set(&ledger, "unchanged");
    tokio::time::sleep(Duration::from_secs(15)).await;
    let unchanged = current(&campaign.publisher, &campaign.https).await;
    assert_eq!(
        unchanged["revision"].as_u64(),
        Some(revision),
        "unchanged source advanced revision"
    );

    // Supersession: both providers stall mid-transfer of one revision while a
    // newer one is published; provider-b is also killed during that transfer.
    phase.set(&ledger, "supersession");
    for t in &terminators[..2] {
        t.fault("pause-artifact");
    }
    append(&mut journal, &mut tip_height);
    let superseded_anchor = tip_height;
    for t in &terminators[..2] {
        t.entered(&ledger, Duration::from_secs(120)).await;
    }
    let (stalled, stalled_map) = campaign
        .converge(&[], superseded_anchor, Duration::from_secs(60))
        .await;
    ledger.record(
        "fault",
        serde_json::json!({"phase":"supersession","point":"mid-transfer",
            "stalled_revision":stalled["revision"],"provider_status":[status(&providers[0].origin).await,
            status(&providers[1].origin).await]}),
    );
    providers[1].stop().await;
    ledger.record(
        "fault",
        serde_json::json!({"phase":"supersession","killed":"provider-b","point":"mid-transfer"}),
    );
    append(&mut journal, &mut tip_height);
    campaign
        .converge(&[], tip_height, Duration::from_secs(120))
        .await;
    for t in &terminators[..2] {
        t.fault("");
    }
    providers[1].start(&binary, &ledger).await;
    let (record, _) = campaign
        .converge(
            &providers.iter().collect::<Vec<_>>(),
            tip_height,
            Duration::from_secs(180),
        )
        .await;
    revision = advanced(revision, &record);
    for p in &providers {
        let log = std::fs::read_to_string(out.join("observations.jsonl")).unwrap();
        let activated_stalled = log.lines().any(|l| {
            l.contains("\"kind\":\"provider_sync\"")
                && l.contains(&format!("\"provider\":\"{}\"", p.name))
                && l.contains(&format!("\"map_sha256\":\"{stalled_map}\""))
        });
        assert!(
            !activated_stalled,
            "{} activated the superseded revision",
            p.name
        );
    }

    // Restart during staging/activation: provider-b's transfer stalls, is
    // released into its activation checks, and is killed while they stall.
    phase.set(&ledger, "activation-restart");
    terminators[1].fault("pause-artifact");
    append(&mut journal, &mut tip_height);
    terminators[1]
        .entered(&ledger, Duration::from_secs(120))
        .await;
    terminators[1].fault("pause-anchor");
    terminators[1]
        .entered(&ledger, Duration::from_secs(120))
        .await;
    ledger.record(
        "fault",
        serde_json::json!({"phase":"activation-restart","point":"activation-anchor-check",
            "provider_status":status(&providers[1].origin).await}),
    );
    providers[1].stop().await;
    ledger.record(
        "fault",
        serde_json::json!({"phase":"activation-restart","killed":"provider-b"}),
    );
    terminators[1].fault("");
    providers[1].start(&binary, &ledger).await;
    let (record, _) = campaign
        .converge(
            &providers.iter().collect::<Vec<_>>(),
            tip_height,
            Duration::from_secs(180),
        )
        .await;
    revision = advanced(revision, &record);

    // Missed revisions: provider-b is down across two publications.
    phase.set(&ledger, "missed");
    providers[1].stop().await;
    for _ in 0..2 {
        append(&mut journal, &mut tip_height);
        campaign
            .converge(&[&providers[0]], tip_height, Duration::from_secs(180))
            .await;
    }
    providers[1].start(&binary, &ledger).await;
    phase.set(&ledger, "missed-recovery");
    let (record, _) = campaign
        .converge(
            &providers.iter().collect::<Vec<_>>(),
            tip_height,
            Duration::from_secs(120),
        )
        .await;
    revision = advanced(revision, &record);

    // Source outage: every publisher terminator stops; both providers keep
    // answering from their active view, then the publisher TLS restarts.
    phase.set(&ledger, "source-outage");
    for t in &mut terminators {
        t.stop().await;
    }
    for p in &providers {
        let deadline = Instant::now() + Duration::from_secs(120);
        while status(&p.origin).await["synchronization"]["state"] != "refused" {
            assert!(
                Instant::now() < deadline,
                "{} never observed the outage",
                p.name
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    phase.set(&ledger, "publisher-restart");
    let restarted = Instant::now();
    for t in &mut terminators {
        t.spawn().await;
    }
    for p in &providers {
        let deadline = Instant::now() + Duration::from_secs(300);
        while status(&p.origin).await["synchronization"]["state"] != "synchronized" {
            assert!(
                Instant::now() < deadline,
                "{} never resynchronized after the publisher restart",
                p.name
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        ledger.record(
            "provider_resynchronized",
            serde_json::json!({"provider":p.name,"ms":restarted.elapsed().as_secs_f64()*1e3}),
        );
    }
    phase.set(&ledger, "post-restart-update");
    append(&mut journal, &mut tip_height);
    let (record, _) = campaign
        .converge(
            &providers.iter().collect::<Vec<_>>(),
            tip_height,
            Duration::from_secs(180),
        )
        .await;
    revision = advanced(revision, &record);

    phase.set(&ledger, "final");
    tokio::time::sleep(Duration::from_secs(5)).await;
    stop.store(true, Ordering::Release);
    for task in background {
        task.await.unwrap();
    }
    for p in &mut providers {
        p.stop().await;
    }
    archive_owner.stop().await;
    archive_tls.kill().await.unwrap();
    for t in &mut terminators {
        t.stop().await;
    }
    controller.kill().await.unwrap();
    controller.wait().await.unwrap();
    let dataset = serde_json::json!({
        "journal": {"genesis": GENESIS, "tip_height": tip_height, "commits": journals.into_inner().unwrap()},
        "initial_publication": {"map_sha256": initial_map},
        "archive": archive_identity,
        "publications": campaign.revisions.into_inner().unwrap(),
        "wallet_script": hex::encode(wallet().as_slice()),
    });
    std::fs::write(
        out.join("dataset.json"),
        serde_json::to_vec_pretty(&dataset).unwrap(),
    )
    .unwrap();
    ledger.record("campaign_end", serde_json::json!({"revision":revision}));
    // Retryable refusals, errors and timeouts are recorded, never hidden; a
    // wrong recovered ledger fails the campaign.
    let wrong = std::fs::read_to_string(out.join("observations.jsonl"))
        .unwrap()
        .lines()
        .filter(|l| l.contains("\"kind\":\"recovery\"") && l.contains("\"outcome\":\"wrong\""))
        .count();
    assert_eq!(
        wrong, 0,
        "a provider served a ledger that differs from the journal"
    );
}
