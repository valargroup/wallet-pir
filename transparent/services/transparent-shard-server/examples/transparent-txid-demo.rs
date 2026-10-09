//! Frozen confirmed-chain qualification of txid display v2: the production
//! extractor and journal, the tiered publisher, an archive-owner and a
//! recent-replica worker behind an edge, and the wallet's own client, checked
//! against entries an independent Python parser derived from the raw blocks
//! and their parent transactions (`fixtures/verify_fixture.py`).
use axum::{
    body::Body,
    extract::{Request, State},
    http::StatusCode,
    response::Response,
    Router,
};
use clap::Parser;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tower::ServiceExt;
use transparent_events::Txid;
use transparent_filter::{BlockHash, MAINNET_GENESIS_DISPLAY};
use transparent_filter_server::{
    events::EventStore,
    extract::{extract_block, PreviousOutputs},
    txid_display::publisher::{publish_shard, write_candidate, DisplayRoot, ShardSpec},
};
use transparent_shard::display::{self, DisplaySealParams, DisplayTable, TXID_2K};
use transparent_shard::txid::{AddressKind, DisplayEntry, DisplayRecord, Tag};
use transparent_shard_server::{
    assignment::WorkerRole,
    display::{
        live::{DisplayCommand, DisplayLive, DisplayPublication},
        service::DisplayRuntime,
    },
    service::{ReadinessMode, ServiceConfig},
};
use transparent_txid_client::{
    Method, Placement, Route, TransportError, TxidDisplayClient, TxidLookup, TxidReply,
    TxidRequest, TxidTransport,
};
use zakura_chain::{
    block::Block,
    serialization::ZcashDeserialize,
    transaction::Transaction,
    transparent::{OutPoint, Output},
};

type Error = Box<dyn std::error::Error + Send + Sync>;

const FIXTURE: &[u8] = include_bytes!("fixtures/txid-confirmed.json");
// Confirmed output 1 returns to the script of the consumed output.
const ORDINARY_WITH_CHANGE: &str =
    "af0e93da89e6f267aa4f9a025bf6e39ca7c98668924c7da29f8766839782b170";

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "transparent/evidence/txid-display-demo.json")]
    report: PathBuf,
    /// Prove the independently derived oracle controls the process exit status.
    #[arg(long)]
    corrupt_oracle: bool,
}

#[derive(Deserialize)]
struct Fixture {
    source_sha: String,
    blocks: Vec<FrozenBlock>,
    parents: Vec<Parent>,
}
#[derive(Deserialize)]
struct Parent {
    txid: String,
    raw: String,
    sha256: String,
}
#[derive(Deserialize)]
struct FrozenBlock {
    height: u64,
    hash: String,
    raw: String,
    sha256: String,
    transactions: Vec<Expected>,
}
#[derive(Deserialize, Clone)]
struct Expected {
    txid: String,
    outputs: Vec<ExpectedOutput>,
    /// The independently derived 113-byte entry, when the transaction has
    /// any transparent input or output.
    entry: Option<String>,
}
#[derive(Deserialize, Clone)]
struct ExpectedOutput {
    script: String,
}

struct Parents(HashMap<OutPoint, Output>);
impl PreviousOutputs for Parents {
    fn previous_output(&mut self, point: &OutPoint) -> Result<Option<Output>, Error> {
        Ok(self.0.get(point).cloned())
    }
}

fn checked_bytes(raw: &str, digest: &str) -> Result<Vec<u8>, Error> {
    let bytes = hex::decode(raw)?;
    if hex::encode(Sha256::digest(&bytes)) != digest {
        return Err("fixture checksum mismatch".into());
    }
    Ok(bytes)
}
fn identity(s: &str) -> Result<Txid, Error> {
    Ok(Txid(hex::decode(s)?.try_into().map_err(|_| "txid length")?))
}
fn compare(entry: &DisplayEntry, expected: &Expected) -> Result<(), Error> {
    let oracle = expected.entry.as_deref().ok_or("no independent entry")?;
    if entry.tag != Tag::of(&identity(&expected.txid)?) || hex::encode(entry.encode()?) != oracle {
        return Err(format!("independent entry comparison failed for {}", expected.txid).into());
    }
    Ok(())
}

/// One exchange as the transport saw it, without bodies.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Sent {
    route: Route,
    request_bytes: usize,
    status: u16,
    reply_bytes: usize,
}

/// The wallet client's transport over loopback HTTP.
struct Http {
    client: reqwest::blocking::Client,
    base: String,
    log: Vec<Sent>,
}

impl TxidTransport for Http {
    fn send(&mut self, request: TxidRequest) -> Result<TxidReply, TransportError> {
        let url = format!("{}{}", self.base, request.path());
        let builder = match request.method {
            Method::Get => self.client.get(url),
            Method::Post => self
                .client
                .post(url)
                .header("content-type", request.content_type().unwrap())
                .body(request.body.clone()),
        };
        let response = builder.send().map_err(|e| TransportError(e.to_string()))?;
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        let reply = TxidReply {
            status: response.status().as_u16(),
            retry_after: header("retry-after"),
            map_sha256: header("x-txid-map-sha256"),
            body: response
                .bytes()
                .map_err(|e| TransportError(e.to_string()))?
                .to_vec(),
        };
        self.log.push(Sent {
            route: request.route,
            request_bytes: request.body.len(),
            status: reply.status,
            reply_bytes: reply.body.len(),
        });
        Ok(reply)
    }
}

/// What the edge saw and may inject.
#[derive(Clone, Default)]
struct Capture {
    paths: Arc<Mutex<Vec<String>>>,
    header_shapes: Arc<Mutex<Vec<Value>>>,
    private_tokens: Arc<Vec<String>>,
    leaked: Arc<AtomicBool>,
    foreign_codec: Arc<AtomicBool>,
}

#[derive(Clone)]
struct Edge {
    archive: Router,
    recent: Router,
    capture: Capture,
}

/// Routes `/v1/txid/archive/` to the owner and the rest to the replica,
/// keeping only public paths and header shapes, never bodies or keys.
async fn edge(State(edge): State<Edge>, request: Request) -> Response {
    let state = &edge.capture;
    let path = request.uri().path().to_string();
    state
        .paths
        .lock()
        .unwrap()
        .push(format!("{} {path}", request.method()));
    if request.uri().query().is_some()
        || state
            .private_tokens
            .iter()
            .any(|t| path.contains(t.as_str()))
    {
        state.leaked.store(true, Ordering::Relaxed);
    }
    for (name, value) in request.headers() {
        if let Ok(value) = value.to_str() {
            if state
                .private_tokens
                .iter()
                .any(|t| value.contains(t.as_str()))
            {
                state.leaked.store(true, Ordering::Relaxed);
            }
        }
        state
            .header_shapes
            .lock()
            .unwrap()
            .push(json!({"name": name.as_str(), "value_bytes": value.as_bytes().len()}));
    }
    let target = if path.starts_with("/v1/txid/archive/") {
        edge.archive
    } else {
        edge.recent
    };
    let response = target.oneshot(request).await.unwrap();
    if path == "/v1/txid/init" && state.foreign_codec.load(Ordering::Acquire) {
        let (mut parts, body) = response.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let mut init: Value = serde_json::from_slice(&bytes).unwrap();
        init["codec"] = "transparent-txid-display-v1".into();
        parts.headers.remove("content-length");
        return Response::from_parts(parts, Body::from(init.to_string()));
    }
    response
}

fn source_sha() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().into())
        .unwrap_or_else(|| "unavailable".into())
}

/// A worker that took `candidate` through prepare and activate, as a freshly
/// deployed worker does.
async fn worker(
    root: &Path,
    role: WorkerRole,
    candidate: &Path,
    map_sha256: &str,
) -> Result<DisplayLive, Error> {
    let config = ServiceConfig {
        cache_bytes: 2 << 30,
        readiness: ReadinessMode::Warm,
        ..ServiceConfig::default()
    };
    let runtime = DisplayRuntime::new(config, None);
    let collect = match role {
        WorkerRole::ArchiveOwner => vec![root.join("sealed")],
        WorkerRole::RecentReplica => Vec::new(),
    };
    let live = DisplayLive::new(
        runtime,
        role,
        3,
        root.join(format!("{}.active.json", role.as_str())),
        collect,
        None,
    )?;
    let expected = live.expected();
    live.command(DisplayCommand::Prepare {
        expected: expected.clone(),
        publication: DisplayPublication {
            directory: candidate.to_path_buf(),
            map_sha256: map_sha256.to_string(),
        },
    })
    .await?;
    live.command(DisplayCommand::Activate {
        expected,
        map_sha256: map_sha256.to_string(),
    })
    .await?;
    Ok(live)
}

/// Occupied slots and rows of every published table, from the files.
fn packing(directory: &Path) -> Result<Vec<Value>, Error> {
    let mut out = Vec::new();
    let mut files: Vec<_> = std::fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "bin"))
        .collect();
    files.sort();
    for file in files {
        let bytes = std::fs::read(&file)?;
        let mut entries = 0usize;
        let mut occupied = 0usize;
        for row in bytes.chunks_exact(transparent_shard::txid::ROW_BYTES) {
            let n = transparent_shard::txid::row_entries(row)?.len();
            entries += n;
            occupied += usize::from(n > 0);
        }
        out.push(json!({
            "file": file.file_name().map(|n| n.to_string_lossy().into_owned()),
            "bytes": bytes.len(),
            "entries": entries,
            "occupied_rows": occupied,
            "sha256": hex::encode(Sha256::digest(&bytes)),
        }));
    }
    Ok(out)
}

/// Transcript of one lookup after warm-up: route, body and reply sizes.
fn shape(log: &[Sent]) -> Vec<(Route, usize, u16, usize)> {
    log.iter()
        .map(|s| (s.route, s.request_bytes, s.status, s.reply_bytes))
        .collect()
}

async fn run_range(
    blocks: &[&FrozenBlock],
    fixture: &Fixture,
    corrupt: bool,
) -> Result<Value, Error> {
    let started = Instant::now();
    let temporary = tempfile::tempdir()?;
    let journal = temporary.path().join("journal");
    let root = temporary.path().join("display");

    // Ingest through the production extractor and journal.
    let mut previous = Parents(HashMap::new());
    for parent in &fixture.parents {
        let bytes = checked_bytes(&parent.raw, &parent.sha256)?;
        let transaction = Transaction::zcash_deserialize(bytes.as_slice())?;
        if transaction.hash().0 != identity(&parent.txid)?.0 {
            return Err("parent identity mismatch".into());
        }
        for (index, output) in transaction.outputs().iter().enumerate() {
            previous.0.insert(
                OutPoint {
                    hash: transaction.hash(),
                    index: index as u32,
                },
                output.clone(),
            );
        }
    }
    let mut store = EventStore::open(&journal, MAINNET_GENESIS_DISPLAY, blocks[0].height)?;
    let mut parent_hash = None;
    let mut expected: Vec<(u64, Expected)> = Vec::new();
    let mut extracted_records: BTreeMap<u64, Vec<DisplayRecord>> = BTreeMap::new();
    for frozen in blocks {
        let bytes = checked_bytes(&frozen.raw, &frozen.sha256)?;
        let block = Block::zcash_deserialize(bytes.as_slice())?;
        let hash = BlockHash::from_internal_bytes(block.hash().0);
        if hash.internal_bytes() != &hex::decode(&frozen.hash)?[..] {
            return Err("confirmed block identity mismatch".into());
        }
        if let Some(previous_block) = blocks.iter().find(|b| b.height + 1 == frozen.height) {
            if block.header.previous_block_hash.0 != identity(&previous_block.hash)?.0 {
                return Err("fixture chain discontinuity".into());
            }
        }
        parent_hash.get_or_insert(BlockHash::from_internal_bytes(
            block.header.previous_block_hash.0,
        ));
        for (transaction, facts) in block.transactions.iter().zip(&frozen.transactions) {
            if transaction.hash().0 != identity(&facts.txid)?.0 {
                return Err("confirmed transaction identity mismatch".into());
            }
        }
        let extracted = extract_block(&block.transactions, &mut previous, frozen.height as u32)?;
        let mut derived = Vec::new();
        for source in &extracted.display {
            let record = source.display_record()?;
            let facts = frozen
                .transactions
                .iter()
                .find(|f| f.txid == hex::encode(record.txid.0))
                .ok_or("missing independent facts")?;
            compare(&record.entry, facts)?;
            derived.push(record);
        }
        let eligible: Vec<_> = frozen
            .transactions
            .iter()
            .filter(|f| f.entry.is_some())
            .cloned()
            .collect();
        if extracted.display.len() != eligible.len() {
            return Err("incomplete canonical display extraction".into());
        }
        expected.extend(eligible.into_iter().map(|e| (frozen.height, e)));
        for transaction in &block.transactions {
            for (index, output) in transaction.outputs().iter().enumerate() {
                previous.0.insert(
                    OutPoint {
                        hash: transaction.hash(),
                        index: index as u32,
                    },
                    output.clone(),
                );
            }
        }
        store.append_block_with_display(
            frozen.height,
            hash,
            &extracted.events,
            &extracted.display,
        )?;
        extracted_records.insert(frozen.height, derived);
    }
    store.commit()?;
    drop(store);
    // Restart at the committed checkpoint: the entries derived from the
    // source sidecars read back exactly.
    let store = EventStore::open_existing(&journal)?;
    for (height, records) in &extracted_records {
        if &store.display_at(*height)? != records {
            return Err("display sidecar did not read back".into());
        }
    }

    // Publish through the production tiered publisher: an archive over the
    // first block when there is more than one, and the recent shard.
    let parent = parent_hash.ok_or("no blocks")?;
    let seal = DisplaySealParams {
        n_archive: 1,
        n_recent: 1,
        archive_target: 1,
        recent_floor: 1,
        reorg_margin: 1,
    };
    let layout = DisplayRoot {
        geometry: TXID_2K.name.to_string(),
        seal,
        max_archive_shards: 4,
        network: transparent_filter::NETWORK.to_string(),
        genesis_hash: store.genesis_hash().to_string(),
        start_height: blocks[0].height,
        base_parent: parent.to_display_hex(),
    };
    let hash_at = |height: u64| -> Result<String, Error> {
        Ok(store
            .block_at(height)
            .ok_or("journal block")?
            .block_hash
            .to_display_hex())
    };
    let heights: Vec<u64> = blocks.iter().map(|b| b.height).collect();
    let mut ranges = Vec::new();
    if heights.len() > 1 {
        ranges.push((heights[0], heights[0], true));
        ranges.push((heights[1], *heights.last().unwrap(), false));
    } else {
        ranges.push((heights[0], heights[0], false));
    }
    let mut archives = Vec::new();
    let mut recent = None;
    let mut parent_digest = String::new();
    let mut parent_block = parent.to_display_hex();
    let mut publish_seconds = 0.0;
    for (id, (start, end, sealed)) in ranges.into_iter().enumerate() {
        let records: Vec<DisplayRecord> = (start..=end)
            .map(|h| store.display_at(h))
            .collect::<Result<Vec<_>, _>>()?
            .concat();
        let spec = ShardSpec {
            shard_id: id as u64,
            start,
            end,
            parent_block_hash: parent_block.clone(),
            terminal_block_hash: hash_at(end)?,
            parent_manifest_digest: parent_digest.clone(),
            sealed,
            previous: None,
        };
        let shard = publish_shard(&root, &layout, &spec, &records)
            .map_err(|e| format!("publish shard {id}: {e:?}"))?;
        publish_seconds += shard.timings.build_s + shard.timings.verify_s + shard.timings.write_s;
        parent_block = spec.terminal_block_hash.clone();
        if sealed {
            parent_digest = shard.digest.clone();
            archives.push(shard);
        } else {
            recent = Some(shard);
        }
    }
    let recent = recent.ok_or("no recent shard")?;
    let archive_entries: Vec<_> = archives.iter().map(|a| a.entry.clone()).collect();
    let map = layout.map(&archive_entries, recent.entry.clone());
    map.check_shape()?;
    let (candidate, map_sha256) = write_candidate(&root, &map)?;

    // Two workers behind an edge, as deployed.
    let archive = worker(&root, WorkerRole::ArchiveOwner, &candidate, &map_sha256).await?;
    let replica = worker(&root, WorkerRole::RecentReplica, &candidate, &map_sha256).await?;
    let mut tokens = Vec::new();
    for (_, facts) in &expected {
        let txid = identity(&facts.txid)?;
        tokens.push(facts.txid.clone());
        tokens.push(txid.to_display_hex());
        tokens.push(hex::encode(Tag::of(&txid).0));
        tokens.extend(
            facts
                .outputs
                .iter()
                .filter(|o| o.script.len() > 8)
                .map(|o| o.script.clone()),
        );
    }
    let capture = Capture {
        private_tokens: Arc::new(tokens),
        ..Default::default()
    };
    let app = Router::new().fallback(edge).with_state(Edge {
        archive: archive.router(),
        recent: replica.router(),
        capture: capture.clone(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    // Lookups with the wallet client, which is synchronous.
    let lookups = {
        let url = url.clone();
        let expected = expected.clone();
        let capture = capture.clone();
        let first = map.start_height;
        let manifest_digest = recent.digest.clone();
        let recent_id = recent.entry.shard_id;
        tokio::task::spawn_blocking(move || -> Result<Value, Error> {
            let mut http = Http {
                client: reqwest::blocking::Client::builder()
                    .timeout(Duration::from_secs(120))
                    .build()?,
                base: url,
                log: Vec::new(),
            };
            let never = || false;
            let mut client = TxidDisplayClient::new();
            let mut counts = BTreeMap::<&str, usize>::new();
            let mut found_shapes = Vec::new();
            let mut lookups = Vec::new();
            for (index, (height, facts)) in expected.iter().enumerate() {
                let txid = identity(&facts.txid)?;
                // A warm-up lookup caches init, map, manifest and setups, so
                // the measured lookup's transcript is queries only.
                client.lookup(&mut http, txid.0, *height, &never)?;
                http.log.clear();
                let entry = match client.lookup(&mut http, txid.0, *height, &never)? {
                    TxidLookup::Found { entry, .. } => entry,
                    other => return Err(format!("{} not found: {other:?}", facts.txid).into()),
                };
                let mut oracle = facts.clone();
                if corrupt && index == 0 {
                    let mut bytes = hex::decode(oracle.entry.as_deref().unwrap())?;
                    bytes[20] ^= 1;
                    oracle.entry = Some(hex::encode(bytes));
                }
                compare(&entry, &oracle)?;
                // Altering any one field of the oracle must be noticed.
                let mut altered = facts.clone();
                let mut bytes = hex::decode(altered.entry.as_deref().unwrap())?;
                bytes[18] ^= 1;
                altered.entry = Some(hex::encode(bytes));
                if compare(&entry, &altered).is_ok() {
                    return Err("altered fee negative control failed".into());
                }
                found_shapes.push(shape(&http.log));
                lookups.push(json!({
                    "txid": facts.txid,
                    "height": height,
                    "complete": entry.is_complete(),
                    "flags": entry.flags(),
                    "input_count": entry.input_count,
                    "output_count": entry.output_count,
                    "requests": http.log.len(),
                    "upload_bytes": http.log.iter().map(|s| s.request_bytes).sum::<usize>(),
                    "download_bytes": http.log.iter().map(|s| s.reply_bytes).sum::<usize>(),
                }));
                if entry.coinbase {
                    *counts.entry("coinbase").or_default() += 1;
                }
                if facts.txid == ORDINARY_WITH_CHANGE {
                    let change = entry.outputs[1].ok_or("change output")?;
                    if !entry.is_complete()
                        || entry.input_count != 1
                        || entry.output_count != 2
                        || entry.fee != 10_000
                        || change.address != entry.source
                    {
                        return Err("ordinary change fixture summary".into());
                    }
                    *counts.entry("ordinary_send_with_change").or_default() += 1;
                }
                if entry.more_than_two_outputs() {
                    *counts.entry("several_outputs").or_default() += 1;
                }
                if entry.output_count >= 300 {
                    *counts.entry("large_output_list").or_default() += 1;
                }
                if entry.multiple_source_scripts {
                    *counts.entry("multiple_source_scripts").or_default() += 1;
                }
                if !entry.coinbase && entry.input_count == 0 && entry.shielded_components {
                    *counts.entry("external_transparent_unshielding").or_default() += 1;
                }
                if entry
                    .outputs
                    .iter()
                    .flatten()
                    .any(|o| o.address.kind == AddressKind::Other)
                {
                    *counts.entry("unusual_output_script").or_default() += 1;
                }
            }
            // Every found lookup in the recent shard has one transcript, and
            // an absent txid has the same one.
            let recent_height = expected
                .iter()
                .map(|(h, _)| *h)
                .max()
                .ok_or("no expectations")?;
            let recent_shapes: Vec<_> = expected
                .iter()
                .zip(&found_shapes)
                .filter(|((h, _), _)| *h == recent_height)
                .map(|(_, s)| s.clone())
                .collect();
            client.lookup(&mut http, [0x55; 32], recent_height, &never)?;
            http.log.clear();
            if client.lookup(&mut http, [0x55; 32], recent_height, &never)? != TxidLookup::Absent {
                return Err("missing txid not distinguished".into());
            }
            let absent = shape(&http.log);
            if recent_shapes.iter().any(|s| *s != absent)
                || absent.iter().filter(|s| s.0 == Route::Query).count() != 2
                || absent.len() != 2
            {
                return Err(format!(
                    "found and absent transcripts differ: {recent_shapes:?} vs {absent:?}"
                )
                .into());
            }
            http.log.clear();
            if first > 0
                && client.lookup(&mut http, [0x44; 32], first - 1, &never)?
                    != TxidLookup::PlacementUnknown(Placement::Below)
            {
                return Err("unknown placement control".into());
            }
            if http.log.iter().any(|s| s.route == Route::Query) {
                return Err("placement control dispatched a query".into());
            }
            // A server publishing another codec is unsupported, with no query.
            capture.foreign_codec.store(true, Ordering::Release);
            let mut fresh = TxidDisplayClient::new();
            http.log.clear();
            let unsupported = fresh.lookup(&mut http, [0x44; 32], recent_height, &never)?;
            capture.foreign_codec.store(false, Ordering::Release);
            if unsupported != TxidLookup::Unsupported
                || http.log.iter().any(|s| s.route == Route::Query)
            {
                return Err("unsupported capability dispatched a lookup".into());
            }
            // A query bound to another revision is refused.
            let profile = transparent_native::TableProfile::new(
                transparent_shard::manifest::SCHEMA,
                TXID_2K.name,
                "txdirectory",
                TXID_2K.directory_rows,
                transparent_shard::txid::ROW_BYTES as u32,
            )?;
            let (_, upload) = profile.prepare(0)?;
            let mut body =
                display::query_binding(&"00".repeat(32), DisplayTable::Directory(0)).to_vec();
            body.extend(upload);
            let status = http
                .client
                .post(format!(
                    "{}/v1/txid/recent/shards/{recent_id}/revisions/{manifest_digest}/query/directory-0",
                    http.base
                ))
                .header("content-type", "application/octet-stream")
                .body(body)
                .send()?
                .status();
            if status != StatusCode::BAD_REQUEST {
                return Err(format!("wrong binding refused with {status}").into());
            }
            let status = http
                .client
                .get(format!(
                    "{}/v1/txid/recent/shards/{recent_id}/revisions/{}/setup/directory-0/0",
                    http.base,
                    "00".repeat(32)
                ))
                .send()?
                .status();
            if status != StatusCode::CONFLICT {
                return Err(format!("unserved revision answered {status}").into());
            }
            Ok(json!({
                "cases": counts,
                "lookups": lookups,
                "absent_transcript": absent.iter().map(|s| json!({"route": format!("{:?}", s.0), "request_bytes": s.1, "status": s.2, "reply_bytes": s.3})).collect::<Vec<_>>(),
            }))
        })
        .await??
    };
    server.abort();

    if capture.leaked.load(Ordering::Relaxed) {
        return Err("private identifier in an HTTP path, header or query".into());
    }
    let paths = capture.paths.lock().unwrap().clone();
    let mut tables = Vec::new();
    for shard in archives.iter().chain([&recent]) {
        tables.push(json!({
            "shard_id": shard.entry.shard_id,
            "sealed": shard.entry.sealed,
            "records": shard.manifest.records,
            "manifest_digest": shard.digest,
            "packing": packing(&shard.directory)?,
        }));
    }
    Ok(json!({
        "start_height": map.start_height,
        "end_height": map.covered_through(),
        "map_sha256": map_sha256,
        "records": expected.len(),
        "cases": lookups["cases"],
        "lookups": lookups["lookups"],
        "absent_transcript": lookups["absent_transcript"],
        "shards": tables,
        "publish_seconds": publish_seconds,
        "http_safe_paths": paths,
        "http_header_shapes": capture.header_shapes.lock().unwrap().clone(),
        "elapsed_seconds": started.elapsed().as_secs_f64(),
        "negative_controls": ["missing_txid", "wrong_revision", "wrong_binding", "altered_entry", "unknown_placement", "unsupported_codec"],
        "checkpoint_restart": true,
    }))
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let started = Instant::now();
    let fixture: Fixture = serde_json::from_slice(FIXTURE)?;
    let mainnet: Vec<_> = fixture.blocks.iter().filter(|b| b.height > 0).collect();
    let genesis: Vec<_> = fixture.blocks.iter().filter(|b| b.height == 0).collect();
    let runs = vec![
        run_range(&mainnet, &fixture, args.corrupt_oracle).await?,
        run_range(&genesis, &fixture, false).await?,
    ];
    let required = [
        "ordinary_send_with_change",
        "several_outputs",
        "multiple_source_scripts",
        "external_transparent_unshielding",
        "coinbase",
        "unusual_output_script",
        "large_output_list",
    ];
    for case in required {
        if !runs
            .iter()
            .any(|r| r["cases"][case].as_u64().unwrap_or(0) > 0)
        {
            return Err(format!("missing required confirmed case {case}").into());
        }
    }
    let lock = include_bytes!("../../../../Cargo.lock");
    let system = sysinfo::System::new_all();
    let report = json!({
        "format": "transparent-txid-native-demo-v2",
        "passed": true,
        "codec": transparent_shard::txid::CODEC,
        "source_sha": source_sha(),
        "source_dirty": !std::process::Command::new("git").args(["status", "--porcelain"]).output()?.stdout.is_empty(),
        "fixture_source_sha": fixture.source_sha,
        "fixture_sha256": hex::encode(Sha256::digest(FIXTURE)),
        "cargo_lock_sha256": hex::encode(Sha256::digest(lock)),
        "chain_parser_sha": "af944f5194ef2e9921bc96af017629450375013c",
        "os": std::env::consts::OS,
        "architecture": std::env::consts::ARCH,
        "cpu": system.cpus().first().map(|c| c.brand()),
        "ram_bytes": system.total_memory(),
        "network": "zcash-mainnet frozen vectors; loopback HTTP; no runtime external lookup",
        "generated_at_unix_seconds": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs(),
        "command": "make transparent-txid-demo OFFLINE=1 REPORT=<report>",
        "profile": "release-fast",
        "runs": runs,
        "elapsed_seconds": started.elapsed().as_secs_f64(),
        "limitations": "Server protocol evidence only; trusted publisher; range, tier, bucket and timing leakage. No production capacity, cryptographic release, wallet recovery or Vizor qualification.",
    });
    if let Some(parent) = args.report.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.report, serde_json::to_vec_pretty(&report)?)?;
    println!(
        "native transparent txid demo passed; report {}",
        args.report.display()
    );
    Ok(())
}
