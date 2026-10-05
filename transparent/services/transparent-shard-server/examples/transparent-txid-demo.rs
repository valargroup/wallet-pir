//! Frozen confirmed-chain qualification over the production publisher and HTTP service.
#[path = "support/txquery.rs"]
mod txquery;

use axum::{extract::Request, middleware::Next, response::Response};
use clap::Parser;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};
use transparent_events::{FeeState, Txid};
use transparent_filter::{BlockHash, MAINNET_GENESIS_DISPLAY};
use transparent_filter_server::{
    events::EventStore,
    extract::{extract_block, PreviousOutputs},
    publication::{publish, PublishOptions},
};
use transparent_shard::{layout::RECENT_4K, txid::TransparentDisplayRecord};
use transparent_shard_server::{
    service::{router, ServiceConfig, ServiceState},
    shardset::{ShardSet, Table, DEFAULT_RETAIN_REVISIONS},
};
use txquery::{Error, PrivateClient};
use zakura_chain::{
    block::Block,
    serialization::ZcashDeserialize,
    transaction::Transaction,
    transparent::{OutPoint, Output},
};

const FIXTURE: &[u8] = include_bytes!("fixtures/txid-confirmed.json");
// Confirmed output 1 returns to the script of the consumed output. The demo
// assigns this public script to Alice without using wallet keys.
const ORDINARY_WITH_CHANGE: &str =
    "af0e93da89e6f267aa4f9a025bf6e39ca7c98668924c7da29f8766839782b170";
const METADATA_SHA: &str = "3cfbc4848b92b1385b9215174251f8e51fbfa804";

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "transparent/evidence/txid-display-demo.json")]
    report: PathBuf,
    /// Prove the independently authored oracle controls the process exit status.
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
    coinbase: bool,
    input_count: u32,
    shielded_components: bool,
    outputs: Vec<ExpectedOutput>,
    fee: Value,
}
#[derive(Deserialize, Clone)]
struct ExpectedOutput {
    value: u64,
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
fn compare(record: &TransparentDisplayRecord, facts: &Expected) -> Result<(), Error> {
    let fee = match facts.fee.as_u64() {
        Some(v) => FeeState::Exact(v),
        None if facts.fee == "not_applicable" => FeeState::NotApplicable,
        None if facts.fee == "unknown" => FeeState::Unknown,
        _ => return Err("invalid independent fee expectation".into()),
    };
    if record.txid != identity(&facts.txid)?
        || record.coinbase != facts.coinbase
        || record.metadata.fee != fee
        || record.metadata.transparent_input_count != facts.input_count
        || record.metadata.has_shielded_components != facts.shielded_components
        || record.outputs.len() != facts.outputs.len()
    {
        return Err("independent identity/summary/output-count comparison failed".into());
    }
    for (actual, expected) in record.outputs.iter().zip(&facts.outputs) {
        if actual.value != expected.value || actual.script != hex::decode(&expected.script)? {
            return Err("independent ordered output comparison failed".into());
        }
    }
    Ok(())
}
#[derive(Clone, Default)]
struct Capture {
    paths: Arc<Mutex<Vec<String>>>,
    interrupt: Arc<AtomicBool>,
    header_shapes: Arc<Mutex<Vec<Value>>>,
    private_tokens: Arc<Vec<String>>,
    leaked: Arc<AtomicBool>,
}
async fn capture(
    axum::extract::State(state): axum::extract::State<Capture>,
    request: Request,
    next: Next,
) -> Response {
    // Keep only the public path and method, never encrypted bodies or client keys.
    let path = format!("{} {}", request.method(), request.uri().path());
    state.paths.lock().unwrap().push(path.clone());
    if request.uri().query().is_some() {
        state.leaked.store(true, Ordering::Relaxed);
    }
    for (name, value) in request.headers() {
        if let Ok(value) = value.to_str() {
            if state
                .private_tokens
                .iter()
                .any(|token| value.contains(token))
            {
                state.leaked.store(true, Ordering::Relaxed);
            }
        }
        state
            .header_shapes
            .lock()
            .unwrap()
            .push(json!({"name":name.as_str(),"value_bytes":value.as_bytes().len()}));
    }
    if path.ends_with("/query/txpages") && state.interrupt.swap(false, Ordering::SeqCst) {
        return axum::response::IntoResponse::into_response((
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "qualification interruption",
        ));
    }
    next.run(request).await
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

async fn run_range(
    blocks: &[&FrozenBlock],
    fixture: &Fixture,
    corrupt: bool,
) -> Result<Value, Error> {
    let started = Instant::now();
    let temporary = tempfile::tempdir()?;
    let journal = temporary.path().join("journal");
    let publication = temporary.path().join("publication");
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
    let mut events = Vec::new();
    let mut expected = Vec::new();
    for frozen in blocks {
        let bytes = checked_bytes(&frozen.raw, &frozen.sha256)?;
        let block = Block::zcash_deserialize(bytes.as_slice())?;
        let hash = BlockHash::from_internal_bytes(block.hash().0);
        if hash.internal_bytes() != &hex::decode(&frozen.hash)?[..] {
            return Err("confirmed block identity mismatch".into());
        }
        if let Some(previous_hash) = blocks.iter().find(|b| b.height + 1 == frozen.height) {
            if block.header.previous_block_hash.0 != identity(&previous_hash.hash)?.0 {
                return Err("fixture chain discontinuity".into());
            }
        }
        parent_hash.get_or_insert(BlockHash::from_internal_bytes(
            block.header.previous_block_hash.0,
        ));
        if block.transactions.len() != frozen.transactions.len() {
            return Err("fixture transaction count".into());
        }
        for (transaction, facts) in block.transactions.iter().zip(&frozen.transactions) {
            if transaction.hash().0 != identity(&facts.txid)?.0 {
                return Err("confirmed transaction identity mismatch".into());
            }
        }
        let extracted = extract_block(&block.transactions, &mut previous, frozen.height as u32)?;
        for record in &extracted.display {
            let facts = frozen
                .transactions
                .iter()
                .find(|f| f.txid == hex::encode(record.txid.0))
                .ok_or("missing independent facts")?;
            compare(record, facts)?;
        }
        let display_facts: Vec<_> = frozen
            .transactions
            .iter()
            .filter(|f| f.coinbase || f.input_count > 0 || !f.outputs.is_empty())
            .cloned()
            .collect();
        if extracted.display.len() != display_facts.len() {
            return Err("incomplete canonical display extraction".into());
        }
        expected.extend(display_facts);
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
        events.extend(extracted.events);
    }
    store.commit()?;
    drop(store);
    let store = EventStore::open_existing(&journal)?; // Restart at the committed checkpoint.
    let options = PublishOptions::parse_from([
        "shard-publish",
        "--data-dir",
        journal.to_str().unwrap(),
        "--output",
        publication.to_str().unwrap(),
        "--zakura-cookie",
        "/unused-demo-cookie",
        "--recent-geometry",
        "recent-4k",
        "--txid-display",
    ]);
    let map = publish(&options, &store, parent_hash.unwrap())?;
    let set = ShardSet::open(&publication, DEFAULT_RETAIN_REVISIONS)?;
    let service = ServiceState::build(set, ServiceConfig::default())?;
    let metrics = service.metrics().clone();
    let mut tokens = Vec::new();
    for facts in &expected {
        tokens.push(facts.txid.clone());
        tokens.push(hex::encode(
            identity(&facts.txid)?
                .0
                .into_iter()
                .rev()
                .collect::<Vec<_>>(),
        ));
        tokens.extend(
            facts
                .outputs
                .iter()
                .filter(|o| o.script.len() > 8)
                .map(|o| o.script.clone()),
        );
    }
    let trace = Capture {
        private_tokens: Arc::new(tokens),
        ..Default::default()
    };
    let app = router(service).layer(axum::middleware::from_fn_with_state(trace.clone(), capture));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let mut client = PrivateClient::new(format!("http://{address}"));
    if !matches!(
        client.lookup_mined(&map, Txid([4; 32]), None).await?,
        txquery::LookupResult::PlacementUnknown
    ) {
        return Err("unknown placement control".into());
    }
    let mut unsupported = map.clone();
    unsupported.shards[0].txid_segments = None;
    if !matches!(
        client
            .lookup_mined(&unsupported, Txid([4; 32]), Some(map.start_height))
            .await?,
        txquery::LookupResult::Unsupported
    ) || !client.paths.is_empty()
    {
        return Err("unsupported capability dispatched a lookup".into());
    }
    let discovery: Value = client
        .http
        .get(format!("{}/v1/shards/init", client.url))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if discovery["geometries"][0]["txid_tables"]
        .as_array()
        .map_or(0, Vec::len)
        != 2
    {
        return Err("missing display discovery capability".into());
    }
    let entry = &map.shards[0];
    let manifest = client.manifest(entry).await?;
    // Script history and display coexist on the exact same production HTTP endpoint.
    let history_fact = events
        .iter()
        .find(|(s, _)| {
            s.as_slice().len() <= transparent_shard::MAX_SCRIPT_BYTES
                && events.iter().filter(|(other, _)| other == s).count() > 2
        })
        .or_else(|| {
            events
                .iter()
                .find(|(s, _)| s.as_slice().len() <= transparent_shard::MAX_SCRIPT_BYTES)
        });
    let terminal = BlockHash::from_display_hex(&manifest.terminal_block_hash)?;
    let salt =
        transparent_shard::tag::tag_salt(manifest.shard_id, &terminal, manifest.tag_salt_counter);
    async fn history(
        client: &mut PrivateClient,
        manifest: &transparent_shard::ShardManifest,
        script: &[u8],
        tag: [u8; 14],
        event: &transparent_events::TransparentEvent,
    ) -> Result<f64, Error> {
        let now = Instant::now();
        let mut found = false;
        for row in transparent_shard::build::candidate_rows(
            manifest.shard_id,
            script,
            RECENT_4K.directory_rows,
        )
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        {
            for bytes in client
                .row(manifest, &RECENT_4K, Table::Directory, row)
                .await?
            {
                for entry in transparent_shard::records::decode_directory_row_with_schema(
                    &bytes,
                    &manifest.schema,
                )? {
                    if entry.tag == tag {
                        found |= entry.inline.iter().any(|e| e == event);
                        if entry.first_page > 0 {
                            let row = (u64::from(entry.first_page) - 1) % RECENT_4K.page_rows;
                            let pages = client.row(manifest, &RECENT_4K, Table::Pages, row).await?;
                            let mut fragment = None;
                            for page in pages {
                                for part in
                                    transparent_shard::page_row::decode_page_row_with_schema(
                                        &page,
                                        &manifest.schema,
                                    )?
                                {
                                    if part.tag == tag {
                                        fragment = Some(part);
                                    }
                                }
                            }
                            let fragment = fragment.ok_or("history first page missing")?;
                            found |= fragment.events.iter().any(|e| e == event);
                            if fragment.ordinal != 0 || fragment.fragment_count != 1 {
                                return Err("fixture history page shape".into());
                            }
                        }
                    }
                }
            }
        }
        if !found {
            return Err("history native query missing independently extracted event".into());
        }
        Ok(now.elapsed().as_secs_f64())
    }
    let history_cold = if let Some((script, event)) = history_fact {
        let tag = transparent_shard::tag::script_tag(&salt, script.as_slice());
        history(&mut client, &manifest, script.as_slice(), tag, event).await?
    } else {
        0.0
    };
    let before = metrics.resident_bytes.load(Ordering::Relaxed);
    let mut counts = HashMap::<&str, usize>::new();
    let mut large = None;
    let mut payload_bytes = 0usize;
    let mut records = Vec::new();
    for (index, facts) in expected.iter().enumerate() {
        let height = blocks
            .iter()
            .find(|b| b.transactions.iter().any(|f| f.txid == facts.txid))
            .ok_or("accepted fixture placement")?
            .height;
        let record = match client
            .lookup_mined(&map, identity(&facts.txid)?, Some(height))
            .await?
        {
            txquery::LookupResult::Found(record) => record,
            _ => return Err("missing confirmed txid".into()),
        };
        let mut oracle = facts.clone();
        if corrupt && index == 0 {
            oracle.outputs[0].value += 1;
        }
        compare(&record, &oracle)?;
        let bytes = record.encode()?.len();
        payload_bytes += bytes;
        records.push(
            json!({"payload_bytes":bytes,"outputs":record.outputs.len(),"inline":bytes <= 128}),
        );
        if record.coinbase {
            *counts.entry("coinbase").or_default() += 1;
        }
        if facts.txid == ORDINARY_WITH_CHANGE {
            if record.coinbase
                || record.metadata.has_shielded_components
                || record.metadata.transparent_input_count != 1
                || record.outputs.len() != 2
                || record.metadata.fee != FeeState::Exact(10_000)
            {
                return Err("ordinary change fixture summary".into());
            }
            *counts.entry("ordinary_send_with_change").or_default() += 1;
        }
        if record.outputs.len() > 2 {
            *counts.entry("several_outputs").or_default() += 1;
        }
        if !record.coinbase
            && record.metadata.transparent_input_count == 0
            && record.metadata.has_shielded_components
            && !record.outputs.is_empty()
        {
            *counts
                .entry("external_transparent_unshielding")
                .or_default() += 1;
        }
        if record.outputs.iter().any(|o| {
            !(o.script.len() == 25 && o.script.starts_with(&[0x76, 0xa9, 0x14])
                || o.script.len() == 23 && o.script.starts_with(&[0xa9, 0x14]))
        }) {
            *counts.entry("unusual_output_script").or_default() += 1;
        }
        if bytes > 4096 {
            *counts.entry("large_output_list").or_default() += 1;
            large = Some((record.txid, facts.clone()));
        }
        let mut changed = facts.clone();
        changed.outputs[0].value += 1;
        if compare(&record, &changed).is_ok() {
            return Err("altered output negative control failed".into());
        }
        if let Some(fee) = facts.fee.as_u64() {
            changed = facts.clone();
            changed.fee = json!(fee + 1);
            if compare(&record, &changed).is_ok() {
                return Err("altered fee negative control failed".into());
            }
        }
    }
    if !matches!(
        client
            .lookup_mined(&map, Txid([0x55; 32]), Some(entry.start_height))
            .await?,
        txquery::LookupResult::Absent
    ) {
        return Err("missing txid not distinguished".into());
    }
    if let Some((txid, facts)) = large {
        trace.interrupt.store(true, Ordering::SeqCst);
        if client
            .lookup(&manifest, &RECENT_4K, txid, entry.start_height)
            .await
            .is_ok()
        {
            return Err("interrupted overflow claimed complete".into());
        }
        compare(
            &client
                .lookup(&manifest, &RECENT_4K, txid, entry.start_height)
                .await?
                .ok_or("resumed lookup missing")?,
            &facts,
        )?;
    }
    let profile = transparent_native::TableProfile::new(
        &manifest.schema,
        RECENT_4K.name,
        "txdirectory",
        RECENT_4K.directory_rows,
        4096,
    )?;
    let (_, upload) = profile.prepare(0)?;
    let mut wrong_binding =
        transparent_shard::manifest::query_binding(&manifest.digest(), "directory").to_vec();
    wrong_binding.extend(upload);
    let response = client
        .http
        .post(format!(
            "{}/v1/shards/{}/revisions/{}/query/txdirectory",
            client.url, entry.shard_id, entry.manifest_digest
        ))
        .body(wrong_binding)
        .send()
        .await?;
    if response.status() != reqwest::StatusCode::BAD_REQUEST {
        return Err("wrong table binding refusal mismatch".into());
    }
    let response = client
        .http
        .get(format!(
            "{}/v1/shards/{}/revisions/{}/setup/txdirectory/0",
            client.url,
            entry.shard_id,
            "00".repeat(32)
        ))
        .send()
        .await?;
    if response.status() != reqwest::StatusCode::CONFLICT {
        return Err("wrong revision refusal mismatch".into());
    }
    let after = metrics.resident_bytes.load(Ordering::Relaxed);
    let history_warm = if let Some((script, event)) = history_fact {
        let tag = transparent_shard::tag::script_tag(&salt, script.as_slice());
        history(&mut client, &manifest, script.as_slice(), tag, event).await?
    } else {
        0.0
    };
    let paths = trace.paths.lock().unwrap().clone();
    if trace.leaked.load(Ordering::Relaxed) {
        return Err("private identifier in HTTP headers/query parameters".into());
    }
    for path in &paths {
        for facts in &expected {
            let display = hex::encode(
                identity(&facts.txid)?
                    .0
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>(),
            );
            if path.contains(&facts.txid) || path.contains(&display) {
                return Err("private txid in HTTP path".into());
            }
            for output in &facts.outputs {
                if output.script.len() > 8 && path.contains(&output.script) {
                    return Err("private script in HTTP path".into());
                }
            }
        }
        if path.contains('?') {
            return Err("unexpected query parameter".into());
        }
    }
    server.abort();
    let tables = manifest.txid_display.as_ref().unwrap();
    let mut packing = Vec::new();
    for (table, segments) in [
        ("txdirectory", &tables.directory_segments),
        ("txpages", &tables.page_segments),
    ] {
        for (i, geometry) in segments.iter().enumerate() {
            let bytes = std::fs::read(
                publication
                    .join(&entry.manifest_digest)
                    .join(format!("{table}.{i}.bin")),
            )?;
            let occupied = bytes
                .chunks_exact(4096)
                .filter(|row| row[..4] != [0; 4])
                .count();
            let entries: u64 = bytes
                .chunks_exact(4096)
                .map(|row| u32::from_le_bytes(row[..4].try_into().unwrap()) as u64)
                .sum();
            packing.push(json!({"table":table,"segment":i,"bytes":bytes.len(),"rows":geometry.rows,"occupied_rows":occupied,"entries":entries,"sha256":geometry.sha256}));
        }
    }
    Ok(
        json!({"start_height":entry.start_height,"end_height":entry.end_height,"cases":counts,"records":records,"record_count":expected.len(),"payload_bytes":payload_bytes,"queries":client.queries,"upload_bytes":client.uploaded,"download_bytes_including_setup":client.downloaded,"byte_accounting":"Helper application payloads only; successful setup/manifest/native responses and native query bodies, including intentional interrupted-query upload. Excludes HTTP headers, discovery and direct negative-control requests.","manifest_digest":entry.manifest_digest,"display_tables":tables,"packing":packing,"http_safe_paths":paths,"http_header_shapes":trace.header_shapes.lock().unwrap().clone(),"prepared_bytes_before_display":before,"prepared_bytes_after_display":after,"prepared_bytes_growth":after.saturating_sub(before),"memory_accounting":"Runtime-cache reservations, not RSS or transient allocation peaks.","history_supported_script":history_fact.is_some(),"history_cold_seconds":history_cold,"history_with_display_loaded_seconds":history_warm,"elapsed_seconds":started.elapsed().as_secs_f64(),"negative_controls":["missing_txid","wrong_revision","wrong_table_binding","altered_output","altered_fee","interrupted_overflow_if_present","unknown_placement","unsupported_capability"],"checkpoint_restart":true,"publication_reload":true}),
    )
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
    let report = json!({"format":"transparent-txid-native-demo-v1","passed":true,"source_sha":source_sha(),"source_dirty":!std::process::Command::new("git").args(["status","--porcelain"]).output()?.stdout.is_empty(),"metadata_dependency_sha":METADATA_SHA,"fixture_source_sha":fixture.source_sha,"fixture_sha256":hex::encode(Sha256::digest(FIXTURE)),"cargo_lock_sha256":hex::encode(Sha256::digest(lock)),"native_dependency_sha":"1f2aec654d42d8c180373036eb1f6d30ff4ad30c","chain_parser_sha":"af944f5194ef2e9921bc96af017629450375013c","os":std::env::consts::OS,"architecture":std::env::consts::ARCH,"cpu":system.cpus().first().map(|c|c.brand()),"ram_bytes":system.total_memory(),"network":"zcash-mainnet frozen vectors; loopback HTTP; no runtime external lookup","generated_at_unix_seconds":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs(),"command":"make transparent-txid-demo OFFLINE=1 REPORT=<report>","concurrency":1,"repetitions":1,"revision_churn":0,"profile":"release-fast","runs":runs,"elapsed_seconds":started.elapsed().as_secs_f64(),"limitations":"Server protocol evidence only; trusted publisher, range/table/page-count/timing leakage. No production capacity, cryptographic release, wallet recovery or Vizor qualification."});
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
