//! Deterministic recent-tail publication benchmark.
//!
//! Replays the retained public mainnet day (`transparent/evidence/baselines/
//! mainnet-study/mainnet-day.jsonl.gz`, decompressed) into a v3 journal as
//! consecutive copies, then measures what the continuous controller does per
//! block: copy a journal snapshot, publish the grown `recent-8k` tail beside
//! the previous publication, and reopen the candidate's public filters. It
//! then times the native cold runtime for each table segment of the final
//! tail, which is what every recent replica builds before it is warm.
//!
//! Copies keep the day's transactions, values and per-transaction structure.
//! Copy `c > 0` renames txids and block hashes, and gives about 70% of the
//! day's scripts a fresh identity so the shard sees a realistic mix of repeat
//! and new scripts instead of one day's scripts with ever-longer histories.
//! Fees are exact where every input value is known, as the v11 journal records
//! them; transactions whose outputs exceed their transparent inputs are
//! recorded as having shielded components with an unknown fee. This is a
//! representative synthetic tail derived from real data, not a replay of a
//! production tail.
//!
//! Usage:
//!   gzip -dc mainnet-day.jsonl.gz > day.jsonl
//!   publication_bench --day day.jsonl --work DIR --initial-copies 13.5 \
//!       --cycles 5 [--runtime] [--record out.json]
use clap::Parser;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    io::BufRead,
    path::{Path, PathBuf},
    time::Instant,
};
use transparent_events::{
    FeeState, ReceiveEvent, SpendEvent, TransactionMetadata, TransparentEvent, Txid,
};
use transparent_filter::{BlockHash, ScriptBytes};
use transparent_filter_server::{
    controller::Snapshot,
    events::EventStore,
    publication::{self, DirectoryChoice, PublishOptions},
    shard_filters::ShardFilters,
};

#[derive(Parser)]
struct Cli {
    /// Decompressed `mainnet-day.jsonl`.
    #[arg(long)]
    day: PathBuf,
    /// Working directory; the journal and publications are created here.
    #[arg(long)]
    work: PathBuf,
    /// Day copies in the journal before the first measured cycle. Fractions
    /// take a prefix of the day.
    #[arg(long, default_value_t = 13.5)]
    initial_copies: f64,
    /// Measured publication cycles, each one block further.
    #[arg(long, default_value_t = 5)]
    cycles: usize,
    #[arg(long, value_enum, default_value_t = DirectoryChoice::All)]
    directory_choice: DirectoryChoice,
    /// Also time native cold runtime builds for the final tail's tables.
    #[arg(long)]
    runtime: bool,
    /// Runtime builds per table segment.
    #[arg(long, default_value_t = 1)]
    runtime_repeats: usize,
    /// Write a JSON record of every measurement here.
    #[arg(long)]
    record: Option<PathBuf>,
}

#[derive(Deserialize)]
struct Block {
    height: u32,
    transactions: Vec<Tx>,
}
#[derive(Deserialize)]
struct Tx {
    index: u16,
    txid: String,
    vin: Vec<Input>,
    vout: Vec<Output>,
}
#[derive(Deserialize)]
struct Input {
    txid: String,
    n: u32,
    script: String,
    value_zat: Option<u64>,
}
#[derive(Deserialize)]
struct Output {
    n: u32,
    value_zat: u64,
    script: String,
}

const START: u64 = 3_000_000;
const MAINNET_GENESIS: &str = "00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08";

fn txid(text: &str, copy: u32) -> Txid {
    let mut bytes: [u8; 32] = hex::decode(text).unwrap().try_into().unwrap();
    bytes.reverse();
    let mut word = u32::from_le_bytes(bytes[..4].try_into().unwrap());
    word = word.wrapping_add(copy.wrapping_mul(0x9e37_79b9));
    bytes[..4].copy_from_slice(&word.to_le_bytes());
    Txid(bytes)
}

fn supported(script: &[u8]) -> bool {
    script.len() == 25 && script.starts_with(&[0x76, 0xa9, 0x14]) && script.ends_with(&[0x88, 0xac])
        || script.len() == 23 && script.starts_with(&[0xa9, 0x14]) && script.ends_with(&[0x87])
}

/// About 70% of scripts are new in each later copy; the rest recur.
fn script(text: &str, copy: u32) -> ScriptBytes {
    let mut bytes = hex::decode(text).unwrap();
    if copy > 0 && supported(&bytes) && Sha256::digest(&bytes)[0] < 179 {
        let at = if bytes[0] == 0x76 { 3 } else { 2 };
        let mut word = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        word ^= copy.wrapping_mul(0x85eb_ca6b);
        bytes[at..at + 4].copy_from_slice(&word.to_le_bytes());
    }
    ScriptBytes::new(bytes)
}

fn block_events(block: &Block, height: u64, copy: u32) -> Vec<(ScriptBytes, TransparentEvent)> {
    let mut events = Vec::new();
    for tx in &block.transactions {
        let coinbase = tx.index == 0;
        let id = txid(&tx.txid, copy);
        let out: u64 = tx.vout.iter().map(|o| o.value_zat).sum();
        let known: Option<u64> = tx.vin.iter().map(|i| i.value_zat).sum();
        let metadata = if coinbase {
            TransactionMetadata {
                fee: FeeState::NotApplicable,
                transparent_input_count: 0,
                has_shielded_components: false,
            }
        } else {
            match known {
                Some(input) if input >= out => TransactionMetadata {
                    fee: FeeState::Exact(input - out),
                    transparent_input_count: tx.vin.len() as u32,
                    has_shielded_components: false,
                },
                _ => TransactionMetadata {
                    fee: FeeState::Unknown,
                    transparent_input_count: tx.vin.len() as u32,
                    has_shielded_components: true,
                },
            }
        };
        for output in &tx.vout {
            let script = script(&output.script, copy);
            if !supported(script.as_slice()) {
                continue;
            }
            events.push((
                script,
                TransparentEvent::Receive(ReceiveEvent {
                    metadata: Some(metadata),
                    height: height as u32,
                    txid: id,
                    transaction_index: tx.index,
                    output_index: output.n,
                    value: output.value_zat,
                    coinbase,
                }),
            ));
        }
        if coinbase {
            continue;
        }
        for (index, input) in tx.vin.iter().enumerate() {
            let script = script(&input.script, copy);
            if !supported(script.as_slice()) {
                continue;
            }
            events.push((
                script,
                TransparentEvent::Spend(SpendEvent {
                    metadata: Some(metadata),
                    height: height as u32,
                    spending_txid: id,
                    transaction_index: tx.index,
                    input_index: index as u32,
                    spent_txid: txid(&input.txid, copy),
                    spent_output_index: input.n,
                }),
            ));
        }
    }
    events
}

fn block_hash(height: u64) -> BlockHash {
    let mut hash = Sha256::new();
    hash.update(b"publication-bench-block\0");
    hash.update(height.to_le_bytes());
    BlockHash::from_internal_bytes(hash.finalize().into())
}

/// Appends journal heights up to `through`, one durable block at a time.
fn extend(store: &mut EventStore, day: &[Block], through: u64) {
    while store.next_height() <= through {
        let height = store.next_height();
        // Height START is the single archive block; the day replays after it.
        let events = if height == START {
            Vec::new()
        } else {
            let offset = (height - START - 1) as usize;
            let block = &day[offset % day.len()];
            debug_assert!(block.height > 0);
            block_events(block, height, (offset / day.len()) as u32)
        };
        store
            .append_block(height, block_hash(height), &events)
            .unwrap();
        if height.is_multiple_of(512) || height == through {
            store.commit().unwrap();
        }
    }
}

fn options(output: &Path, previous: Option<&Path>, choice: DirectoryChoice) -> PublishOptions {
    PublishOptions {
        data_dir: PathBuf::new(),
        output: output.to_owned(),
        previous: previous.map(Path::to_owned),
        recent_geometry: "recent-8k".into(),
        archive_geometry: Some("archive-wide".into()),
        recent_from: Some(START + 1),
        zakura_rpc_url: String::new(),
        zakura_cookie: PathBuf::new(),
        through: None,
        record: None,
        source_sha: None,
        directory_choice: choice,
        range_profile: transparent_filter::RANGE_PROFILE_V2.name.to_string(),
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "transparent_shard_server::runtime=debug".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let day: Vec<Block> = std::io::BufReader::new(std::fs::File::open(&cli.day).unwrap())
        .lines()
        .filter_map(|line| {
            let value: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
            (value["type"] == "block").then(|| serde_json::from_value(value).unwrap())
        })
        .collect();
    let day_sha256 = hex::encode(Sha256::digest(std::fs::read(&cli.day).unwrap()));
    std::fs::create_dir_all(&cli.work).unwrap();
    let journal_dir = cli.work.join("journal");
    let mut store = EventStore::open(&journal_dir, MAINNET_GENESIS, START).unwrap();
    let initial = START + (cli.initial_copies * day.len() as f64).round() as u64;
    if store.covered_through().is_some_and(|end| end > initial) {
        store.rollback_to(Some(initial)).unwrap();
    }
    let started = Instant::now();
    extend(&mut store, &day, initial);
    eprintln!(
        "journal through {initial} in {:.1}s",
        started.elapsed().as_secs_f64()
    );

    let zero = BlockHash::from_internal_bytes([0; 32]);
    let base = cli.work.join("publication-base");
    let started = Instant::now();
    let mut previous_map = if base.join("shards.json").exists() {
        let map: transparent_filter::ShardMap =
            serde_json::from_slice(&std::fs::read(base.join("shards.json")).unwrap()).unwrap();
        if map.shards.last().map(|s| s.end_height) != Some(initial) {
            std::fs::remove_dir_all(&base).unwrap();
            publication::publish(&options(&base, None, cli.directory_choice), &store, zero).unwrap()
        } else {
            map
        }
    } else {
        publication::publish(&options(&base, None, cli.directory_choice), &store, zero).unwrap()
    };
    let base_seconds = started.elapsed().as_secs_f64();
    let mut previous = base.clone();
    let runs = cli.work.join("cycles");
    let _ = std::fs::remove_dir_all(&runs);
    std::fs::create_dir_all(&runs).unwrap();
    let mut cycles = Vec::new();
    for cycle in 0..cli.cycles {
        let height = store.next_height();
        extend(&mut store, &day, height);
        let output = runs.join(format!("candidate-{height}"));
        std::fs::create_dir(&output).unwrap();
        let started = Instant::now();
        let snapshot = Snapshot::capture(&store, &previous_map).unwrap();
        let capture = started.elapsed().as_secs_f64();
        let publish_started = Instant::now();
        let map = publication::publish(
            &options(&output, Some(&previous), cli.directory_choice),
            &snapshot,
            zero,
        )
        .unwrap();
        let publish = publish_started.elapsed().as_secs_f64();
        let open_started = Instant::now();
        let filters = ShardFilters::open(&output).unwrap();
        let open = open_started.elapsed().as_secs_f64();
        let tail = map.shards.last().unwrap();
        let record = serde_json::json!({
            "cycle": cycle,
            "height": height,
            "capture_seconds": capture,
            "publish_seconds": publish,
            "filters_open_seconds": open,
            "total_seconds": started.elapsed().as_secs_f64(),
            "map_sha256": filters.map_digest(),
            "shards": map.shards.len(),
            "tail": {
                "shard_id": tail.shard_id,
                "start_height": tail.start_height,
                "end_height": tail.end_height,
                "scripts": tail.scripts,
                "page_rows": tail.page_rows,
                "revision": tail.revision,
                "manifest_digest": tail.manifest_digest,
            },
        });
        eprintln!("{record}");
        cycles.push(record);
        previous = output;
        previous_map = map;
    }

    let mut runtime = Vec::new();
    if cli.runtime {
        use transparent_shard_server::runtime::{SharedParams, TableRuntime};
        use transparent_shard_server::shardset::{ShardSet, Table};
        let set = ShardSet::open(&previous, 3).unwrap();
        let tail = previous_map.shards.last().unwrap();
        let shard = set.revision(&tail.manifest_digest).unwrap();
        for table in [Table::Directory, Table::Pages] {
            let shared = SharedParams::build(shard.geometry, table).unwrap();
            for segment in 0..shard.segments(table) {
                let bytes = shard.segment(table, segment).unwrap().load().unwrap();
                for repeat in 0..cli.runtime_repeats {
                    let started = Instant::now();
                    let built = transparent_shard_server::runtime::build_pool()
                        .install(|| TableRuntime::build(&shared, &bytes))
                        .unwrap();
                    let record = serde_json::json!({
                        "table": table.as_str(),
                        "segment": segment,
                        "repeat": repeat,
                        "build_threads": transparent_shard_server::runtime::build_pool()
                            .current_num_threads(),
                        "seconds": started.elapsed().as_secs_f64(),
                        "public_params_sha256": built.public_params_sha256,
                    });
                    eprintln!("{record}");
                    runtime.push(record);
                }
            }
        }
    }

    if let Some(path) = &cli.record {
        let record = serde_json::json!({
            "schema": "transparent-publication-bench-v1",
            "day_sha256": day_sha256,
            "day_blocks": day.len(),
            "initial_copies": cli.initial_copies,
            "initial_through": initial,
            "directory_choice": cli.directory_choice,
            "base_publication_seconds": base_seconds,
            "cycles": cycles,
            "runtime": runtime,
        });
        std::fs::write(path, serde_json::to_vec_pretty(&record).unwrap()).unwrap();
    }
}
