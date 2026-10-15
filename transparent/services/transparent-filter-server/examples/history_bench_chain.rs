//! The chain side of the prepared history runtimes benchmark.
//!
//! `fixture` copies a window of a real v3 journal into a new journal that
//! starts at height 0, as the publication controller requires: block `i`
//! holds the events of source block `first + i`, with their heights
//! rewritten to `i`, under the source block's hash. Nothing is written to
//! the source.
//!
//! `drive` serves a mock node's JSON-RPC (block count and hashes only) over a
//! copy of that journal rolled back to the bootstrap height, and appends the
//! fixture's next blocks on command, logging when each was appended. The
//! controller follows the journal (`follow_journal`) and checks every
//! endpoint against this node, as it does against the real one.
//!
//! Commands on stdin, one per line: `advance <n>` appends n blocks at once;
//! `run <n> <interval_ms>` appends n blocks one per interval, the first
//! after one interval; `quit`. Each prints `done <height>` when finished.

use axum::{extract::State, routing::post, Json, Router};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncBufReadExt;
use transparent_events::TransparentEvent;
use transparent_filter_server::events::EventStore;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Copy `blocks` blocks of `source` from height `first` into a new
    /// journal at `out`, rebased to start at height 0.
    Fixture {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        first: u64,
        #[arg(long)]
        blocks: u64,
        #[arg(long)]
        out: PathBuf,
    },
    /// Serve the mock node and append fixture blocks on command.
    Drive {
        /// The complete fixture journal, read only.
        #[arg(long)]
        full: PathBuf,
        /// A copy of it, which this process writes: rolled back to `base`.
        #[arg(long)]
        journal: PathBuf,
        #[arg(long)]
        base: u64,
        #[arg(long)]
        listen: std::net::SocketAddr,
        /// Where each appended block is logged, one JSON object per line.
        #[arg(long)]
        log: PathBuf,
    },
}

fn unix() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn rebase(
    events: Vec<(transparent_filter::ScriptBytes, TransparentEvent)>,
    height: u32,
) -> Vec<(transparent_filter::ScriptBytes, TransparentEvent)> {
    events
        .into_iter()
        .map(|(script, event)| {
            let event = match event {
                TransparentEvent::Receive(mut receive) => {
                    receive.height = height;
                    TransparentEvent::Receive(receive)
                }
                TransparentEvent::Spend(mut spend) => {
                    spend.height = height;
                    TransparentEvent::Spend(spend)
                }
            };
            (script, event)
        })
        .collect()
}

fn fixture(source: PathBuf, first: u64, blocks: u64, out: PathBuf) -> Result<(), BoxError> {
    let store = EventStore::open_existing(&source)?;
    if store.version() != 3 {
        return Err("the source must be a v3 journal".into());
    }
    let genesis = store
        .block_at(first)
        .ok_or("the source does not hold the first block")?
        .block_hash;
    if out.exists() {
        return Err(format!("{} exists", out.display()).into());
    }
    let mut journal = EventStore::open(&out, &genesis.to_display_hex(), 0)?;
    let mut events_total = 0u64;
    for i in 0..blocks {
        let height = first + i;
        let block = store
            .block_at(height)
            .ok_or_else(|| format!("the source does not hold block {height}"))?;
        let events = store
            .events_at(height)?
            .ok_or_else(|| format!("the source holds no event record for {height}"))?;
        events_total += events.len() as u64;
        journal.append_block(i, block.block_hash, &rebase(events, i as u32))?;
        if i % 1_000 == 999 {
            journal.commit()?;
        }
    }
    journal.commit()?;
    let report = serde_json::json!({
        "source": source,
        "source_start_height": store.start_height(),
        "source_covered_through": store.covered_through(),
        "first": first,
        "blocks": blocks,
        "events": events_total,
        "genesis": genesis.to_display_hex(),
    });
    println!("{report}");
    Ok(())
}

struct Node {
    hashes: Vec<String>,
    tip: AtomicU64,
}

async fn rpc(
    State(node): State<Arc<Node>>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let tip = node.tip.load(Ordering::Acquire);
    let height = request["params"][0]
        .as_u64()
        .or_else(|| request["params"][0].as_str().and_then(|s| s.parse().ok()));
    let (result, error) = match request["method"].as_str() {
        Some("getblockcount") => (serde_json::json!(tip), serde_json::Value::Null),
        Some("getblockhash") => match height.filter(|h| *h <= tip) {
            Some(h) => (
                serde_json::json!(node.hashes[h as usize]),
                serde_json::Value::Null,
            ),
            None => (
                serde_json::Value::Null,
                serde_json::json!({"code": -8, "message": "Block height out of range"}),
            ),
        },
        _ => (
            serde_json::Value::Null,
            serde_json::json!({"code": -32601, "message": "Method not found"}),
        ),
    };
    Json(serde_json::json!({"id": request["id"], "result": result, "error": error}))
}

async fn drive(
    full: PathBuf,
    journal: PathBuf,
    base: u64,
    listen: std::net::SocketAddr,
    log: PathBuf,
) -> Result<(), BoxError> {
    let full = EventStore::open_existing(&full)?;
    let end = full.covered_through().ok_or("the fixture is empty")?;
    let hashes: Vec<String> = (0..=end)
        .map(|h| {
            full.block_at(h)
                .expect("fixture block")
                .block_hash
                .to_display_hex()
        })
        .collect();
    let mut writer = EventStore::open(&journal, &hashes[0], 0)?;
    writer.rollback_to(Some(base))?;
    writer.commit()?;
    let node = Arc::new(Node {
        hashes,
        tip: AtomicU64::new(base),
    });
    let listener = tokio::net::TcpListener::bind(listen).await?;
    let app = Router::new().route("/", post(rpc)).with_state(node.clone());
    tokio::spawn(async move { axum::serve(listener, app).await });
    let mut log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)?;
    let mut append = |writer: &mut EventStore| -> Result<u64, BoxError> {
        use std::io::Write;
        let height = writer.next_height();
        if height > end {
            return Err("the fixture is exhausted".into());
        }
        let block = full.block_at(height).expect("fixture block");
        let events = full.events_at(height)?.ok_or("fixture events missing")?;
        writer.append_block(height, block.block_hash, &events)?;
        writer.commit()?;
        // The node reports the block only once the journal holds it, as a
        // node and its ingest would after the block is committed.
        node.tip.store(height, Ordering::Release);
        writeln!(
            log,
            "{}",
            serde_json::json!({"height": height, "hash": node.hashes[height as usize],
                "events": events.len(), "appended_unix": unix()})
        )?;
        log.flush()?;
        Ok(height)
    };
    println!("ready {base}");
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let words: Vec<&str> = line.split_whitespace().collect();
        match words.as_slice() {
            ["advance", n] => {
                let mut height = 0;
                for _ in 0..n.parse::<u64>()? {
                    height = append(&mut writer)?;
                }
                println!("done {height}");
            }
            ["run", n, interval] => {
                let interval = Duration::from_millis(interval.parse()?);
                let start = tokio::time::Instant::now();
                let mut height = 0;
                for i in 1..=n.parse::<u32>()? {
                    tokio::time::sleep_until(start + interval * i).await;
                    height = append(&mut writer)?;
                }
                println!("done {height}");
            }
            ["quit"] => break,
            _ => println!("error unknown command {line:?}"),
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    match Cli::parse().command {
        Command::Fixture {
            source,
            first,
            blocks,
            out,
        } => fixture(source, first, blocks, out),
        Command::Drive {
            full,
            journal,
            base,
            listen,
            log,
        } => drive(full, journal, base, listen, log).await,
    }
}
