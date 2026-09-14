//! Reports what an event journal holds, without changing it.
//!
//! The inventory a deployment record needs: the chain the journal is pinned
//! to, the height it began at, the committed end, the event count, the bytes
//! on disk, and how much sits past the checkpoint as an append that never
//! committed. Opened through the read-only path, which is also what a publish
//! uses, so the figures here are exactly what a publish would see.
//!
//! Given a node source, it also confirms the journal's first, last and
//! optional anchor hashes against the node, and reports the node's tip so a
//! reader can see how far behind the journal is.

use clap::Parser;
use std::path::{Path, PathBuf};
use transparent_filter_server::events::EventStore;
use transparent_filter_server::state::StateReader;
use transparent_filter_server::zakura::ZakuraClient;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "journal-inventory",
    about = "Describe an event journal read-only"
)]
struct Cli {
    #[arg(long, default_value = "./transparent-event-data")]
    data_dir: PathBuf,
    /// A height whose hash the record should carry and confirm.
    #[arg(long)]
    anchor_height: Option<u64>,
    /// The node's `[state] cache_dir`; confirms hashes from its RocksDB.
    #[arg(long)]
    state_dir: Option<PathBuf>,
    #[arg(long, default_value = "http://127.0.0.1:8232")]
    zakura_rpc_url: String,
    /// RPC cookie; confirms hashes over RPC when `--state-dir` is not given.
    #[arg(long)]
    zakura_cookie: Option<PathBuf>,
    #[arg(long)]
    out: Option<PathBuf>,
    #[arg(long)]
    source_sha: Option<String>,
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("8 bytes"))
}

/// Hashes by height from whichever node interface is configured.
enum Node {
    State(Box<StateReader>),
    Rpc(tokio::runtime::Runtime, ZakuraClient),
    None,
}

impl Node {
    fn hash(&self, height: u64) -> Result<Option<String>, BoxError> {
        Ok(match self {
            Node::State(reader) => Some(reader.block_hash(height)?),
            Node::Rpc(runtime, client) => Some(runtime.block_on(client.block_hash(height))?),
            Node::None => None,
        })
    }
    fn tip(&self) -> Result<Option<u64>, BoxError> {
        Ok(match self {
            Node::State(reader) => Some(reader.tip_height()?),
            Node::Rpc(runtime, client) => Some(runtime.block_on(client.tip_height())?),
            Node::None => None,
        })
    }
    fn label(&self) -> &'static str {
        match self {
            Node::State(_) => "state",
            Node::Rpc(..) => "rpc",
            Node::None => "none",
        }
    }
}

fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let store = EventStore::open_existing(&cli.data_dir)?;
    let dir = &cli.data_dir;

    let checkpoint_path = dir.join("checkpoint.bin");
    let checkpoint = std::fs::read(&checkpoint_path).ok().and_then(|bytes| {
        (bytes.len() == 16).then(|| (read_u64(&bytes[0..8]), read_u64(&bytes[8..16])))
    });
    let events_len = file_len(&dir.join("events.bin"));
    let blocks_len = file_len(&dir.join("blocks.bin"));
    let (committed_events, committed_blocks) = checkpoint.unwrap_or((0, 0));

    let mut superseded: Vec<String> = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("superseded"))
        .collect();
    superseded.sort();

    let node = match (&cli.state_dir, &cli.zakura_cookie) {
        (Some(state), _) => Node::State(Box::new(StateReader::open(
            state,
            zakura_chain::parameters::Network::Mainnet,
        )?)),
        (None, Some(cookie)) => Node::Rpc(
            tokio::runtime::Runtime::new()?,
            ZakuraClient::from_cookie_file(&cli.zakura_rpc_url, cookie)?,
        ),
        (None, None) => Node::None,
    };

    let mut confirmations = Vec::new();
    let mut mismatches = 0;
    let mut confirm = |label: &str, height: u64| -> Result<(), BoxError> {
        let journal = store
            .block_at(height)
            .map(|entry| entry.block_hash.to_display_hex());
        let node_hash = node.hash(height)?;
        let agrees = match (&journal, &node_hash) {
            (Some(a), Some(b)) => Some(a == b),
            _ => None,
        };
        if agrees == Some(false) {
            mismatches += 1;
        }
        confirmations.push(serde_json::json!({
            "label": label,
            "height": height,
            "journal_hash": journal,
            "node_hash": node_hash,
            "agrees": agrees,
        }));
        Ok(())
    };
    confirm("start", store.start_height())?;
    if let Some(covered) = store.covered_through() {
        confirm("covered_through", covered)?;
    }
    if let Some(anchor) = cli.anchor_height {
        confirm("anchor", anchor)?;
    }
    let genesis_agrees = node.hash(0)?.map(|hash| hash == store.genesis_hash());
    if genesis_agrees == Some(false) {
        mismatches += 1;
    }

    let record = serde_json::json!({
        "schema": "transparent-journal-inventory-v1",
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "tool_sha": cli.source_sha,
        "data_dir": dir,
        "open": "EventStore::open_existing (read-only; the uncommitted tail is ignored)",
        "network": transparent_filter::NETWORK,
        "genesis_hash": store.genesis_hash(),
        "genesis_agrees_with_node": genesis_agrees,
        "start_height": store.start_height(),
        "covered_through": store.covered_through(),
        "next_height": store.next_height(),
        "blocks_covered": store.blocks_covered(),
        "events_stored": store.events_stored(),
        "files": {
            "events_bin_bytes": events_len,
            "blocks_bin_bytes": blocks_len,
            "checkpoint": checkpoint.map(|(events, blocks)| serde_json::json!({
                "events_bytes": events,
                "blocks_bytes": blocks,
            })),
            "uncommitted_event_bytes": events_len.saturating_sub(committed_events),
            "uncommitted_block_bytes": blocks_len.saturating_sub(committed_blocks),
        },
        "superseded_directories": superseded,
        "node": {
            "source": node.label(),
            "tip_height": node.tip()?,
        },
        "confirmations": confirmations,
        "mismatches": mismatches,
    });
    let bytes = serde_json::to_vec_pretty(&record)?;
    match &cli.out {
        Some(path) => std::fs::write(path, &bytes)?,
        None => println!("{}", String::from_utf8(bytes)?),
    }
    if mismatches > 0 {
        return Err(format!("{mismatches} hash confirmation(s) disagree with the node").into());
    }
    Ok(())
}
