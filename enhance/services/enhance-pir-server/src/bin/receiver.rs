//! Standalone resumable public-chain indexer. No wallet or receiver query endpoint.
use clap::Parser;
use enhance_pir_server::{
    receiver::{MAX_RECEIVER_BATCH_BLOCKS, MAX_RECEIVER_CONCURRENCY},
    zakura::ZakuraClient,
};
use receiver_directory::{
    snapshot::{Snapshot, MAX_ROWS},
    store::{Config, Store},
    Error,
};
use std::{
    io::Write,
    path::{Path, PathBuf},
};
use zakura_chain::{block::Hash, parameters::Network};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    data_dir: PathBuf,
    #[arg(long)]
    rpc_url: String,
    #[arg(long, required_unless_present = "no_auth", conflicts_with = "no_auth")]
    cookie: Option<PathBuf>,
    #[arg(long)]
    no_auth: bool,
    #[arg(long,default_value_t=enhance_pir::ACTIVATION_HEIGHT as u32)]
    start_height: u32,
    #[arg(long)]
    end_height: Option<u32>,
    #[arg(long, default_value_t = 10)]
    confirmations: u32,
    /// Maximum blocks retained for one validated batch.
    #[arg(long, default_value_t = MAX_RECEIVER_BATCH_BLOCKS, value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_RECEIVER_BATCH_BLOCKS)))]
    batch_size: u32,
    /// Maximum simultaneous raw-block RPC requests.
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u32).range(1..=i64::from(MAX_RECEIVER_CONCURRENCY)))]
    concurrency: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if u64::from(args.start_height) < enhance_pir::ACTIVATION_HEIGHT {
        return Err("start must be at or after Ironwood activation".into());
    }
    let rpc = match args.cookie {
        Some(p) => ZakuraClient::from_cookie_file(&args.rpc_url, p)?,
        None => ZakuraClient::unauthenticated(&args.rpc_url)?,
    };
    let genesis: Hash = rpc.block_hash(0).await?.parse()?;
    if genesis != Network::Mainnet.genesis_hash() {
        return Err("this indexer currently requires mainnet".into());
    }
    let boundary = rpc.receiver_boundary(args.start_height - 1).await?;
    std::fs::create_dir_all(&args.data_dir)?;
    let mut store = Store::open(
        args.data_dir.join("directory.sqlite"),
        Config {
            genesis: genesis.0,
            start_height: args.start_height,
            start_parent: boundary.hash,
            start_position: boundary.position,
        },
    )?;
    let node_tip = u32::try_from(rpc.tip_height().await?)?;
    let end = args
        .end_height
        .unwrap_or(node_tip.saturating_sub(args.confirmations));
    if end < args.start_height || end > node_tip {
        return Err("requested range is outside the available chain".into());
    }
    let mut tip = store.tip()?;
    if tip.height > node_tip {
        return Err("node is behind stored coverage; use a caught-up node".into());
    }
    // A saved hash, not elapsed time or provider status, decides which data survives.
    while rpc
        .block_hash(u64::from(tip.height))
        .await?
        .parse::<Hash>()?
        .0
        != tip.hash
    {
        if tip.height < args.start_height {
            return Err("reorg crossed the configured boundary; rebuild explicitly".into());
        }
        tip = store.checkpoint(tip.height - 1)?;
    }
    if tip != store.tip()? {
        store.rewind(tip.height, tip.hash)?;
    }
    if end < tip.height {
        return Err("end height precedes stored tip".into());
    }
    let started = std::time::Instant::now();
    let resume_height = tip.height;
    eprintln!(
        "backfill start={} target={end} batch_size={} concurrency={}",
        resume_height + 1,
        args.batch_size,
        args.concurrency
    );
    while tip.height < end {
        let batch = rpc
            .receiver_batch(
                &tip,
                (end - tip.height).min(args.batch_size),
                args.concurrency,
            )
            .await?;
        for block in batch {
            store.append(&block)?;
        }
        tip = store.tip()?;
        let (records, coinbase) = store.counts()?;
        let rate = f64::from(tip.height - resume_height) / started.elapsed().as_secs_f64();
        eprintln!("height={} target={end} recovered={records} excluded_coinbase_actions={coinbase} blocks_per_second={rate:.2}",tip.height);
    }
    let records = store.counts()?.0;
    // Start at half occupancy. A crowded bucket grows the entire candidate.
    let mut rows = u32::try_from((records / 7 + 1).next_power_of_two())?;
    let snapshot = loop {
        if rows > MAX_ROWS {
            return Err("directory exceeds prototype geometry; no publication created".into());
        }
        match store.snapshot(rows) {
            Ok(s) => break s,
            Err(Error::Capacity) => rows *= 2,
            Err(e) => return Err(e.into()),
        }
    };
    if rpc
        .block_hash(u64::from(snapshot.manifest.end_height))
        .await?
        .parse::<Hash>()?
        .0
        != snapshot.manifest.end_hash
    {
        return Err("chain changed before publication; rerun to reconcile".into());
    }
    let revision = publish(&args.data_dir.join("publications"), &snapshot)?;
    println!(
        "{}",
        serde_json::json!({"revision":revision,"start_height":snapshot.manifest.start_height,
        "end_height":snapshot.manifest.end_height,"records":snapshot.manifest.records,
        "actions":snapshot.manifest.end_position-snapshot.manifest.start_position,
        "excluded_coinbase_actions":store.counts()?.1,"rows":rows,"row_bytes":snapshot.data.len()})
    );
    Ok(())
}

/// Write revision files before atomically replacing the public manifest pointer.
fn publish(root: &Path, snapshot: &Snapshot) -> Result<String> {
    std::fs::create_dir_all(root)?;
    let revision = hex::encode(snapshot.manifest.revision()?);
    let manifest = serde_json::to_vec_pretty(&snapshot.manifest)?;
    for (name, bytes) in [
        (format!("{revision}.rows"), snapshot.data.as_slice()),
        (format!("{revision}.json"), manifest.as_slice()),
    ] {
        let destination = root.join(name);
        let mut temp = tempfile::NamedTempFile::new_in(root)?;
        temp.write_all(bytes)?;
        temp.as_file().sync_all()?;
        match temp.persist_noclobber(&destination) {
            Ok(_) => (),
            Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
                // An existing content-addressed revision must have identical bytes.
                if std::fs::read(destination)? != bytes {
                    return Err("existing revision differs".into());
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
    let mut current = tempfile::NamedTempFile::new_in(root)?;
    current.write_all(&manifest)?;
    current.as_file().sync_all()?;
    current.persist(root.join("current.json"))?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(revision)
}
