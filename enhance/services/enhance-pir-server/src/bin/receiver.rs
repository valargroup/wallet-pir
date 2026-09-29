//! Resumable public-chain indexer and continuous canonical receiver PIR service.
use clap::Parser;
use enhance_pir_server::{
    receiver::{MAX_RECEIVER_BATCH_BLOCKS, MAX_RECEIVER_CONCURRENCY},
    zakura::ZakuraClient,
};
use receiver_directory::{
    snapshot::{Snapshot, MAX_ROWS},
    store::{Config, Store},
    witness::WitnessCache,
    Error,
};
use receiver_pir_server::{Publication, Publications};
use std::{
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};
use zakura_chain::{block::Hash, parameters::Network};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    data_dir: PathBuf,
    /// Minimum served row count. Grows by powers of two up to 65536.
    #[arg(long, default_value_t = receiver_pir::MIN_ROWS)]
    min_rows: u32,
    /// Publish common witness data. Requires commitment history from position zero.
    #[arg(long)]
    witnesses: bool,
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
    /// Additional indexing delay. Serving follows the tip and requires zero.
    #[arg(long, default_value_t = 0)]
    confirmations: u32,
    /// Continuously reconcile the canonical chain and atomically rotate the loopback service.
    #[arg(long, conflicts_with = "end_height")]
    serve: bool,
    #[arg(long, default_value = "127.0.0.1:18380")]
    bind: SocketAddr,
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
    poll_seconds: u64,
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
    receiver_pir::validate_rows(args.min_rows)?;
    if u64::from(args.start_height) < enhance_pir::ACTIVATION_HEIGHT {
        return Err("start must be at or after Ironwood activation".into());
    }
    if args.serve && (!args.bind.ip().is_loopback() || args.confirmations != 0) {
        return Err(
            "continuous serving requires a loopback bind and zero confirmation delay".into(),
        );
    }
    let rpc = match &args.cookie {
        Some(p) => ZakuraClient::from_cookie_file(&args.rpc_url, p)?,
        None => ZakuraClient::unauthenticated(&args.rpc_url)?,
    };
    let genesis: Hash = rpc.block_hash(0).await?.parse()?;
    if genesis != Network::Mainnet.genesis_hash() {
        return Err("this indexer currently requires mainnet".into());
    }
    let mut witness_cache = WitnessCache::default();
    if !args.serve {
        refresh(&args, &rpc, genesis, None, &mut witness_cache).await?;
        return Ok(());
    }
    let publications = Publications::default();
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    let app = receiver_pir_server::router_with_publications(publications.clone());
    eprintln!(
        "Receiver PIR listening at {} (waiting for canonical publication)",
        listener.local_addr()?
    );
    // Canonical checks continue while indexing, proofs and PIR preparation are in progress.
    let guard_publications = publications.clone();
    let guard_rpc = rpc.clone();
    let poll_seconds = args.poll_seconds;
    let guard = tokio::spawn(async move {
        loop {
            if let Err(error) = check_serving(&guard_publications, &guard_rpc).await {
                eprintln!("canonical validation failed; revoking serving sessions: {error}");
                guard_publications.revoke();
            }
            tokio::time::sleep(Duration::from_secs(poll_seconds)).await;
        }
    });
    let http = tokio::spawn(async move { axum::serve(listener, app).await });
    let refresh_loop = async {
        loop {
            let started = std::time::Instant::now();
            if let Err(error) = refresh(
                &args,
                &rpc,
                genesis,
                Some(&publications),
                &mut witness_cache,
            )
            .await
            {
                eprintln!("ingestion/publication deferred: {error}");
            }
            eprintln!(
                "receiver refresh elapsed_ms={}",
                started.elapsed().as_millis()
            );
            tokio::time::sleep(Duration::from_secs(args.poll_seconds)).await;
        }
    };
    tokio::select! {
        result = http => { result??; },
        result = guard => { result?; return Err("canonical guard stopped".into()); },
        _ = refresh_loop => {},
        _ = shutdown() => {},
    }
    Ok(())
}

async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Revoke before rewind or rebuild. A shorter node view is handled like Enhance's common ancestor walk.
async fn check_serving(publications: &Publications, rpc: &ZakuraClient) -> Result<()> {
    let anchors = publications.anchors();
    if anchors.is_empty() {
        return Ok(());
    }
    let tip = rpc.tip_height().await?;
    for (height, hash) in anchors {
        if u64::from(height) > tip
            || rpc.block_hash(u64::from(height)).await?.parse::<Hash>()?.0 != hash
        {
            publications.revoke();
            eprintln!("revoked noncanonical receiver sessions");
            break;
        }
    }
    Ok(())
}

async fn refresh(
    args: &Args,
    rpc: &ZakuraClient,
    genesis: Hash,
    serving: Option<&Publications>,
    witness_cache: &mut WitnessCache,
) -> Result<()> {
    let refresh_started = std::time::Instant::now();
    if let Some(serving) = serving {
        if let Err(error) = check_serving(serving, rpc).await {
            serving.revoke();
            return Err(error);
        }
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
    // A saved hash, not elapsed time or provider status, decides which data survives.
    while tip.height > node_tip
        || rpc
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
        if let Some(serving) = serving {
            serving.revoke();
        }
        store.rewind(tip.height, tip.hash)?;
    }
    if end < tip.height {
        return Err("end height precedes stored tip".into());
    }
    if serving.is_some_and(|s| s.anchors().first() == Some(&(end, tip.hash))) && tip.height == end {
        return Ok(());
    }
    // Capture the fence after any rewind; a concurrent revocation invalidates preparation below.
    let epoch = serving.map(Publications::epoch);
    log_stage("canonical_check", refresh_started);
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
    log_stage("ingestion", started);
    let started = std::time::Instant::now();
    let records = store.counts()?.0;
    // Start at half occupancy. A crowded bucket grows the entire candidate.
    let mut rows = u32::try_from((records / 7 + 1).next_power_of_two())?.max(args.min_rows);
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
    log_stage("directory", started);
    if rpc
        .block_hash(u64::from(snapshot.manifest.end_height))
        .await?
        .parse::<Hash>()?
        .0
        != snapshot.manifest.end_hash
    {
        return Err("chain changed before publication; rerun to reconcile".into());
    }
    if args.witnesses {
        let started = std::time::Instant::now();
        let proof = store
            .witnesses_cached(&snapshot.manifest, witness_cache)?
            .encode();
        log_stage("witness_prepare", started);
        let started = std::time::Instant::now();
        let dir = args.data_dir.join("publications");
        std::fs::create_dir_all(&dir)?;
        let mut temp = tempfile::NamedTempFile::new_in(&dir)?;
        temp.write_all(&proof)?;
        temp.as_file().sync_all()?;
        temp.persist(dir.join(format!(
            "{}.witness",
            hex::encode(snapshot.manifest.revision()?)
        )))?;
        log_stage("witness_write", started);
        eprintln!("witness_bytes={}", proof.len());
    }
    if rpc
        .block_hash(u64::from(snapshot.manifest.end_height))
        .await?
        .parse::<Hash>()?
        .0
        != snapshot.manifest.end_hash
    {
        return Err("chain changed while building proofs; rerun to reconcile".into());
    }
    let started = std::time::Instant::now();
    let revision = publish(&args.data_dir.join("publications"), &snapshot)?;
    log_stage("directory_write", started);
    if let Some(serving) = serving {
        let started = std::time::Instant::now();
        let path = args
            .data_dir
            .join("publications")
            .join(format!("{revision}.json"));
        let publication = tokio::task::spawn_blocking(move || Publication::load(&path)).await??;
        log_stage("pir_prepare", started);
        let started = std::time::Instant::now();
        if rpc
            .block_hash(u64::from(publication.manifest().end_height))
            .await?
            .parse::<Hash>()?
            .0
            != publication.manifest().end_hash
        {
            serving.revoke();
            return Err("chain changed during PIR preparation; retrying".into());
        }
        if !serving.publish(publication, epoch.unwrap()) {
            return Err("publication invalidated during preparation; retrying".into());
        }
        log_stage("activate", started);
        eprintln!(
            "receiver publication height={} processing_ms={}",
            snapshot.manifest.end_height,
            refresh_started.elapsed().as_millis()
        );
        if let Err(error) = serving.prune_files(&args.data_dir.join("publications")) {
            eprintln!("obsolete publication cleanup deferred: {error}");
        }
        eprintln!(
            "Receiver PIR published height={} revision={revision}",
            snapshot.manifest.end_height
        );
    }
    println!(
        "{}",
        serde_json::json!({"revision":revision,"start_height":snapshot.manifest.start_height,
        "end_height":snapshot.manifest.end_height,"records":snapshot.manifest.records,
        "actions":snapshot.manifest.end_position-snapshot.manifest.start_position,
        "excluded_coinbase_actions":store.counts()?.1,"rows":rows,"row_bytes":snapshot.data.len()})
    );
    Ok(())
}

/// Stage processing excludes the poll interval and client retries.
fn log_stage(stage: &str, started: std::time::Instant) {
    eprintln!(
        "receiver stage={stage} elapsed_us={}",
        started.elapsed().as_micros()
    );
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
