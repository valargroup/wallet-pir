//! Resumable public-chain indexer and continuous canonical receiver PIR service.
use clap::Parser;
use receiver_directory::{
    snapshot::{Snapshot, MAX_ROWS},
    store::{Config, ProviderStore, Store},
    witness::WitnessCache,
    Error,
};
use receiver_indexer::{
    blocks::{MAX_RECEIVER_BATCH_BLOCKS, MAX_RECEIVER_CONCURRENCY},
    near::{Explorer, Feed},
    zakura::ZakuraClient,
};
use receiver_pir::server::Server;
use receiver_pir_server::{Publication, Publications};
use std::{
    io::Write,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
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
    /// A node's RPC endpoint. Repeat it for fallbacks, tried in order.
    #[arg(long, required = true)]
    rpc_url: Vec<String>,
    #[arg(long, required_unless_present = "no_auth", conflicts_with = "no_auth")]
    cookie: Option<PathBuf>,
    #[arg(long)]
    no_auth: bool,
    #[arg(long, default_value_t = receiver_indexer::blocks::ironwood_activation())]
    start_height: u32,
    #[arg(long)]
    end_height: Option<u32>,
    /// Blocks below the node's tip to publish at, so a short reorg never revokes
    /// sessions. Wallets accept publications up to 100 blocks behind.
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u32).range(0..=50))]
    depth: u32,
    /// Continuously reconcile the canonical chain and atomically rotate the service,
    /// publishing from memory. Without it, one run writes its publication's files.
    #[arg(long, conflicts_with = "end_height")]
    serve: bool,
    /// The service's listener: loopback, or a private IPv4 or unique-local IPv6 address
    /// that only the TLS proxy and monitoring hosts on the private network can reach.
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
    /// Where the NEAR feed starts on its first read, in Unix seconds; a day ago by
    /// default. The feed runs while serving when `NEAR_INTENTS_EXPLORER` holds a key.
    #[arg(long)]
    near_since: Option<i64>,
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(10..))]
    near_poll_seconds: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();
    receiver_pir::validate_rows(args.min_rows)?;
    if args.start_height < receiver_indexer::blocks::ironwood_activation() {
        return Err("start must be at or after Ironwood activation".into());
    }
    if args.witnesses && args.start_height != receiver_indexer::blocks::ironwood_activation() {
        return Err(
            "witnesses need commitments from Ironwood activation; drop --start-height".into(),
        );
    }
    let private = match args.bind.ip() {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
    };
    if args.serve && !private {
        return Err("continuous serving requires a loopback or private bind".into());
    }
    let rpc = match &args.cookie {
        Some(p) => ZakuraClient::from_cookie_file(args.rpc_url.clone(), p)?,
        None => ZakuraClient::unauthenticated(args.rpc_url.clone())?,
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
    info!(
        address = %listener.local_addr()?,
        "receiver PIR listening; waiting for a canonical publication"
    );
    // Canonical checks continue while indexing, proofs and PIR preparation are in progress.
    let guard_publications = publications.clone();
    let guard_rpc = rpc.clone();
    let poll_seconds = args.poll_seconds;
    let guard = tokio::spawn(async move {
        loop {
            if let Err(error) = check_serving(&guard_publications, &guard_rpc).await {
                warn!(%error, "canonical validation deferred; still serving");
            }
            tokio::time::sleep(Duration::from_secs(poll_seconds)).await;
        }
    });
    if let Some(key) = std::env::var("NEAR_INTENTS_EXPLORER")
        .ok()
        .filter(|k| !k.is_empty())
    {
        let path = args.data_dir.join("provider.sqlite");
        let since = args
            .near_since
            .unwrap_or_else(|| unix_now() - receiver_indexer::near::RECENT_SECS);
        let poll = Duration::from_secs(args.near_poll_seconds);
        let mut explorer = Explorer::new(key)?;
        tokio::spawn(async move {
            loop {
                match ProviderStore::open(&path) {
                    Ok(mut store) => {
                        for feed in [Feed::Payouts, Feed::Refunds] {
                            match explorer.sync(&mut store, feed, since).await {
                                Ok(receivers) => {
                                    info!(feed = feed.name(), receivers, "near feed read")
                                }
                                Err(error) => {
                                    warn!(feed = feed.name(), %error, "near feed deferred")
                                }
                            }
                        }
                    }
                    Err(error) => warn!(%error, "near provider store unavailable"),
                }
                tokio::time::sleep(poll).await;
            }
        });
    }
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
                warn!(%error, "ingestion or publication deferred");
            }
            info!(
                elapsed_ms = started.elapsed().as_millis() as u64,
                "receiver refresh"
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

/// Resolves on Ctrl-C, or on SIGTERM on Unix.
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

/// Revokes every session once a node shows a served anchor is off its chain, unless a
/// revocation or rotation stopped serving that anchor during the check (see
/// [`Publications::revoke_serving`]). A failed request or a node behind an anchor
/// proves nothing, so sessions keep serving.
async fn check_serving(publications: &Publications, rpc: &ZakuraClient) -> Result<()> {
    let (epoch, anchors) = publications.serving();
    if anchors.is_empty() {
        return Ok(());
    }
    let tip = rpc.tip_height().await?;
    for (height, hash) in anchors {
        if u64::from(height) <= tip && !is_canonical(rpc, height, hash).await? {
            if publications.revoke_serving(epoch, (height, hash)) {
                warn!(height, "revoked noncanonical receiver sessions");
            }
            break;
        }
    }
    Ok(())
}

/// Brings the index to the requested end on the canonical chain, rewinding past a
/// reorg, then publishes a directory with fresh NEAR filters.
async fn refresh(
    args: &Args,
    rpc: &ZakuraClient,
    genesis: Hash,
    serving: Option<&Publications>,
    witness_cache: &mut WitnessCache,
) -> Result<()> {
    let refresh_started = std::time::Instant::now();
    if let Some(serving) = serving {
        check_serving(serving, rpc).await?;
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
    let mut provider_store = ProviderStore::open(args.data_dir.join("provider.sqlite"))?;
    let node_tip = u32::try_from(rpc.tip_height().await?)?;
    let end = args
        .end_height
        .unwrap_or(node_tip.saturating_sub(args.depth));
    if end < args.start_height || end > node_tip {
        return Err("requested range is outside the available chain".into());
    }
    let mut tip = store.tip()?;
    // A node behind the index proves nothing about the blocks above its tip, so wait for
    // it unless its own tip block differs from the saved one.
    if tip.height > node_tip
        && node_tip >= args.start_height
        && is_canonical(rpc, node_tip, store.checkpoint(node_tip)?.hash).await?
    {
        return Err("node is behind the index; waiting for it".into());
    }
    // A saved hash, not elapsed time or provider status, decides which data survives.
    while tip.height > node_tip || !is_canonical(rpc, tip.height, tip.hash).await? {
        if tip.height < args.start_height {
            return Err("reorg crossed the configured boundary; rebuild explicitly".into());
        }
        tip = store.checkpoint(tip.height - 1)?;
    }
    if tip != store.tip()? {
        if let Some(serving) = serving {
            serving.revoke();
        }
        receiver_indexer::near::rewind(&mut provider_store, &mut store, tip.height, tip.hash)?;
    }
    if end < tip.height {
        return Err("end height precedes stored tip".into());
    }
    if serving.is_some_and(|s| s.anchors().first() == Some(&(end, tip.hash))) && tip.height == end {
        return Ok(());
    }
    // Wait out the previous revision's grace before building the next (see
    // `Publications::ready_at`); a later poll publishes.
    if serving.is_some_and(|s| s.ready_at().is_some()) {
        return Ok(());
    }
    // Capture the fence after any rewind; a concurrent revocation invalidates preparation below.
    let epoch = serving.map(Publications::epoch);
    log_stage("canonical_check", refresh_started);
    let started = std::time::Instant::now();
    let resume_height = tip.height;
    info!(
        start = resume_height + 1,
        target = end,
        batch_size = args.batch_size,
        concurrency = args.concurrency,
        "backfill"
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
        info!(
            height = tip.height,
            target = end,
            recovered = records,
            excluded_coinbase_actions = coinbase,
            blocks_per_second = format_args!("{rate:.2}"),
            "indexed"
        );
    }
    log_stage("ingestion", started);
    let started = std::time::Instant::now();
    let records = store.counts()?.0;
    let provider = receiver_indexer::near::provider_sets(&provider_store)?;
    // Start at half occupancy. A crowded bucket retries the salt, then grows the table.
    let mut rows = u32::try_from((records / 7 + 1).next_power_of_two())?.max(args.min_rows);
    let snapshot = loop {
        if rows > MAX_ROWS {
            return Err("directory exceeds the maximum row count; no publication created".into());
        }
        match store.snapshot(rows, &provider) {
            Ok(s) => break s,
            Err(Error::Capacity) => rows *= 2,
            Err(e) => return Err(e.into()),
        }
    };
    log_stage("directory", started);
    if !is_canonical(
        rpc,
        snapshot.manifest.end_height,
        snapshot.manifest.end_hash,
    )
    .await?
    {
        return Err("chain changed before publication; rerun to reconcile".into());
    }
    let witnesses = if args.witnesses {
        let started = std::time::Instant::now();
        let proof = store.witnesses(&snapshot.manifest, witness_cache)?.encode();
        log_stage("witness_prepare", started);
        info!(witness_bytes = proof.len(), "witnesses prepared");
        Some(proof)
    } else {
        None
    };
    if !is_canonical(
        rpc,
        snapshot.manifest.end_height,
        snapshot.manifest.end_hash,
    )
    .await?
    {
        return Err("chain changed while building proofs; rerun to reconcile".into());
    }
    let revision = hex::encode(snapshot.manifest.revision()?);
    let summary = serde_json::json!({"revision":revision,"start_height":snapshot.manifest.start_height,
        "end_height":snapshot.manifest.end_height,"records":snapshot.manifest.records,
        "actions":snapshot.manifest.end_position-snapshot.manifest.start_position,
        "excluded_coinbase_actions":store.counts()?.1,"rows":rows,"row_bytes":snapshot.data.len(),
        "filter_bytes":snapshot.filters.len(),"filter_sets":snapshot.manifest.filters.iter()
            .map(|set| (set.label.clone(), set.count)).collect::<std::collections::BTreeMap<_, _>>()});
    let Some(serving) = serving else {
        // A one-shot run's only output is its publication's files.
        let started = std::time::Instant::now();
        write_publication(&args.data_dir.join("publications"), &snapshot, witnesses)?;
        log_stage("directory_write", started);
        println!("{summary}");
        return Ok(());
    };
    let started = std::time::Instant::now();
    let end_height = snapshot.manifest.end_height;
    let publication =
        tokio::task::spawn_blocking(move || Publication::new(Server::new(snapshot)?, witnesses))
            .await??;
    log_stage("pir_prepare", started);
    let started = std::time::Instant::now();
    let anchor = publication.manifest();
    if !is_canonical(rpc, anchor.end_height, anchor.end_hash).await? {
        serving.revoke();
        return Err("chain changed during PIR preparation; retrying".into());
    }
    // The report describes the reconciled index this publication was built from, and
    // activates with it.
    let report = receiver_indexer::near::report(&mut provider_store, &store, unix_now())?;
    if !serving.publish(publication.with_report(report), epoch.unwrap()) {
        return Err("publication invalidated during preparation; retrying".into());
    }
    log_stage("activate", started);
    info!(
        height = end_height,
        %revision,
        processing_ms = refresh_started.elapsed().as_millis() as u64,
        publication = %summary,
        "receiver publication"
    );
    Ok(())
}

/// Stage processing excludes the poll interval and client retries.
fn log_stage(stage: &str, started: std::time::Instant) {
    info!(
        stage,
        elapsed_us = started.elapsed().as_micros() as u64,
        "receiver stage"
    );
}

/// Durably write a revision's row and filter files and any witness file, then its
/// manifest.
fn write_publication(root: &Path, snapshot: &Snapshot, witnesses: Option<Vec<u8>>) -> Result<()> {
    std::fs::create_dir_all(root)?;
    let revision = hex::encode(snapshot.manifest.revision()?);
    write_revision_file(root, &format!("{revision}.rows"), &snapshot.data)?;
    write_revision_file(root, &format!("{revision}.filters"), &snapshot.filters)?;
    if let Some(proof) = witnesses {
        // Witnesses are not covered by the revision, so a rebuild replaces them.
        let mut temp = tempfile::NamedTempFile::new_in(root)?;
        temp.write_all(&proof)?;
        temp.as_file().sync_all()?;
        temp.persist(root.join(format!("{revision}.witness")))?;
    }
    let manifest = serde_json::to_vec_pretty(&snapshot.manifest)?;
    write_revision_file(root, &format!("{revision}.json"), &manifest)?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
}

/// Atomically write one content-addressed revision file. An existing file must be identical.
fn write_revision_file(root: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let destination = root.join(name);
    let mut temp = tempfile::NamedTempFile::new_in(root)?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    match temp.persist_noclobber(&destination) {
        Ok(_) => (),
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
            if std::fs::read(destination)? != bytes {
                return Err("existing revision differs".into());
            }
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

/// Seconds since the Unix epoch.
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs() as i64
}

/// Whether `hash` is the node's canonical block at `height`.
async fn is_canonical(rpc: &ZakuraClient, height: u32, hash: [u8; 32]) -> Result<bool> {
    Ok(rpc.block_hash(u64::from(height)).await?.parse::<Hash>()?.0 == hash)
}

#[cfg(test)]
#[path = "../../../crates/receiver-directory/tests/common/mod.rs"]
mod common;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, routing::post, Json, Router};
    use serde_json::{json, Value};
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };
    use tokio::sync::{Notify, Semaphore};

    /// A node's tip and block hashes by height. Each block hash request signals
    /// `asked`, then waits for a permit from `gate` if there is one.
    #[derive(Clone, Default)]
    struct Node {
        tip: Arc<Mutex<u64>>,
        hashes: Arc<Mutex<HashMap<u64, [u8; 32]>>>,
        gate: Option<Arc<Semaphore>>,
        asked: Arc<Notify>,
    }

    /// Serves `node` over JSON-RPC and returns a client for it.
    async fn serve_node(node: Node) -> ZakuraClient {
        async fn handler(State(node): State<Node>, Json(r): Json<Value>) -> Json<Value> {
            let result = match r["method"].as_str().unwrap() {
                "getblockcount" => json!(*node.tip.lock().unwrap()),
                "getblockhash" => {
                    node.asked.notify_one();
                    if let Some(gate) = &node.gate {
                        gate.acquire().await.unwrap().forget();
                    }
                    let height = r["params"][0].as_u64().unwrap();
                    json!(Hash(node.hashes.lock().unwrap()[&height]).to_string())
                }
                method => panic!("unexpected {method}"),
            };
            Json(json!({"result": result, "error": null}))
        }
        let app = Router::new().route("/", post(handler)).with_state(node);
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        ZakuraClient::unauthenticated(vec![url]).unwrap()
    }

    /// An empty publication ending at block `[hash; 32]`, height 101.
    fn publication(hash: u8) -> Publication {
        let mut manifest = super::common::manifest(receiver_pir::MIN_ROWS);
        manifest.end_hash = [hash; 32];
        let snapshot = Snapshot::build(manifest, &[], &[]).unwrap();
        Publication::new(Server::new(snapshot).unwrap(), None).unwrap()
    }

    /// A check that finds an anchor off the chain after a revocation and a new
    /// publication replaced it leaves the new one serving; a served anchor off the
    /// chain is revoked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stale_failure_does_not_revoke_a_newer_publication() {
        let gate = Arc::new(Semaphore::new(0));
        let node = Node {
            gate: Some(gate.clone()),
            ..Node::default()
        };
        *node.tip.lock().unwrap() = 200;
        node.hashes.lock().unwrap().insert(101, [2; 32]);
        let rpc = serve_node(node.clone()).await;
        let publications = Publications::default();
        assert!(publications.publish(publication(1), 0));
        let check = tokio::spawn({
            let (publications, rpc) = (publications.clone(), rpc.clone());
            async move { check_serving(&publications, &rpc).await.unwrap() }
        });
        // While the check of A waits for the node, a rewind revokes A and B is published.
        node.asked.notified().await;
        publications.revoke();
        assert!(publications.publish(publication(2), 1));
        gate.add_permits(1);
        check.await.unwrap();
        assert_eq!(publications.anchors(), [(101, [2; 32])]);
        gate.add_permits(10);
        check_serving(&publications, &rpc).await.unwrap();
        assert_eq!(publications.anchors(), [(101, [2; 32])]);
        node.hashes.lock().unwrap().insert(101, [3; 32]);
        check_serving(&publications, &rpc).await.unwrap();
        assert!(publications.anchors().is_empty());
    }
}
