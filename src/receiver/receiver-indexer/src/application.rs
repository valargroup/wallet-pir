//! Receiver ingestion application: canonical collection/commit followed by publication.
//! Chain transport and decoding remain in the upstream server chain adapter.
use crate::{
    canonical::{check_serving, freshest, is_canonical},
    verify,
};
use pir_pipeline::{FailurePolicy, Input};
use receiver_directory::snapshot::{Snapshot, MAX_ROWS};
use receiver_pir_server::{Publication, Publications};
use receiver_store::{Checkpoint, Config as StoreConfig, Error, PayoutStore, Store};
use std::{
    io::Write,
    path::{Path, PathBuf},
};
use tracing::{info, warn};
use zakura_receiver_crypto::witness::WitnessCache;
#[cfg(test)]
use zakura_server_chain::Network;
use zakura_server_chain::NodeClient;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The provider database's file in the configured data directory.
pub const PROVIDERS: &str = "providers.sqlite";

/// Collection and publication settings, independent of CLI parsing and credentials.
pub struct Config {
    pub data_dir: PathBuf,
    pub min_rows: u32,
    pub witnesses: bool,
    pub max_witness_commitments: u64,
    pub start_height: u32,
    pub end_height: Option<u32>,
    pub depth: u32,
    pub batch_size: u32,
    pub concurrency: u32,
    /// Whether to include the optional payout audit in health.
    pub audit_enabled: bool,
    pub filter_blocks: u32,
    pub history_revision: u64,
}

use crate::verify::prepare as prepare_snapshot;

/// The publication [`refresh`] last activated, with what it was built from.
struct Active {
    /// Its terminal height and hash.
    anchor: (u32, [u8; 32]),
    history_revision: u64,
}

/// In-memory publication identity and reusable witness preparation across polls.
/// Recreate on restart; committed progress is recovered from the store.
#[derive(Default)]
pub struct State {
    active: Option<Active>,
    witness_cache: WitnessCache,
}

/// Poll the product refresh operation, retaining its state across source or
/// preparation failures. The caller wires the independent canonical guard and HTTP
/// tasks, and drops this future on shutdown just as it drops either serving task.
pub async fn run(
    config: &Config,
    nodes: &[NodeClient],
    genesis: [u8; 32],
    publications: &Publications,
    state: &mut State,
    poll: std::time::Duration,
) {
    let _ = run_until(
        config,
        nodes,
        genesis,
        publications,
        state,
        poll,
        pir_pipeline::service::Shutdown::default(),
    )
    .await;
}

pub async fn run_until(
    config: &Config,
    nodes: &[NodeClient],
    genesis: [u8; 32],
    publications: &Publications,
    state: &mut State,
    poll: std::time::Duration,
    shutdown: pir_pipeline::service::Shutdown,
) -> Result<()> {
    // Outer shutdown still belongs to the caller; no phase is newly cancellable.
    pir_pipeline::supervise(
        &mut Ingestion {
            config,
            nodes,
            genesis,
            publications,
            state,
        },
        poll,
        shutdown.cancelled(),
    )
    .await
}

struct Ingestion<'a> {
    config: &'a Config,
    nodes: &'a [NodeClient],
    genesis: [u8; 32],
    publications: &'a Publications,
    state: &'a mut State,
}
impl pir_pipeline::Task for Ingestion<'_> {
    type Error = Box<dyn std::error::Error + Send + Sync>;
    async fn run(&mut self) -> Result<()> {
        refresh(
            self.config,
            self.nodes,
            self.genesis,
            Some(self.publications),
            self.state,
        )
        .await
    }
    fn failed(&mut self, error: &Self::Error) -> FailurePolicy {
        warn!(%error, "ingestion or publication deferred");
        FailurePolicy::NextPoll
    }
    fn finished(&mut self, elapsed: std::time::Duration) {
        info!(elapsed_ms = elapsed.as_millis() as u64, "receiver refresh");
    }
}

/// Brings the index to the requested end on the canonical chain of the freshest of
/// `nodes` on `genesis`'s network, which serves the whole pass, rewinding past a
/// reorg, then publishes rolling hints and a directory if that node
/// is still on `genesis`'s network. While
/// serving, `active` stays while it is served at an unmoved tip and was built from the
/// same history correction revision, taking the optional audit's latest report in
/// place; otherwise a new one is built once the previous revision's grace ends.
pub async fn refresh(
    args: &Config,
    nodes: &[NodeClient],
    genesis: [u8; 32],
    serving: Option<&Publications>,
    state: &mut State,
) -> Result<()> {
    let refresh_started = std::time::Instant::now();
    let (node_tip, node) = freshest(nodes, genesis).await?;
    let rpc = &node;
    if let Some(serving) = serving {
        check_serving(serving, rpc, genesis, node_tip).await?;
    }
    let boundary = crate::blocks::boundary(rpc, args.start_height - 1).await?;
    std::fs::create_dir_all(&args.data_dir)?;
    let mut store = Store::open(
        args.data_dir.join("directory.sqlite"),
        StoreConfig {
            genesis,
            start_height: args.start_height,
            start_parent: boundary.hash,
            start_position: boundary.position,
        },
    )?;
    // An empty in-memory store without a key; see `Config::audit_enabled`.
    let provider_store = match args.audit_enabled {
        true => PayoutStore::open(args.data_dir.join(PROVIDERS)).or_else(|error| {
            warn!(%error, "payout audit unavailable; continuing chain publication");
            PayoutStore::open(":memory:")
        })?,
        false => PayoutStore::open(":memory:")?,
    };
    if let Some(collected) = collect(
        args,
        rpc,
        node_tip,
        &mut store,
        serving,
        genesis,
        refresh_started,
    )
    .await?
    {
        collected.publish(&provider_store, state).await?;
    }
    Ok(())
}

/// A completed collection pass, fenced after reconciliation and before appending.
/// Only collection creates this token; publication consumes it separately.
pub struct Collected<'a> {
    args: &'a Config,
    rpc: &'a NodeClient,
    store: &'a mut Store,
    serving: Option<&'a Publications>,
    genesis: [u8; 32],
    tip: receiver_store::Checkpoint,
    epoch: Option<u64>,
    refresh_started: std::time::Instant,
}

/// Reconcile and commit validated blocks, revoking served sessions before rewind.
/// Grace-period backpressure returns `None` without collecting or preparing.
/// The caller must keep the independent canonical guard running throughout both phases.
pub async fn collect<'a>(
    args: &'a Config,
    rpc: &'a NodeClient,
    node_tip: u64,
    store: &'a mut Store,
    serving: Option<&'a Publications>,
    genesis: [u8; 32],
    refresh_started: std::time::Instant,
) -> Result<Option<Collected<'a>>> {
    let input = pir_pipeline::acquire(
        async {
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                match collect_inner(
                    args,
                    rpc,
                    node_tip,
                    store,
                    serving,
                    genesis,
                    refresh_started,
                )
                .await?
                {
                    Some(work) => Input::Work(work),
                    None => Input::Deferred,
                },
            )
        },
        &mut (),
    )
    .await?;
    Ok(match input {
        Input::Work(work) => Some(work),
        Input::Unchanged | Input::Deferred => None,
    })
}

async fn collect_inner<'a>(
    args: &'a Config,
    rpc: &'a NodeClient,
    node_tip: u64,
    store: &'a mut Store,
    serving: Option<&'a Publications>,
    genesis: [u8; 32],
    refresh_started: std::time::Instant,
) -> Result<Option<Collected<'a>>> {
    let node_tip = u32::try_from(node_tip)?;
    let requested = args
        .end_height
        .unwrap_or(node_tip.saturating_sub(args.depth));
    if requested > node_tip {
        return Err("requested range is outside the available chain".into());
    }
    // The configured correction is durable before it revokes sessions or any fallible
    // work runs, so a failure and a rollback restart cannot serve older history.
    crate::revision::advance(store, serving, args.history_revision)?;
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
        store.rewind(tip.height, tip.hash)?;
    }
    // See `Config::depth`; an explicit `--end-height` is kept.
    let end = match args.end_height {
        Some(_) => requested,
        None => requested.max(tip.height),
    };
    if end < args.start_height {
        return Err("requested range is outside the available chain".into());
    }
    if end < tip.height {
        return Err("end height precedes stored tip".into());
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
        let batch = crate::blocks::batch(
            rpc,
            &tip,
            (end - tip.height).min(args.batch_size),
            args.concurrency,
        )
        .await?;
        for block in batch {
            verify::append(store, &block)?;
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
    if let Some(serving) = serving {
        let filter_started = std::time::Instant::now();
        publish_filters(args, rpc, store, serving, genesis, &tip, epoch.unwrap()).await?;
        log_stage("block_filters", filter_started);
    }
    if serving.is_some_and(|s| s.ready_at().is_some()) {
        return Ok(None);
    }

    Ok(Some(Collected {
        args,
        rpc,
        store,
        serving,
        genesis,
        tip,
        epoch,
        refresh_started,
    }))
}

/// Activates the rolling filter window ending at `tip` on `serving`, fenced by `epoch`,
/// before witness and PIR preparation; the canonical guard also checks its anchor while
/// that runs. Only serving builds one: a one-shot run writes no filter artifact. A
/// window `serving` already holds over the same blocks under the same correction
/// revision is kept, so a caught-up service does not rebuild it every poll.
async fn publish_filters(
    args: &Config,
    rpc: &NodeClient,
    store: &mut Store,
    serving: &Publications,
    genesis: [u8; 32],
    tip: &Checkpoint,
    epoch: u64,
) -> Result<()> {
    let first = (tip
        .height
        .saturating_sub(args.filter_blocks.saturating_sub(1)))
    .max(args.start_height);
    let current = serving.filter_window();
    if current.is_some_and(|window| {
        let head = window.head();
        (
            head.genesis,
            head.revision,
            head.first,
            head.tip.height,
            head.tip.hash,
        ) == (genesis, args.history_revision, first, tip.height, tip.hash)
    }) {
        return Ok(());
    }
    let filters = store.block_filters(args.filter_blocks, args.history_revision)?;
    let window =
        receiver_pir_server::blocks::BlockWindow::new(genesis, args.history_revision, filters)?;
    rpc.check_network(genesis).await?;
    if !is_canonical(rpc, tip.height, tip.hash).await? || !serving.publish_filters(window, epoch) {
        return Err("filter anchor changed during collection; retrying".into());
    }
    Ok(())
}

impl<'a> Collected<'a> {
    /// Build from committed records and an optional audit report, then write
    /// files or prepare and activate in memory. Preparation failures keep commits;
    /// anchor revalidation and the captured publication fence govern activation.
    pub async fn publish(self, provider_store: &PayoutStore, state: &'a mut State) -> Result<()> {
        pir_pipeline::advance(
            Input::Work(self),
            |work| work.prepare(provider_store, state),
            |candidate| candidate.activate(),
            &mut (),
        )
        .await?;
        Ok(())
    }

    async fn prepare(
        self,
        provider_store: &PayoutStore,
        state: &'a mut State,
    ) -> Result<Input<Prepared<'a>>> {
        let Self {
            args,
            rpc,
            store,
            serving,
            genesis,
            tip,
            epoch,
            refresh_started,
        } = self;
        let State {
            active,
            witness_cache,
        } = state;
        // The optional audit never prevents chain publication. Off, its empty store needs
        // no terminal block time; on, a time or store that is unavailable degrades its
        // report instead.
        let anchor_time = match args.audit_enabled {
            true => rpc
                .block_time(tip.hash)
                .await
                .map(|time| time.min(unix_now()))
                .map_err(Into::into),
            false => Ok(unix_now()),
        };
        let report = anchor_time
            .and_then(|time| crate::near::capture(provider_store, time))
            .and_then(|capture| capture.report(store))
            .unwrap_or_else(|error| {
                warn!(%error, "payout audit unavailable");
                serde_json::Value::Null
            });
        if let (Some(serving), Some(active)) = (serving, active.as_ref()) {
            if active.anchor == (tip.height, tip.hash)
                && serving.current_anchor() == Some(active.anchor)
                && active.history_revision == args.history_revision
            {
                // The audit never rebuilds a publication; its report refreshes in place.
                if !serving.update_report(active.anchor, report, epoch.unwrap()) {
                    return Err("publication invalidated during the audit refresh; retrying".into());
                }
                return Ok(Input::Unchanged);
            }
        }
        let started = std::time::Instant::now();
        let records = store.counts()?.0;
        // Start at half occupancy. A crowded bucket retries the salt, then grows the table.
        let mut rows = u32::try_from((records / 7 + 1).next_power_of_two())?.max(args.min_rows);
        let snapshot = loop {
            if rows > MAX_ROWS {
                return Err(
                    "directory exceeds the maximum row count; no publication created".into(),
                );
            }
            match store.snapshot(rows, args.history_revision) {
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
            // Blocks only the main future, not a worker: the HTTP server and guard are spawned tasks.
            let proof = match verify::build_witnesses(
                store,
                &snapshot.manifest,
                witness_cache,
                args.max_witness_commitments,
            ) {
                Err(Error::Capacity) => {
                    return Err(format!(
                        "witnesses for {} commitments exceed --max-witness-commitments or the \
                     witness file's node limit; no publication created",
                        snapshot.manifest.end_position
                    )
                    .into())
                }
                proof => proof?,
            };
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
        // Ranking read the genesis before the pass, and an endpoint can be repointed.
        rpc.check_network(genesis).await?;
        let revision = hex::encode(snapshot.manifest.revision()?);
        let summary = serde_json::json!({"revision":revision,"start_height":snapshot.manifest.start_height,
        "end_height":snapshot.manifest.end_height,"records":snapshot.manifest.records,
        "actions":snapshot.manifest.end_position-snapshot.manifest.start_position,
        "excluded_coinbase_actions":store.counts()?.1,"rows":rows,"row_bytes":snapshot.data.len(),
        "history_revision":args.history_revision});
        let candidate = if serving.is_some() {
            let started = std::time::Instant::now();
            let publication =
                tokio::task::spawn_blocking(move || prepare_snapshot(snapshot, witnesses))
                    .await??;
            log_stage("pir_prepare", started);
            Candidate::Serving(publication)
        } else {
            Candidate::Files {
                snapshot,
                witnesses,
            }
        };
        Ok(Input::Work(Prepared {
            candidate,
            args,
            rpc,
            serving,
            epoch,
            active,
            history_revision: args.history_revision,
            report,
            refresh_started,
            revision,
            summary,
        }))
    }
}

enum Candidate {
    Serving(Publication),
    Files {
        snapshot: Snapshot,
        witnesses: Option<Vec<u8>>,
    },
}

struct Prepared<'a> {
    candidate: Candidate,
    args: &'a Config,
    rpc: &'a NodeClient,
    serving: Option<&'a Publications>,
    epoch: Option<u64>,
    active: &'a mut Option<Active>,
    history_revision: u64,
    report: serde_json::Value,
    refresh_started: std::time::Instant,
    revision: String,
    summary: serde_json::Value,
}
impl Prepared<'_> {
    async fn activate(self) -> Result<()> {
        let Self {
            candidate,
            args,
            rpc,
            serving,
            epoch,
            active,
            history_revision,
            report,
            refresh_started,
            revision,
            summary,
        } = self;
        let publication = match candidate {
            Candidate::Files {
                snapshot,
                witnesses,
            } => {
                let started = std::time::Instant::now();
                write_publication(&args.data_dir.join("publications"), &snapshot, witnesses)?;
                log_stage("directory_write", started);
                println!("{summary}");
                return Ok(());
            }
            Candidate::Serving(publication) => publication,
        };
        let serving = serving.expect("serving candidate has publication owner");
        let end_height = publication.manifest().end_height;
        let started = std::time::Instant::now();
        let anchor = publication.manifest();
        if !is_canonical(rpc, anchor.end_height, anchor.end_hash).await? {
            serving.revoke();
            return Err("chain changed during PIR preparation; retrying".into());
        }
        let anchor = (anchor.end_height, anchor.end_hash);
        if !serving.publish(publication.with_report(report), epoch.unwrap()) {
            return Err("publication invalidated during preparation; retrying".into());
        }
        *active = Some(Active {
            anchor,
            history_revision,
        });
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
}

/// Stage processing excludes the poll interval and client retries.
fn log_stage(stage: &str, started: std::time::Instant) {
    info!(
        stage,
        elapsed_us = started.elapsed().as_micros() as u64,
        "receiver stage"
    );
}

/// Durably write a revision's rows and any witness file, then its manifest. A manifest over [`receiver_pir::MAX_MANIFEST_BYTES`], which clients read
/// no more of, or invalid records or witnesses, writes nothing.
fn write_publication(root: &Path, snapshot: &Snapshot, witnesses: Option<Vec<u8>>) -> Result<()> {
    verify::check_publication(snapshot, witnesses.as_deref())?;
    let manifest = serde_json::to_vec_pretty(&snapshot.manifest)?;
    if manifest.len() > receiver_pir::MAX_MANIFEST_BYTES {
        return Err("publication manifest exceeds its size bound; nothing written".into());
    }
    std::fs::create_dir_all(root)?;
    let revision = hex::encode(snapshot.manifest.revision()?);
    write_revision_file(root, &format!("{revision}.rows"), &snapshot.data)?;
    if let Some(proof) = witnesses {
        // Witnesses are not covered by the revision, so a rebuild replaces them.
        let mut temp = tempfile::NamedTempFile::new_in(root)?;
        temp.write_all(&proof)?;
        temp.as_file().sync_all()?;
        temp.persist(root.join(format!("{revision}.witness")))?;
    }
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
pub fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs() as i64
}

#[cfg(test)]
use crate::probe::common;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State as HttpState, routing::post, Json, Router};
    use receiver_pir_server::Publication;
    use serde_json::{json, Value};
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };
    use tokio::sync::{Notify, Semaphore};

    /// A node's tip, block hashes by height and the time in every block header. Each
    /// block hash request runs `hook` if there is one, signals `asked`, then waits for a
    /// permit from `gate` if there is one. A genesis read then applies `repoint`, if
    /// set: the next genesis, or none, so later reads fail.
    #[derive(Clone, Default)]
    struct Node {
        tip: Arc<Mutex<u64>>,
        hashes: Arc<Mutex<HashMap<u64, [u8; 32]>>>,
        time: Arc<Mutex<i64>>,
        repoint: Arc<Mutex<Option<Option<[u8; 32]>>>>,
        hook: Option<Arc<dyn Fn() + Send + Sync>>,
        gate: Option<Arc<Semaphore>>,
        asked: Arc<Notify>,
    }

    /// Serves `node` over JSON-RPC and returns a client for it.
    async fn serve_node(node: Node) -> NodeClient {
        async fn handler(HttpState(node): HttpState<Node>, Json(r): Json<Value>) -> Json<Value> {
            let result = match r["method"].as_str().unwrap() {
                "getblockcount" => json!(*node.tip.lock().unwrap()),
                "getblockhash" => {
                    if let Some(hook) = &node.hook {
                        hook();
                    }
                    node.asked.notify_one();
                    if let Some(gate) = &node.gate {
                        gate.acquire().await.unwrap().forget();
                    }
                    let height = r["params"][0].as_u64().unwrap();
                    let mut hashes = node.hashes.lock().unwrap();
                    let Some(hash) = hashes.get(&height).copied() else {
                        return Json(
                            json!({"result": null, "error": {"code": -8, "message": "no block"}}),
                        );
                    };
                    match node.repoint.lock().unwrap().take_if(|_| height == 0) {
                        Some(Some(genesis)) => hashes.insert(0, genesis),
                        Some(None) => hashes.remove(&0),
                        None => None,
                    };
                    json!(zakura_server_chain::hash::display(&hash))
                }
                "getblockheader" => match *node.time.lock().unwrap() {
                    // A node that cannot give the header.
                    i64::MIN => {
                        return Json(json!({"result": null,
                        "error": {"code": -8, "message": "header unavailable"}}))
                    }
                    time => json!({"time": time}),
                },
                "getblock" => {
                    return Json(json!({"result": null,
                    "error": {"code": -8, "message": "temporarily unavailable"}}))
                }
                method => panic!("unexpected {method}"),
            };
            Json(json!({"result": result, "error": null}))
        }
        let app = Router::new().route("/", post(handler)).with_state(node);
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        NodeClient::new(url, None).unwrap()
    }

    /// An empty publication ending at block `[hash; 32]`, height 101.
    fn publication(hash: u8) -> Publication {
        let mut manifest = super::common::manifest(receiver_pir::MIN_ROWS);
        manifest.end_hash = [hash; 32];
        let snapshot = Snapshot::build(manifest, &[]).unwrap();
        prepare_snapshot(snapshot, None).unwrap()
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
        let mainnet = Network::Mainnet.genesis_hash();
        *node.tip.lock().unwrap() = 200;
        node.hashes
            .lock()
            .unwrap()
            .extend([(0, mainnet), (101, [2; 32])]);
        let rpc = serve_node(node.clone()).await;
        let publications = Publications::default();
        assert!(publications.publish(publication(1), 0));
        let check = tokio::spawn({
            let (publications, rpc) = (publications.clone(), rpc.clone());
            async move {
                check_serving(&publications, &rpc, mainnet, 200)
                    .await
                    .unwrap()
            }
        });
        // While the check of A waits for the node, a rewind revokes A and B is published.
        node.asked.notified().await;
        publications.revoke();
        assert!(publications.publish(publication(2), 1));
        // A's hash, then the genesis that confirms the node's network.
        gate.add_permits(2);
        check.await.unwrap();
        assert_eq!(publications.anchors(), [(101, [2; 32])]);
        gate.add_permits(10);
        check_serving(&publications, &rpc, mainnet, 200)
            .await
            .unwrap();
        assert_eq!(publications.anchors(), [(101, [2; 32])]);
        node.hashes.lock().unwrap().insert(101, [3; 32]);
        check_serving(&publications, &rpc, mainnet, 200)
            .await
            .unwrap();
        assert!(publications.anchors().is_empty());
    }

    /// The report time the serving tests start from, in Unix seconds.
    const NOW: i64 = 1_000_000_000;

    /// A serving indexer whose index and node both stop at Ironwood activation.
    struct Paused {
        dir: tempfile::TempDir,
        args: Config,
        rpc: NodeClient,
        /// The node `rpc` serves, its block headers dated [`NOW`] at first.
        node: Node,
        publications: Publications,
        origin: String,
        state: State,
    }

    impl Paused {
        /// Indexes the activation block and serves it. Each block hash request first
        /// runs `on_hash` on the provider store, as a concurrent feed read would.
        async fn new(on_hash: impl Fn(&mut PayoutStore) + Send + Sync + 'static) -> Self {
            let height = u64::from(crate::blocks::ironwood_activation());
            let dir = tempfile::tempdir().unwrap();
            let provider = dir.path().join(PROVIDERS);
            let node = Node {
                hook: Some(Arc::new(move || {
                    on_hash(&mut PayoutStore::open(&provider).unwrap())
                })),
                ..Node::default()
            };
            *node.tip.lock().unwrap() = height;
            node.hashes.lock().unwrap().extend([
                (0, Network::Mainnet.genesis_hash()),
                (height - 1, [2; 32]),
                (height, [3; 32]),
            ]);
            *node.time.lock().unwrap() = NOW;
            let rpc = serve_node(node.clone()).await;
            let mut store = Store::open(
                dir.path().join("directory.sqlite"),
                StoreConfig {
                    genesis: Network::Mainnet.genesis_hash(),
                    start_height: height as u32,
                    start_parent: [2; 32],
                    start_position: 0,
                },
            )
            .unwrap();
            store
                .append(&receiver_store::IndexedBlock {
                    height: height as u32,
                    hash: [3; 32],
                    parent: [2; 32],
                    start_position: 0,
                    end_position: 0,
                    coinbase_actions: 0,
                    payments: Vec::new(),
                    commitments: Vec::new(),
                })
                .unwrap();
            let args = Config {
                data_dir: dir.path().to_owned(),
                min_rows: receiver_pir::MIN_ROWS,
                witnesses: false,
                max_witness_commitments: 1 << 24,
                start_height: height as u32,
                end_height: None,
                depth: 0,
                batch_size: crate::blocks::MAX_RECEIVER_BATCH_BLOCKS,
                concurrency: 8,
                audit_enabled: true,
                filter_blocks: receiver_store::blocks::MAX_FILTER_BLOCKS,
                history_revision: 0,
            };
            let publications = Publications::default();
            let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", socket.local_addr().unwrap());
            let app = crate::router::router_with_publications(publications.clone());
            tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
            Self {
                dir,
                args,
                rpc,
                node,
                publications,
                origin,
                state: State::default(),
            }
        }

        /// Refreshes, as the serving loop does.
        async fn refresh(&mut self) -> Result<()> {
            refresh(
                &self.args,
                std::slice::from_ref(&self.rpc),
                Network::Mainnet.genesis_hash(),
                Some(&self.publications),
                &mut self.state,
            )
            .await
        }

        /// The service's `/v1/receiver/{path}` response.
        async fn get(&self, path: &str) -> Value {
            let url = format!("{}/v1/receiver/{path}", self.origin);
            reqwest::get(url).await.unwrap().json().await.unwrap()
        }

        fn store(&self) -> Store {
            Store::open(
                self.dir.path().join("directory.sqlite"),
                StoreConfig {
                    genesis: Network::Mainnet.genesis_hash(),
                    start_height: crate::blocks::ironwood_activation(),
                    start_parent: [2; 32],
                    start_position: 0,
                },
            )
            .unwrap()
        }

        /// The provider store.
        fn provider(&self) -> PayoutStore {
            PayoutStore::open(self.dir.path().join(PROVIDERS)).unwrap()
        }
    }

    /// An outage proves nothing about canonicality; a restarted application recovers
    /// its committed index and serves the same session through the real HTTP router.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_failure_keeps_serving_and_restart_recovers() {
        let mut paused = Paused::new(|_| {}).await;
        paused.refresh().await.unwrap();
        let first = paused.get("init").await;
        paused.node.hashes.lock().unwrap().remove(&0);
        assert!(paused.refresh().await.is_err());
        assert_eq!(paused.get("init").await, first);
        paused
            .node
            .hashes
            .lock()
            .unwrap()
            .insert(0, Network::Mainnet.genesis_hash());
        paused.state = State::default();
        paused.refresh().await.unwrap();
        assert_eq!(paused.get("init").await, first);
    }

    /// A raised correction revision is durable before the backfill it needs: when that
    /// backfill fails, the store already refuses a restart that rolls the revision back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_raised_revision_is_durable_before_backfill() {
        let mut paused = Paused::new(|_| {}).await;
        paused.refresh().await.unwrap();
        let next = u64::from(crate::blocks::ironwood_activation()) + 1;
        *paused.node.tip.lock().unwrap() = next;
        paused.node.hashes.lock().unwrap().insert(next, [4; 32]);
        paused.args.history_revision = 1;
        // The node serves no block bodies, so the backfill fails.
        assert!(paused.refresh().await.is_err());
        assert_eq!(paused.store().history_revision().unwrap(), 1);
        assert_eq!(
            paused.publications.epoch(),
            1,
            "older sessions were revoked"
        );
        paused.args.history_revision = 0;
        paused.state = State::default();
        let refused = paused.refresh().await.unwrap_err().to_string();
        assert!(refused.contains("below"), "{refused}");
    }

    /// A one-shot run writes no rolling filters, so it neither builds them nor fails on
    /// them: a filter window that cannot be built leaves its publication unaffected,
    /// while a serving run, which does build it, refuses.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_one_shot_run_does_not_build_rolling_filters() {
        let mut paused = Paused::new(|_| {}).await;
        paused.args.filter_blocks = receiver_store::blocks::MAX_FILTER_BLOCKS + 1;
        let rpc = std::slice::from_ref(&paused.rpc);
        let genesis = Network::Mainnet.genesis_hash();
        refresh(&paused.args, rpc, genesis, None, &mut State::default())
            .await
            .unwrap();
        let written = std::fs::read_dir(paused.dir.path().join("publications")).unwrap();
        assert!(written.count() > 0);
        assert!(paused.refresh().await.is_err());
    }

    /// A caught-up service keeps the rolling window it serves while the tip and
    /// correction revision stay put, rather than rebuilding it every poll, and builds a
    /// new one once the tip moves.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unchanged_tip_keeps_the_rolling_window() {
        let mut paused = Paused::new(|_| {}).await;
        paused.refresh().await.unwrap();
        let window = paused.publications.filter_window().unwrap();
        paused.refresh().await.unwrap();
        assert!(Arc::ptr_eq(
            &window,
            &paused.publications.filter_window().unwrap()
        ));
        // An empty block after the tip, indexed and on the node's chain.
        let next = crate::blocks::ironwood_activation() + 1;
        paused
            .store()
            .append(&receiver_store::IndexedBlock {
                height: next,
                hash: [4; 32],
                parent: [3; 32],
                start_position: 0,
                end_position: 0,
                coinbase_actions: 0,
                payments: Vec::new(),
                commitments: Vec::new(),
            })
            .unwrap();
        *paused.node.tip.lock().unwrap() = u64::from(next);
        paused
            .node
            .hashes
            .lock()
            .unwrap()
            .insert(u64::from(next), [4; 32]);
        paused.refresh().await.unwrap();
        assert!(!Arc::ptr_eq(
            &window,
            &paused.publications.filter_window().unwrap()
        ));
    }

    /// The terminal block's time only dates the optional payout audit: a node that
    /// cannot give it degrades the audit report, and with the audit off it is never
    /// asked for, so publication goes ahead either way.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unavailable_block_time_never_blocks_publication() {
        let mut paused = Paused::new(|_| {}).await;
        *paused.node.time.lock().unwrap() = i64::MIN;
        paused.refresh().await.unwrap();
        let health = paused.get("health").await;
        assert!(health["serving"].is_string());
        assert!(health["indexer"].is_null(), "{health}");
        paused.args.audit_enabled = false;
        paused.publications.revoke();
        paused.state = State::default();
        paused.refresh().await.unwrap();
        let health = paused.get("health").await;
        assert!(health["serving"].is_string());
        assert!(health["indexer"].is_object(), "{health}");
    }

    /// Restoring hints after revocation cannot masquerade as a live PIR publication.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unchanged_anchor_rebuilds_revoked_pir() {
        let mut paused = Paused::new(|_| {}).await;
        paused.refresh().await.unwrap();
        let first = paused.get("init").await;
        paused.publications.revoke();
        assert!(paused.publications.current_anchor().is_none());
        paused.refresh().await.unwrap();
        assert_eq!(paused.get("init").await, first);
        assert!(paused.publications.current_anchor().is_some());
    }

    /// Revocation and durable rewind precede collection: a source that cannot supply
    /// replacement blocks must not leave the orphaned session available over HTTP.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reorg_revokes_before_replacement_source_failure() {
        let mut paused = Paused::new(|_| {}).await;
        paused.refresh().await.unwrap();
        let height = crate::blocks::ironwood_activation();
        paused
            .node
            .hashes
            .lock()
            .unwrap()
            .insert(u64::from(height), [4; 32]);
        assert!(paused.refresh().await.is_err());
        assert!(paused.publications.anchors().is_empty());
        let response = reqwest::get(format!("{}/v1/receiver/init", paused.origin))
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        let store = paused.store();
        assert_eq!(store.tip().unwrap().height, height - 1);
        assert_eq!(store.counts().unwrap(), (0, 0));
    }

    /// Invalid persisted cryptographic input fails in actual PIR preparation, after
    /// collection, preserving both committed progress and the prior HTTP publication.
    /// A restart retries the committed candidate once the stored input is repaired.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn preparation_failure_keeps_commits_and_restart_retries() {
        let mut paused = Paused::new(|_| {}).await;
        paused.refresh().await.unwrap();
        let first = paused.get("init").await;
        let height = crate::blocks::ironwood_activation() + 1;
        let mut record = super::common::record(0, 1);
        record.payment.height = height;
        record.payment.block_hash = [4; 32];
        record.payment.position = 0;
        record.payment.cmx = [255; 32];
        let mut block = receiver_store::IndexedBlock {
            height,
            hash: [4; 32],
            parent: [3; 32],
            start_position: 0,
            end_position: 1,
            coinbase_actions: 0,
            payments: vec![(record.receiver, record.payment)],
            commitments: vec![[255; 32]],
        };
        // Bypass the ingestion verifier to model damaged persisted data. Store::append
        // checks structure; the application's preparation must still check crypto.
        paused.store().append(&block).unwrap();
        *paused.node.tip.lock().unwrap() = u64::from(height);
        paused
            .node
            .hashes
            .lock()
            .unwrap()
            .insert(u64::from(height), [4; 32]);
        let error = paused.refresh().await.unwrap_err();
        assert!(error.to_string().contains("malformed"), "{error}");
        assert_eq!(paused.store().tip().unwrap().height, height);
        assert_eq!(paused.get("init").await, first);
        let mut store = paused.store();
        store.rewind(height - 1, [3; 32]).unwrap();
        block.payments[0].1.cmx = [6; 32];
        block.commitments[0] = [6; 32];
        verify::append(&mut store, &block).unwrap();
        drop(store);
        paused.state = State::default();
        paused.refresh().await.unwrap();
        let next = paused.get("init").await;
        assert_eq!(next["directory"]["end_height"], height);
        assert_eq!(next["directory"]["records"], 1);
        assert_ne!(next, first);
    }

    /// Invalidation after real preparation must fence activation without discarding
    /// the independently committed input. A new pass recovers through the same API.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_pipeline_invalidation_between_preparation_and_activation() {
        let mut paused = Paused::new(|_| {}).await;
        let mut store = paused.store();
        let provider = paused.provider();
        let height = crate::blocks::ironwood_activation();
        let work = collect(
            &paused.args,
            &paused.rpc,
            u64::from(height),
            &mut store,
            Some(&paused.publications),
            Network::Mainnet.genesis_hash(),
            std::time::Instant::now(),
        )
        .await
        .unwrap()
        .unwrap();
        let result = pir_pipeline::advance(
            Input::Work(work),
            |work| async {
                let candidate = work.prepare(&provider, &mut paused.state).await?;
                paused.publications.revoke();
                Ok(candidate)
            },
            |candidate| candidate.activate(),
            &mut (),
        )
        .await;
        assert!(result.is_err());
        assert!(paused.publications.anchors().is_empty());
        assert!(paused.state.active.is_none());
        assert_eq!(store.tip().unwrap().height, height);
        drop(store);
        paused.refresh().await.unwrap();
        assert_eq!(paused.get("init").await["directory"]["end_height"], height);
    }

    /// A response admitted under the old revision retains its immutable bytes
    /// while the shared lifecycle activates different rows at the next anchor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_pipeline_handover_keeps_an_admitted_old_publication_response() {
        use sha2::{Digest, Sha256};
        let mut paused = Paused::new(|_| {}).await;
        paused.refresh().await.unwrap();
        let old = paused.get("init").await;
        let old_manifest: receiver_pir::Manifest = serde_json::from_value(old.clone()).unwrap();
        let height = crate::blocks::ironwood_activation();
        let accepted = receiver_pir::AcceptedCoverage {
            genesis: Network::Mainnet.genesis_hash(),
            required_start: height,
            height,
            hash: [3; 32],
        };
        let public = reqwest::get(format!(
            "{}/v1/receiver/public/{}",
            paused.origin,
            hex::encode(old_manifest.id().unwrap())
        ))
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
        let client = receiver_pir::Client::new(old_manifest.clone(), &public, accepted).unwrap();
        let query = client.prepare(super::common::receiver(), 0).unwrap();
        let response = reqwest::Client::new()
            .post(format!("{}/v1/receiver/query", paused.origin))
            .body(query.body().to_vec())
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        // Reuse the same valid public record as the preparation/restart case;
        // writable store and publication ownership remain independent.
        let height = crate::blocks::ironwood_activation() + 1;
        let mut record = super::common::record(0, 1);
        record.payment.height = height;
        record.payment.block_hash = [4; 32];
        record.payment.position = 0;
        let expected = record.clone();
        verify::append(
            &mut paused.store(),
            &receiver_store::IndexedBlock {
                height,
                hash: [4; 32],
                parent: [3; 32],
                start_position: 0,
                end_position: 1,
                coinbase_actions: 0,
                commitments: vec![record.payment.cmx],
                payments: vec![(record.receiver, record.payment)],
            },
        )
        .unwrap();
        *paused.node.tip.lock().unwrap() = u64::from(height);
        paused
            .node
            .hashes
            .lock()
            .unwrap()
            .insert(u64::from(height), [4; 32]);
        paused.refresh().await.unwrap();
        let next: receiver_pir::Manifest =
            serde_json::from_value(paused.get("init").await).unwrap();
        assert_ne!(
            next.directory.data_sha256,
            old_manifest.directory.data_sha256
        );
        let bytes = response.bytes().await.unwrap();
        // The old empty index has an independently known all-zero row oracle.
        let mut zero_rows = Sha256::new();
        for _ in 0..receiver_pir::MIN_ROWS {
            zero_rows.update([0; receiver_directory::snapshot::ROW_BYTES]);
        }
        assert_eq!(old_manifest.directory.rows, receiver_pir::MIN_ROWS);
        assert_eq!(
            <[u8; 32]>::from(zero_rows.finalize()),
            old_manifest.directory.data_sha256
        );
        assert_eq!(client.decode(query, &bytes).unwrap(), None);

        // The new publication must independently answer the now-present record.
        let public = reqwest::get(format!(
            "{}/v1/receiver/public/{}",
            paused.origin,
            hex::encode(next.id().unwrap())
        ))
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
        let client = receiver_pir::Client::new(
            next,
            &public,
            receiver_pir::AcceptedCoverage {
                height,
                hash: [4; 32],
                ..accepted
            },
        )
        .unwrap();
        let query = client.prepare(expected.receiver, 0).unwrap();
        let response = reqwest::Client::new()
            .post(format!("{}/v1/receiver/query", paused.origin))
            .body(query.body().to_vec())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        assert_eq!(
            client
                .decode(query, &response.bytes().await.unwrap())
                .unwrap(),
            Some(expected)
        );
    }

    /// A refresh with nothing new keeps the publication, and a changed audit report
    /// replaces only its health without rebuilding it: a completed payout stays pending
    /// until the terminal block's time passes its grace, however long the chain pauses.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_paused_chain_refreshes_its_report_without_rebuilding() {
        let mut paused = Paused::new(|_| {}).await;
        // A completed read of the payout feed that saw a payout complete.
        let payout = super::common::receiver();
        paused
            .provider()
            .record(
                "near-payout-audit",
                &[(payout, [7; 32], NOW - 10)],
                NOW - 50,
                NOW - 10,
            )
            .unwrap();
        paused.refresh().await.unwrap();
        let first = paused.get("health").await;
        assert_eq!(first["indexer"]["payouts_missing"], 0);
        paused.refresh().await.unwrap();
        assert_eq!(paused.get("health").await, first);
        assert!(paused.publications.ready_at().is_none());
        // A terminal block past the payout's grace. Any rebuild would now exceed the
        // row limit, so success shows the same session took the new report in place.
        *paused.node.time.lock().unwrap() = NOW - 10 + 3600;
        paused.args.min_rows = u32::MAX;
        paused.refresh().await.unwrap();
        let reported = paused.get("health").await;
        assert_eq!(reported["indexer"]["payouts_missing"], 1);
        assert_eq!(reported["serving"], first["serving"]);
        assert!(paused.publications.ready_at().is_none());
    }

    /// A node on another network with a higher tip is left out of a refresh, so its
    /// chain cannot revoke the served publication.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_node_on_another_network_cannot_revoke_sessions() {
        let mut paused = Paused::new(|_| {}).await;
        paused.refresh().await.unwrap();
        let anchors = paused.publications.anchors();
        let height = u64::from(crate::blocks::ironwood_activation());
        let other = Node::default();
        *other.tip.lock().unwrap() = height + 10;
        other
            .hashes
            .lock()
            .unwrap()
            .extend((0..=height + 10).map(|h| (h, [9; 32])));
        let nodes = [serve_node(other).await, paused.rpc.clone()];
        let genesis = Network::Mainnet.genesis_hash();
        let publications = Some(&paused.publications);
        refresh(
            &paused.args,
            &nodes,
            genesis,
            publications,
            &mut paused.state,
        )
        .await
        .unwrap();
        assert_eq!(paused.publications.anchors(), anchors);
    }

    /// A node that leaves mainnet, or stops giving its genesis, after ranking publishes
    /// nothing, one-shot or serving, though its chain still matches the index; one
    /// still on mainnet publishes both ways.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_node_repointed_during_a_pass_publishes_nothing() {
        let mainnet = Network::Mainnet.genesis_hash();
        for repoint in [Some(Some([9; 32])), Some(None), None] {
            let mut paused = Paused::new(|_| {}).await;
            *paused.node.repoint.lock().unwrap() = repoint;
            let rpc = std::slice::from_ref(&paused.rpc);
            let one_shot = refresh(&paused.args, rpc, mainnet, None, &mut paused.state).await;
            assert_eq!(one_shot.is_ok(), repoint.is_none());
            let written = paused.dir.path().join("publications").exists();
            assert_eq!(written, repoint.is_none());
            *paused.node.repoint.lock().unwrap() = repoint;
            paused.node.hashes.lock().unwrap().insert(0, mainnet);
            assert_eq!(paused.refresh().await.is_ok(), repoint.is_none());
            let serving = !paused.publications.anchors().is_empty();
            assert_eq!(serving, repoint.is_none());
            assert_eq!(paused.state.active.is_some(), repoint.is_none());
        }
    }

    /// A node that leaves mainnet, or stops giving its genesis, after ranking cannot
    /// revoke the served publication with its other chain; one still on mainnet does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_node_repointed_after_ranking_cannot_revoke_sessions() {
        let mainnet = Network::Mainnet.genesis_hash();
        let height = u64::from(crate::blocks::ironwood_activation());
        for repoint in [Some(Some([9; 32])), Some(None), None] {
            let mut paused = Paused::new(|_| {}).await;
            paused.refresh().await.unwrap();
            // The node now answers from a chain without the served anchor.
            paused.node.hashes.lock().unwrap().insert(height, [8; 32]);
            *paused.node.repoint.lock().unwrap() = repoint;
            let nodes = std::slice::from_ref(&paused.rpc);
            let (tip, rpc) = freshest(nodes, mainnet).await.unwrap();
            let checked = check_serving(&paused.publications, &rpc, mainnet, tip).await;
            assert_eq!(checked.is_ok(), repoint.is_none());
            let serving = !paused.publications.anchors().is_empty();
            assert_eq!(serving, repoint.is_some());
        }
    }

    /// A terminal block dated ahead of the wall clock leaves a fresh payout pending.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_future_block_time_reports_no_payout_missing_early() {
        let mut paused = Paused::new(|_| {}).await;
        let (payout, now) = (super::common::receiver(), unix_now());
        paused
            .provider()
            .record(
                "near-payout-audit",
                &[(payout, [7; 32], now - 10)],
                now - 50,
                now - 10,
            )
            .unwrap();
        *paused.node.time.lock().unwrap() = now + 2 * 3600;
        paused.refresh().await.unwrap();
        assert_eq!(paused.get("health").await["indexer"]["payouts_missing"], 0);
    }

    /// A stored tip above the requested depth, as after a restart with a larger
    /// `--depth`, is published rather than waited past.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_larger_depth_publishes_the_stored_tip() {
        let mut paused = Paused::new(|_| {}).await;
        paused.args.depth = 5;
        paused.refresh().await.unwrap();
        let height = crate::blocks::ironwood_activation();
        assert_eq!(paused.get("init").await["directory"]["end_height"], height);
    }

    #[test]
    fn fixture_preparation_returns_protocol_errors_before_reusing_a_server() {
        use receiver_directory::Error::Malformed;
        use receiver_pir::Error::Directory;
        let mut manifest = super::common::manifest(receiver_pir::MIN_ROWS);
        manifest.end_position = 300;
        let valid = super::common::record(0, 1);
        let mut malformed = valid.clone();
        malformed.payment.cmx = [255; 32];
        let snapshot = Snapshot::build(manifest.clone(), &[malformed]).unwrap();
        assert!(matches!(
            prepare_snapshot(snapshot, None),
            Err(Directory(Malformed))
        ));
        let snapshot = Snapshot::build(manifest, &[valid]).unwrap();
        assert!(matches!(
            prepare_snapshot(snapshot, Some(vec![0; 152])),
            Err(Directory(Malformed))
        ));
    }

    /// One-shot output validates persisted records and proofs before creating files.
    #[test]
    fn one_shot_output_refuses_invalid_records_and_proofs_before_writing() {
        let mut manifest = super::common::manifest(receiver_pir::MIN_ROWS);
        manifest.end_position = 300;
        let valid = Snapshot::build(manifest.clone(), &[super::common::record(0, 1)]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        write_publication(&dir.path().join("valid"), &valid, None).unwrap();
        for field in 0..4 {
            let mut record = super::common::record(0, 1);
            match field {
                0 => record.receiver = [255; 43],
                1 => record.payment.cmx = [255; 32],
                2 => record.payment.action_nullifier = [255; 32],
                _ => record.payment.ephemeral_key = [255; 32],
            }
            let snapshot = Snapshot::build(manifest.clone(), &[record]).unwrap();
            let root = dir.path().join(format!("invalid-{field}"));
            assert!(
                write_publication(&root, &snapshot, None).is_err(),
                "field {field}"
            );
            assert!(!root.exists());
        }
        let root = dir.path().join("invalid-proof");
        assert!(write_publication(&root, &valid, Some(vec![0; 152])).is_err());
        assert!(!root.exists());
    }
}
