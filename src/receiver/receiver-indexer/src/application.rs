//! Receiver ingestion application: canonical collection/commit followed by publication.
//! Chain transport and decoding remain in the upstream server chain adapter.
use crate::{
    canonical::{check_serving, freshest, is_canonical},
    seal::{Policy, Sealer},
    verify,
};
use pir_pipeline::{FailurePolicy, Input};
use receiver_directory::{shards::ShardFilter, witness};
#[cfg(test)]
use receiver_pir_server::publication::DEFAULT_RETENTION;
use receiver_pir_server::{publication::Segment, tables::FilterSource, Publication, Publications};
use receiver_store::{Checkpoint, Config as StoreConfig, Error, PayoutStore, SealedShard, Store};
use sha2::Digest;
use std::sync::Arc;
use std::{
    io::Write,
    path::{Path, PathBuf},
};
use tracing::{info, warn};
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
    pub start_height: u32,
    pub end_height: Option<u32>,
    pub depth: u32,
    pub batch_size: u32,
    pub concurrency: u32,
    /// Whether to include the optional payout audit in health.
    pub audit_enabled: bool,
    /// An operator cap on the rolling filter window; see [`crate::blocks::filter_window_blocks`].
    pub filter_blocks: Option<u32>,
    pub history_revision: u64,
    /// Pinned payment table rows of sealed shards' segments.
    pub shard_rows: u32,
    /// Payments after which a shard seals; see [`Policy::new`] for the default.
    pub shard_target: Option<u64>,
}

#[cfg(test)]
use crate::verify::prepare as prepare_snapshot;

/// The publication [`refresh`] last activated, with what it was built from.
struct Active {
    /// Its terminal height and hash.
    anchor: (u32, [u8; 32]),
    history_revision: u64,
}

/// In-memory publication identity, the filling Ironwood shard's paths, the open shard's
/// projection and sealed shards' prepared sessions across polls. Recreate on restart:
/// committed progress, seals and completed witnesses are recovered from the store.
#[derive(Default)]
pub struct State {
    active: Option<Active>,
    witnesses: crate::witness::Witnesses,
    sealer: Option<Sealer>,
    kept: Vec<Kept>,
    durable: Option<crate::durable::Durable>,
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
/// place; otherwise a new one is built once the retained set has room.
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
    let config = StoreConfig {
        genesis,
        start_height: args.start_height,
        start_parent: boundary.hash,
        start_position: boundary.position,
    };
    let (mut store, converted) = receiver_store::open_in(&args.data_dir, config, true)?;
    if converted {
        info!("converted the version 2 receiver store; the original is kept for rollback");
    }
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
    /// The correction revision this pass publishes; see [`collect`].
    history_revision: u64,
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
    // An offline correction (see [`correct`]) may have advanced the store past the
    // configured revision; publish the higher one rather than refuse to collect. It is
    // durable before it revokes sessions or any fallible work runs, so a failure and a
    // rollback restart cannot serve older history.
    let history_revision = args.history_revision.max(store.history_revision()?);
    crate::revision::advance(store, serving, history_revision)?;
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
        history_revision,
    }))
}

/// Activates the rolling filter window ending at `tip` on `serving`, fenced by `epoch`,
/// before witness and PIR preparation; the canonical guard also checks its anchor while
/// that runs, under the correction revision the store records. Only serving builds
/// one: a one-shot run writes no filter artifact. A window `serving` already holds over
/// the same blocks under the same correction revision is kept, so a caught-up service does not rebuild it every poll.
async fn publish_filters(
    args: &Config,
    rpc: &NodeClient,
    store: &mut Store,
    serving: &Publications,
    genesis: [u8; 32],
    tip: &Checkpoint,
    epoch: u64,
) -> Result<()> {
    // The revision this pass collects under, which `revision::advance` recorded.
    let revision = store.history_revision()?;
    let blocks = crate::blocks::filter_window_blocks(tip.height, args.filter_blocks);
    let first = (tip.height.saturating_sub(blocks - 1)).max(args.start_height);
    let current = serving.filter_window();
    if current.is_some_and(|window| {
        let head = window.head();
        (
            head.genesis,
            head.revision,
            head.first,
            head.tip.height,
            head.tip.hash,
        ) == (genesis, revision, first, tip.height, tip.hash)
    }) {
        return Ok(());
    }
    let filters = store.block_filters(blocks, revision)?;
    let window = receiver_pir_server::blocks::BlockWindow::new(genesis, revision, filters)?;
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
            history_revision,
        } = self;
        let State {
            active,
            witnesses,
            sealer,
            kept,
            durable,
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
                && active.history_revision == history_revision
            {
                // The audit never rebuilds a publication; its report refreshes in place.
                if !serving.update_report(active.anchor, report, epoch.unwrap()) {
                    return Err("publication invalidated during the audit refresh; retrying".into());
                }
                return Ok(Input::Unchanged);
            }
        }
        let started = std::time::Instant::now();
        let policy = Policy::new(args.shard_rows, args.shard_target)?;
        if sealer.as_ref().is_none_or(|s| *s.policy() != policy) {
            *sealer = Some(Sealer::new(policy));
        }
        let sealer = sealer.as_mut().expect("set above");
        sealer
            .advance(store, genesis)
            .map_err(|error| match error {
                Error::Capacity => format!(
                    "block {} needs more segments than a shard holds; no publication created",
                    tip.height + 1
                )
                .into(),
                error => Box::<dyn std::error::Error + Send + Sync>::from(error),
            })?;
        log_stage("seal", started);
        let started = std::time::Instant::now();
        let revision = history_revision;
        let sealed_shards = store.sealed_shards()?;
        if let Some(serving) = serving.filter(|_| durable.is_none()) {
            *durable = Some(crate::durable::Durable::open(
                &args.data_dir,
                serving.tables(),
            )?);
        }
        // Each shard is built and prepared, or written, before the next is built, so one
        // shard's tables at most are held outside the serving cache.
        let root = args.data_dir.join("publications");
        let mut prepared = Vec::new();
        let mut layout = Vec::new();
        let (mut records, mut segments) = (0u64, 0usize);
        let mut next_kept = Vec::new();
        for shard in sealed_shards.iter().map(Some).chain([None]) {
            let plan = match shard {
                Some(shard) => {
                    let end = store.checkpoint(shard.end_height)?;
                    let reusable = kept.iter().position(|k| {
                        serving.is_some()
                            && k.shard == *shard
                            && k.end == end
                            && k.history_revision == revision
                    });
                    match reusable {
                        Some(index) => {
                            let k = kept.swap_remove(index);
                            let mut parts = Vec::new();
                            for mut segment in k.segments {
                                let frozen = segment.frozen.as_ref().is_some_and(|c| {
                                    store.checkpoint(c.height).ok().as_ref() == Some(c)
                                });
                                parts.push(if frozen {
                                    Part::Ready(segment)
                                } else {
                                    let source = &segment.segment.payments;
                                    // A frozen companion whose block a reorg replaced reads
                                    // its records back from the stored payment table.
                                    let leaves = match segment.leaves.take() {
                                        Some(leaves) => leaves,
                                        None => {
                                            let durable = durable.as_ref().expect("serving");
                                            let snapshot = durable.snapshot(source)?;
                                            crate::shards::leaves(&snapshot)?
                                        }
                                    };
                                    let payments = &source
                                        .payment_manifest()
                                        .ok_or(Error::Malformed)?
                                        .directory;
                                    let (table, frozen) = crate::shards::rewitness(
                                        store, witnesses, payments, &leaves, &tip,
                                    )?;
                                    segment.leaves = Some(leaves);
                                    Part::Witness(segment, Box::new((table, frozen)))
                                });
                            }
                            Plan {
                                sealed: Some((k.shard, k.end)),
                                parts,
                                filter: Some(PlanFilter::Stored(k.filter)),
                            }
                        }
                        None => {
                            let built =
                                crate::shards::sealed(store, witnesses, shard, &tip, revision)?;
                            Plan {
                                sealed: Some((built.shard, built.end)),
                                parts: built
                                    .segments
                                    .into_iter()
                                    .map(|segment| Part::Fresh(Box::new(segment)))
                                    .collect(),
                                filter: Some(PlanFilter::Built(built.filter)),
                            }
                        }
                    }
                }
                None => {
                    let from = sealed_shards
                        .last()
                        .map_or(args.start_height, |s| s.end_height + 1);
                    if from > tip.height {
                        break;
                    }
                    let recent = crate::shards::recent(
                        store,
                        witnesses,
                        from,
                        &tip,
                        args.min_rows,
                        sealer.policy(),
                        revision,
                    )?;
                    Plan {
                        sealed: None,
                        parts: vec![Part::Fresh(Box::new(recent))],
                        filter: None,
                    }
                }
            };
            records += plan.parts.iter().map(Part::records).sum::<u64>();
            segments += plan.parts.len();
            if serving.is_some() {
                let mut tables = durable.take().expect("opened when serving");
                let (tables, shard) = tokio::task::spawn_blocking(move || {
                    let shard = prepare_plan(&mut tables, plan);
                    (tables, shard)
                })
                .await?;
                *durable = Some(tables);
                let (sealed, segments, filter) = shard?;
                if let Some((shard, end)) = &sealed {
                    next_kept.push(Kept {
                        shard: shard.clone(),
                        end: end.clone(),
                        history_revision: revision,
                        filter: filter.clone().expect("sealed shards have filters"),
                        segments: segments.clone(),
                    });
                }
                let segments: Vec<_> = segments.into_iter().map(|s| s.segment).collect();
                prepared.push((sealed.is_some(), segments, filter));
            } else {
                // Nothing is written for a chain or node that has changed since ranking.
                if !is_canonical(rpc, tip.height, tip.hash).await? {
                    return Err("chain changed while building shards; rerun to reconcile".into());
                }
                rpc.check_network(genesis).await?;
                layout.push(write_plan(&root, plan)?);
            }
        }
        // Exactly this publication's sealed shards are kept for the next.
        *kept = next_kept;
        // Stored tables neither a live inventory nor this candidate names are dropped.
        // Only now does the candidate name its own: after a restart nothing else does,
        // so pruning earlier would delete every reusable table.
        if let Some(serving) = serving {
            let mut live = serving.references();
            for (_, segments, filter) in &prepared {
                live.extend(filter.as_ref().map(|f| f.digest()));
                live.extend(
                    segments
                        .iter()
                        .flat_map(|s| [s.payments.id(), s.witnesses.id()]),
                );
            }
            durable
                .as_mut()
                .expect("opened when serving")
                .retain(&live)?;
        }
        log_stage("shards", started);
        if !is_canonical(rpc, tip.height, tip.hash).await? {
            return Err("chain changed while building shards; rerun to reconcile".into());
        }
        // Ranking read the genesis before the pass, and an endpoint can be repointed.
        rpc.check_network(genesis).await?;
        let shards = prepared.len().max(layout.len());
        let summary = serde_json::json!({"start_height":args.start_height,
        "end_height":tip.height,"records":records,
        "actions":tip.position-store.config().start_position,
        "excluded_coinbase_actions":store.counts()?.1,
        "shards":shards,"sealed_shards":sealed_shards.len(),
        "segments":segments,"history_revision":revision});
        let candidate = match serving {
            Some(_) => Candidate::Serving(Box::new(Publication::assemble(
                genesis, revision, prepared,
            )?)),
            None => Candidate::Files(Layout {
                genesis,
                history_revision: revision,
                shards: layout,
            }),
        };
        let revision = match &candidate {
            Candidate::Serving(publication) => hex::encode(publication.id()),
            Candidate::Files(_) => String::new(),
        };
        Ok(Input::Work(Prepared {
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
        }))
    }
}

/// A sealed shard prepared for serving, kept across publications while its blocks and
/// correction generation are unchanged.
pub(crate) struct Kept {
    shard: SealedShard,
    end: Checkpoint,
    history_revision: u64,
    filter: Arc<FilterSource>,
    segments: Vec<KeptSegment>,
}

/// One kept segment: its sessions, its record count, the block its companion is frozen
/// at (`None` while provisional) and, only while provisional, its checked records in
/// ordinal order. A provisional companion is rebuilt at every tip from those records; a
/// frozen one keeps none, so sealed history holds no per-payment state in memory, and
/// reads them back from its stored table if a reorg unfreezes it.
#[derive(Clone)]
pub(crate) struct KeptSegment {
    segment: Segment,
    records: u64,
    leaves: Option<Vec<(u64, [u8; 32], u32)>>,
    frozen: Option<Checkpoint>,
}

impl KeptSegment {
    /// A segment serving `segment` with `leaves`, keeping them only if `frozen` is `None`.
    fn new(
        segment: Segment,
        leaves: Vec<(u64, [u8; 32], u32)>,
        frozen: Option<Checkpoint>,
    ) -> Self {
        Self {
            segment,
            records: leaves.len() as u64,
            leaves: frozen.is_none().then_some(leaves),
            frozen,
        }
    }
}

/// How one segment reaches serving.
enum Part {
    /// Kept as it is: a frozen companion whose dependency is still stored.
    Ready(KeptSegment),
    /// A kept payment session with a rebuilt companion and its frozen context.
    Witness(KeptSegment, Box<(witness::Table, Option<Checkpoint>)>),
    /// Freshly built tables.
    Fresh(Box<crate::shards::Built>),
}

impl Part {
    /// The payments the segment holds.
    fn records(&self) -> u64 {
        match self {
            Part::Ready(k) | Part::Witness(k, ..) => k.records,
            Part::Fresh(built) => built.snapshot.manifest.records,
        }
    }
}

/// One shard of a publication: sealed with its end, or the recent tier, with its
/// segments and filter.
struct Plan {
    sealed: Option<(SealedShard, Checkpoint)>,
    parts: Vec<Part>,
    filter: Option<PlanFilter>,
}

/// A planned shard's filter: freshly built, or a kept shard's stored source.
enum PlanFilter {
    Built(ShardFilter),
    Stored(Arc<FilterSource>),
}

/// A planned shard's prepared segments.
type PreparedShard = (
    Option<(SealedShard, Checkpoint)>,
    Vec<KeptSegment>,
    Option<Arc<FilterSource>>,
);

/// Prepares every segment of `plan` for serving through `durable`, reusing kept and
/// stored tables. Blocking.
fn prepare_plan(
    durable: &mut crate::durable::Durable,
    plan: Plan,
) -> std::result::Result<PreparedShard, receiver_pir::Error> {
    let segments = plan
        .parts
        .into_iter()
        .map(|part| match part {
            Part::Ready(kept) => Ok(kept),
            Part::Witness(kept, rebuilt) => {
                let (table, frozen) = *rebuilt;
                Ok(KeptSegment::new(
                    durable.rewitness(&kept.segment.payments, table)?,
                    kept.leaves.expect("taken to rebuild"),
                    frozen,
                ))
            }
            Part::Fresh(built) => Ok(KeptSegment::new(
                durable.segment(built.snapshot, built.witnesses)?,
                built.leaves,
                built.frozen,
            )),
        })
        .collect::<std::result::Result<_, receiver_pir::Error>>()?;
    let filter = match plan.filter {
        Some(PlanFilter::Built(filter)) => Some(durable.filter(&filter)?),
        Some(PlanFilter::Stored(source)) => Some(source),
        None => None,
    };
    Ok((plan.sealed, segments, filter))
}

enum Candidate {
    Serving(Box<Publication>),
    /// A one-shot run's layout, whose tables are already written.
    Files(Layout),
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
            Candidate::Files(layout) => {
                let started = std::time::Instant::now();
                let root = args.data_dir.join("publications");
                let layout = write_layout(&root, &layout)?;
                log_stage("directory_write", started);
                let mut summary = summary;
                summary["revision"] = hex::encode(layout).into();
                println!("{summary}");
                return Ok(());
            }
            Candidate::Serving(publication) => *publication,
        };
        let serving = serving.expect("serving candidate has publication owner");
        let end_height = publication.head().end_height;
        let started = std::time::Instant::now();
        let anchor = publication.head();
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

/// Applies an indexing correction to the store in `data_dir` offline: replaces the
/// payments of `blocks`, which must match the stored chain, under `history_revision`
/// (see [`Store::correct`]). It holds the data directory's lock, so it refuses while a
/// serving indexer runs there; the restarted service publishes the higher revision and
/// revokes every older session before serving.
pub fn correct(
    data_dir: &Path,
    config: StoreConfig,
    history_revision: u64,
    blocks: &[receiver_store::IndexedBlock],
) -> Result<()> {
    let _lock = pir_pipeline::service::lock(data_dir).map_err(|e| {
        format!(
            "the receiver service is running in {}: {e}",
            data_dir.display()
        )
    })?;
    for block in blocks {
        for (receiver, payment) in &block.payments {
            verify::check_payment(receiver, payment)?;
        }
    }
    // A correction edits an existing index, converting a version 2 one first; it never
    // starts an empty one.
    let (mut store, converted) = receiver_store::open_in(data_dir, config, false)
        .map_err(|e| format!("receiver index in {}: {e}", data_dir.display()))?;
    if converted {
        info!("converted the version 2 receiver store; the original is kept for rollback");
    }
    store.correct(history_revision, blocks)?;
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

/// Durably writes every segment's rows and manifest with its companion witness table
/// and manifest, every sealed shard's filter, then the [`Layout`] naming them, which
/// staged serving loads. Returns the layout's identity. Invalid records or witnesses,
/// or a session manifest over [`receiver_pir::MAX_MANIFEST_BYTES`], write nothing for
/// their shard.
#[cfg(test)]
fn write_publication(
    root: &Path,
    genesis: [u8; 32],
    history_revision: u64,
    plans: Vec<Plan>,
) -> Result<[u8; 32]> {
    let shards = plans
        .into_iter()
        .map(|plan| write_plan(root, plan))
        .collect::<Result<_>>()?;
    write_layout(
        root,
        &Layout {
            genesis,
            history_revision,
            shards,
        },
    )
}

/// Durably writes one planned shard's tables and filter for a one-shot run, after
/// checking every segment, and returns its entry in the [`Layout`].
fn write_plan(root: &Path, plan: Plan) -> Result<LayoutShard> {
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    let mut segments = Vec::new();
    for part in plan.parts {
        let Part::Fresh(built) = part else {
            return Err("one-shot publications build every table".into());
        };
        let (snapshot, witnesses) = (built.snapshot, built.witnesses);
        verify::check_publication(&snapshot, &witnesses)?;
        let manifest = serde_json::to_vec_pretty(&snapshot.manifest)?;
        let witness_manifest = serde_json::to_vec_pretty(&witnesses.manifest)?;
        if manifest.len() + witness_manifest.len() > receiver_pir::MAX_MANIFEST_BYTES {
            return Err("publication manifest exceeds its size bound; nothing written".into());
        }
        // A sealed payment table keeps its revision while its provisional companion
        // changes, so each table is named by its own revision.
        let segment = LayoutSegment {
            payments: snapshot.manifest.revision()?,
            witnesses: witnesses.manifest.revision()?,
        };
        let (name, companion) = (
            hex::encode(segment.payments),
            hex::encode(segment.witnesses),
        );
        files.push((format!("{name}.rows"), snapshot.data));
        files.push((format!("{name}.json"), manifest));
        files.push((format!("{companion}.witness"), witnesses.data));
        files.push((format!("{companion}.witness.json"), witness_manifest));
        segments.push(segment);
    }
    let filter = match plan.filter {
        Some(PlanFilter::Built(filter)) => Some(filter),
        Some(PlanFilter::Stored(_)) => {
            return Err("one-shot publications build every filter".into())
        }
        None => None,
    };
    let filter = filter.map(|filter| {
        files.push((
            format!("{}.filter", hex::encode(filter.digest())),
            filter.bytes().to_vec(),
        ));
        filter.digest()
    });
    std::fs::create_dir_all(root)?;
    for (name, bytes) in files {
        write_revision_file(root, &name, &bytes)?;
    }
    Ok(LayoutShard {
        sealed: plan.sealed.is_some(),
        segments,
        filter,
    })
}

/// Durably writes `layout` beside the tables it names and returns its identity.
fn write_layout(root: &Path, layout: &Layout) -> Result<[u8; 32]> {
    let (id, bytes) = layout.encode()?;
    std::fs::create_dir_all(root)?;
    write_revision_file(root, &format!("{}.inventory.json", hex::encode(id)), &bytes)?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(id)
}

/// The files of a one-shot publication that staged serving assembles into an
/// inventory: each shard's segments by payment and companion table revision and its
/// filter by digest.
/// Coverage comes from the segments' own manifests.
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Layout {
    pub genesis: [u8; 32],
    pub history_revision: u64,
    pub shards: Vec<LayoutShard>,
}

/// One shard of a [`Layout`].
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LayoutShard {
    pub sealed: bool,
    pub segments: Vec<LayoutSegment>,
    pub filter: Option<LayoutHash>,
}

/// One segment of a [`LayoutShard`]: its payment table's revision, naming its `.json`
/// and `.rows`, and its companion's own revision, naming its `.witness.json` and
/// `.witness`.
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LayoutSegment {
    pub payments: LayoutHash,
    pub witnesses: LayoutHash,
}

/// A hash written as hex in a [`Layout`].
pub type LayoutHash = [u8; 32];

impl Layout {
    /// The compact JSON a layout file holds and its identity, the SHA-256 of those bytes.
    pub fn encode(&self) -> Result<([u8; 32], Vec<u8>)> {
        let bytes = serde_json::to_vec(self)?;
        Ok((sha2::Sha256::digest(&bytes).into(), bytes))
    }
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
    use receiver_directory::snapshot::Snapshot;
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

    /// An empty one-shard inventory ending at block `[hash; 32]`, height 101.
    fn publication(hash: u8) -> Publication {
        let mut manifest = super::common::manifest(receiver_pir::MIN_ROWS);
        manifest.end_hash = [hash; 32];
        let snapshot = Snapshot::build(manifest, &[]).unwrap();
        let table = crate::witness::table(&snapshot, &[]).unwrap();
        let segment = prepare_snapshot(snapshot, table).unwrap();
        Publication::assemble([1; 32], 0, vec![(false, vec![segment], None)]).unwrap()
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
            Self::serving(on_hash, Publications::default()).await
        }

        /// [`Self::new`] serving through `publications`.
        async fn serving(
            on_hash: impl Fn(&mut PayoutStore) + Send + Sync + 'static,
            publications: Publications,
        ) -> Self {
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
                dir.path().join(receiver_store::STORE_FILE),
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
                start_height: height as u32,
                end_height: None,
                depth: 0,
                batch_size: crate::blocks::MAX_RECEIVER_BATCH_BLOCKS,
                concurrency: 8,
                audit_enabled: true,
                filter_blocks: None,
                history_revision: 0,
                shard_rows: receiver_pir::MIN_ROWS,
                shard_target: None,
            };
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

        /// Restarts the service as a new process would: fresh serving state, table cache
        /// and listener, with the same budget, over the same data directory and node.
        async fn restart(&mut self) {
            self.publications =
                Publications::new(DEFAULT_RETENTION, self.publications.tables().budget());
            let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            self.origin = format!("http://{}", socket.local_addr().unwrap());
            let app = crate::router::router_with_publications(self.publications.clone());
            tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
            self.state = State::default();
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

        /// The current inventory's last shard's first segment, from its first chunk.
        async fn segment(&self) -> receiver_pir::Publication {
            let head: receiver_pir::inventory::Head =
                serde_json::from_value(self.get("inventory").await).unwrap();
            let chunk = self
                .get(&format!("inventory/{}/0", hex::encode(head.inventory)))
                .await;
            let chunk: receiver_pir::inventory::Chunk = serde_json::from_value(chunk).unwrap();
            chunk.shards.last().unwrap().segments[0].clone()
        }

        fn store(&self) -> Store {
            Store::open(
                self.dir.path().join(receiver_store::STORE_FILE),
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

    /// A reqwest transport for the library client that counts encrypted queries.
    #[derive(Clone, Default)]
    struct Wallet {
        http: reqwest::Client,
        queries: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl receiver_pir::transport::Transport for Wallet {
        async fn get(
            &self,
            url: &str,
            limit: usize,
        ) -> std::result::Result<Vec<u8>, receiver_pir::Error> {
            wallet_read(self.http.get(url), limit).await
        }
        async fn post(
            &self,
            url: &str,
            body: Vec<u8>,
            limit: usize,
        ) -> std::result::Result<Vec<u8>, receiver_pir::Error> {
            self.queries
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            wallet_read(self.http.post(url).body(body), limit).await
        }
    }

    /// One response within `limit`, 410 as a revision change.
    async fn wallet_read(
        request: reqwest::RequestBuilder,
        limit: usize,
    ) -> std::result::Result<Vec<u8>, receiver_pir::Error> {
        let failed = |e: reqwest::Error| receiver_pir::Error::Transport(e.to_string());
        let response = request.send().await.map_err(failed)?;
        if response.status() == reqwest::StatusCode::GONE {
            return Err(receiver_pir::Error::Revision);
        }
        let response = response.error_for_status().map_err(failed)?;
        crate::read_limited(response, limit)
            .await
            .map_err(|e| receiver_pir::Error::Transport(e.to_string()))
    }

    impl Paused {
        /// Appends a validated block after the store's tip paying each of `paid` once,
        /// with `extra` further commitments, every commitment `variant`, and makes the
        /// node agree. Returns the block.
        fn extend(
            &mut self,
            paid: &[[u8; 43]],
            extra: usize,
            variant: u8,
        ) -> receiver_store::IndexedBlock {
            let mut store = self.store();
            let tip = store.tip().unwrap();
            let height = tip.height + 1;
            let mut hash = [variant; 32];
            hash[..4].copy_from_slice(&height.to_le_bytes());
            let cmx = [6; 32];
            let payments = paid
                .iter()
                .enumerate()
                .map(|(i, receiver)| {
                    let mut payment = super::common::record(0, 1).payment;
                    (payment.height, payment.block_hash, payment.cmx) = (height, hash, cmx);
                    payment.position = tip.position + i as u64;
                    payment.tx_index = i as u32 + 1;
                    payment.txid = [variant; 32];
                    payment.txid[..8].copy_from_slice(&payment.position.to_le_bytes());
                    (*receiver, payment)
                })
                .collect();
            let count = paid.len() + extra;
            let mut commitments = vec![cmx; count];
            if let Some(first) = commitments.get_mut(paid.len()) {
                first[31] = variant & 0x3f;
            }
            let block = receiver_store::IndexedBlock {
                height,
                hash,
                parent: tip.hash,
                start_position: tip.position,
                end_position: tip.position + count as u64,
                coinbase_actions: 0,
                payments,
                commitments,
            };
            verify::append(&mut store, &block).unwrap();
            *self.node.tip.lock().unwrap() = u64::from(height);
            self.node
                .hashes
                .lock()
                .unwrap()
                .insert(u64::from(height), hash);
            block
        }

        /// Captures the served inventory at the store's tip as a wallet does, with the
        /// count of encrypted queries the client then sends.
        async fn capture(
            &self,
            filters: bool,
        ) -> (
            receiver_pir::transport::InventoryClient<Wallet>,
            Arc<std::sync::atomic::AtomicUsize>,
        ) {
            use receiver_pir::transport::{Inventories, InventoryClient};
            let wallet = Wallet::default();
            let queries = wallet.queries.clone();
            let genesis = Network::Mainnet.genesis_hash();
            let head = Inventories::head(&self.origin, &wallet, &genesis)
                .await
                .unwrap();
            let inventory = Inventories::capture(&self.origin, &wallet, head)
                .await
                .unwrap();
            let tip = self.store().tip().unwrap();
            let activation = crate::blocks::ironwood_activation();
            inventory
                .accept(&genesis, activation, tip.height, &tip.hash)
                .unwrap();
            (
                InventoryClient::new(&self.origin, wallet, inventory, filters),
                queries,
            )
        }
    }

    /// Every record of `receiver` in the captured inventory, with the shard and segment
    /// holding it, skipping sealed shards whose filter excludes it as a wallet may.
    async fn lookup(
        client: &receiver_pir::transport::InventoryClient<Wallet>,
        receiver: [u8; 43],
    ) -> Vec<(u32, u32, receiver_directory::RawRecord)> {
        let mut found = Vec::new();
        for shard in client.inventory().shards() {
            if client.uses_filters() && shard.filter.is_some() {
                let filter = client.filter(shard.index).await.unwrap();
                if !filter.matches(&[receiver])[0] {
                    continue;
                }
            }
            for index in 0..shard.segments.len() as u32 {
                let segment = client.segment(shard.index, index).await.unwrap();
                let mut page = 0;
                while let Some(record) = client.page(&segment, receiver, page).await.unwrap() {
                    page += 1;
                    let last = page == record.total;
                    found.push((shard.index, index, record));
                    if last {
                        break;
                    }
                }
            }
        }
        found
    }

    /// The lower path of every position in a fresh rebuild from all `store`'s
    /// commitments, the independent oracle for served payloads.
    fn fresh(store: &Store, positions: &[u64]) -> Vec<[u8; 512]> {
        let size = store.tip().unwrap().position;
        let tree: Vec<_> = (0..size.div_ceil(1 << 16))
            .flat_map(|s| store.commitments(s << 16, size.min((s + 1) << 16)).unwrap())
            .collect();
        zakura_receiver_crypto::witness::payloads(&tree, positions).unwrap()
    }

    /// Blocks that each reach the target seal into shards. A wallet's library client
    /// skips a sealed shard its filter excludes and reads the rest over real encrypted
    /// PIR, with each payment's companion payload equal to a fresh rebuild; the filters
    /// serve by digest; a restart rebuilds the identical inventory; and a displaced
    /// inventory keeps serving its chunks and sessions while sealed sessions carry over.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sealed_shards_serve_through_the_library_client() {
        use super::common::{receiver, seeded_receiver};
        let mut paused = Paused::new(|_| {}).await;
        paused.args.shard_target = Some(1);
        let (one, two) = (receiver(), seeded_receiver(1));
        paused.extend(&[one], 2, 0);
        paused.extend(&[two], 0, 0);
        paused.extend(&[one], 1, 0);
        paused.extend(&[], 3, 0);
        paused.refresh().await.unwrap();
        let (client, queries) = paused.capture(true).await;
        let shards = client.inventory().shards();
        let sealed: Vec<_> = shards
            .iter()
            .map(|s| (s.sealed, s.filter.is_some()))
            .collect();
        assert_eq!(
            sealed,
            [(true, true), (true, true), (true, true), (false, false)]
        );
        let found = lookup(&client, one).await;
        // Shard 1 holds only the other receiver, so its filter spares a query.
        assert_eq!(
            queries.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "one page each in shards 0 and 2, and the recent tier's empty page"
        );
        let positions: Vec<u64> = found.iter().map(|f| f.2.payment.position).collect();
        assert_eq!(
            found.iter().map(|f| f.0).collect::<Vec<_>>(),
            [0, 2],
            "{positions:?}"
        );
        let store = paused.store();
        let expected = fresh(&store, &positions);
        for ((shard, segment, record), expected) in found.iter().zip(expected) {
            let segment = client.segment(*shard, *segment).await.unwrap();
            assert_eq!(
                client.witness(&segment, record.ordinal).await.unwrap(),
                expected
            );
        }
        let head = client.inventory().head().clone();
        // A restart rebuilds the identical inventory from the store.
        paused.state = State::default();
        paused.refresh().await.unwrap();
        let (again, _) = paused.capture(true).await;
        assert_eq!(again.inventory().head(), &head);
        // A new tip replaces the recent tier; the sealed shards keep their sessions and
        // the displaced inventory still serves its chunk.
        paused.extend(&[two], 0, 0);
        paused.refresh().await.unwrap();
        let (next, _) = paused.capture(true).await;
        assert_ne!(next.inventory().head().inventory, head.inventory);
        let sessions = |c: &receiver_pir::transport::InventoryClient<Wallet>, i: usize| {
            c.inventory().shards()[i].segments[0].payments.id().unwrap()
        };
        assert_eq!(sessions(&next, 0), sessions(&client, 0));
        // Both inventories and the kept shards share each sealed filter's one source,
        // which holds none of its bytes.
        let live = paused.publications.live();
        assert_eq!(live.len(), 2);
        let mut shared = 0;
        for kept in &paused.state.kept {
            assert_eq!(kept.filter.held_bytes(), 0);
            for publication in &live {
                if let Some(source) = publication.filter(&kept.filter.digest()) {
                    assert!(Arc::ptr_eq(source, &kept.filter));
                    shared += 1;
                }
            }
        }
        assert_eq!(
            shared,
            3 * 2 + 1,
            "three shards in both, the new seal in one"
        );
        let chunk = format!(
            "{}/v1/receiver/inventory/{}/0",
            paused.origin,
            hex::encode(head.inventory)
        );
        assert!(reqwest::get(chunk).await.unwrap().status().is_success());
        assert_eq!(lookup(&client, two).await.len(), 1);
    }

    /// Two one-shot runs a block apart keep a sealed payment table whose provisional
    /// companion changed: each table is written under its own revision, so the second run
    /// writes the new companion beside the old instead of colliding with it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_shot_runs_name_a_changed_companion_by_its_own_revision() {
        let mut paused = Paused::new(|_| {}).await;
        paused.args.shard_target = Some(1);
        paused.extend(&[super::common::receiver()], 2, 0);
        paused.extend(&[], 1, 0);
        let genesis = Network::Mainnet.genesis_hash();
        let root = paused.dir.path().join("publications");
        let layouts = || -> Vec<Layout> {
            std::fs::read_dir(&root)
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|p| p.to_str().unwrap().ends_with(".inventory.json"))
                .map(|p| serde_json::from_slice(&std::fs::read(p).unwrap()).unwrap())
                .collect()
        };
        let mut state = State::default();
        let rpc = std::slice::from_ref(&paused.rpc);
        refresh(&paused.args, rpc, genesis, None, &mut state)
            .await
            .unwrap();
        let first = layouts().remove(0);
        paused.extend(&[], 1, 0);
        let rpc = std::slice::from_ref(&paused.rpc);
        refresh(&paused.args, rpc, genesis, None, &mut state)
            .await
            .unwrap();
        let all = layouts();
        assert_eq!(all.len(), 2);
        let second = all.into_iter().find(|l| *l != first).unwrap();
        let sealed = |l: &Layout| {
            let segment = &l.shards[0].segments[0];
            (segment.payments, segment.witnesses)
        };
        let ((payments, old), (same, new)) = (sealed(&first), sealed(&second));
        assert_eq!(payments, same);
        assert_ne!(old, new);
        for companion in [old, new] {
            let name = format!("{}.witness.json", hex::encode(companion));
            assert!(root.join(name).exists());
        }
    }

    /// More sealed shards than the table budget holds still serve every payment over
    /// real encrypted PIR: prepared tables are cached within the budget and reloaded from
    /// the data directory when evicted, and a restart prepares none of them again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sealed_tables_beyond_the_budget_load_on_demand() {
        use super::common::{receiver, seeded_receiver};
        let table = 3 * receiver_pir::MIN_ROWS as usize * receiver_directory::snapshot::ROW_BYTES;
        let budget = 3 * table;
        let publications = Publications::new(std::time::Duration::from_secs(600), budget);
        let mut paused = Paused::serving(|_| {}, publications.clone()).await;
        paused.args.shard_target = Some(1);
        let payees = [
            receiver(),
            seeded_receiver(1),
            seeded_receiver(2),
            seeded_receiver(3),
        ];
        for payee in payees {
            paused.extend(&[payee], 0, 0);
        }
        paused.extend(&[], 1, 0);
        paused.refresh().await.unwrap();
        let tables = publications.tables();
        assert!(tables.resident() <= budget);
        let (client, _) = paused.capture(false).await;
        assert_eq!(client.inventory().shards().len(), 5);
        let loads = tables.loads();
        for payee in payees {
            let found = lookup(&client, payee).await;
            assert_eq!(found.len(), 1);
            let (shard, segment, record) = &found[0];
            let segment = client.segment(*shard, *segment).await.unwrap();
            let payload = client.witness(&segment, record.ordinal).await.unwrap();
            assert_eq!(
                payload,
                fresh(&paused.store(), &[record.payment.position])[0]
            );
            assert!(tables.resident() <= budget);
        }
        assert!(tables.loads() > loads, "evicted tables were loaded again");
        // A new process finds every table in the data directory and prepares none:
        // nothing it serves yet names them, so they must outlive its first pass.
        let head = client.inventory().head().clone();
        let stored = std::fs::read_dir(paused.dir.path().join(crate::durable::TABLES_DIR))
            .unwrap()
            .count();
        paused.restart().await;
        paused.refresh().await.unwrap();
        let (again, _) = paused.capture(false).await;
        assert_eq!(again.inventory().head(), &head);
        assert_eq!(paused.state.durable.as_ref().unwrap().prepared(), 0);
        let tables = paused.publications.tables();
        assert_eq!(tables.loads(), 0);
        assert!(tables.resident() <= budget);
        let found = lookup(&again, payees[0]).await;
        assert_eq!(found.len(), 1);
        assert_eq!(
            std::fs::read_dir(paused.dir.path().join(crate::durable::TABLES_DIR))
                .unwrap()
                .count(),
            stored
        );
    }

    /// The checked records kept shards hold in memory.
    fn kept_leaves(state: &State) -> usize {
        state
            .kept
            .iter()
            .flat_map(|k| &k.segments)
            .map(|s| s.leaves.as_ref().map_or(0, Vec::len))
            .sum()
    }

    /// Memory a serving indexer holds for its inventories: the table budget's resident
    /// bytes (prepared tables and filter encodings), and bytes held outside it, which are
    /// sources holding prepared state in memory and kept shards' checked records.
    fn footprint(paused: &Paused) -> (usize, usize) {
        let held: usize = paused
            .publications
            .live()
            .iter()
            .map(|p| p.held_bytes())
            .sum();
        let leaves = kept_leaves(&paused.state) * std::mem::size_of::<(u64, [u8; 32], u32)>();
        (paused.publications.tables().resident(), held + leaves)
    }

    /// Publishing more sealed shards, and restarting as a new process, keeps the accounted
    /// footprint within the table budget, with nothing held outside it once companions
    /// freeze: memory does not grow with sealed history. Every payment still serves.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resident_memory_stays_bounded_as_sealed_shards_accumulate() {
        use super::common::{receiver, seeded_receiver};
        // Four sealed shards per batch, the second batch all paying one receiver.
        let batches = [
            [
                receiver(),
                seeded_receiver(1),
                seeded_receiver(2),
                seeded_receiver(3),
            ],
            [seeded_receiver(4); 4],
        ];
        let budget = 2 * receiver_pir_server::tables::resident_bytes(receiver_pir::MIN_ROWS);
        let publications = Publications::new(DEFAULT_RETENTION, budget);
        let mut paused = Paused::serving(|_| {}, publications).await;
        paused.args.shard_target = Some(1);
        let mut footprints = Vec::new();
        for batch in batches {
            for payee in batch {
                paused.extend(&[payee], 0, 0);
            }
            // Completing the Ironwood shard freezes every companion so far.
            paused.extend(&[], 1 << 16, 0);
            paused.refresh().await.unwrap();
            footprints.push(footprint(&paused));
        }
        let sealed = paused.state.kept.len();
        assert_eq!(sealed, 8);
        paused.restart().await;
        paused.refresh().await.unwrap();
        assert_eq!(paused.state.kept.len(), sealed);
        assert_eq!(paused.state.durable.as_ref().unwrap().prepared(), 0);
        footprints.push(footprint(&paused));
        for (resident, held) in &footprints {
            assert!(*resident <= budget, "{footprints:?}");
            assert_eq!(*held, 0, "{footprints:?}");
        }
        // The oldest and newest shards serve through cold loads within the budget.
        let (client, _) = paused.capture(true).await;
        for (payee, payments) in [(receiver(), 1), (seeded_receiver(4), 4)] {
            assert_eq!(lookup(&client, payee).await.len(), payments);
            let (resident, held) = footprint(&paused);
            assert!(resident <= budget && held == 0);
        }
    }

    /// A displaced inventory that a request still holds keeps its stored tables through
    /// later passes after its retention ends, so the request can still load them; the
    /// first pass after it is released deletes them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_held_inventory_keeps_its_stored_tables_until_released() {
        let publications = Publications::new(std::time::Duration::from_millis(1), 1 << 30);
        let mut paused = Paused::serving(|_| {}, publications.clone()).await;
        paused.extend(&[super::common::receiver()], 0, 0);
        paused.refresh().await.unwrap();
        // A request selected the recent tier and waits to load it.
        let held = publications.live().remove(0);
        let recent = held.segments().last().unwrap().payments.clone();
        let revision = recent
            .payment_manifest()
            .unwrap()
            .directory
            .revision()
            .unwrap();
        let rows = paused
            .dir
            .path()
            .join(crate::durable::TABLES_DIR)
            .join(format!("p-{}.rows", hex::encode(revision)));
        for _ in 0..2 {
            paused.extend(&[], 1, 0);
            paused.refresh().await.unwrap();
            assert!(rows.exists());
        }
        drop((held, recent));
        paused.extend(&[], 1, 0);
        paused.refresh().await.unwrap();
        assert!(!rows.exists());
    }

    /// A staged check verifies every segment of a one-shot inventory but keeps none
    /// prepared: every table and filter it lists reloads from the staged files. Staged
    /// serving keeps its prepared tables within the budget.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_staged_check_keeps_no_prepared_segment() {
        use super::common::{receiver, seeded_receiver};
        use crate::serve_publication::{load, PublicationRecord};
        let mut paused = Paused::new(|_| {}).await;
        paused.args.shard_target = Some(1);
        for payee in [receiver(), seeded_receiver(1), seeded_receiver(2)] {
            paused.extend(&[payee], 0, 0);
        }
        paused.extend(&[], 1, 0);
        let rpc = std::slice::from_ref(&paused.rpc);
        let genesis = Network::Mainnet.genesis_hash();
        refresh(&paused.args, rpc, genesis, None, &mut State::default())
            .await
            .unwrap();
        let root = paused.dir.path().join("publications");
        let layout = std::fs::read_dir(&root)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .find_map(|n| n.strip_suffix(".inventory.json").map(str::to_owned))
            .unwrap();
        let path = paused.dir.path().join("record.json");
        let record = json!({"directory": root, "revision": layout});
        std::fs::write(&path, record.to_string()).unwrap();
        let config = serde_json::from_value(json!({
            "publication_record": path, "bind": "127.0.0.1:18380",
            "state_dir": paused.dir.path().join("state"),
            "rpc_urls": ["http://127.0.0.1:1"], "no_auth": true,
        }))
        .unwrap();
        let record = PublicationRecord::read(&path).unwrap();
        let checked = load(&record, &config, None).unwrap();
        let shards = checked.publication().inventory().shards();
        assert_eq!(shards.iter().filter(|s| s.filter.is_some()).count(), 3);
        assert_eq!(checked.publication().segments().len(), 4);
        assert_eq!(checked.publication().held_bytes(), 0);
        let budget = 2 * receiver_pir_server::tables::resident_bytes(receiver_pir::MIN_ROWS);
        let publications = Publications::new(DEFAULT_RETENTION, budget);
        let served = load(&record, &config, Some(&publications)).unwrap();
        assert_eq!(served.publication().held_bytes(), 0);
        assert!(publications.tables().resident() <= budget);
        // A staged rows file replaced by a longer one is refused before it is read.
        let staged = checked.publication().segments()[0].payments.clone();
        let revision = staged
            .payment_manifest()
            .unwrap()
            .directory
            .revision()
            .unwrap();
        let rows = root.join(format!("{}.rows", hex::encode(revision)));
        let mut longer = std::fs::read(&rows).unwrap();
        longer.extend_from_slice(&[0; 4096]);
        std::fs::write(&rows, longer).unwrap();
        let tables = receiver_pir_server::tables::Tables::new(1 << 30);
        assert!(tables.get(&staged).is_err());
        assert_eq!(tables.resident(), 0);
    }

    /// A stored table whose public setup is not the one its manifest binds is not
    /// served after a restart: it is prepared again, and a wallet reads it as usual.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_corrupt_stored_setup_is_prepared_again_after_a_restart() {
        let payee = super::common::receiver();
        let mut paused = Paused::new(|_| {}).await;
        paused.extend(&[payee], 0, 0);
        paused.refresh().await.unwrap();
        let tables = paused.dir.path().join(crate::durable::TABLES_DIR);
        let publics: Vec<_> = std::fs::read_dir(&tables)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "public"))
            .collect();
        assert_eq!(publics.len(), 2, "the payment and witness tables");
        for public in &publics {
            let mut bytes = std::fs::read(public).unwrap();
            bytes[0] ^= 1;
            std::fs::write(public, bytes).unwrap();
        }
        paused.restart().await;
        paused.refresh().await.unwrap();
        assert_eq!(paused.state.durable.as_ref().unwrap().prepared(), 2);
        let (client, _) = paused.capture(false).await;
        let found = lookup(&client, payee).await;
        assert_eq!(found.len(), 1);
        let (shard, segment, record) = &found[0];
        let segment = client.segment(*shard, *segment).await.unwrap();
        let payload = client.witness(&segment, record.ordinal).await.unwrap();
        assert_eq!(
            payload,
            fresh(&paused.store(), &[record.payment.position])[0]
        );
    }

    /// After a restart a stored table or filter is reused only if it is exactly what the
    /// index rebuilds: corrupted or missing rows are prepared again and a corrupted filter
    /// is written again, so wallets read every payment, payload and filter as before.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stored_copies_are_rebuilt_unless_they_match_the_index() {
        let payee = super::common::receiver();
        let mut paused = Paused::new(|_| {}).await;
        paused.args.shard_target = Some(1);
        paused.extend(&[payee], 0, 0);
        paused.extend(&[], 1, 0);
        paused.refresh().await.unwrap();
        let tables = paused.dir.path().join(crate::durable::TABLES_DIR);
        let files = |extension: &str, prefix: &str| -> Vec<std::path::PathBuf> {
            std::fs::read_dir(&tables)
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|p| {
                    p.extension().is_some_and(|e| e == extension)
                        && p.file_name().unwrap().to_str().unwrap().starts_with(prefix)
                })
                .collect()
        };
        let flip = |path: &std::path::Path| {
            let mut bytes = std::fs::read(path).unwrap();
            bytes[0] ^= 1;
            std::fs::write(path, bytes).unwrap();
        };
        let (payments, witnesses, filters) = (
            files("rows", "p-"),
            files("rows", "w-"),
            files("filter", "f-"),
        );
        assert_eq!((payments.len(), witnesses.len(), filters.len()), (2, 2, 1));
        let filter = std::fs::read(&filters[0]).unwrap();
        payments.iter().for_each(|p| flip(p));
        witnesses
            .iter()
            .for_each(|w| std::fs::remove_file(w).unwrap());
        flip(&filters[0]);
        paused.restart().await;
        paused.refresh().await.unwrap();
        assert_eq!(paused.state.durable.as_ref().unwrap().prepared(), 4);
        assert_eq!(std::fs::read(&filters[0]).unwrap(), filter);
        let (client, _) = paused.capture(true).await;
        let found = lookup(&client, payee).await;
        assert_eq!(found.len(), 1);
        let (shard, segment, record) = &found[0];
        let segment = client.segment(*shard, *segment).await.unwrap();
        let payload = client.witness(&segment, record.ordinal).await.unwrap();
        assert_eq!(
            payload,
            fresh(&paused.store(), &[record.payment.position])[0]
        );
    }

    /// A cold reload, staged or stored, reads only the rows of the geometry it was
    /// verified under: a longer replacement is refused before it is read, and a
    /// replaced manifest is never used.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_reloads_read_only_the_verified_geometry() {
        let mut paused = Paused::new(|_| {}).await;
        paused.extend(&[super::common::receiver()], 0, 0);
        paused.refresh().await.unwrap();
        let stored = paused.publications.live()[0].segments()[0].payments.clone();
        let revision = stored
            .payment_manifest()
            .unwrap()
            .directory
            .revision()
            .unwrap();
        let file = |extension: &str| {
            let name = format!("p-{}.{extension}", hex::encode(revision));
            paused
                .dir
                .path()
                .join(crate::durable::TABLES_DIR)
                .join(name)
        };
        let rows = std::fs::read(file("rows")).unwrap();
        let mut manifest: receiver_pir::Manifest =
            serde_json::from_slice(&std::fs::read(file("json")).unwrap()).unwrap();
        manifest.directory.rows *= 2;
        std::fs::write(file("json"), serde_json::to_vec(&manifest).unwrap()).unwrap();
        let cold = || receiver_pir_server::tables::Tables::new(1 << 30);
        assert!(cold().get(&stored).is_ok());
        std::fs::write(file("rows"), [rows.as_slice(), &rows].concat()).unwrap();
        let tables = cold();
        assert!(matches!(
            tables.get(&stored),
            Err(receiver_pir_server::tables::LoadError::Load(_))
        ));
        assert_eq!(tables.resident(), 0);
    }

    /// `receiver`'s first payment's companion payload in the first shard, checked against
    /// a fresh rebuild from `store`.
    async fn served_payload(
        client: &receiver_pir::transport::InventoryClient<Wallet>,
        store: &Store,
        receiver: [u8; 43],
    ) -> [u8; 512] {
        let segment = client.segment(0, 0).await.unwrap();
        let record = client.page(&segment, receiver, 0).await.unwrap().unwrap();
        let payload = client.witness(&segment, record.ordinal).await.unwrap();
        assert_eq!(payload, fresh(store, &[record.payment.position])[0]);
        payload
    }

    /// The first shard's payment and witness session IDs and witness tree height.
    fn first_shard(
        client: &receiver_pir::transport::InventoryClient<Wallet>,
    ) -> ([u8; 32], [u8; 32], u32) {
        let segment = &client.inventory().shards()[0].segments[0];
        (
            segment.payments.id().unwrap(),
            segment.witnesses.id().unwrap(),
            segment.witnesses.table.tree_height,
        )
    }

    /// A sealed shard's payment table stays while its companion follows its witness
    /// dependencies: provisional at each tip until its payment's Ironwood shard
    /// completes, rebuilt when a reorg replaces the block after the seal, frozen at the
    /// completing block and reused, and rebuilt again when a reorg replaces that block
    /// even though every payment block is unchanged. Served payloads equal a fresh
    /// rebuild throughout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn witness_dependencies_follow_reorgs_after_a_seal() {
        let one = super::common::receiver();
        let mut paused = Paused::new(|_| {}).await;
        paused.args.shard_target = Some(1);
        let paid = paused.extend(&[one], 4, 0);
        paused.refresh().await.unwrap();
        let (payments, w0, _) = first_shard(&paused.capture(false).await.0);
        // Short of completing Ironwood shard 0, after the seal.
        let filler = (1 << 16) - 15;
        paused.extend(&[], filler, 0);
        paused.refresh().await.unwrap();
        let (client, _) = paused.capture(false).await;
        let (p1, w1, _) = first_shard(&client);
        assert_eq!(p1, payments);
        assert_ne!(w1, w0);
        let provisional = served_payload(&client, &paused.store(), one).await;
        // A provisional companion keeps its records to rebuild at the next tip.
        assert_eq!(kept_leaves(&paused.state), 1);
        // A reorg after the seal, before witness finality.
        paused.store().rewind(paid.height, paid.hash).unwrap();
        paused.extend(&[], filler, 1);
        paused.refresh().await.unwrap();
        let (client, _) = paused.capture(false).await;
        let (p2, w2, _) = first_shard(&client);
        assert_eq!(p2, payments);
        assert_ne!(w2, w1);
        assert_ne!(
            served_payload(&client, &paused.store(), one).await,
            provisional
        );
        // Completing the Ironwood shard freezes the companion at the completing block.
        let completing = paused.extend(&[], 20, 0);
        paused.refresh().await.unwrap();
        let (client, _) = paused.capture(false).await;
        let (_, w3, frozen_at) = first_shard(&client);
        assert_eq!(frozen_at, completing.height);
        let final_payload = served_payload(&client, &paused.store(), one).await;
        // A frozen one keeps none: only its record count stays in memory.
        assert_eq!(kept_leaves(&paused.state), 0);
        assert_eq!(paused.state.kept[0].segments[0].records, 1);
        paused.extend(&[], 1, 0);
        paused.refresh().await.unwrap();
        assert_eq!(first_shard(&paused.capture(false).await.0).1, w3);
        // A reorg of the completing block rebuilds the companion of the unchanged table,
        // from records read back from its stored payment table.
        let before = paused.store().checkpoint(completing.height - 1).unwrap();
        paused.store().rewind(before.height, before.hash).unwrap();
        paused.extend(&[], 20, 2);
        paused.refresh().await.unwrap();
        let (client, _) = paused.capture(false).await;
        let (p5, w5, _) = first_shard(&client);
        assert_eq!(p5, payments);
        assert_ne!(w5, w3);
        assert_ne!(
            served_payload(&client, &paused.store(), one).await,
            final_payload
        );
    }

    /// A reorg across a seal removes the sealed shard and revokes its sessions, and the
    /// replacement chain is sealed afresh.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reorg_across_a_seal_reseals_the_replacement() {
        use super::common::{receiver, seeded_receiver};
        let mut paused = Paused::new(|_| {}).await;
        paused.args.shard_target = Some(1);
        paused.extend(&[receiver()], 0, 0);
        paused.extend(&[], 1, 0);
        paused.refresh().await.unwrap();
        let (old, _) = paused.capture(true).await;
        let old_head = old.inventory().head().clone();
        let activation = paused
            .store()
            .checkpoint(crate::blocks::ironwood_activation())
            .unwrap();
        paused
            .store()
            .rewind(activation.height, activation.hash)
            .unwrap();
        assert!(paused.store().sealed_shards().unwrap().is_empty());
        paused.extend(&[seeded_receiver(1)], 0, 7);
        paused.extend(&[], 1, 7);
        paused.refresh().await.unwrap();
        let chunk = format!(
            "{}/v1/receiver/inventory/{}/0",
            paused.origin,
            hex::encode(old_head.inventory)
        );
        assert_eq!(
            reqwest::get(chunk).await.unwrap().status(),
            reqwest::StatusCode::GONE
        );
        let (client, _) = paused.capture(true).await;
        let shard = &client.inventory().shards()[0];
        assert!(shard.sealed);
        assert_ne!(shard.end_hash, old.inventory().shards()[0].end_hash);
        assert!(lookup(&client, receiver()).await.is_empty());
        assert_eq!(lookup(&client, seeded_receiver(1)).await.len(), 1);
    }

    /// A correction adding a payment to a sealed shard older than the rolling filters
    /// revokes every session at once, unseals from the corrected block and republishes
    /// under the new generation with the same boundaries, the payment and its witness.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_correction_older_than_retention_rebuilds_its_shard() {
        use super::common::{receiver, seeded_receiver};
        let mut paused = Paused::new(|_| {}).await;
        (paused.args.shard_target, paused.args.filter_blocks) = (Some(1), Some(1));
        let block = paused.extend(&[receiver()], 1, 0);
        paused.extend(&[seeded_receiver(1)], 0, 0);
        paused.extend(&[], 1, 0);
        paused.refresh().await.unwrap();
        let (old, _) = paused.capture(true).await;
        let bounds = |c: &receiver_pir::transport::InventoryClient<Wallet>| {
            let shards = c.inventory().shards();
            shards
                .iter()
                .map(|s| (s.start_height, s.end_height))
                .collect::<Vec<_>>()
        };
        let mut corrected = block.clone();
        let mut omitted = corrected.payments[0].1.clone();
        omitted.position += 1;
        omitted.cmx = corrected.commitments[1];
        (omitted.tx_index, omitted.txid) = (2, [9; 32]);
        let three = seeded_receiver(2);
        corrected.payments.push((three, omitted.clone()));
        paused.store().correct(1, &[corrected]).unwrap();
        assert!(paused.store().sealed_shards().unwrap().is_empty());
        paused.args.history_revision = 1;
        paused.refresh().await.unwrap();
        let chunk = format!(
            "{}/v1/receiver/inventory/{}/0",
            paused.origin,
            hex::encode(old.inventory().head().inventory)
        );
        assert_eq!(
            reqwest::get(chunk).await.unwrap().status(),
            reqwest::StatusCode::GONE
        );
        let (client, _) = paused.capture(true).await;
        assert_eq!(client.inventory().head().history_revision, 1);
        assert_eq!(bounds(&client), bounds(&old));
        let found = lookup(&client, three).await;
        assert_eq!(found.len(), 1);
        let (shard, segment, record) = &found[0];
        assert_eq!((*shard, record.payment.position), (0, omitted.position));
        let segment = client.segment(*shard, *segment).await.unwrap();
        assert_eq!(
            client.witness(&segment, record.ordinal).await.unwrap(),
            fresh(&paused.store(), &[omitted.position])[0]
        );
    }

    /// An offline correction edits an existing index and never starts an empty one,
    /// which would hide a version 2 index from its conversion.
    #[test]
    fn a_correction_never_starts_an_empty_index() {
        let dir = tempfile::tempdir().unwrap();
        let config = StoreConfig {
            genesis: Network::Mainnet.genesis_hash(),
            start_height: crate::blocks::ironwood_activation(),
            start_parent: [2; 32],
            start_position: 0,
        };
        assert!(correct(dir.path(), config, 1, &[]).is_err());
        assert!(!dir.path().join(receiver_store::STORE_FILE).exists());
    }

    /// A correction is refused while the service holds the data directory, and once
    /// applied offline the restarted service publishes the store's higher revision under
    /// its unchanged configuration, revoking older sessions before serving.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_offline_correction_is_published_at_the_next_start() {
        let mut paused = Paused::new(|_| {}).await;
        paused.refresh().await.unwrap();
        let old = paused.segment().await.payments;
        let height = crate::blocks::ironwood_activation();
        let block = receiver_store::IndexedBlock {
            height,
            hash: [3; 32],
            parent: [2; 32],
            start_position: 0,
            end_position: 0,
            coinbase_actions: 0,
            payments: Vec::new(),
            commitments: Vec::new(),
        };
        let config = StoreConfig {
            genesis: Network::Mainnet.genesis_hash(),
            start_height: height,
            start_parent: [2; 32],
            start_position: 0,
        };
        let held = pir_pipeline::service::lock(paused.dir.path()).unwrap();
        let refused = correct(
            paused.dir.path(),
            config.clone(),
            1,
            std::slice::from_ref(&block),
        );
        assert!(refused
            .unwrap_err()
            .to_string()
            .contains("service is running"));
        drop(held);
        correct(paused.dir.path(), config, 1, std::slice::from_ref(&block)).unwrap();
        paused.state = State::default();
        paused.refresh().await.unwrap();
        assert_eq!(paused.args.history_revision, 0);
        assert_eq!(paused.get("inventory").await["history_revision"], 1);
        let id = hex::encode(old.id().unwrap());
        let gone = reqwest::get(format!("{}/v1/receiver/public/{id}", paused.origin))
            .await
            .unwrap();
        assert_eq!(gone.status(), reqwest::StatusCode::GONE);
    }

    /// An outage proves nothing about canonicality; a restarted application recovers
    /// its committed index and serves the same session through the real HTTP router.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_failure_keeps_serving_and_restart_recovers() {
        let mut paused = Paused::new(|_| {}).await;
        paused.refresh().await.unwrap();
        let first = paused.get("inventory").await;
        paused.node.hashes.lock().unwrap().remove(&0);
        assert!(paused.refresh().await.is_err());
        assert_eq!(paused.get("inventory").await, first);
        paused
            .node
            .hashes
            .lock()
            .unwrap()
            .insert(0, Network::Mainnet.genesis_hash());
        paused.state = State::default();
        paused.refresh().await.unwrap();
        assert_eq!(paused.get("inventory").await, first);
    }

    /// A raised correction revision is durable before the backfill it needs: when that
    /// backfill fails, a restart that rolls the configured revision back still uses it.
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
        // A restart configured with the old revision still collects under the store's.
        paused.args.history_revision = 0;
        paused.state = State::default();
        let epoch = paused.publications.epoch();
        assert!(paused.refresh().await.is_err());
        assert_eq!(paused.store().history_revision().unwrap(), 1);
        assert_eq!(paused.publications.epoch(), epoch, "nothing moved back");
    }

    /// A one-shot run writes no rolling filters, so it neither builds them nor fails on
    /// them: a filter window that cannot be built leaves its publication unaffected,
    /// while a serving run, which does build it, refuses.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_one_shot_run_does_not_build_rolling_filters() {
        let mut paused = Paused::new(|_| {}).await;
        paused.args.filter_blocks = Some(receiver_store::blocks::MAX_FILTER_BLOCKS + 1);
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
        let first = paused.get("inventory").await;
        paused.publications.revoke();
        assert!(paused.publications.current_anchor().is_none());
        paused.refresh().await.unwrap();
        assert_eq!(paused.get("inventory").await, first);
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
        let response = reqwest::get(format!("{}/v1/receiver/inventory", paused.origin))
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
        let first = paused.get("inventory").await;
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
        assert_eq!(paused.get("inventory").await, first);
        let mut store = paused.store();
        store.rewind(height - 1, [3; 32]).unwrap();
        block.payments[0].1.cmx = [6; 32];
        block.commitments[0] = [6; 32];
        verify::append(&mut store, &block).unwrap();
        drop(store);
        paused.state = State::default();
        paused.refresh().await.unwrap();
        let next = paused.get("inventory").await;
        assert_eq!(next["end_height"], height);
        assert_eq!(paused.segment().await.payments.directory.records, 1);
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
        assert_eq!(paused.get("inventory").await["end_height"], height);
    }

    /// A response admitted under the old revision retains its immutable bytes
    /// while the shared lifecycle activates different rows at the next anchor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_pipeline_handover_keeps_an_admitted_old_publication_response() {
        use sha2::{Digest, Sha256};
        let mut paused = Paused::new(|_| {}).await;
        paused.refresh().await.unwrap();
        let old_manifest = paused.segment().await.payments;
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
        let client = receiver_pir::Client::new(old_manifest.clone(), &public).unwrap();
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
        let next = paused.segment().await.payments;
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
        let client = receiver_pir::Client::new(next, &public).unwrap();
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
        assert_eq!(paused.get("inventory").await["end_height"], height);
    }

    /// A zero companion payload for each record of `snapshot`, all positions in its
    /// filling Ironwood shard. With `invalid`, each supplies a sibling at level 15, which no
    /// position below 32,768 has in a tree of fewer leaves.
    fn placeholder(snapshot: &Snapshot, invalid: bool) -> witness::Table {
        let mut payload = [0; receiver_store::PAYLOAD_BYTES];
        payload[15 * 32] = u8::from(invalid);
        let payloads = vec![payload; snapshot.manifest.records as usize];
        crate::witness::table(snapshot, &payloads).unwrap()
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
        let table = placeholder(&snapshot, false);
        assert!(matches!(
            prepare_snapshot(snapshot, table),
            Err(Directory(Malformed))
        ));
        let snapshot = Snapshot::build(manifest, &[valid]).unwrap();
        let table = placeholder(&snapshot, true);
        assert!(matches!(
            prepare_snapshot(snapshot, table),
            Err(Directory(Malformed))
        ));
    }

    /// A one-shot plan of one recent-tier segment, `snapshot` with `witnesses`.
    fn plan(snapshot: Snapshot, witnesses: witness::Table) -> Vec<Plan> {
        vec![Plan {
            sealed: None,
            parts: vec![Part::Fresh(Box::new(crate::shards::Built {
                snapshot,
                witnesses,
                leaves: Vec::new(),
                frozen: None,
            }))],
            filter: None,
        }]
    }

    /// One-shot output validates persisted records and proofs before creating files.
    #[test]
    fn one_shot_output_refuses_invalid_records_and_proofs_before_writing() {
        let mut manifest = super::common::manifest(receiver_pir::MIN_ROWS);
        manifest.end_position = 300;
        let valid = Snapshot::build(manifest.clone(), &[super::common::record(0, 1)]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let write = |root: &Path, snapshot: &Snapshot, table| {
            write_publication(root, [1; 32], 0, plan(snapshot.clone(), table))
        };
        write(
            &dir.path().join("valid"),
            &valid,
            placeholder(&valid, false),
        )
        .unwrap();
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
            let table = placeholder(&snapshot, false);
            assert!(write(&root, &snapshot, table).is_err(), "field {field}");
            assert!(!root.exists());
        }
        let root = dir.path().join("invalid-proof");
        assert!(write(&root, &valid, placeholder(&valid, true)).is_err());
        assert!(!root.exists());
    }
}
