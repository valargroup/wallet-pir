//! Receiver ingestion application: canonical collection/commit followed by publication.
//! Chain transport and decoding remain in the upstream server chain adapter.
use crate::{
    canonical::{check_serving, freshest, is_canonical},
    seal::Sealer,
    verify,
};
mod publication;
use pir_pipeline::publisher::{PublicationInput, PublisherExecutor};
use publication::ReceiverPublication;
use receiver_pir_server::{publication::Segment, tables::ObjectSource, Publication, Publications};
use receiver_store::{Checkpoint, Config as StoreConfig, Error, PayoutStore, SealedShard, Store};
use sha2::Digest;
use std::sync::Arc;
use std::{
    io::Write,
    path::{Path, PathBuf},
};
use tracing::{info, warn};
use zakura_pir_publisher::receiver::{shards::ShardFilter, RawRecord};
use zakura_pir_publisher::receiver::{CompanionTable, Entry, Table as _};
#[cfg(test)]
use zakura_server_chain::Network;
use zakura_server_chain::NodeClient;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The provider database's file in the configured data directory.
pub const PROVIDERS: &str = "providers.sqlite";

/// Collection and publication settings, independent of CLI parsing and credentials.
pub struct Config {
    pub data_dir: PathBuf,
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
}

/// The publication [`refresh`] last activated, with what it was built from.
struct Active {
    /// Its terminal height and hash.
    anchor: (u32, [u8; 32]),
    history_revision: u64,
}

/// In-memory publication identity, the filling Ironwood shard's paths, the open shard's
/// projection and sealed shards' prepared sessions across polls.
/// Recreate on restart: committed progress, seals and completed witnesses are recovered
/// from the store.
#[derive(Default)]
pub struct State {
    active: Option<Active>,
    witnesses: crate::witness::Witnesses,
    /// Seals shards under the format's limits, which only tests lower.
    sealer: Sealer,
    kept: Vec<Kept>,
    durable: Option<crate::durable::Durable>,
    /// A requested stop ends shard building between shards and between a shard's build
    /// steps; nothing a later pass reuses is replaced until every shard is built.
    stop: Option<pir_pipeline::service::Shutdown>,
}

impl State {
    pub(crate) fn stopping_with(stop: pir_pipeline::service::Shutdown) -> Self {
        Self {
            stop: Some(stop),
            ..Self::default()
        }
    }

    /// Applies `change` to the seal policy, starting a fresh sealer, as a restart
    /// would, if the policy differs.
    #[cfg(test)]
    pub(crate) fn with_policy(&mut self, change: impl FnOnce(&mut crate::seal::Policy)) {
        let mut policy = *self.sealer.policy();
        change(&mut policy);
        if policy != *self.sealer.policy() {
            self.sealer = Sealer::new(policy);
        }
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
    let mut provider_store = match args.audit_enabled {
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
        collected.publish(&mut provider_store, state).await?;
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

/// Prepare from the root scheduler's committed checkpoint, without polling a
/// tip or reconciling a second time. The independent canonical guard remains
/// active while this operation prepares and atomically installs its candidate.
pub(crate) async fn publish_committed<E: pir_pipeline::publisher::PublisherExecutor>(
    args: &Config,
    rpc: &NodeClient,
    store: &mut Store,
    providers: &mut PayoutStore,
    serving: &Publications,
    state: &mut State,
    distribution: Option<(&crate::distribution::Distribution, &E)>,
) -> Result<()> {
    let genesis = store.config().genesis;
    let tip = store.tip()?;
    if tip.height < args.start_height {
        return Ok(());
    }
    let epoch = serving.epoch();
    publish_filters(args, rpc, store, serving, genesis, &tip, epoch).await?;
    if let Some((distribution, executor)) = distribution {
        distribution.export(serving, executor).await?;
    }
    if serving.ready_at().is_some() {
        return Ok(());
    }
    let history_revision = store.history_revision()?;
    let work = Collected {
        args,
        rpc,
        store,
        serving: Some(serving),
        genesis,
        tip,
        epoch: Some(epoch),
        refresh_started: std::time::Instant::now(),
        history_revision,
    };
    let selected = work.select(providers, state).await?;
    match distribution {
        Some((_, executor)) => {
            executor.publish_input(selected).await?;
        }
        None => {
            pir_pipeline::engine::PublisherEngine
                .publish_input(selected)
                .await?;
        }
    }
    if let Some((distribution, executor)) = distribution {
        distribution.export(serving, executor).await?;
    }
    Ok(())
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
    collect_inner(
        args,
        rpc,
        node_tip,
        store,
        serving,
        genesis,
        refresh_started,
    )
    .await
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
/// one: a one-shot run writes no filter artifact. A window `serving` already holds for
/// the same network, correction revision and tip, within the requested retention, is
/// kept, so a caught-up service does not rebuild it every poll. The stored chain ending
/// at that tip is fixed, so a rebuild would read the same blocks, and stop at the same
/// one when the byte bound shortens the window.
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
    if blocks == 0 {
        return Err("the rolling filter window holds no blocks".into());
    }
    let first = (tip.height.saturating_sub(blocks - 1)).max(args.start_height);
    let current = serving.filter_window();
    if current.is_some_and(|window| {
        let head = window.head();
        head.first >= first
            && (head.genesis, head.revision, head.tip.height, head.tip.hash)
                == (genesis, revision, tip.height, tip.hash)
    }) {
        return Ok(());
    }
    let hints = store.hints(first, tip.height, receiver_store::blocks::MAX_FILTER_BYTES)?;
    let window = receiver_pir_server::blocks::BlockWindow::new(
        genesis,
        revision,
        hints.first,
        hints.parent,
        hints.hashes,
        hints.sets,
    )?;
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
    pub async fn publish(
        self,
        provider_store: &mut PayoutStore,
        state: &'a mut State,
    ) -> Result<()> {
        pir_pipeline::engine::PublisherEngine
            .publish_input(self.select(provider_store, state).await?)
            .await?;
        Ok(())
    }

    async fn select(
        mut self,
        provider_store: &mut PayoutStore,
        state: &'a mut State,
    ) -> Result<PublicationInput<ReceiverPublication<'a>>> {
        let anchor_time = self.audit_time().await;
        self.select_with_report(provider_store, state, anchor_time)
    }
    async fn audit_time(&mut self) -> Result<i64> {
        match self.args.audit_enabled {
            true => self
                .rpc
                .block_time(self.tip.hash)
                .await
                .map(|time| time.min(unix_now()))
                .map_err(Into::into),
            false => Ok(unix_now()),
        }
    }
    fn select_with_report(
        self,
        provider_store: &mut PayoutStore,
        state: &'a mut State,
        anchor_time: Result<i64>,
    ) -> Result<PublicationInput<ReceiverPublication<'a>>> {
        let report = anchor_time
            .and_then(|time| crate::near::capture(provider_store, time))
            .and_then(|capture| capture.report(self.store))
            .unwrap_or_else(|error| {
                warn!(%error, "payout audit unavailable");
                serde_json::Value::Null
            });
        let refresh = self
            .serving
            .zip(state.active.as_ref())
            .is_some_and(|(serving, active)| {
                active.anchor == (self.tip.height, self.tip.hash)
                    && serving.current_anchor() == Some(active.anchor)
                    && active.history_revision == self.history_revision
            });
        let fence = self.serving.zip(self.epoch);
        let publication = ReceiverPublication {
            fence,
            work: Some(self),
            state: Some(state),
            report: Some(report),
            refresh,
        };
        Ok(if refresh {
            PublicationInput::MetadataRefresh(publication)
        } else {
            PublicationInput::NewMaterial(publication)
        })
    }

    async fn prepare(
        self,
        state: &'a mut State,
        report: serde_json::Value,
    ) -> Result<Prepared<'a>> {
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
            stop,
        } = state;
        let started = std::time::Instant::now();
        let staged = sealer.advance(store).map_err(|error| match error {
            Error::Capacity => format!(
                "a block after {} needs more segments than a shard holds; no publication created",
                store
                    .sealed_shards()
                    .ok()
                    .and_then(|s| s.last().map(|s| s.end_height))
                    .unwrap_or(args.start_height)
            )
            .into(),
            error => Box::<dyn std::error::Error + Send + Sync>::from(error),
        })?;
        log_stage("seal", started);
        let started = std::time::Instant::now();
        let revision = history_revision;
        let mut sealed_shards = store.sealed_shards()?;
        let recorded = sealed_shards.len();
        sealed_shards.extend(staged);
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
        let stopped = || stop.as_ref().is_some_and(|stop| stop.requested());
        for (index, shard) in sealed_shards.iter().map(Some).chain([None]).enumerate() {
            if stopped() {
                return Err(Error::Stopped.into());
            }
            let plan = match shard {
                Some(shard) => {
                    let end = store.checkpoint(shard.end_height)?;
                    // Only serving passes keep shards, and a state serves or not for life.
                    let reusable = kept.iter().position(|k| {
                        k.shard == *shard && k.end == end && k.history_revision == revision
                    });
                    // A frozen companion whose block a reorg replaced holds no records
                    // to rebuild from, so its shard is built again; its directory's
                    // bytes are unchanged, so the stored copy is reused.
                    let kept_shard = reusable.map(|index| kept.swap_remove(index)).filter(|k| {
                        k.segments.iter().all(|s| match &s.frozen {
                            Some(c) => store.checkpoint(c.height).ok().as_ref() == Some(c),
                            None => s.records.is_some(),
                        })
                    });
                    match kept_shard {
                        Some(k) => {
                            let mut parts = Vec::new();
                            for segment in k.segments {
                                if segment.frozen.is_some() {
                                    parts.push(Part::Ready(segment));
                                    continue;
                                }
                                let directory = &segment
                                    .segment
                                    .directory
                                    .directory_session()
                                    .ok_or(Error::Malformed)?
                                    .manifest;
                                let records = segment.records.clone().expect("provisional");
                                let rebuilt = crate::shards::rebuild(
                                    store, witnesses, directory, &records, &tip, &stopped,
                                )?;
                                parts.push(Part::Rebuilt(segment, Box::new(rebuilt)));
                            }
                            Plan {
                                sealed: Some((k.shard, k.end)),
                                parts,
                                filter: Some(PlanFilter::Stored(k.filter)),
                            }
                        }
                        None => {
                            let built = crate::shards::sealed(
                                store,
                                witnesses,
                                sealer.policy(),
                                shard,
                                &tip,
                                revision,
                                &stopped,
                            )?;
                            // A staged seal is recorded only once its shard, choice
                            // objects included, has been built.
                            if index >= recorded {
                                store.seal(shard)?;
                            }
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
                    let Some(open) = sealer.open() else {
                        break;
                    };
                    let open = (open.0, &open.1.projection);
                    let built =
                        crate::shards::open(store, witnesses, open, &tip, revision, &stopped)?;
                    Plan {
                        sealed: None,
                        parts: vec![Part::Fresh(Box::new(built))],
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
                        .flat_map(|s| [s.directory.id(), s.companion.id()]),
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
            Some(_) => Candidate::Unassembled {
                genesis,
                revision,
                shards: prepared,
            },
            None => Candidate::Files(Layout {
                genesis,
                history_revision: revision,
                shards: layout,
            }),
        };
        Ok(Prepared {
            candidate,
            args,
            rpc,
            serving,
            epoch,
            active,
            history_revision,
            report,
            refresh_started,
            // Set once a serving candidate is assembled.
            revision: String::new(),
            summary,
        })
    }
}

/// A sealed shard prepared for serving, kept across publications while its blocks and
/// correction generation are unchanged.
pub(crate) struct Kept {
    shard: SealedShard,
    end: Checkpoint,
    history_revision: u64,
    filter: Arc<ObjectSource>,
    segments: Vec<KeptSegment>,
}

/// One kept segment: its sources, its payment count, the block its companion is frozen
/// at (`None` while provisional) and, only while provisional, its directory's entries
/// and its records in slot order, which every tip's companion is rebuilt from. A frozen
/// one keeps none, so sealed history holds no per-payment state in memory.
#[derive(Clone)]
pub(crate) struct KeptSegment {
    segment: Segment,
    payments: u64,
    entries: Option<Arc<Vec<Entry>>>,
    records: Option<Arc<Vec<RawRecord>>>,
    frozen: Option<Checkpoint>,
}

impl KeptSegment {
    /// A segment serving `segment`, keeping `entries` and `records` only while its
    /// companion is provisional (`frozen` is `None`).
    fn new(
        segment: Segment,
        entries: Arc<Vec<Entry>>,
        records: Arc<Vec<RawRecord>>,
        frozen: Option<Checkpoint>,
    ) -> Self {
        let provisional = frozen.is_none();
        // A frozen or open segment's build keeps no records, so count its companion's.
        let payments = segment
            .companion
            .companion_session()
            .expect("a companion")
            .manifest
            .payments
            .into();
        Self {
            segment,
            payments,
            entries: provisional.then_some(entries),
            records: provisional.then_some(records),
            frozen,
        }
    }
}

/// How one segment reaches serving.
enum Part {
    /// Kept as it is: a frozen companion whose dependency is still stored.
    Ready(KeptSegment),
    /// A kept directory with a rebuilt companion and its frozen context.
    Rebuilt(KeptSegment, Box<(CompanionTable, Option<Checkpoint>)>),
    /// Freshly built tables.
    Fresh(Box<crate::shards::Built>),
}

impl Part {
    /// The payments the segment holds.
    fn records(&self) -> u64 {
        match self {
            Part::Ready(k) | Part::Rebuilt(k, ..) => k.payments,
            Part::Fresh(built) => built.tables.companion.manifest.payments.into(),
        }
    }
}

/// One shard of a publication: sealed with its end, or the open shard, with its
/// segments and filter.
struct Plan {
    sealed: Option<(SealedShard, Checkpoint)>,
    parts: Vec<Part>,
    filter: Option<PlanFilter>,
}

/// A planned shard's filter: freshly built, or a kept shard's stored source.
enum PlanFilter {
    Built(ShardFilter),
    Stored(Arc<ObjectSource>),
}

/// A planned shard's prepared segments and its filter.
type PreparedShard = (
    Option<(SealedShard, Checkpoint)>,
    Vec<KeptSegment>,
    Option<Arc<ObjectSource>>,
);

/// Prepares every segment of `plan` for serving through `durable`, reusing kept and
/// stored tables. Blocking.
fn prepare_plan(
    durable: &mut crate::durable::Durable,
    plan: Plan,
) -> std::result::Result<PreparedShard, zakura_pir_publisher::receiver::session::Error> {
    let segments = plan
        .parts
        .into_iter()
        .map(|part| match part {
            Part::Ready(kept) => Ok(kept),
            Part::Rebuilt(kept, rebuilt) => {
                let (table, frozen) = *rebuilt;
                let entries = kept.entries.clone().expect("provisional");
                let segment = durable.rebuilt(&kept.segment, &entries, table)?;
                Ok(KeptSegment::new(
                    segment,
                    entries,
                    kept.records.expect("provisional"),
                    frozen,
                ))
            }
            Part::Fresh(built) => {
                let built = *built;
                let segment = durable.segment(built.tables, &built.entries)?;
                Ok(KeptSegment::new(
                    segment,
                    Arc::new(built.entries),
                    built.records,
                    built.frozen,
                ))
            }
        })
        .collect::<std::result::Result<_, zakura_pir_publisher::receiver::session::Error>>()?;
    let filter = match plan.filter {
        Some(PlanFilter::Built(filter)) => Some(durable.filter(&filter)?),
        Some(PlanFilter::Stored(source)) => Some(source),
        None => None,
    };
    Ok((plan.sealed, segments, filter))
}

enum Candidate {
    Unassembled {
        genesis: [u8; 32],
        revision: u64,
        shards: Vec<(bool, Vec<Segment>, Option<Arc<ObjectSource>>)>,
    },
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

/// Durably writes one planned shard's tables and filter for a one-shot run, after
/// checking every segment, and returns its entry in the [`Layout`].
fn write_plan(root: &Path, plan: Plan) -> Result<LayoutShard> {
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    let mut segments = Vec::new();
    for part in plan.parts {
        let Part::Fresh(built) = part else {
            return Err("one-shot publications build every table".into());
        };
        let tables = built.tables;
        let (d, c) = (&tables.directory.manifest, &tables.companion.manifest);
        let (directory, companion) = (serde_json::to_vec_pretty(d)?, serde_json::to_vec_pretty(c)?);
        if directory.len() + companion.len()
            > zakura_pir_publisher::receiver::session::MAX_MANIFEST_BYTES
        {
            return Err("publication manifest exceeds its size bound; nothing written".into());
        }
        // A sealed directory keeps its revision while its provisional companion
        // changes, so each table is named by its own revision.
        let segment = LayoutSegment {
            directory: d.revision()?,
            companion: c.revision()?,
        };
        let (d, c) = (
            hex::encode(segment.directory),
            hex::encode(segment.companion),
        );
        files.push((format!("d-{d}.rows"), tables.directory.data));
        files.push((format!("d-{d}.choice"), tables.directory.choice));
        files.push((format!("d-{d}.json"), directory));
        files.push((format!("c-{c}.rows"), tables.companion.data));
        files.push((format!("c-{c}.json"), companion));
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
/// inventory: each shard's segments by directory and companion revision and its
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

/// One segment of a [`LayoutShard`]: its directory's revision, naming its `d-<hex>`
/// `.json`, `.rows` and `.choice`, and its companion's own revision, naming its
/// `c-<hex>` `.json` and `.rows`.
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LayoutSegment {
    pub directory: LayoutHash,
    pub companion: LayoutHash,
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
mod tests;
