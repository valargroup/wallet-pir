//! Receiver writer operations driven by the root chain and publication engine.
use crate::{application, blocks, service};
use pir_pipeline::{
    ingestion::{
        BlockId, ChainFeed, ChainObservation, ChainSettings, Ingestion, IngestionExecutor,
    },
    publisher::{
        PublisherDefinition, PublisherExecutor, PublisherResources, ReadinessProbe, ReadinessRule,
    },
    service::{Http, Shutdown},
};
use receiver_pir_server::Publications;
use receiver_store::{Checkpoint, PayoutStore, Store};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};
use zakura_server_chain::{hash, Network, NodeClient};
type Result<T> = pir_pipeline::publisher::Result<T>;

/// Continuous publisher settings. The root supplies the node and credentials.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub data_dir: PathBuf,
    pub bind: SocketAddr,
    #[serde(default = "min_rows")]
    pub min_rows: u32,
    #[serde(default = "depth")]
    pub depth: u32,
    #[serde(default = "batch_size")]
    pub batch_size: u32,
    #[serde(default = "concurrency")]
    pub concurrency: u32,
    pub filter_blocks: Option<u32>,
    #[serde(default = "shard_rows")]
    pub shard_rows: u32,
    pub shard_target: Option<u64>,
    #[serde(default = "retention_seconds")]
    pub retention_seconds: u64,
    #[serde(default = "table_memory_mib")]
    pub table_memory_mib: u64,
    #[serde(default)]
    pub history_revision: u64,
    pub near_since: Option<i64>,
    #[serde(default = "near_poll_seconds")]
    pub near_poll_seconds: u64,
}
fn min_rows() -> u32 {
    8192
}
fn depth() -> u32 {
    2
}
fn batch_size() -> u32 {
    64
}
fn concurrency() -> u32 {
    8
}
fn shard_rows() -> u32 {
    65536
}
fn retention_seconds() -> u64 {
    600
}
fn table_memory_mib() -> u64 {
    4096
}
fn near_poll_seconds() -> u64 {
    60
}

impl Config {
    fn source(&self, chain: &ChainSettings) -> service::Config {
        service::Config {
            data_dir: self.data_dir.clone(),
            min_rows: self.min_rows,
            rpc_url: vec![chain.rpc_url.clone()],
            cookie: chain.rpc_cookie.clone(),
            no_auth: chain.rpc_cookie.is_none(),
            start_height: blocks::ironwood_activation(),
            end_height: None,
            depth: self.depth,
            serve: true,
            bind: self.bind,
            poll_seconds: 10,
            batch_size: self.batch_size,
            concurrency: self.concurrency,
            filter_blocks: self.filter_blocks,
            shard_rows: self.shard_rows,
            shard_target: self.shard_target,
            retention_seconds: self.retention_seconds,
            table_memory_mib: self.table_memory_mib,
            history_revision: self.history_revision,
            near_since: self.near_since,
            near_poll_seconds: self.near_poll_seconds,
            near_key: None,
        }
    }
}

/// Directory retrieval is ready only after a completed directory is authorized;
/// filter-only startup envelopes do not advertise full Receiver readiness.
pub fn distribution_ready(bytes: &[u8]) -> bool {
    serde_json::from_slice::<receiver_pir_server::distribution::Current>(bytes).is_ok_and(
        |current| {
            current.eligible
                && current
                    .publication
                    .is_some_and(|p| p.descriptor.directory.is_some())
        },
    )
}

/// Declare resources without opening state, sockets or credential files.
pub fn configure<E: PublisherExecutor + IngestionExecutor>(
    config: Config,
    chain: Option<&ChainSettings>,
    executor: E,
) -> Result<PublisherDefinition<axum::Router>> {
    let chain = chain
        .cloned()
        .ok_or("enabled receiver publisher needs the root chain settings")?;
    let args = config.source(&chain);
    args.validate()?;
    if !args.data_dir.is_absolute() {
        return Err("Receiver data_dir must be absolute".into());
    }
    let resources = PublisherResources {
        listeners: vec![args.bind],
        writable_paths: vec![args.data_dir.clone()],
        probes: vec![ReadinessProbe {
            address: args.bind,
            path: "/provider/receiver/v1/current",
            rule: ReadinessRule::Body(distribution_ready),
        }],
    };
    Ok(
        PublisherDefinition::new("receiver", resources, move |context| async move {
            let feed = context
                .chain
                .ok_or("Receiver started without the root chain")?;
            pir_pipeline::publisher::dedicated_runtime("receiver-publisher", move || async move {
                run(args, feed, context.http, context.shutdown, executor).await
            })
            .await
        })
        .following_chain(),
    )
}

/// Run the managed writer while an independent root-observation guard fences serving.
async fn run<E: PublisherExecutor + IngestionExecutor>(
    mut args: service::Config,
    feed: ChainFeed,
    http: Http<axum::Router>,
    stop: Shutdown,
    executor: E,
) -> Result<()> {
    args.near_key = std::env::var("NEAR_INTENTS_EXPLORER")
        .ok()
        .filter(|key| !key.is_empty());
    let _lock = pir_pipeline::service::lock(&args.data_dir)?;
    let rpc = NodeClient::new(args.rpc_url[0].clone(), args.cookie.as_deref())?;
    let distribution = crate::distribution::Distribution::open(
        args.data_dir.clone(),
        rpc.clone(),
        args.retention_seconds,
    )?;
    let genesis = Network::Mainnet.genesis_hash();
    rpc.check_network(genesis).await?;
    let boundary = blocks::boundary(&rpc, args.start_height - 1).await?;
    let (store, _) = receiver_store::open_in(
        &args.data_dir,
        receiver_store::Config {
            genesis,
            start_height: args.start_height,
            start_parent: boundary.hash,
            start_position: boundary.position,
        },
        true,
    )?;
    let tip = store.tip()?;
    let publications = Publications::new(
        std::time::Duration::from_secs(args.retention_seconds),
        usize::try_from(args.table_memory_mib << 20)?,
    );
    struct Fence(Publications);
    impl Drop for Fence {
        fn drop(&mut self) {
            self.0.revoke();
        }
    }
    let _fence = Fence(publications.clone());
    distribution.attach(&publications);
    let reads = crate::near::Reads::default();
    let mut tasks = pir_pipeline::service::Tasks::default();
    let audit_publications = publications.clone();
    let audit_reads = reads.clone();
    let app = axum::Router::new()
        .route(
            "/operator/receiver/health",
            axum::routing::get(move || {
                let publications = audit_publications.clone();
                let reads = audit_reads.clone();
                async move {
                    let (current, epoch) = publications.health();
                    axum::Json(audit_fields(current, epoch, &reads))
                }
            }),
        )
        .nest(
            receiver_pir_server::distribution::PREFIX.trim_end_matches('/'),
            distribution.router(),
        );
    if let Some(key) = args.near_key.clone() {
        let path = args.data_dir.join(application::PROVIDERS);
        let since = args
            .near_since
            .unwrap_or_else(|| application::unix_now() - crate::near::AUDIT_LOOKBACK_SECS);
        let poll = std::time::Duration::from_secs(args.near_poll_seconds);
        let mut explorer = crate::near::Explorer::new(key)?;
        let task = tokio::spawn(async move {
            loop {
                crate::near::poll(&mut explorer, &path, since, &reads).await;
                tokio::time::sleep(poll).await;
            }
        });
        tasks.own(&task);
    }
    let providers = if args.near_key.is_some() {
        PayoutStore::open(args.data_dir.join(application::PROVIDERS)).or_else(|error| {
            tracing::warn!(%error, "payout audit unavailable; continuing chain publication");
            PayoutStore::open(":memory:")
        })?
    } else {
        PayoutStore::open(":memory:")?
    };
    let writer = Writer {
        config: application::Config::from(&args),
        rpc,
        store,
        tip,
        providers,
        publications: publications.clone(),
        state: application::State::stopping_with(stop.clone()),
        distribution,
        executor: executor.clone(),
        restore: true,
    };
    let guard = spawn_guard(publications, feed.clone(), stop.clone());
    tasks.own(&guard);
    let bind = args.bind;
    let serve_stop = stop.clone();
    let serving = tokio::spawn(async move {
        tokio::select! { result = http.serve(bind, app) => result.map_err(Into::into), _ = serve_stop.cancelled() => Ok(()) }
    });
    tasks.own(&serving);
    let ingestion = executor.ingest(writer, feed, stop.clone());
    let guarded = async {
        let result = guard.await;
        stop.request();
        result.map_err(|error| -> pir_pipeline::publisher::Error { error.into() })?
    };
    let served = async {
        let result = serving.await;
        stop.request();
        result.map_err(|error| -> pir_pipeline::publisher::Error { error.into() })?
    };
    let ingested = async {
        let result = ingestion.await;
        stop.request();
        result
    };
    let (guarded, served, ingested) = tokio::join!(guarded, served, ingested);
    guarded.and(served).and(ingested)
}

/// Keep report and inventory identity from one atomic publication snapshot.
pub(crate) fn audit_fields(
    current: Option<([u8; 32], Option<serde_json::Value>)>,
    epoch: u64,
    reads: &crate::near::Reads,
) -> serde_json::Value {
    let mut fields = reads.health();
    let (serving, report) = current.unzip();
    fields.insert("epoch".into(), epoch.into());
    fields.insert("serving".into(), serving.map(hex::encode).into());
    fields.insert("indexer".into(), report.flatten().into());
    serde_json::Value::Object(fields)
}

fn spawn_guard(
    publications: Publications,
    feed: ChainFeed,
    stop: Shutdown,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let result = guard(publications, feed, stop.clone()).await;
        stop.request();
        result
    })
}

async fn guard(publications: Publications, mut feed: ChainFeed, stop: Shutdown) -> Result<()> {
    loop {
        tokio::select! { _ = stop.cancelled() => return Ok(()), changed = feed.observations.changed() => {
            if changed.is_err() { return Err("root chain feed stopped".into()); }
        }}
        let Some(observation) = feed.observations.borrow_and_update().clone() else {
            continue;
        };
        if observation.genesis != hash::display(&Network::Mainnet.genesis_hash()) {
            publications.revoke();
            return Err("Receiver root chain changed network".into());
        }
        let (epoch, anchors) = publications.serving();
        for (height, expected) in anchors {
            if u64::from(height) > observation.tip.height {
                continue;
            }
            match feed.chain.hash_at(u64::from(height)).await {
                Ok(actual) if actual != hash::display(&expected) => {
                    publications.revoke_serving(epoch, (height, expected));
                    break;
                }
                Err(error) => tracing::warn!(%error, "Receiver canonical validation deferred"),
                _ => {}
            }
        }
    }
}

struct Writer<E> {
    config: application::Config,
    rpc: NodeClient,
    store: Store,
    tip: Checkpoint,
    providers: PayoutStore,
    publications: Publications,
    state: application::State,
    distribution: crate::distribution::Distribution,
    executor: E,
    /// The previous process's publication has not been offered again yet.
    restore: bool,
}
fn block(tip: &Checkpoint) -> BlockId {
    BlockId {
        height: u64::from(tip.height),
        hash: hash::display(&tip.hash),
    }
}
impl<E: PublisherExecutor> Ingestion for Writer<E> {
    type Error = pir_pipeline::publisher::Error;
    fn target_height(&self, tip: u64) -> u64 {
        tip.saturating_sub(u64::from(self.config.depth))
    }
    fn start_height(&self, _: u64) -> u64 {
        u64::from(self.config.start_height)
    }
    fn committed(&self) -> Option<BlockId> {
        Some(block(&self.tip))
    }
    fn checkpoint_at_or_below(
        &self,
        height: u64,
    ) -> impl std::future::Future<Output = Option<BlockId>> + Send {
        let checkpoint = u32::try_from(height.min(u64::from(self.tip.height)))
            .ok()
            .and_then(|height| self.store.checkpoint(height).ok())
            .as_ref()
            .map(block);
        std::future::ready(checkpoint)
    }
    async fn observed(&mut self, observation: &ChainObservation) -> Result<()> {
        if observation.genesis != hash::display(&Network::Mainnet.genesis_hash()) {
            self.publications.revoke();
            return Err("Receiver needs the mainnet chain".into());
        }
        let revision = self
            .config
            .history_revision
            .max(self.store.history_revision()?);
        if revision > self.publications.history_revision() {
            self.distribution.withdraw()?;
        }
        crate::revision::advance(&mut self.store, Some(&self.publications), revision)?;
        if std::mem::take(&mut self.restore) {
            match self
                .distribution
                .restore(&self.publications, revision)
                .await
            {
                Ok(true) => tracing::info!("Receiver serves its previous publication again"),
                Ok(false) => {}
                Err(error) => tracing::warn!(%error, "Receiver previous publication not restored"),
            }
        }
        Ok(())
    }
    async fn withdraw_above(&mut self, _: Option<&BlockId>) -> Result<()> {
        self.distribution.withdraw()?;
        self.publications.revoke();
        Ok(())
    }
    async fn rewind(&mut self, keep: Option<&BlockId>) -> Result<()> {
        let keep =
            keep.ok_or("reorg crossed Receiver's activation boundary; rebuild explicitly")?;
        self.store.rewind(
            u32::try_from(keep.height)?,
            hash::parse_display(&keep.hash)?,
        )?;
        self.tip = self.store.tip()?;
        Ok(())
    }
    async fn ingest(&mut self, from: u64, through: u64) -> Result<()> {
        if from != u64::from(self.tip.height) + 1 {
            return Err("Receiver batch is not contiguous".into());
        }
        let mut remaining = through - from + 1;
        while remaining > 0 {
            let count = remaining.min(u64::from(self.config.batch_size)) as u32;
            let blocks =
                blocks::batch(&self.rpc, &self.tip, count, self.config.concurrency).await?;
            for block in blocks {
                crate::verify::append(&mut self.store, &block)?;
                self.tip = self.store.tip()?;
            }
            remaining -= u64::from(count);
        }
        Ok(())
    }
    async fn caught_up(&mut self, _: &ChainObservation) -> Result<()> {
        if self.config.audit_enabled {
            // The polling task can recover after this writer's startup failed.
            // Reopen the shared file on each pass rather than keeping an empty fallback.
            self.providers = PayoutStore::open(self.config.data_dir.join(application::PROVIDERS))
                .or_else(|error| {
                tracing::warn!(%error, "payout audit unavailable; continuing chain publication");
                PayoutStore::open(":memory:")
            })?;
        }
        application::publish_committed(
            &self.config,
            &self.rpc,
            &mut self.store,
            &mut self.providers,
            &self.publications,
            &mut self.state,
            Some((&self.distribution, &self.executor)),
        )
        .await
    }
    fn failed(&mut self, error: &Self::Error) -> pir_pipeline::FailurePolicy {
        tracing::warn!(%error, "Receiver ingestion or publication deferred");
        pir_pipeline::FailurePolicy::NextPoll
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pir_pipeline::ingestion::{catch_up, CanonicalChain, ChainFuture};
    use std::{sync::Arc, time::Instant};
    struct Chain;
    impl CanonicalChain for Chain {
        fn hash_at(&self, height: u64) -> ChainFuture<'_, String> {
            Box::pin(async move { Ok(hash::display(&[height as u8; 32])) })
        }
    }

    fn writer(
        directory: &std::path::Path,
        config: receiver_store::Config,
    ) -> Writer<pir_pipeline::engine::PublisherEngine> {
        let start = config.start_height;
        let (store, _) = receiver_store::open_in(directory, config, true).unwrap();
        let tip = store.tip().unwrap();
        Writer {
            tip,
            store,
            rpc: NodeClient::new("http://127.0.0.1:1".into(), None).unwrap(),
            providers: PayoutStore::open(":memory:").unwrap(),
            publications: Publications::default(),
            state: application::State::default(),
            executor: pir_pipeline::engine::PublisherEngine,
            restore: false,
            distribution: crate::distribution::Distribution::open(
                directory.into(),
                NodeClient::new("http://127.0.0.1:1".into(), None).unwrap(),
                600,
            )
            .unwrap(),
            config: application::Config {
                data_dir: directory.into(),
                min_rows: 8192,
                start_height: start,
                end_height: None,
                depth: 5,
                batch_size: 64,
                concurrency: 8,
                audit_enabled: false,
                filter_blocks: None,
                history_revision: 0,
                shard_rows: 65536,
                shard_target: None,
            },
        }
    }

    /// Reopening the existing SQLite journal preserves its commits and does
    /// not acquire replacement blocks when the confirmation depth increases.
    #[tokio::test]
    async fn existing_store_keeps_confirmed_progress_and_withdraws_before_rewind() {
        let directory = tempfile::tempdir().unwrap();
        let start = blocks::ironwood_activation();
        let config = receiver_store::Config {
            genesis: Network::Mainnet.genesis_hash(),
            start_height: start,
            start_parent: [start.wrapping_sub(1) as u8; 32],
            start_position: 0,
        };
        {
            let (mut store, _) =
                receiver_store::open_in(directory.path(), config.clone(), true).unwrap();
            for height in start..=start + 2 {
                store
                    .append(&receiver_store::IndexedBlock {
                        height,
                        hash: [height as u8; 32],
                        parent: [height.wrapping_sub(1) as u8; 32],
                        start_position: 0,
                        end_position: 0,
                        coinbase_actions: 0,
                        payments: vec![],
                        commitments: vec![],
                    })
                    .unwrap();
            }
        }
        let mut writer = writer(directory.path(), config.clone());
        assert_eq!(
            writer.target_height(u64::from(start + 3)),
            u64::from(start - 2)
        );
        assert_eq!(
            writer
                .checkpoint_at_or_below(u64::from(start + 1))
                .await
                .unwrap()
                .height,
            u64::from(start + 1)
        );
        // A lagging root observation must not rewind or invoke preparation/RPC.
        let observation = ChainObservation {
            genesis: hash::display(&config.genesis),
            tip: BlockId {
                height: u64::from(start + 1),
                hash: hash::display(&[(start + 1) as u8; 32]),
            },
            pass: 1,
        };
        catch_up(&mut writer, &Chain, &observation).await.unwrap();
        assert_eq!(writer.store.tip().unwrap().height, start + 2);
        let before = writer.publications.epoch();
        let keep = writer
            .checkpoint_at_or_below(u64::from(start))
            .await
            .unwrap();
        writer.withdraw_above(Some(&keep)).await.unwrap();
        assert!(writer.publications.epoch() > before);
        assert_eq!(writer.store.tip().unwrap().height, start + 2);
        writer.rewind(Some(&keep)).await.unwrap();
        assert_eq!(writer.store.tip().unwrap().height, start);
    }

    /// Synchronous writer work must not hold up the independently scheduled
    /// canonical guard, including when no new RPC block can be fetched yet.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canonical_guard_runs_while_preparation_blocks_its_task() {
        use zakura_pir_query_server::receiver::blocks::{BlockFilter, BlockId as ReceiverBlock};
        let publications = Publications::default();
        let genesis = Network::Mainnet.genesis_hash();
        let height = blocks::ironwood_activation();
        let filter = BlockFilter::build(
            &genesis,
            ReceiverBlock {
                height,
                hash: [9; 32],
            },
            [0; 32],
            &[],
        )
        .unwrap();
        let window =
            receiver_pir_server::blocks::BlockWindow::new(genesis, 0, vec![filter]).unwrap();
        assert!(publications.publish_filters(window, 0));
        let (sender, observations) = tokio::sync::watch::channel(None);
        let stop = Shutdown::default();
        let guard = spawn_guard(
            publications.clone(),
            ChainFeed {
                chain: Arc::new(Chain),
                observations,
            },
            stop.clone(),
        );
        let epoch = publications.epoch();
        sender.send_replace(Some(ChainObservation {
            genesis: hash::display(&genesis),
            tip: BlockId {
                height: u64::from(height),
                hash: hash::display(&[height as u8; 32]),
            },
            pass: 1,
        }));
        // Block this task exactly as synchronous shard construction does. The
        // spawned guard can use the second runtime worker; join! could not.
        let deadline = Instant::now() + std::time::Duration::from_secs(2);
        while publications.epoch() == epoch && Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let revoked = publications.epoch() != epoch;
        stop.request();
        guard.await.unwrap().unwrap();
        assert!(
            revoked,
            "canonical revocation must finish before preparation yields"
        );
    }
    #[tokio::test]
    async fn audit_recovers_on_the_next_pass_without_restarting_the_writer() {
        let directory = tempfile::tempdir().unwrap();
        let config = receiver_store::Config {
            genesis: Network::Mainnet.genesis_hash(),
            start_height: blocks::ironwood_activation(),
            start_parent: [2; 32],
            start_position: 0,
        };
        let mut writer = writer(directory.path(), config.clone());
        writer.config.audit_enabled = true;
        let observation = ChainObservation {
            genesis: hash::display(&config.genesis),
            tip: block(&writer.tip),
            pass: 1,
        };
        let path = directory.path().join(application::PROVIDERS);
        std::fs::create_dir(&path).unwrap();
        writer.caught_up(&observation).await.unwrap();
        let now = 1_000_000;
        assert_eq!(
            crate::near::capture(&writer.providers, now)
                .unwrap()
                .report(&writer.store)
                .unwrap()["payouts_checked"],
            0
        );
        std::fs::remove_dir(&path).unwrap();
        PayoutStore::open(&path)
            .unwrap()
            .record(
                "near-payout-audit",
                &[([1; 43], [7; 32], now - 7200)],
                now,
                now,
            )
            .unwrap();
        writer.caught_up(&observation).await.unwrap();
        let report = crate::near::capture(&writer.providers, now)
            .unwrap()
            .report(&writer.store)
            .unwrap();
        assert_eq!(report["payouts_checked"], 1);
        assert_eq!(report["payouts_missing"], 1);
        assert!(writer.publications.current_anchor().is_none());
    }
}
