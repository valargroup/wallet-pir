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
            cookie: Some(chain.rpc_cookie.clone()),
            no_auth: false,
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
            path: "/v1/receiver/inventory",
            rule: ReadinessRule::HttpSuccess,
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
    let reads = crate::near::Reads::default();
    let mut tasks = pir_pipeline::service::Tasks::default();
    let app = receiver_pir_server::router_with_health(publications.clone(), {
        let reads = reads.clone();
        std::sync::Arc::new(move || reads.health())
    });
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
        state: application::State::default(),
    };
    let guard = guard(publications, feed.clone(), stop.clone());
    let bind = args.bind;
    let serve_stop = stop.clone();
    let serving = async move {
        tokio::select! { result = http.serve(bind, app) => result.map_err(Into::into), _ = serve_stop.cancelled() => Ok(()) }
    };
    let ingestion = executor.ingest(writer, feed, stop.clone());
    let guarded = async {
        let result = guard.await;
        stop.request();
        result
    };
    let served = async {
        let result = serving.await;
        stop.request();
        result
    };
    let ingested = async {
        let result = ingestion.await;
        stop.request();
        result
    };
    let (guarded, served, ingested) = tokio::join!(guarded, served, ingested);
    guarded.and(served).and(ingested)
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

struct Writer {
    config: application::Config,
    rpc: NodeClient,
    store: Store,
    tip: Checkpoint,
    providers: PayoutStore,
    publications: Publications,
    state: application::State,
}
fn block(tip: &Checkpoint) -> BlockId {
    BlockId {
        height: u64::from(tip.height),
        hash: hash::display(&tip.hash),
    }
}
impl Ingestion for Writer {
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
        crate::revision::advance(&mut self.store, Some(&self.publications), revision)?;
        Ok(())
    }
    async fn withdraw_above(&mut self, _: Option<&BlockId>) -> Result<()> {
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
        application::publish_committed(
            &self.config,
            &self.rpc,
            &mut self.store,
            &mut self.providers,
            &self.publications,
            &mut self.state,
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
    struct Chain;
    impl CanonicalChain for Chain {
        fn hash_at(&self, height: u64) -> ChainFuture<'_, String> {
            Box::pin(async move { Ok(hash::display(&[height as u8; 32])) })
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
        let (store, _) = receiver_store::open_in(directory.path(), config.clone(), true).unwrap();
        let tip = store.tip().unwrap();
        let mut writer = Writer {
            tip,
            store,
            rpc: NodeClient::new("http://127.0.0.1:1".into(), None).unwrap(),
            providers: PayoutStore::open(":memory:").unwrap(),
            publications: Publications::default(),
            state: application::State::default(),
            config: application::Config {
                data_dir: directory.path().into(),
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
        };
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
}
