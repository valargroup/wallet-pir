//! Enhance publication hosted by pir-publisher. Serving destinations are local
//! consumer configuration and never appear in this application's settings.
use crate::{
    application,
    chain_source::{Prevouts, ZakuraClient},
    ingest::EnhanceJournal,
    publisher::EnhancePublisher,
};
use pir_pipeline::ingestion::{ChainFeed, ChainSettings, IngestionExecutor};
use pir_pipeline::publisher::PublisherExecutor;
use pir_pipeline::service::{Http, Shutdown, Tasks};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
/// Durable state directory and local publisher listeners; the canonical source
/// is the root chain.
/// Consumers choose their own serving destinations independently.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnhancePublisherConfig {
    pub artifact_listen: SocketAddr,
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub prevout_cache_outputs: usize,
}
impl EnhancePublisherConfig {
    /// Check state paths without opening state. The root validates chain settings.
    pub fn validate(&self) -> Result<()> {
        if !self.data_dir.is_absolute() {
            return Err("Enhance publisher needs an absolute state path".into());
        }
        Ok(())
    }
}
/// Lock the service directory, verify offline recovery, adopt any completed
/// rebuild, and supervise the artifact listener, root-scheduled ingestion and
/// the publication loop.
pub async fn run<E: PublisherExecutor + IngestionExecutor>(
    config: EnhancePublisherConfig,
    chain: ChainSettings,
    feed: ChainFeed,
    executor: E,
    http: Http<axum::Router>,
    shutdown: Shutdown,
) -> Result<()> {
    config.validate()?;
    let _lock = pir_pipeline::service::lock(&config.data_dir)?;
    crate::handoff::verify_startup(&config.data_dir)?;
    let mode = config.data_dir.join("source-mode");
    if mode.exists() && std::fs::read_to_string(&mode)?.trim() != "canonical-archive" {
        return Err(
            "Enhance state was created by a synthetic source; use a fresh canonical directory"
                .into(),
        );
    }
    enhance_shared::artifact::write_atomic(&config.data_dir, "source-mode", |file| {
        file.write_all(b"canonical-archive")
    })?;
    let mut tasks = Tasks::default();
    // A publication built from a journal this start replaced is not served again.
    let mut restore = true;
    match crate::rebuild::adopt_staged(&config.data_dir)? {
        crate::rebuild::JournalAdoption::None => {}
        crate::rebuild::JournalAdoption::Adopted {
            receipt_id,
            height,
            changed_records,
            previous,
        } => {
            tracing::info!(%receipt_id, height, changed_records, previous = %previous.display(), "adopted staged Enhance journal rebuild");
            restore = false;
        }
        crate::rebuild::JournalAdoption::Rejected { reason, moved_to } => {
            tracing::error!(%reason, moved_to = %moved_to.display(), "rejected staged Enhance journal rebuild")
        }
    }
    let rpc = ZakuraClient::from_cookie_file(
        &chain.rpc_url,
        chain
            .rpc_cookie
            .as_ref()
            .ok_or("Enhance publisher requires a node cookie")?,
    )?;
    let publisher = EnhancePublisher::open(&config.data_dir.join("publication"), rpc.clone())?;
    let artifacts =
        tokio::spawn(http.serve(config.artifact_listen, publisher.distribution().router()));
    tasks.own(&artifacts);
    let failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let artifact_failed = failed.clone();
    let stop = shutdown.clone();
    let watch = tokio::spawn(async move {
        let _ = artifacts.await;
        if !stop.requested() {
            artifact_failed.store(true, std::sync::atomic::Ordering::Release);
            stop.request();
        }
    });
    tasks.own(&watch);
    let serving = tokio::spawn(http.serve(config.listen, publisher.router()));
    tasks.own(&serving);
    application::run_until(
        executor,
        feed,
        publisher,
        EnhanceJournal::open(&config.data_dir)?,
        application::CanonicalSource {
            rpc,
            prevouts: Prevouts::new(config.prevout_cache_outputs),
        },
        serving,
        shutdown,
        restore,
    )
    .await?;
    if failed.load(std::sync::atomic::Ordering::Acquire) {
        return Err("Enhance artifact listener stopped".into());
    }
    Ok(())
}

/// Validate and declare this publisher without opening credentials or state.
pub fn configure<E: PublisherExecutor + IngestionExecutor>(
    config: EnhancePublisherConfig,
    chain: Option<&ChainSettings>,
    executor: E,
) -> Result<pir_pipeline::publisher::PublisherDefinition<axum::Router>> {
    use pir_pipeline::publisher::{
        PublisherDefinition, PublisherResources, ReadinessProbe, ReadinessRule,
    };
    config.validate()?;
    let chain = chain
        .cloned()
        .ok_or("enabled enhance publisher needs the root chain settings")?;
    if chain.rpc_cookie.is_none() {
        return Err("Enhance publisher requires a node cookie".into());
    }
    let resources = PublisherResources {
        listeners: vec![config.listen, config.artifact_listen],
        writable_paths: vec![config.data_dir.clone()],
        probes: vec![ReadinessProbe {
            address: config.listen,
            path: "/ready",
            rule: ReadinessRule::HttpSuccess,
        }],
    };
    Ok(
        PublisherDefinition::new("enhance", resources, move |context| async move {
            let feed = context
                .chain
                .ok_or("Enhance publisher started without the root chain")?;
            run(
                config,
                chain,
                feed,
                executor,
                context.http,
                context.shutdown,
            )
            .await
        })
        .following_chain(),
    )
}
