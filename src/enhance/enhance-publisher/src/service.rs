//! Enhance publication hosted by pir-publisher. Serving destinations are local
//! consumer configuration and never appear in this application's settings.
use crate::{
    application,
    chain_source::{Prevouts, ZakuraClient},
    ingest::EnhanceJournal,
    publisher::EnhancePublisher,
};
use pir_pipeline::service::{Http, Shutdown, Tasks};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, time::Duration};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
/// Canonical source, durable state directory, and local publisher listeners.
/// Consumers choose their own serving destinations independently.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnhancePublisherConfig {
    pub artifact_listen: Option<SocketAddr>,
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    pub zakura_rpc_url: String,
    pub zakura_cookie: PathBuf,
    pub poll_seconds: u64,
    pub prevout_cache_outputs: usize,
}
impl EnhancePublisherConfig {
    /// Check paths, polling, listeners, and credential-free HTTP RPC URLs
    /// without opening state or reading the cookie file.
    pub fn validate(&self) -> Result<()> {
        let rpc = reqwest::Url::parse(&self.zakura_rpc_url)?;
        if self.poll_seconds == 0
            || self.artifact_listen.is_none()
            || !self.data_dir.is_absolute()
            || !self.zakura_cookie.is_absolute()
            || !matches!(rpc.scheme(), "http" | "https")
            || rpc.host_str().is_none()
            || !rpc.username().is_empty()
            || rpc.password().is_some()
        {
            return Err("Enhance publisher needs canonical RPC credentials, absolute state paths, an artifact listener and nonzero polling".into());
        }
        Ok(())
    }
}
/// Lock the service directory, verify offline recovery, adopt any completed
/// rebuild, and supervise the artifact listener and canonical publisher loop.
pub async fn run(
    config: EnhancePublisherConfig,
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
    match crate::rebuild::adopt_staged(&config.data_dir)? {
        crate::rebuild::JournalAdoption::None => {}
        crate::rebuild::JournalAdoption::Adopted {
            receipt_id,
            height,
            changed_records,
            previous,
        } => {
            tracing::info!(%receipt_id, height, changed_records, previous = %previous.display(), "adopted staged Enhance journal rebuild")
        }
        crate::rebuild::JournalAdoption::Rejected { reason, moved_to } => {
            tracing::error!(%reason, moved_to = %moved_to.display(), "rejected staged Enhance journal rebuild")
        }
    }
    let rpc = ZakuraClient::from_cookie_file(&config.zakura_rpc_url, &config.zakura_cookie)?;
    let publisher = EnhancePublisher::open(&config.data_dir.join("publication"), rpc.clone())?;
    let artifacts = tokio::spawn(http.serve(
        config.artifact_listen.ok_or("artifact listener missing")?,
        publisher.distribution().router(),
    ));
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
        application::AcquisitionConfig {
            poll: Duration::from_secs(config.poll_seconds),
        },
        publisher,
        EnhanceJournal::open(&config.data_dir)?,
        application::CanonicalSource {
            rpc,
            prevouts: Prevouts::new(config.prevout_cache_outputs),
        },
        serving,
        shutdown,
    )
    .await?;
    if failed.load(std::sync::atomic::Ordering::Acquire) {
        return Err("Enhance artifact listener stopped".into());
    }
    Ok(())
}

/// Validate and declare this publisher without opening credentials or state.
pub fn configure(
    config: EnhancePublisherConfig,
) -> Result<pir_pipeline::publisher::PublisherDefinition<axum::Router>> {
    use pir_pipeline::publisher::{
        PublisherDefinition, PublisherResources, ReadinessProbe, ReadinessRule,
    };
    config.validate()?;
    let resources = PublisherResources {
        listeners: vec![
            config.listen,
            config.artifact_listen.ok_or("artifact listener missing")?,
        ],
        writable_paths: vec![config.data_dir.clone()],
        probes: vec![ReadinessProbe {
            address: config.listen,
            path: "/ready",
            rule: ReadinessRule::HttpSuccess,
        }],
    };
    Ok(PublisherDefinition::new(
        "enhance",
        resources,
        move |context| run(config, context.http, context.shutdown),
    ))
}
