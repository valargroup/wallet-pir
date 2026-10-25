//! Receiver serving from publisher-authorized immutable HTTPS objects.
mod material;
mod sync;

use crate::{public::Slot, readiness::Readiness, Result};
use pir_pipeline::service::Shutdown;
use pir_sync_manager::HttpsSource;
use receiver_pir_server::{distribution::PREFIX, Publications};
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc, time::Duration};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub publisher: String,
    pub publisher_ca: Option<PathBuf>,
    pub state_dir: PathBuf,
    pub genesis: [u8; 32],
    pub activation_height: u32,
    #[serde(default = "poll")]
    pub poll_seconds: u64,
    #[serde(default = "download")]
    pub bootstrap_max_bytes: u64,
    #[serde(default = "memory")]
    pub table_memory_mib: usize,
    #[serde(default = "retention")]
    pub retention_seconds: u64,
}
fn poll() -> u64 {
    5
}
fn download() -> u64 {
    64 << 30
}
fn memory() -> usize {
    4096
}
fn retention() -> u64 {
    600
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        self.source(false)?;
        if !self.state_dir.is_absolute()
            || self.publisher_ca.as_ref().is_some_and(|p| !p.is_absolute())
        {
            return Err("Receiver paths must be absolute".into());
        }
        if self.poll_seconds == 0
            || self.poll_seconds > 3600
            || self.bootstrap_max_bytes == 0
            || self.activation_height == 0
            || self.genesis == [0; 32]
            || self.retention_seconds == 0
            || self.retention_seconds > 86400
        {
            return Err("Receiver configuration limits are invalid".into());
        }
        let budget = self
            .table_memory_mib
            .checked_mul(1 << 20)
            .ok_or("Receiver table budget overflow")?;
        if budget < receiver_pir_server::tables::MIN_BUDGET {
            return Err(
                "Receiver table budget cannot load a maximum payment and witness pair".into(),
            );
        }
        Ok(())
    }
    /// Offline validation checks URL syntax without reading certificates or state.
    pub(crate) fn source(&self, certificates: bool) -> Result<HttpsSource> {
        let roots = match self.publisher_ca.as_ref().filter(|_| certificates) {
            Some(path) => vec![reqwest::Certificate::from_pem(&std::fs::read(path)?)?],
            None => Vec::new(),
        };
        HttpsSource::new(&self.publisher, PREFIX, self.bootstrap_max_bytes, &roots)
    }
}

pub async fn run(config: Config, wallet: Slot, readiness: Readiness, stop: Shutdown) -> Result<()> {
    config.validate()?;
    let publications = Publications::new(
        Duration::from_secs(config.retention_seconds),
        config.table_memory_mib * (1 << 20),
    );
    let ready = publications.clone();
    readiness.register(move || {
        let active = ready.current_anchor().is_some();
        async move { active }
    });
    let lifecycle = sync::synchronization(&config, publications.clone())?;
    wallet.install(receiver_pir_server::router_with_health(
        publications.clone(),
        Arc::new(Default::default),
    ));
    // A normal stop drains HTTP first. An error still fences admission and any
    // response that outlived its publisher authorization.
    struct Fence(Publications);
    impl Drop for Fence {
        fn drop(&mut self) {
            self.0.revoke();
        }
    }
    let _fence = Fence(publications);
    let result = crate::synchronization::SyncManager::new(lifecycle)
        .with_success_interval(Duration::from_secs(config.poll_seconds))
        .run_until(stop.cancelled())
        .await;
    match result {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests;
