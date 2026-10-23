//! Filter ingestion and HTTP publication owned by the root publisher host.
use super::service::{router, ServiceState};
use super::shard_filters::ShardFilters;
use crate::ingest::{self, store::FilterStore};
use pir_pipeline::service::{Http, Shutdown, Tasks};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};
use zakura_pir_publisher::transparent::filter as transparent_filter;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub listen: SocketAddr,
    pub zakura_rpc_url: String,
    pub zakura_cookie: Option<PathBuf>,
    pub data_dir: PathBuf,
    pub start_height: u64,
    pub cache_outputs: usize,
    pub poll_seconds: u64,
    pub commit_every: u64,
    pub shard_dir: Option<PathBuf>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8090".parse().unwrap(),
            zakura_rpc_url: "http://127.0.0.1:8232".into(),
            zakura_cookie: None,
            data_dir: "./transparent-filter-data".into(),
            start_height: transparent_filter::START_HEIGHT,
            cache_outputs: crate::ingest::prevout::DEFAULT_CACHE_OUTPUTS,
            poll_seconds: 10,
            commit_every: 1000,
            shard_dir: None,
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<(), ingest::BoxError> {
        if self.zakura_cookie.is_none() {
            return Err("filter publishing requires zakura_cookie".into());
        }
        if self.poll_seconds == 0 || self.commit_every == 0 {
            return Err("filter polling and commit intervals must be positive".into());
        }
        Ok(())
    }
}

pub async fn run(
    cli: Config,
    http: Http<axum::Router>,
    stop: Shutdown,
) -> Result<(), ingest::BoxError> {
    cli.validate()?;
    let cookie = cli
        .zakura_cookie
        .as_ref()
        .ok_or("--zakura-cookie is required to serve")?;
    let zakura = crate::ingest::zakura::connect(&cli.zakura_rpc_url, cookie)?;
    // Chain identity comes from the node, once, and is then pinned in the
    // store: a store built against one chain must never be served as another.
    let genesis_hash = zakura.block_hash_display(0).await?;
    if genesis_hash != transparent_filter::MAINNET_GENESIS_DISPLAY {
        tracing::warn!(
            %genesis_hash,
            expected = transparent_filter::MAINNET_GENESIS_DISPLAY,
            "node is not on Zcash mainnet; the store will be pinned to this chain"
        );
    }
    let store = FilterStore::open(
        &cli.data_dir,
        transparent_filter::PROFILE,
        &genesis_hash,
        cli.start_height,
    )?;
    tracing::info!(
        covered_through = ?store.covered_through(),
        filters_stored = store.filters_stored(),
        start_height = cli.start_height,
        "opened filter store"
    );

    let mut state = ServiceState::new(store);
    if let Some(dir) = &cli.shard_dir {
        // Verified against the map at load. A filter that does not match its
        // published digest would make every wallet under-match -- skipping
        // history it should have retrieved, which is a wrong answer rather
        // than an error -- so this refuses to start instead.
        let filters = ShardFilters::open(dir)?;
        tracing::info!(
            shards = filters.shard_count(),
            covered_through = ?filters.covered_through(),
            map_sha256 = %filters.map_digest(),
            dir = %dir.display(),
            "serving published shard filters"
        );
        state = state.with_shard_filters(filters);
    }
    let ingest_state = state.clone();
    let ingest_cli = cli.clone();
    let ingest_stop = stop.clone();
    let ingestion = tokio::spawn(async move {
        ingest::run(
            zakura,
            ingest_state,
            ingest_cli.cache_outputs,
            ingest_cli.poll_seconds,
            ingest_cli.commit_every,
            ingest_stop,
        )
        .await
    });

    tracing::info!(listen = %cli.listen, "transparent filter service started");
    supervise(ingestion, http.serve(cli.listen, router(state)), stop).await
}

/// Listener exit requests a boundary stop and waits for ingestion to commit.
/// An unexpected ingestion exit propagates to the root instead of serving stale
/// coverage indefinitely. An already requested shutdown is a normal completion.
async fn supervise(
    mut ingestion: tokio::task::JoinHandle<Result<(), ingest::BoxError>>,
    listener: pir_pipeline::service::HttpFuture,
    stop: Shutdown,
) -> Result<(), ingest::BoxError> {
    let mut tasks = Tasks::default();
    tasks.own(&ingestion);
    let result = tokio::select! {
        result = listener => result.map_err(ingest::BoxError::from),
        result = &mut ingestion => {
            let stopping = stop.requested();
            stop.request();
            result??;
            return if stopping { Ok(()) } else {
                Err("filter ingestion ended before publisher shutdown".into())
            };
        },
        _ = stop.cancelled() => Ok(()),
    };
    stop.request();
    ingestion.await??;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn unexpected_ingestion_exit_stops_the_service() {
        let stop = Shutdown::default();
        let ingestion = tokio::spawn(async { Err("journal unavailable".into()) });
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            supervise(ingestion, Box::pin(std::future::pending()), stop.clone()),
        )
        .await
        .unwrap();
        assert_eq!(result.unwrap_err().to_string(), "journal unavailable");
        assert!(stop.requested());
    }
    #[tokio::test]
    async fn listener_failure_waits_for_ingestion_to_finish() {
        let stop = Shutdown::default();
        let (finished, received) = tokio::sync::oneshot::channel();
        let ingestion = tokio::spawn({
            let stop = stop.clone();
            async move {
                stop.cancelled().await;
                tokio::task::yield_now().await;
                finished.send(()).unwrap();
                Ok(())
            }
        });
        let result = supervise(
            ingestion,
            Box::pin(async { Err(std::io::Error::other("listener unavailable")) }),
            stop,
        )
        .await;
        assert_eq!(result.unwrap_err().to_string(), "listener unavailable");
        received.await.unwrap();
    }
}
