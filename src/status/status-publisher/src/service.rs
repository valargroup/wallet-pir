//! Embeddable Status application; process signals and HTTP listeners belong to the host.
use crate::publisher::{Config as PublisherConfig, Publisher};
use pir_pipeline::service::{Http, Shutdown, Tasks};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};
use zakura_server_chain::NodeClient;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub publisher: PublisherConfig,
    pub rpc_url: String,
    pub cookie: PathBuf,
    pub public_listen: Option<SocketAddr>,
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.publisher.public_enabled != self.public_listen.is_some() {
            return Err("public_enabled requires exactly one public listener".into());
        }
        if self.public_listen.is_some_and(|a| !a.ip().is_loopback()) {
            return Err("Status public proxy listener must be loopback".into());
        }
        Publisher::new(self.publisher.clone())?;
        Ok(())
    }
}
pub async fn run(config: Config, http: Http<axum::Router>, stop: Shutdown) -> Result<()> {
    config.validate()?;
    let publisher = Publisher::new(config.publisher)?;
    let mut tasks = Tasks::default();
    let mut public = config
        .public_listen
        .map(|addr| tokio::spawn(http.serve(addr, publisher.routes())));
    if let Some(task) = &public {
        tasks.own(task);
    }
    let rpc = NodeClient::from_cookie_file(config.rpc_url, config.cookie)?;
    let worker_stop = stop.clone();
    let thread = std::thread::Builder::new()
        .name("status-publisher".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .max_blocking_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(publisher.run_managed(rpc, http, worker_stop))
        })?;
    let mut joined = tokio::task::spawn_blocking(move || {
        thread
            .join()
            .map_err(|_| -> Box<dyn std::error::Error + Send + Sync> {
                "Status publisher thread panicked".into()
            })?
    });
    let result = tokio::select! {
        result = &mut joined => return result?,
        result = async { match &mut public { Some(t) => t.await, None => std::future::pending().await } } => result.map_err(|e| e.to_string()).and_then(|r| r.map_err(|e| e.to_string())),
    };
    stop.request();
    joined.await??;
    result?;
    Err("Status HTTP listener stopped".into())
}
