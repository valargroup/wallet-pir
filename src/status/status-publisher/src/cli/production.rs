//! Production subcommands: a private Status role (`role`), the controller
//! (`serve-distributed`) and the live encrypted probe that monitoring parses
//! (`probe-live`).
use super::{require, AnyError};
use crate::{status, zakura::ZakuraClient};
use serde_json::json;
use status_query_server::protocol::*;
use std::{
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::net::TcpListener;

pub use status::distributed::RoleConfig;

pub async fn role(config: RoleConfig) -> Result<(), AnyError> {
    status::distributed::serve_role(config).await
}

pub async fn serve_distributed(
    config: PathBuf,
    rpc_url: String,
    cookie: PathBuf,
    public_listen: Option<SocketAddr>,
) -> Result<(), AnyError> {
    let config: crate::publisher::Config = serde_json::from_slice(&std::fs::read(config)?)?;
    require(
        !config.public_enabled || public_listen.is_some(),
        "public_enabled requires --public-listen",
    )?;
    require(
        config.public_enabled || public_listen.is_none(),
        "--public-listen requires public_enabled in the controller config",
    )?;
    require(
        public_listen.is_none_or(|l| l.ip().is_loopback()),
        "--public-listen must be loopback; the HTTPS ingress proxies to it",
    )?;
    let publisher = crate::publisher::Publisher::new(config)?;
    // Bind before starting the controller so a port conflict fails fast.
    let public = match public_listen {
        Some(listen) => Some(TcpListener::bind(listen).await?),
        None => None,
    };
    publisher.start(ZakuraClient::from_cookie_file(rpc_url, cookie)?)?;
    match public {
        // Public admission still depends on the publisher's healthy flag.
        Some(listener) => {
            let routes = publisher.routes();
            tokio::select! {
                result = axum::serve(listener, routes) => result?,
                result = tokio::signal::ctrl_c() => result?,
            }
        }
        None => tokio::signal::ctrl_c().await?,
    }
    Ok(())
}

pub async fn probe_live(
    origin: String,
    rpc_url: String,
    cookie: PathBuf,
    queries: usize,
) -> Result<(), AnyError> {
    require((1..=100).contains(&queries), "queries must be 1..=100")?;
    let rpc = ZakuraClient::from_cookie_file(rpc_url, cookie)?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let manifest: Manifest = http
        .get(format!("{origin}/v1/status/init"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    manifest.fresh(status::now_ms())?;
    let network = rpc.block_hash(0).await?;
    let anchor = rpc.block_hash(u64::from(manifest.anchor_height)).await?;
    require(
        manifest.network == network && manifest.anchor_hash == anchor,
        "live manifest anchor is not canonical",
    )?;
    let block = crate::source::status_block(&rpc, u64::from(manifest.anchor_height)).await?;
    require(
        block.hash == anchor,
        "raw block hash differs from canonical anchor",
    )?;
    let txid = *block
        .txids
        .first()
        .ok_or("anchor block has no transactions")?;
    let mut wallet = status_query_server::protocol::HttpClient::new(&origin, http);
    let began = Instant::now();
    for _ in 0..queries {
        let answer = wallet
            .lookup(txid, Some(block.height), |_| {
                Ok(AcceptedAnchor {
                    network,
                    height: block.height,
                    hash: anchor,
                })
            })
            .await?;
        let current_anchor = rpc.block_hash(u64::from(block.height)).await?;
        require(
            current_anchor == anchor,
            "live probe oracle is not canonical after query",
        )?;
        require(
            answer == Observation::Mined(block.height),
            "live encrypted answer differs from canonical block oracle",
        )?;
    }
    println!(
        "{}",
        json!({"phase":"live_encrypted_probe","queries":queries,"correct":queries,
            "anchor_height":block.height,"elapsed_ms":began.elapsed().as_secs_f64()*1000.,
            "source":"canonical_rpc","txid_logged":false})
    );
    Ok(())
}
