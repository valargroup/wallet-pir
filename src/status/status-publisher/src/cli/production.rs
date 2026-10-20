//! Status role diagnostics and the live encrypted probe used by monitoring.
use super::{require, AnyError};
use crate::{status, zakura::ZakuraClient};
use serde_json::json;
use status_query_server::protocol::*;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

pub use status::distributed::RoleConfig;

pub async fn role(config: RoleConfig) -> Result<(), AnyError> {
    status::distributed::serve_role(config).await
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
    let mut wallet = super::http_client::HttpClient::new(&origin, http);
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
