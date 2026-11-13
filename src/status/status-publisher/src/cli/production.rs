//! Live encrypted probes used by monitoring. Publication runs on pir-publisher.
use super::{require, AnyError};
use crate::{status, zakura::ZakuraClient};
use serde_json::json;
use sha2::{Digest, Sha256};
use status_shared::protocol::*;
use std::{
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

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
        // The session initializes on its own, so a block published since the
        // manifest above gives it a later anchor. That anchor is accepted here
        // and must be canonical once the answer is in.
        let mut session = (block.height, anchor);
        let answer = wallet
            .lookup(txid, Some(block.height), |manifest| {
                session = if manifest.anchor_height > block.height {
                    (manifest.anchor_height, manifest.anchor_hash)
                } else {
                    (block.height, anchor)
                };
                Ok(AcceptedAnchor {
                    network,
                    height: session.0,
                    hash: session.1,
                })
            })
            .await?;
        if session.0 > block.height {
            require(
                canonical_hash(&rpc, session.0).await? == session.1,
                "live session anchor is not canonical",
            )?;
        }
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

/// The node's hash at `height`, waiting up to 5 s for a block the publisher's node
/// reported before this one.
async fn canonical_hash(rpc: &ZakuraClient, height: u32) -> Result<Hash, AnyError> {
    for _ in 0..10 {
        if let Ok(hash) = rpc.block_hash(u64::from(height)).await {
            return Ok(hash);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Ok(rpc.block_hash(u64::from(height)).await?)
}

/// One encrypted lookup of a random txid, accepted against the service's own fresh
/// manifest. Deploy verification runs it on query hosts, which have no node: it proves
/// the session, query and decode round trip and that the answer is well formed, but not
/// a positive answer, which needs the canonical oracle `probe_live` reads.
pub async fn probe_local(origin: String) -> Result<(), AnyError> {
    // Init waits up to 15 s for a restarting publisher; stay just inside
    // pir-probe's 12 s per-product limit so a held init can still answer.
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(11))
        .build()?;
    let manifest: Manifest = http
        .get(format!("{origin}/v1/status/init"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    manifest.fresh(status::now_ms())?;
    let mut wallet = super::http_client::HttpClient::new(&origin, http);
    let seed = format!(
        "{}:{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let txid: Hash = Sha256::digest(seed.as_bytes()).into();
    let began = Instant::now();
    // Not found is only an answer inside the covered window, and the anchor is in it.
    let mut anchor_height = manifest.anchor_height;
    let answer = wallet
        .lookup(txid, Some(manifest.anchor_height), |manifest| {
            anchor_height = manifest.anchor_height;
            Ok(AcceptedAnchor {
                network: manifest.network,
                height: manifest.anchor_height,
                hash: manifest.anchor_hash,
            })
        })
        .await?;
    require(
        answer == Observation::NotFound,
        "a random txid was not reported missing",
    )?;
    println!(
        "{}",
        json!({"phase":"local_encrypted_probe","queries":1,"anchor_height":anchor_height,
            "elapsed_ms":began.elapsed().as_secs_f64()*1000.,"source":"own_manifest"})
    );
    Ok(())
}
