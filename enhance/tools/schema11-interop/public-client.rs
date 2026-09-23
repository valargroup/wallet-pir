//! Public HTTPS query check using the pinned wallet client and a chain-derived oracle.
use anyhow::{ensure, Context, Result};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fs, path::Path};
use zcash_protocol::consensus::{NetworkUpgrade, Parameters, MAIN_NETWORK};
use zakura_pir_enhance::{
    transport::{PendingClient, ReqwestTransport},
    AcceptedAnchor, ClientResourceLimits, GenerationAcceptance,
};

#[derive(Deserialize)]
struct OracleManifest {
    kind: String,
    oracle_sha256: String,
    published_anchor_at_end: u64,
    published_anchor_hash_at_end: String,
    published_anchor_tree_size_at_end: u64,
    generation_at_end: u64,
}

#[derive(Deserialize)]
struct OracleRecord {
    position: u64,
    record_hex: String,
}

#[derive(Serialize)]
struct Report<'a> {
    kind: &'static str,
    server: &'a str,
    generation: u64,
    anchor_height: u64,
    oracle_sha256: &'a str,
    exact_answers: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let server = args
        .next()
        .context("usage: public-client HTTPS_ORIGIN ORACLE_DIR")?;
    let directory = args
        .next()
        .context("usage: public-client HTTPS_ORIGIN ORACLE_DIR")?;
    ensure!(args.next().is_none(), "too many arguments");
    ensure!(server.starts_with("https://"), "HTTPS origin required");

    let manifest: OracleManifest =
        serde_json::from_slice(&fs::read(Path::new(&directory).join("manifest.json"))?)?;
    ensure!(
        manifest.kind == "enhance-chain-oracle-v1",
        "wrong oracle kind"
    );
    let oracle_bytes = fs::read(Path::new(&directory).join("oracle.json"))?;
    let digest = hex::encode(Sha256::digest(&oracle_bytes));
    ensure!(digest == manifest.oracle_sha256, "oracle SHA-256 mismatch");
    let records: Vec<OracleRecord> = serde_json::from_slice(&oracle_bytes)?;
    ensure!(!records.is_empty(), "empty oracle");

    let transport = ReqwestTransport::new()?;
    let pending = PendingClient::fetch(&transport, &server).await?;
    let public = pending.manifest();
    ensure!(
        public.generation == manifest.generation_at_end
            && public.anchor_height == manifest.published_anchor_at_end
            && public
                .anchor_block_hash
                .eq_ignore_ascii_case(&manifest.published_anchor_hash_at_end)
            && public.coverage.records == manifest.published_anchor_tree_size_at_end,
        "public generation moved or disagrees with the chain-derived anchor; extract a fresh oracle"
    );
    let hash: [u8; 32] = hex::decode(&manifest.published_anchor_hash_at_end)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid anchor hash"))?;
    let activation = MAIN_NETWORK
        .activation_height(NetworkUpgrade::Nu6_3)
        .context("mainnet NU6.3 activation unavailable in the pinned wallet consensus rules")?;
    let accepted = GenerationAcceptance::new(
        "main",
        u64::from(u32::from(activation)),
        AcceptedAnchor::new(
            manifest.published_anchor_at_end,
            hash,
            manifest.published_anchor_tree_size_at_end,
        ),
        ClientResourceLimits::with_cache(32_768, 1),
    );
    let mut client = pending.accept(&accepted)?;
    let mut exact = 0;
    for expected in records {
        let expected_bytes = hex::decode(&expected.record_hex)?;
        let stream = client.query_batch(&transport, [expected.position])?;
        futures_util::pin_mut!(stream);
        let result = stream.next().await.context("missing query result")?;
        ensure!(
            result.position == expected.position,
            "wrong result position"
        );
        let actual = result.record?;
        ensure!(
            actual.as_bytes().as_slice() == expected_bytes.as_slice(),
            "wrong record at position {}",
            expected.position
        );
        ensure!(
            stream.next().await.is_none(),
            "unexpected extra query result"
        );
        exact += 1;
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&Report {
            kind: "enhance-public-wallet-client-check-v1",
            server: &server,
            generation: manifest.generation_at_end,
            anchor_height: manifest.published_anchor_at_end,
            oracle_sha256: &digest,
            exact_answers: exact,
        })?
    );
    Ok(())
}
