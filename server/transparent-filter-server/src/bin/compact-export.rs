//! Resumable, read-only raw-chain export for the block-scanning benchmark.
use clap::Parser;
use std::{path::PathBuf, sync::Arc};
use transparent_blocks::dataset::{decode, read_batch, write_batch, Manifest};
use transparent_filter_server::{compact::compact_block, state::StateReader, zakura::ZakuraClient};
use zakura_chain::parameters::Network;
type Error = Box<dyn std::error::Error + Send + Sync>;
#[derive(Parser)]
struct Cli {
    #[arg(long)]
    state_dir: Option<PathBuf>,
    #[arg(long, default_value = "http://127.0.0.1:8232")]
    rpc_url: String,
    #[arg(long)]
    cookie: Option<PathBuf>,
    #[arg(long)]
    anchor_height: u64,
    #[arg(long)]
    anchor_hash: String,
    #[arg(long)]
    out_dir: PathBuf,
    #[arg(long, default_value_t = 1000)]
    batch_blocks: usize,
}
#[tokio::main]
async fn main() -> Result<(), Error> {
    let c = Cli::parse();
    if !(1..=1000).contains(&c.batch_blocks) {
        return Err("batch-blocks must be 1..1000".into());
    }
    let state = c
        .state_dir
        .as_ref()
        .map(|p| StateReader::open(p, Network::Mainnet))
        .transpose()?;
    let rpc = if state.is_none() {
        Some(ZakuraClient::from_cookie_file(
            &c.rpc_url,
            c.cookie.as_ref().ok_or("--cookie required with RPC")?,
        )?)
    } else {
        None
    };
    async fn fetch(
        state: &Option<StateReader>,
        rpc: &Option<ZakuraClient>,
        height: u64,
    ) -> Result<Arc<zakura_chain::block::Block>, Error> {
        if let Some(s) = state {
            Ok(s.raw_block(height)?)
        } else {
            Ok(Arc::new(rpc.as_ref().unwrap().block(height).await?.1))
        }
    }
    let genesis = fetch(&state, &rpc, 0).await?.hash().to_string();
    if fetch(&state, &rpc, c.anchor_height)
        .await?
        .hash()
        .to_string()
        != c.anchor_hash
    {
        return Err("accepted anchor does not match node".into());
    }
    std::fs::create_dir_all(&c.out_dir)?;
    let source = format!(
        "compact-export {}; zakura 1faf150fc3648aae22c55a6b30f8f5a9b9ce934e",
        transparent_blocks::digest(&std::fs::read(std::env::current_exe()?)?)
    );
    let mut manifest = if c.out_dir.join("manifest.json").exists() {
        Manifest::load(&c.out_dir, false)?
    } else {
        Manifest::new(
            genesis.clone(),
            0,
            c.anchor_height,
            c.anchor_hash.clone(),
            source.clone(),
        )
    };
    if manifest.source != source {
        return Err(
            "exporter changed; use the original binary to resume or a new dataset directory".into(),
        );
    }
    if manifest.genesis_hash != genesis
        || manifest.anchor_height != c.anchor_height
        || manifest.anchor_hash != c.anchor_hash
    {
        return Err("existing dataset belongs to another chain or target".into());
    }
    let mut counts = [0; 3];
    // Verify committed artifacts on resume, including representations not scanned by wallets.
    for (index, batch) in manifest.batches.iter().enumerate() {
        for variant in transparent_blocks::dataset::VARIANTS {
            for encoding in ["identity", "gzip"] {
                read_batch(&c.out_dir, &manifest, index, variant, encoding)?;
            }
        }
        if index + 1 == manifest.batches.len() {
            let blocks = decode(
                &read_batch(&c.out_dir, &manifest, index, "combined", "identity")?,
                "identity",
            )?;
            transparent_blocks::dataset::verify_blocks(&blocks, batch)?;
            let metadata = blocks
                .last()
                .unwrap()
                .chain_metadata
                .as_ref()
                .ok_or("missing tree sizes")?;
            counts = [
                metadata.sapling_commitment_tree_size,
                metadata.orchard_commitment_tree_size,
                metadata.ironwood_commitment_tree_size,
            ];
            if fetch(&state, &rpc, batch.end).await?.hash().to_string() != batch.hash {
                return Err("node changed below export checkpoint".into());
            }
        }
    }
    if manifest.complete {
        println!("Dataset already complete: {}", manifest.id()?);
        return Ok(());
    }
    manifest.save(&c.out_dir)?;
    let start = manifest.batches.last().map_or(0, |b| b.end + 1);
    let mut blocks = Vec::new();
    for height in start..=c.anchor_height {
        let block = fetch(&state, &rpc, height).await?;
        blocks.push(compact_block(&block, height, &mut counts)?);
        if blocks.len() == c.batch_blocks || height == c.anchor_height {
            let batch = write_batch(&c.out_dir, manifest.batches.len(), &blocks)?;
            manifest.batches.push(batch);
            manifest.save(&c.out_dir)?;
            blocks.clear();
            eprintln!(
                "compact export: {}/{} blocks",
                height + 1,
                c.anchor_height + 1
            );
        }
    }
    if fetch(&state, &rpc, c.anchor_height)
        .await?
        .hash()
        .to_string()
        != c.anchor_hash
    {
        return Err("node anchor changed during export".into());
    }
    manifest.complete = true;
    manifest.save(&c.out_dir)?;
    println!("Dataset complete: {}", manifest.id()?);
    Ok(())
}
