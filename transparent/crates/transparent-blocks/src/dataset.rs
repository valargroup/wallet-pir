use crate::{digest, proto::CompactBlock, schema_digest};
use anyhow::{bail, ensure, Context, Result};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    path::Path,
};

pub const VARIANTS: [&str; 3] = ["transparent", "shielded", "combined"];
pub const MAX_BATCH_BYTES: u64 = 256 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Artifact {
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Batch {
    pub start: u64,
    pub end: u64,
    pub previous_hash: String,
    pub hash: String,
    pub artifacts: BTreeMap<String, Artifact>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub schema: String,
    pub protobuf_sha256: String,
    pub genesis_hash: String,
    pub start: u64,
    pub anchor_height: u64,
    pub anchor_hash: String,
    pub complete: bool,
    pub source: String,
    pub batches: Vec<Batch>,
}
impl Manifest {
    pub fn new(
        genesis_hash: String,
        start: u64,
        anchor_height: u64,
        anchor_hash: String,
        source: String,
    ) -> Self {
        Self {
            schema: "transparent-block-dataset-v1".into(),
            protobuf_sha256: schema_digest(),
            genesis_hash,
            start,
            anchor_height,
            anchor_hash,
            complete: false,
            source,
            batches: vec![],
        }
    }
    pub fn validate(&self, require_complete: bool) -> Result<()> {
        ensure!(
            self.schema == "transparent-block-dataset-v1"
                && self.protobuf_sha256 == schema_digest(),
            "unsupported dataset format"
        );
        ensure!(
            self.start <= self.anchor_height && self.anchor_height <= u64::from(u32::MAX),
            "invalid dataset range"
        );
        for hash in [&self.genesis_hash, &self.anchor_hash] {
            ensure!(hex::decode(hash)?.len() == 32, "invalid hash");
        }
        let mut next = self.start;
        let mut previous: Option<&str> = None;
        for batch in &self.batches {
            ensure!(
                batch.start == next
                    && batch.end >= next
                    && batch.end <= self.anchor_height
                    && batch.end - next < 1000,
                "invalid batch range"
            );
            if let Some(hash) = previous {
                ensure!(batch.previous_hash == hash, "batch chain discontinuity");
            }
            for variant in VARIANTS {
                for encoding in ["identity", "gzip"] {
                    let artifact = batch
                        .artifacts
                        .get(&format!("{variant}.{encoding}"))
                        .context("missing artifact")?;
                    ensure!(
                        artifact.bytes <= MAX_BATCH_BYTES
                            && hex::decode(&artifact.sha256)?.len() == 32,
                        "invalid artifact"
                    );
                }
            }
            next = batch.end + 1;
            previous = Some(&batch.hash);
        }
        if require_complete || self.complete {
            ensure!(
                self.complete
                    && next == self.anchor_height + 1
                    && previous == Some(self.anchor_hash.as_str()),
                "incomplete dataset"
            );
        }
        Ok(())
    }
    pub fn id(&self) -> Result<String> {
        Ok(digest(&serde_json::to_vec(self)?))
    }
    pub fn load(path: &Path, complete: bool) -> Result<Self> {
        let result: Self = serde_json::from_slice(&fs::read(path.join("manifest.json"))?)?;
        result.validate(complete)?;
        Ok(result)
    }
    pub fn save(&self, path: &Path) -> Result<()> {
        self.validate(false)?;
        atomic(
            &path.join("manifest.json"),
            &serde_json::to_vec_pretty(self)?,
        )
    }
}
pub fn atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension(format!("{}.tmp", std::process::id()));
    let mut file = fs::File::create(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(tmp, path)?;
    Ok(())
}
pub fn file_name(index: usize, variant: &str, encoding: &str) -> Result<String> {
    ensure!(
        VARIANTS.contains(&variant) && ["identity", "gzip"].contains(&encoding),
        "unknown representation"
    );
    Ok(format!("{index:06}-{variant}.{encoding}"))
}
pub fn representation(block: &CompactBlock, variant: &str) -> Result<CompactBlock> {
    let mut block = block.clone();
    match variant {
        "combined" => (),
        "transparent" => {
            block.chain_metadata = None;
            for tx in &mut block.vtx {
                tx.spends.clear();
                tx.outputs.clear();
                tx.actions.clear();
                tx.ironwood_actions.clear();
            }
            block
                .vtx
                .retain(|tx| !tx.vin.is_empty() || !tx.vout.is_empty());
        }
        "shielded" => {
            for tx in &mut block.vtx {
                tx.vin.clear();
                tx.vout.clear();
            }
            block.vtx.retain(|tx| {
                !tx.spends.is_empty()
                    || !tx.outputs.is_empty()
                    || !tx.actions.is_empty()
                    || !tx.ironwood_actions.is_empty()
            });
        }
        _ => bail!("unknown representation"),
    }
    Ok(block)
}
pub fn display_hash(bytes: &[u8]) -> Result<String> {
    ensure!(bytes.len() == 32, "hash must contain 32 bytes");
    Ok(hex::encode(bytes.iter().rev().copied().collect::<Vec<_>>()))
}
pub fn encode(blocks: &[CompactBlock], variant: &str) -> Result<Vec<u8>> {
    let mut raw = Vec::new();
    for block in blocks {
        representation(block, variant)?.encode_length_delimited(&mut raw)?;
    }
    ensure!(
        raw.len() as u64 <= MAX_BATCH_BYTES,
        "batch exceeds memory limit; export fewer blocks per batch"
    );
    Ok(raw)
}
pub fn decode(bytes: &[u8], encoding: &str) -> Result<Vec<CompactBlock>> {
    let raw = match encoding {
        "identity" => bytes.to_vec(),
        "gzip" => {
            let mut raw = Vec::new();
            GzDecoder::new(bytes)
                .take(MAX_BATCH_BYTES + 1)
                .read_to_end(&mut raw)?;
            raw
        }
        _ => bail!("unsupported encoding"),
    };
    ensure!(
        raw.len() as u64 <= MAX_BATCH_BYTES,
        "decoded batch too large"
    );
    let mut input = raw.as_slice();
    let mut blocks = Vec::new();
    while !input.is_empty() {
        ensure!(blocks.len() < 1000, "too many blocks");
        blocks.push(CompactBlock::decode_length_delimited(&mut input)?);
    }
    Ok(blocks)
}
pub fn verify_blocks(blocks: &[CompactBlock], batch: &Batch) -> Result<()> {
    ensure!(
        blocks.len() as u64 == batch.end - batch.start + 1,
        "missing or duplicate block"
    );
    let mut previous = batch.previous_hash.clone();
    for (offset, block) in blocks.iter().enumerate() {
        ensure!(
            block.height == batch.start + offset as u64
                && display_hash(&block.prev_hash)? == previous,
            "block chain discontinuity"
        );
        previous = display_hash(&block.hash)?;
        let mut last = None;
        for tx in &block.vtx {
            ensure!(
                tx.txid.len() == 32 && last.is_none_or(|i| tx.index > i),
                "invalid transaction ordering"
            );
            last = Some(tx.index);
            for input in &tx.vin {
                ensure!(input.prevout_txid.len() == 32, "invalid outpoint");
            }
        }
    }
    ensure!(previous == batch.hash, "batch terminal hash mismatch");
    Ok(())
}
pub fn write_batch(path: &Path, index: usize, blocks: &[CompactBlock]) -> Result<Batch> {
    let first = blocks.first().context("empty batch")?;
    let last = blocks.last().unwrap();
    let mut batch = Batch {
        start: first.height,
        end: last.height,
        previous_hash: display_hash(&first.prev_hash)?,
        hash: display_hash(&last.hash)?,
        artifacts: BTreeMap::new(),
    };
    verify_blocks(blocks, &batch)?;
    for variant in VARIANTS {
        let raw = encode(blocks, variant)?;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&raw)?;
        let gzip = encoder.finish()?;
        for (encoding, bytes) in [("identity", raw), ("gzip", gzip)] {
            let name = file_name(index, variant, encoding)?;
            atomic(&path.join(name), &bytes)?;
            batch.artifacts.insert(
                format!("{variant}.{encoding}"),
                Artifact {
                    bytes: bytes.len() as u64,
                    sha256: digest(&bytes),
                },
            );
        }
    }
    Ok(batch)
}
pub fn read_batch(
    path: &Path,
    manifest: &Manifest,
    index: usize,
    variant: &str,
    encoding: &str,
) -> Result<Vec<u8>> {
    let batch = manifest.batches.get(index).context("unknown batch")?;
    let name = file_name(index, variant, encoding)?;
    let bytes = fs::read(path.join(name))?;
    let expected = &batch.artifacts[&format!("{variant}.{encoding}")];
    ensure!(
        bytes.len() as u64 == expected.bytes && digest(&bytes) == expected.sha256,
        "artifact corruption"
    );
    Ok(bytes)
}
