//! Synthetic display publications for tests, local benches and tooling.
//!
//! Writes the layout the display controller writes, from records supplied by
//! the caller: immutable `sealed/<digest>/` and `recent/<digest>/` revision
//! directories, and candidate directories that hard-link them beside a
//! `txid-shards.json`. Block hashes are a deterministic function of height, so
//! adjacent shards chain. Nothing here is used to serve.

use super::set::{MANIFEST_FILE, MAP_FILE};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use transparent_events::{FeeState, TransactionMetadata, Txid};
use transparent_shard::display::{
    build_shard, DisplayManifest, DisplayMap, DisplayMapEntry, DisplaySealParams, ManifestHeader,
    DISPLAY_SCHEMA,
};
use transparent_shard::layout::Geometry;
use transparent_shard::txid::{DisplayOutput, TransparentDisplayRecord};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Network label of synthetic publications.
pub const NETWORK: &str = "synthetic";

pub fn genesis_hash() -> String {
    hex::encode(Sha256::digest(
        b"transparent-txid-display/synthetic-genesis",
    ))
}

/// The synthetic block hash at `height`, in display hex.
pub fn block_hash(height: u64) -> String {
    hex::encode(
        Sha256::new()
            .chain_update(b"transparent-txid-display/synthetic-block")
            .chain_update(height.to_le_bytes())
            .finalize(),
    )
}

/// The txid of synthetic record `index` under `seed`.
pub fn txid(seed: u64, index: u64) -> Txid {
    Txid(
        Sha256::new()
            .chain_update(b"transparent-txid-display/synthetic-txid")
            .chain_update(seed.to_le_bytes())
            .chain_update(index.to_le_bytes())
            .finalize()
            .into(),
    )
}

/// A non-coinbase record with one output whose script is `script_len` bytes,
/// plus `extra_outputs` standard 25-byte outputs.
pub fn record(
    seed: u64,
    index: u64,
    script_len: usize,
    extra_outputs: usize,
) -> TransparentDisplayRecord {
    let p2pkh = |tag: u64| {
        let mut script = vec![0x76, 0xa9, 0x14];
        script.extend_from_slice(&Sha256::digest(tag.to_le_bytes())[..20]);
        script.extend_from_slice(&[0x88, 0xac]);
        script
    };
    let mut outputs = vec![DisplayOutput {
        value: 1_000 + index % 100_000,
        script: if script_len == 25 {
            p2pkh(index)
        } else {
            vec![0x6a; script_len]
        },
    }];
    outputs.extend((0..extra_outputs as u64).map(|i| DisplayOutput {
        value: 546 + i,
        script: p2pkh(index ^ (i << 40)),
    }));
    TransparentDisplayRecord {
        txid: txid(seed, index),
        coinbase: false,
        metadata: TransactionMetadata {
            fee: FeeState::Exact(1_000 + index % 9_000),
            transparent_input_count: 1 + (index % 3) as u32,
            has_shielded_components: index.is_multiple_of(7),
        },
        outputs,
    }
}

/// `n` records with a mainnet-like size mix: about 88% inline, 8% one page,
/// 2.5% two pages, 1% three pages and 0.5% five pages.
pub fn records(n: usize, seed: u64) -> Vec<TransparentDisplayRecord> {
    (0..n as u64)
        .map(|index| {
            let draw = u64::from_le_bytes(
                Sha256::digest([seed.to_le_bytes(), index.to_le_bytes()].concat())[..8]
                    .try_into()
                    .unwrap(),
            );
            let spread = (draw >> 16) as usize;
            match draw % 1_000 {
                0..=879 => record(seed, index, 25, spread % 3),
                880..=959 => record(seed, index, 150 + spread % 3_850, 0),
                960..=984 => record(seed, index, 4_100 + spread % 3_900, 0),
                985..=994 => record(seed, index, 8_200 + spread % 3_900, 0),
                _ => record(seed, index, 16_300 + spread % 3_800, 0),
            }
        })
        .collect()
}

/// What one shard revision is, apart from its records.
#[derive(Clone, Debug)]
pub struct ShardSpec {
    pub shard_id: u64,
    pub start_height: u64,
    pub end_height: u64,
    pub sealed: bool,
    pub revision: u32,
    pub supersedes: String,
    pub parent_manifest_digest: String,
    pub n_buckets: u32,
    pub archive_target: u64,
    pub geometry: &'static Geometry,
}

/// One revision written to the store.
#[derive(Clone, Debug)]
pub struct Published {
    pub manifest: DisplayManifest,
    pub digest: String,
    /// `<root>/{sealed|recent}/<digest>`.
    pub dir: PathBuf,
}

impl Published {
    pub fn entry(&self) -> DisplayMapEntry {
        DisplayMapEntry::from_manifest(&self.manifest, &self.digest)
    }
}

/// Builds one shard and writes it, immutably, under `root`.
pub fn write_shard(
    root: &Path,
    spec: &ShardSpec,
    records: &[TransparentDisplayRecord],
) -> Result<Published, Error> {
    let built = build_shard(spec.shard_id, spec.geometry, spec.n_buckets, records)
        .map_err(|error| error.0)?;
    let manifest = DisplayManifest::new(
        ManifestHeader {
            network: NETWORK.into(),
            genesis_hash: genesis_hash(),
            shard_id: spec.shard_id,
            start_height: spec.start_height,
            end_height: spec.end_height,
            parent_block_hash: block_hash(spec.start_height.saturating_sub(1)),
            terminal_block_hash: block_hash(spec.end_height),
            parent_manifest_digest: spec.parent_manifest_digest.clone(),
            sealed: spec.sealed,
            revision: spec.revision,
            supersedes: spec.supersedes.clone(),
            archive_target: spec.archive_target,
        },
        spec.geometry,
        &built,
    );
    manifest.validate()?;
    let digest = manifest.digest();
    let store = root.join(if spec.sealed { "sealed" } else { "recent" });
    let dir = store.join(&digest);
    if dir.exists() {
        // Content-addressed: an existing directory must be this content.
        if std::fs::read(dir.join(MANIFEST_FILE))? != manifest.canonical_bytes() {
            return Err(format!("{} holds other content", dir.display()).into());
        }
        return Ok(Published {
            manifest,
            digest,
            dir,
        });
    }
    let temp = store.join(format!(".{digest}.partial"));
    let _ = std::fs::remove_dir_all(&temp);
    std::fs::create_dir_all(&temp)?;
    for table in built.tables() {
        for (segment, bytes) in built.segments(table).iter().enumerate() {
            std::fs::write(temp.join(table.file_name(segment)), bytes)?;
        }
    }
    std::fs::write(temp.join(MANIFEST_FILE), manifest.canonical_bytes())?;
    std::fs::rename(&temp, &dir)?;
    Ok(Published {
        manifest,
        digest,
        dir,
    })
}

/// Hard-links every file of revision directory `from` into `to`. Refuses to
/// copy: serving reuses verified tables only across shared inodes.
pub fn link_revision(from: &Path, to: &Path) -> Result<(), Error> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        std::fs::hard_link(entry.path(), to.join(entry.file_name()))?;
    }
    Ok(())
}

/// The map listing `shards`, oldest first.
pub fn map(params: &DisplaySealParams, shards: &[Published]) -> DisplayMap {
    DisplayMap {
        schema: DISPLAY_SCHEMA.into(),
        network: NETWORK.into(),
        genesis_hash: genesis_hash(),
        seal: *params,
        start_height: shards.first().map_or(0, |s| s.manifest.start_height),
        first_shard_id: shards.first().map_or(0, |s| s.manifest.shard_id),
        shards: shards.iter().map(Published::entry).collect(),
    }
}

/// Writes `<root>/candidate-<label>/`: a hard link of every shard and the map,
/// written last. Returns the directory and the map's SHA-256.
pub fn write_candidate(
    root: &Path,
    params: &DisplaySealParams,
    shards: &[Published],
    label: &str,
) -> Result<(PathBuf, String), Error> {
    let map = map(params, shards);
    map.check_shape()?;
    let dir = root.join(format!("candidate-{label}"));
    if dir.exists() {
        return Err(format!("{} already exists", dir.display()).into());
    }
    std::fs::create_dir_all(&dir)?;
    for shard in shards {
        link_revision(&shard.dir, &dir.join(&shard.digest))?;
    }
    std::fs::write(dir.join(MAP_FILE), map.to_bytes())?;
    Ok((dir, map.sha256()))
}

/// Appends `txid || height` records, the controller's tooling index format.
pub fn append_heights(path: &Path, heights: &[(Txid, u64)]) -> Result<(), Error> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let mut bytes = Vec::with_capacity(heights.len() * 40);
    for (txid, height) in heights {
        bytes.extend_from_slice(&txid.0);
        bytes.extend_from_slice(&height.to_le_bytes());
    }
    file.write_all(&bytes)?;
    Ok(())
}

/// Reads a `txid || height` index; heights must not decrease.
pub fn read_heights(path: &Path) -> Result<Vec<(Txid, u64)>, Error> {
    let bytes = std::fs::read(path)?;
    if bytes.len() % 40 != 0 {
        return Err(format!("{} is not a whole number of records", path.display()).into());
    }
    let mut out = Vec::with_capacity(bytes.len() / 40);
    for chunk in bytes.chunks_exact(40) {
        let height = u64::from_le_bytes(chunk[32..].try_into().unwrap());
        if out.last().is_some_and(|(_, last)| *last > height) {
            return Err("heights decrease".into());
        }
        out.push((Txid(chunk[..32].try_into().unwrap()), height));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_size_mix_spans_inline_and_every_page_class() {
        let records = records(4_000, 1);
        let mut pages = std::collections::BTreeMap::new();
        for record in &records {
            let len = record.encode().unwrap().len();
            let class = if len <= transparent_shard::txid::INLINE_BYTES {
                0
            } else {
                len.div_ceil(4_050)
            };
            *pages.entry(class).or_insert(0usize) += 1;
        }
        assert!(pages[&0] > 3_300, "{pages:?}");
        for class in [1, 2, 3, 5] {
            assert!(pages.contains_key(&class), "{pages:?}");
        }
        assert_eq!(pages.keys().max(), Some(&5));
    }

    #[test]
    fn heights_round_trip_and_must_not_decrease() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tooling/heights.bin");
        append_heights(&path, &[(txid(0, 1), 5), (txid(0, 2), 5)]).unwrap();
        append_heights(&path, &[(txid(0, 3), 9)]).unwrap();
        assert_eq!(read_heights(&path).unwrap().len(), 3);
        append_heights(&path, &[(txid(0, 4), 1)]).unwrap();
        assert!(read_heights(&path).is_err());
    }
}
