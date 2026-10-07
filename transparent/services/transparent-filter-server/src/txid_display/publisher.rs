//! Display revisions and candidate publications on disk.
//!
//! Under the publication root:
//! - `sealed/<digest>/` and `recent/<digest>/` each hold one revision: its
//!   canonical `manifest.json` and segment files. A revision is written once;
//!   writing it again verifies the existing bytes instead, and a difference
//!   is a hard error because a published identity never changes content.
//! - `index/<sha256>.json` holds one archive index chunk of the split map
//!   (`transparent_shard::display::split`), written once when a seal or a
//!   window drop first produces it.
//! - `candidate-<tip>-<hash>-<nanos>/` is one publication: a hard link of
//!   every revision the map names and of every index chunk
//!   (`txid-index-<sha256>.json`), the recent map `txid-map.json`, then
//!   `txid-shards.json`, written last.
//! - `display-root.json` pins what every shard depends on (geometry, seal
//!   parameters, chain, start), and `active.json` names the activated
//!   candidate together with every seal made since the start.
//!
//! `sealed/` is never collected. Candidates, recent revisions and index
//! chunks no retained candidate links are.

use super::cache::flatten;
use super::timeline::{HeightIndex, Timeline};
use crate::publication::{link_revision, write_atomic, write_immutable, BoxError, Journal};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::io::Read;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::time::Instant;
use transparent_shard::display::split::{index_file, RECENT_MAP_FILE};
use transparent_shard::display::{
    build_shard, display_by_name, plan_seals, verify, verify_rows, DisplayManifest, DisplayMap,
    DisplayMapEntry, DisplaySealParams, DisplayTable, ManifestHeader, DISPLAY_SCHEMA, MAX_BUCKETS,
};
use transparent_shard::layout::Geometry;
use transparent_shard::manifest::{PublishedRevision, RevisionError};
use transparent_shard::txid::{TransparentDisplayRecord, ROW_BYTES};

pub const MAP_FILE: &str = "txid-shards.json";
pub const MANIFEST_FILE: &str = "manifest.json";
pub const ROOT_FILE: &str = "display-root.json";
pub const ACTIVE_FILE: &str = "active.json";
pub const HALTED_FILE: &str = "halted.json";
pub const SEALED_DIR: &str = "sealed";
pub const RECENT_DIR: &str = "recent";
pub const INDEX_DIR: &str = "index";
pub const CANDIDATE_PREFIX: &str = "candidate-";

/// What every shard of a root depends on. Written once by `bootstrap`; a
/// controller refuses a root whose record differs from what it would build.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DisplayRoot {
    pub geometry: String,
    pub seal: DisplaySealParams,
    /// Archives the map lists at most; a seal beyond it drops the oldest.
    pub max_archive_shards: u64,
    pub network: String,
    pub genesis_hash: String,
    /// First display height, shard 0's start.
    pub start_height: u64,
    /// The block before `start_height`, in display hex.
    pub base_parent: String,
}

impl DisplayRoot {
    pub fn geometry(&self) -> Result<&'static Geometry, BoxError> {
        display_by_name(&self.geometry)
            .ok_or_else(|| format!("unknown display geometry {:?}", self.geometry).into())
    }

    pub fn validate(&self) -> Result<&'static Geometry, BoxError> {
        let geometry = self.geometry()?;
        let s = &self.seal;
        for n in [s.n_archive, s.n_recent] {
            if n == 0 || n > MAX_BUCKETS {
                return Err(format!("{n} buckets is outside 1..={MAX_BUCKETS}").into());
            }
        }
        if s.archive_target == 0 {
            return Err("archive target must be positive".into());
        }
        // A margin of at least one block keeps the recent shard non-empty
        // after every seal.
        if s.reorg_margin == 0 {
            return Err("reorg margin must be at least one block".into());
        }
        if self.max_archive_shards == 0 {
            return Err("the archive window must hold at least one shard".into());
        }
        if self.start_height == 0 || self.base_parent.len() != 64 || self.genesis_hash.len() != 64 {
            return Err("display start needs a parent block in the journal".into());
        }
        Ok(geometry)
    }

    pub fn load(root: &Path) -> Result<Self, BoxError> {
        let bytes = std::fs::read(root.join(ROOT_FILE)).map_err(|e| {
            format!(
                "{} has no {ROOT_FILE}; bootstrap it first: {e}",
                root.display()
            )
        })?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn store(&self, root: &Path) -> Result<(), BoxError> {
        let bytes = serde_json::to_vec_pretty(self)?;
        write_immutable(root, ROOT_FILE, &bytes)
            .map_err(|e| format!("display root parameters differ from {ROOT_FILE}: {e}").into())
    }

    pub fn map(&self, archives: &[DisplayMapEntry], recent: DisplayMapEntry) -> DisplayMap {
        let mut shards = archives.to_vec();
        shards.push(recent);
        DisplayMap {
            schema: DISPLAY_SCHEMA.to_string(),
            network: self.network.clone(),
            genesis_hash: self.genesis_hash.clone(),
            seal: self.seal,
            start_height: shards[0].start_height,
            first_shard_id: shards[0].shard_id,
            shards,
        }
    }
}

/// One seal as the controller remembers it, kept after the window drops it
/// so the parent chain and `verify` can still name it.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SealRecord {
    pub shard_id: u64,
    pub start_height: u64,
    pub end_height: u64,
    pub digest: String,
    pub terminal_block_hash: String,
    /// The tip the seal rule was evaluated at when it chose this seal.
    pub decided_tip: u64,
}

/// `active.json`: the activated candidate and the seal history.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ActiveRecord {
    pub directory: PathBuf,
    pub map_sha256: String,
    pub tip: u64,
    pub tip_hash: String,
    pub cycle: u64,
    pub seals: Vec<SealRecord>,
}

impl ActiveRecord {
    pub fn load(root: &Path) -> Result<Self, BoxError> {
        let bytes = std::fs::read(root.join(ACTIVE_FILE)).map_err(|e| {
            format!(
                "{} has no {ACTIVE_FILE}; bootstrap it first: {e}",
                root.display()
            )
        })?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub fn store(&self, root: &Path) -> Result<(), BoxError> {
        write_atomic(&root.join(ACTIVE_FILE), &serde_json::to_vec_pretty(self)?)
    }

    /// SHA-256 over every sealed digest since the start, in order: changes
    /// exactly when a seal is published, and only by extension.
    pub fn archive_chain_sha256(&self) -> String {
        chain_sha256(&self.seals)
    }
}

pub fn chain_sha256(seals: &[SealRecord]) -> String {
    let mut hasher = Sha256::new();
    for seal in seals {
        hasher.update(seal.digest.as_bytes());
    }
    hex::encode(hasher.finalize())
}

/// Reads a candidate's map and checks it against the digest that names it.
pub fn read_map(directory: &Path, map_sha256: &str) -> Result<DisplayMap, BoxError> {
    let bytes = std::fs::read(directory.join(MAP_FILE))?;
    if hex::encode(Sha256::digest(&bytes)) != map_sha256 {
        return Err(format!("{} does not match {map_sha256}", directory.display()).into());
    }
    let map: DisplayMap = serde_json::from_slice(&bytes)?;
    map.check_shape()?;
    Ok(map)
}

/// The identity inputs of one shard; the records supply the rest.
#[derive(Clone, Debug)]
pub struct ShardSpec {
    pub shard_id: u64,
    pub start: u64,
    pub end: u64,
    pub parent_block_hash: String,
    pub terminal_block_hash: String,
    /// The previous sealed shard's digest; empty for shard 0.
    pub parent_manifest_digest: String,
    pub sealed: bool,
    /// The recent revision this one may supersede: same shard id and start.
    pub previous: Option<PublishedRevision>,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Timings {
    pub build_s: f64,
    pub verify_s: f64,
    pub digest_s: f64,
    pub write_s: f64,
}

#[derive(Clone, Debug)]
pub struct PublishedShard {
    pub manifest: DisplayManifest,
    pub digest: String,
    pub entry: DisplayMapEntry,
    pub directory: PathBuf,
    pub timings: Timings,
    /// Directory row bytes before zero padding, every bucket and segment.
    pub used_bytes: u64,
}

impl PublishedShard {
    pub fn revision(&self) -> PublishedRevision {
        PublishedRevision {
            digest: self.digest.clone(),
            revision: self.manifest.revision,
            supersedes: self.manifest.supersedes.clone(),
            sealed: self.manifest.sealed,
        }
    }

    pub fn summary(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.manifest.shard_id,
            "start": self.manifest.start_height,
            "end": self.manifest.end_height,
            "records": self.manifest.records,
            "dir_segments": self.entry.directory_segments,
            "page_segments": self.entry.page_segments,
            "used_bytes": self.used_bytes,
            "digest": self.digest,
            "revision": self.manifest.revision,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    /// Bytes on disk differ under an identity that was already published.
    #[error("published content changed under an immutable identity: {0}")]
    Immutable(String),
    #[error(transparent)]
    Revision(#[from] RevisionError),
    #[error("{0}")]
    Other(String),
}

impl From<BoxError> for PublishError {
    fn from(error: BoxError) -> Self {
        Self::Other(error.to_string())
    }
}

fn other(error: impl std::fmt::Display) -> PublishError {
    PublishError::Other(error.to_string())
}

/// Builds, verifies and writes one shard into `<root>/{sealed|recent}/<digest>/`.
///
/// A sealed shard is content-pure: revision 0, nothing superseded. A recent
/// shard reproduces its previous revision's identity when its content is the
/// same, and otherwise takes the next revision, superseding it.
pub fn publish_shard(
    root: &Path,
    layout: &DisplayRoot,
    spec: &ShardSpec,
    records: &[TransparentDisplayRecord],
) -> Result<PublishedShard, PublishError> {
    let geometry = layout.geometry()?;
    let n_buckets = if spec.sealed {
        layout.seal.n_archive
    } else {
        layout.seal.n_recent
    };
    let started = Instant::now();
    let built = build_shard(spec.shard_id, geometry, n_buckets, records).map_err(other)?;
    let build_s = started.elapsed().as_secs_f64();
    let started = Instant::now();
    verify(spec.shard_id, geometry, &built).map_err(other)?;
    let verify_s = started.elapsed().as_secs_f64();

    let started = Instant::now();
    let header = |revision: u32, supersedes: String| ManifestHeader {
        network: layout.network.clone(),
        genesis_hash: layout.genesis_hash.clone(),
        shard_id: spec.shard_id,
        start_height: spec.start,
        end_height: spec.end,
        parent_block_hash: spec.parent_block_hash.clone(),
        terminal_block_hash: spec.terminal_block_hash.clone(),
        parent_manifest_digest: spec.parent_manifest_digest.clone(),
        sealed: spec.sealed,
        revision,
        supersedes,
        archive_target: layout.seal.archive_target,
    };
    let manifest = match (&spec.previous, spec.sealed) {
        (Some(_), true) => return Err(other("a sealed shard has no previous revision")),
        (None, _) => DisplayManifest::new(header(0, String::new()), geometry, &built),
        (Some(previous), false) => {
            // Digest under the published numbering first: identical content
            // must keep the identity it already has.
            let same = DisplayManifest::new(
                header(previous.revision, previous.supersedes.clone()),
                geometry,
                &built,
            );
            let reproduced = same.digest() == previous.digest;
            let (revision, supersedes) =
                PublishedRevision::next(spec.shard_id, Some(previous), reproduced)?;
            // Only the numbering differs; the segment digests carry over.
            DisplayManifest {
                revision,
                supersedes,
                ..same
            }
        }
    };
    manifest.validate().map_err(other)?;
    let digest = manifest.digest();
    let digest_s = started.elapsed().as_secs_f64();

    let started = Instant::now();
    let directory = root
        .join(if spec.sealed { SEALED_DIR } else { RECENT_DIR })
        .join(&digest);
    std::fs::create_dir_all(&directory).map_err(other)?;
    let write = |name: &str, bytes: &[u8]| {
        let existed = directory.join(name).exists();
        write_immutable(&directory, name, bytes).map_err(|e| {
            if existed {
                PublishError::Immutable(e.to_string())
            } else {
                other(e)
            }
        })
    };
    for table in built.tables() {
        for (segment, bytes) in built.segments(table).iter().enumerate() {
            write(&table.file_name(segment), bytes)?;
        }
    }
    // The manifest goes last: its presence means every segment is in place.
    write(MANIFEST_FILE, &manifest.canonical_bytes())?;
    let write_s = started.elapsed().as_secs_f64();

    Ok(PublishedShard {
        entry: DisplayMapEntry::from_manifest(&manifest, &digest),
        used_bytes: built.buckets.iter().map(|b| b.used_bytes).sum(),
        manifest,
        digest,
        directory,
        timings: Timings {
            build_s,
            verify_s,
            digest_s,
            write_s,
        },
    })
}

/// Writes one candidate: a hard link of every named revision and index
/// chunk, the recent map, then the full map.
///
/// Returns the candidate directory and the map's SHA-256, which is its
/// identity on the control protocol.
pub fn write_candidate(root: &Path, map: &DisplayMap) -> Result<(PathBuf, String), BoxError> {
    let last = map.shards.last().ok_or("an empty display map")?;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let directory = root.join(format!(
        "{CANDIDATE_PREFIX}{}-{}-{nanos}",
        last.end_height, last.terminal_block_hash
    ));
    std::fs::create_dir(&directory)?;
    for entry in &map.shards {
        let tier = if entry.sealed { SEALED_DIR } else { RECENT_DIR };
        link_revision(&root.join(tier), &directory, &entry.manifest_digest)?;
    }
    let split = map.split()?;
    let index = root.join(INDEX_DIR);
    std::fs::create_dir_all(&index)?;
    for chunk in &split.chunks {
        let name = format!("{}.json", chunk.sha256);
        write_immutable(&index, &name, &chunk.bytes)?;
        std::fs::hard_link(index.join(&name), directory.join(index_file(&chunk.sha256)))?;
    }
    write_atomic(&directory.join(RECENT_MAP_FILE), &split.recent_bytes)?;
    let bytes = map.to_bytes();
    write_atomic(&directory.join(MAP_FILE), &bytes)?;
    Ok((directory, hex::encode(Sha256::digest(&bytes))))
}

/// Removes unused candidates and the recent revisions only they named.
///
/// Keeps every directory in `keep` (the active candidate, worker-held ones)
/// and the `newest` most recent others. A recent revision survives while any
/// retained candidate links it. `sealed/` is never touched.
pub fn collect(
    root: &Path,
    keep: &BTreeSet<PathBuf>,
    newest: usize,
) -> Result<Vec<PathBuf>, BoxError> {
    let mut candidates: Vec<_> = std::fs::read_dir(root)?
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_ok_and(|t| t.is_dir())
                && e.file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with(CANDIDATE_PREFIX))
        })
        .map(|e| {
            let modified = e.metadata().and_then(|m| m.modified()).ok();
            (modified, e.path())
        })
        .collect();
    candidates.sort_by(|a, b| b.cmp(a));
    // Candidates live directly under the root, so names identify them however
    // the kept paths were spelled.
    let keep: BTreeSet<_> = keep.iter().filter_map(|p| p.file_name()).collect();
    let mut removed = Vec::new();
    let mut retained = Vec::new();
    let mut unused = 0;
    for (_, path) in candidates {
        if path.file_name().is_some_and(|name| keep.contains(name)) {
            retained.push(path);
        } else if unused < newest {
            unused += 1;
            retained.push(path);
        } else {
            std::fs::remove_dir_all(&path)?;
            removed.push(path);
        }
    }
    let mut linked = BTreeSet::new();
    let mut chunks = BTreeSet::new();
    for candidate in &retained {
        for entry in std::fs::read_dir(candidate)?.filter_map(Result::ok) {
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                linked.insert(entry.file_name());
            } else if let Some(sha) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix("txid-index-"))
            {
                chunks.insert(sha.to_string());
            }
        }
    }
    let index = root.join(INDEX_DIR);
    if index.is_dir() {
        for entry in std::fs::read_dir(&index)?.filter_map(Result::ok) {
            let name = entry.file_name().to_string_lossy().into_owned();
            if entry.file_type().is_ok_and(|t| t.is_file()) && !chunks.contains(&name) {
                std::fs::remove_file(entry.path())?;
                removed.push(entry.path());
            }
        }
    }
    let recent = root.join(RECENT_DIR);
    if recent.is_dir() {
        for entry in std::fs::read_dir(&recent)?.filter_map(Result::ok) {
            if entry.file_type().is_ok_and(|t| t.is_dir()) && !linked.contains(&entry.file_name()) {
                std::fs::remove_dir_all(entry.path())?;
                removed.push(entry.path());
            }
        }
    }
    Ok(removed)
}

/// Refuses a root where revisions cannot be hard linked into candidates.
///
/// `link_revision` falls back to copying when a link fails, which across a
/// filesystem boundary would copy every archive on every block.
pub fn probe_hard_links(root: &Path) -> Result<(), BoxError> {
    let pid = std::process::id();
    for tier in [SEALED_DIR, RECENT_DIR] {
        let dir = root.join(tier);
        std::fs::create_dir_all(&dir)?;
        let source = dir.join(format!(".link-probe-{pid}"));
        let target = root.join(format!(".link-probe-{pid}-{tier}"));
        let _ = std::fs::remove_file(&target);
        std::fs::write(&source, b"probe")?;
        let linked = std::fs::hard_link(&source, &target).and_then(|()| {
            let (a, b) = (std::fs::metadata(&source)?, std::fs::metadata(&target)?);
            Ok(a.dev() == b.dev() && a.ino() == b.ino())
        });
        let _ = std::fs::remove_file(&source);
        let _ = std::fs::remove_file(&target);
        match linked {
            Ok(true) => {}
            Ok(false) => {
                return Err(format!(
                    "{} does not hard link into {}",
                    dir.display(),
                    root.display()
                )
                .into())
            }
            Err(e) => {
                return Err(format!(
                    "{} cannot hard link into {}: {e}; keep the display root on one filesystem",
                    dir.display(),
                    root.display()
                )
                .into())
            }
        }
    }
    Ok(())
}

/// Checks a revision directory end to end, reading it as a worker would.
///
/// The manifest must hash to `digest` and be in canonical form, the directory
/// must hold exactly its files, every segment must hash to its recorded
/// SHA-256, and the rows must pass the display verifier: bucket membership,
/// candidate placement, duplicates, page extents and orphan fragments.
pub fn verify_dir(directory: &Path, digest: &str) -> Result<DisplayManifest, BoxError> {
    let bytes = std::fs::read(directory.join(MANIFEST_FILE))?;
    if hex::encode(Sha256::digest(&bytes)) != digest {
        return Err(format!(
            "{} manifest does not hash to its digest",
            directory.display()
        )
        .into());
    }
    let manifest: DisplayManifest = serde_json::from_slice(&bytes)?;
    if manifest.canonical_bytes() != bytes {
        return Err(format!("{} manifest is not canonical", directory.display()).into());
    }
    let geometry = manifest.validate()?;
    let mut expected = BTreeSet::from([MANIFEST_FILE.to_string()]);
    for table in manifest.tables() {
        for segment in 0..manifest.segments(table).map_or(0, <[_]>::len) {
            expected.insert(table.file_name(segment));
        }
    }
    let present: BTreeSet<String> = std::fs::read_dir(directory)?
        .map(|e| e.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<Result<_, _>>()?;
    if present != expected {
        return Err(format!("{} files differ from its manifest", directory.display()).into());
    }
    let mut files = std::collections::BTreeMap::new();
    for table in manifest.tables() {
        for (segment, recorded) in manifest.segments(table).unwrap_or(&[]).iter().enumerate() {
            let path = directory.join(table.file_name(segment));
            let mut file = std::fs::File::open(&path)?;
            if file.metadata()?.len() != recorded.rows * ROW_BYTES as u64 {
                return Err(format!("{} length", path.display()).into());
            }
            let mut hasher = Sha256::new();
            let mut buffer = vec![0u8; 1 << 20];
            loop {
                let n = file.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buffer[..n]);
            }
            if hex::encode(hasher.finalize()) != recorded.sha256 {
                return Err(format!("{} does not hash to its manifest", path.display()).into());
            }
            files.insert((table, segment), file);
        }
    }
    let segments: Vec<usize> = manifest
        .buckets
        .iter()
        .map(|b| b.directory_segments.len())
        .collect();
    let records: Vec<u64> = manifest.buckets.iter().map(|b| b.records).collect();
    let verified = verify_rows(
        manifest.shard_id,
        geometry,
        &segments,
        manifest.page_segments.len(),
        &records,
        |table: DisplayTable, segment, row| {
            let file = files
                .get(&(table, segment))
                .ok_or_else(|| transparent_shard::txid::Error("segment out of range".into()))?;
            let mut bytes = vec![0u8; ROW_BYTES];
            file.read_exact_at(&mut bytes, (row * ROW_BYTES) as u64)
                .map_err(|e| transparent_shard::txid::Error(e.to_string()))?;
            Ok(bytes)
        },
    )?;
    let histograms: Vec<_> = manifest
        .buckets
        .iter()
        .map(|b| b.page_histogram.clone())
        .collect();
    if verified.page_histograms != histograms {
        return Err(format!(
            "{} page histogram differs from its rows",
            directory.display()
        )
        .into());
    }
    Ok(manifest)
}

/// What `bootstrap` published.
pub struct Bootstrapped {
    pub seals: Vec<SealRecord>,
    pub map: DisplayMap,
    pub candidate: PathBuf,
    pub map_sha256: String,
    pub recent: PublishedShard,
}

fn read_range(
    store: &impl Journal,
    from: u64,
    through: u64,
) -> Result<Vec<TransparentDisplayRecord>, BoxError> {
    let mut records = Vec::new();
    for height in from..=through {
        records.extend(store.display_at(height)?);
    }
    Ok(records)
}

fn hash_at(store: &impl Journal, height: u64) -> Result<String, BoxError> {
    Ok(store
        .block_at(height)
        .ok_or_else(|| format!("journal has no block at {height}"))?
        .block_hash
        .to_display_hex())
}

/// Publishes a fresh root from the journal through `through`: every seal the
/// rule makes over `[start, through]`, the recent shard after the last one,
/// and candidate 0 with `active.json`. No worker is involved.
///
/// Seals depend only on the start, the targets and the chain, so this is also
/// how `verify` reproduces a running root's archives.
pub fn bootstrap(
    store: &impl Journal,
    root: &Path,
    layout: &DisplayRoot,
    through: u64,
) -> Result<Bootstrapped, BoxError> {
    layout.validate()?;
    let start = layout.start_height;
    if root.join(ACTIVE_FILE).exists() {
        return Err(format!("{} is already bootstrapped", root.display()).into());
    }
    if store.genesis_hash() != layout.genesis_hash {
        return Err("journal genesis differs from the display root".into());
    }
    if start <= store.start_height() {
        return Err(format!(
            "display start {start} must be above the journal start {}, whose block is its parent",
            store.start_height()
        )
        .into());
    }
    let end = store.covered_through().ok_or("the journal is empty")?;
    if through < start || through > end {
        return Err(format!("--through {through} is outside {start}..={end}").into());
    }
    if hash_at(store, start - 1)? != layout.base_parent {
        return Err("the display start's parent is not the journal's block".into());
    }
    std::fs::create_dir_all(root)?;
    // Workers are handed absolute candidate paths.
    let root = &std::fs::canonicalize(root)?;
    probe_hard_links(root)?;
    layout.store(root)?;
    let timeline = Timeline::open(root)?;
    let mut index = HeightIndex::open(root)?;
    index.truncate_after(start - 1)?;

    // One pass for counts (and the tooling index), then each range is read
    // again only when it is built, so memory stays at one shard.
    let started = Instant::now();
    let mut counts = Vec::with_capacity((through - start + 1) as usize);
    for height in start..=through {
        let records = store.display_at(height)?;
        counts.push(transparent_shard::display::height_counts(
            &layout.seal,
            &records,
        ));
        index.append(height, &records)?;
    }
    let count_s = started.elapsed().as_secs_f64();
    let ranges = plan_seals(&layout.seal, start, &counts);

    let mut seals: Vec<SealRecord> = Vec::new();
    let mut entries = Vec::new();
    for (id, range) in ranges.iter().enumerate() {
        let records = read_range(store, *range.start(), *range.end())?;
        let spec = ShardSpec {
            shard_id: id as u64,
            start: *range.start(),
            end: *range.end(),
            parent_block_hash: seals.last().map_or(layout.base_parent.clone(), |s| {
                s.terminal_block_hash.clone()
            }),
            terminal_block_hash: hash_at(store, *range.end())?,
            parent_manifest_digest: seals.last().map_or(String::new(), |s| s.digest.clone()),
            sealed: true,
            previous: None,
        };
        let shard = publish_shard(root, layout, &spec, &records)?;
        timeline.record(
            "seal",
            serde_json::json!({
                "shard_id": spec.shard_id, "start": spec.start, "end": spec.end,
                "decided_tip": through, "bootstrap": true,
                "bucket_counts": shard.manifest.buckets.iter().map(|b| b.records).collect::<Vec<_>>(),
                "build_s": shard.timings.build_s, "verify_s": shard.timings.verify_s,
                "digest_s": shard.timings.digest_s, "write_s": shard.timings.write_s,
                "stage_s": null, "digest": shard.digest,
            }),
        );
        seals.push(SealRecord {
            shard_id: spec.shard_id,
            start_height: spec.start,
            end_height: spec.end,
            digest: shard.digest.clone(),
            terminal_block_hash: spec.terminal_block_hash,
            decided_tip: through,
        });
        entries.push(shard.entry);
    }

    let recent_start = seals.last().map_or(start, |s| s.end_height + 1);
    let spec = ShardSpec {
        shard_id: seals.len() as u64,
        start: recent_start,
        end: through,
        parent_block_hash: seals.last().map_or(layout.base_parent.clone(), |s| {
            s.terminal_block_hash.clone()
        }),
        terminal_block_hash: hash_at(store, through)?,
        parent_manifest_digest: seals.last().map_or(String::new(), |s| s.digest.clone()),
        sealed: false,
        previous: None,
    };
    let recent = publish_shard(
        root,
        layout,
        &spec,
        &read_range(store, recent_start, through)?,
    )?;
    let window = entries
        .len()
        .saturating_sub(layout.max_archive_shards as usize);
    for dropped in &entries[..window] {
        timeline.record(
            "drop",
            serde_json::json!({"shard_id": dropped.shard_id, "digest": dropped.manifest_digest,
                "start": dropped.start_height, "end": dropped.end_height, "cycle": 0}),
        );
    }
    let map = layout.map(&entries[window..], recent.entry.clone());
    map.check_shape()?;
    let (candidate, map_sha256) = write_candidate(root, &map)?;
    let active = ActiveRecord {
        directory: candidate.clone(),
        map_sha256: map_sha256.clone(),
        tip: through,
        tip_hash: spec.terminal_block_hash.clone(),
        cycle: 0,
        seals: seals.clone(),
    };
    active.store(root)?;
    timeline.record(
        "bootstrap",
        serde_json::json!({
            "start": start, "through": through, "seals": seals.len(), "dropped": window,
            "count_s": count_s, "seconds": started.elapsed().as_secs_f64(),
            "recent": recent.summary(), "map_sha256": map_sha256,
            "archive_chain_sha256": active.archive_chain_sha256(),
        }),
    );
    Ok(Bootstrapped {
        seals,
        map,
        candidate,
        map_sha256,
        recent,
    })
}

/// Flattens cache handles and publishes; for builds on a blocking thread.
pub fn publish_parts(
    root: &Path,
    layout: &DisplayRoot,
    spec: &ShardSpec,
    parts: &[std::sync::Arc<[TransparentDisplayRecord]>],
) -> Result<PublishedShard, PublishError> {
    publish_shard(root, layout, spec, &flatten(parts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventStore;
    use crate::txid_display::fixture::{self, record};

    #[test]
    fn bootstrap_publishes_verifiable_chained_shards() {
        let temp = tempfile::tempdir().unwrap();
        let journal = temp.path().join("journal");
        fixture::write_journal(&journal, 100, 125, 0);
        let store = EventStore::open_existing(&journal).unwrap();
        let layout = fixture::layout(&store, 101, 100);
        let root = temp.path().join("root");
        let done = bootstrap(&store, &root, &layout, 125).unwrap();
        assert!(done.seals.len() >= 4, "{} seals", done.seals.len());
        done.map.check_shape().unwrap();
        assert_eq!(done.map.first_shard_id, 0);
        assert_eq!(done.map.start_height, 101);
        let recent = done.map.shards.last().unwrap();
        assert!(!recent.sealed && recent.end_height == 125);
        // Every revision verifies from disk, and the parent digests chain.
        let mut parent = String::new();
        for entry in &done.map.shards {
            let tier = if entry.sealed { SEALED_DIR } else { RECENT_DIR };
            let manifest = verify_dir(
                &root.join(tier).join(&entry.manifest_digest),
                &entry.manifest_digest,
            )
            .unwrap();
            assert!(entry.describes(&manifest));
            assert_eq!(manifest.parent_manifest_digest, parent);
            if entry.sealed {
                parent = entry.manifest_digest.clone();
            }
            // The candidate holds hard links, not copies.
            let linked = done
                .candidate
                .join(&entry.manifest_digest)
                .join(MANIFEST_FILE);
            let source = root
                .join(tier)
                .join(&entry.manifest_digest)
                .join(MANIFEST_FILE);
            assert_eq!(
                std::fs::metadata(linked).unwrap().ino(),
                std::fs::metadata(source).unwrap().ino()
            );
        }
        let active = ActiveRecord::load(&root).unwrap();
        assert_eq!(active.map_sha256, done.map_sha256);
        assert_eq!(
            read_map(&done.candidate, &done.map_sha256).unwrap(),
            done.map
        );
        // The tooling index covers every record from the display start.
        let index = HeightIndex::open(&root).unwrap();
        let total: u64 = (101..=125)
            .map(|h| fixture::chain_records(0, h).len() as u64)
            .sum();
        assert_eq!(index.records(), total);
        // A second bootstrap into the same root is refused.
        assert!(bootstrap(&store, &root, &layout, 125).is_err());
    }

    #[test]
    fn verification_refuses_tampering_and_strays() {
        let temp = tempfile::tempdir().unwrap();
        let layout = DisplayRoot {
            geometry: "txid-2k".into(),
            seal: fixture::params(),
            max_archive_shards: 4,
            network: transparent_filter::NETWORK.into(),
            genesis_hash: fixture::GENESIS.into(),
            start_height: 10,
            base_parent: "11".repeat(32),
        };
        let mut records: Vec<_> = (0..50).map(|i| record(i, 20)).collect();
        records.push(record(99, 9_000));
        let spec = ShardSpec {
            shard_id: 3,
            start: 10,
            end: 19,
            parent_block_hash: "11".repeat(32),
            terminal_block_hash: "22".repeat(32),
            parent_manifest_digest: String::new(),
            sealed: true,
            previous: None,
        };
        let shard = publish_shard(temp.path(), &layout, &spec, &records).unwrap();
        verify_dir(&shard.directory, &shard.digest).unwrap();
        // Republishing identical content verifies rather than rewrites.
        let inode = std::fs::metadata(shard.directory.join(MANIFEST_FILE))
            .unwrap()
            .ino();
        let again = publish_shard(temp.path(), &layout, &spec, &records).unwrap();
        assert_eq!(again.digest, shard.digest);
        assert_eq!(
            std::fs::metadata(shard.directory.join(MANIFEST_FILE))
                .unwrap()
                .ino(),
            inode
        );
        // A stray file is refused.
        let stray = shard.directory.join("extra.bin");
        std::fs::write(&stray, b"x").unwrap();
        assert!(verify_dir(&shard.directory, &shard.digest).is_err());
        std::fs::remove_file(&stray).unwrap();
        // So is a changed page row, and a rebuild over it is an immutable error.
        let pages = shard.directory.join(DisplayTable::Pages.file_name(0));
        let mut bytes = std::fs::read(&pages).unwrap();
        bytes[ROW_BYTES + 100] ^= 1;
        std::fs::remove_file(&pages).unwrap();
        std::fs::write(&pages, &bytes).unwrap();
        assert!(verify_dir(&shard.directory, &shard.digest).is_err());
        assert!(matches!(
            publish_shard(temp.path(), &layout, &spec, &records),
            Err(PublishError::Immutable(_))
        ));
        // A sealed shard cannot be given a previous revision.
        let mut chained = spec.clone();
        chained.previous = Some(shard.revision());
        assert!(publish_shard(temp.path(), &layout, &chained, &records).is_err());
    }

    #[test]
    fn recent_revisions_reproduce_or_supersede() {
        let temp = tempfile::tempdir().unwrap();
        let store_dir = temp.path().join("journal");
        fixture::write_journal(&store_dir, 1, 3, 0);
        let store = EventStore::open_existing(&store_dir).unwrap();
        let layout = fixture::layout(&store, 2, 4);
        let records: Vec<_> = (0..10).map(|i| record(i, 20)).collect();
        let mut spec = ShardSpec {
            shard_id: 0,
            start: 2,
            end: 3,
            parent_block_hash: layout.base_parent.clone(),
            terminal_block_hash: "33".repeat(32),
            parent_manifest_digest: String::new(),
            sealed: false,
            previous: None,
        };
        let first = publish_shard(temp.path(), &layout, &spec, &records).unwrap();
        assert_eq!(first.manifest.revision, 0);
        spec.previous = Some(first.revision());
        let same = publish_shard(temp.path(), &layout, &spec, &records).unwrap();
        assert_eq!(same.digest, first.digest);
        spec.end = 4;
        spec.terminal_block_hash = "44".repeat(32);
        let grown = publish_shard(temp.path(), &layout, &spec, &records).unwrap();
        assert_eq!(grown.manifest.revision, 1);
        assert_eq!(grown.manifest.supersedes, first.digest);
        spec.previous = Some(grown.revision());
        let again = publish_shard(temp.path(), &layout, &spec, &records[..5]).unwrap();
        assert_eq!(again.manifest.revision, 2);
        assert_eq!(again.manifest.supersedes, grown.digest);
    }

    #[test]
    fn a_root_that_cannot_hard_link_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("root");
        std::fs::create_dir(&root).unwrap();
        probe_hard_links(&root).unwrap();
        // Another filesystem under `sealed/` makes every link cross a device.
        let Some(other) = ["/dev/shm", "/run", "/var/tmp"].into_iter().find(|p| {
            std::fs::metadata(p).is_ok_and(|m| {
                m.dev() != std::fs::metadata(temp.path()).unwrap().dev()
                    && tempfile::tempdir_in(p).is_ok()
            })
        }) else {
            eprintln!("no second filesystem is writable here; cross-device refusal not exercised");
            return;
        };
        let elsewhere = tempfile::tempdir_in(other).unwrap();
        std::fs::remove_dir(root.join(SEALED_DIR)).unwrap();
        std::os::unix::fs::symlink(elsewhere.path(), root.join(SEALED_DIR)).unwrap();
        let error = probe_hard_links(&root).unwrap_err().to_string();
        assert!(error.contains("hard link"), "{error}");
        // Bootstrap refuses the same root before writing any revision.
        let journal = temp.path().join("journal");
        fixture::write_journal(&journal, 100, 120, 0);
        let store = EventStore::open_existing(&journal).unwrap();
        let layout = fixture::layout(&store, 101, 4);
        assert!(bootstrap(&store, &root, &layout, 120).is_err());
        assert!(!root.join(ACTIVE_FILE).exists());
        assert_eq!(std::fs::read_dir(elsewhere.path()).unwrap().count(), 0);
    }

    #[test]
    fn collection_keeps_held_and_newest_candidates_and_never_sealed() {
        let temp = tempfile::tempdir().unwrap();
        let journal = temp.path().join("journal");
        fixture::write_journal(&journal, 100, 115, 0);
        let store = EventStore::open_existing(&journal).unwrap();
        let layout = fixture::layout(&store, 101, 100);
        let root = temp.path().join("root");
        let done = bootstrap(&store, &root, &layout, 115).unwrap();
        let mut candidates = vec![done.candidate.clone()];
        let mut recents = vec![done.recent.digest.clone()];
        let mut previous = done.recent.revision();
        let last = done.seals.last().unwrap();
        for end in 116..=120u64 {
            let spec = ShardSpec {
                shard_id: done.seals.len() as u64,
                start: last.end_height + 1,
                end,
                parent_block_hash: last.terminal_block_hash.clone(),
                terminal_block_hash: format!("{end:064x}"),
                parent_manifest_digest: last.digest.clone(),
                sealed: false,
                previous: Some(previous.clone()),
            };
            let records: Vec<_> = (0..end as u32).map(|i| record(i, 20)).collect();
            let shard = publish_shard(&root, &layout, &spec, &records).unwrap();
            previous = shard.revision();
            recents.push(shard.digest.clone());
            let map = layout.map(&done.map.shards[..done.map.shards.len() - 1], shard.entry);
            candidates.push(write_candidate(&root, &map).unwrap().0);
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let keep = BTreeSet::from([candidates[0].clone()]);
        let removed = collect(&root, &keep, 3).unwrap();
        assert!(candidates[0].exists(), "a held candidate survives");
        for (i, candidate) in candidates.iter().enumerate().skip(1) {
            assert_eq!(candidate.exists(), i >= candidates.len() - 3, "{i}");
        }
        for (i, digest) in recents.iter().enumerate() {
            let kept = i == 0 || i >= recents.len() - 3;
            assert_eq!(root.join(RECENT_DIR).join(digest).exists(), kept, "{i}");
        }
        assert!(!removed.is_empty());
        assert_eq!(
            std::fs::read_dir(root.join(SEALED_DIR)).unwrap().count(),
            done.seals.len()
        );
    }

    #[test]
    fn candidates_link_the_split_map_and_collection_drops_unlinked_chunks() {
        let temp = tempfile::tempdir().unwrap();
        let journal = temp.path().join("journal");
        fixture::write_journal(&journal, 100, 125, 0);
        let store = EventStore::open_existing(&journal).unwrap();
        let layout = fixture::layout(&store, 101, 100);
        let root = temp.path().join("root");
        let done = bootstrap(&store, &root, &layout, 125).unwrap();
        assert!(done.seals.len() >= 2);
        let split = done.map.split().unwrap();
        let stored = |sha: &str| root.join(INDEX_DIR).join(format!("{sha}.json"));
        assert_eq!(
            std::fs::read(done.candidate.join(RECENT_MAP_FILE)).unwrap(),
            split.recent_bytes
        );
        for chunk in &split.chunks {
            let linked = done.candidate.join(index_file(&chunk.sha256));
            assert_eq!(std::fs::read(&linked).unwrap(), chunk.bytes);
            assert_eq!(
                std::fs::metadata(&linked).unwrap().ino(),
                std::fs::metadata(stored(&chunk.sha256)).unwrap().ino()
            );
        }

        // The window drops the oldest archive: the oldest chunk is rewritten
        // under a new digest, and the one only the old candidate linked goes.
        let mut dropped = done.map.clone();
        dropped.shards.remove(0);
        dropped.first_shard_id += 1;
        dropped.start_height = dropped.shards[0].start_height;
        let (next, _) = write_candidate(&root, &dropped).unwrap();
        let moved = dropped.split().unwrap();
        assert_ne!(moved.chunks[0].sha256, split.chunks[0].sha256);
        let removed = collect(&root, &BTreeSet::from([next.clone()]), 0).unwrap();
        assert!(removed.contains(&stored(&split.chunks[0].sha256)));
        assert!(!stored(&split.chunks[0].sha256).exists());
        assert!(stored(&moved.chunks[0].sha256).exists());
        assert!(next.join(index_file(&moved.chunks[0].sha256)).exists());
    }
}
