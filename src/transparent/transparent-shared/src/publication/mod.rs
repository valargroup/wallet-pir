//! History-only prepared-publication distribution. Verified bytes alone never
//! grant serving authority. This inventory is not a wallet format.
use zakura_pir_protocol::transparent::filter as transparent_filter;
use zakura_pir_protocol::transparent::shard as transparent_shard;

use crate::{
    history::shardset::ShardSet,
    runtime::{disk::ShippedRuntimes, SharedParams},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Read},
    path::{Path, PathBuf},
};

pub const SCHEMA: &str = "transparent-prepared-distribution-v1";
pub const PREFIX: &str = "/provider/v1/";
pub const MAX_METADATA_BYTES: u64 = 16 << 20;
// Bound independently by every native loader identity; change with its layout.
pub const RUNTIME: &str = crate::runtime::disk::PREPARED_RUNTIME_FORMAT;
pub const PROFILE: &str = "reinspiring-two-mask-p8-g27x1-m22-dq28-r10-v1";
pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Current {
    pub schema: String,
    /// Durable monotonic publisher pointer revision, including controller opens.
    pub revision: u64,
    pub recovery_epoch: u64,
    pub inventory_sha256: String,
    pub network: String,
    pub genesis_hash: String,
    pub anchor_height: u64,
    pub anchor_hash: String,
    pub eligibility: Eligibility,
    /// A journal-only invalidation above the anchor does not revoke this
    /// canonical publication, but still fences in-progress bootstrap attempts.
    pub invalidated_from: Option<u64>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Eligibility {
    Canonical,
    Withdrawn,
    Noncanonical,
}

impl Current {
    pub fn eligible(&self) -> bool {
        self.eligibility == Eligibility::Canonical
            && self
                .invalidated_from
                .is_none_or(|from| from > self.anchor_height)
    }

    /// Equal-revision state may only become less eligible. The ingestion loop
    /// can withdraw immediately before discovering the exact fork/advancing
    /// its epoch. A fresh good poll must never undo that observed withdrawal.
    pub fn succeeds(&self, previous: &Self) -> bool {
        if self.revision != previous.revision {
            return self.revision > previous.revision;
        }
        let mut same = previous.clone();
        same.eligibility = self.eligibility;
        self == &same && (!self.eligible() || previous.eligible())
    }
}

/// A live publisher attestation from its existing canonicality source. This is
/// neither indexed-data completeness proof nor permission to serve a revision.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnchorCheck {
    pub schema: String,
    pub revision: u64,
    pub recovery_epoch: u64,
    pub genesis_hash: String,
    pub height: u64,
    pub hash: String,
    pub canonical: bool,
}

/// Most exact anchors one batched canonicality check names.
pub const MAX_ANCHOR_BATCH: usize = 256;
/// Largest batched canonicality request or response body.
pub const MAX_ANCHOR_BATCH_BYTES: usize = 64 << 10;
/// Path below [`PREFIX`] for a POSTed [`AnchorBatchRequest`].
pub const ANCHOR_BATCH_PATH: &str = "anchors/check";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct AnchorPair {
    pub height: u64,
    pub hash: String,
}

/// Exact anchors checked against one source snapshot. The publisher refuses a
/// request for any other snapshot instead of answering for its own.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AnchorBatchRequest {
    pub schema: String,
    pub revision: u64,
    pub recovery_epoch: u64,
    pub inventory_sha256: String,
    pub anchors: Vec<AnchorPair>,
}
impl AnchorBatchRequest {
    /// The request names `current`'s snapshot and at most
    /// [`MAX_ANCHOR_BATCH`] distinct well-formed anchors.
    pub fn new(current: &Current, anchors: Vec<AnchorPair>) -> Result<Self, Error> {
        let request = Self {
            schema: current.schema.clone(),
            revision: current.revision,
            recovery_epoch: current.recovery_epoch,
            inventory_sha256: current.inventory_sha256.clone(),
            anchors,
        };
        request.validate()?;
        Ok(request)
    }
    pub fn validate(&self) -> Result<(), Error> {
        let distinct: BTreeSet<_> = self.anchors.iter().collect();
        if self.schema != SCHEMA
            || !is_digest(&self.inventory_sha256)
            || self.anchors.is_empty()
            || self.anchors.len() > MAX_ANCHOR_BATCH
            || distinct.len() != self.anchors.len()
            || self.anchors.iter().any(|a| !is_digest(&a.hash))
        {
            return Err("invalid anchor batch request".into());
        }
        Ok(())
    }
    /// Whether this request names the snapshot `current` describes.
    pub fn names(&self, current: &Current) -> bool {
        self.schema == current.schema
            && self.revision == current.revision
            && self.recovery_epoch == current.recovery_epoch
            && self.inventory_sha256 == current.inventory_sha256
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AnchorResult {
    pub height: u64,
    pub hash: String,
    pub canonical: bool,
}

/// A live publisher attestation for every anchor of one request, from the same
/// canonicality source and snapshot fences as [`AnchorCheck`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AnchorBatch {
    pub schema: String,
    pub revision: u64,
    pub recovery_epoch: u64,
    pub inventory_sha256: String,
    pub genesis_hash: String,
    pub results: Vec<AnchorResult>,
}
impl AnchorBatch {
    /// Exactly one answer for every requested anchor, for the requested
    /// snapshot and genesis. Anything else proves nothing about any anchor.
    pub fn answers(
        &self,
        request: &AnchorBatchRequest,
        genesis_hash: &str,
    ) -> Result<BTreeMap<AnchorPair, bool>, Error> {
        if self.schema != request.schema
            || self.revision != request.revision
            || self.recovery_epoch != request.recovery_epoch
            || self.inventory_sha256 != request.inventory_sha256
            || self.genesis_hash != genesis_hash
        {
            return Err("canonicality attestation identity/snapshot superseded".into());
        }
        let mut answers = BTreeMap::new();
        for result in &self.results {
            let pair = AnchorPair {
                height: result.height,
                hash: result.hash.clone(),
            };
            if answers.insert(pair, result.canonical).is_some() {
                return Err("duplicate canonicality answer".into());
            }
        }
        if answers.len() != request.anchors.len()
            || request.anchors.iter().any(|a| !answers.contains_key(a))
        {
            return Err("canonicality answers do not match the request".into());
        }
        Ok(answers)
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Metadata,
    RecentTable,
    RecentRuntime,
    ArchiveTable,
    ArchiveRuntime,
}
impl Kind {
    pub fn local(self) -> bool {
        !matches!(self, Self::ArchiveTable | Self::ArchiveRuntime)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
    pub kind: Kind,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Inventory {
    pub schema: String,
    pub network: String,
    pub genesis_hash: String,
    pub map_sha256: String,
    pub shard_schema: String,
    pub runtime: String,
    pub profile: String,
    /// Exact shard identities/ranges stay in the existing digest-bound map.
    /// Archive metadata here does not qualify an archive destination.
    pub recent_from_shard: u64,
    pub artifacts: Vec<Artifact>,
}
pub fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
pub fn is_digest(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
/// No escaping, queries, fragments, absolute paths or dot components.
pub fn safe_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 256
        && path.split('/').all(|c| {
            !c.is_empty()
                && c != "."
                && c != ".."
                // Publisher artifacts cannot overwrite private local receipts.
                && !c.ends_with(".received.json")
                && !c.contains(".checked-")
                && c.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        })
}
impl Inventory {
    /// Classifications must agree with the existing map before any table or
    /// runtime is fetched. A publisher cannot label archive tables as metadata.
    pub fn validate_map(&self, map: &transparent_filter::ShardMap) -> Result<(), Error> {
        map.validate_supported()?;
        if digest(&serde_json::to_vec(map)?) != self.map_sha256
            || map.network != self.network
            || map.genesis_hash != self.genesis_hash
            || self.recent_from_shard >= map.shards.len() as u64
        {
            return Err("inventory holdings disagree with shard map".into());
        }
        let mut expected_metadata = BTreeSet::from(["shards.json".to_string()]);
        for shard in &map.shards {
            expected_metadata.insert(format!("{}/manifest.json", shard.manifest_digest));
            expected_metadata.insert(format!("{}/filter.bin", shard.manifest_digest));
        }
        for file in &self.artifacts {
            match file.kind {
                Kind::Metadata => {
                    if !expected_metadata.remove(&file.path) {
                        return Err("invalid metadata classification".into());
                    }
                }
                Kind::RecentRuntime | Kind::ArchiveRuntime => {
                    let Some((a, b)) = file
                        .path
                        .strip_suffix(".runtime")
                        .and_then(|s| s.split_once('-'))
                    else {
                        return Err("invalid native runtime filename".into());
                    };
                    if !is_digest(a) || !is_digest(b) {
                        return Err("invalid native runtime identity".into());
                    }
                }
                Kind::RecentTable | Kind::ArchiveTable => {
                    let Some((revision, name)) = file.path.split_once('/') else {
                        return Err("invalid table path".into());
                    };
                    if name.contains('/') || !name.ends_with(".bin") || name == "filter.bin" {
                        return Err("invalid table classification".into());
                    }
                    let entry = map
                        .shards
                        .iter()
                        .find(|s| s.manifest_digest == revision)
                        .ok_or("table names unknown revision")?;
                    if (entry.shard_id >= self.recent_from_shard)
                        != (file.kind == Kind::RecentTable)
                    {
                        return Err("table classification disagrees with holdings".into());
                    }
                }
            }
        }
        if !expected_metadata.is_empty() {
            return Err("inventory omits required metadata".into());
        }
        Ok(())
    }
    pub fn validate(&self, network: &str, genesis: &str) -> Result<(), Error> {
        if self.schema != SCHEMA
            || self.shard_schema != transparent_shard::manifest::SCHEMA
            || self.runtime != RUNTIME
            || self.profile != PROFILE
        {
            return Err("incompatible prepared publication schema/runtime/profile".into());
        }
        if self.network != network
            || self.genesis_hash != genesis
            || !is_digest(genesis)
            || !is_digest(&self.map_sha256)
        {
            return Err("prepared publication chain identity mismatch".into());
        }
        if self.artifacts.len() > 100_000 {
            return Err("artifact inventory exceeds limit".into());
        }
        let mut paths = BTreeSet::new();
        for file in &self.artifacts {
            if !safe_path(&file.path)
                || !is_digest(&file.sha256)
                || !paths.insert(&file.path)
                || file.bytes == 0
                || (file.kind == Kind::Metadata && file.bytes > MAX_METADATA_BYTES)
            {
                return Err("unsafe, duplicate or incomplete artifact inventory".into());
            }
        }
        if !self
            .artifacts
            .iter()
            .any(|f| f.path == "shards.json" && f.kind == Kind::Metadata)
        {
            return Err("inventory omits shard map artifact".into());
        }
        Ok(())
    }
}
/// Finalized immutable inventory and exact sources. HTTP never resolves a
/// caller-supplied filename against the filesystem.
#[derive(Clone)]
pub struct Finalized {
    inventory: Inventory,
    bytes: Vec<u8>,
    sha256: String,
    files: BTreeMap<String, PathBuf>,
}
impl Finalized {
    pub fn inventory(&self) -> &Inventory {
        &self.inventory
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn file(&self, digest: &str) -> Option<&Path> {
        self.files.get(digest).map(PathBuf::as_path)
    }
    pub fn prepare(root: &Path, set: &ShardSet, recent_from_height: u64) -> Result<Self, Error> {
        Self::prepare_holdings(
            root,
            set,
            recent_from_height,
            false,
            &mut CheckedRuntimes::default(),
        )
    }
    /// Finalize every table and runtime for autonomous full-coverage consumers.
    pub fn prepare_full(
        root: &Path,
        set: &ShardSet,
        recent_from_height: u64,
    ) -> Result<Self, Error> {
        Self::prepare_holdings(
            root,
            set,
            recent_from_height,
            true,
            &mut CheckedRuntimes::default(),
        )
    }
    /// [`Self::prepare`] or [`Self::prepare_full`], skipping the load and
    /// self-check of runtimes whose exact bytes `checked` already proved.
    pub fn prepare_reusing(
        root: &Path,
        set: &ShardSet,
        recent_from_height: u64,
        full: bool,
        checked: &mut CheckedRuntimes,
    ) -> Result<Self, Error> {
        Self::prepare_holdings(root, set, recent_from_height, full, checked)
    }
    fn prepare_holdings(
        root: &Path,
        set: &ShardSet,
        recent_from_height: u64,
        full: bool,
        checked: &mut CheckedRuntimes,
    ) -> Result<Self, Error> {
        let previous = std::mem::take(&mut checked.passed);
        checked.loaded = 0;
        checked.reused = 0;
        let recent = set
            .map
            .shards
            .iter()
            .find(|s| s.start_height == recent_from_height)
            .ok_or("recent cutoff is not an existing shard boundary")?
            .shard_id;
        let mut inventory = Inventory {
            schema: SCHEMA.into(),
            network: set.map.network.clone(),
            genesis_hash: set.map.genesis_hash.clone(),
            map_sha256: set.map_digest.clone(),
            shard_schema: transparent_shard::manifest::SCHEMA.into(),
            runtime: RUNTIME.into(),
            profile: PROFILE.into(),
            recent_from_shard: recent,
            artifacts: Vec::new(),
        };
        let mut files = BTreeMap::new();
        let mut paths = BTreeMap::new();
        // Returns the digest and, the first time a path is added, the open
        // handle it was computed from.
        let mut add =
            |path: PathBuf, kind: Kind| -> Result<(String, Option<std::fs::File>), Error> {
                let relative = path
                    .strip_prefix(root)?
                    .to_str()
                    .ok_or("non-UTF8 artifact")?
                    .to_string();
                if !safe_path(&relative) {
                    return Err("unsafe publisher artifact path".into());
                }
                let mut checked = root.to_path_buf();
                for component in relative.split('/') {
                    checked.push(component);
                    if std::fs::symlink_metadata(&checked)?
                        .file_type()
                        .is_symlink()
                    {
                        return Err("symlink in publisher artifact".into());
                    }
                }
                if let Some(sha256) = paths.get(&relative) {
                    return Ok((String::clone(sha256), None));
                }
                let mut file = std::fs::File::open(&path)?;
                if !file.metadata()?.is_file() {
                    return Err("artifact is not a regular file".into());
                }
                let bytes = file.metadata()?.len();
                let mut hash = Sha256::new();
                io::copy(&mut file, &mut HashWriter(&mut hash))?;
                let sha256 = hex::encode(hash.finalize());
                if full || kind.local() {
                    files.insert(sha256.clone(), path);
                }
                paths.insert(relative.clone(), sha256.clone());
                inventory.artifacts.push(Artifact {
                    path: relative,
                    sha256: sha256.clone(),
                    bytes,
                    kind,
                });
                Ok((sha256, Some(file)))
            };
        add(root.join("shards.json"), Kind::Metadata)?;
        let shipped = ShippedRuntimes::required(root.to_path_buf());
        for entry in &set.map.shards {
            let dir = root.join(&entry.manifest_digest);
            add(dir.join("manifest.json"), Kind::Metadata)?;
            add(dir.join("filter.bin"), Kind::Metadata)?;
            let shard = set
                .revision(&entry.manifest_digest)
                .ok_or("producer needs verified complete set")?;
            let local = entry.shard_id >= recent;
            for table in shard.tables() {
                let params = if full || local {
                    Some(SharedParams::build(shard.geometry, table.kind())?)
                } else {
                    None
                };
                for segment in 0..shard.segments(table) {
                    let source = shard
                        .segment(table, segment)
                        .ok_or("missing source segment")?;
                    let (source_sha256, _) = add(
                        source.path().to_path_buf(),
                        if local {
                            Kind::RecentTable
                        } else {
                            Kind::ArchiveTable
                        },
                    )?;
                    // The inventory publishes the bytes just hashed; they must
                    // be the committed segment whether or not a runtime is loaded.
                    if source_sha256 != source.sha256() {
                        return Err("source segment differs from its manifest".into());
                    }
                    if let Some(shared) = &params {
                        let key = (
                            shard.runtime_identity(table).to_string(),
                            table.kind(),
                            segment,
                        );
                        let path = shipped.path(&key, shared, source.sha256());
                        let kind = if local {
                            Kind::RecentRuntime
                        } else {
                            Kind::ArchiveRuntime
                        };
                        // The file name binds the runtime key, parameters and
                        // source digest; the digest computed by `add` binds
                        // every byte. A pair this process already loaded and
                        // self-checked therefore names the same proven input.
                        let name = path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .ok_or("non-UTF8 runtime name")?
                            .to_string();
                        let (sha256, file) = add(path, kind)?;
                        if previous.get(&name) == Some(&sha256) {
                            checked.reused += 1;
                        } else {
                            // Load from the handle just hashed, so the bytes
                            // checked are the inode the inventory publishes
                            // and caches, not a later replacement of the path.
                            let file = file.ok_or("runtime listed twice")?;
                            // Header/length checks alone cannot finalize malformed native bytes.
                            ShippedRuntimes::load_file(file, &key, shared, source.sha256())?
                                .self_check(shared, &source.load()?)?;
                            checked.loaded += 1;
                        }
                        checked.passed.insert(name, sha256);
                    }
                }
            }
        }
        inventory.artifacts.sort_by(|a, b| a.path.cmp(&b.path));
        inventory.validate(&inventory.network, &inventory.genesis_hash)?;
        inventory.validate_map(&set.map)?;
        let bytes = serde_json::to_vec(&inventory)?;
        let sha256 = digest(&bytes);
        Ok(Self {
            inventory,
            bytes,
            sha256,
            files,
        })
    }
}
/// Runtimes one process loaded and self-checked while finalizing, by file
/// name and whole-file SHA-256. Each preparation keeps only the runtimes of
/// its own publication, so this never grows past one inventory. Neither
/// value is trusted from disk: the name is derived and the digest recomputed.
#[derive(Debug, Default)]
pub struct CheckedRuntimes {
    passed: BTreeMap<String, String>,
    /// Runtimes the last preparation loaded and self-checked.
    pub loaded: usize,
    /// Runtimes the last preparation accepted from an earlier check.
    pub reused: usize,
}
struct HashWriter<'a>(&'a mut Sha256);
impl io::Write for HashWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub fn verify_file(path: &Path, expected: &Artifact) -> Result<(), Error> {
    let file = crate::verification::open(path)?;
    if crate::verification::matches_received(path, &file, &expected.sha256, expected.bytes) {
        return Ok(());
    }
    verify_open(&file, expected)
}

/// The caller has verified the network stream, or linked/copied its completed
/// cache object. Persist its download identity after checking the local size.
pub fn record_received_file(path: &Path, expected: &Artifact, object: &Path) -> Result<(), Error> {
    crate::verification::received(path, &expected.sha256, expected.bytes, object)?;
    Ok(())
}

fn verify_open(file: &std::fs::File, expected: &Artifact) -> Result<(), Error> {
    let before = file.metadata()?;
    if before.len() != expected.bytes {
        return Err("artifact length mismatch".into());
    }
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 1 << 20];
    let mut input = file;
    std::io::Seek::rewind(&mut input)?;
    loop {
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    if hex::encode(hash.finalize()) != expected.sha256 {
        return Err("artifact digest mismatch".into());
    }
    tracing::debug!(bytes = expected.bytes, "artifact fully hashed");
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn received_files_without_durable_evidence_are_checked_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("object");
        let bytes = b"correct bytes";
        std::fs::write(&path, bytes).unwrap();
        let expected = Artifact {
            path: "object".into(),
            sha256: digest(bytes),
            bytes: bytes.len() as u64,
            kind: Kind::Metadata,
        };
        verify_file(&path, &expected).unwrap();
        std::fs::write(&path, b"corrupt bytes").unwrap();
        assert!(verify_file(&path, &expected).is_err());
        std::fs::write(&path, bytes).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        verify_open(&file, &expected).unwrap();
        // A caller's consumed descriptor must not make a retry hash EOF.
        verify_open(&file, &expected).unwrap();
    }

    #[test]
    fn downloaded_artifacts_use_size_checks_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("object");
        let bytes = b"network bytes";
        std::fs::write(&path, bytes).unwrap();
        let expected = Artifact {
            path: "object".into(),
            sha256: digest(bytes),
            bytes: bytes.len() as u64,
            kind: Kind::Metadata,
        };
        record_received_file(&path, &expected, &path).unwrap();
        verify_file(&path, &expected).unwrap();
        std::fs::write(&path, b"same size!!!!").unwrap();
        verify_file(&path, &expected).unwrap();
        std::fs::write(&path, b"short").unwrap();
        assert!(verify_file(&path, &expected).is_err());
    }

    #[test]
    fn rejects_unsafe_paths_and_noncanonical_digests() {
        for p in [
            "../x",
            "data.bin.received.json",
            "data.bin.checked-receipt",
            "/x",
            "a//b",
            "a/./b",
            "a/../b",
            "a%2fb",
            "https://host/x",
            "x?y",
            "x#y",
            "C:x",
            r"a\b",
            "",
        ] {
            assert!(!safe_path(p), "{p}");
        }
        assert!(safe_path("abc-123/directory.0.bin"));
        assert!(is_digest(&"ab".repeat(32)));
        assert!(!is_digest(&"AB".repeat(32)));
    }

    #[test]
    fn anchor_batches_bind_one_snapshot_and_answer_every_anchor_once() {
        let current = Current {
            schema: SCHEMA.into(),
            revision: 7,
            recovery_epoch: 2,
            inventory_sha256: "11".repeat(32),
            network: "main".into(),
            genesis_hash: "00".repeat(32),
            anchor_height: 9,
            anchor_hash: "22".repeat(32),
            eligibility: Eligibility::Canonical,
            invalidated_from: None,
        };
        let pair = |height, byte: &str| AnchorPair {
            height,
            hash: byte.repeat(32),
        };
        assert!(AnchorBatchRequest::new(&current, vec![]).is_err());
        assert!(AnchorBatchRequest::new(&current, vec![pair(1, "aa"), pair(1, "aa")]).is_err());
        assert!(AnchorBatchRequest::new(&current, vec![pair(1, "AA")]).is_err());
        let many = (0..=MAX_ANCHOR_BATCH as u64)
            .map(|h| pair(h, "aa"))
            .collect();
        assert!(AnchorBatchRequest::new(&current, many).is_err());
        let full: Vec<_> = (0..MAX_ANCHOR_BATCH as u64)
            .map(|h| pair(h, "aa"))
            .collect();
        let largest = AnchorBatchRequest::new(&current, full).unwrap();
        assert!(serde_json::to_vec(&largest).unwrap().len() < MAX_ANCHOR_BATCH_BYTES);
        let response = AnchorBatch {
            schema: SCHEMA.into(),
            revision: 7,
            recovery_epoch: 2,
            inventory_sha256: "11".repeat(32),
            genesis_hash: "00".repeat(32),
            results: (0..MAX_ANCHOR_BATCH as u64)
                .map(|height| AnchorResult {
                    height,
                    hash: "aa".repeat(32),
                    canonical: true,
                })
                .collect(),
        };
        assert!(serde_json::to_vec(&response).unwrap().len() < MAX_ANCHOR_BATCH_BYTES);

        let request =
            AnchorBatchRequest::new(&current, vec![pair(1, "aa"), pair(2, "bb")]).unwrap();
        assert!(request.names(&current));
        let mut other = current.clone();
        other.inventory_sha256 = "33".repeat(32);
        assert!(!request.names(&other));
        let result = |height, byte: &str, canonical| AnchorResult {
            height,
            hash: byte.repeat(32),
            canonical,
        };
        let batch = AnchorBatch {
            schema: SCHEMA.into(),
            revision: 7,
            recovery_epoch: 2,
            inventory_sha256: "11".repeat(32),
            genesis_hash: "00".repeat(32),
            results: vec![result(2, "bb", false), result(1, "aa", true)],
        };
        let answers = batch.answers(&request, &current.genesis_hash).unwrap();
        assert!(answers[&pair(1, "aa")]);
        assert!(!answers[&pair(2, "bb")]);
        assert!(batch.answers(&request, &"44".repeat(32)).is_err());
        for broken in [
            vec![result(1, "aa", true)],
            vec![result(1, "aa", true), result(1, "aa", true)],
            vec![
                result(1, "aa", true),
                result(2, "bb", false),
                result(1, "aa", true),
            ],
            vec![result(1, "aa", true), result(3, "bb", false)],
        ] {
            let mut wrong = batch.clone();
            wrong.results = broken;
            assert!(wrong.answers(&request, &current.genesis_hash).is_err());
        }
        for mutate in [
            |b: &mut AnchorBatch| b.revision += 1,
            |b: &mut AnchorBatch| b.recovery_epoch += 1,
            |b: &mut AnchorBatch| b.inventory_sha256 = "33".repeat(32),
            |b: &mut AnchorBatch| b.schema = "other".into(),
        ] {
            let mut wrong = batch.clone();
            mutate(&mut wrong);
            assert!(wrong.answers(&request, &current.genesis_hash).is_err());
        }
    }
}
