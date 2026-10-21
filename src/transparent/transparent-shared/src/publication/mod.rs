//! History-only prepared-publication distribution. Verified bytes alone never
//! grant serving authority. This inventory is not a wallet format.
use zakura_pir_protocol::transparent::filter as transparent_filter;
use zakura_pir_protocol::transparent::shard as transparent_shard;

use crate::{
    history::shardset::{FileStamp, ShardSet},
    runtime::{disk::ShippedRuntimes, SharedParams},
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
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

/// Expected publisher identity saved at receipt; this is not proof of a body hash.
#[derive(Clone, Debug)]
pub struct ReceivedArtifact {
    pub sha256: String,
    pub bytes: u64,
    pub object: PathBuf,
}

pub fn received_artifact(path: &Path) -> io::Result<Option<ReceivedArtifact>> {
    crate::verification::expected(path)
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
                && !c.contains(".validation.")
                && !c.starts_with(".validation-")
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
            &|| false,
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
            &|| false,
        )
    }
    /// Finalize an inventory, reusing only native runtimes whose exact bytes
    /// this process already loaded and self-checked. Every artifact is rehashed.
    pub fn prepare_reusing(
        root: &Path,
        set: &ShardSet,
        recent_from_height: u64,
        full: bool,
        checked: &mut CheckedRuntimes,
    ) -> Result<Self, Error> {
        Self::prepare_holdings(root, set, recent_from_height, full, checked, &|| false)
    }
    /// [`Self::prepare_reusing`], asking `stop` before each file it reads;
    /// once it answers true the preparation ends with [`Stopped`], nothing is
    /// finalized and `checked` keeps its earlier evidence.
    pub fn prepare_reusing_until(
        root: &Path,
        set: &ShardSet,
        recent_from_height: u64,
        full: bool,
        checked: &mut CheckedRuntimes,
        stop: &(dyn Fn() -> bool + Sync),
    ) -> Result<Self, Error> {
        Self::prepare_holdings(root, set, recent_from_height, full, checked, stop)
    }
    /// A table whose bytes `set` verified in this process, and whose file is
    /// unchanged since, is listed with that verified digest instead of being
    /// hashed again; any other file is hashed.
    fn prepare_holdings(
        root: &Path,
        set: &ShardSet,
        recent_from_height: u64,
        full: bool,
        checked: &mut CheckedRuntimes,
        stop: &(dyn Fn() -> bool + Sync),
    ) -> Result<Self, Error> {
        let previous = &checked.passed;
        checked.loaded = 0;
        checked.reused = 0;
        let mut passed = BTreeMap::new();
        let mut files = BTreeMap::new();
        let Plan {
            mut inventory,
            pending,
            sources,
            native,
        } = plan(root, set, recent_from_height, full)?;
        // Hash immutable artifacts on the existing bounded CPU pool. Native
        // handles stay open so the later self-check reads the inode just hashed.
        let mut runtimes = BTreeMap::new();
        for (artifact, path, file) in hash_listed(root, pending, stop)? {
            if let Some(expected) = sources.get(&path) {
                if artifact.sha256 != *expected {
                    return Err("source segment differs from its manifest".into());
                }
            }
            if let Some(file) = file {
                runtimes.insert(path.clone(), (artifact.sha256.clone(), file));
            }
            if full || artifact.kind.local() {
                files.insert(artifact.sha256.clone(), path);
            }
            inventory.artifacts.push(artifact);
        }
        // Keep native loads sequential to bound resident runtime memory.
        for (path, key, shared, source) in native {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or("non-UTF8 runtime name")?
                .to_owned();
            let (sha256, file) = runtimes.remove(&path).ok_or("unhashed runtime")?;
            // The derived name binds parameters and source; the whole-file
            // digest binds the exact bytes checked by a previous inventory.
            if previous.get(&name) == Some(&sha256) {
                checked.reused += 1;
            } else {
                if stop() {
                    return Err(Box::new(Stopped));
                }
                ShippedRuntimes::load_file(file, &key, &shared, source.sha256())?
                    .self_check(&shared, &source.load()?)?;
                checked.loaded += 1;
            }
            passed.insert(name, sha256);
        }
        inventory.artifacts.sort_by(|a, b| a.path.cmp(&b.path));
        inventory.validate(&inventory.network, &inventory.genesis_hash)?;
        inventory.validate_map(&set.map)?;
        let bytes = serde_json::to_vec(&inventory)?;
        let sha256 = digest(&bytes);
        checked.passed = passed;
        Ok(Self {
            inventory,
            bytes,
            sha256,
            files,
        })
    }
}
/// Every artifact a publication of `set` lists, in discovery order, with the
/// manifest digest of each source segment and the native runtimes to check.
/// Paths are checked to be safe, free of symbolic links and regular files;
/// nothing is hashed or loaded.
struct Plan<'a> {
    inventory: Inventory,
    pending: Vec<(PathBuf, Kind, Option<Known>)>,
    sources: BTreeMap<PathBuf, &'a str>,
    native: Vec<NativeCheck<'a>>,
}
type NativeCheck<'a> = (
    PathBuf,
    crate::runtime::RuntimeKey,
    std::sync::Arc<SharedParams>,
    &'a crate::history::shardset::SegmentSource,
);
fn plan<'a>(
    root: &Path,
    set: &'a ShardSet,
    recent_from_height: u64,
    full: bool,
) -> Result<Plan<'a>, Error> {
    let recent = set
        .map
        .shards
        .iter()
        .find(|s| s.start_height == recent_from_height)
        .ok_or("recent cutoff is not an existing shard boundary")?
        .shard_id;
    let inventory = Inventory {
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
    let mut paths = BTreeSet::new();
    let mut pending = Vec::new();
    let mut sources = BTreeMap::new();
    let mut native = Vec::new();
    // `known`: a digest and length this process verified for the file
    // with this stamp, used only while the opened file still has it.
    let mut add = |path: PathBuf, kind: Kind, known: Option<Known>| -> Result<bool, Error> {
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
        if !paths.insert(relative.clone()) {
            return Ok(false);
        }
        if !std::fs::metadata(&path)?.is_file() {
            return Err("artifact is not a regular file".into());
        }
        pending.push((path, kind, known));
        Ok(true)
    };
    add(root.join("shards.json"), Kind::Metadata, None)?;
    let shipped = ShippedRuntimes::required(root.to_path_buf());
    // Parameters depend only on geometry and table kind; build each once.
    let mut built = BTreeMap::new();
    for entry in &set.map.shards {
        let dir = root.join(&entry.manifest_digest);
        add(dir.join("manifest.json"), Kind::Metadata, None)?;
        add(dir.join("filter.bin"), Kind::Metadata, None)?;
        let shard = set
            .revision(&entry.manifest_digest)
            .ok_or("producer needs verified complete set")?;
        let local = entry.shard_id >= recent;
        for table in shard.tables() {
            let params = if full || local {
                let id = (shard.geometry.name, table.kind().as_str());
                Some(match built.entry(id) {
                    std::collections::btree_map::Entry::Occupied(entry) => {
                        std::sync::Arc::clone(entry.get())
                    }
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        let shared = SharedParams::build(shard.geometry, table.kind())?;
                        std::sync::Arc::clone(entry.insert(std::sync::Arc::new(shared)))
                    }
                })
            } else {
                None
            };
            for segment in 0..shard.segments(table) {
                let source = shard
                    .segment(table, segment)
                    .ok_or("missing source segment")?;
                sources.insert(source.path().to_path_buf(), source.sha256());
                add(
                    source.path().to_path_buf(),
                    if local {
                        Kind::RecentTable
                    } else {
                        Kind::ArchiveTable
                    },
                    Known::of(source),
                )?;
                if let Some(shared) = &params {
                    let key = (
                        shard.runtime_identity(table).to_string(),
                        table.kind(),
                        segment,
                    );
                    let path = shipped.path(&key, shared, source.sha256());
                    if add(
                        path.clone(),
                        if local {
                            Kind::RecentRuntime
                        } else {
                            Kind::ArchiveRuntime
                        },
                        None,
                    )? {
                        native.push((path, key, shared.clone(), source));
                    }
                }
            }
        }
    }
    Ok(Plan {
        inventory,
        pending,
        sources,
        native,
    })
}
/// The publisher's record of a completed History publication, written beside
/// it only after its required preparation passed. It lets a restarted
/// publisher offer the exact same inventory without rescanning its bodies;
/// the bodies are verified again in the background after serving starts.
/// Completion evidence never grants serving authority.
pub const RESTORE_SCHEMA: &str = "transparent-history-restore-v1";
/// The restore record's file name inside the publication directory.
pub const RESTORE_FILE: &str = "history-restore.json";
/// Preparation phases a restore record must name as completed.
const COMPLETED_PHASES: [&str; 2] = ["integrity", "correctness"];

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RestoreRecord {
    pub schema: String,
    /// The exact inventory bytes offered over HTTPS.
    pub inventory: String,
    pub inventory_sha256: String,
    pub map_sha256: String,
    pub network: String,
    pub genesis_hash: String,
    pub runtime: String,
    pub profile: String,
    /// Whether every runtime is listed and offered, not only recent holdings.
    pub full: bool,
    pub recent_from_height: u64,
    pub anchor_height: u64,
    pub anchor_hash: String,
    /// Preparation phases that passed before this record was written.
    pub completed: Vec<String>,
}

/// What the restoring publisher expects of a record: its own settings and the
/// saved publication's identity.
#[derive(Clone, Debug)]
pub struct RestoreExpectation {
    pub network: String,
    pub genesis_hash: String,
    pub map_sha256: String,
    pub full: bool,
    pub recent_from_height: u64,
    pub anchor_height: u64,
    pub anchor_hash: String,
}

/// A restored publication and the body verification it still owes.
pub struct Restored {
    pub finalized: Finalized,
    pub validation: Validation,
}

impl Finalized {
    /// The record a completed preparation of this inventory writes.
    pub fn restore_record(
        &self,
        full: bool,
        recent_from_height: u64,
        anchor_height: u64,
        anchor_hash: &str,
    ) -> Result<RestoreRecord, Error> {
        Ok(RestoreRecord {
            schema: RESTORE_SCHEMA.into(),
            inventory: String::from_utf8(self.bytes.clone())?,
            inventory_sha256: self.sha256.clone(),
            map_sha256: self.inventory.map_sha256.clone(),
            network: self.inventory.network.clone(),
            genesis_hash: self.inventory.genesis_hash.clone(),
            runtime: self.inventory.runtime.clone(),
            profile: self.inventory.profile.clone(),
            full,
            recent_from_height,
            anchor_height,
            anchor_hash: anchor_hash.into(),
            completed: COMPLETED_PHASES.iter().map(|p| p.to_string()).collect(),
        })
    }

    /// Restore a completed publication from its record without reading large
    /// bodies or loading runtimes.
    ///
    /// Checks the record and inventory digests, schema/runtime/profile, chain
    /// and publication identity, the map, manifest and filter digests (small
    /// metadata, hashed here), that the inventory lists exactly the artifacts
    /// the publication holds, that each is a regular file under a safe path
    /// without symbolic links and of the listed length, that every source digest
    /// is its manifest's, and that every runtime header names its identity.
    /// Only those paths are offered. The returned [`Validation`] hashes and
    /// checks the bodies again.
    pub fn restore_completed(
        root: &Path,
        record: &[u8],
        expected: &RestoreExpectation,
    ) -> Result<Restored, Error> {
        let record: RestoreRecord = serde_json::from_slice(record)?;
        if record.schema != RESTORE_SCHEMA
            || record.runtime != RUNTIME
            || record.profile != PROFILE
            || record.completed != COMPLETED_PHASES
        {
            return Err("unsupported restore record schema, runtime, profile or evidence".into());
        }
        let bytes = record.inventory.into_bytes();
        let sha256 = digest(&bytes);
        if sha256 != record.inventory_sha256 {
            return Err("restore record inventory digest mismatch".into());
        }
        if record.network != expected.network
            || record.genesis_hash != expected.genesis_hash
            || record.map_sha256 != expected.map_sha256
            || record.full != expected.full
            || record.recent_from_height != expected.recent_from_height
            || record.anchor_height != expected.anchor_height
            || record.anchor_hash != expected.anchor_hash
        {
            return Err("restore record does not describe the saved publication".into());
        }
        let inventory: Inventory = serde_json::from_slice(&bytes)?;
        inventory.validate(&expected.network, &expected.genesis_hash)?;
        if inventory.map_sha256 != expected.map_sha256
            || inventory.runtime != record.runtime
            || inventory.profile != record.profile
        {
            return Err("restore record and inventory disagree".into());
        }
        let set = ShardSet::open_for_serving(
            root,
            &crate::history::shardset::LoadOptions::whole(
                crate::history::shardset::DEFAULT_RETAIN_REVISIONS,
            ),
        )
        .map_err(|e| format!("restored publication is incomplete: {e}"))?;
        if set.map_digest != inventory.map_sha256 {
            return Err("restored publication map differs from its inventory".into());
        }
        inventory.validate_map(&set.map)?;
        let plan = plan(root, &set, expected.recent_from_height, expected.full)?;
        if plan.inventory.recent_from_shard != inventory.recent_from_shard {
            return Err("restored holdings disagree with the inventory".into());
        }
        let listed: BTreeMap<&str, &Artifact> = inventory
            .artifacts
            .iter()
            .map(|a| (a.path.as_str(), a))
            .collect();
        if listed.len() != plan.pending.len() {
            return Err("restored inventory does not list exactly the publication".into());
        }
        let mut files = BTreeMap::new();
        for (path, kind, _) in &plan.pending {
            let relative = path
                .strip_prefix(root)?
                .to_str()
                .ok_or("non-UTF8 artifact")?;
            let artifact = listed
                .get(relative)
                .filter(|a| a.kind == *kind)
                .ok_or("restored inventory does not list exactly the publication")?;
            let file = crate::verification::open(path)?;
            let metadata = file.metadata()?;
            if !metadata.is_file() || metadata.len() != artifact.bytes {
                return Err(format!("restored artifact {relative} has the wrong length").into());
            }
            if let Some(manifest) = plan.sources.get(path) {
                if *manifest != artifact.sha256 {
                    return Err("restored source digest differs from its manifest".into());
                }
            }
            if *kind == Kind::Metadata {
                let mut file = file;
                let (found, _) = listed_digest(path, &mut file, None, &|| false)?;
                if found != artifact.sha256 {
                    return Err(format!("restored metadata {relative} digest mismatch").into());
                }
            }
            if expected.full || kind.local() {
                files.insert(artifact.sha256.clone(), path.clone());
            }
        }
        let shipped = ShippedRuntimes::required(root.to_path_buf());
        for (path, key, shared, source) in &plan.native {
            if shipped.path(key, shared, source.sha256()) != *path {
                return Err("restored runtime path differs from its identity".into());
            }
            shipped
                .completed(key, shared, source.sha256())
                .map_err(|e| format!("restored runtime {}: {e}", path.display()))?;
        }
        drop(plan);
        Ok(Restored {
            finalized: Self {
                inventory,
                bytes,
                sha256,
                files,
            },
            validation: Validation {
                root: root.to_path_buf(),
                set,
                recent_from_height: expected.recent_from_height,
                full: expected.full,
            },
        })
    }

    /// The body validation a restored or freshly prepared inventory owes.
    pub fn validation(
        &self,
        root: &Path,
        recent_from_height: u64,
        full: bool,
    ) -> Result<Validation, Error> {
        let set = ShardSet::open_for_serving(
            root,
            &crate::history::shardset::LoadOptions::whole(
                crate::history::shardset::DEFAULT_RETAIN_REVISIONS,
            ),
        )?;
        Ok(Validation {
            root: root.to_path_buf(),
            set,
            recent_from_height,
            full,
        })
    }
}

/// Why integrity validation did not pass.
#[derive(Debug)]
pub enum IntegrityError {
    /// The bytes on disk are not the listed ones: a confirmed mismatch.
    Mismatch {
        path: String,
        expected_sha256: String,
        found_sha256: Option<String>,
        reason: String,
    },
    /// The artifact could not be read; nothing is known about its bytes.
    Unavailable(String),
    /// The caller asked to stop.
    Stopped,
}
impl std::fmt::Display for IntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mismatch { path, reason, .. } => write!(f, "{path}: {reason}"),
            Self::Unavailable(reason) => write!(f, "could not validate: {reason}"),
            Self::Stopped => f.write_str("validation stopped"),
        }
    }
}
impl std::error::Error for IntegrityError {}

/// Why correctness validation did not pass.
#[derive(Debug)]
pub enum CorrectnessError {
    /// A runtime did not load as its identity or did not answer its rows.
    Failed(String),
    /// Memory admission refused the load; validation stays pending.
    NoCapacity,
    /// A runtime or source could not be read.
    Unavailable(String),
    Stopped,
}
impl std::fmt::Display for CorrectnessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(reason) => f.write_str(reason),
            Self::NoCapacity => f.write_str("insufficient memory for a runtime check"),
            Self::Unavailable(reason) => write!(f, "could not validate: {reason}"),
            Self::Stopped => f.write_str("validation stopped"),
        }
    }
}
impl std::error::Error for CorrectnessError {}

/// Body verification owed by a publication's exact inventory, in the two
/// phases preparation runs: integrity hashes every listed artifact against
/// its saved SHA-256 (sources were bound to their manifests at restore), and
/// correctness loads each runtime in turn and checks it against its source
/// rows with a sampled private query.
pub struct Validation {
    root: PathBuf,
    set: ShardSet,
    recent_from_height: u64,
    full: bool,
}
/// The exact files one integrity pass found to hold their listed bytes:
/// each artifact's path, digest and the stamp of the inode hashed, unchanged
/// from opening to the end of reading and still the one its path names.
/// Correctness checks only these inodes, and a later pass reuses an entry only
/// while the file still has the same stamp.
#[derive(Clone, Debug, Default)]
pub struct Verified {
    /// Bytes hashed by this pass; reused entries are not counted.
    pub hashed: u64,
    files: BTreeMap<String, (String, FileStamp)>,
}
impl Verified {
    fn stamp(&self, artifact: &Artifact) -> Option<FileStamp> {
        self.files
            .get(&artifact.path)
            .filter(|(sha256, _)| *sha256 == artifact.sha256)
            .map(|(_, stamp)| *stamp)
    }
}

/// The stamp of `file`, opened at `path`, when the path still names it.
fn same_file(path: &Path, file: &std::fs::File) -> io::Result<Option<FileStamp>> {
    let opened = FileStamp::of(&file.metadata()?);
    let named = FileStamp::of(&std::fs::symlink_metadata(path)?);
    Ok(opened.filter(|_| opened == named))
}

impl Validation {
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Hash every listed artifact, one at a time, in 1 MiB reads. `pause` is
    /// asked before each read; true stops. An artifact `known` verified is
    /// not read again while its path still names the same unchanged inode.
    pub fn integrity(
        &self,
        inventory: &Inventory,
        known: Option<&Verified>,
        pause: &dyn Fn() -> bool,
    ) -> Result<Verified, IntegrityError> {
        let mut verified = Verified::default();
        for artifact in &inventory.artifacts {
            if let Some(stamp) = known.and_then(|k| k.stamp(artifact)) {
                let path = self.root.join(&artifact.path);
                let still = std::fs::symlink_metadata(&path)
                    .ok()
                    .and_then(|m| FileStamp::of(&m));
                if safe_path(&artifact.path) && still == Some(stamp) {
                    verified
                        .files
                        .insert(artifact.path.clone(), (artifact.sha256.clone(), stamp));
                    continue;
                }
            }
            let stamp = self.hash_one(artifact, pause)?;
            verified.hashed += artifact.bytes;
            verified
                .files
                .insert(artifact.path.clone(), (artifact.sha256.clone(), stamp));
        }
        Ok(verified)
    }

    fn hash_one(
        &self,
        artifact: &Artifact,
        pause: &dyn Fn() -> bool,
    ) -> Result<FileStamp, IntegrityError> {
        use io::Read;
        let unavailable = |e: &dyn std::fmt::Display| {
            IntegrityError::Unavailable(format!("{}: {e}", artifact.path))
        };
        let mismatch = |found: Option<String>, reason: &str| IntegrityError::Mismatch {
            path: artifact.path.clone(),
            expected_sha256: artifact.sha256.clone(),
            found_sha256: found,
            reason: reason.into(),
        };
        if !safe_path(&artifact.path) {
            return Err(mismatch(None, "unsafe artifact path"));
        }
        let mut checked = self.root.clone();
        for component in artifact.path.split('/') {
            checked.push(component);
            let metadata = std::fs::symlink_metadata(&checked).map_err(|e| unavailable(&e))?;
            if metadata.file_type().is_symlink() {
                return Err(mismatch(None, "artifact became a symbolic link"));
            }
        }
        let mut file = std::fs::File::open(&checked).map_err(|e| unavailable(&e))?;
        let metadata = file.metadata().map_err(|e| unavailable(&e))?;
        if !metadata.is_file() {
            return Err(mismatch(None, "artifact is not a regular file"));
        }
        if metadata.len() != artifact.bytes {
            return Err(mismatch(None, "artifact length changed"));
        }
        // A file replaced or rewritten while it is read says nothing about
        // either version; it is read again later.
        let before = same_file(&checked, &file)
            .map_err(|e| unavailable(&e))?
            .ok_or_else(|| unavailable(&"artifact was replaced while opening"))?;
        let mut hash = Sha256::new();
        let mut buffer = vec![0u8; 1 << 20];
        let mut total = 0u64;
        loop {
            if pause() {
                return Err(IntegrityError::Stopped);
            }
            let read = file.read(&mut buffer).map_err(|e| unavailable(&e))?;
            if read == 0 {
                break;
            }
            total += read as u64;
            hash.update(&buffer[..read]);
        }
        if same_file(&checked, &file).map_err(|e| unavailable(&e))? != Some(before) {
            return Err(unavailable(&"artifact changed while it was read"));
        }
        let found = hex::encode(hash.finalize());
        if total != artifact.bytes {
            return Err(mismatch(
                Some(found),
                "artifact length changed while reading",
            ));
        }
        if found != artifact.sha256 {
            return Err(mismatch(Some(found), "artifact digest mismatch"));
        }
        Ok(before)
    }

    /// Load each listed runtime alone, admitted by `memory`, and check it
    /// against its source rows with a sampled private query. Only the inodes
    /// `verified` hashed are used: a runtime or source replaced or changed
    /// since then leaves this phase unavailable, never passed, and source rows
    /// are checked against their manifest digest again as they are read.
    /// Returns the runtimes checked, as preparation records them, so later
    /// preparations in this process reuse them while their bytes are unchanged.
    pub fn correctness(
        &self,
        inventory: &Inventory,
        verified: &Verified,
        memory: &std::sync::Arc<crate::runtime::memory::WorkMemory>,
        pause: &dyn Fn() -> bool,
    ) -> Result<CheckedRuntimes, CorrectnessError> {
        let plan = plan(&self.root, &self.set, self.recent_from_height, self.full)
            .map_err(|e| CorrectnessError::Unavailable(e.to_string()))?;
        let listed: BTreeMap<PathBuf, &Artifact> = inventory
            .artifacts
            .iter()
            .map(|a| (self.root.join(&a.path), a))
            .collect();
        let relative = |path: &Path| {
            path.strip_prefix(&self.root)
                .ok()
                .and_then(|p| p.to_str())
                .and_then(|p| inventory.artifacts.iter().find(|a| a.path == p))
        };
        // Open `path` only while it still names the inode integrity hashed.
        let open = |path: &Path, artifact: &Artifact| {
            let stamp = verified.stamp(artifact).ok_or_else(|| {
                CorrectnessError::Unavailable(format!("{} was not integrity-verified", artifact.path))
            })?;
            let file = crate::verification::open(path)
                .map_err(|e| CorrectnessError::Unavailable(e.to_string()))?;
            match same_file(path, &file) {
                Ok(Some(found)) if found == stamp => Ok((file, stamp)),
                Ok(_) => Err(CorrectnessError::Unavailable(format!(
                    "{} changed since integrity validation",
                    artifact.path
                ))),
                Err(e) => Err(CorrectnessError::Unavailable(e.to_string())),
            }
        };
        let unchanged = |path: &Path, file: &std::fs::File, stamp: FileStamp, name: &str| {
            match same_file(path, file) {
                Ok(Some(found)) if found == stamp => Ok(()),
                _ => Err(CorrectnessError::Unavailable(format!(
                    "{name} changed during correctness validation"
                ))),
            }
        };
        let mut checked = CheckedRuntimes::default();
        for (path, key, shared, source) in plan.native {
            if pause() {
                return Err(CorrectnessError::Stopped);
            }
            let artifact = listed
                .get(&path)
                .ok_or_else(|| CorrectnessError::Failed("runtime is not listed".into()))?;
            let source_artifact = relative(source.path())
                .ok_or_else(|| CorrectnessError::Failed("source is not listed".into()))?;
            let _admitted = memory
                .reserve(artifact.bytes.saturating_add(source.bytes() as u64))
                .ok_or(CorrectnessError::NoCapacity)?;
            let (file, stamp) = open(&path, artifact)?;
            let probe = file
                .try_clone()
                .map_err(|e| CorrectnessError::Unavailable(e.to_string()))?;
            let runtime = ShippedRuntimes::load_file(file, &key, &shared, source.sha256())
                .map_err(|e| match e.kind() {
                    io::ErrorKind::InvalidData => CorrectnessError::Failed(e.to_string()),
                    _ => CorrectnessError::Unavailable(e.to_string()),
                })?;
            unchanged(&path, &probe, stamp, &artifact.path)?;
            let (mut rows_file, rows_stamp) = open(source.path(), source_artifact)?;
            let mut rows = Vec::with_capacity(source.bytes());
            io::Read::read_to_end(
                &mut io::Read::take(&mut rows_file, source.bytes() as u64 + 1),
                &mut rows,
            )
            .map_err(|e| CorrectnessError::Unavailable(e.to_string()))?;
            unchanged(source.path(), &rows_file, rows_stamp, &source_artifact.path)?;
            source.verify_bytes(&rows).map_err(|e| match e {
                crate::history::shardset::LoadError::Invalid(reason) => {
                    CorrectnessError::Failed(reason)
                }
                other => CorrectnessError::Unavailable(other.to_string()),
            })?;
            runtime
                .self_check(&shared, &rows)
                .map_err(|e| CorrectnessError::Failed(format!("{}: {e}", artifact.path)))?;
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| CorrectnessError::Failed("non-UTF8 runtime name".into()))?;
            checked
                .passed
                .insert(name.to_owned(), artifact.sha256.clone());
            checked.loaded += 1;
        }
        Ok(checked)
    }
}
/// Native material verified by this process, bounded to the last successful
/// inventory. Names and whole-file digests are derived again on every prepare.
#[derive(Debug, Default)]
pub struct CheckedRuntimes {
    pub(crate) passed: BTreeMap<String, String>,
    /// Native runtimes loaded and self-checked by the last attempt.
    pub loaded: usize,
    /// Native runtimes matching earlier checked bytes in the last attempt.
    pub reused: usize,
}

impl CheckedRuntimes {
    /// Add runtimes another check of this process loaded and self-checked.
    pub fn adopt(&mut self, other: CheckedRuntimes) {
        self.passed.extend(other.passed);
    }
}

/// A digest and length this process verified for a file with this stamp.
pub(crate) struct Known {
    sha256: String,
    bytes: u64,
    stamp: FileStamp,
}
impl Known {
    /// The verification `source` holds for its exact file, if any: only a
    /// source this process hashed with settled evidence; never a receipt-only,
    /// received or size-only one.
    pub(crate) fn of(source: &crate::history::shardset::SegmentSource) -> Option<Self> {
        source.evidence().map(|stamp| Self {
            sha256: source.sha256().to_owned(),
            bytes: source.bytes() as u64,
            stamp,
        })
    }
}

/// [`hash_listed`] without evidence or a stop: every file is hashed.
#[cfg(test)]
fn hash_artifacts(
    root: &Path,
    pending: Vec<(PathBuf, Kind)>,
) -> Result<Vec<(Artifact, PathBuf, Option<std::fs::File>)>, Error> {
    let pending = pending
        .into_iter()
        .map(|(path, kind)| (path, kind, None))
        .collect();
    hash_listed(root, pending, &|| false)
}

/// Hash each pending artifact, listing a file with its `Known` digest when it is
/// unchanged since verified, and ending with [`Stopped`] once `stop` answers
/// true before a file or any bounded read of one.
pub(crate) fn hash_listed(
    root: &Path,
    pending: Vec<(PathBuf, Kind, Option<Known>)>,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Vec<(Artifact, PathBuf, Option<std::fs::File>)>, Error> {
    crate::runtime::build_pool().install(|| {
        pending
            .into_par_iter()
            .map(|(path, kind, known)| {
                if stop() {
                    return Err(Box::new(Stopped) as Error);
                }
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
                let mut file = std::fs::File::open(&path)?;
                let known = known
                    .as_ref()
                    .map(|k| (k.sha256.as_str(), k.bytes, k.stamp));
                let (sha256, bytes) = listed_digest(&path, &mut file, known, stop)?;
                let opened =
                    matches!(kind, Kind::RecentRuntime | Kind::ArchiveRuntime).then_some(file);
                Ok((
                    Artifact {
                        path: relative,
                        sha256,
                        bytes,
                        kind,
                    },
                    path,
                    opened,
                ))
            })
            .collect()
    })
}

/// A finalization ended because its caller asked it to stop: a normal end
/// of the lifecycle, not a faulty publication.
#[derive(Debug)]
pub struct Stopped;
impl std::fmt::Display for Stopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("publication finalization stopped")
    }
}
impl std::error::Error for Stopped {}

/// Inventory files streamed through SHA-256, from any pool thread.
#[cfg(test)]
static LISTED_HASHED: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

/// How many inventory files under `dir` were streamed through SHA-256.
#[cfg(test)]
pub(crate) fn listed_hashes(dir: &Path) -> usize {
    let hashed = LISTED_HASHED.lock().unwrap();
    hashed.iter().filter(|path| path.starts_with(dir)).count()
}

/// The digest and length to list for `file`, opened at `path`: `known` when
/// it was verified for this very file and the file is unchanged since, both
/// as opened and as the path names it now; otherwise those of a full read.
fn listed_digest(
    path: &Path,
    file: &mut std::fs::File,
    known: Option<(&str, u64, FileStamp)>,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<(String, u64), Error> {
    let opened = file.metadata()?;
    if !opened.is_file() {
        return Err("artifact is not a regular file".into());
    }
    if let Some((sha256, bytes, stamp)) = known {
        let named = std::fs::symlink_metadata(path).ok();
        if FileStamp::of(&opened) == Some(stamp)
            && named.as_ref().and_then(FileStamp::of) == Some(stamp)
            && opened.len() == bytes
        {
            return Ok((sha256.to_string(), bytes));
        }
    }
    #[cfg(test)]
    LISTED_HASHED.lock().unwrap().push(path.to_path_buf());
    let bytes = opened.len();
    let mut hash = Sha256::new();
    let mut buffer = vec![0u8; 1 << 20];
    loop {
        if stop() {
            return Err(Box::new(Stopped));
        }
        let read = io::Read::read(file, &mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok((hex::encode(hash.finalize()), bytes))
}

pub fn verify_file(path: &Path, expected: &Artifact) -> Result<(), Error> {
    let file = crate::verification::open(path)?;
    if file.metadata()?.len() != expected.bytes {
        return Err("artifact length mismatch".into());
    }
    Ok(())
}

/// The caller has verified the network stream, or linked/copied its completed
/// cache object. Persist its download identity after checking the local size.
pub fn record_received_file(path: &Path, expected: &Artifact, object: &Path) -> Result<(), Error> {
    crate::verification::received(path, &expected.sha256, expected.bytes, object)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_hashes_match_the_complete_files_in_input_order() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let inputs = [
            ("large.bin", vec![7u8; 128 << 10]),
            ("small.bin", vec![3u8; 17]),
        ];
        let mut pending = Vec::new();
        for (name, bytes) in &inputs {
            let path = root.join(name);
            std::fs::write(&path, bytes).unwrap();
            pending.push((path, Kind::RecentTable));
        }
        let actual = hash_artifacts(root, pending).unwrap();
        for ((artifact, path, file), (name, bytes)) in actual.iter().zip(&inputs) {
            assert!(file.is_none());
            assert_eq!(artifact.path, *name);
            assert_eq!(*path, root.join(name));
            assert_eq!(artifact.bytes, bytes.len() as u64);
            assert_eq!(artifact.sha256, hex::encode(Sha256::digest(bytes)));
            assert_eq!(artifact.kind, Kind::RecentTable);
        }
    }

    #[test]
    fn publisher_inventory_hashes_received_artifacts_in_full() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let path = root.join("source.bin");
        let original = b"network bytes";
        std::fs::write(&path, original).unwrap();
        let expected = Artifact {
            path: "source.bin".into(),
            sha256: digest(original),
            bytes: original.len() as u64,
            kind: Kind::RecentTable,
        };
        record_received_file(&path, &expected, &path).unwrap();
        let changed = b"same size!!!!";
        std::fs::write(&path, changed).unwrap();
        let hashed = hash_artifacts(root, vec![(path, Kind::RecentTable)]).unwrap();
        assert_eq!(hashed[0].0.sha256, digest(changed));
        assert_ne!(hashed[0].0.sha256, expected.sha256);
    }

    #[test]
    fn native_hash_keeps_the_opened_inode_for_verification() {
        use std::io::{Read, Seek, SeekFrom};
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let path = root.join("native.runtime");
        let original = b"the bytes the inventory hashed";
        std::fs::write(&path, original).unwrap();
        let mut hashed = hash_artifacts(root, vec![(path.clone(), Kind::RecentRuntime)]).unwrap();
        let (artifact, _, opened) = hashed.pop().unwrap();
        let replacement = root.join("replacement");
        std::fs::write(&replacement, b"a later path replacement").unwrap();
        std::fs::rename(replacement, &path).unwrap();
        let mut file = opened.expect("native verification needs the hashed handle");
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, original);
        assert_eq!(artifact.sha256, digest(&bytes));
        assert_ne!(artifact.sha256, digest(&std::fs::read(path).unwrap()));
    }

    #[test]
    fn an_unreadable_artifact_prevents_finalization() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let valid = root.join("valid.bin");
        std::fs::write(&valid, b"complete file").unwrap();
        assert!(hash_artifacts(
            root,
            vec![
                (valid, Kind::RecentTable),
                (root.join("missing.bin"), Kind::RecentTable),
            ]
        )
        .is_err());
    }

    #[test]
    #[cfg(unix)]
    fn artifact_hashing_refuses_a_symbolic_link() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::write(root.join("real.bin"), b"complete file").unwrap();
        std::os::unix::fs::symlink(root.join("real.bin"), root.join("link.bin")).unwrap();
        assert!(hash_artifacts(root, vec![(root.join("link.bin"), Kind::RecentTable)]).is_err());
    }
    /// A stop between bounded reads ends the hash of a file mid-stream, and
    /// one before a file ends the listing before it is opened.
    #[test]
    fn a_stop_ends_listing_between_bounded_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("table.bin");
        std::fs::write(&path, vec![7u8; (2 << 20) + 1]).unwrap();
        let asked = std::sync::atomic::AtomicU32::new(0);
        let stop = || asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1;
        let mut file = std::fs::File::open(&path).unwrap();
        let error = listed_digest(&path, &mut file, None, &stop).unwrap_err();
        assert!(error.is::<Stopped>());
        assert_eq!(
            asked.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "one chunk was read"
        );
        let pending = vec![(path.clone(), Kind::RecentTable, None)];
        let error = hash_listed(dir.path(), pending, &|| true).unwrap_err();
        assert!(error.is::<Stopped>());
        // Resumed without a stop, the same file is hashed whole.
        let pending = vec![(path.clone(), Kind::RecentTable, None)];
        let listed = hash_listed(dir.path(), pending, &|| false).unwrap();
        assert_eq!(listed[0].0.sha256, digest(&vec![7u8; (2 << 20) + 1]));
    }

    #[test]
    fn verified_evidence_lists_an_unchanged_file_without_hashing_it() {
        use std::os::unix::fs::FileExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("table.bin");
        std::fs::write(&path, b"verified table").unwrap();
        let stamp = FileStamp::of(&std::fs::metadata(&path).unwrap()).unwrap();
        let sha = digest(b"verified table");
        let open = || std::fs::File::open(&path).unwrap();
        let before = listed_hashes(dir.path());
        let listed = listed_digest(&path, &mut open(), Some((&sha, 14, stamp)), &|| false).unwrap();
        assert_eq!(listed, (sha.clone(), 14));
        assert_eq!(listed_hashes(dir.path()), before);
        // Same size, different bytes: the stamp no longer matches, so the
        // listed digest is that of the bytes now present.
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all_at(b"V", 0)
            .unwrap();
        let listed = listed_digest(&path, &mut open(), Some((&sha, 14, stamp)), &|| false).unwrap();
        assert_eq!(listed, (digest(b"Verified table"), 14));
        assert_eq!(listed_hashes(dir.path()), before + 1);
        // A replaced path is another inode.
        std::fs::write(dir.path().join("next"), b"verified table").unwrap();
        std::fs::rename(dir.path().join("next"), &path).unwrap();
        listed_digest(&path, &mut open(), Some((&sha, 14, stamp)), &|| false).unwrap();
        assert_eq!(listed_hashes(dir.path()), before + 2);
    }

    #[test]
    fn local_files_without_receipts_are_checked_only_for_size() {
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
        verify_file(&path, &expected).unwrap();
        std::fs::write(&path, b"short").unwrap();
        assert!(verify_file(&path, &expected).is_err());
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
