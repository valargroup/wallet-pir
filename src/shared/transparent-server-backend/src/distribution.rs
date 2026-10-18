//! History-only prepared-publication distribution. Verified bytes alone never
//! grant serving authority. This inventory is not a wallet format.
use crate::{
    runtime::{disk::ShippedRuntimes, SharedParams},
    shardset::ShardSet,
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Metadata,
    RecentTable,
    RecentRuntime,
    ArchiveTable,
}
impl Kind {
    pub fn local(self) -> bool {
        self != Self::ArchiveTable
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
#[derive(Clone, Debug, Serialize, Deserialize)]
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
                Kind::RecentRuntime => {
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
        let mut paths = BTreeSet::new();
        let mut add = |path: PathBuf, kind: Kind| -> Result<(), Error> {
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
                return Ok(());
            }
            let mut file = std::fs::File::open(&path)?;
            if !file.metadata()?.is_file() {
                return Err("artifact is not a regular file".into());
            }
            let bytes = file.metadata()?.len();
            let mut hash = Sha256::new();
            io::copy(&mut file, &mut HashWriter(&mut hash))?;
            let sha256 = hex::encode(hash.finalize());
            if kind.local() {
                files.insert(sha256.clone(), path);
            }
            inventory.artifacts.push(Artifact {
                path: relative,
                sha256,
                bytes,
                kind,
            });
            Ok(())
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
                let params = if local {
                    Some(SharedParams::build(shard.geometry, table.kind())?)
                } else {
                    None
                };
                for segment in 0..shard.segments(table) {
                    let source = shard
                        .segment(table, segment)
                        .ok_or("missing source segment")?;
                    add(
                        source.path().to_path_buf(),
                        if local {
                            Kind::RecentTable
                        } else {
                            Kind::ArchiveTable
                        },
                    )?;
                    if let Some(shared) = &params {
                        let key = (
                            shard.runtime_identity(table).to_string(),
                            table.kind(),
                            segment,
                        );
                        // Header/length checks alone cannot finalize malformed native bytes.
                        let runtime = shipped.load(&key, shared, source.sha256())?;
                        runtime.self_check(shared, &source.load()?)?;
                        add(
                            shipped.path(&key, shared, source.sha256()),
                            Kind::RecentRuntime,
                        )?;
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
    let mut file = std::fs::File::open(path)?;
    if file.metadata()?.len() != expected.bytes {
        return Err("artifact length mismatch".into());
    }
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 1 << 20];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    if hex::encode(hash.finalize()) != expected.sha256 {
        return Err("artifact digest mismatch".into());
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_unsafe_paths_and_noncanonical_digests() {
        for p in [
            "../x",
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
}
