//! History distribution identities and authority attestations.
use super::*;

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
