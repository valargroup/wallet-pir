//! Completed publication evidence and restart validation.
use super::*;

/// The publisher's record of a completed History publication, written beside
/// it only after its required preparation passed. It lets a restarted
/// publisher offer the exact same inventory without rescanning its bodies;
/// the bodies are verified again in the background after serving starts.
/// Completion evidence never grants serving authority.
pub const RESTORE_SCHEMA: &str = "transparent-history-restore-v1";
/// The restore record's file name inside the publication directory.
pub const RESTORE_FILE: &str = "history-restore.json";
/// Preparation phases a restore record must name as completed.
pub(super) const COMPLETED_PHASES: [&str; 2] = ["integrity", "correctness"];

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
            let (physical, file) = resolve_path(root, path)?;
            let metadata = file.metadata()?;
            if metadata.len() != artifact.bytes {
                return Err(format!("restored artifact {relative} has the wrong length").into());
            }
            if let Some(manifest) = plan.sources.get(path) {
                if *manifest != artifact.sha256 {
                    return Err("restored source digest differs from its manifest".into());
                }
            }
            if *kind == Kind::Metadata {
                let mut file = file;
                let found = listed_digest(&physical, &mut file, None, &|| false)?.sha256;
                if found != artifact.sha256 {
                    return Err(format!("restored metadata {relative} digest mismatch").into());
                }
            }
            if expected.full || kind.local() {
                files.insert(artifact.sha256.clone(), (physical, artifact.bytes));
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
