//! Local publisher preparation, separate from this process's content checks.
use super::*;

/// Publisher-local preparation receipt, never part of the distributed inventory.
pub const PREPARATION_FILE: &str = "history-preparation.json";
const SEMANTICS: &str = "history-publisher-preparation-v1";

/// A local receipt has no serving authority and is never a verified-object proof.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationRecord {
    semantics: String,
    completion_sha256: String,
    files: BTreeMap<String, (String, u64, FileIdentity)>,
}

/// Preparation inherited by the publisher while independent validation is pending.
pub struct PreparationReuse {
    pub tables: ShardSet,
    pub runtimes: CheckedRuntimes,
    /// Exact identities adopted on first bootstrap, for durable local reuse.
    pub record: PreparationRecord,
}

impl Finalized {
    /// Capture installed, sealed object identities only after preparation completes.
    /// The exact completion record binds inventory, network, parameters and profile.
    pub fn preparation_record(
        &self,
        root: &Path,
        completion: &RestoreRecord,
    ) -> Result<PreparationRecord, Error> {
        if completion.inventory_sha256 != self.sha256 {
            return Err("preparation completion differs from inventory".into());
        }
        let mut files = BTreeMap::new();
        for artifact in &self.inventory.artifacts {
            let reference = ObjectReference {
                sha256: artifact.sha256.clone(),
                bytes: artifact.bytes,
            };
            let (physical, file) = open_artifact(root, &artifact.path, Some(&reference))?;
            // Only publisher-owned immutable-store bodies may bypass preparation.
            if !matches!(artifact.kind, Kind::Metadata)
                && (physical.parent() != root.parent().map(|p| p.join(OBJECT_STORE)).as_deref()
                    || !file.metadata()?.permissions().readonly())
            {
                return Err("preparation requires sealed publisher objects".into());
            }
            let identity = FileIdentity::capture(&physical, &file)?;
            if self
                .prepared_files
                .as_ref()
                .is_some_and(|files| files.get(&artifact.path) != Some(&identity))
            {
                return Err("artifact changed after publisher preparation".into());
            }
            files.insert(
                artifact.path.clone(),
                (artifact.sha256.clone(), artifact.bytes, identity),
            );
        }
        Ok(PreparationRecord {
            semantics: SEMANTICS.into(),
            completion_sha256: digest(&serde_json::to_vec(completion)?),
            files,
        })
    }
}

impl Restored {
    /// Called only for an active publisher-owned completion after failure and
    /// activation-intent checks. Absence permits first adoption; a present but
    /// invalid receipt fails closed to ordinary foreground verification.
    /// Background validation always starts with empty content-check evidence.
    pub fn publisher_preparation(
        &self,
        completion: &RestoreRecord,
        saved: Option<&[u8]>,
    ) -> Result<PreparationReuse, Error> {
        let root = &self.validation.root;
        let current = self.finalized.preparation_record(root, completion)?;
        let saved = saved
            .map(serde_json::from_slice::<PreparationRecord>)
            .transpose()?;
        if let Some(saved) = &saved {
            if saved.semantics != current.semantics
                || saved.completion_sha256 != current.completion_sha256
                || saved.files.len() != current.files.len()
                || current.files.iter().any(|(name, (digest, bytes, _))| {
                    saved
                        .files
                        .get(name)
                        .is_none_or(|(d, b, _)| d != digest || b != bytes)
                })
            {
                return Err("publisher preparation record is invalid".into());
            }
        }
        let mut tables = ShardSet::open_for_serving(
            root,
            &crate::history::shardset::LoadOptions::whole(
                crate::history::shardset::DEFAULT_RETAIN_REVISIONS,
            ),
        )?;
        let mut identities = BTreeMap::new();
        let mut runtimes = CheckedRuntimes::default();
        for artifact in &self.finalized.inventory.artifacts {
            let (_, _, identity) = current.files[&artifact.path];
            let (physical, file) = open_artifact(
                root,
                &artifact.path,
                Some(&ObjectReference {
                    sha256: artifact.sha256.clone(),
                    bytes: artifact.bytes,
                }),
            )?;
            identity.check(&physical, &file)?;
            if !identity.settled_before(std::time::SystemTime::now())
                || saved
                    .as_ref()
                    .is_some_and(|saved| saved.files[&artifact.path].2 != identity)
            {
                continue;
            }
            identities.insert(root.join(&artifact.path), identity);
            if matches!(artifact.kind, Kind::RecentRuntime | Kind::ArchiveRuntime) {
                let name = Path::new(&artifact.path)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .ok_or("invalid runtime name")?;
                runtimes.preparation.insert(
                    name.into(),
                    (artifact.sha256.clone(), artifact.bytes, identity),
                );
            }
        }
        tables.inherit_preparation(&identities);
        Ok(PreparationReuse {
            tables,
            runtimes,
            record: current,
        })
    }
}
