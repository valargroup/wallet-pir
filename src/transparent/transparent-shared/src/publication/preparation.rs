//! Local publisher preparation, separate from this process's content checks.
use super::*;
use pir_publication::{is_sealed_object, PreparationRecord};

/// Publisher-local preparation receipt, never part of the distributed inventory.
pub const PREPARATION_FILE: &str = "history-preparation.json";
const SEMANTICS: &str = "history-publisher-preparation-v1";

/// Preparation inherited by the publisher while independent validation is pending.
pub struct PreparationReuse {
    pub tables: ShardSet,
    pub runtimes: CheckedRuntimes,
    /// Exact identities adopted on first bootstrap, for durable local reuse.
    pub record: PreparationRecord,
}

impl Finalized {
    /// At activation, require every file to match its prepared identity without
    /// rereading bodies or requiring immutable-store adoption. Fails for absent
    /// preparation evidence, unsafe paths, or changed files. A metadata-only
    /// restoration supplies no such evidence; its separate serving path owes
    /// background validation instead.
    pub fn check_prepared_files(&self, root: &Path) -> Result<(), Error> {
        let files = self
            .prepared_files
            .as_ref()
            .ok_or("publication has no prepared file identities")?;
        for (path, identity) in files {
            let (physical, file) = open_artifact(root, path, None)?;
            identity.check(&physical, &file)?;
        }
        Ok(())
    }

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
        let mut record =
            PreparationRecord::new(SEMANTICS, digest(&serde_json::to_vec(completion)?));
        let store = root.parent().map(|p| p.join(OBJECT_STORE));
        for artifact in &self.inventory.artifacts {
            let reference = ObjectReference {
                sha256: artifact.sha256.clone(),
                bytes: artifact.bytes,
            };
            let (physical, file) = open_artifact(root, &artifact.path, Some(&reference))?;
            // Only publisher-owned immutable-store bodies may bypass preparation.
            if !matches!(artifact.kind, Kind::Metadata)
                && !store
                    .as_deref()
                    .map(|store| is_sealed_object(store, &physical, &file))
                    .transpose()?
                    .unwrap_or(false)
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
            record.insert(
                artifact.path.clone(),
                artifact.sha256.clone(),
                artifact.bytes,
                identity,
            );
        }
        Ok(record)
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
        if saved.as_ref().is_some_and(|saved| !current.agrees(saved)) {
            return Err("publisher preparation record is invalid".into());
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
            let (_, _, identity) = current.files()[&artifact.path];
            let (physical, file) = open_artifact(
                root,
                &artifact.path,
                Some(&ObjectReference {
                    sha256: artifact.sha256.clone(),
                    bytes: artifact.bytes,
                }),
            )?;
            identity.check(&physical, &file)?;
            // Without a saved record the first restart adopts what it finds.
            let now = std::time::SystemTime::now();
            if !saved
                .as_ref()
                .map_or(identity.settled_before(now), |saved| {
                    saved.adopts(&artifact.path, identity, now)
                })
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
