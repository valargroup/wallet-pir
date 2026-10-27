//! Integrity and correctness checks of prepared publications.
use super::*;

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
    pub(super) root: PathBuf,
    pub(super) set: ShardSet,
    pub(super) recent_from_height: u64,
    pub(super) full: bool,
}
/// The exact files one integrity pass found to hold their listed bytes:
/// each artifact's path, digest and the stamp of the inode hashed, unchanged
/// from opening to the end of reading and still the one its path names.
/// Correctness checks only these inodes, and a later pass reuses an entry only
/// while the file still has the same stamp.
#[derive(Clone, Debug, Default)]
pub struct Verified {
    context: Option<[String; 4]>,
    /// Bytes hashed by this pass; reused entries are not counted.
    pub hashed: u64,
    pub(super) files: BTreeMap<String, (String, FileIdentity)>,
    /// Fresh observations support this pass's correctness checks, but only
    /// files settled before its start may vouch for contents in another pass.
    pub(super) started: Option<std::time::SystemTime>,
}
impl Verified {
    /// Drop progress for objects the publisher's ordinary collector removed.
    /// Metadata is cheap and is checked again under each publication's path.
    pub fn retain_stored(&mut self, root: &Path) {
        self.files
            .retain(|_, (digest, stamp)| unchanged_stored(root, digest, *stamp));
    }

    pub(super) fn stamp(&self, artifact: &Artifact) -> Option<FileIdentity> {
        self.files
            .get(&artifact.path)
            .filter(|(sha256, _)| *sha256 == artifact.sha256)
            .map(|(_, stamp)| *stamp)
    }
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
        let mut progress = known.cloned().unwrap_or_default();
        self.integrity_continuing(inventory, &mut progress, pause)
    }

    /// Preserve completed settled artifact hashes even if a newer job cancels
    /// this scan. Pruned to the incoming inventory on every job; preparation
    /// receipts are never accepted here.
    pub fn integrity_continuing(
        &self,
        inventory: &Inventory,
        progress: &mut Verified,
        pause: &dyn Fn() -> bool,
    ) -> Result<Verified, IntegrityError> {
        let context = validation_context(inventory);
        if progress.context.as_ref() != Some(&context) {
            *progress = Verified::default();
        }
        progress.context = Some(context.clone());
        let mut retained = BTreeMap::new();
        for artifact in &inventory.artifacts {
            if let Some((_, value)) = progress.files.iter().find(|(_, (digest, stamp))| {
                digest == &artifact.sha256
                    && progress
                        .started
                        .is_none_or(|started| stamp.settled_before(started))
            }) {
                retained.insert(artifact.path.clone(), value.clone());
            }
        }
        progress.files = retained;
        progress.started = None;
        let known = progress.clone();
        let mut verified = Verified {
            context: Some(context),
            started: Some(std::time::SystemTime::now()),
            ..Verified::default()
        };
        for artifact in &inventory.artifacts {
            if let Some(stamp) = known.stamp(artifact) {
                let still = resolve(&self.root, &artifact.path)
                    .ok()
                    .and_then(|(path, file)| {
                        let identity = FileIdentity::capture(&path, &file).ok()?;
                        (file.metadata().ok()?.len() == artifact.bytes).then_some(identity)
                    });
                if still == Some(stamp) {
                    verified
                        .files
                        .insert(artifact.path.clone(), (artifact.sha256.clone(), stamp));
                    continue;
                }
            }
            let stamp = self.hash_one(artifact, pause)?;
            verified.hashed += artifact.bytes;
            if stamp.settled_before(verified.started.unwrap()) {
                progress
                    .files
                    .insert(artifact.path.clone(), (artifact.sha256.clone(), stamp));
            }
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
    ) -> Result<FileIdentity, IntegrityError> {
        let unavailable = |error: &dyn std::fmt::Display| {
            IntegrityError::Unavailable(format!("{}: {error}", artifact.path))
        };
        let mismatch = |found, reason: &str| IntegrityError::Mismatch {
            path: artifact.path.clone(),
            expected_sha256: artifact.sha256.clone(),
            found_sha256: found,
            reason: reason.into(),
        };
        if !safe_path(&artifact.path) {
            return Err(mismatch(None, "unsafe artifact path"));
        }
        let (physical, mut file) =
            resolve(&self.root, &artifact.path).map_err(|error| open_failure(artifact, &error))?;
        let expected = ObjectReference {
            sha256: artifact.sha256.clone(),
            bytes: artifact.bytes,
        };
        hash_object_file(&physical, &mut file, Some(&expected), pause)
            .map(|(_, identity)| identity)
            .map_err(|error| match error {
                ObjectFileError::Cancelled => IntegrityError::Stopped,
                ObjectFileError::DigestMismatch { actual, .. } => {
                    mismatch(Some(actual), "artifact digest mismatch")
                }
                ObjectFileError::LengthMismatch { .. } => mismatch(None, "artifact length changed"),
                ObjectFileError::NotRegularFile(_) => {
                    mismatch(None, "artifact is not a regular file")
                }
                error => unavailable(&error),
            })
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
        self.correctness_continuing(
            inventory,
            verified,
            memory,
            pause,
            &mut CheckedRuntimes::default(),
        )
    }

    /// Continue independently completed runtime checks across superseding jobs.
    /// Only the same derived runtime, digest and unchanged hashed inode qualify.
    pub fn correctness_continuing(
        &self,
        inventory: &Inventory,
        verified: &Verified,
        memory: &std::sync::Arc<crate::runtime::memory::WorkMemory>,
        pause: &dyn Fn() -> bool,
        progress: &mut CheckedRuntimes,
    ) -> Result<CheckedRuntimes, CorrectnessError> {
        let context = validation_context(inventory);
        if progress.context.as_ref() != Some(&context) {
            *progress = CheckedRuntimes::default();
        }
        progress.context = Some(context);
        progress.evidence.retain(|name, (digest, _, _)| {
            inventory.artifacts.iter().any(|a| {
                Path::new(&a.path).file_name().and_then(|n| n.to_str()) == Some(name.as_str())
                    && &a.sha256 == digest
            })
        });
        progress
            .passed
            .retain(|name, _| progress.evidence.contains_key(name));
        let started = std::time::SystemTime::now();
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
                CorrectnessError::Unavailable(format!(
                    "{} was not integrity-verified",
                    artifact.path
                ))
            })?;
            let (path, file) = resolve_path(&self.root, path)
                .map_err(|e| CorrectnessError::Unavailable(e.to_string()))?;
            stamp
                .check(&path, &file)
                .map_err(|e| CorrectnessError::Unavailable(e.to_string()))?;
            Ok((file, stamp))
        };
        let unchanged = |path: &Path, file: &std::fs::File, stamp: FileIdentity, name: &str| {
            let (physical, _) = resolve_path(&self.root, path)
                .map_err(|e| CorrectnessError::Unavailable(e.to_string()))?;
            stamp.check(&physical, file).map_err(|e| {
                CorrectnessError::Unavailable(format!(
                    "{name} changed during correctness validation: {e}"
                ))
            })
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
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| CorrectnessError::Failed("non-UTF8 runtime name".into()))?;
            if let Some((digest, bytes, stamp)) = progress.evidence.get(name) {
                if digest == &artifact.sha256
                    && *bytes == artifact.bytes
                    && verified.stamp(artifact) == Some(*stamp)
                    && open(&path, artifact).is_ok()
                    && open(source.path(), source_artifact).is_ok()
                {
                    checked.passed.insert(name.into(), digest.clone());
                    checked.reused += 1;
                    continue;
                }
            }
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
            unchanged(&path, &probe, stamp, &artifact.path)?;
            unchanged(source.path(), &rows_file, rows_stamp, &source_artifact.path)?;
            if stamp.settled_before(started) && rows_stamp.settled_before(started) {
                progress.passed.insert(name.into(), artifact.sha256.clone());
                progress.evidence.insert(
                    name.into(),
                    (artifact.sha256.clone(), artifact.bytes, stamp),
                );
            }
            checked.loaded += 1;
        }
        Ok(checked)
    }
}
/// Native material verified by this process, bounded to the last successful
/// inventory. Names and whole-file digests are derived again on every prepare.
#[derive(Clone, Debug, Default)]
pub struct CheckedRuntimes {
    pub(crate) context: Option<[String; 4]>,
    pub(crate) passed: BTreeMap<String, String>,
    /// Publisher-owned preparation; kept distinct from checks by this process.
    pub(crate) preparation: BTreeMap<String, (String, u64, FileIdentity)>,
    /// For runtimes the last successful inventory read in full: the digest,
    /// length and settled stamp of the exact inode, so an unchanged file is
    /// not hashed again. A changed or replaced file has another stamp.
    pub(crate) evidence: BTreeMap<String, (String, u64, FileIdentity)>,
    /// Native runtimes loaded and self-checked by the last attempt.
    pub loaded: usize,
    /// Native runtimes matching earlier checked bytes in the last attempt.
    pub reused: usize,
    /// Listed bytes the last attempt hashed, and those it listed from
    /// evidence of the same unchanged files without reading them.
    pub bytes_hashed: u64,
    pub bytes_reused: u64,
}

impl CheckedRuntimes {
    /// Drop completed checks whose immutable objects have been collected.
    pub fn retain_stored(&mut self, root: &Path) {
        self.evidence
            .retain(|_, (digest, _, stamp)| unchanged_stored(root, digest, *stamp));
        self.passed
            .retain(|name, _| self.evidence.contains_key(name));
    }

    /// Add runtimes another check of this process loaded and self-checked.
    pub fn adopt(&mut self, other: CheckedRuntimes) {
        self.passed.extend(other.passed);
    }
}

/// Why `artifact` could not be opened for hashing. No byte was read, so only a
/// path that is not a stored regular file is a confirmed mismatch. A read
/// error, or a hard link to the shared inode added or collected between
/// naming and opening it (which changes its ctime), leaves it unavailable.
pub(super) fn open_failure(artifact: &Artifact, error: &Error) -> IntegrityError {
    let unavailable = |error: &dyn std::fmt::Display| {
        IntegrityError::Unavailable(format!("{}: {error}", artifact.path))
    };
    if let Some(error) = error.downcast_ref::<io::Error>() {
        return unavailable(error);
    }
    match error.downcast_ref::<ObjectFileError>() {
        Some(error @ (ObjectFileError::Io(_) | ObjectFileError::FileChanged(_))) => {
            unavailable(error)
        }
        _ => IntegrityError::Mismatch {
            path: artifact.path.clone(),
            expected_sha256: artifact.sha256.clone(),
            found_sha256: None,
            reason: error.to_string(),
        },
    }
}

fn validation_context(inventory: &Inventory) -> [String; 4] {
    [
        inventory.network.clone(),
        inventory.genesis_hash.clone(),
        inventory.runtime.clone(),
        inventory.profile.clone(),
    ]
}

fn unchanged_stored(root: &Path, digest: &str, stamp: FileIdentity) -> bool {
    if !is_digest(digest) {
        return false;
    }
    let physical = root.join(OBJECT_STORE).join(digest);
    open_object_file(&physical, None).is_ok_and(|file| stamp.check(&physical, &file).is_ok())
}
