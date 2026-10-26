//! Artifact path resolution, listing and file verification.
use super::*;

pub fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
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

/// Open the publication artifact `relative` names under `root`, returning
/// its checked physical path and the handle acquired at that name.
///
/// Every component must be a real directory or file except exact references
/// into the real [`OBJECT_STORE`] beside the publication: a runtime or split
/// index file directly under `root` may link to `../.objects/<sha256>`, and
/// a table segment of a revision directory to `../../.objects/<sha256>`. Traversal, any other
/// symbolic link, a redirect elsewhere, a linked store and a missing or
/// non-regular target are refused. Publication paths stay logical; only reads
/// use the returned handle. A segment link's digest is checked against its
/// manifest by the loader and by the digest listed for it.
/// This grants no verification evidence or distribution authority.
/// An expected reference binds length and, for stored aliases, the target name.
pub fn open_artifact(
    root: &Path,
    relative: &str,
    expected: Option<&ObjectReference>,
) -> Result<(PathBuf, std::fs::File), Error> {
    let (physical, file) = resolve(root, relative)?;
    if let Some(expected) = expected {
        expected.validate(u64::MAX)?;
        if file.metadata()?.len() != expected.bytes {
            return Err("publication object length differs from its reference".into());
        }
        if physical
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == OBJECT_STORE)
            && physical.file_name().and_then(|name| name.to_str()) != Some(expected.sha256.as_str())
        {
            return Err("publication object target differs from its reference".into());
        }
    }
    Ok((physical, file))
}

pub(crate) fn resolve(root: &Path, relative: &str) -> Result<(PathBuf, std::fs::File), Error> {
    if !safe_path(relative) {
        return Err("unsafe publisher artifact path".into());
    }
    let components: Vec<&str> = relative.split('/').collect();
    let base = root.parent().ok_or("publication has no parent")?;
    let mut physical = root.to_path_buf();
    for (index, component) in components.iter().enumerate() {
        physical.push(component);
        let last = index + 1 == components.len();
        if !std::fs::symlink_metadata(&physical)?
            .file_type()
            .is_symlink()
        {
            continue;
        }
        let target = std::fs::read_link(&physical)?;
        let prefix = if last
            && index == 0
            && (ShippedRuntimes::is_entry(component)
                || component
                    .strip_prefix("txid-index-")
                    .and_then(|name| name.strip_suffix(".json"))
                    .is_some_and(is_digest))
        {
            Path::new("..").join(OBJECT_STORE)
        } else if last && index == 1 && is_digest(components[0]) && *component != "filter.bin" {
            Path::new("../..").join(OBJECT_STORE)
        } else {
            return Err("symlink in publisher artifact".into());
        };
        let name = target
            .strip_prefix(prefix)
            .ok()
            .and_then(|name| name.to_str())
            .filter(|name| is_digest(name))
            .ok_or("publisher artifact link leaves the object store")?
            .to_string();
        let store = base.join(OBJECT_STORE);
        physical = store.join(name);
    }
    let file = open_object_file(&physical, None)?;
    Ok((physical, file))
}

/// [`resolve`] for an artifact given by its logical path under `root`.
pub(super) fn resolve_path(root: &Path, logical: &Path) -> Result<(PathBuf, std::fs::File), Error> {
    let relative = logical
        .strip_prefix(root)?
        .to_str()
        .ok_or("non-UTF8 artifact")?;
    resolve(root, relative)
}

/// The digest and length to list for an unchanged file, with explicit provenance.
pub(crate) struct Known {
    pub(super) sha256: String,
    pub(super) bytes: u64,
    pub(super) stamp: FileIdentity,
    pub(super) prepared: bool,
}
impl Known {
    pub(crate) fn from_evidence(sha256: String, bytes: u64, stamp: FileIdentity) -> Self {
        Self {
            sha256,
            bytes,
            stamp,
            prepared: false,
        }
    }
    /// A publisher's saved preparation of an unchanged file: listed without a
    /// read, and never this process's evidence.
    pub(crate) fn from_preparation(sha256: String, bytes: u64, stamp: FileIdentity) -> Self {
        Self {
            sha256,
            bytes,
            stamp,
            prepared: true,
        }
    }
    /// A source's process-local verification or explicitly inherited publisher
    /// preparation. Received and ordinary size-only sources carry neither.
    pub(crate) fn of(source: &crate::history::shardset::SegmentSource) -> Option<Self> {
        source.preparation_identity().map(|stamp| Self {
            sha256: source.sha256().to_owned(),
            bytes: source.bytes() as u64,
            stamp,
            prepared: source.evidence().is_none(),
        })
    }
}

/// One artifact [`hash_listed`] listed.
#[derive(Debug)]
pub(crate) struct Listed {
    pub(crate) artifact: Artifact,
    /// The logical path under the publication root.
    pub(crate) path: PathBuf,
    /// The checked file the bytes were read from.
    pub(crate) physical: PathBuf,
    /// The open runtime handle, for a self-check of the inode just listed.
    pub(crate) opened: Option<std::fs::File>,
    /// The stamp of the inode listed, unchanged throughout the listing.
    pub(crate) stamp: FileIdentity,
    /// Whether `stamp` predates the listing enough to vouch for it later.
    pub(crate) settled: bool,
    /// Whether the bytes were read and hashed.
    pub(crate) read: bool,
    pub(crate) prepared: bool,
}

/// [`hash_listed`] without evidence or a stop: every file is hashed.
#[cfg(test)]
pub(super) fn hash_artifacts(
    root: &Path,
    pending: Vec<(PathBuf, Kind)>,
) -> Result<Vec<(Artifact, PathBuf, Option<std::fs::File>)>, Error> {
    let pending = pending
        .into_iter()
        .map(|(path, kind)| (path, kind, None))
        .collect();
    Ok(hash_listed(root, pending, &|| false)?
        .into_iter()
        .map(|listed| (listed.artifact, listed.path, listed.opened))
        .collect())
}

/// Hash each pending artifact, listing a file with its `Known` digest when it is
/// unchanged since verified, and ending with [`Stopped`] once `stop` answers
/// true before a file or any bounded read of one.
pub(crate) fn hash_listed(
    root: &Path,
    pending: Vec<(PathBuf, Kind, Option<Known>)>,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Vec<Listed>, Error> {
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
                let (physical, mut file) = resolve(root, &relative)?;
                // Publisher preparation never enters the process-local verified-file API.
                let prepared = known.as_ref().is_some_and(|k| {
                    k.prepared
                        && file.metadata().is_ok_and(|m| m.len() == k.bytes)
                        && k.stamp.check(&physical, &file).is_ok()
                });
                let listing = if prepared {
                    let known = known.as_ref().unwrap();
                    Listing {
                        sha256: known.sha256.clone(),
                        bytes: known.bytes,
                        stamp: known.stamp,
                        settled: true,
                        read: false,
                    }
                } else {
                    let verified = known
                        .as_ref()
                        .filter(|k| !k.prepared)
                        .map(|k| (k.sha256.as_str(), k.bytes, k.stamp));
                    listed_digest(&physical, &mut file, verified, stop)?
                };
                let opened =
                    matches!(kind, Kind::RecentRuntime | Kind::ArchiveRuntime).then_some(file);
                Ok(Listed {
                    artifact: Artifact {
                        path: relative,
                        sha256: listing.sha256,
                        bytes: listing.bytes,
                        kind,
                    },
                    path,
                    physical,
                    opened,
                    stamp: listing.stamp,
                    settled: listing.settled,
                    read: listing.read,
                    prepared,
                })
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
/// A file that changed while it was read, or was not the one its path names,
/// is an error, never a listed digest. Also returned: the stamp of the file
/// listed, and whether it is settled enough to vouch for the file later, as
/// it is when it predates the read by more than any timestamp granularity.
pub(super) fn listed_digest(
    path: &Path,
    file: &mut std::fs::File,
    known: Option<(&str, u64, FileIdentity)>,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<Listing, Error> {
    let reference = known.as_ref().map(|(sha256, bytes, _)| ObjectReference {
        sha256: (*sha256).to_owned(),
        bytes: *bytes,
    });
    let prior = known
        .as_ref()
        .zip(reference.as_ref())
        .map(|((_, _, stamp), reference)| (reference, *stamp));
    let verified =
        verify_object_file(path, file, None, prior, stop).map_err(|error| match error {
            ObjectFileError::Cancelled => Box::new(Stopped) as Error,
            error => Box::new(error),
        })?;
    #[cfg(test)]
    if !verified.reused {
        LISTED_HASHED.lock().unwrap().push(path.to_path_buf());
    }
    Ok(Listing {
        sha256: verified.reference.sha256,
        bytes: verified.reference.bytes,
        stamp: verified.identity,
        settled: verified.reusable,
        read: !verified.reused,
    })
}

/// What [`listed_digest`] found.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Listing {
    pub(super) sha256: String,
    pub(super) bytes: u64,
    pub(super) stamp: FileIdentity,
    pub(super) settled: bool,
    /// Whether the bytes were read, rather than vouched for by `known`.
    pub(super) read: bool,
}

/// Read and hash the artifact at `relative` under `root` on the calling
/// thread, asking `stop` before each bounded read; its digest and length.
pub(crate) fn hash_until(
    root: &Path,
    relative: &str,
    stop: &(dyn Fn() -> bool + Sync),
) -> Result<(String, u64), Error> {
    let (physical, mut file) = resolve(root, relative)?;
    let listing = listed_digest(&physical, &mut file, None, stop)?;
    Ok((listing.sha256, listing.bytes))
}

pub fn verify_file(path: &Path, expected: &Artifact) -> Result<(), Error> {
    open_object_file(path, Some(expected.bytes))?;
    Ok(())
}

/// The caller has verified the network stream, or linked/copied its completed
/// cache object. Persist its download identity after checking the local size.
pub fn record_received_file(path: &Path, expected: &Artifact, object: &Path) -> Result<(), Error> {
    crate::verification::received(path, &expected.sha256, expected.bytes, object)?;
    Ok(())
}
