//! Clone completed publications into a separate, owned storage root.
//!
//! Source paths and journals are never rewritten. Reflinks give the destination
//! independent inodes: predecessor hard-link churn cannot invalidate its file
//! evidence. Product verification, not a successful clone, completes preparation.
use crate::{storage::records::write_durable, BoxError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io,
    path::{Component, Path, PathBuf},
};

pub const OWNER: &str = "storage-preparation.json";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ownership {
    pub schema: String,
    pub product: String,
    pub source: PathBuf,
    pub target: PathBuf,
}

pub fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Open only a sibling target created for this exact source and product.
/// The controller lock also excludes a publisher that has started on the target.
pub fn open(source: &Path, target: &Path, product: &str) -> Result<(Ownership, File), BoxError> {
    let source = std::fs::canonicalize(source)?;
    let parent = std::fs::canonicalize(target.parent().ok_or("target has no parent")?)?;
    let name = target.file_name().ok_or("target has no name")?;
    let target = parent.join(name);
    if source.parent() != Some(parent.as_path()) || source == target || target.is_symlink() {
        return Err("prepared storage must be a separate real sibling of its source".into());
    }
    let owner = Ownership {
        schema: "transparent-storage-preparation/1".into(),
        product: product.into(),
        source,
        target,
    };
    match std::fs::create_dir(&owner.target) {
        Ok(()) => write_durable(&owner.target.join(OWNER), &serde_json::to_vec(&owner)?)?,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let previous: Ownership =
                serde_json::from_slice(&std::fs::read(owner.target.join(OWNER))?)?;
            if previous != owner {
                return Err("preparation target has another owner".into());
            }
        }
        Err(error) => return Err(error.into()),
    }
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(owner.target.join("controller.lock"))?;
    lock.try_lock()
        .map_err(|_| "preparation target is in use")?;
    Ok((owner, lock))
}

#[cfg(target_os = "linux")]
fn reflink(source: &File, destination: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: Both handles are owned, regular files. No write or hard-link fallback:
    // unsupported cloning must not silently allocate a second large dataset.
    let result = unsafe { libc::ioctl(destination.as_raw_fd(), libc::FICLONE, source.as_raw_fd()) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
#[cfg(not(target_os = "linux"))]
fn reflink(_: &File, _: &File) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "storage preparation requires Linux FICLONE",
    ))
}

/// Clone a regular source into a new file, then atomically expose it. On resume,
/// compare existing bytes instead of overwriting a conflicting destination.
pub struct Cloner(fn(&File, &File) -> io::Result<()>);
impl Default for Cloner {
    fn default() -> Self {
        Self(reflink)
    }
}
impl Cloner {
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self(|source, target| {
            io::copy(&mut source.try_clone()?, &mut target.try_clone()?).map(|_| ())
        })
    }
    pub fn file(&self, source: &Path, target: &Path) -> Result<(), BoxError> {
        clone_with(source, target, self.0)
    }
}

fn clone_with(
    source: &Path,
    target: &Path,
    clone: impl Fn(&File, &File) -> io::Result<()>,
) -> Result<(), BoxError> {
    let metadata = std::fs::symlink_metadata(source)?;
    if !metadata.is_file() {
        return Err("legacy snapshot contains a non-regular file".into());
    }
    let input = pir_publication::open_object_file(source, Some(metadata.len()))?;
    let identity = pir_publication::FileIdentity::capture(source, &input)?;
    if target.exists() || target.is_symlink() {
        let mut destination = pir_publication::open_object_file(target, Some(metadata.len()))?;
        let mut input = input;
        let (expected, _) = pir_publication::hash_object_file(source, &mut input, None, &|| false)?;
        pir_publication::hash_object_file(target, &mut destination, Some(&expected), &|| false)?;
        return Ok(());
    }
    let parent = target.parent().ok_or("clone has no parent")?;
    std::fs::create_dir_all(parent)?;
    let temporary = tempfile::NamedTempFile::new_in(parent)?;
    clone(&input, temporary.as_file())
        .map_err(|error| format!("reflink required for {}: {error}", source.display()))?;
    temporary.as_file().sync_all()?;
    identity.check(source, &input)?;
    temporary
        .persist_noclobber(target)
        .map_err(|error| error.error)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

/// Clone a publication's regular files and checked object references. Arbitrary
/// links and special files are refused by the shared artifact resolver.
impl Cloner {
    pub fn tree(&self, source: &Path, target: &Path, objects: &Path) -> Result<(), BoxError> {
        self.tree_from(source, source, target, objects)
    }

    /// Clone a subtree using the publication root that owns its exact links.
    pub(crate) fn tree_from(
        &self,
        publication: &Path,
        source: &Path,
        target: &Path,
        objects: &Path,
    ) -> Result<(), BoxError> {
        if !std::fs::symlink_metadata(source)?.is_dir() || target.is_symlink() {
            return Err("snapshot needs real directories".into());
        }
        std::fs::create_dir_all(target)?;
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            let name = entry.file_name();
            if name.to_string_lossy().ends_with(".partial") {
                continue;
            }
            let destination = target.join(name);
            if entry.file_type()?.is_dir() {
                self.tree_from(publication, &entry.path(), &destination, objects)?;
                continue;
            }
            let logical = entry.path();
            let relative = logical
                .strip_prefix(publication)?
                .to_str()
                .ok_or("non-UTF8 source artifact")?;
            let (physical_source, mut input) =
                transparent_shared::publication::open_artifact(publication, relative, None)?;
            if destination.is_symlink() {
                let physical = std::fs::canonicalize(&destination)?;
                if physical.parent() != Some(objects) {
                    return Err("resumed reference leaves the prepared object store".into());
                }
                let (reference, _) =
                    pir_publication::hash_object_file(&physical_source, &mut input, None, &|| {
                        false
                    })?;
                if physical.file_name().and_then(|n| n.to_str()) != Some(reference.sha256.as_str())
                {
                    return Err("resumed object differs from source material".into());
                }
                let mut file = pir_publication::open_object_file(&physical, Some(reference.bytes))?;
                pir_publication::hash_object_file(&physical, &mut file, Some(&reference), &|| {
                    false
                })?;
            } else {
                self.file(&physical_source, &destination)?;
            }
        }
        File::open(target)?.sync_all()?;
        Ok(())
    }
}

/// Exact local identities of the prepared tree, used only to detect drift
/// between offline verification and handoff. They grant no HTTP authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    identity: (u64, u64, u64, i64, i64, i64, i64),
    link: Option<PathBuf>,
}

pub fn fingerprint(root: &Path) -> Result<std::collections::BTreeMap<PathBuf, Entry>, BoxError> {
    fn walk(
        root: &Path,
        directory: &Path,
        entries: &mut std::collections::BTreeMap<PathBuf, Entry>,
    ) -> Result<(), BoxError> {
        use std::os::unix::fs::MetadataExt;
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            let relative = path.strip_prefix(root)?.to_path_buf();
            if relative == Path::new("controller.lock")
                || relative == Path::new("prepared-storage.json")
            {
                continue;
            }
            let metadata = std::fs::symlink_metadata(&path)?;
            if metadata.is_dir() {
                walk(root, &path, entries)?;
            } else if metadata.is_file() || metadata.file_type().is_symlink() {
                entries.insert(
                    relative,
                    Entry {
                        identity: (
                            metadata.dev(),
                            metadata.ino(),
                            metadata.len(),
                            metadata.mtime(),
                            metadata.mtime_nsec(),
                            metadata.ctime(),
                            metadata.ctime_nsec(),
                        ),
                        link: if metadata.file_type().is_symlink() {
                            Some(std::fs::read_link(path)?)
                        } else {
                            None
                        },
                    },
                );
            } else {
                return Err("prepared tree contains a special file".into());
            }
        }
        Ok(())
    }
    let mut result = std::collections::BTreeMap::new();
    walk(root, root, &mut result)?;
    Ok(result)
}

/// Translate recorded absolute directories without accepting traversal or an
/// out-of-root predecessor reference.
pub fn destination(owner: &Ownership, source: &Path) -> Result<PathBuf, BoxError> {
    let relative = source.strip_prefix(&owner.source)?;
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err("publication is outside its source storage root".into());
    }
    Ok(owner.target.join(relative))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn copy(source: &File, target: &File) -> io::Result<()> {
        io::copy(&mut source.try_clone()?, &mut target.try_clone()?).map(|_| ())
    }
    #[test]
    fn cloning_refuses_arbitrary_source_links_without_changing_their_targets() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir(&source).unwrap();
        let outside = temp.path().join("journal");
        std::fs::write(&outside, b"committed bytes").unwrap();
        std::os::unix::fs::symlink(&outside, source.join("shards.json")).unwrap();
        let target = temp.path().join("target");
        assert!(Cloner::for_test()
            .tree(&source, &target, &temp.path().join(".objects"))
            .is_err());
        assert_eq!(std::fs::read(&outside).unwrap(), b"committed bytes");
        assert!(!target.join("shards.json").exists());
    }
    #[test]
    fn cloning_is_independent_and_a_conflicting_resume_never_overwrites() {
        use std::os::unix::fs::MetadataExt;
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let target = temp.path().join("target");
        std::fs::write(&source, b"published material").unwrap();
        clone_with(&source, &target, copy).unwrap();
        assert_ne!(
            std::fs::metadata(&source).unwrap().ino(),
            std::fs::metadata(&target).unwrap().ino()
        );
        clone_with(&source, &target, copy).unwrap();
        std::fs::write(&source, b"changed source now").unwrap();
        assert!(clone_with(&source, &target, copy).is_err());
        assert_eq!(std::fs::read(target).unwrap(), b"published material");
    }
    #[test]
    fn failed_cloning_and_foreign_targets_leave_the_source_untouched() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("body"), b"bytes").unwrap();
        let target = temp.path().join("prepared");
        let fail =
            |_: &File, _: &File| Err(io::Error::new(io::ErrorKind::Unsupported, "no reflinks"));
        assert!(clone_with(&source.join("body"), &target, fail).is_err());
        assert!(!target.exists());
        let (_owner, lock) = open(&source, &target, "history").unwrap();
        assert!(open(&source, &target, "history").is_err());
        drop(lock);
        assert!(open(&source, &target, "transaction_details").is_err());
        assert_eq!(std::fs::read(source.join("body")).unwrap(), b"bytes");
    }
    #[test]
    fn changed_during_clone_never_exposes_partial_material() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let target = temp.path().join("target");
        std::fs::write(&source, b"first bytes").unwrap();
        let change = |input: &File, output: &File| {
            copy(input, output)?;
            std::fs::write(&source, b"changed bytes")
        };
        assert!(clone_with(&source, &target, change).is_err());
        assert_eq!(std::fs::read(&source).unwrap(), b"changed bytes");
        assert!(!target.exists());
    }
}
