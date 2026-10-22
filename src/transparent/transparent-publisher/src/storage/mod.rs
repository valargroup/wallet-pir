//! Publisher-owned storage of table segments and prepared runtimes.
//!
//! A History publication repeats almost every table segment and runtime of
//! the one before it. Hard-linking those files into each candidate changes
//! their inode change time twice per cycle, once when linked and once when
//! the old candidate is collected, and the verification evidence bound to
//! that time never survives a cycle. Instead every segment and runtime is
//! stored once in [`OBJECT_STORE`] under the SHA-256 of its bytes, beside the
//! publications; candidates link them, so an unchanged object's inode never
//! changes while any publication still references it.
//!
//! Objects are installed by linking a complete, synced and verified file
//! under its final name, which fails rather than replaces an installed one,
//! and are never written in place. The loader and the artifact resolver
//! accept only the exact links written here.
pub mod layout;
pub mod preparation;
pub mod records;
mod retention;
pub(crate) use retention::collecting;
pub use retention::PublicationLease;

use pir_publication::is_object_digest as is_digest;
use pir_publication::{open_object_file, InstalledObject, ObjectFileError, ObjectInstaller};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use transparent_shared::history::shardset::OBJECT_STORE;
use transparent_shared::runtime::disk::ShippedRuntimes;
use zakura_pir_publisher::transparent::manifest::ShardManifest;

use crate::BoxError;

/// What one adoption did, for logs, the cycle record and tests.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct Adopted {
    /// Files installed as new objects, and the bytes hashed to install them.
    pub installed: u64,
    pub bytes_hashed: u64,
    /// Files whose object was already installed.
    pub shared: u64,
    /// Entries already linked.
    pub linked: u64,
}

/// Publication roots whose next object install a test interrupts, as a
/// process dying just before it would: the adoption ends with an error and
/// changes nothing more.
#[cfg(test)]
pub(crate) static INTERRUPT_INSTALL: std::sync::Mutex<BTreeSet<PathBuf>> =
    std::sync::Mutex::new(BTreeSet::new());

/// The store `name` beside `publication`, created as a real directory.
fn store(publication: &Path, name: &str) -> Result<PathBuf, BoxError> {
    let store = publication
        .parent()
        .ok_or("publication has no parent")?
        .join(name);
    match std::fs::create_dir(&store) {
        Ok(()) => sync_directory(store.parent().ok_or("store has no parent")?)?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    real(&store, true)?;
    Ok(store)
}

/// Refuses `path` unless it is a real directory or regular file.
fn real(path: &Path, dir: bool) -> Result<(), BoxError> {
    let meta = std::fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() || (if dir { !meta.is_dir() } else { !meta.is_file() }) {
        return Err(format!("{} is not a real store entry", path.display()).into());
    }
    Ok(())
}

fn sync_directory(dir: &Path) -> Result<(), BoxError> {
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Replaces `path` with a link to `target`, atomically.
fn replace_with_link(path: &Path, target: &Path) -> Result<(), BoxError> {
    let name = path
        .file_name()
        .ok_or("link has no name")?
        .to_string_lossy();
    let temporary = path.with_file_name(format!(".{name}.link"));
    let _ = std::fs::remove_file(&temporary);
    std::os::unix::fs::symlink(target, &temporary)?;
    std::fs::rename(&temporary, path)?;
    Ok(())
}

fn installed(
    result: Result<InstalledObject, ObjectFileError>,
) -> Result<InstalledObject, BoxError> {
    result.map_err(|error| match error {
        ObjectFileError::Cancelled => {
            Box::new(transparent_shared::publication::Stopped) as BoxError
        }
        error => Box::new(error),
    })
}

/// Installs the complete file `path` through the shared installer.
fn install(
    installer: &ObjectInstaller,
    path: &Path,
    digest: Option<&str>,
    stop: &dyn Fn() -> bool,
) -> Result<InstalledObject, BoxError> {
    #[cfg(test)]
    if let Some(root) = installer.directory().parent() {
        if INTERRUPT_INSTALL.lock().unwrap().remove(root) {
            return Err("interrupted before installing an object".into());
        }
    }
    installed(installer.install_prepared_file(path, digest, stop))
}

impl Adopted {
    fn record(&mut self, object: &InstalledObject) {
        self.bytes_hashed += object.bytes_hashed;
        if object.installed {
            self.installed += 1;
        } else {
            self.shared += 1;
        }
    }
}

/// An existing object can be referenced without another installation. This
/// checks only its storage entry, not its bytes: the candidate's subsequent
/// ShardSet load must verify them or reuse its exact-file evidence before any
/// preparation can succeed. In particular, this never changes inode metadata.
fn can_reference(installer: &ObjectInstaller, digest: &str) -> Result<bool, BoxError> {
    let object = installer.directory().join(digest);
    match open_object_file(&object, None) {
        Ok(_) => Ok(true),
        Err(ObjectFileError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

/// Every table segment file of a revision directory, with its digest.
fn segments(dir: &Path) -> Result<Vec<(String, String)>, BoxError> {
    let raw = std::fs::read(dir.join("manifest.json"))?;
    let value: serde_json::Value = serde_json::from_slice(&raw)?;
    if value.get("buckets").is_some() {
        let manifest: zakura_pir_publisher::transparent::display::DisplayManifest =
            serde_json::from_value(value)?;
        manifest.validate()?;
        return Ok(manifest
            .tables()
            .into_iter()
            .flat_map(|table| {
                manifest
                    .segments(table)
                    .unwrap_or(&[])
                    .iter()
                    .enumerate()
                    .map(move |(index, segment)| (table.file_name(index), segment.sha256.clone()))
            })
            .collect());
    }
    let manifest: ShardManifest = serde_json::from_value(value)?;
    let mut files = Vec::new();
    for (index, segment) in manifest.directory_segments.iter().enumerate() {
        files.push((format!("directory.{index}.bin"), segment.sha256.clone()));
    }
    for child in &manifest.page_children {
        for (segment, published) in child.page_segments.iter().enumerate() {
            files.push((
                page_segment_file(child.index, segment),
                published.sha256.clone(),
            ));
        }
    }
    Ok(files)
}

/// Install immutable bytes once and reference them from a publication. Existing
/// objects are verified by the caller's subsequent preparation, never trusted
/// from their name. Reference changes do not change an object's inode metadata.
pub fn write_object(root: &Path, path: &Path, bytes: &[u8]) -> Result<(), BoxError> {
    let reference = pir_publication::ObjectReference::for_bytes(bytes);
    let installer = ObjectInstaller::open(root.join(OBJECT_STORE))?;
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        let parent = path.parent().ok_or("object reference has no parent")?;
        let relative = parent.strip_prefix(root)?;
        if relative
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
        {
            return Err("object reference is outside its storage root".into());
        }
        let mut expected_target = PathBuf::new();
        for _ in relative.components() {
            expected_target.push("..");
        }
        expected_target.push(OBJECT_STORE);
        expected_target.push(&reference.sha256);
        let physical = if metadata.file_type().is_symlink() {
            if std::fs::read_link(path)? != expected_target {
                return Err("existing object reference points at different bytes".into());
            }
            root.join(OBJECT_STORE).join(&reference.sha256)
        } else {
            path.to_path_buf()
        };
        let mut file = open_object_file(&physical, Some(reference.bytes))?;
        pir_publication::hash_object_file(&physical, &mut file, Some(&reference), &|| false)?;
        if metadata.file_type().is_symlink() {
            return Ok(());
        }
        if !can_reference(&installer, &reference.sha256)? {
            install(&installer, path, Some(&reference.sha256), &|| false)?;
        }
        replace_with_link(path, &expected_target)?;
        sync_directory(parent)?;
        return Ok(());
    }
    if !can_reference(&installer, &reference.sha256)? {
        installed(installer.install_stream(bytes, &|| false))?;
    }
    reference_object(root, path, &reference)
}

/// Create an exact relative reference, rejecting a conflicting named entry.
/// The shared artifact resolver checks it again when reading or serving.
pub fn reference_object(
    root: &Path,
    path: &Path,
    reference: &pir_publication::ObjectReference,
) -> Result<(), BoxError> {
    reference.validate(u64::MAX)?;
    let parent = path.parent().ok_or("object reference has no parent")?;
    let relative = parent.strip_prefix(root)?;
    if relative
        .components()
        .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err("object reference is outside the publication root".into());
    }
    let mut target = PathBuf::new();
    for _ in relative.components() {
        target.push("..");
    }
    target.push(OBJECT_STORE);
    target.push(&reference.sha256);
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() && std::fs::read_link(path)? == target => {
            return Ok(())
        }
        Ok(_) => {
            return Err(format!("{} already has different object ownership", path.display()).into())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    open_object_file(
        &root.join(OBJECT_STORE).join(&reference.sha256),
        Some(reference.bytes),
    )?;
    replace_with_link(path, &target)?;
    sync_directory(parent)?;
    Ok(())
}

/// Moves every table segment of the revisions in `output` into the object
/// store and links it from there. New objects pass the shared installer;
/// already stored objects become references without another hard link or hash.
/// The later publisher preparation must reuse or establish their verification
/// evidence before succeeding. Existing links stay exact. Metadata stays local.
pub fn adopt_segments(output: &Path, stop: &dyn Fn() -> bool) -> Result<Adopted, BoxError> {
    let objects = store(output, OBJECT_STORE)?;
    let installer = ObjectInstaller::open(objects)?;
    let mut adopted = Adopted::default();
    for entry in std::fs::read_dir(output)? {
        if stop() {
            return Err(transparent_shared::publication::Stopped.into());
        }
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str().filter(|name| is_digest(name)) else {
            continue;
        };
        if !entry.file_type()?.is_dir() {
            return Err(format!("revision {name} is not a real directory").into());
        }
        let dir = entry.path();
        for (file, sha256) in segments(&dir)? {
            if !is_digest(&sha256) {
                return Err(format!("revision {name} names a malformed segment digest").into());
            }
            let path = dir.join(&file);
            let target = Path::new("../..").join(OBJECT_STORE).join(&sha256);
            let meta = std::fs::symlink_metadata(&path)?;
            if meta.file_type().is_symlink() {
                if std::fs::read_link(&path)? != target {
                    return Err(format!("{} links outside its object", path.display()).into());
                }
                adopted.linked += 1;
                continue;
            }
            if !meta.is_file() {
                return Err(format!("{} is not a regular file", path.display()).into());
            }
            if can_reference(&installer, &sha256)? {
                adopted.shared += 1;
            } else {
                let object = install(&installer, &path, Some(&sha256), stop)?;
                adopted.record(&object);
            }
            replace_with_link(&path, &target)?;
        }
        sync_directory(&dir)?;
    }
    Ok(adopted)
}

/// Moves every prepared runtime of `directory` into the object store and
/// links it from there as `../.objects/<sha256>`. A runtime new to the store is
/// hashed and installed; one whose object is installed is linked to it, and
/// the inventory hashes or vouches for that object like any other.
pub fn adopt_runtimes(directory: &Path, stop: &dyn Fn() -> bool) -> Result<Adopted, BoxError> {
    let objects = store(directory, OBJECT_STORE)?;
    let installer = ObjectInstaller::open(objects)?;
    let mut adopted = Adopted::default();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if !entry
            .file_name()
            .to_str()
            .is_some_and(ShippedRuntimes::is_entry)
        {
            continue;
        }
        let path = entry.path();
        let kind = entry.file_type()?;
        if kind.is_symlink() {
            let target = std::fs::read_link(&path)?;
            let stored = target
                .strip_prefix(Path::new("..").join(OBJECT_STORE))
                .ok()
                .and_then(|name| name.to_str())
                .is_some_and(is_digest);
            if !stored {
                return Err(format!("{} links outside the object store", path.display()).into());
            }
            adopted.linked += 1;
            continue;
        }
        if !kind.is_file() {
            return Err(format!("{} is not a regular file", path.display()).into());
        }
        let object = install(&installer, &path, None, stop)?;
        adopted.record(&object);
        replace_with_link(
            &path,
            &Path::new("..")
                .join(OBJECT_STORE)
                .join(&object.reference.sha256),
        )?;
    }
    sync_directory(directory)?;
    Ok(adopted)
}

/// Import split-index chunks using their declared content names. Interrupted
/// imports are resumable: completed references are checked and left untouched.
pub fn adopt_indexes(directory: &Path, stop: &dyn Fn() -> bool) -> Result<Adopted, BoxError> {
    let objects = store(directory, OBJECT_STORE)?;
    let installer = ObjectInstaller::open(objects)?;
    let mut report = Adopted::default();
    for entry in std::fs::read_dir(directory)? {
        if stop() {
            return Err(transparent_shared::publication::Stopped.into());
        }
        let entry = entry?;
        let name = entry.file_name();
        let Some(digest) = name
            .to_str()
            .and_then(|name| {
                name.strip_prefix("txid-index-")
                    .unwrap_or(name)
                    .strip_suffix(".json")
            })
            .filter(|digest| is_digest(digest))
        else {
            continue;
        };
        let target = Path::new("..").join(OBJECT_STORE).join(digest);
        if entry.file_type()?.is_symlink() {
            if std::fs::read_link(entry.path())? != target {
                return Err("index reference leaves its object store".into());
            }
            report.linked += 1;
        } else {
            let object = install(&installer, &entry.path(), Some(digest), stop)?;
            report.record(&object);
            replace_with_link(&entry.path(), &target)?;
        }
    }
    sync_directory(directory)?;
    Ok(report)
}

/// What [`collect`] removed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Collected {
    pub objects: u64,
}

/// Removes stored objects no publication directory under
/// `root` links. Every directory there counts, whatever its role (active,
/// candidate, activation intent, held by validation), so the caller removes
/// unreferenced publications first. A file being read keeps its bytes until
/// it is closed. Any unreadable publication stops the collection.
pub fn collect(root: &Path) -> Result<Collected, BoxError> {
    let mut objects = BTreeSet::new();
    let name_of = |link: &Path| -> Result<String, BoxError> {
        Ok(std::fs::read_link(link)?
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("malformed store link")?
            .to_owned())
    };
    for publication in std::fs::read_dir(root)? {
        let publication = publication?;
        if publication.file_name().to_string_lossy().starts_with('.')
            || !publication.file_type()?.is_dir()
        {
            continue;
        }
        for entry in std::fs::read_dir(publication.path())? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                objects.insert(name_of(&entry.path())?);
            } else if kind.is_dir() {
                for file in std::fs::read_dir(entry.path())? {
                    let file = file?;
                    if file.file_type()?.is_symlink() {
                        objects.insert(name_of(&file.path())?);
                    }
                }
            }
        }
    }
    let mut collected = Collected::default();
    let store = root.join(OBJECT_STORE);
    match std::fs::symlink_metadata(&store) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(collected),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    real(&store, true)?;
    for entry in std::fs::read_dir(&store)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !objects.contains(&name) {
            std::fs::remove_file(entry.path())?;
            collected.objects += 1;
        }
    }
    Ok(collected)
}

#[cfg(all(test, feature = "fixtures"))]
mod tests;

/// The file one segment of page shard `child` is published under.
pub fn page_segment_file(child: u32, segment: usize) -> String {
    format!(
        "{}.{segment}.bin",
        zakura_pir_publisher::transparent::manifest::pages_table(child)
    )
}

/// Reference one immutable revision in another publication, preserving object
/// aliases. Ordinary metadata may use a hard link or a small copy. Loading the
/// candidate still verifies every reference before publication.
pub fn link_revision(from: &Path, to: &Path, digest: &str) -> Result<(), BoxError> {
    if from == to {
        return Ok(());
    }
    let target = to.join(digest);
    std::fs::create_dir_all(&target)?;
    for entry in std::fs::read_dir(from.join(digest))? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if !kind.is_file() && !kind.is_symlink() {
            return Err("unexpected revision artifact".into());
        }
        let dest = target.join(entry.file_name());
        if std::fs::symlink_metadata(&dest).is_ok() {
            continue;
        }
        if link_file(&entry.path(), &dest).is_err() {
            if kind.is_symlink() {
                return Err("could not link a stored revision segment".into());
            }
            std::fs::copy(entry.path(), &dest)?;
        }
    }
    Ok(())
}

/// Gives `to` the file `from` names. A link into the publisher's segment
/// store is linked again, never followed, so republishing a stored segment
/// leaves its object untouched; the loader checks the link is exact.
pub(crate) fn link_file(from: &Path, to: &Path) -> std::io::Result<()> {
    if std::fs::symlink_metadata(from)?.file_type().is_symlink() {
        return std::os::unix::fs::symlink(std::fs::read_link(from)?, to);
    }
    std::fs::hard_link(from, to)
}
