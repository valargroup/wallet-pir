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
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use transparent_shared::history::shardset::OBJECT_STORE;
use transparent_shared::runtime::disk::ShippedRuntimes;
use zakura_pir_publisher::transparent::manifest::ShardManifest;

use crate::history::publication::{page_segment_file, BoxError};

/// What one adoption did, for logs and tests.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Adopted {
    /// Files installed as new objects, and the bytes hashed to install them.
    pub installed: u64,
    pub bytes_hashed: u64,
    /// Files whose object was already installed.
    pub shared: u64,
    /// Entries already linked.
    pub linked: u64,
}

fn is_digest(name: &str) -> bool {
    transparent_shared::publication::is_digest(name)
}

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

/// Installs the complete file `path` as `object`, unless one is installed.
/// Returns whether it installed it.
fn install(path: &Path, object: &Path) -> Result<bool, BoxError> {
    std::fs::File::open(path)?.sync_all()?;
    match std::fs::hard_link(path, object) {
        Ok(()) => {
            sync_directory(object.parent().ok_or("object has no parent")?)?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// SHA-256 of `path`, asking `stop` before each bounded read.
fn sha256_file(path: &Path, stop: &dyn Fn() -> bool) -> Result<(String, u64), BoxError> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0u8; 1 << 20];
    let mut total = 0u64;
    loop {
        if stop() {
            return Err(Box::new(transparent_shared::publication::Stopped));
        }
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total += read as u64;
        hash.update(&buffer[..read]);
    }
    Ok((hex::encode(hash.finalize()), total))
}

/// Every table segment file of a revision directory, with its digest.
fn segments(dir: &Path) -> Result<Vec<(String, String)>, BoxError> {
    let manifest: ShardManifest =
        serde_json::from_slice(&std::fs::read(dir.join("manifest.json"))?)?;
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

/// Moves every table segment of the revisions in `output` into the object
/// store and links it from there. A segment whose object is installed is
/// linked without being read; any other is hashed against its manifest
/// first, so the store never holds an object under another digest. Existing
/// links must already be exact. Revision metadata stays local.
pub fn adopt_segments(output: &Path, stop: &dyn Fn() -> bool) -> Result<Adopted, BoxError> {
    let objects = store(output, OBJECT_STORE)?;
    let mut adopted = Adopted::default();
    for entry in std::fs::read_dir(output)? {
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
            let object = objects.join(&sha256);
            if std::fs::symlink_metadata(&object).is_ok() {
                real(&object, false)?;
                adopted.shared += 1;
            } else {
                let (found, bytes) = sha256_file(&path, stop)?;
                if found != sha256 {
                    return Err(format!("{} does not match its manifest", path.display()).into());
                }
                adopted.bytes_hashed += bytes;
                if install(&path, &object)? {
                    adopted.installed += 1;
                } else {
                    adopted.shared += 1;
                }
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
        let (sha256, bytes) = sha256_file(&path, stop)?;
        adopted.bytes_hashed += bytes;
        let object = objects.join(&sha256);
        if install(&path, &object)? {
            adopted.installed += 1;
        } else {
            real(&object, false)?;
            adopted.shared += 1;
        }
        replace_with_link(&path, &Path::new("..").join(OBJECT_STORE).join(&sha256))?;
    }
    sync_directory(directory)?;
    Ok(adopted)
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
    if std::fs::symlink_metadata(&store).is_err() {
        return Ok(collected);
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
