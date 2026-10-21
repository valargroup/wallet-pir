//! Verified objects and stable revision directories shared by both HTTPS consumers.
use crate::source::Source;
use tokio::io::AsyncWriteExt;
use transparent_shared::publication::{self, Error};

pub(crate) const PARTIAL: &str = ".partial-";

/// Create or open `<directory>/objects`, refuse anything but a real directory
/// before enumerating it, and remove interrupted partials. This directory has
/// one owner; an interrupted object is never reused.
pub(crate) fn prepare_objects(directory: &std::path::Path) -> Result<std::path::PathBuf, Error> {
    let objects = directory.join("objects");
    std::fs::create_dir_all(&objects)?;
    if !std::fs::symlink_metadata(&objects)?.is_dir() {
        return Err("object cache must be a real directory".into());
    }
    remove_partials(&objects)?;
    Ok(objects)
}

pub(crate) fn remove_partials(objects: &std::path::Path) -> Result<(), Error> {
    let entries = match std::fs::read_dir(objects) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    for entry in entries {
        let entry = entry?;
        // Includes the previous `<sha256>.partial` copy name.
        if entry.file_name().to_string_lossy().contains(".partial") {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// Identity of a read-only cached object at the time its body was hashed.
/// Any write, truncation, permission change or replacement changes the inode,
/// size, mtime or ctime, so an equal identity means the checked bytes remain.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Checked {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
}
impl Checked {
    fn of(metadata: &std::fs::Metadata) -> Option<Self> {
        use std::os::unix::fs::MetadataExt;
        (metadata.is_file() && metadata.permissions().readonly()).then(|| Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
            len: metadata.len(),
            mtime: (metadata.mtime(), metadata.mtime_nsec()),
            ctime: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
}

/// Make `objects/<sha256>` a complete, verified, read-only object and return
/// whether its body was downloaded. A reused regular file is made read-only,
/// hashed and synced before its first stage through this source handle, so a
/// legacy writable object is never linked writable; later stages skip the
/// body hash only while the open file keeps the identity recorded after that
/// hash. Anything else under that name is unlinked and fetched.
/// Downloads stream into a unique partial name while hashing, so a partial
/// never has a digest name, and are synced before and after the rename.
pub(crate) async fn ensure_object(
    source: &mut Source,
    objects: &std::path::Path,
    artifact: &publication::Artifact,
    transfer: &mut std::time::Duration,
) -> Result<bool, Error> {
    use sha2::Digest;
    if !std::fs::symlink_metadata(objects)?.is_dir() {
        return Err("object cache must be a real directory".into());
    }
    let object = objects.join(&artifact.sha256);
    match std::fs::symlink_metadata(&object) {
        Ok(metadata) => {
            if metadata.is_file() {
                let (path, file) = (object.clone(), artifact.clone());
                let previous = source.checked.remove(&artifact.sha256);
                let checked =
                    tokio::task::spawn_blocking(move || reuse_object(&path, &file, previous))
                        .await??;
                if let Some(checked) = checked {
                    source.checked.insert(artifact.sha256.clone(), checked);
                    return Ok(false);
                }
            }
            // A corrupt object, or a symlink/directory under its name: unlink
            // the entry itself and never follow it.
            if metadata.is_dir() {
                std::fs::remove_dir_all(&object)?;
            } else {
                std::fs::remove_file(&object)?;
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let partial = objects.join(format!(
        "{}{PARTIAL}{}-{}",
        artifact.sha256,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let fetching = std::time::Instant::now();
    let result = async {
        let mut output = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&partial)
            .await?;
        let mut response = source
            .get(&format!("artifacts/{}", artifact.sha256))
            .await?;
        let mut hash = sha2::Sha256::new();
        let mut remaining = artifact.bytes;
        while let Some(chunk) = response.chunk().await? {
            if chunk.len() as u64 > remaining {
                return Err::<(), Error>("artifact exceeds declared length".into());
            }
            hash.update(&chunk);
            output.write_all(&chunk).await?;
            remaining -= chunk.len() as u64;
        }
        if remaining != 0 {
            return Err("artifact incomplete".into());
        }
        if hex::encode(hash.finalize()) != artifact.sha256 {
            return Err("artifact digest mismatch".into());
        }
        let mut permissions = output.metadata().await?.permissions();
        permissions.set_readonly(true);
        output.set_permissions(permissions).await?;
        output.sync_all().await?;
        drop(output);
        std::fs::rename(&partial, &object)?;
        std::fs::File::open(objects)?.sync_all()?;
        Ok(())
    }
    .await;
    *transfer += fetching.elapsed();
    if result.is_err() {
        let _ = std::fs::remove_file(&partial);
    }
    result.map(|()| true)
}

/// Make a cached regular file read-only, then verify and sync it. Returns the
/// checked identity when it may be linked; a digest mismatch leaves it for
/// replacement. The body hash is skipped only when the opened file still has
/// the identity `previous` recorded after this handle last hashed it.
fn reuse_object(
    path: &std::path::Path,
    artifact: &publication::Artifact,
    previous: Option<Checked>,
) -> Result<Option<Checked>, Error> {
    let file = std::fs::File::open(path)?;
    let mut permissions = file.metadata()?.permissions();
    if !permissions.readonly() {
        permissions.set_readonly(true);
        file.set_permissions(permissions)?;
    }
    let before = file.metadata()?;
    if before.len() != artifact.bytes {
        return Ok(None);
    }
    let Some(checked) = Checked::of(&before) else {
        return Ok(None);
    };
    if previous == Some(checked) {
        return Ok(Some(checked));
    }
    if verify_open(&file, artifact).is_err() || Checked::of(&file.metadata()?) != Some(checked) {
        return Ok(None);
    }
    file.sync_all()?;
    Ok(Some(checked))
}

/// Hash the already opened file, so the recorded identity names these bytes.
fn verify_open(file: &std::fs::File, artifact: &publication::Artifact) -> Result<(), Error> {
    use sha2::Digest;
    use std::io::Read;
    let mut hash = sha2::Sha256::new();
    let mut reader = file;
    let mut buffer = vec![0; 1 << 20];
    let mut total = 0u64;
    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        total += n as u64;
        hash.update(&buffer[..n]);
    }
    if total != artifact.bytes || hex::encode(hash.finalize()) != artifact.sha256 {
        return Err("artifact digest mismatch".into());
    }
    Ok(())
}

/// Stage a verified immutable object by hard link: the stage and cache share
/// one read-only inode that nothing writes, so it is not hashed again here.
/// Only a filesystem that explicitly cannot link falls back to a copy, which
/// is then verified on its own.
pub(crate) fn link_object(
    object: &std::path::Path,
    target: &std::path::Path,
    artifact: &publication::Artifact,
    link: impl FnOnce(&std::path::Path, &std::path::Path) -> std::io::Result<()>,
) -> Result<(), Error> {
    match link(object, target) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
            std::fs::copy(object, target)?;
            publication::verify_file(target, artifact)
        }
        Err(e) => Err(e.into()),
    }
}

/// Link a revision directory once, then stage directory symlinks to that stable
/// tree. Repeated publications must not change a source file's inode or ctime:
/// both are part of the proof that previously checked rows are still unchanged.
/// Hold the collector's lock until the stage references its source; if collection
/// owns it, refuse this attempt so the next synchronization can retry safely.
pub(crate) fn stage_object(
    root: &std::path::Path,
    stage: &std::path::Path,
    object: &std::path::Path,
    artifact: &publication::Artifact,
) -> Result<(), Error> {
    use std::os::unix::fs::MetadataExt;
    use std::path::Component;
    let path = std::path::Path::new(&artifact.path);
    let components: Vec<_> = path.components().collect();
    if components.is_empty()
        || components
            .iter()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err("invalid staged artifact path".into());
    }
    let target = stage.join(path);
    let first = components[0]
        .as_os_str()
        .to_str()
        .ok_or("non-UTF8 artifact path")?;
    if components.len() == 1 {
        return link_object(object, &target, artifact, |a, b| std::fs::hard_link(a, b));
    }
    if !publication::is_digest(first) {
        return Err("revision directory must be a digest".into());
    }
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join(".sources.lock"))?;
    lock.try_lock().map_err(|error| match error {
        std::fs::TryLockError::WouldBlock => std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "source collection is in progress; retry staging",
        ),
        std::fs::TryLockError::Error(error) => error,
    })?;
    let catalog = root.join(".sources");
    real_directory(&catalog)?;
    let revision = catalog.join(first);
    real_directory(&revision)?;
    let mut parent = revision.clone();
    for component in &components[1..components.len() - 1] {
        parent.push(component.as_os_str());
        real_directory(&parent)?;
    }
    let source = parent.join(components.last().unwrap().as_os_str());
    match std::fs::symlink_metadata(&source) {
        Ok(metadata) => {
            if !metadata.is_file() || !metadata.permissions().readonly() {
                return Err("catalog source must be a read-only regular file".into());
            }
            let cached = std::fs::symlink_metadata(object)?;
            if metadata.dev() != cached.dev() || metadata.ino() != cached.ino() {
                publication::verify_file(&source, artifact)?;
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            link_object(object, &source, artifact, |a, b| std::fs::hard_link(a, b))?;
            // The copy fallback creates new data outside the stage tree.
            // Sync it here because syncing the stage does not follow directory links.
            std::fs::File::open(&source)?.sync_all()?;
            std::fs::File::open(&parent)?.sync_all()?;
        }
        Err(error) => return Err(error.into()),
    }
    let staged_revision = stage.join(first);
    match std::fs::symlink_metadata(&staged_revision) {
        Ok(metadata) => {
            if !metadata.file_type().is_symlink()
                || std::fs::read_link(&staged_revision)?
                    != std::path::Path::new("../.sources").join(first)
            {
                return Err("staged revision points outside the source catalog".into());
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::os::unix::fs::symlink(
                std::path::Path::new("../.sources").join(first),
                &staged_revision,
            )?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn real_directory(path: &std::path::Path) -> Result<(), Error> {
    match std::fs::create_dir(path) {
        Ok(()) => {
            std::fs::File::open(path.parent().ok_or("directory has no parent")?)?.sync_all()?
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    if !std::fs::symlink_metadata(path)?.is_dir() {
        return Err("source catalog must contain real directories".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    fn fixture(root: &std::path::Path) -> (std::path::PathBuf, publication::Artifact) {
        let object = root.join("object");
        std::fs::write(&object, b"checked rows").unwrap();
        let mut permissions = std::fs::metadata(&object).unwrap().permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&object, permissions).unwrap();
        let artifact = publication::Artifact {
            path: format!("{}/nested/rows.bin", "ab".repeat(32)),
            sha256: publication::digest(b"checked rows"),
            bytes: 12,
            kind: publication::Kind::RecentTable,
        };
        (object, artifact)
    }

    #[test]
    fn catalog_staging_defers_during_collection() {
        let root = tempfile::tempdir().unwrap();
        let (object, artifact) = fixture(root.path());
        let stage = root.path().join("staging-1-100");
        std::fs::create_dir(&stage).unwrap();
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.path().join(".sources.lock"))
            .unwrap();
        lock.lock().unwrap();
        assert!(stage_object(root.path(), &stage, &object, &artifact).is_err());
        assert!(!stage.join(&artifact.path).exists());
        drop(lock);
        stage_object(root.path(), &stage, &object, &artifact).unwrap();
        publication::verify_file(&stage.join(&artifact.path), &artifact).unwrap();
    }

    #[test]
    fn repeated_staging_and_collection_preserve_checked_file_identity() {
        let root = tempfile::tempdir().unwrap();
        let (object, artifact) = fixture(root.path());
        let first = root.path().join("staging-first");
        let second = root.path().join("staging-second");
        std::fs::create_dir(&first).unwrap();
        std::fs::create_dir(&second).unwrap();
        stage_object(root.path(), &first, &object, &artifact).unwrap();
        let before = std::fs::metadata(first.join(&artifact.path)).unwrap();
        stage_object(root.path(), &second, &object, &artifact).unwrap();
        std::fs::remove_dir_all(&first).unwrap();
        let after = std::fs::metadata(second.join(&artifact.path)).unwrap();
        assert_eq!(
            (
                before.dev(),
                before.ino(),
                before.ctime(),
                before.ctime_nsec()
            ),
            (after.dev(), after.ino(), after.ctime(), after.ctime_nsec())
        );
        publication::verify_file(&second.join(&artifact.path), &artifact).unwrap();
    }

    #[test]
    fn reused_objects_are_hashed_once_per_unchanged_identity() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let (object, artifact) = fixture(root.path());
        // First load through a handle always hashes: wrong bytes fail.
        let wrong = root.path().join("wrong");
        std::fs::write(&wrong, b"wrong rows!!").unwrap();
        assert_eq!(reuse_object(&wrong, &artifact, None).unwrap(), None);
        let checked = reuse_object(&object, &artifact, None).unwrap().unwrap();
        // An unchanged identity skips the body hash: a forged record for
        // unrelated bytes is accepted, proving no rehash happens.
        std::fs::set_permissions(&wrong, std::fs::Permissions::from_mode(0o444)).unwrap();
        let forged = Checked::of(&std::fs::metadata(&wrong).unwrap());
        assert!(forged.is_some());
        assert_eq!(reuse_object(&wrong, &artifact, forged).unwrap(), forged);
        assert_eq!(
            reuse_object(&object, &artifact, Some(checked)).unwrap(),
            Some(checked)
        );
        // In-place rewrite of equal length changes ctime: hashed and refused.
        std::fs::set_permissions(&object, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::fs::write(&object, b"Checked rows").unwrap();
        assert_eq!(
            reuse_object(&object, &artifact, Some(checked)).unwrap(),
            None
        );
        // Replacement under the same name is a new inode: hashed again.
        std::fs::remove_file(&object).unwrap();
        std::fs::write(&object, b"checked rows").unwrap();
        let replaced = reuse_object(&object, &artifact, Some(checked))
            .unwrap()
            .unwrap();
        assert_ne!(replaced, checked);
        // A record for another length is never trusted.
        let short = publication::Artifact {
            bytes: 11,
            ..artifact.clone()
        };
        assert_eq!(reuse_object(&object, &short, Some(replaced)).unwrap(), None);
    }

    #[test]
    fn catalog_directory_symlinks_are_refused() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let (object, artifact) = fixture(root.path());
        let stage = root.path().join("staging-first");
        std::fs::create_dir(&stage).unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join(".sources")).unwrap();
        assert!(stage_object(root.path(), &stage, &object, &artifact).is_err());
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
    }

    #[test]
    fn replaced_catalog_content_cannot_inherit_verification() {
        let root = tempfile::tempdir().unwrap();
        let (object, artifact) = fixture(root.path());
        let stage = root.path().join("staging-first");
        std::fs::create_dir(&stage).unwrap();
        stage_object(root.path(), &stage, &object, &artifact).unwrap();
        let source = root.path().join(".sources").join(&artifact.path);
        std::fs::remove_file(&source).unwrap();
        std::fs::write(&source, b"wrong rows!!").unwrap();
        let mut permissions = std::fs::metadata(&source).unwrap().permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&source, permissions).unwrap();
        assert!(stage_object(root.path(), &stage, &object, &artifact).is_err());
    }
}
