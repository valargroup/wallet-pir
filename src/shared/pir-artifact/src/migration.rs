//! Resumable checksum conversion. Product adapters enumerate references and
//! activate replacement metadata; this module never changes publication authority.
use crate::{Algorithm, Checksum, Hasher, CURRENT};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, Metadata},
    io::{self, Read, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stamp {
    device: u64,
    inode: u64,
    bytes: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}
impl Stamp {
    pub fn of(metadata: &Metadata) -> io::Result<Self> {
        if !metadata.is_file() {
            return Err(invalid("artifact is not a regular file"));
        }
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            bytes: metadata.len(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }

    pub fn identity(&self) -> (u64, u64) {
        (self.device, self.inode)
    }
    pub fn same_contents(&self, other: &Self) -> bool {
        self.identity() == other.identity()
            && self.bytes == other.bytes
            && self.modified == other.modified
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Converted {
    version: u32,
    pub source: PathBuf,
    pub previous: Checksum,
    pub checksum: Checksum,
    pub bytes: u64,
    stamp: Stamp,
}

impl Converted {
    pub fn vouches(&self, checksum: &Checksum, stamp: &Stamp) -> bool {
        self.version == 1 && self.checksum == *checksum && self.stamp == *stamp
    }
    pub fn refresh_link_stamp(&mut self, checkpoints: &Path, expected: &Stamp) -> io::Result<()> {
        let actual = Stamp::of(&fs::symlink_metadata(&self.source)?)?;
        if actual != *expected || !self.stamp.same_contents(&actual) {
            return Err(invalid(
                "converted source changed during alias installation",
            ));
        }
        self.stamp = actual;
        persist(
            &checkpoints.join(format!("{}-{}.json", self.previous.key()?, CURRENT.name())),
            &serde_json::to_vec(self)?,
        )
    }
}

/// Atomically install a small prepared metadata record, retaining its existing
/// owner and mode. The expected predecessor digest is an operations pin, not
/// the data-plane checksum policy. The caller owns the production lock and
/// must have stopped every process that can write this record.
pub fn install_record(
    source: &Path,
    expected_source: &Checksum,
    destination: &Path,
    expected_destination: &Checksum,
) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    expected_source.validate()?;
    expected_destination.validate()?;
    let bytes = fs::read(source)?;
    expected_source.verify(&bytes)?;
    let metadata = fs::symlink_metadata(destination)?;
    let before = Stamp::of(&metadata)?;
    expected_destination.verify(&fs::read(destination)?)?;
    if before != Stamp::of(&fs::symlink_metadata(destination)?)? {
        return Err(invalid("live metadata changed before installation"));
    }
    let parent = destination
        .parent()
        .ok_or_else(|| invalid("metadata has no parent"))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(&bytes)?;
    let actual = temporary.as_file().metadata()?;
    if actual.uid() != metadata.uid() || actual.gid() != metadata.gid() {
        std::os::unix::fs::chown(temporary.path(), Some(metadata.uid()), Some(metadata.gid()))?;
    }
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(metadata.mode() & 0o7777))?;
    temporary.as_file().sync_all()?;
    if before != Stamp::of(&fs::symlink_metadata(destination)?)? {
        return Err(invalid("live metadata changed during installation"));
    }
    temporary
        .persist(destination)
        .map_err(|error| error.error)?;
    File::open(parent)?.sync_all()
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub fn persist(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("record has no parent"))?;
    fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    File::open(parent)?.sync_all()
}

/// The caller retains the source against collection. Completed records are
/// independent, so checkpoint cost does not grow with the total object count.
/// Repeated calls for unchanged files do not read their bodies again.
pub fn prepare(
    directory: &Path,
    source: &Path,
    previous: &Checksum,
    expected_bytes: u64,
) -> io::Result<Converted> {
    previous.validate()?;
    if !source.is_absolute() {
        return Err(invalid("migration source must be absolute"));
    }
    let source = source.to_path_buf();
    let named = Stamp::of(&fs::symlink_metadata(&source)?)?;
    let mut file = File::open(&source)?;
    if Stamp::of(&file.metadata()?)? != named || named.bytes != expected_bytes {
        return Err(invalid("migration source changed or has the wrong length"));
    }
    let checkpoint = directory.join(format!("{}-{}.json", previous.key()?, CURRENT.name()));
    if checkpoint.exists() {
        let mut record: Converted = serde_json::from_reader(File::open(&checkpoint)?.take(65537))?;
        if record.version == 1
            && record.previous == *previous
            && record.bytes == expected_bytes
            && record.stamp == named
            && record.checksum.algorithm == CURRENT
        {
            record.checksum.require_current()?;
            if record.source != source {
                record.source = source;
                persist(&checkpoint, &serde_json::to_vec(&record)?)?;
            }
            return Ok(record);
        }
    }
    let mut old = Hasher::for_algorithm(previous.algorithm);
    let mut new = Hasher::new();
    let mut buffer = vec![0; 1 << 20];
    let mut bytes = 0u64;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        old.update(&buffer[..count]);
        new.update(&buffer[..count]);
        bytes = bytes
            .checked_add(count as u64)
            .ok_or_else(|| invalid("artifact length overflow"))?;
        if bytes > expected_bytes {
            return Err(invalid("migration source grew"));
        }
    }
    if bytes != expected_bytes || old.finish() != *previous {
        return Err(invalid(
            "migration source does not match its previous checksum",
        ));
    }
    if named != Stamp::of(&file.metadata()?)?
        || named != Stamp::of(&fs::symlink_metadata(&source)?)?
    {
        return Err(invalid("migration source changed while hashing"));
    }
    let record = Converted {
        version: 1,
        source,
        previous: previous.clone(),
        checksum: new.finish(),
        bytes,
        stamp: named,
    };
    persist(&checkpoint, &serde_json::to_vec(&record)?)?;
    Ok(record)
}

/// Pin an input before a long batch starts. An existing positive checkpoint
/// can follow this exact alias operation; otherwise the new pin remains
/// unverified until `prepare` hashes it against the expected predecessor.
pub fn pin_unverified(
    directory: &Path,
    source: &Path,
    previous: &Checksum,
    bytes: u64,
    destination: &Path,
) -> io::Result<()> {
    previous.validate()?;
    let named = Stamp::of(&fs::symlink_metadata(source)?)?;
    if named.bytes != bytes {
        return Err(invalid("source pin length differs"));
    }
    let checkpoint = directory.join(format!("{}-{}.json", previous.key()?, CURRENT.name()));
    if checkpoint.exists() {
        let mut record: Converted = serde_json::from_reader(File::open(checkpoint)?.take(65537))?;
        if record.version == 1
            && record.previous == *previous
            && record.bytes == bytes
            && record.stamp == named
            && record.checksum.algorithm == CURRENT
        {
            record.checksum.require_current()?;
            record.source = source.into();
            return link_to(directory, &mut record, destination).map(|_| ());
        }
    }
    fs::hard_link(source, destination)?;
    let pinned = Stamp::of(&fs::symlink_metadata(destination)?)?;
    if !pinned.same_contents(&named) {
        return Err(invalid("source changed while being pinned"));
    }
    File::open(
        destination
            .parent()
            .ok_or_else(|| invalid("pin has no parent"))?,
    )?
    .sync_all()
}

/// Link unchanged bytes into a prepared namespace without changing the source.
/// A distinct existing inode is refused; the product must explicitly validate
/// copies rather than inherit another file's verification record.
pub fn link(checkpoints: &Path, record: &mut Converted, directory: &Path) -> io::Result<PathBuf> {
    link_to(checkpoints, record, &directory.join(record.checksum.key()?))
}

/// Install the exact alias a product's object index names, then checkpoint the
/// ctime change caused by our own hard link so resumption does not rehash it.
pub fn link_to(checkpoints: &Path, record: &mut Converted, target: &Path) -> io::Result<PathBuf> {
    record.checksum.require_current()?;
    let before = Stamp::of(&fs::symlink_metadata(&record.source)?)?;
    if before != record.stamp {
        return Err(invalid("converted source changed"));
    }
    let directory = target
        .parent()
        .ok_or_else(|| invalid("converted alias has no parent"))?;
    fs::create_dir_all(directory)?;
    match fs::hard_link(&record.source, target) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    let source = Stamp::of(&fs::symlink_metadata(&record.source)?)?;
    let destination = Stamp::of(&fs::symlink_metadata(target)?)?;
    // Creating a hard link changes ctime. Other fields must remain identical.
    if source.device != before.device
        || source.inode != before.inode
        || source.bytes != before.bytes
        || source.modified != before.modified
        || source != destination
    {
        return Err(invalid("converted object link differs"));
    }
    File::open(directory)?.sync_all()?;
    record.stamp = source;
    persist(
        &checkpoints.join(format!(
            "{}-{}.json",
            record.previous.key()?,
            CURRENT.name()
        )),
        &serde_json::to_vec(record)?,
    )?;
    Ok(target.to_path_buf())
}

/// Explicit legacy record constructor for product migration adapters only.
pub fn sha256(digest: String) -> io::Result<Checksum> {
    let value = Checksum {
        algorithm: Algorithm::Sha256,
        digest,
    };
    value.validate()?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn old(bytes: &[u8]) -> Checksum {
        let mut hash = Hasher::for_algorithm(Algorithm::Sha256);
        hash.update(bytes);
        hash.finish()
    }

    #[test]
    fn resumes_and_links_without_modifying_original_bytes() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("source");
        fs::write(&path, b"artifact").unwrap();
        let checkpoints = root.path().join("checkpoints");
        let mut a = prepare(&checkpoints, &path, &old(b"artifact"), 8).unwrap();
        let b = prepare(&checkpoints, &path, &old(b"artifact"), 8).unwrap();
        assert_eq!(a.checksum, b.checksum);
        let target = link(&checkpoints, &mut a, &root.path().join("objects")).unwrap();
        link(&checkpoints, &mut a, &root.path().join("objects")).unwrap();
        let resumed = prepare(&checkpoints, &path, &old(b"artifact"), 8).unwrap();
        assert_eq!(resumed.stamp, a.stamp);
        assert_eq!(fs::read(target).unwrap(), b"artifact");
        assert_eq!(fs::read(path).unwrap(), b"artifact");
    }

    #[test]
    fn rejects_same_length_corruption_and_changed_checkpoint_sources() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("source");
        fs::write(&path, b"abc").unwrap();
        let mut a = prepare(root.path(), &path, &old(b"abc"), 3).unwrap();
        fs::write(&path, b"abd").unwrap();
        assert!(prepare(root.path(), &path, &old(b"abc"), 3).is_err());
        assert!(link(root.path(), &mut a, &root.path().join("objects")).is_err());
    }
}
