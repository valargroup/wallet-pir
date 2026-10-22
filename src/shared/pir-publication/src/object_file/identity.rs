use super::ObjectFileError;
use std::{
    fs::{self, File, Metadata},
    os::unix::fs::MetadataExt,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

/// A regular file's identity and change metadata, not proof of its contents.
/// Reusable evidence must also bind a successful hash and semantic context.
/// Reuse assumes immutable files and cooperative writers; metadata cannot
/// establish integrity against an unrelated writer holding a writable mapping.
/// Hard-link changes alter change time and conservatively invalidate evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileIdentity {
    device: u64,
    inode: u64,
    bytes: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}
impl FileIdentity {
    pub fn from_metadata(metadata: &Metadata) -> Option<Self> {
        metadata.is_file().then(|| Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            bytes: metadata.len(),
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }

    /// Observe an opened regular file only while its name selects that inode.
    pub fn capture(path: &Path, file: &File) -> Result<Self, ObjectFileError> {
        let identity = Self::from_metadata(&file.metadata()?)
            .ok_or_else(|| ObjectFileError::NotRegularFile(path.to_path_buf()))?;
        identity.check(path, file)?;
        Ok(identity)
    }

    /// Fail if the opened file or its named identity changed. No body is read.
    pub fn check(&self, path: &Path, file: &File) -> Result<(), ObjectFileError> {
        if Self::from_metadata(&file.metadata()?) != Some(*self)
            || Self::from_metadata(&fs::symlink_metadata(path)?) != Some(*self)
        {
            return Err(ObjectFileError::FileChanged(path.to_path_buf()));
        }
        Ok(())
    }

    /// Both timestamps must predate the read by more than one second, the
    /// coarsest filesystem timestamp tick supported by publisher evidence.
    pub fn settled_before(&self, started: SystemTime) -> bool {
        let Ok(since) = started.duration_since(UNIX_EPOCH) else {
            return false;
        };
        let nanos = |(seconds, fraction): (i64, i64)| {
            i128::from(seconds) * 1_000_000_000 + i128::from(fraction)
        };
        nanos(self.modified).max(nanos(self.changed)) + 1_000_000_000 < since.as_nanos() as i128
    }
}
