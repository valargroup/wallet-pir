//! Checked physical bytes whose digest may still await product validation.
use super::{open_object_file, FileIdentity, ObjectFileError};
use crate::ObjectReference;
use std::{
    fs::File,
    path::{Path, PathBuf},
};

/// Receipt of the expected length at one exact physical inode. This is not
/// whole-file integrity evidence. Transparent validates bulk bytes after activation.
pub struct ReceivedObjectFile {
    path: PathBuf,
    file: File,
    reference: ObjectReference,
    identity: FileIdentity,
}
impl ReceivedObjectFile {
    pub fn open(path: PathBuf, reference: ObjectReference) -> Result<Self, ObjectFileError> {
        let file = open_object_file(&path, Some(reference.bytes))?;
        let identity = FileIdentity::capture(&path, &file)?;
        Ok(Self {
            path,
            file,
            reference,
            identity,
        })
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn file(&self) -> &File {
        &self.file
    }
    pub fn reference(&self) -> &ObjectReference {
        &self.reference
    }
    pub fn check(&self) -> Result<(), ObjectFileError> {
        self.identity.check(&self.path, &self.file)
    }
    pub(crate) fn seal(mut self) -> Result<Self, ObjectFileError> {
        self.check()?;
        let mut permissions = self.file.metadata()?.permissions();
        if !permissions.readonly() {
            permissions.set_readonly(true);
            self.file.set_permissions(permissions)?;
            self.identity = self.identity.after_rename(&self.path, &self.file)?;
        }
        Ok(self)
    }
    pub(crate) fn renamed(mut self, path: PathBuf) -> Result<Self, ObjectFileError> {
        self.identity = self.identity.after_rename(&path, &self.file)?;
        self.path = path;
        Ok(self)
    }
}
