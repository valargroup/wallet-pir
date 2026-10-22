//! Private construction files; dropping one attempt never touches another.
use super::{verification::copy_and_hash, ObjectInstallError};
use crate::ObjectReference;
use std::{
    fs,
    io::Read,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

pub(super) struct StagedObject {
    directory: PathBuf,
    path: PathBuf,
}

impl StagedObject {
    fn reserve(directory: &Path) -> Result<Self, ObjectInstallError> {
        // Keep construction outside the completed-object namespace. Existing
        // product collectors need see only completed objects, never an active
        // installer's private directory.
        let parent = directory.parent().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "object directory has no parent",
            )
        })?;
        let directory = parent.join(format!(
            ".install-{}",
            hex::encode(rand::random::<[u8; 16]>())
        ));
        fs::DirBuilder::new().mode(0o700).create(&directory)?;
        Ok(Self {
            path: directory.join("object"),
            directory,
        })
    }

    pub(super) fn create(directory: &Path) -> Result<Self, ObjectInstallError> {
        let staged = Self::reserve(directory)?;
        fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&staged.path)?;
        Ok(staged)
    }

    pub(super) fn adopt(directory: &Path, source: &Path) -> Result<Self, ObjectInstallError> {
        let staged = Self::reserve(directory)?;
        fs::hard_link(source, &staged.path)?;
        Ok(staged)
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn write(
        &mut self,
        input: &mut impl Read,
        stop: &dyn Fn() -> bool,
    ) -> Result<ObjectReference, ObjectInstallError> {
        let mut file = fs::OpenOptions::new().write(true).open(&self.path)?;
        let reference = copy_and_hash(input, &mut file, stop)?;
        if file.metadata()?.len() != reference.bytes {
            return Err(ObjectInstallError::FileChanged(self.path.clone()));
        }
        Ok(reference)
    }
}

impl Drop for StagedObject {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_dir(&self.directory);
    }
}
