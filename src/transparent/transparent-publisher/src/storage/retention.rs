//! In-process publication ownership coordinated with candidate collection.
//! The directory is the durable owner of its object references. A lease keeps
//! that directory available until the final reader or prepared view releases it.
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{LazyLock, Mutex, MutexGuard},
};
static OWNERS: LazyLock<Mutex<BTreeMap<PathBuf, usize>>> = LazyLock::new(Default::default);

pub struct PublicationLease(PathBuf);
impl PublicationLease {
    /// Acquire ownership before reading any publication file. Collection holds
    /// the same lock while removing directories, so acquisition cannot race it.
    pub fn acquire(directory: &Path) -> std::io::Result<Self> {
        let mut owners = OWNERS.lock().unwrap();
        let path = std::fs::canonicalize(directory)?;
        *owners.entry(path.clone()).or_default() += 1;
        Ok(Self(path))
    }
}
impl Drop for PublicationLease {
    fn drop(&mut self) {
        let mut owners = OWNERS.lock().unwrap();
        let count = owners
            .get_mut(&self.0)
            .expect("a publication lease retains its owner");
        *count -= 1;
        if *count == 0 {
            owners.remove(&self.0);
        }
    }
}
pub(crate) struct CollectionGuard(MutexGuard<'static, BTreeMap<PathBuf, usize>>);
impl CollectionGuard {
    pub(crate) fn holds(&self, path: &Path) -> bool {
        // Unreadable ownership must never authorize removal.
        std::fs::canonicalize(path).map_or(true, |path| self.0.contains_key(&path))
    }
}
pub(crate) fn collecting() -> CollectionGuard {
    CollectionGuard(OWNERS.lock().unwrap())
}
