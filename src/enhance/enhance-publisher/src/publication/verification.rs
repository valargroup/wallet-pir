//! Hash evidence is process-local and only reusable for unchanged, settled files.
use crate::artifact;
use enhance_shared::publication::{Inventory, Object};
use pir_publication::{open_object_file, verify_object_file, FileIdentity};
use std::{collections::BTreeMap, path::Path, sync::atomic::Ordering};

/// Work done by one verification pass: files hashed, bytes read to hash them,
/// and files accepted from earlier evidence without reading.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VerificationStats {
    pub files: u64,
    pub bytes: u64,
    pub reused: u64,
}

/// An inventory whose objects this process hashed from disk, with the identity
/// of each file it hashed. The evidence exists only in memory and is never
/// accepted from another process or a file.
/// Reuse assumes immutable objects and writers that honor the preparation lock.
#[derive(Debug)]
pub struct PreparedPublication {
    inventory: Inventory,
    evidence: BTreeMap<String, FileIdentity>,
    hashed: VerificationStats,
}

impl PreparedPublication {
    pub fn inventory(&self) -> &Inventory {
        &self.inventory
    }

    /// Work done by the pass that produced this evidence.
    pub fn verification_stats(&self) -> VerificationStats {
        self.hashed
    }

    /// Verify the objects again under `root`, rehashing only files whose
    /// identity differs from the one recorded when they were last hashed.
    pub fn recheck(self, root: &Path) -> Result<Self, String> {
        self.recheck_until(root, &|| false)
    }

    /// [`Self::recheck`] that ends with [`super::STOPPED`] once `stop` is true.
    pub fn recheck_until(self, root: &Path, stop: super::Stop) -> Result<Self, String> {
        verify_with(root, self.inventory, &self.evidence, stop)
    }
}

/// Validate an inventory and hash every object it references.
pub fn verify_publication(
    root: &Path,
    inventory: Inventory,
) -> Result<PreparedPublication, String> {
    verify_publication_until(root, inventory, &|| false)
}

/// [`verify_publication`] that ends with [`super::STOPPED`] once `stop` is true.
pub fn verify_publication_until(
    root: &Path,
    inventory: Inventory,
    stop: super::Stop,
) -> Result<PreparedPublication, String> {
    verify_with(root, inventory, &BTreeMap::new(), stop)
}

fn verify_with(
    root: &Path,
    inventory: Inventory,
    prior: &BTreeMap<String, FileIdentity>,
    stop: super::Stop,
) -> Result<PreparedPublication, String> {
    inventory.validate()?;
    let packings: Vec<Object> = inventory
        .domains
        .iter()
        .map(|d| d.packing_object())
        .collect();
    let objects: Vec<&Object> = inventory
        .domains
        .iter()
        .zip(&packings)
        .flat_map(|(domain, packing)| {
            domain
                .rows
                .iter()
                .chain([&domain.hint, &domain.public, packing])
        })
        .collect();
    let mut pass = VerificationPass::default();
    pass.check_all(root, &objects, prior, stop)?;
    Ok(PreparedPublication {
        inventory,
        evidence: pass.evidence,
        hashed: pass.hashed,
    })
}

/// Objects are hashed on up to this many threads at once. Hashing is most of a
/// publication's verification, and a restarted publisher serves nothing until it ends.
const HASHING_THREADS: usize = 4;

/// A fresh object can be checked once in this pass without retaining evidence
/// that would allow a later pass to skip its contents.
#[derive(Default)]
pub(super) struct VerificationPass {
    checked: BTreeMap<String, u64>,
    pub(super) evidence: BTreeMap<String, FileIdentity>,
    pub(super) hashed: VerificationStats,
}

impl VerificationPass {
    #[cfg(test)]
    pub(super) fn check_object(
        &mut self,
        root: &Path,
        object: &Object,
        prior: Option<&FileIdentity>,
    ) -> Result<(), String> {
        let prior = prior
            .map(|identity| (object.sha256.clone(), *identity))
            .into_iter()
            .collect();
        self.check_all(root, &[object], &prior, &|| false)
    }

    /// Checks each of `objects` once per pass, hashing the ones not checked yet on up to
    /// [`HASHING_THREADS`] threads. An object named twice must have one length.
    pub(super) fn check_all(
        &mut self,
        root: &Path,
        objects: &[&Object],
        prior: &BTreeMap<String, FileIdentity>,
        stop: super::Stop,
    ) -> Result<(), String> {
        if stop() {
            return Err(super::STOPPED.into());
        }
        let mut fresh = BTreeMap::new();
        for object in objects {
            let bytes = self
                .checked
                .get(&object.sha256)
                .or_else(|| fresh.get(&object.sha256).map(|o: &&Object| &o.bytes));
            match bytes {
                Some(bytes) if *bytes != object.bytes => {
                    return Err("publication object checksum or length mismatch".into());
                }
                Some(_) => {}
                None => {
                    fresh.insert(object.sha256.clone(), *object);
                }
            }
        }
        let fresh: Vec<&Object> = fresh.into_values().collect();
        let next = std::sync::atomic::AtomicUsize::new(0);
        let failed = std::sync::atomic::AtomicBool::new(false);
        let hash = || {
            let mut done = Vec::new();
            while let Some(object) = fresh.get(next.fetch_add(1, Ordering::Relaxed)) {
                if stop() {
                    return Err(super::STOPPED.to_string());
                }
                // Another thread's failure fails the pass: start no further object.
                if failed.load(Ordering::Relaxed) {
                    break;
                }
                let mut hashed = VerificationStats::default();
                let prior = prior.get(&object.sha256);
                match check_object_until(root, object, prior, &mut hashed, stop) {
                    Ok(identity) => done.push((*object, identity, hashed)),
                    Err(error) => {
                        failed.store(true, Ordering::Relaxed);
                        return Err(error);
                    }
                }
            }
            Ok(done)
        };
        let results: Vec<Result<Vec<_>, String>> = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..HASHING_THREADS.min(fresh.len()))
                .map(|_| scope.spawn(&hash))
                .collect();
            threads
                .into_iter()
                .map(|thread| {
                    thread
                        .join()
                        .unwrap_or_else(|_| Err("object hashing thread panicked".into()))
                })
                .collect()
        });
        for done in results {
            for (object, identity, hashed) in done? {
                if let Some(identity) = identity {
                    self.evidence.insert(object.sha256.clone(), identity);
                }
                self.checked.insert(object.sha256.clone(), object.bytes);
                self.hashed.files += hashed.files;
                self.hashed.bytes += hashed.bytes;
                self.hashed.reused += hashed.reused;
            }
        }
        Ok(())
    }
}

/// Check one object, reusing `prior` while the path still names the same,
/// unchanged file. Returns the identity to reuse next time, if it has settled.
#[cfg(test)]
pub(super) fn check_object(
    root: &Path,
    object: &Object,
    prior: Option<&FileIdentity>,
    hashed: &mut VerificationStats,
) -> Result<Option<FileIdentity>, String> {
    check_object_until(root, object, prior, hashed, &|| false)
}

fn check_object_until(
    root: &Path,
    object: &Object,
    prior: Option<&FileIdentity>,
    hashed: &mut VerificationStats,
    stop: super::Stop,
) -> Result<Option<FileIdentity>, String> {
    let path = object.path(root)?;
    let mut file = open_object_file(&path, Some(object.bytes)).map_err(|e| e.to_string())?;
    let reference = object.reference();
    let verified = verify_object_file(
        &path,
        &mut file,
        Some(&reference),
        prior.map(|identity| (&reference, *identity)),
        stop,
    )
    .map_err(|e| match stop() {
        true => super::STOPPED.to_string(),
        false => e.to_string(),
    })?;
    if verified.reused {
        hashed.reused += 1;
    } else {
        hashed.files += 1;
        hashed.bytes += verified.reference.bytes;
        artifact::release_file_cache(&file);
    }
    Ok(verified.reusable.then_some(verified.identity))
}
