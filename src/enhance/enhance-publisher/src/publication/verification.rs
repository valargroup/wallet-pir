//! Hash evidence is process-local and only reusable for unchanged, settled files.
use crate::artifact;
use enhance_shared::publication::{Inventory, Object};
use pir_publication::{hash_object_file, FileIdentity};
use std::{collections::BTreeMap, fs::File, path::Path, time::SystemTime};

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
        verify_with(root, self.inventory, &self.evidence)
    }
}

/// Validate an inventory and hash every object it references.
pub fn verify_publication(
    root: &Path,
    inventory: Inventory,
) -> Result<PreparedPublication, String> {
    verify_with(root, inventory, &BTreeMap::new())
}

fn verify_with(
    root: &Path,
    inventory: Inventory,
    prior: &BTreeMap<String, FileIdentity>,
) -> Result<PreparedPublication, String> {
    inventory.validate()?;
    let mut pass = VerificationPass::default();
    for domain in &inventory.domains {
        let packing = domain.packing_object();
        for object in domain
            .rows
            .iter()
            .chain([&domain.hint, &domain.public, &packing])
        {
            pass.check_object(root, object, prior.get(&object.sha256))?;
        }
    }
    Ok(PreparedPublication {
        inventory,
        evidence: pass.evidence,
        hashed: pass.hashed,
    })
}

/// A fresh object can be checked once in this pass without retaining evidence
/// that would allow a later pass to skip its contents.
#[derive(Default)]
pub(super) struct VerificationPass {
    checked: BTreeMap<String, u64>,
    pub(super) evidence: BTreeMap<String, FileIdentity>,
    pub(super) hashed: VerificationStats,
}

impl VerificationPass {
    pub(super) fn check_object(
        &mut self,
        root: &Path,
        object: &Object,
        prior: Option<&FileIdentity>,
    ) -> Result<(), String> {
        if let Some(bytes) = self.checked.get(&object.sha256) {
            if *bytes != object.bytes {
                return Err("publication object checksum or length mismatch".into());
            }
            return Ok(());
        }
        if let Some(identity) = check_object(root, object, prior, &mut self.hashed)? {
            self.evidence.insert(object.sha256.clone(), identity);
        }
        self.checked.insert(object.sha256.clone(), object.bytes);
        Ok(())
    }
}

/// Check one object, reusing `prior` while the path still names the same,
/// unchanged file. Returns the identity to reuse next time, if it has settled.
pub(super) fn check_object(
    root: &Path,
    object: &Object,
    prior: Option<&FileIdentity>,
    hashed: &mut VerificationStats,
) -> Result<Option<FileIdentity>, String> {
    let path = object.path(root)?;
    let started = SystemTime::now();
    let mut file = File::open(&path).map_err(|e| e.to_string())?;
    let identity = FileIdentity::capture(&path, &file).map_err(|e| e.to_string())?;
    if prior == Some(&identity) && file.metadata().map_err(|e| e.to_string())?.len() == object.bytes
    {
        hashed.reused += 1;
        return Ok(Some(identity));
    }
    let (reference, identity) =
        hash_object_file(&path, &mut file, Some(&object.reference()), &|| false)
            .map_err(|e| e.to_string())?;
    hashed.files += 1;
    hashed.bytes += reference.bytes;
    artifact::release_file_cache(&file);
    Ok(identity.settled_before(started).then_some(identity))
}
