//! Hash evidence is process-local and only reusable for unchanged, settled files.
use crate::artifact;
use enhance_query_server::publication::{Inventory, Object};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Read,
    os::unix::fs::MetadataExt,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// A file status change this recent may share its timestamp with a later write
/// (timestamps advance in coarse ticks), so a hash taken now is not reused.
pub(super) const SETTLED: Duration = Duration::from_secs(1);

/// Filesystem identity of an immutable publisher object. Ordinary writes,
/// truncation, replacement and metadata restoration change these fields.
/// This is not an integrity boundary against unrelated writers, such as a
/// process holding a writable mapping of the file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FileIdentity {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: (i64, i64),
    pub(super) ctime: (i64, i64),
}

impl FileIdentity {
    pub(super) fn of(metadata: &fs::Metadata) -> Self {
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
            len: metadata.len(),
            mtime: (metadata.mtime(), metadata.mtime_nsec()),
            ctime: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }

    /// Whether the recorded status change predates the timestamp safety window.
    pub(super) fn settled_before(&self, started: SystemTime) -> bool {
        let (Ok(seconds), Ok(nanos)) = (u64::try_from(self.ctime.0), u32::try_from(self.ctime.1))
        else {
            return false;
        };
        UNIX_EPOCH
            .checked_add(Duration::new(seconds, nanos))
            .and_then(|ctime| ctime.checked_add(SETTLED))
            .is_some_and(|settled| settled <= started)
    }
}

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
            pass.check_object(root, object, prior.get(&object.checksum))?;
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
        if let Some(bytes) = self.checked.get(&object.checksum) {
            if *bytes != object.bytes {
                return Err("publication object checksum or length mismatch".into());
            }
            return Ok(());
        }
        if let Some(identity) = check_object(root, object, prior, &mut self.hashed)? {
            self.evidence.insert(object.checksum.clone(), identity);
        }
        self.checked.insert(object.checksum.clone(), object.bytes);
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
    if let Some(prior) = prior {
        let current = FileIdentity::of(&fs::metadata(&path).map_err(|e| e.to_string())?);
        if current == *prior && current.len == object.bytes {
            hashed.reused += 1;
            return Ok(Some(current));
        }
    }
    let started = SystemTime::now();
    let mut file = File::open(&path).map_err(|e| e.to_string())?;
    let before = FileIdentity::of(&file.metadata().map_err(|e| e.to_string())?);
    if before.len != object.bytes {
        return Err("publication object checksum or length mismatch".into());
    }
    let mut hash = pir_artifact::Hasher::new();
    let mut buffer = vec![0; 1 << 16];
    loop {
        let n = file.read(&mut buffer).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        hashed.bytes += n as u64;
        hash.update(&buffer[..n]);
    }
    hashed.files += 1;
    artifact::release_file_cache(&file);
    if hex::encode(hash.finalize()) != object.checksum {
        return Err("publication object checksum or length mismatch".into());
    }
    if FileIdentity::of(&file.metadata().map_err(|e| e.to_string())?) != before {
        return Err("publication object changed while it was verified".into());
    }
    Ok(before.settled_before(started).then_some(before))
}
