//! Prepare complete, immutable publications without reaching a serving process.
//! Only the inventory is installed last; an interrupted build leaves reusable
//! objects but never a complete inventory referring to unfinished bytes.
//!
//! Every referenced object is hashed from disk once, when preparation finishes.
//! That pass records which file each hash was read from, so publication only
//! rehashes files that were changed, replaced or truncated since.
use crate::{artifact, prepared_packing, runtime, PackingBudget};
use enhance_query_server::{
    protocol::{digest, Coverage, Geometry, MAX_QUERY_SHARDS, PROTOCOL_REVISION},
    publication::{Domain, Inventory, Object, METADATA_LIMIT, VERSION},
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read},
    os::unix::fs::MetadataExt,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Produce material for validated coverage, one domain at a time. The caller
/// owns chain authority and supplies the recovery epoch of every domain.
/// This operation neither activates consumers nor grants serving authority.
pub fn prepare(
    root: &Path,
    coverage: Coverage,
    epochs: &BTreeMap<u64, String>,
    mut read: impl FnMut(u64, usize) -> Result<Vec<u8>, String>,
) -> Result<Prepared, String> {
    coverage.validate(Geometry::default())?;
    if coverage.shards.len() > MAX_QUERY_SHARDS as usize || epochs.len() != coverage.shards.len() {
        return Err("incomplete publication recovery identities".into());
    }
    // Check every epoch before creating large files or preparing a runtime.
    for shard in &coverage.shards {
        let text = epochs.get(&shard.id).ok_or("missing domain epoch")?;
        let epoch: u64 = text.parse().map_err(|_| "invalid domain epoch")?;
        if epoch.to_string() != *text {
            return Err("noncanonical domain epoch".into());
        }
    }
    fs::create_dir_all(root.join("objects")).map_err(|e| e.to_string())?;
    let owner = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join("prepare.lock"))
        .map_err(|e| e.to_string())?;
    owner
        .try_lock()
        .map_err(|_| "publication preparation already owned")?;
    let mut engine = runtime::Engine::new(&root.join("preparation"));
    let mut domains = Vec::new();
    for shard in &coverage.shards {
        let mut plan = runtime::plan(shard.clone(), &mut read)?;
        for unit in &mut plan.units {
            unit.recovery_epoch = epochs[&shard.id]
                .parse()
                .map_err(|_| "invalid domain epoch")?;
        }
        let mut rows = Vec::new();
        for (spec, unit) in plan.shard.units.iter().zip(&plan.units) {
            let bytes = runtime::unit_rows(&plan.shard, spec, &mut read)?;
            if hex::encode(Sha256::digest(&bytes)) != unit.content_sha256 {
                return Err("canonical rows changed during publication preparation".into());
            }
            rows.push(install(root, &bytes)?);
        }
        // A previously verified domain avoids hint and packing preparation.
        let key = digest(&plan);
        let retained = root.join("domains").join(format!("{key}.json"));
        if retained.exists() {
            let domain: Domain = read_metadata(&retained)?;
            if domain.plan != plan || domain.rows != rows {
                return Err("retained publication domain identity differs".into());
            }
            // The complete inventory is validated and its objects hashed below.
            domains.push(domain);
            continue;
        }
        let (evaluation, _) = engine
            .prepare(plan.clone(), &mut read)
            .map_err(|e| e.to_string())?;
        let hint = evaluation.hint()?;
        let packing =
            runtime::Packing::new(shard.logical_rows, &hint, &PackingBudget::coordinator())?;
        let prepared = prepared_packing::persist(root, &packing)?;
        install_file(
            root,
            &root.join(prepared_packing::DIRECTORY).join(&prepared.name),
            &prepared,
        )?;
        let mut encoded = Vec::new();
        crate::wire::write_crs_blocks(&mut encoded, &hint).map_err(|e| e.to_string())?;
        let domain = Domain {
            plan,
            rows,
            hint: install(root, &encoded)?,
            public: install(root, &packing.public)?,
            packing: prepared,
        };
        persist_metadata(root, "domains", &format!("{key}.json"), &domain)?;
        domains.push(domain);
    }
    let inventory = Inventory {
        version: VERSION,
        network: "main".into(),
        profile: PROTOCOL_REVISION.into(),
        coverage,
        domains,
    };
    let prepared = verify(root, inventory)?;
    File::open(root.join("objects"))
        .and_then(|f| f.sync_all())
        .map_err(|e| e.to_string())?;
    persist_metadata(
        root,
        "inventories",
        &format!("{}.json", digest(&prepared.inventory)),
        &prepared.inventory,
    )?;
    Ok(prepared)
}

/// A file status change this recent may share its timestamp with a later write
/// (timestamps advance in coarse ticks), so a hash taken now is not reused.
const SETTLED: Duration = Duration::from_secs(1);

/// The exact file a hash was read from. A write, truncation, rename over the
/// path or metadata restore changes at least one of these fields; the status
/// change time cannot be set back by a writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileIdentity {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
}

impl FileIdentity {
    fn of(metadata: &fs::Metadata) -> Self {
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
            len: metadata.len(),
            mtime: (metadata.mtime(), metadata.mtime_nsec()),
            ctime: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }

    /// Whether any later change to the file must move its status change time.
    fn settled_before(&self, started: SystemTime) -> bool {
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
pub struct Hashed {
    pub files: u64,
    pub bytes: u64,
    pub reused: u64,
}

/// An inventory whose objects this process hashed from disk, with the identity
/// of each file it hashed. The evidence exists only in memory and is never
/// accepted from another process or a file.
#[derive(Debug)]
pub struct Prepared {
    inventory: Inventory,
    evidence: BTreeMap<String, FileIdentity>,
    hashed: Hashed,
}

impl Prepared {
    pub fn inventory(&self) -> &Inventory {
        &self.inventory
    }

    /// Work done by the pass that produced this evidence.
    pub fn hashed(&self) -> Hashed {
        self.hashed
    }

    /// Verify the objects again under `root`, rehashing only files whose
    /// identity differs from the one recorded when they were last hashed.
    pub fn recheck(self, root: &Path) -> Result<Self, String> {
        verify_with(root, self.inventory, &self.evidence)
    }
}

/// Validate an inventory and hash every object it references.
pub fn verify(root: &Path, inventory: Inventory) -> Result<Prepared, String> {
    verify_with(root, inventory, &BTreeMap::new())
}

fn verify_with(
    root: &Path,
    inventory: Inventory,
    prior: &BTreeMap<String, FileIdentity>,
) -> Result<Prepared, String> {
    inventory.validate()?;
    let mut objects = Vec::new();
    for domain in &inventory.domains {
        objects.extend(domain.rows.iter().cloned());
        objects.extend([
            domain.hint.clone(),
            domain.public.clone(),
            domain.packing_object(),
        ]);
    }
    let (evidence, hashed) = check_all(root, &objects, prior)?;
    Ok(Prepared {
        inventory,
        evidence,
        hashed,
    })
}

/// Check every reference, hashing each distinct object once. All references
/// to one digest must declare the same length before any file is read.
/// Returns the settled identities that a later pass may reuse.
fn check_all(
    root: &Path,
    objects: &[Object],
    prior: &BTreeMap<String, FileIdentity>,
) -> Result<(BTreeMap<String, FileIdentity>, Hashed), String> {
    let mut lengths = BTreeMap::new();
    let mut distinct = Vec::new();
    for object in objects {
        object.validate()?;
        match lengths.get(&object.sha256) {
            Some(&bytes) if bytes != object.bytes => {
                return Err("publication object checksum or length mismatch".into());
            }
            Some(_) => {}
            None => {
                lengths.insert(object.sha256.clone(), object.bytes);
                distinct.push(object);
            }
        }
    }
    let mut evidence = BTreeMap::new();
    let mut hashed = Hashed::default();
    for object in distinct {
        if let Some(identity) = check(root, object, prior.get(&object.sha256), &mut hashed)? {
            evidence.insert(object.sha256.clone(), identity);
        }
    }
    Ok((evidence, hashed))
}

/// Check one object, reusing `prior` while the path still names the same,
/// unchanged file. Returns the identity to reuse next time, if it has settled.
fn check(
    root: &Path,
    object: &Object,
    prior: Option<&FileIdentity>,
    hashed: &mut Hashed,
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
    let mut hash = Sha256::new();
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
    if hex::encode(hash.finalize()) != object.sha256 {
        return Err("publication object checksum or length mismatch".into());
    }
    if FileIdentity::of(&file.metadata().map_err(|e| e.to_string())?) != before {
        return Err("publication object changed while it was verified".into());
    }
    Ok(before.settled_before(started).then_some(before))
}

pub fn read_metadata<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    if file.metadata().map_err(|e| e.to_string())?.len() > METADATA_LIMIT {
        return Err("publication metadata exceeds limit".into());
    }
    let mut bytes = Vec::new();
    file.take(METADATA_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() as u64 > METADATA_LIMIT {
        return Err("publication metadata exceeds limit".into());
    }
    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
}

fn persist_metadata(
    root: &Path,
    directory: &str,
    name: &str,
    value: &impl serde::Serialize,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    if bytes.len() as u64 > METADATA_LIMIT {
        return Err("publication metadata exceeds limit".into());
    }
    let directory = root.join(directory);
    fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
    artifact::write_atomic(&directory, name, |f| f.write_all(&bytes)).map_err(|e| e.to_string())?;
    File::open(directory)
        .and_then(|f| f.sync_all())
        .and_then(|_| File::open(root)?.sync_all())
        .map_err(|e| e.to_string())
}

fn install(root: &Path, bytes: &[u8]) -> Result<Object, String> {
    let object = Object {
        sha256: hex::encode(Sha256::digest(bytes)),
        bytes: bytes.len() as u64,
    };
    // An existing object is hashed with the complete inventory.
    if !object.path(root)?.exists() {
        artifact::write_atomic_cold(&root.join("objects"), &object.sha256, |f| {
            f.write_all(bytes)
        })
        .map_err(|e| e.to_string())?;
    }
    Ok(object)
}

fn install_file(
    root: &Path,
    source: &Path,
    prepared: &prepared_packing::Artifact,
) -> Result<(), String> {
    let object = Object {
        sha256: prepared.sha256.clone(),
        bytes: prepared.bytes,
    };
    // The copy is hashed with the complete inventory.
    if object.path(root)?.exists() {
        return Ok(());
    }
    artifact::write_atomic_cold(&root.join("objects"), &object.sha256, |f| {
        io::copy(&mut File::open(source)?, f)?;
        Ok(())
    })
    .map(drop)
    .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use enhance_query_server::protocol::Lifecycle;

    fn object(root: &Path, bytes: &[u8]) -> Object {
        let object = Object {
            sha256: hex::encode(Sha256::digest(bytes)),
            bytes: bytes.len() as u64,
        };
        fs::create_dir_all(root.join("objects")).unwrap();
        fs::write(object.path(root).unwrap(), bytes).unwrap();
        object
    }

    /// Let every file written so far settle, so its hash can be reused.
    fn settle() {
        std::thread::sleep(SETTLED + Duration::from_millis(100));
    }

    fn verified(root: &Path, object: &Object) -> FileIdentity {
        let mut hashed = Hashed::default();
        let identity = check(root, object, None, &mut hashed).unwrap().unwrap();
        assert_eq!(hashed.files, 1);
        identity
    }

    #[test]
    fn changed_replaced_and_truncated_objects_are_rehashed() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let unchanged = object(root, b"unchanged object");
        let rewritten = object(root, b"rewritten object");
        let restored = object(root, b"restored mtime!!");
        let replaced = object(root, b"replaced object!");
        let truncated = object(root, b"truncated object");
        settle();
        let all = [&unchanged, &rewritten, &restored, &replaced, &truncated];
        let evidence: Vec<_> = all.iter().map(|o| verified(root, o)).collect();
        let mut hashed = Hashed::default();
        for (object, identity) in all.iter().zip(&evidence) {
            check(root, object, Some(identity), &mut hashed).unwrap();
        }
        assert_eq!(
            hashed,
            Hashed {
                files: 0,
                bytes: 0,
                reused: 5,
            }
        );

        fs::write(rewritten.path(root).unwrap(), b"Rewritten object").unwrap();
        let path = restored.path(root).unwrap();
        let modified = fs::metadata(&path).unwrap().modified().unwrap();
        fs::write(&path, b"Restored mtime!!").unwrap();
        File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(modified)
            .unwrap();
        assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), modified);
        let path = replaced.path(root).unwrap();
        let replacement = root.join("replacement");
        fs::write(&replacement, b"Replaced object!").unwrap();
        File::options()
            .write(true)
            .open(&replacement)
            .unwrap()
            .set_modified(fs::metadata(&path).unwrap().modified().unwrap())
            .unwrap();
        fs::rename(&replacement, &path).unwrap();
        File::options()
            .write(true)
            .open(truncated.path(root).unwrap())
            .unwrap()
            .set_len(1)
            .unwrap();

        let mut hashed = Hashed::default();
        check(root, &unchanged, Some(&evidence[0]), &mut hashed).unwrap();
        assert_eq!(hashed.reused, 1);
        for (object, identity) in all.iter().zip(&evidence).skip(1) {
            let mut hashed = Hashed::default();
            assert!(
                check(root, object, Some(identity), &mut hashed).is_err(),
                "{object:?} must not be accepted from stale evidence"
            );
            assert_eq!(hashed.reused, 0);
        }
    }

    #[test]
    fn a_replaced_object_with_the_same_bytes_is_hashed_again() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let object = object(root, b"same bytes");
        settle();
        let identity = verified(root, &object);
        let replacement = root.join("replacement");
        fs::write(&replacement, b"same bytes").unwrap();
        fs::rename(&replacement, object.path(root).unwrap()).unwrap();
        let mut hashed = Hashed::default();
        check(root, &object, Some(&identity), &mut hashed).unwrap();
        assert_eq!((hashed.files, hashed.reused), (1, 0));
    }

    #[test]
    fn a_duplicate_fresh_object_is_hashed_once_without_evidence() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let fresh = object(root, b"fresh duplicate");
        let (evidence, hashed) =
            check_all(root, &[fresh.clone(), fresh.clone()], &BTreeMap::new()).unwrap();
        assert!(evidence.is_empty(), "an unsettled file mints no evidence");
        assert_eq!(
            hashed,
            Hashed {
                files: 1,
                bytes: fresh.bytes,
                reused: 0,
            }
        );
        settle();
        let (evidence, hashed) =
            check_all(root, &[fresh.clone(), fresh.clone()], &BTreeMap::new()).unwrap();
        assert_eq!((hashed.files, evidence.len()), (1, 1));
        let (_, hashed) = check_all(root, &[fresh.clone(), fresh], &evidence).unwrap();
        assert_eq!((hashed.files, hashed.reused), (0, 1));
    }

    #[test]
    fn references_with_conflicting_lengths_are_rejected_before_reading() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let stored = object(root, b"declared once");
        settle();
        let (evidence, _) =
            check_all(root, std::slice::from_ref(&stored), &BTreeMap::new()).unwrap();
        let other = Object {
            bytes: stored.bytes + 1,
            ..stored.clone()
        };
        for prior in [BTreeMap::new(), evidence] {
            for objects in [
                [stored.clone(), other.clone()],
                [other.clone(), stored.clone()],
            ] {
                assert!(check_all(root, &objects, &prior).is_err());
            }
        }
        // Reusable evidence never vouches for a length the file does not have.
        let mut hashed = Hashed::default();
        let identity = verified(root, &stored);
        assert!(check(root, &other, Some(&identity), &mut hashed).is_err());
        assert_eq!(hashed.reused, 0);
    }

    #[test]
    fn a_recently_changed_file_is_not_reused() {
        let root = tempfile::tempdir().unwrap();
        let object = object(root.path(), b"fresh object");
        let identity = FileIdentity::of(&fs::metadata(object.path(root.path()).unwrap()).unwrap());
        let ctime = UNIX_EPOCH + Duration::new(identity.ctime.0 as u64, identity.ctime.1 as u32);
        assert!(!identity.settled_before(ctime));
        assert!(!identity.settled_before(ctime + SETTLED - Duration::from_nanos(1)));
        assert!(identity.settled_before(ctime + SETTLED));
    }

    #[test]
    fn invalid_geometry_and_recovery_are_rejected_before_reading_records() {
        let scratch = tempfile::tempdir().unwrap();
        let root = scratch.path().join("publication");
        let coverage = Lifecycle::default()
            .coverage(1, Geometry::default())
            .unwrap();
        for epochs in [BTreeMap::new(), [(0, "01".into())].into()] {
            assert!(prepare(&root, coverage.clone(), &epochs, |_, _| {
                panic!("invalid metadata must not read records")
            })
            .is_err());
            assert!(!root.exists());
        }
        let mut malformed = coverage;
        malformed.shards[0].logical_rows = u64::MAX;
        assert!(
            prepare(&root, malformed, &[(0, "0".into())].into(), |_, _| {
                panic!("invalid geometry must not read records")
            })
            .is_err()
        );
        assert!(!root.exists());
    }

    #[test]
    fn preparation_has_one_writer() {
        let root = tempfile::tempdir().unwrap();
        let owner = File::create(root.path().join("prepare.lock")).unwrap();
        owner.lock().unwrap();
        let coverage = Lifecycle::default()
            .coverage(1, Geometry::default())
            .unwrap();
        assert!(
            prepare(root.path(), coverage, &[(0, "0".into())].into(), |_, _| {
                panic!("a second owner must not read records")
            })
            .is_err()
        );
    }
}
