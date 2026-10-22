//! Shared preparation mechanics for product-owned metadata adapters.
//!
//! Preparation writes a separate tree. It never replaces live metadata or
//! changes authority. The deployment transaction owns the eventual activation.
use crate::{migration, Checksum};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Object {
    pub checksum: Checksum,
    pub bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileProof {
    pub checksum: Checksum,
    pub stamp: migration::Stamp,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prepared {
    pub schema: String,
    pub product: String,
    pub previous_inventory: Checksum,
    pub inventory: Object,
    pub inventory_file: PathBuf,
    pub objects: BTreeMap<String, Object>,
    pub files: BTreeMap<PathBuf, FileProof>,
    pub root: PathBuf,
    pub stage: PathBuf,
    pub checkpoints: PathBuf,
    pub records: Vec<migration::Converted>,
}

pub struct Catalog {
    root: PathBuf,
    stage: PathBuf,
    objects: BTreeMap<String, Object>,
    records: Vec<migration::Converted>,
    files: BTreeMap<PathBuf, Checksum>,
    checkpoints: PathBuf,
}

impl Catalog {
    pub fn new(root: &Path, stage: &Path) -> io::Result<Self> {
        if !root.is_absolute() || !stage.is_absolute() || root == stage {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "migration roots must be distinct absolute paths",
            ));
        }
        fs::create_dir_all(stage)?;
        let canonical_root = root.canonicalize()?;
        let canonical_stage = stage.canonicalize()?;
        if canonical_root.starts_with(&canonical_stage)
            || canonical_stage.starts_with(&canonical_root)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "migration roots must not overlap",
            ));
        }
        Ok(Self {
            root: root.into(),
            stage: stage.into(),
            objects: BTreeMap::new(),
            records: Vec::new(),
            files: BTreeMap::new(),
            checkpoints: stage.join("checkpoints"),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn stage(&self) -> &Path {
        &self.stage
    }

    pub fn with_checkpoints(mut self, path: PathBuf) -> io::Result<Self> {
        if !path.is_absolute() {
            return Err(io::Error::other("checkpoint directory must be absolute"));
        }
        self.checkpoints = path;
        Ok(self)
    }

    pub fn checkpoints(&self) -> &Path {
        &self.checkpoints
    }

    pub fn previous_for(&self, digest: &str) -> Option<Checksum> {
        self.objects
            .iter()
            .find(|(_, object)| object.checksum.digest == digest)
            .and_then(|(key, _)| key.strip_prefix("sha256-"))
            .map(|digest| Checksum {
                algorithm: crate::Algorithm::Sha256,
                digest: digest.into(),
            })
    }

    /// Convert one immutable body and stage the alias the new object index uses.
    /// Repeated references reuse the first verified object with that old identity.
    pub fn object(
        &mut self,
        relative: &Path,
        previous: &Checksum,
        bytes: u64,
    ) -> io::Result<Object> {
        previous.validate()?;
        let key = previous.key()?;
        if let Some(object) = self.objects.get(&key) {
            if object.bytes != bytes {
                return Err(io::Error::other(
                    "one artifact identity has contradictory lengths",
                ));
            }
            return Ok(object.clone());
        }
        safe_relative(relative)?;
        let path = self.root.join(relative);
        reject_symlinks(&self.root, relative)?;
        let checkpoints = self.checkpoints.clone();
        let mut record = migration::prepare(&checkpoints, &path, previous, bytes)?;
        let destination = self
            .stage
            .join(crate::namespace("objects"))
            .join(&record.checksum.digest);
        migration::link_to(&checkpoints, &mut record, &destination)?;
        let object = Object {
            checksum: record.checksum.clone(),
            bytes,
        };
        self.objects.insert(key, object.clone());
        self.files.insert(destination, object.checksum.clone());
        self.records.push(record);
        Ok(object)
    }

    /// Preserve a product's publication-relative path as well as the common
    /// object-cache alias. The link's ctime change is included in the checkpoint.
    pub fn install_at(&mut self, previous: &Checksum, relative: &Path) -> io::Result<()> {
        safe_relative(relative)?;
        let record = self
            .records
            .iter_mut()
            .find(|r| r.previous == *previous)
            .ok_or_else(|| io::Error::other("artifact has not been converted"))?;
        let destination = self.stage.join(relative);
        migration::link_to(&self.checkpoints, record, &destination)?;
        self.files.insert(destination, record.checksum.clone());
        Ok(())
    }

    /// Register a separately converted header-bearing file after the product
    /// converter has verified both formats and returned its final byte checksum.
    pub fn transformed(
        &mut self,
        relative: &Path,
        previous: &Checksum,
        bytes: u64,
        output: &Path,
        checksum: Checksum,
    ) -> io::Result<Object> {
        safe_relative(relative)?;
        checksum.require_current()?;
        let record = migration::prepare(
            &self.checkpoints,
            &self.root.join(relative),
            previous,
            bytes,
        )?;
        if !output.starts_with(&self.stage) || fs::symlink_metadata(output)?.len() != bytes {
            return Err(io::Error::other(
                "converted artifact is outside staging or has the wrong length",
            ));
        }
        let object = Object { checksum, bytes };
        self.files.insert(output.into(), object.checksum.clone());
        self.objects.insert(previous.key()?, object.clone());
        self.records.push(record);
        Ok(object)
    }

    /// Store newly encoded metadata. The adapter supplies the product's own
    /// canonical serializer, rather than letting a generic JSON walk choose it.
    pub fn metadata(&mut self, bytes: &[u8]) -> io::Result<Object> {
        let checksum = crate::checksum(bytes);
        let path = self
            .stage
            .join(crate::namespace("objects"))
            .join(&checksum.digest);
        self.metadata_at(
            &path
                .strip_prefix(&self.stage)
                .map_err(io::Error::other)?
                .to_path_buf(),
            bytes,
        )?;
        Ok(Object {
            checksum,
            bytes: bytes.len() as u64,
        })
    }

    pub fn metadata_at(&mut self, relative: &Path, bytes: &[u8]) -> io::Result<()> {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        safe_relative(relative)?;
        let path = self.stage.join(relative);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_file() && fs::read(&path)? == bytes => {}
            Ok(_) => {
                return Err(io::Error::other(
                    "immutable staged metadata holds different bytes",
                ))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                migration::persist(&path, bytes)?
            }
            Err(error) => return Err(error),
        }
        let owner = fs::metadata(&self.root)?;
        let actual = fs::metadata(&path)?;
        if actual.uid() != owner.uid() || actual.gid() != owner.gid() {
            std::os::unix::fs::chown(&path, Some(owner.uid()), Some(owner.gid()))?;
        }
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644))?;
        self.files.insert(path, crate::checksum(bytes));
        Ok(())
    }

    pub fn finish(
        mut self,
        product: &str,
        previous: Checksum,
        bytes: &[u8],
    ) -> io::Result<Prepared> {
        let inventory = self.metadata(bytes)?;
        let inventory_file = self.stage.join("inventory.json");
        migration::persist(&inventory_file, bytes)?;
        let result = Prepared {
            schema: "artifact-checksum-preparation/1".into(),
            product: product.into(),
            previous_inventory: previous,
            inventory,
            inventory_file,
            objects: self.objects,
            records: self.records,
            files: self
                .files
                .into_iter()
                .map(|(path, checksum)| {
                    let stamp = migration::Stamp::of(&fs::symlink_metadata(&path)?)?;
                    Ok((path, FileProof { checksum, stamp }))
                })
                .collect::<io::Result<_>>()?,
            root: self.root,
            stage: self.stage.clone(),
            checkpoints: self.checkpoints,
        };
        migration::persist(
            &self.stage.join("prepared.json"),
            &serde_json::to_vec(&result)?,
        )?;
        Ok(result)
    }
}

impl Prepared {
    /// Other preparations in the same locked batch can create aliases of the
    /// same inode. Accept their refreshed proof only when the shared checkpoint
    /// names this exact current checksum and this exact post-link stamp.
    pub fn refresh_files(&mut self) -> io::Result<()> {
        for (path, proof) in &mut self.files {
            let current = migration::Stamp::of(&fs::symlink_metadata(path)?)?;
            if current == proof.stamp {
                continue;
            }
            let mut vouched = false;
            for (previous, object) in &self.objects {
                if object.checksum != proof.checksum {
                    continue;
                }
                let record = self
                    .checkpoints
                    .join(format!("{previous}-{}.json", crate::CURRENT.name()));
                if let Ok(raw) = fs::read(record) {
                    let record: migration::Converted = serde_json::from_slice(&raw)?;
                    vouched |= record.vouches(&proof.checksum, &current);
                }
            }
            if !vouched {
                return Err(io::Error::other(
                    "prepared artifact changed without a verified alias operation",
                ));
            }
            proof.stamp = current;
        }
        Ok(())
    }
    /// Check unchanged staged inodes without rereading large files. Preparation
    /// has already verified their complete contents; these stamps are local
    /// completion evidence, never publisher authority or portable validation.
    pub fn verify_files(&self) -> io::Result<()> {
        if self.schema != "artifact-checksum-preparation/1" {
            return Err(io::Error::other("unsupported artifact preparation"));
        }
        for (path, proof) in &self.files {
            let relative = path.strip_prefix(&self.stage).map_err(io::Error::other)?;
            safe_relative(relative)?;
            reject_symlinks(&self.stage, relative)?;
            proof.checksum.require_current()?;
            if migration::Stamp::of(&fs::symlink_metadata(path)?)? != proof.stamp {
                return Err(io::Error::other(
                    "prepared artifact changed; repeat preparation",
                ));
            }
        }
        Ok(())
    }

    /// Install only immutable aliases, never live authority records. Repeating
    /// the operation accepts the same inode and refuses conflicting bytes.
    /// Checkpoint our own link changes so final preflight does not rehash them.
    pub fn install(&mut self, destination: &Path) -> io::Result<usize> {
        if destination != self.root {
            return Err(io::Error::other("preparation destination root differs"));
        }
        self.verify_files()?;
        let mut stamps: BTreeMap<_, _> = self
            .files
            .values()
            .map(|p| (p.stamp.identity(), p.stamp.clone()))
            .collect();
        let mut installed = 0;
        for source in self.files.keys() {
            let relative = source.strip_prefix(&self.stage).map_err(io::Error::other)?;
            let before = migration::Stamp::of(&fs::symlink_metadata(source)?)?;
            if stamps.get(&before.identity()) != Some(&before) {
                return Err(io::Error::other(
                    "staged artifact changed during installation",
                ));
            }
            let target = destination.join(relative);
            let parent = target
                .parent()
                .ok_or_else(|| io::Error::other("artifact has no parent"))?;
            fs::create_dir_all(parent)?;
            reject_symlinks(destination, relative.parent().unwrap_or(Path::new("")))?;
            match fs::hard_link(source, &target) {
                Ok(()) => installed += 1,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
            let after = migration::Stamp::of(&fs::symlink_metadata(source)?)?;
            let named = migration::Stamp::of(&fs::symlink_metadata(&target)?)?;
            if after != named || !after.same_contents(&before) {
                return Err(io::Error::other(
                    "immutable artifact destination holds different bytes",
                ));
            }
            stamps.insert(after.identity(), after);
            fs::File::open(parent)?.sync_all()?;
        }
        for (path, proof) in &mut self.files {
            let after = migration::Stamp::of(&fs::symlink_metadata(path)?)?;
            if stamps.get(&after.identity()) != Some(&after) {
                return Err(io::Error::other("artifact changed during installation"));
            }
            proof.stamp = after;
        }
        for record in &mut self.records {
            if let Ok(metadata) = fs::symlink_metadata(&record.source) {
                let current = migration::Stamp::of(&metadata)?;
                if let Some(expected) = stamps.get(&current.identity()) {
                    record.refresh_link_stamp(&self.checkpoints, expected)?;
                }
            }
        }
        migration::persist(
            &self.stage.join("prepared.json"),
            &serde_json::to_vec(self)?,
        )?;
        Ok(installed)
    }
}

pub fn safe_relative(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "artifact path must stay inside its publication",
        ));
    }
    Ok(())
}

fn reject_symlinks(root: &Path, relative: &Path) -> io::Result<()> {
    let mut path = root.to_path_buf();
    for component in relative.components() {
        path.push(component);
        if fs::symlink_metadata(&path)?.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "symbolic link in migration source",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Algorithm, Hasher};

    #[test]
    fn repeated_references_share_one_verified_body_and_resume_after_linking() {
        let root = tempfile::tempdir().unwrap();
        let stage = tempfile::tempdir().unwrap();
        fs::write(root.path().join("old"), b"abc").unwrap();
        let mut hash = Hasher::for_algorithm(Algorithm::Sha256);
        hash.update(b"abc");
        let old = hash.finish();
        let mut catalog = Catalog::new(root.path(), stage.path()).unwrap();
        let a = catalog.object(Path::new("old"), &old, 3).unwrap();
        let b = catalog
            .object(Path::new("unused-duplicate"), &old, 3)
            .unwrap();
        assert_eq!(a.checksum, b.checksum);
        assert_eq!(catalog.records.len(), 1);
        assert!(catalog.object(Path::new("old"), &old, 4).is_err());
        drop(catalog);
        let mut resumed = Catalog::new(root.path(), stage.path()).unwrap();
        assert_eq!(
            resumed.object(Path::new("old"), &old, 3).unwrap().checksum,
            a.checksum
        );
    }

    #[test]
    fn installation_is_repeatable_and_keeps_verified_file_proofs_current() {
        let root = tempfile::tempdir().unwrap();
        let stage = tempfile::tempdir().unwrap();
        fs::write(root.path().join("old"), b"abc").unwrap();
        let mut hash = Hasher::for_algorithm(Algorithm::Sha256);
        hash.update(b"abc");
        let previous = hash.finish();
        let mut catalog = Catalog::new(root.path(), stage.path()).unwrap();
        catalog.object(Path::new("old"), &previous, 3).unwrap();
        let mut result = catalog.finish("fixture", previous, b"inventory").unwrap();
        assert_eq!(result.install(root.path()).unwrap(), 2);
        result.verify_files().unwrap();
        assert_eq!(result.install(root.path()).unwrap(), 0);
        let path = root
            .path()
            .join(crate::namespace("objects"))
            .join(crate::digest(b"abc"));
        assert_eq!(fs::read(path).unwrap(), b"abc");
    }
}
