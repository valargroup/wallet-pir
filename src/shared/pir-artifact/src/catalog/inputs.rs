//! Retain an entire batch before hashing so publisher collection cannot race it.
use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RootState {
    root: PathBuf,
    parent: PathBuf,
    owner: DirectoryOwner,
}

impl RootState {
    pub(super) fn open(root: &Path, stage: &Path) -> io::Result<Self> {
        let parent = root
            .parent()
            .ok_or_else(|| io::Error::other("publication root has no parent"))?
            .canonicalize()?;
        match fs::symlink_metadata(root) {
            Ok(_) => Ok(Self {
                root: root.into(),
                parent,
                owner: DirectoryOwner::read(root)?,
            }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let saved: Self =
                    serde_json::from_slice(&fs::read(stage.join("root-state.json"))?)?;
                if saved.root != root || saved.parent != parent {
                    return Err(io::Error::other(
                        "source pin belongs to another publication root",
                    ));
                }
                Ok(saved)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn canonical(&self) -> io::Result<PathBuf> {
        Ok(self.parent.join(
            self.root
                .file_name()
                .ok_or_else(|| io::Error::other("publication root has no name"))?,
        ))
    }

    pub(super) fn retain(self, stage: &Path) -> io::Result<DirectoryOwner> {
        migration::persist(&stage.join("root-state.json"), &serde_json::to_vec(&self)?)?;
        Ok(self.owner)
    }
}

impl Catalog {
    /// Pin every declared input before any large hash starts. Existing pins
    /// remain immutable and are checked by the normal expected-digest path.
    /// A complete pin also lets an interrupted preparation resume after live
    /// publication collection removes the original source directory.
    pub fn pin_inputs(&mut self, inputs: &[(PathBuf, Checksum, u64)]) -> io::Result<()> {
        let mirror = self.stage.join("source-pin");
        let owner = DirectoryOwner::read(&self.stage)?;
        owner.create(&mirror)?;
        for (relative, previous, bytes) in inputs {
            safe_relative(relative)?;
            previous.validate()?;
            let target = mirror.join(relative);
            let mut parent = mirror.clone();
            for component in relative.parent().unwrap_or(Path::new("")).components() {
                parent.push(component);
                owner.create(&parent)?;
            }
            match fs::symlink_metadata(&target) {
                Ok(metadata) if metadata.is_file() && metadata.len() == *bytes => continue,
                Ok(_) => return Err(io::Error::other("source pin has an invalid type or length")),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            reject_symlinks(&self.source_root, relative)?;
            migration::pin_unverified(
                &self.checkpoints,
                &self.source_root.join(relative),
                previous,
                *bytes,
                &target,
            )?;
        }
        self.source_root = mirror;
        Ok(())
    }
}
