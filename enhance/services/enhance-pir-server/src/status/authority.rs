//! Durable Status publication counter. Opening always starts a new recovery
//! epoch; restored metadata alone never makes a serving generation ready.
use enhance_pir::status::{Error as StatusError, Hash, Manifest};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

const VERSION: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum AuthorityError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Status(#[from] StatusError),
    #[error("incompatible or corrupt Status publication state")]
    State,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    version: u32,
    network: Hash,
    salt: Hash,
    generation: u64,
    recovery_epoch: u64,
    last_manifest: Option<Hash>,
}

pub struct Authority {
    dir: PathBuf,
    _lock: File,
    state: State,
    poisoned: bool,
}

impl Authority {
    pub fn open(path: impl AsRef<Path>, network: Hash, salt: Hash) -> Result<Self, AuthorityError> {
        if network == [0; 32] || salt == [0; 32] {
            return Err(AuthorityError::State);
        }
        fs::create_dir_all(path.as_ref())?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref().join("authority.lock"))?;
        lock.try_lock()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let state_path = path.as_ref().join("authority.json");
        let mut state: State = if state_path.exists() {
            serde_json::from_slice(&fs::read(state_path)?)?
        } else {
            State {
                version: VERSION,
                network,
                salt,
                generation: 0,
                recovery_epoch: 0,
                last_manifest: None,
            }
        };
        if state.version != VERSION
            || state.network != network
            || state.salt != salt
            || (state.generation == 0) != state.last_manifest.is_none()
        {
            return Err(AuthorityError::State);
        }
        state.recovery_epoch = state
            .recovery_epoch
            .checked_add(1)
            .ok_or(AuthorityError::State)?;
        // Persist the fence before admitting a new observation or query.
        persist(path.as_ref(), &state)?;
        Ok(Self {
            dir: path.as_ref().to_path_buf(),
            _lock: lock,
            state,
            poisoned: false,
        })
    }

    pub fn next_identity(&self) -> Result<(u64, u64), AuthorityError> {
        if self.poisoned {
            return Err(AuthorityError::State);
        }
        Ok((
            self.state
                .generation
                .checked_add(1)
                .ok_or(AuthorityError::State)?,
            self.state.recovery_epoch,
        ))
    }

    /// Commit before installing material in a serving controller. A crash
    /// after this commit leaves a gap, then a new recovery epoch on reopen.
    pub fn commit(&mut self, manifest: &Manifest) -> Result<(), AuthorityError> {
        manifest.fresh(super::now_ms())?;
        let (generation, epoch) = self.next_identity()?;
        if manifest.network != self.state.network
            || manifest.salt != self.state.salt
            || manifest.generation != generation
            || manifest.recovery_epoch != epoch
        {
            return Err(AuthorityError::State);
        }
        let next = State {
            version: VERSION,
            network: self.state.network,
            salt: self.state.salt,
            generation,
            recovery_epoch: epoch,
            last_manifest: Some(manifest.id()),
        };
        if let Err(error) = persist(&self.dir, &next) {
            // A rename may have happened even when directory sync failed.
            // Require a reopen and new recovery epoch before further commits.
            self.poisoned = true;
            return Err(error);
        }
        self.state = next;
        Ok(())
    }
}

fn persist(dir: &Path, state: &State) -> Result<(), AuthorityError> {
    let temp = dir.join("authority.json.tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp)?;
    file.write_all(&serde_json::to_vec(state)?)?;
    file.sync_all()?;
    fs::rename(temp, dir.join("authority.json"))?;
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use enhance_pir::status::PROTOCOL;

    fn manifest(generation: u64, recovery_epoch: u64) -> Manifest {
        Manifest {
            protocol: PROTOCOL.into(),
            network: [1; 32],
            salt: [2; 32],
            generation,
            recovery_epoch,
            coverage_start: 1,
            anchor_height: 1,
            anchor_hash: [3; 32],
            observed_ms: super::super::now_ms(),
            entries: 1,
            rows_digest: [4; 32],
            public_digest: [5; 32],
        }
    }

    #[test]
    fn restart_fences_prior_sessions_and_never_reuses_generation() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut authority = Authority::open(dir.path(), [1; 32], [2; 32]).unwrap();
            assert_eq!(authority.next_identity().unwrap(), (1, 1));
            authority.commit(&manifest(1, 1)).unwrap();
            assert_eq!(authority.next_identity().unwrap(), (2, 1));
        }
        let mut recovered = Authority::open(dir.path(), [1; 32], [2; 32]).unwrap();
        assert_eq!(recovered.next_identity().unwrap(), (2, 2));
        assert!(matches!(
            recovered.commit(&manifest(2, 1)),
            Err(AuthorityError::State)
        ));
        recovered.commit(&manifest(2, 2)).unwrap();
        assert_eq!(recovered.next_identity().unwrap(), (3, 2));
    }

    #[test]
    fn wrong_network_and_corruption_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        drop(Authority::open(dir.path(), [1; 32], [2; 32]).unwrap());
        assert!(matches!(
            Authority::open(dir.path(), [3; 32], [2; 32]),
            Err(AuthorityError::State)
        ));
        fs::write(dir.path().join("authority.json"), b"broken").unwrap();
        assert!(Authority::open(dir.path(), [1; 32], [2; 32]).is_err());
    }
}
