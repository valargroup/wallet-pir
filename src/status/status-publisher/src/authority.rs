//! Durable Status generation counter. Recovery epochs belong to the shared
//! publication authority; restored metadata alone never makes a serving
//! generation ready.
use serde::{Deserialize, Serialize};
use status_material::protocol::{Error as StatusError, Hash, Manifest};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
};

const VERSION: u32 = 2;

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
    last_manifest: Option<Hash>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SavedState {
    Current(State),
    BeforePublicationAuthority(PreviousState),
}

/// The counter saved before recovery epochs moved to the shared authority.
/// Only the generation counter and its last manifest survive the rewrite.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PreviousState {
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
        let state = if state_path.exists() {
            match serde_json::from_slice::<SavedState>(&fs::read(state_path)?)? {
                SavedState::Current(state) => state,
                SavedState::BeforePublicationAuthority(previous)
                    if previous.version == 1 && previous.recovery_epoch > 0 =>
                {
                    State {
                        version: VERSION,
                        network: previous.network,
                        salt: previous.salt,
                        generation: previous.generation,
                        last_manifest: previous.last_manifest,
                    }
                }
                SavedState::BeforePublicationAuthority(_) => return Err(AuthorityError::State),
            }
        } else {
            State {
                version: VERSION,
                network,
                salt,
                generation: 0,
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
        // Rewrite the previous format under the state lock before any new
        // publication can use its counter. A failed sync refuses the open.
        persist(path.as_ref(), &state)?;
        Ok(Self {
            dir: path.as_ref().to_path_buf(),
            _lock: lock,
            state,
            poisoned: false,
        })
    }

    pub fn next_generation(&self) -> Result<u64, AuthorityError> {
        if self.poisoned {
            return Err(AuthorityError::State);
        }
        self.state
            .generation
            .checked_add(1)
            .ok_or(AuthorityError::State)
    }

    /// Commit before publishing material. A crash after this commit leaves a
    /// generation gap; generations are never reused.
    pub fn commit(&mut self, manifest: &Manifest) -> Result<(), AuthorityError> {
        manifest.fresh(super::now_ms())?;
        let generation = self.next_generation()?;
        if manifest.network != self.state.network
            || manifest.salt != self.state.salt
            || manifest.generation != generation
        {
            return Err(AuthorityError::State);
        }
        let next = State {
            version: VERSION,
            network: self.state.network,
            salt: self.state.salt,
            generation,
            last_manifest: Some(manifest.id()),
        };
        if let Err(error) = persist(&self.dir, &next) {
            // A rename may have happened even when directory sync failed.
            // Require a reopen before further commits.
            self.poisoned = true;
            return Err(error);
        }
        self.state = next;
        Ok(())
    }
}

fn persist(dir: &Path, state: &State) -> Result<(), AuthorityError> {
    let bytes = serde_json::to_vec(state)?;
    pir_control::durable::write_atomic(dir, "authority.json", |file| file.write_all(&bytes))?;
    pir_control::durable::sync_dir(dir)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use status_material::protocol::PROTOCOL;

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
    fn restart_never_reuses_a_generation() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut authority = Authority::open(dir.path(), [1; 32], [2; 32]).unwrap();
            assert_eq!(authority.next_generation().unwrap(), 1);
            authority.commit(&manifest(1, 1)).unwrap();
            assert_eq!(authority.next_generation().unwrap(), 2);
        }
        let mut recovered = Authority::open(dir.path(), [1; 32], [2; 32]).unwrap();
        assert_eq!(recovered.next_generation().unwrap(), 2);
        assert!(matches!(
            recovered.commit(&manifest(1, 2)),
            Err(AuthorityError::State)
        ));
        recovered.commit(&manifest(2, 2)).unwrap();
        assert_eq!(recovered.next_generation().unwrap(), 3);
    }

    fn previous_counter() -> serde_json::Value {
        serde_json::json!({
            "version": 1,
            "network": vec![1; 32],
            "salt": vec![2; 32],
            "generation": 7,
            "recovery_epoch": 12,
            "last_manifest": vec![9; 32],
        })
    }

    #[test]
    fn previous_counter_is_rewritten_without_reusing_a_generation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("authority.json");
        let previous = previous_counter();
        fs::write(&path, serde_json::to_vec(&previous).unwrap()).unwrap();
        {
            let mut authority = Authority::open(dir.path(), [1; 32], [2; 32]).unwrap();
            assert_eq!(authority.next_generation().unwrap(), 8);
            let saved: serde_json::Value =
                serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            assert_eq!(saved["version"], VERSION);
            assert_eq!(saved["generation"], previous["generation"]);
            assert_eq!(saved["last_manifest"], previous["last_manifest"]);
            assert!(saved.get("recovery_epoch").is_none());
            authority.commit(&manifest(8, 1)).unwrap();
        }
        let reopened = Authority::open(dir.path(), [1; 32], [2; 32]).unwrap();
        assert_eq!(reopened.next_generation().unwrap(), 9);
    }

    #[test]
    fn previous_empty_and_exhausted_counters_keep_their_bounds() {
        for generation in [0, u64::MAX] {
            let dir = tempfile::tempdir().unwrap();
            let mut previous = previous_counter();
            previous["generation"] = serde_json::json!(generation);
            if generation == 0 {
                previous["last_manifest"] = serde_json::Value::Null;
            }
            fs::write(
                dir.path().join("authority.json"),
                serde_json::to_vec(&previous).unwrap(),
            )
            .unwrap();
            let authority = Authority::open(dir.path(), [1; 32], [2; 32]).unwrap();
            assert_eq!(authority.next_generation().ok(), generation.checked_add(1));
        }
    }

    #[test]
    fn incompatible_previous_counters_are_refused_without_rewriting() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("authority.json");
        let mut malformed = Vec::new();
        for (field, value) in [
            ("version", serde_json::json!(3)),
            ("recovery_epoch", serde_json::json!(0)),
            ("network", serde_json::json!(vec![3; 32])),
            ("salt", serde_json::json!(vec![3; 32])),
            ("generation", serde_json::json!(0)),
            ("last_manifest", serde_json::Value::Null),
        ] {
            let mut previous = previous_counter();
            previous[field] = value;
            malformed.push(previous);
        }
        let mut missing_epoch = previous_counter();
        missing_epoch
            .as_object_mut()
            .unwrap()
            .remove("recovery_epoch");
        malformed.push(missing_epoch);
        let mut unknown_field = previous_counter();
        unknown_field["unexpected"] = serde_json::json!(1);
        malformed.push(unknown_field);
        for previous in malformed {
            let bytes = serde_json::to_vec(&previous).unwrap();
            fs::write(&path, &bytes).unwrap();
            assert!(
                Authority::open(dir.path(), [1; 32], [2; 32]).is_err(),
                "{previous}"
            );
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
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
