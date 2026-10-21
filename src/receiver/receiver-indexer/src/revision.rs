//! The history correction floor: the highest correction revision a service has accepted.
//!
//! Every path that raises the revision goes through [`advance`], which refuses one below
//! the floor and makes a higher one durable before anything relies on it: before serving
//! state advances, which revokes older sessions, and before any further fallible work. A
//! restart then reads the floor back with [`restore`], so a crash or a rollback of the
//! configured revision can never serve older, uncorrected history again.
use receiver_pir_server::Publications;
use std::path::{Path, PathBuf};

/// Where a service keeps its floor.
pub trait Floor {
    type Error: std::fmt::Display;
    /// The recorded floor, zero if none.
    fn floor(&mut self) -> Result<u64, Self::Error>;
    /// Durably records `revision`, which is above the floor, as the new floor.
    fn record(&mut self, revision: u64) -> Result<(), Self::Error>;
}

/// Why [`advance`] refused a revision.
#[derive(Debug, thiserror::Error)]
pub enum Refused<E: std::fmt::Display> {
    /// The revision is below the recorded or served one.
    #[error("history correction revision {revision} is below {floor}")]
    Lower { revision: u64, floor: u64 },
    /// The floor could not be read or recorded; nothing advanced.
    #[error("history correction floor: {0}")]
    Floor(E),
}

/// Accepts correction `revision`: refuses one below `floor`'s record, durably records a
/// higher one, and only then advances `serving` to it.
pub fn advance<F: Floor>(
    floor: &mut F,
    serving: Option<&Publications>,
    revision: u64,
) -> Result<(), Refused<F::Error>> {
    let recorded = floor.floor().map_err(Refused::Floor)?;
    if revision < recorded {
        return Err(Refused::Lower {
            revision,
            floor: recorded,
        });
    }
    if revision > recorded {
        floor.record(revision).map_err(Refused::Floor)?;
    }
    if serving.is_some_and(|s| !s.advance_history_revision(revision)) {
        return Err(Refused::Lower {
            revision,
            floor: recorded,
        });
    }
    Ok(())
}

/// Advances fresh serving state `serving` to `floor`'s record, so sessions of older
/// revisions stay fenced across a restart.
pub fn restore<F: Floor>(floor: &mut F, serving: &Publications) -> Result<(), F::Error> {
    serving.advance_history_revision(floor.floor()?);
    Ok(())
}

impl Floor for receiver_store::Store {
    type Error = receiver_store::Error;

    fn floor(&mut self) -> Result<u64, Self::Error> {
        self.history_revision()
    }

    fn record(&mut self, revision: u64) -> Result<(), Self::Error> {
        self.raise_history_revision(revision)
    }
}

/// A staged server's floor: a file in its state directory, synced with the directory.
pub struct StateFile(pub PathBuf);

impl StateFile {
    /// The floor file `name` in `state_dir`.
    pub fn new(state_dir: &Path, name: &str) -> Self {
        Self(state_dir.join(name))
    }
}

impl Floor for StateFile {
    type Error = std::io::Error;

    fn floor(&mut self) -> Result<u64, Self::Error> {
        match std::fs::read(&self.0) {
            Ok(bytes) if bytes.len() <= 64 => {
                serde_json::from_slice(&bytes).map_err(std::io::Error::other)
            }
            Ok(_) => Err(std::io::Error::other("larger than 64 bytes")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e),
        }
    }

    fn record(&mut self, revision: u64) -> Result<(), Self::Error> {
        use std::io::Write;
        let parent = self.0.parent().ok_or(std::io::ErrorKind::InvalidInput)?;
        let mut temp = tempfile::NamedTempFile::new_in(parent)?;
        temp.write_all(revision.to_string().as_bytes())?;
        temp.as_file().sync_all()?;
        temp.persist(&self.0).map_err(|e| e.error)?;
        // The rename is durable only once the directory holding it is synced.
        std::fs::File::open(parent)?.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A floor that records in memory and fails to record when told to.
    #[derive(Default)]
    struct Memory {
        floor: u64,
        fail: bool,
    }

    impl Floor for Memory {
        type Error = &'static str;

        fn floor(&mut self) -> Result<u64, Self::Error> {
            Ok(self.floor)
        }

        fn record(&mut self, revision: u64) -> Result<(), Self::Error> {
            match self.fail {
                true => Err("disk full"),
                false => {
                    self.floor = revision;
                    Ok(())
                }
            }
        }
    }

    /// Serving state advances only once a higher floor is recorded; a lower revision is
    /// refused, and a failed record leaves serving untouched.
    #[test]
    fn serving_advances_only_after_the_floor_is_recorded() {
        let serving = Publications::default();
        let mut floor = Memory {
            fail: true,
            ..Memory::default()
        };
        assert!(matches!(
            advance(&mut floor, Some(&serving), 2),
            Err(Refused::Floor(_))
        ));
        assert_eq!(serving.epoch(), 0, "nothing was revoked");
        floor.fail = false;
        advance(&mut floor, Some(&serving), 2).unwrap();
        assert_eq!((floor.floor, serving.epoch()), (2, 1));
        assert!(matches!(
            advance(&mut floor, Some(&serving), 1),
            Err(Refused::Lower { floor: 2, .. })
        ));
        let restarted = Publications::default();
        restore(&mut floor, &restarted).unwrap();
        assert!(!restarted.advance_history_revision(1));
    }

    /// The state file reads back what it recorded, is zero when absent, and refuses an
    /// unreadable record rather than guess.
    #[test]
    fn the_state_file_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = StateFile::new(dir.path(), "floor.json");
        assert_eq!(file.floor().unwrap(), 0);
        file.record(3).unwrap();
        assert_eq!(StateFile::new(dir.path(), "floor.json").floor().unwrap(), 3);
        std::fs::write(dir.path().join("floor.json"), "x").unwrap();
        assert!(file.floor().is_err());
    }
}
