//! The query server's durable publication floor: the newest accepted pointer,
//! the source it came from, and every permanently invalidated serving view.
//!
//! A floor only ever moves forward. It is never reset by a restart or a format
//! change, and it never grants permission to serve by itself.
use crate::{MaterialIdentity, PointerSuccession, PublicationPointer, PublicationReplay};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fmt, fs, io,
    path::{Path, PathBuf},
};

/// The publication source a floor is bound to: the publisher's HTTPS origin,
/// the product's distribution namespace and the chain. A floor saved for one
/// source refuses to open for another, so a wrong publisher, product or chain
/// cannot inherit or reset another source's replay protection.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceBinding {
    pub publisher_origin: String,
    pub product_namespace: String,
    pub chain: [u8; 32],
}

/// Why a floor could not be opened or saved.
#[derive(Debug)]
pub enum AuthorityFloorError {
    /// The saved floor belongs to a different publisher, product or chain.
    WrongSource {
        saved: SourceBinding,
        configured: SourceBinding,
    },
    /// The saved floor is unreadable or not a floor.
    Corrupt(serde_json::Error),
    Io(io::Error),
}
impl fmt::Display for AuthorityFloorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongSource { saved, configured } => write!(
                f,
                "publication floor belongs to {saved:?}, not the configured {configured:?}"
            ),
            Self::Corrupt(error) => write!(f, "publication floor is corrupt: {error}"),
            Self::Io(error) => write!(f, "publication floor I/O failed: {error}"),
        }
    }
}
impl std::error::Error for AuthorityFloorError {}
impl From<io::Error> for AuthorityFloorError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Saved representation. `source` is absent in floors written before source
/// binding existed; such a floor keeps its pointer and invalidations and is
/// bound to the configured source on its next save.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedFloor<P> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source: Option<SourceBinding>,
    current: Option<P>,
    invalid: BTreeSet<MaterialIdentity>,
}

/// The floor as last observed in memory, and which version of it is durable.
///
/// Every change bumps `observed_version`. The floor is *dirty* until a
/// complete write of the latest version has been synced; a dirty floor permits
/// no positive progression (accepting a pointer as fresh, committing a view),
/// even when the next pointer is unchanged, until a later write succeeds.
pub struct AuthorityFloor<P> {
    path: PathBuf,
    source: SourceBinding,
    pointer: Option<P>,
    invalidated_views: BTreeSet<MaterialIdentity>,
    observed_version: u64,
    durable_version: Option<u64>,
}

/// A pointer [`AuthorityFloor::classify`] accepted as a successor of one exact
/// floor version. Only this token can raise the floor, so no caller can
/// advance it without the succession check.
pub struct CheckedSuccession<P> {
    pointer: P,
    succession: PointerSuccession,
    floor_version: u64,
}
impl<P> CheckedSuccession<P> {
    pub fn succession(&self) -> PointerSuccession {
        self.succession
    }
    pub fn pointer(&self) -> &P {
        &self.pointer
    }
}

/// The floor changed after the succession was checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaleSuccessionCheck;
impl fmt::Display for StaleSuccessionCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("publication floor changed after the succession check")
    }
}
impl std::error::Error for StaleSuccessionCheck {}

impl<P: PublicationPointer + Serialize + DeserializeOwned> AuthorityFloor<P> {
    /// Open the floor at `path`, or an empty floor when none was saved. A floor
    /// without a saved source binding (new, or written before binding existed)
    /// opens dirty, so the binding is made durable before any progression.
    pub fn open(path: PathBuf, source: SourceBinding) -> Result<Self, AuthorityFloorError> {
        let saved = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<SavedFloor<P>>(&bytes)
                .map_err(AuthorityFloorError::Corrupt)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => SavedFloor {
                source: None,
                current: None,
                invalid: BTreeSet::new(),
            },
            Err(error) => return Err(error.into()),
        };
        let durable_version = match saved.source {
            Some(saved) if saved != source => {
                return Err(AuthorityFloorError::WrongSource {
                    saved,
                    configured: source,
                })
            }
            Some(_) => Some(0),
            None => None,
        };
        Ok(Self {
            path,
            source,
            pointer: saved.current,
            invalidated_views: saved.invalid,
            observed_version: 0,
            durable_version,
        })
    }
    pub fn pointer(&self) -> Option<&P> {
        self.pointer.as_ref()
    }
    pub fn source(&self) -> &SourceBinding {
        &self.source
    }
    pub fn is_invalidated(&self, view: &MaterialIdentity) -> bool {
        self.invalidated_views.contains(view)
    }
    /// Whether the latest observed floor is durably saved.
    pub fn is_durable(&self) -> bool {
        self.durable_version == Some(self.observed_version)
    }
    /// Check `next` against the floor without changing it.
    pub fn classify(&self, next: P) -> Result<CheckedSuccession<P>, PublicationReplay> {
        let succession = PointerSuccession::classify(&next, self.pointer.as_ref())?;
        Ok(CheckedSuccession {
            pointer: next,
            succession,
            floor_version: self.observed_version,
        })
    }
    /// Raise the floor to a checked successor and permanently invalidate
    /// `fenced_views`. The caller fences serving first and saves afterwards;
    /// the floor stays dirty until then. An unchanged pointer with no new
    /// invalidation changes nothing.
    pub fn record(
        &mut self,
        checked: CheckedSuccession<P>,
        fenced_views: impl IntoIterator<Item = MaterialIdentity>,
    ) -> Result<(), StaleSuccessionCheck> {
        if checked.floor_version != self.observed_version {
            return Err(StaleSuccessionCheck);
        }
        let newly_invalidated = self.extend_invalidations(fenced_views);
        if checked.succession != PointerSuccession::Unchanged || newly_invalidated {
            self.pointer = Some(checked.pointer);
            self.observed_version += 1;
        }
        Ok(())
    }
    /// Permanently invalidate `views` without changing the pointer. The floor
    /// becomes dirty when any view is newly invalidated.
    pub fn invalidate(&mut self, views: impl IntoIterator<Item = MaterialIdentity>) {
        if self.extend_invalidations(views) {
            self.observed_version += 1;
        }
    }
    fn extend_invalidations(&mut self, views: impl IntoIterator<Item = MaterialIdentity>) -> bool {
        let before = self.invalidated_views.len();
        self.invalidated_views.extend(views);
        self.invalidated_views.len() != before
    }
    /// An owned durable write of the latest floor, runnable off the async
    /// executor. A failed write may have replaced the file; callers treat any
    /// error as a reason to close admission and keep the floor dirty.
    pub fn pending_write(&self) -> Result<PendingFloorWrite, AuthorityFloorError> {
        let bytes = serde_json::to_vec(&SavedFloor {
            source: Some(self.source.clone()),
            current: self.pointer.clone(),
            invalid: self.invalidated_views.clone(),
        })
        .map_err(AuthorityFloorError::Corrupt)?;
        Ok(PendingFloorWrite {
            path: self.path.clone(),
            bytes,
            floor_version: self.observed_version,
        })
    }
    /// Record that `write` (from [`Self::pending_write`]) completed. Only the
    /// latest version makes the floor durable; an older completed write does not.
    pub fn confirm_durable(&mut self, written: WrittenFloor) {
        if written.floor_version == self.observed_version {
            self.durable_version = Some(written.floor_version);
        }
    }
}

/// Bytes of one floor version and where they are made durable.
pub struct PendingFloorWrite {
    path: PathBuf,
    bytes: Vec<u8>,
    floor_version: u64,
}
/// Proof that one floor version was synced.
pub struct WrittenFloor {
    floor_version: u64,
}
impl PendingFloorWrite {
    /// Atomically replace the floor file and sync its directory.
    pub fn write(self) -> io::Result<WrittenFloor> {
        let directory = self.path.parent().unwrap_or(Path::new("."));
        let name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("floor path has no file name"))?;
        pir_control::durable::write_atomic(directory, name, |file| file.write_all(&self.bytes))?;
        pir_control::durable::sync_dir(directory)?;
        Ok(WrittenFloor {
            floor_version: self.floor_version,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct SavedPointer {
        revision: u64,
        recovery_epoch: u64,
        generation: u64,
        eligible: bool,
    }
    impl PublicationPointer for SavedPointer {
        fn revision(&self) -> u64 {
            self.revision
        }
        fn recovery_epoch(&self) -> u64 {
            self.recovery_epoch
        }
        fn eligible(&self) -> bool {
            self.eligible
        }
        fn inventory(&self) -> Option<&crate::ObjectReference> {
            None
        }
        fn with_eligibility(&self, eligible: bool) -> Self {
            Self {
                eligible,
                ..self.clone()
            }
        }
        fn may_follow(&self, previous: &Self) -> bool {
            self.generation >= previous.generation
        }
    }
    fn source(origin: &str) -> SourceBinding {
        SourceBinding {
            publisher_origin: origin.into(),
            product_namespace: "/provider/test/v1/".into(),
            chain: [1; 32],
        }
    }
    fn pointer(revision: u64) -> SavedPointer {
        SavedPointer {
            revision,
            recovery_epoch: 1,
            generation: 1,
            eligible: true,
        }
    }

    #[test]
    fn floors_survive_restart_and_refuse_a_different_source() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("floor.json");
        let mut floor = AuthorityFloor::open(path.clone(), source("https://a")).unwrap();
        assert!(
            !floor.is_durable(),
            "a new source binding is not yet durable"
        );
        let checked = floor.classify(pointer(4)).unwrap();
        floor.record(checked, [[9; 32]]).unwrap();
        let written = floor.pending_write().unwrap().write().unwrap();
        floor.confirm_durable(written);
        assert!(floor.is_durable());

        let reopened =
            AuthorityFloor::<SavedPointer>::open(path.clone(), source("https://a")).unwrap();
        assert_eq!(reopened.pointer(), Some(&pointer(4)));
        assert!(reopened.is_invalidated(&[9; 32]));
        assert!(reopened.is_durable());
        assert!(reopened.classify(pointer(3)).is_err());

        for wrong in [
            source("https://b"),
            SourceBinding {
                product_namespace: "/provider/other/v1/".into(),
                ..source("https://a")
            },
            SourceBinding {
                chain: [2; 32],
                ..source("https://a")
            },
        ] {
            assert!(matches!(
                AuthorityFloor::<SavedPointer>::open(path.clone(), wrong),
                Err(AuthorityFloorError::WrongSource { .. })
            ));
        }
    }

    #[test]
    fn unbound_floors_keep_their_replay_protection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("floor.json");
        fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({
                "current": pointer(7),
                "invalid": vec![[3u8; 32]],
            }))
            .unwrap(),
        )
        .unwrap();
        let mut floor =
            AuthorityFloor::<SavedPointer>::open(path.clone(), source("https://a")).unwrap();
        assert!(
            !floor.is_durable(),
            "the binding must be saved before progression"
        );
        assert!(floor.classify(pointer(6)).is_err());
        assert!(floor.is_invalidated(&[3; 32]));
        let written = floor.pending_write().unwrap().write().unwrap();
        floor.confirm_durable(written);
        assert!(floor.is_durable());
        assert!(AuthorityFloor::<SavedPointer>::open(path, source("https://b")).is_err());
    }

    #[test]
    fn a_failed_save_keeps_the_floor_dirty_until_the_latest_version_is_written() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("floor.json");
        let mut floor = AuthorityFloor::open(path.clone(), source("https://a")).unwrap();
        let first = floor.pending_write().unwrap().write().unwrap();
        floor.confirm_durable(first);

        // A write prepared before a later change cannot make the newer floor durable.
        let stale = floor.pending_write().unwrap();
        let checked = floor.classify(pointer(5)).unwrap();
        floor.record(checked, []).unwrap();
        let stale = stale.write().unwrap();
        floor.confirm_durable(stale);
        assert!(!floor.is_durable());

        // Repeating the identical pointer does not hide the pending save.
        let unchanged = floor.classify(pointer(5)).unwrap();
        assert_eq!(unchanged.succession(), PointerSuccession::Unchanged);
        floor.record(unchanged, []).unwrap();
        assert!(!floor.is_durable());

        // A newly invalidated view also makes the floor dirty.
        let written = floor.pending_write().unwrap().write().unwrap();
        floor.confirm_durable(written);
        floor.invalidate([[7; 32]]);
        assert!(!floor.is_durable());
        floor.invalidate([[7; 32]]);
        let written = floor.pending_write().unwrap().write().unwrap();
        floor.confirm_durable(written);
        assert!(floor.is_durable());

        // A check against an older floor version cannot be recorded.
        let checked = floor.classify(pointer(6)).unwrap();
        floor.invalidate([[8; 32]]);
        assert_eq!(floor.record(checked, []), Err(StaleSuccessionCheck));

        let reopened = AuthorityFloor::<SavedPointer>::open(path, source("https://a")).unwrap();
        assert!(reopened.classify(pointer(4)).is_err());
        assert!(reopened.is_invalidated(&[7; 32]));
    }
}
