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

pub struct AuthorityFloor<P> {
    path: PathBuf,
    source: SourceBinding,
    pointer: Option<P>,
    invalidated_views: BTreeSet<MaterialIdentity>,
}

impl<P: PublicationPointer + Serialize + DeserializeOwned> AuthorityFloor<P> {
    /// Open the floor at `path`, or an empty floor when none was saved.
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
        if let Some(saved) = saved.source.filter(|saved| *saved != source) {
            return Err(AuthorityFloorError::WrongSource {
                saved,
                configured: source,
            });
        }
        Ok(Self {
            path,
            source,
            pointer: saved.current,
            invalidated_views: saved.invalid,
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
    /// Classify `next` without changing the floor.
    pub fn classify(&self, next: &P) -> Result<PointerSuccession, PublicationReplay> {
        PointerSuccession::classify(next, self.pointer.as_ref())
    }
    /// Raise the floor to `next` (already classified as a successor) and
    /// permanently invalidate `fenced_views`. The caller fences serving before
    /// calling this and saves afterwards.
    pub fn advance(&mut self, next: P, fenced_views: impl IntoIterator<Item = MaterialIdentity>) {
        self.pointer = Some(next);
        self.invalidated_views.extend(fenced_views);
    }
    /// Permanently invalidate `views` without changing the pointer. Returns
    /// whether any view was newly invalidated and the floor must be saved.
    pub fn invalidate(&mut self, views: impl IntoIterator<Item = MaterialIdentity>) -> bool {
        let before = self.invalidated_views.len();
        self.invalidated_views.extend(views);
        self.invalidated_views.len() != before
    }
    /// An owned durable write of the current floor, runnable off the async
    /// executor. A failed write may have replaced the file; callers treat any
    /// error as a reason to close admission.
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
        })
    }
}

/// Bytes of one floor and where they are made durable.
pub struct PendingFloorWrite {
    path: PathBuf,
    bytes: Vec<u8>,
}
impl PendingFloorWrite {
    /// Atomically replace the floor file and sync its directory.
    pub fn write(self) -> io::Result<()> {
        let directory = self.path.parent().unwrap_or(Path::new("."));
        let name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("floor path has no file name"))?;
        pir_control::durable::write_atomic(directory, name, |file| file.write_all(&self.bytes))?;
        pir_control::durable::sync_dir(directory)
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
        floor.advance(pointer(4), [[9; 32]]);
        floor.pending_write().unwrap().write().unwrap();

        let reopened =
            AuthorityFloor::<SavedPointer>::open(path.clone(), source("https://a")).unwrap();
        assert_eq!(reopened.pointer(), Some(&pointer(4)));
        assert!(reopened.is_invalidated(&[9; 32]));
        assert!(reopened.classify(&pointer(3)).is_err());

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
        let floor =
            AuthorityFloor::<SavedPointer>::open(path.clone(), source("https://a")).unwrap();
        assert!(floor.classify(&pointer(6)).is_err());
        assert!(floor.is_invalidated(&[3; 32]));
        floor.pending_write().unwrap().write().unwrap();
        assert!(AuthorityFloor::<SavedPointer>::open(path, source("https://b")).is_err());
    }
}
