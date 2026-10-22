//! The query server's durable publication floor: the newest accepted envelope,
//! the source it came from, and every permanently invalidated serving view.
//!
//! A floor only ever moves forward. It is never reset by a restart or a format
//! change, and it never grants permission to serve by itself.
use crate::{EnvelopeSuccession, PublicationEnvelope, PublicationReplay, ViewIdentity};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fmt, fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
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
        saved: Box<SourceBinding>,
        configured: Box<SourceBinding>,
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
/// binding existed; such a floor keeps its envelope and invalidations and is
/// bound to the configured source on its next save.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedFloor<D> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source: Option<SourceBinding>,
    current: Option<PublicationEnvelope<D>>,
    invalid: BTreeSet<ViewIdentity>,
}

/// One version of one opened floor. Tokens carry it so a succession check or
/// a completed write applies only to the floor instance that issued it, never
/// to another floor that happens to be at the same version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FloorVersion {
    floor: u64,
    version: u64,
}

/// Distinguishes floor instances within this process.
static NEXT_FLOOR: AtomicU64 = AtomicU64::new(0);

/// The floor as last observed in memory, and which version of it is durable.
///
/// Every change bumps `observed_version`. The floor is *dirty* until a
/// complete write of the latest version has been synced; a dirty floor permits
/// no positive progression (accepting an envelope as fresh, committing a
/// view), even when the next envelope is unchanged, until a later write
/// succeeds.
pub struct AuthorityFloor<D> {
    floor: u64,
    path: PathBuf,
    source: SourceBinding,
    envelope: Option<PublicationEnvelope<D>>,
    invalidated_views: BTreeSet<ViewIdentity>,
    observed_version: u64,
    durable_version: Option<u64>,
}

/// An envelope [`AuthorityFloor::classify`] accepted as a successor of one
/// exact version of one floor. Only this token can raise that floor, so no
/// caller can advance it without the succession check.
pub struct CheckedSuccession<D> {
    envelope: PublicationEnvelope<D>,
    succession: EnvelopeSuccession,
    checked_at: FloorVersion,
}
impl<D> CheckedSuccession<D> {
    pub fn succession(&self) -> EnvelopeSuccession {
        self.succession
    }
    pub fn envelope(&self) -> &PublicationEnvelope<D> {
        &self.envelope
    }
}

/// The floor changed after the succession was checked, or the check came from
/// another floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaleSuccessionCheck;
impl fmt::Display for StaleSuccessionCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("publication floor changed after the succession check")
    }
}
impl std::error::Error for StaleSuccessionCheck {}

impl<D: Clone + PartialEq + Serialize + DeserializeOwned> AuthorityFloor<D> {
    /// Open the floor at `path`, or an empty floor when none was saved. A floor
    /// without a saved source binding (new, or written before binding existed)
    /// opens dirty, so the binding is made durable before any progression.
    pub fn open(path: PathBuf, source: SourceBinding) -> Result<Self, AuthorityFloorError> {
        let saved = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<SavedFloor<D>>(&bytes)
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
                    saved: Box::new(saved),
                    configured: Box::new(source),
                })
            }
            Some(_) => Some(0),
            None => None,
        };
        Ok(Self {
            floor: NEXT_FLOOR.fetch_add(1, Ordering::Relaxed),
            path,
            source,
            envelope: saved.current,
            invalidated_views: saved.invalid,
            observed_version: 0,
            durable_version,
        })
    }
    pub fn envelope(&self) -> Option<&PublicationEnvelope<D>> {
        self.envelope.as_ref()
    }
    pub fn source(&self) -> &SourceBinding {
        &self.source
    }
    pub fn is_invalidated(&self, view: &ViewIdentity) -> bool {
        self.invalidated_views.contains(view)
    }
    /// Whether the latest observed floor is durably saved.
    pub fn is_durable(&self) -> bool {
        self.durable_version == Some(self.observed_version)
    }
    fn current_version(&self) -> FloorVersion {
        FloorVersion {
            floor: self.floor,
            version: self.observed_version,
        }
    }
    /// Check `next` against the floor without changing it.
    pub fn classify(
        &self,
        next: PublicationEnvelope<D>,
    ) -> Result<CheckedSuccession<D>, PublicationReplay> {
        let succession = EnvelopeSuccession::classify(&next, self.envelope.as_ref())?;
        Ok(CheckedSuccession {
            envelope: next,
            succession,
            checked_at: self.current_version(),
        })
    }
    /// Raise the floor to a checked successor and permanently invalidate
    /// `fenced_views`. The caller fences serving first and saves afterwards;
    /// the floor stays dirty until then. An unchanged envelope with no new
    /// invalidation changes nothing.
    pub fn record(
        &mut self,
        checked: CheckedSuccession<D>,
        fenced_views: impl IntoIterator<Item = ViewIdentity>,
    ) -> Result<(), StaleSuccessionCheck> {
        if checked.checked_at != self.current_version() {
            return Err(StaleSuccessionCheck);
        }
        let newly_invalidated = self.extend_invalidations(fenced_views);
        if checked.succession != EnvelopeSuccession::Unchanged || newly_invalidated {
            self.envelope = Some(checked.envelope);
            self.observed_version += 1;
        }
        Ok(())
    }
    /// Permanently invalidate `views` without changing the envelope. The floor
    /// becomes dirty when any view is newly invalidated.
    pub fn invalidate(&mut self, views: impl IntoIterator<Item = ViewIdentity>) {
        if self.extend_invalidations(views) {
            self.observed_version += 1;
        }
    }
    fn extend_invalidations(&mut self, views: impl IntoIterator<Item = ViewIdentity>) -> bool {
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
            current: self.envelope.clone(),
            invalid: self.invalidated_views.clone(),
        })
        .map_err(AuthorityFloorError::Corrupt)?;
        Ok(PendingFloorWrite {
            path: self.path.clone(),
            bytes,
            floor_version: self.current_version(),
        })
    }
    /// Record that `write` (from [`Self::pending_write`]) completed. Only the
    /// latest version of this floor makes it durable; an older completed write,
    /// or one issued by another floor, does not.
    pub fn confirm_durable(&mut self, written: WrittenFloor) {
        if written.floor_version == self.current_version() {
            self.durable_version = Some(self.observed_version);
        }
    }
}

/// Bytes of one floor version and where they are made durable.
pub struct PendingFloorWrite {
    path: PathBuf,
    bytes: Vec<u8>,
    floor_version: FloorVersion,
}
/// Proof that one version of one floor was synced.
pub struct WrittenFloor {
    floor_version: FloorVersion,
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

    type SavedEnvelope = PublicationEnvelope<u64>;
    fn source(origin: &str) -> SourceBinding {
        SourceBinding {
            publisher_origin: origin.into(),
            product_namespace: "/provider/test/v1/".into(),
            chain: [1; 32],
        }
    }
    fn envelope(sequence: u64) -> SavedEnvelope {
        PublicationEnvelope {
            scope: crate::PublicationScope {
                product: "test".into(),
                chain: [1; 32],
                source: [2; 32],
            },
            sequence,
            eligible: false,
            fence: crate::AuthorityFence {
                recovery_epoch: 1,
                latest_withdrawal: 0,
            },
            publication: None,
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
        let checked = floor.classify(envelope(4)).unwrap();
        floor.record(checked, [ViewIdentity([9; 32])]).unwrap();
        let written = floor.pending_write().unwrap().write().unwrap();
        floor.confirm_durable(written);
        assert!(floor.is_durable());

        let reopened = AuthorityFloor::<u64>::open(path.clone(), source("https://a")).unwrap();
        assert_eq!(reopened.envelope(), Some(&envelope(4)));
        assert!(reopened.is_invalidated(&ViewIdentity([9; 32])));
        assert!(reopened.is_durable());
        assert!(reopened.classify(envelope(3)).is_err());

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
                AuthorityFloor::<u64>::open(path.clone(), wrong),
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
                "current": envelope(7),
                "invalid": vec![[3u8; 32]],
            }))
            .unwrap(),
        )
        .unwrap();
        let mut floor = AuthorityFloor::<u64>::open(path.clone(), source("https://a")).unwrap();
        assert!(
            !floor.is_durable(),
            "the binding must be saved before progression"
        );
        assert!(floor.classify(envelope(6)).is_err());
        assert!(floor.is_invalidated(&ViewIdentity([3; 32])));
        let written = floor.pending_write().unwrap().write().unwrap();
        floor.confirm_durable(written);
        assert!(floor.is_durable());
        assert!(AuthorityFloor::<u64>::open(path, source("https://b")).is_err());
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
        let checked = floor.classify(envelope(5)).unwrap();
        floor.record(checked, []).unwrap();
        let stale = stale.write().unwrap();
        floor.confirm_durable(stale);
        assert!(!floor.is_durable());

        // Repeating the identical envelope does not hide the pending save.
        let unchanged = floor.classify(envelope(5)).unwrap();
        assert_eq!(unchanged.succession(), EnvelopeSuccession::Unchanged);
        floor.record(unchanged, []).unwrap();
        assert!(!floor.is_durable());

        // A newly invalidated view also makes the floor dirty.
        let written = floor.pending_write().unwrap().write().unwrap();
        floor.confirm_durable(written);
        floor.invalidate([ViewIdentity([7; 32])]);
        assert!(!floor.is_durable());
        floor.invalidate([ViewIdentity([7; 32])]);
        let written = floor.pending_write().unwrap().write().unwrap();
        floor.confirm_durable(written);
        assert!(floor.is_durable());

        // A check against an older floor version cannot be recorded.
        let checked = floor.classify(envelope(6)).unwrap();
        floor.invalidate([ViewIdentity([8; 32])]);
        assert_eq!(floor.record(checked, []), Err(StaleSuccessionCheck));

        let reopened = AuthorityFloor::<u64>::open(path, source("https://a")).unwrap();
        assert!(reopened.classify(envelope(4)).is_err());
        assert!(reopened.is_invalidated(&ViewIdentity([7; 32])));
    }

    #[test]
    fn tokens_apply_only_to_the_floor_that_issued_them() {
        let dir = tempfile::tempdir().unwrap();
        let open = |name: &str| {
            let mut floor =
                AuthorityFloor::open(dir.path().join(name), source("https://a")).unwrap();
            let written = floor.pending_write().unwrap().write().unwrap();
            floor.confirm_durable(written);
            floor
        };
        let (mut first, mut second) = (open("first.json"), open("second.json"));

        // Both floors are at the same version, but a check of one cannot
        // raise the other.
        let checked = first.classify(envelope(9)).unwrap();
        assert_eq!(second.record(checked, []), Err(StaleSuccessionCheck));
        assert_eq!(second.envelope(), None);

        // A write of one floor cannot make the other durable.
        let checked = second.classify(envelope(2)).unwrap();
        second.record(checked, []).unwrap();
        let checked = first.classify(envelope(3)).unwrap();
        first.record(checked, []).unwrap();
        let written = first.pending_write().unwrap().write().unwrap();
        second.confirm_durable(written);
        assert!(!second.is_durable());
    }
}
