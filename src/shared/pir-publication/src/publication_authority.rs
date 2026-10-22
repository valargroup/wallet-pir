//! The publisher's durable authority: the one place that turns publication,
//! withdrawal and recovery into successive envelopes.
//!
//! Every transition advances the authority sequence and is synced before the
//! new envelope becomes current, so consumers never see two different
//! envelopes at one sequence from a healthy publisher.
use crate::{AuthorityFence, PublicationEnvelope, PublicationReference, PublicationScope};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt, fs, io, path::PathBuf};

/// One publisher authority transition.
pub enum AuthorityTransition<D> {
    /// Make `PublicationReference` current and eligible.
    Publish(PublicationReference<D>),
    /// Stop serving the current publication; the fence records the withdrawal.
    Withdraw,
    /// Start a new recovery epoch, ineligible until the next publication.
    /// Earlier answers may no longer be trusted.
    Recover,
}

/// Why the authority could not be opened or changed.
#[derive(Debug)]
pub enum PublicationAuthorityError {
    /// The saved authority belongs to another product, chain or source.
    WrongScope,
    /// A previous transition may not be durable; only [`AuthorityTransition::Recover`]
    /// is accepted until one succeeds.
    RecoveryRequired,
    /// A counter would overflow.
    Exhausted,
    Corrupt(serde_json::Error),
    Io(io::Error),
}
impl fmt::Display for PublicationAuthorityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongScope => f.write_str("publication authority belongs to another scope"),
            Self::RecoveryRequired => {
                f.write_str("publication authority must recover after an uncertain write")
            }
            Self::Exhausted => f.write_str("publication authority sequence exhausted"),
            Self::Corrupt(error) => write!(f, "publication authority is corrupt: {error}"),
            Self::Io(error) => write!(f, "publication authority I/O failed: {error}"),
        }
    }
}
impl std::error::Error for PublicationAuthorityError {}

/// The current envelope and whether a newer one may have been exposed without
/// becoming durable.
pub struct PublicationAuthority<D> {
    path: PathBuf,
    current: PublicationEnvelope<D>,
    /// A transition's write failed. Its envelope is current in memory (so a
    /// withdrawal still takes effect) but may be absent from disk, so its
    /// sequence must never be reused.
    uncertain: bool,
}

impl<D: Clone + Serialize + DeserializeOwned> PublicationAuthority<D> {
    /// Open the authority at `path` and recover: a restart starts a new
    /// recovery epoch and is ineligible until the next publication. The saved
    /// envelope is treated as possibly followed by one unsynced transition.
    pub fn open(path: PathBuf, scope: PublicationScope) -> Result<Self, PublicationAuthorityError> {
        let current = match fs::read(&path) {
            Ok(bytes) => {
                let saved: PublicationEnvelope<D> =
                    serde_json::from_slice(&bytes).map_err(PublicationAuthorityError::Corrupt)?;
                if saved.scope != scope {
                    return Err(PublicationAuthorityError::WrongScope);
                }
                saved
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => PublicationEnvelope {
                scope,
                sequence: 0,
                eligible: false,
                fence: AuthorityFence {
                    recovery_epoch: 0,
                    latest_withdrawal: 0,
                },
                publication: None,
            },
            Err(error) => return Err(PublicationAuthorityError::Io(error)),
        };
        let mut authority = Self {
            path,
            current,
            uncertain: true,
        };
        authority.apply(AuthorityTransition::Recover)?;
        Ok(authority)
    }

    pub fn current(&self) -> &PublicationEnvelope<D> {
        &self.current
    }

    /// Apply `transition`, persist it, and make it current.
    ///
    /// A failed write leaves the authority *uncertain*: the envelope may be on
    /// disk without being synced. A negative transition (withdrawal or
    /// recovery) from a certain state still becomes current, so a disk error
    /// cannot undo a withdrawal; any other failed envelope is discarded. While
    /// uncertain, publication is refused, a withdrawal recovers instead, and a
    /// recovery skips one sequence so it never reuses the sequence of an
    /// envelope that may have been exposed or written.
    pub fn apply(
        &mut self,
        transition: AuthorityTransition<D>,
    ) -> Result<&PublicationEnvelope<D>, PublicationAuthorityError> {
        let transition = match transition {
            AuthorityTransition::Withdraw if self.uncertain => AuthorityTransition::Recover,
            AuthorityTransition::Publish(_) if self.uncertain => {
                return Err(PublicationAuthorityError::RecoveryRequired)
            }
            transition => transition,
        };
        let step = if self.uncertain { 2 } else { 1 };
        let sequence = self
            .current
            .sequence
            .checked_add(step)
            .ok_or(PublicationAuthorityError::Exhausted)?;
        let mut next = PublicationEnvelope {
            sequence,
            ..self.current.clone()
        };
        match transition {
            AuthorityTransition::Publish(publication) => {
                next.eligible = true;
                next.publication = Some(publication);
            }
            AuthorityTransition::Withdraw => {
                next.eligible = false;
                next.fence.latest_withdrawal = sequence;
            }
            AuthorityTransition::Recover => {
                next.eligible = false;
                next.fence.recovery_epoch = next
                    .fence
                    .recovery_epoch
                    .checked_add(1)
                    .ok_or(PublicationAuthorityError::Exhausted)?;
            }
        }
        match self.write(&next) {
            Ok(()) => {
                self.current = next;
                self.uncertain = false;
                Ok(&self.current)
            }
            Err(error) => {
                if !self.uncertain && !next.eligible {
                    self.current = next;
                }
                self.uncertain = true;
                Err(error)
            }
        }
    }

    fn write(&self, envelope: &PublicationEnvelope<D>) -> Result<(), PublicationAuthorityError> {
        let bytes = serde_json::to_vec(envelope).map_err(PublicationAuthorityError::Corrupt)?;
        let directory = self.path.parent().unwrap_or(std::path::Path::new("."));
        let name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("authority path has no file name"))
            .map_err(PublicationAuthorityError::Io)?;
        fs::create_dir_all(directory).map_err(PublicationAuthorityError::Io)?;
        pir_control::durable::write_atomic(directory, name, |file| file.write_all(&bytes))
            .and_then(|_| pir_control::durable::sync_dir(directory))
            .map_err(PublicationAuthorityError::Io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EnvelopeSuccession, ObjectReference};

    fn scope() -> PublicationScope {
        PublicationScope {
            product: "test".into(),
            chain: [1; 32],
            source: [2; 32],
        }
    }
    fn publication(descriptor: u64) -> PublicationReference<u64> {
        PublicationReference::new(
            [7; 32],
            ObjectReference::for_bytes(b"inventory"),
            descriptor,
        )
    }

    #[test]
    fn every_transition_advances_the_sequence_and_withdrawals_fence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("authority.json");
        let mut authority = PublicationAuthority::<u64>::open(path.clone(), scope()).unwrap();
        let opened = authority.current().clone();
        assert!(!opened.eligible);
        assert_eq!(opened.fence.recovery_epoch, 1);

        let published = authority
            .apply(AuthorityTransition::Publish(publication(1)))
            .unwrap()
            .clone();
        let withdrawn = authority
            .apply(AuthorityTransition::Withdraw)
            .unwrap()
            .clone();
        assert_eq!(withdrawn.fence.latest_withdrawal, withdrawn.sequence);
        let republished = authority
            .apply(AuthorityTransition::Publish(publication(1)))
            .unwrap()
            .clone();
        for (previous, next) in [
            (&opened, &published),
            (&published, &withdrawn),
            (&withdrawn, &republished),
        ] {
            assert!(next.sequence > previous.sequence);
            assert!(next.validate().is_ok());
            EnvelopeSuccession::classify(next, Some(previous)).unwrap();
        }
        // A consumer that missed the withdrawal still fences its old views.
        assert!(EnvelopeSuccession::classify(&republished, Some(&published))
            .unwrap()
            .fences_earlier_views());

        // A restart recovers into a new epoch above everything published.
        drop(authority);
        let reopened = PublicationAuthority::<u64>::open(path.clone(), scope()).unwrap();
        assert!(reopened.current().sequence > republished.sequence + 1);
        assert_eq!(reopened.current().fence.recovery_epoch, 2);
        assert!(
            EnvelopeSuccession::classify(reopened.current(), Some(&republished))
                .unwrap()
                .fences_earlier_views()
        );

        let other = PublicationScope {
            source: [3; 32],
            ..scope()
        };
        assert!(matches!(
            PublicationAuthority::<u64>::open(path, other),
            Err(PublicationAuthorityError::WrongScope)
        ));
    }

    #[test]
    fn a_failed_write_keeps_the_withdrawal_and_skips_its_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let mut authority =
            PublicationAuthority::<u64>::open(state.join("authority.json"), scope()).unwrap();
        let published = authority
            .apply(AuthorityTransition::Publish(publication(1)))
            .unwrap()
            .clone();
        // Replace the directory with a file so the next write fails.
        fs::remove_dir_all(&state).unwrap();
        fs::write(&state, b"not a directory").unwrap();
        assert!(authority.apply(AuthorityTransition::Withdraw).is_err());
        let exposed = authority.current().clone();
        assert!(!exposed.eligible);
        assert!(matches!(
            authority.apply(AuthorityTransition::Publish(publication(2))),
            Err(PublicationAuthorityError::RecoveryRequired)
        ));
        fs::remove_file(&state).unwrap();
        let recovered = authority
            .apply(AuthorityTransition::Recover)
            .unwrap()
            .clone();
        assert_eq!(recovered.sequence, exposed.sequence + 2);
        assert!(EnvelopeSuccession::classify(&recovered, Some(&exposed)).is_ok());
        assert!(EnvelopeSuccession::classify(&recovered, Some(&published))
            .unwrap()
            .fences_earlier_views());
    }
}
