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

/// The current envelope and the bookkeeping that keeps failed writes safe.
///
/// The authority assumes a single writer: the caller must hold an exclusive
/// lock on its state directory for as long as it is open (Status holds the
/// publisher's service lock). Two writers would reuse sequences.
pub struct PublicationAuthority<D> {
    path: PathBuf,
    current: PublicationEnvelope<D>,
    /// The highest sequence that may be on disk or may have been served. Each
    /// new envelope takes the next one, so no sequence is reused for a
    /// different envelope.
    reserved_sequence: u64,
    /// A write failed since the last durable transition. While uncertain,
    /// `current` is ineligible and publication is refused.
    uncertain: bool,
}

/// Sequences a restart skips above the saved envelope. A failed publication
/// reserves one sequence and serves an unsynced withdrawal at the next, and an
/// uncertain authority serves nothing newer; see [`PublicationAuthority::apply`].
const RESTART_RESERVED_SEQUENCES: u64 = 2;

impl<D: Clone + Serialize + DeserializeOwned> PublicationAuthority<D> {
    /// Open the authority at `path` and recover: a restart starts a new
    /// recovery epoch above every sequence the previous process may have
    /// written or served, and is ineligible until the next publication.
    pub fn open(path: PathBuf, scope: PublicationScope) -> Result<Self, PublicationAuthorityError> {
        let saved = match fs::read(&path) {
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
        let reserved_sequence = saved
            .sequence
            .checked_add(RESTART_RESERVED_SEQUENCES)
            .ok_or(PublicationAuthorityError::Exhausted)?;
        let mut authority = Self {
            path,
            current: saved,
            reserved_sequence,
            uncertain: true,
        };
        // The previous process may have served a withdrawal newer than the
        // saved envelope, so the restart records its own withdrawal too.
        let mut recovery = authority.successor(AuthorityTransition::Recover)?;
        recovery.fence.latest_withdrawal = recovery.sequence;
        authority.write(&recovery)?;
        authority.current = recovery;
        authority.uncertain = false;
        Ok(authority)
    }

    pub fn current(&self) -> &PublicationEnvelope<D> {
        &self.current
    }

    /// Apply `transition`, persist it, and make it current.
    ///
    /// Every attempt reserves a fresh sequence before writing, so an envelope
    /// that may have reached disk never shares a sequence with another one.
    ///
    /// A failed write leaves the authority *uncertain* and its current
    /// envelope ineligible, so no positive authority outlives a write error:
    /// - from a certain state, a failed withdrawal or recovery becomes current
    ///   anyway, and a failed publication is replaced by a withdrawal at the
    ///   next sequence;
    /// - once uncertain, the current negative envelope stays as it is until a
    ///   write succeeds, so repeated failures expose no further unsynced
    ///   envelopes than a restart skips.
    ///
    /// While uncertain, publication is refused with
    /// [`PublicationAuthorityError::RecoveryRequired`] and a withdrawal
    /// recovers instead, because earlier answers may no longer be trusted.
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
        let next = self.successor(transition)?;
        let Err(error) = self.write(&next) else {
            self.current = next;
            self.uncertain = false;
            return Ok(&self.current);
        };
        if !self.uncertain {
            self.current = if next.eligible {
                self.successor(AuthorityTransition::Withdraw)
                    .expect("publication reserves its withdrawal sequence")
            } else {
                next
            };
            self.uncertain = true;
        }
        Err(error)
    }

    /// Reserve the next sequence and derive the envelope `transition` makes
    /// from the current one.
    fn successor(
        &mut self,
        transition: AuthorityTransition<D>,
    ) -> Result<PublicationEnvelope<D>, PublicationAuthorityError> {
        let sequence = self
            .reserved_sequence
            .checked_add(1)
            .ok_or(PublicationAuthorityError::Exhausted)?;
        let mut next = PublicationEnvelope {
            sequence,
            ..self.current.clone()
        };
        match transition {
            AuthorityTransition::Publish(publication) => {
                // Keep room for the withdrawal that replaces a failed publication.
                sequence
                    .checked_add(1)
                    .ok_or(PublicationAuthorityError::Exhausted)?;
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
        self.reserved_sequence = sequence;
        Ok(next)
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
        assert_eq!(recovered.sequence, exposed.sequence + 1);
        assert!(EnvelopeSuccession::classify(&recovered, Some(&exposed)).is_ok());
        assert!(EnvelopeSuccession::classify(&recovered, Some(&published))
            .unwrap()
            .fences_earlier_views());
    }

    /// Replace `state` with a file so every authority write below it fails.
    fn break_writes(state: &std::path::Path) {
        fs::remove_dir_all(state).unwrap();
        fs::write(state, b"not a directory").unwrap();
    }
    fn restore_writes(state: &std::path::Path) {
        fs::remove_file(state).unwrap();
        fs::create_dir(state).unwrap();
    }

    #[test]
    fn a_failed_publication_revokes_authority_through_repeated_failures_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let path = state.join("authority.json");
        let mut authority = PublicationAuthority::<u64>::open(path.clone(), scope()).unwrap();
        let published = authority
            .apply(AuthorityTransition::Publish(publication(1)))
            .unwrap()
            .clone();
        let granted = crate::AuthorityCheckRequest {
            authority: published.authority().unwrap(),
            subjects: vec![0_u8],
        };
        assert!(granted.admit(authority.current()).is_ok());

        // A newer publication fails: the old one stops being authoritative
        // at once, behind a withdrawal that fences its views.
        break_writes(&state);
        assert!(authority
            .apply(AuthorityTransition::Publish(publication(2)))
            .is_err());
        let revoked = authority.current().clone();
        assert!(!revoked.eligible && revoked.authority().is_none());
        assert_eq!(revoked.fence.latest_withdrawal, revoked.sequence);
        assert!(EnvelopeSuccession::classify(&revoked, Some(&published))
            .unwrap()
            .fences_earlier_views());
        assert_eq!(
            granted.admit(authority.current()),
            Err(crate::AuthorityCheckRefusal::AuthorityAdvanced)
        );

        // Further failed withdrawals and recoveries keep that one negative
        // envelope instead of exposing more unsynced sequences.
        for transition in [
            AuthorityTransition::Withdraw,
            AuthorityTransition::Recover,
            AuthorityTransition::Recover,
        ] {
            assert!(authority.apply(transition).is_err());
            assert_eq!(authority.current(), &revoked);
        }
        assert!(matches!(
            authority.apply(AuthorityTransition::Publish(publication(1))),
            Err(PublicationAuthorityError::RecoveryRequired)
        ));

        // The disk still holds the old eligible publication; a restart must
        // neither restore it nor reuse the revoked envelope's sequence.
        drop(authority);
        restore_writes(&state);
        fs::write(&path, serde_json::to_vec(&published).unwrap()).unwrap();
        let reopened = PublicationAuthority::<u64>::open(path, scope()).unwrap();
        let recovered = reopened.current();
        assert!(!recovered.eligible);
        assert!(granted.admit(recovered).is_err());
        assert!(recovered.sequence > revoked.sequence);
        for earlier in [&published, &revoked] {
            assert!(EnvelopeSuccession::classify(recovered, Some(earlier))
                .unwrap()
                .fences_earlier_views());
        }
    }

    #[test]
    fn an_uncertain_authority_recovers_above_every_exposed_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let mut authority =
            PublicationAuthority::<u64>::open(state.join("authority.json"), scope()).unwrap();
        authority
            .apply(AuthorityTransition::Publish(publication(1)))
            .unwrap();
        break_writes(&state);
        assert!(authority
            .apply(AuthorityTransition::Publish(publication(2)))
            .is_err());
        let revoked = authority.current().clone();
        assert!(authority.apply(AuthorityTransition::Withdraw).is_err());
        restore_writes(&state);

        // A withdrawal while uncertain recovers, at a sequence no failed
        // attempt used, and only then may publication resume.
        let recovered = authority
            .apply(AuthorityTransition::Withdraw)
            .unwrap()
            .clone();
        assert!(recovered.sequence > revoked.sequence + 1);
        assert_eq!(
            recovered.fence.recovery_epoch,
            revoked.fence.recovery_epoch + 1
        );
        assert!(EnvelopeSuccession::classify(&recovered, Some(&revoked)).is_ok());
        let republished = authority
            .apply(AuthorityTransition::Publish(publication(2)))
            .unwrap();
        assert!(republished.eligible);
    }
}
