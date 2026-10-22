//! The mutable publication envelope every product publishes, and its one
//! succession rule.
//!
//! Common fields carry the publication's scope, authority sequence,
//! eligibility and fence. Only the typed descriptor inside the publication
//! reference is product-specific, so no product can diverge on replay,
//! withdrawal or recovery semantics.
use crate::{MaterialIdentity, ObjectReference};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

/// Distribution path, relative to the product prefix, of the current envelope.
pub const CURRENT_PATH: &str = "current";

/// What a publication is about: the product, the chain and the product's
/// source dataset (for Status, the dataset salt). A scope never changes
/// between envelopes accepted from one publisher.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublicationScope {
    pub product: String,
    pub chain: [u8; 32],
    pub source: [u8; 32],
}

/// The publisher's durable withdrawal and recovery history. Both components
/// only grow. Any change fences every view accepted before it, including when
/// a consumer missed the withdrawal and first sees a later eligible
/// republication.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AuthorityFence {
    /// Starts anew whenever earlier answers may no longer be trusted, for
    /// example after a publisher restart.
    pub recovery_epoch: u64,
    /// Authority sequence of the newest withdrawal, or zero for none.
    pub latest_withdrawal: u64,
}
impl AuthorityFence {
    fn may_follow(&self, previous: &Self) -> bool {
        self.recovery_epoch >= previous.recovery_epoch
            && self.latest_withdrawal >= previous.latest_withdrawal
    }
}

/// Digest of one publication reference's exact metadata: material, inventory
/// and descriptor. A metadata-only refresh changes it while the material
/// identity, and the bytes behind it, stay reusable.
pub type PublicationIdentity = [u8; 32];

/// The publication an envelope names.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublicationReference<D> {
    pub publication: PublicationIdentity,
    /// Identity of the reusable material bytes the inventory references.
    pub material: MaterialIdentity,
    pub inventory: ObjectReference,
    pub descriptor: D,
}

/// The publication reference's identity does not describe its metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationIdentityMismatch;
impl fmt::Display for PublicationIdentityMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("publication identity does not describe its metadata")
    }
}
impl std::error::Error for PublicationIdentityMismatch {}

impl<D: Serialize> PublicationReference<D> {
    /// A reference whose identity is computed from its metadata.
    pub fn new(material: MaterialIdentity, inventory: ObjectReference, descriptor: D) -> Self {
        Self {
            publication: publication_identity(&material, &inventory, &descriptor),
            material,
            inventory,
            descriptor,
        }
    }
    /// Check that the identity describes exactly this metadata.
    pub fn verify(&self) -> Result<(), PublicationIdentityMismatch> {
        if self.publication
            != publication_identity(&self.material, &self.inventory, &self.descriptor)
        {
            return Err(PublicationIdentityMismatch);
        }
        Ok(())
    }
}

fn publication_identity<D: Serialize>(
    material: &MaterialIdentity,
    inventory: &ObjectReference,
    descriptor: &D,
) -> PublicationIdentity {
    let metadata = serde_json::to_vec(&(material, inventory, descriptor))
        .expect("publication metadata serializes");
    Sha256::digest(metadata).into()
}

/// One authority state of a publisher. The publisher advances `sequence` for
/// every transition, including withdrawal and recovery, so two envelopes with
/// the same sequence must be identical.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PublicationEnvelope<D> {
    pub scope: PublicationScope,
    pub sequence: u64,
    /// Whether the publisher currently allows serving the referenced material.
    pub eligible: bool,
    pub fence: AuthorityFence,
    pub publication: Option<PublicationReference<D>>,
}

/// An envelope that is internally inconsistent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MalformedEnvelope;
impl fmt::Display for MalformedEnvelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("publication envelope is malformed")
    }
}
impl std::error::Error for MalformedEnvelope {}

/// The exact authority an envelope grants: authority checks bind it so an
/// answer under one publication or fence never counts for another.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AuthorityBinding {
    pub sequence: u64,
    pub fence: AuthorityFence,
    pub publication: PublicationIdentity,
}

impl<D: Serialize> PublicationEnvelope<D> {
    /// Product-independent shape checks: a positive sequence and recovery
    /// epoch, a withdrawal no newer than the envelope, and an eligible
    /// envelope names a publication whose identity matches its metadata.
    pub fn validate(&self) -> Result<(), MalformedEnvelope> {
        if self.sequence == 0
            || self.fence.recovery_epoch == 0
            || self.fence.latest_withdrawal > self.sequence
            || (self.eligible && self.publication.is_none())
        {
            return Err(MalformedEnvelope);
        }
        match &self.publication {
            Some(publication) if publication.verify().is_err() => Err(MalformedEnvelope),
            _ => Ok(()),
        }
    }
}

impl<D> PublicationEnvelope<D> {
    /// The binding of an eligible envelope; withdrawn envelopes grant nothing.
    pub fn authority(&self) -> Option<AuthorityBinding> {
        let publication = self.publication.as_ref().filter(|_| self.eligible)?;
        Some(AuthorityBinding {
            sequence: self.sequence,
            fence: self.fence,
            publication: publication.publication,
        })
    }
    /// The product inventory object, when the envelope names a publication.
    pub fn inventory(&self) -> Option<&ObjectReference> {
        self.publication
            .as_ref()
            .map(|publication| &publication.inventory)
    }
}

/// How an accepted envelope relates to the durable floor it followed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvelopeSuccession {
    /// No floor existed yet: the first envelope accepted from this source.
    First,
    /// Exactly the floor's envelope; nothing changed.
    Unchanged,
    /// A strictly newer sequence. `fences_earlier_views` means the fence
    /// advanced or the envelope is withdrawn, so every view accepted under the
    /// floor must stop serving.
    Advanced { fences_earlier_views: bool },
}

/// An envelope that cannot follow the durable floor: an older sequence, a
/// different envelope at the same sequence, a regressed fence or a different
/// scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationReplay;
impl fmt::Display for PublicationReplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("publication envelope replays or rewrites its durable floor")
    }
}
impl std::error::Error for PublicationReplay {}

impl EnvelopeSuccession {
    /// Classify `next` against the durable `floor`.
    pub fn classify<D: PartialEq>(
        next: &PublicationEnvelope<D>,
        floor: Option<&PublicationEnvelope<D>>,
    ) -> Result<Self, PublicationReplay> {
        let Some(floor) = floor else {
            return Ok(Self::First);
        };
        if next.scope != floor.scope
            || next.sequence < floor.sequence
            || !next.fence.may_follow(&floor.fence)
        {
            return Err(PublicationReplay);
        }
        if next.sequence == floor.sequence {
            return if next == floor {
                Ok(Self::Unchanged)
            } else {
                Err(PublicationReplay)
            };
        }
        Ok(Self::Advanced {
            fences_earlier_views: !next.eligible || next.fence != floor.fence,
        })
    }
    /// Whether serving views accepted under the floor must be fenced.
    pub fn fences_earlier_views(self) -> bool {
        matches!(
            self,
            Self::Advanced {
                fences_earlier_views: true
            }
        )
    }
}

/// Entity tag for conditional `current` requests: the sequence and a digest
/// of the exact envelope, so a publisher that rewrote an envelope at the same
/// sequence returns the new body and is refused as a replay. A 304 for this
/// tag means only "unchanged" and never renews source-observation age or
/// authority-check time.
pub fn conditional_token<D: Serialize>(envelope: &PublicationEnvelope<D>) -> String {
    let bytes = serde_json::to_vec(envelope).expect("publication envelope serializes");
    format!(
        "\"{}-{}\"",
        envelope.sequence,
        hex::encode(&Sha256::digest(bytes)[..16])
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn scope() -> PublicationScope {
        PublicationScope {
            product: "test".into(),
            chain: [1; 32],
            source: [2; 32],
        }
    }
    fn envelope(sequence: u64, descriptor: u64) -> PublicationEnvelope<u64> {
        PublicationEnvelope {
            scope: scope(),
            sequence,
            eligible: true,
            fence: AuthorityFence {
                recovery_epoch: 2,
                latest_withdrawal: 1,
            },
            publication: Some(PublicationReference::new(
                [7; 32],
                ObjectReference::for_bytes(b"inventory"),
                descriptor,
            )),
        }
    }

    #[test]
    fn equal_sequences_accept_only_the_identical_envelope() {
        let old = envelope(10, 7);
        assert_eq!(
            EnvelopeSuccession::classify(&old.clone(), Some(&old)),
            Ok(EnvelopeSuccession::Unchanged)
        );
        let mut withdrawn = old.clone();
        withdrawn.eligible = false;
        let divergent = envelope(10, 8);
        for rewrite in [withdrawn, divergent] {
            assert_eq!(
                EnvelopeSuccession::classify(&rewrite, Some(&old)),
                Err(PublicationReplay)
            );
        }
    }

    #[test]
    fn replays_regressed_fences_and_scope_changes_are_refused() {
        let old = envelope(10, 7);
        let mut older_sequence = envelope(9, 7);
        older_sequence.fence = old.fence;
        let mut older_epoch = envelope(11, 7);
        older_epoch.fence.recovery_epoch = 1;
        let mut older_withdrawal = envelope(11, 7);
        older_withdrawal.fence.latest_withdrawal = 0;
        let mut other_scope = envelope(11, 7);
        other_scope.scope.source = [9; 32];
        for replay in [older_sequence, older_epoch, older_withdrawal, other_scope] {
            assert_eq!(
                EnvelopeSuccession::classify(&replay, Some(&old)),
                Err(PublicationReplay),
                "{replay:?}"
            );
        }
    }

    #[test]
    fn a_missed_withdrawal_still_fences_through_a_later_republication() {
        let old = envelope(10, 7);
        // The consumer never saw sequence 11, the withdrawal; sequence 12
        // republishes eligible material but carries the withdrawal in its fence.
        let mut republished = envelope(12, 7);
        republished.fence.latest_withdrawal = 11;
        let succession = EnvelopeSuccession::classify(&republished, Some(&old)).unwrap();
        assert!(succession.fences_earlier_views());

        let refreshed = envelope(13, 8);
        let mut refreshed = refreshed;
        refreshed.fence = republished.fence;
        let succession = EnvelopeSuccession::classify(&refreshed, Some(&republished)).unwrap();
        assert!(!succession.fences_earlier_views());
    }

    #[test]
    fn publication_identity_covers_metadata_but_not_the_envelope() {
        let first = envelope(10, 7);
        let refreshed = envelope(11, 8);
        let (first, refreshed) = (first.publication.unwrap(), refreshed.publication.unwrap());
        assert_eq!(first.material, refreshed.material);
        assert_ne!(first.publication, refreshed.publication);
        assert_eq!(
            envelope(12, 7).publication.unwrap().publication,
            first.publication
        );
        let mut forged = refreshed.clone();
        forged.descriptor = 7;
        assert_eq!(forged.verify(), Err(PublicationIdentityMismatch));
    }

    #[test]
    fn withdrawn_envelopes_grant_no_authority_and_tokens_track_sequence() {
        let old = envelope(10, 7);
        let mut withdrawn = envelope(11, 7);
        withdrawn.eligible = false;
        withdrawn.fence.latest_withdrawal = 11;
        assert!(withdrawn.validate().is_ok());
        assert!(withdrawn.authority().is_none());
        assert!(old.authority().is_some());
        assert_ne!(conditional_token(&old), conditional_token(&withdrawn));
        let mut future_withdrawal = old.clone();
        future_withdrawal.fence.latest_withdrawal = 11;
        assert_eq!(future_withdrawal.validate(), Err(MalformedEnvelope));
    }
}
