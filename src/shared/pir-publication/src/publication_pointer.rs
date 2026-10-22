//! The mutable publication pointer and its one succession rule.
use crate::ObjectReference;
use std::fmt;

/// Distribution path, relative to the product prefix, of the current pointer.
pub const CURRENT_PATH: &str = "current";

/// A product's publication pointer. The publisher increments `revision` for
/// every pointer change, including a withdrawal, and starts a new
/// `recovery_epoch` whenever earlier answers may no longer be trusted.
///
/// Implementations expose their fields; the succession rule lives only in
/// [`PointerSuccession::classify`], so products cannot diverge on replay or
/// equal-revision semantics.
pub trait PublicationPointer: Clone + PartialEq {
    fn revision(&self) -> u64;
    fn recovery_epoch(&self) -> u64;
    /// Whether the publisher currently allows serving the referenced material.
    fn eligible(&self) -> bool;
    /// The product inventory object, when the pointer names one.
    fn inventory(&self) -> Option<&ObjectReference>;
    /// The same pointer with eligibility set to `eligible`.
    fn with_eligibility(&self, eligible: bool) -> Self;
    /// Product-owned monotonic fields (for example network, salt and generation)
    /// permit `self` to follow `previous`. Revision and epoch are checked separately.
    fn may_follow(&self, previous: &Self) -> bool;
}

/// How an accepted pointer relates to the durable floor it followed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PointerSuccession {
    /// No floor existed yet: the first pointer accepted from this source.
    First,
    /// Byte-for-byte the floor's pointer; nothing changed.
    Unchanged,
    /// The same revision, now withdrawn. Withdrawal can never be undone at the
    /// same revision.
    Withdrawn,
    /// A strictly newer revision. `recovery_epoch_advanced` means every earlier
    /// answer must be invalidated.
    Advanced { recovery_epoch_advanced: bool },
}

/// A pointer that cannot follow the durable floor: an older or rewritten
/// revision, an older recovery epoch, or a product identity change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationReplay;
impl fmt::Display for PublicationReplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("publication pointer replays or rewrites its durable floor")
    }
}
impl std::error::Error for PublicationReplay {}

impl PointerSuccession {
    /// Classify `next` against the durable `floor`. Equal revisions accept only
    /// the identical pointer or its withdrawal; a withdrawn pointer can never
    /// become eligible again without a new revision.
    pub fn classify<P: PublicationPointer>(
        next: &P,
        floor: Option<&P>,
    ) -> Result<Self, PublicationReplay> {
        let Some(floor) = floor else {
            return Ok(Self::First);
        };
        if next.revision() < floor.revision()
            || next.recovery_epoch() < floor.recovery_epoch()
            || !next.may_follow(floor)
        {
            return Err(PublicationReplay);
        }
        if next.revision() > floor.revision() {
            return Ok(Self::Advanced {
                recovery_epoch_advanced: next.recovery_epoch() > floor.recovery_epoch(),
            });
        }
        if next == floor {
            return Ok(Self::Unchanged);
        }
        if floor.eligible() && !next.eligible() && next.with_eligibility(true) == *floor {
            return Ok(Self::Withdrawn);
        }
        Err(PublicationReplay)
    }
    /// Whether serving views accepted under the floor must be fenced.
    pub fn fences_earlier_views(self) -> bool {
        matches!(
            self,
            Self::Withdrawn
                | Self::Advanced {
                    recovery_epoch_advanced: true
                }
        )
    }
}

/// Entity tag for conditional `current` requests. It changes with every
/// revision and with withdrawal; a 304 for this tag means only "unchanged" and
/// never renews source-observation age or authority-check time.
pub fn conditional_token(pointer: &impl PublicationPointer) -> String {
    format!("\"{}-{}\"", pointer.revision(), pointer.eligible())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    struct TestPointer {
        revision: u64,
        recovery_epoch: u64,
        generation: u64,
        eligible: bool,
    }
    impl PublicationPointer for TestPointer {
        fn revision(&self) -> u64 {
            self.revision
        }
        fn recovery_epoch(&self) -> u64 {
            self.recovery_epoch
        }
        fn eligible(&self) -> bool {
            self.eligible
        }
        fn inventory(&self) -> Option<&ObjectReference> {
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
    fn pointer() -> TestPointer {
        TestPointer {
            revision: 10,
            recovery_epoch: 2,
            generation: 7,
            eligible: true,
        }
    }

    #[test]
    fn withdrawal_cannot_be_undone_at_the_same_revision() {
        let old = pointer();
        let withdrawn = old.with_eligibility(false);
        assert_eq!(
            PointerSuccession::classify(&withdrawn, Some(&old)),
            Ok(PointerSuccession::Withdrawn)
        );
        assert_eq!(
            PointerSuccession::classify(&old, Some(&withdrawn)),
            Err(PublicationReplay)
        );
        assert_eq!(
            PointerSuccession::classify(&withdrawn, Some(&withdrawn)),
            Ok(PointerSuccession::Unchanged)
        );
        let newer = TestPointer {
            revision: 11,
            ..old.clone()
        };
        assert_eq!(
            PointerSuccession::classify(&newer, Some(&withdrawn)),
            Ok(PointerSuccession::Advanced {
                recovery_epoch_advanced: false
            })
        );
    }

    #[test]
    fn replays_and_rewrites_are_refused() {
        let old = pointer();
        let older_revision = TestPointer {
            revision: 9,
            ..old.clone()
        };
        let older_epoch = TestPointer {
            revision: 11,
            recovery_epoch: 1,
            ..old.clone()
        };
        let older_generation = TestPointer {
            revision: 11,
            generation: 6,
            ..old.clone()
        };
        let rewritten = TestPointer {
            generation: 8,
            ..old.clone()
        };
        for replay in [older_revision, older_epoch, older_generation, rewritten] {
            assert_eq!(
                PointerSuccession::classify(&replay, Some(&old)),
                Err(PublicationReplay),
                "{replay:?}"
            );
        }
        let recovered = TestPointer {
            revision: 11,
            recovery_epoch: 3,
            ..old.clone()
        };
        let succession = PointerSuccession::classify(&recovered, Some(&old)).unwrap();
        assert!(succession.fences_earlier_views());
    }

    #[test]
    fn conditional_tokens_change_with_withdrawal() {
        let old = pointer();
        assert_ne!(
            conditional_token(&old),
            conditional_token(&old.with_eligibility(false))
        );
    }
}
