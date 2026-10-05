//! The serving fence and activation match that the packing router and query
//! ingress both enforce against the coordinator.
//!
//! Each keeps its own durable file layout; only the decisions are shared.

use crate::packing_router::Activation;
use crate::worker::Revocation;
use enhance_pir::protocol::canonical_hash;

/// Whether a proposed fence may replace the current one.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Advance {
    /// Identical to the current fence: nothing to persist.
    Unchanged,
    /// Strictly newer: persist it, then require a fresh activation.
    Newer,
}

/// Checks that `(epoch, next)` does not move the fence backwards.
///
/// The controller epoch and recovery epoch may only grow, revoked sessions may
/// only be added, and every revoked session must be a canonical hash.
pub(crate) fn advance(
    current_epoch: u64,
    current: &Revocation,
    epoch: u64,
    next: &Revocation,
) -> Result<Advance, ()> {
    if epoch < current_epoch
        || next.recovery_epoch < current.recovery_epoch
        || !current.sessions.is_subset(&next.sessions)
        || next.sessions.iter().any(|id| !canonical_hash(id))
    {
        return Err(());
    }
    if epoch == current_epoch
        && next.recovery_epoch == current.recovery_epoch
        && next.sessions == current.sessions
    {
        return Ok(Advance::Unchanged);
    }
    Ok(Advance::Newer)
}

/// Whether an activation or refresh names exactly this process and a view it
/// prepared under the current fence.
pub(crate) fn activation_matches(
    incarnation: &str,
    fence_epoch: u64,
    fence: &Revocation,
    activation: &Activation,
    view_epoch: u64,
    view_revocation: &Revocation,
    view_digest: &str,
) -> bool {
    activation.incarnation == incarnation
        && activation.controller_epoch == fence_epoch
        && activation.controller_epoch == view_epoch
        && activation.digest == view_digest
        && fence.sessions.is_subset(&view_revocation.sessions)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn revocation(recovery_epoch: u64, sessions: &[&str]) -> Revocation {
        Revocation {
            recovery_epoch,
            sessions: sessions.iter().map(|s| s.to_string()).collect(),
        }
    }

    const A: &str = "0000000000000000000000000000000000000000000000000000000000000001";
    const B: &str = "0000000000000000000000000000000000000000000000000000000000000002";

    #[test]
    fn the_fence_only_moves_forward() {
        let current = revocation(3, &[A]);
        assert_eq!(advance(5, &current, 5, &current), Ok(Advance::Unchanged));
        assert_eq!(advance(5, &current, 6, &current), Ok(Advance::Newer));
        assert_eq!(
            advance(5, &current, 5, &revocation(4, &[A])),
            Ok(Advance::Newer)
        );
        assert_eq!(
            advance(5, &current, 5, &revocation(3, &[A, B])),
            Ok(Advance::Newer)
        );
        assert_eq!(advance(5, &current, 4, &current), Err(()));
        assert_eq!(advance(5, &current, 5, &revocation(2, &[A])), Err(()));
        assert_eq!(advance(5, &current, 5, &revocation(3, &[B])), Err(()));
        assert_eq!(
            advance(5, &current, 5, &revocation(3, &[A, "not-a-hash"])),
            Err(())
        );
    }

    #[test]
    fn an_activation_must_name_this_process_epoch_view_and_fence() {
        let fence = revocation(1, &[A]);
        let view = revocation(1, &[A, B]);
        let ok = Activation {
            incarnation: "me".into(),
            digest: "view".into(),
            controller_epoch: 7,
        };
        let check = |a: &Activation, fence: &Revocation| {
            activation_matches("me", 7, fence, a, 7, &view, "view")
        };
        assert!(check(&ok, &fence));
        for bad in [
            Activation {
                incarnation: "other".into(),
                ..ok.clone()
            },
            Activation {
                digest: "stale".into(),
                ..ok.clone()
            },
            Activation {
                controller_epoch: 8,
                ..ok.clone()
            },
        ] {
            assert!(!check(&bad, &fence));
        }
        // A fence that revoked a session the view still serves.
        let wider = revocation(1, &[A, B, "c"]);
        assert!(!check(&ok, &wider));
        // The view was prepared under another epoch.
        assert!(!activation_matches("me", 7, &fence, &ok, 6, &view, "view"));
    }
}
