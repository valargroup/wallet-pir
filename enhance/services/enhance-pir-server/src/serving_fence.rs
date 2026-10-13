//! The serving fence and activation match that the packing router and query
//! ingress both enforce against the coordinator, and the prepared/active view
//! state they both keep under it.
//!
//! Each keeps its own durable file layout; only the decisions are shared.

use crate::packing_router::Activation;
use crate::worker::Revocation;
use enhance_pir::protocol::canonical_hash;
use pir_control::Watchdog;
use std::sync::Arc;

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

/// A view a controller prepared, as far as activation matching needs to see it.
pub(crate) trait PreparedView {
    fn controller_epoch(&self) -> u64;
    fn revocation(&self) -> &Revocation;
    fn digest(&self) -> &str;
}

/// Why a proposed fence was not taken.
#[derive(Debug)]
pub(crate) enum FenceRefusal {
    /// It would move the fence backwards; nothing was written.
    Backwards,
    /// Saving it failed; the fence in memory is unchanged.
    Persist(String),
}

/// Why a response may not be released for a session.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Withheld {
    /// The fence revoked the session.
    Revoked,
    /// The fence moved on, or the controller stopped renewing authority.
    NoAuthority,
}

/// The fence, the prepared and active views, and the controller watchdog of a
/// serving process.
///
/// A controller prepares a candidate, activates it, and then refreshes it
/// every second; the process serves only while the watchdog is live. Saving
/// the fence is the caller's job, so each process keeps its own file layout.
pub(crate) struct ControlledView<T> {
    pub(crate) epoch: u64,
    pub(crate) fence: Revocation,
    pub(crate) active: Option<Arc<T>>,
    pub(crate) candidate: Option<Arc<T>>,
    // A liveness watchdog, not permission to complete recovery without a fence ACK.
    pub(crate) refreshed: Watchdog,
}

impl<T: PreparedView> ControlledView<T> {
    /// Starts from a saved fence, with no views and no authority.
    pub(crate) fn new(epoch: u64, fence: Revocation) -> Self {
        Self {
            epoch,
            fence,
            active: None,
            candidate: None,
            refreshed: Watchdog::default(),
        }
    }

    /// Moves the fence to `(epoch, fence)` when that is newer, saving it with
    /// `persist` first. A newer fence withdraws authority until the next
    /// activation; an identical one changes nothing and saves nothing.
    pub(crate) fn advance_fence(
        &mut self,
        epoch: u64,
        fence: Revocation,
        persist: impl FnOnce(u64, &Revocation) -> Result<(), String>,
    ) -> Result<(), FenceRefusal> {
        match advance(self.epoch, &self.fence, epoch, &fence) {
            Err(()) => return Err(FenceRefusal::Backwards),
            Ok(Advance::Unchanged) => return Ok(()),
            Ok(Advance::Newer) => {}
        }
        persist(epoch, &fence).map_err(FenceRefusal::Persist)?;
        self.epoch = epoch;
        self.fence = fence;
        // A new controller must explicitly activate after reconciliation.
        self.refreshed.clear();
        Ok(())
    }

    /// Whether `activation` names this process and `view` under the current fence.
    pub(crate) fn matches(&self, incarnation: &str, activation: &Activation, view: &T) -> bool {
        activation_matches(
            incarnation,
            self.epoch,
            &self.fence,
            activation,
            view.controller_epoch(),
            view.revocation(),
            view.digest(),
        )
    }

    /// Promotes the candidate when the activation names it, or accepts a
    /// repeat for the active view, and renews authority. False changes nothing.
    pub(crate) fn activate(&mut self, incarnation: &str, activation: &Activation) -> bool {
        if self
            .candidate
            .as_ref()
            .is_some_and(|v| self.matches(incarnation, activation, v))
        {
            self.active = self.candidate.take();
        } else if !self
            .active
            .as_ref()
            .is_some_and(|v| self.matches(incarnation, activation, v))
        {
            return false;
        }
        self.refreshed.renew();
        true
    }

    /// Renews authority when the activation names the active view. False
    /// changes nothing.
    pub(crate) fn refresh(&mut self, incarnation: &str, activation: &Activation) -> bool {
        if !self
            .active
            .as_ref()
            .is_some_and(|v| self.matches(incarnation, activation, v))
        {
            return false;
        }
        self.refreshed.renew();
        true
    }

    /// Whether a response for `session`, admitted under controller `epoch`,
    /// may still be released.
    pub(crate) fn release(&self, session: &str, epoch: u64) -> Result<(), Withheld> {
        if self.fence.sessions.contains(session) {
            return Err(Withheld::Revoked);
        }
        if self.epoch != epoch || !self.refreshed.live() {
            return Err(Withheld::NoAuthority);
        }
        Ok(())
    }
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
