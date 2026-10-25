//! Which cached objects each committed view references, kept while the
//! product still needs the view.
use crate::ViewIdentity;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::SystemTime,
};

/// Why a product still needs one of its committed views.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewRequirement {
    /// The view can still answer a request.
    Serving,
    /// A peer holds the view until the deadline, after which its objects may
    /// be collected even if the hold was never released.
    HeldUntil(SystemTime),
}

/// The objects of every committed view the product still needs, and whether
/// that mapping covers every needed view.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RetainedViews {
    views: BTreeMap<ViewIdentity, BTreeSet<String>>,
    /// A needed view this lifecycle never committed, such as one the product
    /// installed from local state before its first commit. Its objects are
    /// unknown, so nothing may be collected.
    unknown_view_required: bool,
}

impl RetainedViews {
    /// The views retained once `committed`, referencing `objects`, installs:
    /// it and every view `required` still names at `now`. A required view this
    /// lifecycle never committed keeps every cached object until no longer
    /// required; see [`Self::collectable_beyond`].
    pub fn after_commit(
        &self,
        committed: ViewIdentity,
        objects: BTreeSet<String>,
        required: &[(ViewIdentity, ViewRequirement)],
        now: SystemTime,
    ) -> Self {
        let needed = required
            .iter()
            .filter(|(view, requirement)| {
                *view != committed
                    && match requirement {
                        ViewRequirement::Serving => true,
                        ViewRequirement::HeldUntil(deadline) => *deadline > now,
                    }
            })
            .map(|(view, _)| *view);
        let mut views = BTreeMap::new();
        let mut unknown_view_required = false;
        for view in needed {
            match self.views.get(&view) {
                Some(objects) => {
                    views.insert(view, objects.clone());
                }
                None => unknown_view_required = true,
            }
        }
        views.insert(committed, objects);
        Self {
            views,
            unknown_view_required,
        }
    }
    /// The objects collection must keep, or `None` when a required view's
    /// objects are unknown and nothing may be collected.
    pub fn collectable_beyond(&self) -> Option<BTreeSet<String>> {
        (!self.unknown_view_required).then(|| self.views.values().flatten().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn objects(name: &str) -> BTreeSet<String> {
        BTreeSet::from([name.to_string()])
    }

    #[test]
    fn a_required_view_never_committed_here_defers_all_collection() {
        let now = SystemTime::now();
        let preinstalled = ViewIdentity([1; 32]);
        let first = ViewIdentity([2; 32]);
        let retained = RetainedViews::default().after_commit(
            first,
            objects("first"),
            &[(preinstalled, ViewRequirement::Serving)],
            now,
        );
        assert_eq!(retained.collectable_beyond(), None);

        // Once the preinstalled view is no longer needed, collection resumes.
        let second = ViewIdentity([3; 32]);
        let retained = retained.after_commit(
            second,
            objects("second"),
            &[(
                first,
                ViewRequirement::HeldUntil(now + Duration::from_secs(5)),
            )],
            now,
        );
        assert_eq!(
            retained.collectable_beyond(),
            Some(BTreeSet::from(["first".to_string(), "second".to_string()]))
        );
    }

    #[test]
    fn an_expired_hold_on_an_unknown_view_does_not_defer_collection() {
        let now = SystemTime::now();
        let retained = RetainedViews::default().after_commit(
            ViewIdentity([2; 32]),
            objects("committed"),
            &[(
                ViewIdentity([1; 32]),
                ViewRequirement::HeldUntil(now - Duration::from_secs(1)),
            )],
            now,
        );
        assert_eq!(retained.collectable_beyond(), Some(objects("committed")));
    }
}
