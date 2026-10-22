//! Which cached objects each committed view references, kept while the
//! product still needs the view.
use pir_publication::ViewIdentity;
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

/// The objects of every committed view the product still needs.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct RetainedViews(BTreeMap<ViewIdentity, BTreeSet<String>>);

impl RetainedViews {
    /// The views retained once `committed`, referencing `objects`, installs:
    /// it and every known view `required` still names at `now`. A view this
    /// lifecycle never committed has no known objects to keep.
    pub(super) fn after_commit(
        &self,
        committed: ViewIdentity,
        objects: BTreeSet<String>,
        required: &[(ViewIdentity, ViewRequirement)],
        now: SystemTime,
    ) -> Self {
        let mut retained: BTreeMap<_, _> = required
            .iter()
            .filter(|(_, requirement)| match requirement {
                ViewRequirement::Serving => true,
                ViewRequirement::HeldUntil(deadline) => *deadline > now,
            })
            .filter_map(|(view, _)| Some((*view, self.0.get(view)?.clone())))
            .collect();
        retained.insert(committed, objects);
        Self(retained)
    }
    /// Every object some retained view references.
    pub(super) fn objects(&self) -> BTreeSet<String> {
        self.0.values().flatten().cloned().collect()
    }
    #[cfg(test)]
    pub(super) fn views(&self) -> Vec<ViewIdentity> {
        self.0.keys().copied().collect()
    }
}
