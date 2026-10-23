//! The rollback fence shared by the display journal's writer and its controller.
//!
//! The root-scheduled writer runs beside the controller, so the sealed floor,
//! the active map and the durable rollback record live here behind one lock.
//! A writer refuses a rollback below the floor, and otherwise withdraws HTTP
//! authority and records the rollback in `invalidate.json` before any
//! committed block leaves the journal.
use super::*;

#[derive(Default)]
struct FenceState {
    floor: u64,
    map_sha256: String,
    rollback: Option<RollbackRecord>,
    saved: Invalidations,
    /// A refused rollback: the controller halts on it.
    sealed: Option<(i128, u64)>,
}

pub struct RollbackFence {
    root: PathBuf,
    distribution: Option<super::super::distribution::Distribution>,
    state: std::sync::Mutex<FenceState>,
}

impl RollbackFence {
    pub(super) fn new(
        root: &Path,
        distribution: Option<super::super::distribution::Distribution>,
        saved: Invalidations,
        map_sha256: &str,
        floor: u64,
    ) -> Self {
        Self {
            root: root.to_path_buf(),
            distribution,
            state: std::sync::Mutex::new(FenceState {
                floor,
                map_sha256: map_sha256.into(),
                rollback: saved.rollback.clone(),
                saved,
                sealed: None,
            }),
        }
    }

    /// A fence with no distribution or recorded rollback, for writer tests.
    #[cfg(test)]
    pub(crate) fn detached(root: &Path, floor: u64) -> Self {
        Self::new(root, None, Invalidations::default(), "", floor)
    }

    /// The highest height a rollback may not reach below.
    pub fn floor(&self) -> u64 {
        self.state.lock().unwrap().floor
    }

    pub(super) fn set_floor(&self, floor: u64) {
        self.state.lock().unwrap().floor = floor;
    }

    /// Refuse a rollback to `ancestor` (`None` empties the journal) below the
    /// sealed floor, remembering it for the controller's durable halt.
    pub fn check(&self, ancestor: Option<u64>) -> Result<(), (i128, u64)> {
        let mut state = self.state.lock().unwrap();
        let ancestor = ancestor.map_or(-1, i128::from);
        if ancestor < i128::from(state.floor) {
            let refused = (ancestor, state.floor);
            state.sealed.get_or_insert(refused);
            return Err(refused);
        }
        Ok(())
    }

    /// A rollback a writer refused, if any.
    pub fn sealed(&self) -> Option<(i128, u64)> {
        self.state.lock().unwrap().sealed
    }

    /// Withdraw HTTP authority from `ancestor + 1`, then durably record the
    /// rollback under the active map. An error leaves the journal untouched.
    pub fn before_rollback(&self, ancestor: u64, hash: BlockHash) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        if let Some(distribution) = &self.distribution {
            distribution
                .withdraw(ancestor + 1)
                .map_err(|e| e.to_string())?;
        }
        Self::note(&mut state, ancestor, hash);
        let next = Invalidations {
            rollback: state.rollback.clone(),
        };
        next.store(&self.root)
            .map_err(|e| format!("recording the invalidation from {}: {e}", ancestor + 1))?;
        state.saved = next;
        Ok(())
    }

    /// Note a rollback a source learned of after the journal changed.
    pub(super) fn noted(&self, ancestor: u64, hash: BlockHash) {
        Self::note(&mut self.state.lock().unwrap(), ancestor, hash);
    }

    fn note(state: &mut FenceState, ancestor: u64, hash: BlockHash) {
        let map_sha256 = state.map_sha256.clone();
        note_rollback(&mut state.rollback, &map_sha256, ancestor, hash);
    }

    /// Mirror the rollback record into `invalidate.json` when it changed.
    pub(super) fn save(&self) -> Result<(), BoxError> {
        let mut state = self.state.lock().unwrap();
        let next = Invalidations {
            rollback: state.rollback.clone(),
        };
        if next != state.saved {
            next.store(&self.root)?;
            state.saved = next;
        }
        Ok(())
    }

    /// Forget the rollback record before a candidate may be published.
    pub(super) fn clear(&self) -> Result<(), BoxError> {
        let mut state = self.state.lock().unwrap();
        if state.rollback.is_some() {
            let next = Invalidations { rollback: None };
            next.store(&self.root)?;
            state.saved = next;
            state.rollback = None;
        }
        Ok(())
    }

    /// Later rollbacks are recorded under the newly active map.
    pub(super) fn activated(&self, map_sha256: &str) {
        self.state.lock().unwrap().map_sha256 = map_sha256.into();
    }
}
