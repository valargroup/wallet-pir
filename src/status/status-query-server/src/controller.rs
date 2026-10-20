//! Which generations a Status process serves, and when a request against one
//! must be refused.
use super::{now_ms, Generation};
use crate::protocol::*;
use http::StatusCode;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};

struct Views {
    active: Arc<Generation>,
    retained: Vec<Arc<Generation>>,
}
#[derive(Clone)]
pub struct Controller {
    views: Arc<RwLock<Views>>,
    revoked: Arc<AtomicBool>,
}
impl Controller {
    pub fn new(g: Generation) -> Self {
        Self {
            revoked: Arc::new(AtomicBool::new(false)),
            views: Arc::new(RwLock::new(Views {
                active: Arc::new(g),
                retained: Vec::new(),
            })),
        }
    }
    pub fn can_prepare(&self) -> bool {
        let views = self.views.read().unwrap();
        views.retained.iter().all(|g| Arc::strong_count(g) == 1)
    }
    pub fn revoke(&self) {
        self.revoked.store(true, Ordering::SeqCst);
    }
    pub fn revoked(&self) -> bool {
        self.revoked.load(Ordering::SeqCst)
    }
    pub fn current(&self) -> Arc<Generation> {
        self.views.read().unwrap().active.clone()
    }
    pub fn resolve(&self, id: Hash) -> Result<Arc<Generation>, StatusCode> {
        let views = self.views.read().unwrap();
        std::iter::once(&views.active)
            .chain(views.retained.iter())
            .find(|g| g.manifest().id() == id)
            .cloned()
            .ok_or(StatusCode::CONFLICT)
    }
    pub fn for_body(&self, body: &[u8]) -> Result<Arc<Generation>, StatusCode> {
        if body.len() < HEADER_BYTES || &body[..4] != QUERY_MAGIC {
            return Err(StatusCode::BAD_REQUEST);
        }
        self.resolve(body[4..36].try_into().unwrap())
    }
    pub fn activate(&self, g: Generation) -> Result<(), Error> {
        self.activate_deferring_release(g).map(drop)
    }
    /// Activate like [`Self::activate`], but return the generations it evicts
    /// instead of freeing them under the views lock. Freeing a generation's
    /// packing material can take long enough to stall every query waiting on it.
    pub fn activate_deferring_release(&self, g: Generation) -> Result<Vec<Arc<Generation>>, Error> {
        g.manifest().fresh(now_ms())?;
        let mut views = self.views.write().unwrap();
        if g.manifest().generation <= views.active.manifest().generation
            || g.manifest().recovery_epoch < views.active.manifest().recovery_epoch
            || g.manifest().network != views.active.manifest().network
        {
            return Err(Error::Malformed);
        }
        // Keep only the immediately preceding material generation. In-flight
        // requests pin their own Arc; older sessions receive 409 and must
        // reinitialize. This bounds material retention without serializing
        // publications behind the entire freshness window.
        let old_active = views.active.clone();
        // Preserve pins across metadata-only refreshes as well as material
        // changes. `can_prepare` applies backpressure until older pins drain;
        // losing their bookkeeping here would allow unbounded hidden material.
        let (kept, evicted): (Vec<_>, Vec<_>) = views
            .retained
            .drain(..)
            .partition(|old| Arc::strong_count(old) > 1);
        views.retained = kept;
        // Retain one revoked view as a tombstone so clients receive 410 on
        // epoch changes. `check` rejects it before its material is used.
        views.retained.push(old_active);
        views.active = Arc::new(g);
        Ok(evicted)
    }
    pub fn check(&self, g: &Generation) -> Result<(), StatusCode> {
        if self.revoked.load(Ordering::SeqCst)
            || self.current().manifest().recovery_epoch != g.manifest().recovery_epoch
        {
            return Err(StatusCode::GONE);
        }
        g.manifest()
            .fresh(now_ms())
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
    }
}

#[cfg(test)]
mod controller_tests {
    use super::*;

    fn generation() -> Generation {
        Generation::public_only(
            Manifest {
                protocol: PROTOCOL.into(),
                network: [1; 32],
                salt: [2; 32],
                generation: 1,
                recovery_epoch: 0,
                coverage_start: 1,
                anchor_height: 1,
                anchor_hash: [3; 32],
                observed_ms: now_ms(),
                entries: 0,
                rows_digest: [4; 32],
                public_digest: [5; 32],
            },
            Arc::new(Vec::new()),
        )
    }

    #[test]
    fn rapid_activation_bounds_retention_and_recovery_fences_old_sessions() {
        let base = generation();
        let controller = Controller::new(base.clone());
        let first = base.manifest().id();
        let second = base.reidentified(
            2,
            base.manifest().recovery_epoch,
            base.manifest().observed_ms,
        );
        let second_id = second.manifest().id();
        controller.activate(second).unwrap();
        assert!(controller.resolve(first).is_ok());
        let third = base.reidentified(
            3,
            base.manifest().recovery_epoch,
            base.manifest().observed_ms,
        );
        let third_id = third.manifest().id();
        controller.activate(third).unwrap();
        assert_eq!(controller.resolve(first).err(), Some(StatusCode::CONFLICT));
        assert!(controller.resolve(second_id).is_ok());
        let recovered = base.reidentified(4, 1, base.manifest().observed_ms);
        controller.activate(recovered).unwrap();
        let revoked = controller.resolve(third_id).unwrap();
        assert_eq!(controller.check(&revoked), Err(StatusCode::GONE));
    }

    #[test]
    fn deferred_activation_hands_back_unpinned_generations_only() {
        let base = generation();
        let controller = Controller::new(base.clone());
        let second = base.reidentified(
            2,
            base.manifest().recovery_epoch,
            base.manifest().observed_ms,
        );
        assert!(controller
            .activate_deferring_release(second)
            .unwrap()
            .is_empty());
        // Generation 1 is now retained only by the controller, so the next
        // activation evicts it and returns it to the caller to free.
        let third = base.reidentified(
            3,
            base.manifest().recovery_epoch,
            base.manifest().observed_ms,
        );
        let evicted = controller.activate_deferring_release(third).unwrap();
        assert_eq!(
            evicted
                .iter()
                .map(|g| g.manifest().generation)
                .collect::<Vec<_>>(),
            vec![1]
        );
        // A generation pinned by an in-flight request is kept, not evicted.
        let pinned = controller.current();
        let fourth = base.reidentified(
            4,
            base.manifest().recovery_epoch,
            base.manifest().observed_ms,
        );
        assert!(controller
            .activate_deferring_release(fourth)
            .unwrap()
            .iter()
            .all(|g| g.manifest().generation != 3));
        let fifth = base.reidentified(
            5,
            base.manifest().recovery_epoch,
            base.manifest().observed_ms,
        );
        let evicted = controller.activate_deferring_release(fifth).unwrap();
        assert!(evicted.iter().all(|g| g.manifest().generation != 3));
        drop(pinned);
    }

    #[test]
    fn identity_transitions_keep_the_prepared_material() {
        let base = generation();
        let observed = base.reobserved(base.manifest().observed_ms + 7);
        let mut expected = base.manifest().clone();
        expected.observed_ms += 7;
        assert_eq!(observed.manifest(), &expected);
        assert!(Arc::ptr_eq(observed.public(), base.public()));

        let next = observed.reidentified(9, 2, expected.observed_ms + 1);
        expected.generation = 9;
        expected.recovery_epoch = 2;
        expected.observed_ms += 1;
        assert_eq!(next.manifest(), &expected);
        assert_eq!(next.manifest().rows_digest, base.manifest().rows_digest);
        assert_eq!(next.manifest().public_digest, base.manifest().public_digest);
        assert!(Arc::ptr_eq(next.public(), base.public()));
        // The source generation is left as it was.
        assert_eq!(base.manifest().generation, 1);
    }

    #[test]
    fn stale_candidate_cannot_be_activated() {
        let base = generation();
        let controller = Controller::new(base.clone());
        let stale = base.reidentified(2, base.manifest().recovery_epoch, now_ms() - MAX_AGE_MS - 1);
        assert_eq!(controller.activate(stale), Err(Error::Stale));
        assert_eq!(controller.current().manifest().generation, 1);
    }
}
