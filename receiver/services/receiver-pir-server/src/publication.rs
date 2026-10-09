//! Immutable prepared publications and atomic, reorg-fenced serving state.
use axum::{body::Bytes, http::StatusCode};
use receiver_directory::{snapshot::Manifest, Hash};
use receiver_pir::server::Server;
use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

/// One revision prepared for serving, with its optional common witness file and the
/// owner's report on the index it was built from.
pub struct Publication {
    /// Shared with the same revision under a replaced report; see
    /// [`Publications::replace_report`].
    pub(crate) server: Arc<Server>,
    pub(crate) witnesses: Option<Bytes>,
    pub(crate) id: Hash,
    pub(crate) report: Option<serde_json::Value>,
}

impl Publication {
    /// Prepare a complete immutable revision. The caller must separately accept its chain anchor.
    pub fn new(server: Server, witnesses: Option<Vec<u8>>) -> Result<Self, receiver_pir::Error> {
        if let Some(proof) = &witnesses {
            receiver_directory::witness::WitnessSnapshot::decode(
                proof,
                &server.manifest().directory,
            )?;
        }
        Ok(Self {
            id: server.manifest().id()?,
            server: Arc::new(server),
            witnesses: witnesses.map(Into::into),
            report: None,
        })
    }

    /// Attaches the owner's report, such as feed freshness, which health serves with
    /// this publication's ID for monitoring, so the two activate together.
    pub fn with_report(mut self, report: serde_json::Value) -> Self {
        self.report = Some(report);
        self
    }

    /// The directory manifest this publication serves.
    pub fn manifest(&self) -> &Manifest {
        &self.server.manifest().directory
    }

    /// The session ID, which names this publication in every route.
    pub fn id(&self) -> Hash {
        self.id
    }
}

/// How long a displaced revision keeps serving the sessions that began on it.
const GRACE: Duration = Duration::from_secs(60);

#[derive(Default)]
struct State {
    epoch: u64,
    current: Option<Arc<Publication>>,
    previous: Option<(Arc<Publication>, Instant)>,
    revoked: VecDeque<Hash>,
}

impl State {
    /// When the previous revision's grace ends, if it has not yet.
    fn grace_until(&self) -> Option<Instant> {
        self.previous
            .as_ref()
            .map(|(_, until)| *until)
            .filter(|until| *until > Instant::now())
    }

    /// Whether the previous revision's grace has ended, so it can be dropped.
    fn previous_expired(&self) -> bool {
        self.previous.is_some() && self.grace_until().is_none()
    }

    /// See [`Publications::anchors`].
    fn anchors(&self) -> Vec<(u32, Hash)> {
        self.current
            .iter()
            .chain(
                self.previous
                    .iter()
                    .filter(|(_, until)| *until > Instant::now())
                    .map(|(p, _)| p),
            )
            .map(|p| (p.manifest().end_height, p.manifest().end_hash))
            .collect()
    }

    /// See [`Publications::revoke`].
    fn revoke(&mut self) {
        self.epoch = self.epoch.checked_add(1).expect("recovery epoch exhausted");
        if let Some(p) = self.current.take() {
            self.revoked.push_back(p.id);
        }
        if let Some((p, _)) = self.previous.take() {
            self.revoked.push_back(p.id);
        }
        while self.revoked.len() > 8 {
            self.revoked.pop_front();
        }
    }
}

/// One current and one briefly retained canonical revision. Revocation also fences in-flight work.
#[derive(Clone, Default)]
pub struct Publications(Arc<RwLock<State>>);

impl Publications {
    /// Drops the previous revision once its grace has ended, freeing its rows and PIR
    /// state; queries still running on it keep their own reference. Every read path
    /// calls it, including the owner's periodic [`Self::anchors`] check, so an idle
    /// service frees it within a poll.
    fn expire(&self) {
        if self.0.read().unwrap().previous_expired() {
            let mut state = self.0.write().unwrap();
            if state.previous_expired() {
                state.previous = None;
            }
        }
    }

    /// The recovery epoch. Every revocation advances it.
    pub fn epoch(&self) -> u64 {
        self.0.read().unwrap().epoch
    }

    /// Anchors that still accept new requests, current first. Validate independently of
    /// expensive preparation.
    pub fn anchors(&self) -> Vec<(u32, Hash)> {
        self.serving().1
    }

    /// The recovery epoch and [`Self::anchors`], read together for
    /// [`Self::revoke_serving`].
    pub fn serving(&self) -> (u64, Vec<(u32, Hash)>) {
        self.expire();
        let state = self.0.read().unwrap();
        (state.epoch, state.anchors())
    }

    /// When the next revision may activate, if the previous one is still in its grace.
    /// Activating only after it ends gives every displaced revision its full grace, so
    /// two quick rotations cannot strand a session.
    pub fn ready_at(&self) -> Option<Instant> {
        self.expire();
        self.0.read().unwrap().grace_until()
    }

    /// Install only if no revocation occurred since the caller began preparing and validating,
    /// and no earlier revision is still in its grace (see [`Self::ready_at`]).
    /// The caller must verify the new canonical anchor and revoke before replacing forked coverage.
    pub fn publish(&self, publication: Publication, expected_epoch: u64) -> bool {
        let mut state = self.0.write().unwrap();
        if state.epoch != expected_epoch || state.grace_until().is_some() {
            return false;
        }
        state.revoked.retain(|id| *id != publication.id);
        state.previous = state.current.take().map(|p| (p, Instant::now() + GRACE));
        state.current = Some(Arc::new(publication));
        true
    }

    /// Replaces the owner's report on the current publication `id`, if no revocation
    /// occurred since `expected_epoch`, for a report that changed while the
    /// publication did not. The prepared revision, its sessions and the previous
    /// revision's grace are unchanged. Returns whether it replaced the report.
    pub fn replace_report(&self, id: Hash, report: serde_json::Value, expected_epoch: u64) -> bool {
        let mut state = self.0.write().unwrap();
        let Some(current) = state.current.as_ref().filter(|p| p.id == id) else {
            return false;
        };
        if state.epoch != expected_epoch {
            return false;
        }
        let replaced = Publication {
            server: current.server.clone(),
            witnesses: current.witnesses.clone(),
            id,
            report: Some(report),
        };
        state.current = Some(Arc::new(replaced));
        true
    }

    /// Conservatively invalidate all sessions, including CPU work that started before the reorg.
    pub fn revoke(&self) {
        self.0.write().unwrap().revoke();
    }

    /// For a check that read `epoch` and `anchor` from [`Self::serving`] and found the
    /// anchor off the chain: revokes as [`Self::revoke`] does, but atomically only if
    /// `epoch` is still current and `anchor` still accepts requests, so a publication
    /// made after a revocation, or after `anchor`'s grace ended, survives the stale
    /// result. An anchor in its grace still revokes every session. Returns whether it
    /// revoked.
    pub fn revoke_serving(&self, epoch: u64, anchor: (u32, Hash)) -> bool {
        let mut state = self.0.write().unwrap();
        if state.epoch != epoch || !state.anchors().contains(&anchor) {
            return false;
        }
        state.revoke();
        true
    }

    /// The publication `id` names, or the current one for `None`, with the current
    /// epoch. A revoked id is `GONE` and an unknown or expired one is `CONFLICT`.
    pub(crate) fn select(&self, id: Option<Hash>) -> Result<(Arc<Publication>, u64), StatusCode> {
        self.expire();
        let state = self.0.read().unwrap();
        if let Some(id) = id {
            if state.revoked.contains(&id) {
                return Err(StatusCode::GONE);
            }
            for p in state.current.iter().chain(
                state
                    .previous
                    .iter()
                    .filter(|(_, until)| *until > Instant::now())
                    .map(|(p, _)| p),
            ) {
                if p.id == id {
                    return Ok((p.clone(), state.epoch));
                }
            }
            return Err(StatusCode::CONFLICT);
        }
        state
            .current
            .clone()
            .map(|p| (p, state.epoch))
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)
    }
}

#[cfg(test)]
#[path = "../../../crates/receiver-directory/tests/common/mod.rs"]
mod common;

#[cfg(test)]
mod tests {
    use super::*;

    /// An empty prepared publication whose salt starts with `salt`.
    fn publication(salt: u8) -> Publication {
        publication_at(salt, 3)
    }

    /// An empty prepared publication whose salt starts with `salt`, ending at a block
    /// hash of `hash` bytes.
    fn publication_at(salt: u8, hash: u8) -> Publication {
        let mut manifest = super::common::manifest(receiver_pir::MIN_ROWS);
        manifest.salt[0] = salt;
        manifest.end_hash = [hash; 32];
        let snapshot = receiver_directory::snapshot::Snapshot::build(manifest, &[], &[]).unwrap();
        Publication::new(Server::new(snapshot).unwrap(), None).unwrap()
    }

    /// A check that found an anchor off the chain revokes only while its epoch is
    /// current and the anchor still accepts requests.
    #[test]
    fn a_stale_check_revokes_only_while_its_anchor_is_served() {
        let (a, b) = ((101, [1; 32]), (101, [2; 32]));
        let publications = Publications::default();
        assert!(publications.publish(publication_at(1, 1), 0));
        // A check reads A, then a revocation and B's publication overtake it.
        let (epoch, anchors) = publications.serving();
        assert_eq!(anchors, [a]);
        publications.revoke();
        assert!(publications.publish(publication_at(2, 2), epoch + 1));
        assert!(!publications.revoke_serving(epoch, a));
        assert_eq!(publications.serving(), (epoch + 1, vec![b]));
        // An ordinary rotation keeps the epoch, but once A's grace ends a check that
        // read A no longer revokes C.
        let publications = Publications::default();
        assert!(publications.publish(publication_at(1, 1), 0));
        let (epoch, _) = publications.serving();
        assert!(publications.publish(publication_at(3, 3), 0));
        let ended = Instant::now()
            .checked_sub(Duration::from_millis(1))
            .unwrap();
        publications.0.write().unwrap().previous.as_mut().unwrap().1 = ended;
        assert!(!publications.revoke_serving(epoch, a));
        assert_eq!(publications.anchors(), [(101, [3; 32])]);
        // A served anchor off the chain still revokes every session.
        assert!(publications.revoke_serving(epoch, (101, [3; 32])));
        assert_eq!(publications.serving(), (epoch + 1, vec![]));
    }

    /// An anchor still in its grace revokes the current publication with it.
    #[test]
    fn an_anchor_in_its_grace_still_revokes_every_session() {
        let publications = Publications::default();
        assert!(publications.publish(publication_at(1, 1), 0));
        let (epoch, _) = publications.serving();
        assert!(publications.publish(publication_at(2, 2), 0));
        assert!(publications.revoke_serving(epoch, (101, [1; 32])));
        assert!(publications.anchors().is_empty());
    }

    /// A replaced report keeps the current revision, its sessions and the previous
    /// revision's grace, and needs the current ID and epoch.
    #[test]
    fn only_the_current_publication_takes_a_new_report() {
        let publications = Publications::default();
        assert!(publications.publish(publication(1), 0));
        let previous = publications.select(None).unwrap().0.id;
        assert!(publications.publish(publication(2), 0));
        let (current, _) = publications.select(None).unwrap();
        let report = serde_json::json!({"payouts_missing": 1});
        assert!(!publications.replace_report(previous, report.clone(), 0));
        assert!(!publications.replace_report(current.id, report.clone(), 1));
        let grace = publications.ready_at();
        assert!(publications.replace_report(current.id, report.clone(), 0));
        let (replaced, epoch) = publications.select(None).unwrap();
        assert_eq!((replaced.id, epoch), (current.id, 0));
        assert!(Arc::ptr_eq(&replaced.server, &current.server));
        assert_eq!(replaced.report, Some(report));
        assert!(publications.select(Some(previous)).is_ok());
        assert_eq!(publications.ready_at(), grace);
        publications.revoke();
        assert!(!publications.replace_report(current.id, serde_json::Value::Null, 1));
    }

    /// A displaced revision is dropped once its grace ends, not kept until the next
    /// publication.
    #[test]
    fn an_expired_previous_publication_is_dropped() {
        let publications = Publications::default();
        assert!(publications.publish(publication(1), 0));
        let first = Arc::downgrade(publications.0.read().unwrap().current.as_ref().unwrap());
        let id = first.upgrade().unwrap().id;
        assert!(publications.publish(publication(2), 0));
        assert!(publications.select(Some(id)).is_ok());
        assert!(first.upgrade().is_some());
        let ended = Instant::now()
            .checked_sub(Duration::from_millis(1))
            .unwrap();
        publications.0.write().unwrap().previous.as_mut().unwrap().1 = ended;
        assert_eq!(publications.anchors().len(), 1);
        assert!(first.upgrade().is_none());
        assert!(matches!(
            publications.select(Some(id)),
            Err(StatusCode::CONFLICT)
        ));
    }
}
