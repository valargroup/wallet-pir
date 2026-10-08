//! Immutable prepared publications and atomic, reorg-fenced serving state.
use axum::{body::Bytes, http::StatusCode};
use receiver_directory::{snapshot::Manifest, Hash};
use receiver_pir::server::Server;
use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

/// One revision prepared for serving, with its optional common witness file.
pub struct Publication {
    pub(crate) server: Server,
    pub(crate) witnesses: Option<Bytes>,
    pub(crate) id: Hash,
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
            server,
            witnesses: witnesses.map(Into::into),
        })
    }

    /// The directory manifest this publication serves.
    pub fn manifest(&self) -> &Manifest {
        &self.server.manifest().directory
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
    report: Option<serde_json::Value>,
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

    /// Anchors that still accept new requests. Validate independently of expensive preparation.
    pub fn anchors(&self) -> Vec<(u32, Hash)> {
        self.expire();
        let state = self.0.read().unwrap();
        state
            .current
            .iter()
            .chain(
                state
                    .previous
                    .iter()
                    .filter(|(_, until)| *until > Instant::now())
                    .map(|(p, _)| p),
            )
            .map(|p| (p.manifest().end_height, p.manifest().end_hash))
            .collect()
    }

    /// Records the owner's latest report, such as feed freshness, which health serves
    /// for monitoring.
    pub fn set_report(&self, report: serde_json::Value) {
        self.0.write().unwrap().report = Some(report);
    }

    /// See [`Self::set_report`].
    pub(crate) fn report(&self) -> Option<serde_json::Value> {
        self.0.read().unwrap().report.clone()
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

    /// Conservatively invalidate all sessions, including CPU work that started before the reorg.
    pub fn revoke(&self) {
        let mut state = self.0.write().unwrap();
        state.epoch = state
            .epoch
            .checked_add(1)
            .expect("recovery epoch exhausted");
        if let Some(p) = state.current.take() {
            state.revoked.push_back(p.id);
        }
        if let Some((p, _)) = state.previous.take() {
            state.revoked.push_back(p.id);
        }
        while state.revoked.len() > 8 {
            state.revoked.pop_front();
        }
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
        let mut manifest = super::common::manifest(receiver_pir::MIN_ROWS);
        manifest.salt[0] = salt;
        let snapshot = receiver_directory::snapshot::Snapshot::build(manifest, &[], &[]).unwrap();
        Publication::new(Server::new(snapshot).unwrap(), None).unwrap()
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
