//! Which generations a Status process serves, and when a request against one
//! must be refused.
use super::{now_ms, Generation};
use axum::http::StatusCode;
use enhance_pir::status::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};

pub(super) struct Views {
    pub(super) active: Arc<Generation>,
    pub(super) retained: Vec<Arc<Generation>>,
}
#[derive(Clone)]
pub struct Controller {
    pub(super) views: Arc<RwLock<Views>>,
    pub(super) revoked: Arc<AtomicBool>,
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
    pub fn current(&self) -> Arc<Generation> {
        self.views.read().unwrap().active.clone()
    }
    pub(super) fn resolve(&self, id: Hash) -> Result<Arc<Generation>, StatusCode> {
        let views = self.views.read().unwrap();
        std::iter::once(&views.active)
            .chain(views.retained.iter())
            .find(|g| g.manifest.id() == id)
            .cloned()
            .ok_or(StatusCode::CONFLICT)
    }
    pub(super) fn for_body(&self, body: &[u8]) -> Result<Arc<Generation>, StatusCode> {
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
        g.manifest.fresh(now_ms())?;
        let mut views = self.views.write().unwrap();
        if g.manifest.generation <= views.active.manifest.generation
            || g.manifest.recovery_epoch < views.active.manifest.recovery_epoch
            || g.manifest.network != views.active.manifest.network
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
    pub(super) fn check(&self, g: &Generation) -> Result<(), StatusCode> {
        if self.revoked.load(Ordering::SeqCst)
            || self.current().manifest.recovery_epoch != g.manifest.recovery_epoch
        {
            return Err(StatusCode::GONE);
        }
        g.manifest
            .fresh(now_ms())
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
    }
}

#[cfg(test)]
mod controller_tests {
    use super::*;
    use crate::status::http::{query, serving_http_client, HttpState};
    use axum::{body::Bytes, extract::State, routing::get, Router};
    use std::time::Duration;

    fn generation() -> Generation {
        Generation {
            manifest: Manifest {
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
            public: Arc::new(Vec::new()),
            units: Vec::new(),
            packing: Arc::new(Vec::new()),
            top: None,
            packing_digests: Vec::new(),
        }
    }

    #[tokio::test]
    async fn serving_client_clones_reuse_forwarding_connection() {
        async fn peer(
            axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<std::net::SocketAddr>,
        ) -> String {
            peer.to_string()
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().route("/", get(peer));
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        let client = serving_http_client().unwrap();
        let first = client
            .get(&origin)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let second = client
            .clone()
            .get(&origin)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(
            first, second,
            "requests must reuse the same TCP/SSH channel"
        );
        server.abort();
    }

    #[tokio::test]
    async fn authority_is_rechecked_after_admission_wait() {
        let controller = Controller::new(generation());
        let state = HttpState::new(controller.clone(), "http://127.0.0.1:1".into());
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(state.permits.acquire("router").await.unwrap());
        }
        let mut body = vec![0; HEADER_BYTES];
        body[..4].copy_from_slice(QUERY_MAGIC);
        body[4..36].copy_from_slice(&controller.current().manifest.id());
        let request = query(State(state), Bytes::from(body));
        tokio::pin!(request);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut request)
                .await
                .is_err()
        );
        controller.revoked.store(true, Ordering::Release);
        drop(held);
        assert_eq!(request.await, Err(StatusCode::GONE));
    }

    #[tokio::test]
    async fn source_freshness_is_rechecked_after_admission_wait() {
        let mut g = generation();
        g.manifest.observed_ms = now_ms() - MAX_AGE_MS + 100;
        let controller = Controller::new(g);
        let state = HttpState::new(controller.clone(), "http://127.0.0.1:1".into());
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(state.permits.acquire("router").await.unwrap());
        }
        let mut body = vec![0; HEADER_BYTES];
        body[..4].copy_from_slice(QUERY_MAGIC);
        body[4..36].copy_from_slice(&controller.current().manifest.id());
        let request = query(State(state), Bytes::from(body));
        tokio::pin!(request);
        assert!(
            tokio::time::timeout(Duration::from_millis(120), &mut request)
                .await
                .is_err()
        );
        drop(held);
        assert_eq!(request.await, Err(StatusCode::SERVICE_UNAVAILABLE));
    }

    #[test]
    fn rapid_activation_bounds_retention_and_recovery_fences_old_sessions() {
        let base = generation();
        let controller = Controller::new(base.clone());
        let first = base.manifest.id();
        let mut second = base.clone();
        second.manifest.generation = 2;
        let second_id = second.manifest.id();
        controller.activate(second).unwrap();
        assert!(controller.resolve(first).is_ok());
        let mut third = base.clone();
        third.manifest.generation = 3;
        let third_id = third.manifest.id();
        controller.activate(third).unwrap();
        assert_eq!(controller.resolve(first).err(), Some(StatusCode::CONFLICT));
        assert!(controller.resolve(second_id).is_ok());
        let mut recovered = base;
        recovered.manifest.generation = 4;
        recovered.manifest.recovery_epoch = 1;
        controller.activate(recovered).unwrap();
        let revoked = controller.resolve(third_id).unwrap();
        assert_eq!(controller.check(&revoked), Err(StatusCode::GONE));
    }

    #[test]
    fn deferred_activation_hands_back_unpinned_generations_only() {
        let base = generation();
        let controller = Controller::new(base.clone());
        let mut second = base.clone();
        second.manifest.generation = 2;
        assert!(controller
            .activate_deferring_release(second)
            .unwrap()
            .is_empty());
        // Generation 1 is now retained only by the controller, so the next
        // activation evicts it and returns it to the caller to free.
        let mut third = base.clone();
        third.manifest.generation = 3;
        let evicted = controller.activate_deferring_release(third).unwrap();
        assert_eq!(
            evicted
                .iter()
                .map(|g| g.manifest.generation)
                .collect::<Vec<_>>(),
            vec![1]
        );
        // A generation pinned by an in-flight request is kept, not evicted.
        let pinned = controller.current();
        let mut fourth = base.clone();
        fourth.manifest.generation = 4;
        assert!(controller
            .activate_deferring_release(fourth)
            .unwrap()
            .iter()
            .all(|g| g.manifest.generation != 3));
        let mut fifth = base;
        fifth.manifest.generation = 5;
        let evicted = controller.activate_deferring_release(fifth).unwrap();
        assert!(evicted.iter().all(|g| g.manifest.generation != 3));
        drop(pinned);
    }

    #[test]
    fn stale_candidate_cannot_be_activated() {
        let base = generation();
        let controller = Controller::new(base.clone());
        let mut stale = base;
        stale.manifest.generation = 2;
        stale.manifest.observed_ms = now_ms() - MAX_AGE_MS - 1;
        assert_eq!(controller.activate(stale), Err(Error::Stale));
        assert_eq!(controller.current().manifest.generation, 1);
    }
}
