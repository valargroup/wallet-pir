//! Completion and retention of installed commits across passes.
use super::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

/// Commits views referencing `retained`, declaring `required` as still
/// needed; its peer release blocks until `release_gate` has a permit and then
/// answers `handover`.
struct Adapter {
    retained: BTreeSet<String>,
    required: Vec<(ViewIdentity, ViewRequirement)>,
    handover: PeerHandover,
    installed: Arc<AtomicUsize>,
    releases: Arc<AtomicUsize>,
    release_gate: Arc<tokio::sync::Semaphore>,
    invalidated: Arc<std::sync::Mutex<Vec<ViewIdentity>>>,
    invalidating: Arc<tokio::sync::Notify>,
    invalidation_gate: Arc<tokio::sync::Semaphore>,
    revocations: Arc<AtomicUsize>,
    admission_closed: Arc<AtomicUsize>,
}
impl PublicationAdapter for Adapter {
    type Descriptor = ();
    type AuthoritySubject = ();
    type InstalledView = &'static str;
    type Candidate = ();
    type Loaded = ();
    type Report = u64;

    fn validate_envelope(&self, _envelope: &Envelope<Self>) -> Result<(), Error> {
        Ok(())
    }
    fn fence_serving(&mut self) -> impl Future<Output = Vec<ViewIdentity>> + Send + 'static {
        let revocations = self.revocations.clone();
        async move {
            revocations.fetch_add(1, Ordering::SeqCst);
            Vec::new()
        }
    }
    fn invalidate_views(
        &mut self,
        views: Vec<ViewIdentity>,
    ) -> impl Future<Output = ()> + Send + 'static {
        let invalidated = self.invalidated.clone();
        let invalidating = self.invalidating.clone();
        let gate = self.invalidation_gate.clone();
        async move {
            invalidating.notify_one();
            gate.acquire().await.unwrap().forget();
            invalidated.lock().unwrap().extend(views);
        }
    }
    fn close_admission(&mut self) -> impl Future<Output = ()> + Send + 'static {
        let closed = self.admission_closed.clone();
        async move {
            closed.fetch_add(1, Ordering::SeqCst);
        }
    }
    async fn serving_subjects(&mut self, _envelope: &Envelope<Self>) -> Vec<(ViewIdentity, ())> {
        Vec::new()
    }
    async fn plan(
        &mut self,
        _envelope: &Envelope<Self>,
        _objects: &mut ObjectFetcher<'_>,
    ) -> Result<PublicationPlan<()>, Error> {
        unreachable!("the test commits directly")
    }
    async fn load(
        &mut self,
        _candidate: (),
        _objects: pir_publication::VerifiedObjects,
    ) -> Result<(), Error> {
        unreachable!("the test commits directly")
    }
    async fn rebind(
        &mut self,
        _envelope: &Envelope<Self>,
        _loaded: (),
        _objects: &mut ObjectFetcher<'_>,
    ) -> Result<(), Error> {
        unreachable!("the test commits directly")
    }
    fn revalidate(
        &self,
        _envelope: &Envelope<Self>,
        _loaded: &(),
    ) -> Result<(ViewIdentity, ()), Error> {
        unreachable!("the test commits directly")
    }
    async fn reserve_peer(&mut self, _loaded: &()) -> Result<PeerReservation, Error> {
        Ok(PeerReservation::Reserved)
    }
    fn view_objects(&self, _loaded: &()) -> BTreeSet<String> {
        self.retained.clone()
    }
    async fn retained_views(&mut self) -> Vec<(ViewIdentity, ViewRequirement)> {
        self.required.clone()
    }
    fn prepare_commit(
        &mut self,
        _loaded: (),
    ) -> Result<(PreparedCommit<u64>, &'static str), Error> {
        Ok((
            PreparedCommit {
                record: Box::new(|| Ok(())),
                install: Box::pin(async { Ok(7) }),
                revoke_after_failed_record: Box::pin(async {}),
            },
            "view",
        ))
    }
    fn installed(&mut self, _view: &'static str) {
        self.installed.fetch_add(1, Ordering::SeqCst);
    }
    async fn release_peer(&mut self, _report: &u64) -> PeerHandover {
        self.releases.fetch_add(1, Ordering::SeqCst);
        self.release_gate.acquire().await.unwrap().forget();
        self.handover
    }
    async fn serving_report(&mut self) -> u64 {
        0
    }
}

fn store(cache: &ObjectCache, bytes: &[u8]) -> ObjectReference {
    let object = ObjectReference::for_bytes(bytes);
    let mut partial = cache.begin_partial(&object).unwrap();
    let mut file = partial.take_file().unwrap();
    std::io::Write::write_all(&mut file, bytes).unwrap();
    partial.promote_verified(file).unwrap();
    object
}

struct Fixture {
    _root: tempfile::TempDir,
    lifecycle: EnvelopeConsumer<Adapter>,
    cache: ObjectCache,
    installed: Arc<AtomicUsize>,
    releases: Arc<AtomicUsize>,
    release_gate: Arc<tokio::sync::Semaphore>,
}
async fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let source = HttpsSource::new("https://localhost:1", "/feed/", 1 << 20, &[]).unwrap();
    let floor = pir_publication::AuthorityFloor::<()>::open(
        root.path().join("floor.json"),
        pir_publication::SourceBinding {
            publisher_origin: source.origin().to_string(),
            product_namespace: "/feed/".into(),
            chain: [1; 32],
        },
    )
    .unwrap();
    let cache = ObjectCache::open(root.path().join("objects")).unwrap();
    let (installed, releases) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let release_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let adapter = Adapter {
        retained: BTreeSet::new(),
        required: Vec::new(),
        handover: PeerHandover::Complete,
        installed: installed.clone(),
        releases: releases.clone(),
        release_gate: release_gate.clone(),
        invalidated: Arc::default(),
        invalidating: Arc::default(),
        invalidation_gate: Arc::new(tokio::sync::Semaphore::new(0)),
        revocations: Arc::default(),
        admission_closed: Arc::default(),
    };
    let mut lifecycle = EnvelopeConsumer::new(adapter, source, floor, cache.clone()).unwrap();
    lifecycle.persist_floor().await.unwrap();
    Fixture {
        _root: root,
        lifecycle,
        cache,
        installed,
        releases,
        release_gate,
    }
}
fn objects(references: &[&ObjectReference]) -> BTreeSet<String> {
    references.iter().map(|r| r.sha256.clone()).collect()
}

#[tokio::test]
async fn cancelled_selective_invalidation_finishes_once_and_closes_admission_on_write_failure() {
    for fail_write in [false, true] {
        let mut fixture = fixture().await;
        if fail_write {
            let path = fixture._root.path().join("floor.json");
            std::fs::rename(&path, fixture._root.path().join("previous-floor.json")).unwrap();
            std::fs::create_dir(path).unwrap();
        }
        let invalidating = fixture.lifecycle.adapter.invalidating.clone();
        let gate = fixture.lifecycle.adapter.invalidation_gate.clone();
        let refuted = ViewIdentity([7; 32]);
        {
            let pass = fixture.lifecycle.invalidate_views(vec![refuted]);
            tokio::pin!(pass);
            tokio::select! {
                result = &mut pass => panic!("invalidation finished before its gate opened: {result:?}"),
                () = invalidating.notified() => {}
            }
            // Dropping the pass leaves revocation and persistence in the
            // owned slot, even though revocation has not acquired its lock yet.
        }
        assert!(fixture.lifecycle.floor.is_invalidated(&refuted));
        assert!(!fixture.lifecycle.floor.is_durable());
        assert!(fixture
            .lifecycle
            .adapter
            .invalidated
            .lock()
            .unwrap()
            .is_empty());
        gate.add_permits(1);
        fixture.lifecycle.settle_durable_operation().await.unwrap();
        fixture.lifecycle.settle_durable_operation().await.unwrap();
        assert_eq!(
            *fixture.lifecycle.adapter.invalidated.lock().unwrap(),
            vec![refuted]
        );
        assert!(!fixture
            .lifecycle
            .floor
            .is_invalidated(&ViewIdentity([8; 32])));
        assert_eq!(fixture.lifecycle.floor.is_durable(), !fail_write);
        assert_eq!(
            fixture.lifecycle.adapter.revocations.load(Ordering::SeqCst),
            usize::from(fail_write)
        );
        assert_eq!(
            fixture
                .lifecycle
                .adapter
                .admission_closed
                .load(Ordering::SeqCst),
            usize::from(fail_write)
        );
    }
}

/// A pass cancelled while releasing the peer keeps the completion pending:
/// the next pass releases again, the adapter receives the view exactly once,
/// and the collection that ran inside the commit is not lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pass_cancelled_during_peer_release_completes_on_the_next_pass() {
    let Fixture {
        mut lifecycle,
        cache,
        installed,
        releases,
        release_gate,
        _root,
        ..
    } = fixture().await;
    let kept = store(&cache, b"kept");
    let stale = store(&cache, b"stale");
    lifecycle.adapter.retained = objects(&[&kept]);

    // The pass is cancelled while the peer release is blocked.
    assert!(tokio::time::timeout(
        Duration::from_millis(100),
        lifecycle.commit(ViewIdentity([9; 32]), ())
    )
    .await
    .is_err());
    assert_eq!(installed.load(Ordering::SeqCst), 1);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
    assert!(lifecycle.pending_completion.is_some());
    assert_eq!(
        lifecycle.retained_views.collectable_beyond(),
        Some(objects(&[&kept]))
    );
    assert!(cache.open_verified(&stale).unwrap().is_none());
    assert!(cache.open_verified(&kept).unwrap().is_some());

    // The next pass finishes the completion before observing anything.
    release_gate.add_permits(1);
    lifecycle.settle_durable_operation().await.unwrap();
    assert!(lifecycle.pending_completion.is_none());
    assert_eq!(releases.load(Ordering::SeqCst), 2, "the release is retried");
    assert_eq!(
        installed.load(Ordering::SeqCst),
        1,
        "the adapter receives the view exactly once"
    );
    assert!(cache.open_verified(&kept).unwrap().is_some());
}

/// A view a peer holds keeps its objects through any number of
/// metadata-only commits that share the reusable material, while each
/// superseded, unneeded view's own objects are collected. Once the hold's
/// deadline passes, the next commit collects the held view's objects too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_views_survive_refreshes_until_their_deadline_passes() {
    let Fixture {
        mut lifecycle,
        cache,
        release_gate,
        _root,
        ..
    } = fixture().await;
    release_gate.add_permits(64);
    let held_view = ViewIdentity([1; 32]);
    let (held, material) = (store(&cache, b"held inventory"), store(&cache, b"material"));
    lifecycle.adapter.retained = objects(&[&held, &material]);
    lifecycle.commit(held_view, ()).await.unwrap();

    lifecycle.adapter.required = vec![(
        held_view,
        ViewRequirement::HeldUntil(SystemTime::now() + Duration::from_secs(3600)),
    )];
    let mut superseded: Vec<ObjectReference> = Vec::new();
    for refresh in 2..12u8 {
        let inventory = store(&cache, &[refresh]);
        lifecycle.adapter.retained = objects(&[&inventory, &material]);
        lifecycle
            .commit(ViewIdentity([refresh; 32]), ())
            .await
            .unwrap();
        assert!(cache.open_verified(&held).unwrap().is_some());
        assert!(cache.open_verified(&material).unwrap().is_some());
        assert!(cache.open_verified(&inventory).unwrap().is_some());
        for earlier in &superseded {
            assert!(cache.open_verified(earlier).unwrap().is_none());
        }
        superseded.push(inventory);
    }

    lifecycle.adapter.required = vec![(
        held_view,
        ViewRequirement::HeldUntil(SystemTime::now() - Duration::from_secs(1)),
    )];
    let last = store(&cache, b"last inventory");
    lifecycle.adapter.retained = objects(&[&last, &material]);
    lifecycle.commit(ViewIdentity([99; 32]), ()).await.unwrap();
    assert!(cache.open_verified(&held).unwrap().is_none());
    assert!(cache.open_verified(&material).unwrap().is_some());
    assert_eq!(
        lifecycle.retained_views.collectable_beyond(),
        Some(objects(&[&last, &material]))
    );
}

/// A peer release answered pending stays owed: each later pass releases
/// again before observing anything, without failing, and the local
/// installation never repeats. A completed release clears it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pending_peer_release_is_retried_on_later_passes_until_complete() {
    let Fixture {
        mut lifecycle,
        installed,
        releases,
        release_gate,
        _root,
        ..
    } = fixture().await;
    release_gate.add_permits(64);
    lifecycle.adapter.handover = PeerHandover::Pending;
    let (report, handover) = lifecycle.commit(ViewIdentity([3; 32]), ()).await.unwrap();
    assert_eq!((report, handover), (7, PeerHandover::Pending));
    assert_eq!(lifecycle.owed_peer_release, Some(7));

    lifecycle.settle_durable_operation().await.unwrap();
    assert_eq!(releases.load(Ordering::SeqCst), 2);
    assert_eq!(lifecycle.owed_peer_release, Some(7), "still owed");

    lifecycle.adapter.handover = PeerHandover::Complete;
    lifecycle.settle_durable_operation().await.unwrap();
    assert_eq!(releases.load(Ordering::SeqCst), 3);
    assert_eq!(lifecycle.owed_peer_release, None);
    lifecycle.settle_durable_operation().await.unwrap();
    assert_eq!(releases.load(Ordering::SeqCst), 3, "nothing is owed");
    assert_eq!(installed.load(Ordering::SeqCst), 1);
}
