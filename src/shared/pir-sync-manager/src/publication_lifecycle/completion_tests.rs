//! Completion of an installed commit across cancelled passes.
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// Commits one view; its peer release blocks until `release_gate` has a permit.
struct Adapter {
    retained: BTreeSet<String>,
    installed: Arc<AtomicUsize>,
    releases: Arc<AtomicUsize>,
    release_gate: Arc<tokio::sync::Semaphore>,
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
    async fn fence_serving(&mut self) -> Vec<MaterialIdentity> {
        Vec::new()
    }
    async fn close_admission(&mut self) {}
    async fn serving_subjects(
        &mut self,
        _envelope: &Envelope<Self>,
    ) -> Vec<(MaterialIdentity, ())> {
        Vec::new()
    }
    async fn plan(
        &mut self,
        _envelope: &Envelope<Self>,
        _objects: &mut ObjectFetcher<'_>,
    ) -> Result<PublicationPlan<()>, Error> {
        unreachable!("the test commits directly")
    }
    async fn load(&mut self, _candidate: (), _cache: &ObjectCache) -> Result<(), Error> {
        unreachable!("the test commits directly")
    }
    fn revalidate(
        &self,
        _envelope: &Envelope<Self>,
        _loaded: &(),
    ) -> Result<(MaterialIdentity, ()), Error> {
        unreachable!("the test commits directly")
    }
    async fn reserve_peer(&mut self, _loaded: &()) -> Result<PeerReservation, Error> {
        Ok(PeerReservation::Reserved)
    }
    fn retained_objects(&self, _loaded: &()) -> BTreeSet<String> {
        self.retained.clone()
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
        PeerHandover::Complete
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
    partial.promote(file).unwrap();
    object
}

/// A pass cancelled while releasing the peer keeps the completion pending:
/// the next pass releases again, the adapter receives the view exactly once,
/// and the collection that ran inside the commit is not lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pass_cancelled_during_peer_release_completes_on_the_next_pass() {
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
    let kept = store(&cache, b"kept");
    let stale = store(&cache, b"stale");
    let (installed, releases) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let release_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let adapter = Adapter {
        retained: BTreeSet::from([kept.sha256.clone()]),
        installed: installed.clone(),
        releases: releases.clone(),
        release_gate: release_gate.clone(),
    };
    let mut lifecycle = PublicationLifecycle::new(adapter, source, floor, cache.clone()).unwrap();
    lifecycle.persist_floor().await.unwrap();

    // The pass is cancelled while the peer release is blocked.
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(100),
        lifecycle.commit([9; 32], ())
    )
    .await
    .is_err());
    assert_eq!(installed.load(Ordering::SeqCst), 1);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
    assert!(lifecycle.pending_completion.is_some());
    assert_eq!(
        lifecycle.retained_commits,
        vec![BTreeSet::from([kept.sha256.clone()])]
    );
    assert!(cache.verified_cached(&stale).unwrap().is_none());
    assert!(cache.verified_cached(&kept).unwrap().is_some());

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
    assert!(cache.verified_cached(&kept).unwrap().is_some());
}
