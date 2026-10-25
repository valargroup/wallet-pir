//! Protocol-specific outcomes of shared owned durable operations.
use pir_pipeline::consumer::durable::*;
use pir_publication::RetainedViews;
use pir_publication::WrittenFloor;
use std::io;

pub(super) struct OwnedCommit<R, V> {
    pub(super) outcome: CommitOutcome<R>,
    pub(super) view: V,
    pub(super) retained_views: RetainedViews,
}
pub(super) enum DurableOutcome<R, V> {
    FloorWritten(io::Result<WrittenFloor>),
    Commit(OwnedCommit<R, V>),
}
#[cfg(test)]
mod tests {
    use super::*;
    use pir_publication::ViewIdentity;
    use std::sync::Arc;
    use std::{
        collections::BTreeSet,
        sync::{
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::Duration,
    };

    fn blocked_commit(
        release: mpsc::Receiver<()>,
        installed: Arc<AtomicBool>,
    ) -> PreparedCommit<()> {
        PreparedCommit {
            record: Box::new(move || {
                release.recv().unwrap();
                Ok(())
            }),
            install: Box::pin(async move {
                installed.store(true, Ordering::SeqCst);
                Ok(())
            }),
            revoke_after_failed_record: Box::pin(async {}),
        }
    }

    #[tokio::test]
    async fn a_cancelled_waiter_never_abandons_an_owned_commit_or_its_completion() {
        let fence = CommitFence::default();
        let installed = Arc::new(AtomicBool::new(false));
        let (release, blocked) = mpsc::channel();
        let mut slot = OwnedOperationSlot::new();
        let prepared = blocked_commit(blocked, installed.clone());
        let retained = || {
            RetainedViews::default().after_commit(
                ViewIdentity([1; 32]),
                BTreeSet::from(["object".to_string()]),
                &[],
                std::time::SystemTime::now(),
            )
        };
        let carried = retained();
        let commit = async move {
            OwnedCommit {
                outcome: run_commit(fence, 0, prepared, || Ok(())).await,
                view: "installed view",
                retained_views: carried,
            }
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(20), slot.run(commit))
                .await
                .is_err()
        );
        release.send(()).unwrap();
        let settled = slot.settle().await.unwrap().unwrap();
        assert!(matches!(
            settled.outcome,
            CommitOutcome::Installed {
                report: (),
                collected: Ok(())
            }
        ));
        assert!(installed.load(Ordering::SeqCst));
        assert_eq!(settled.view, "installed view");
        assert_eq!(settled.retained_views, retained());
        assert!(slot.settle().await.is_none(), "settled exactly once");
    }

    /// A pass cancelled while its commit collects leaves the collection
    /// owned: the next pass joins it before it can fetch, so an object that
    /// pass promotes afterwards is never removed under the older keep set.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_collection_cancelled_with_its_pass_finishes_before_the_next_pass_fetches() {
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = pir_publication::ObjectCache::open(cache_dir.path().into()).unwrap();
        let kept = pir_publication::ObjectReference::for_bytes(b"kept");
        let stale = pir_publication::ObjectReference::for_bytes(b"stale");
        for (object, bytes) in [(&kept, b"kept".as_slice()), (&stale, b"stale")] {
            let mut partial = cache.begin_partial(object).unwrap();
            let mut file = partial.take_file().unwrap();
            std::io::Write::write_all(&mut file, bytes).unwrap();
            partial.promote_verified(file).unwrap();
        }
        let (release, blocked) = mpsc::channel::<()>();
        let collecting = cache.clone();
        let keep = BTreeSet::from([kept.sha256.clone()]);
        let prepared = PreparedCommit::<()> {
            record: Box::new(|| Ok(())),
            install: Box::pin(async { Ok(()) }),
            revoke_after_failed_record: Box::pin(async {}),
        };
        let mut slot = OwnedOperationSlot::new();
        let commit = run_commit(CommitFence::default(), 0, prepared, move || {
            blocked.recv().unwrap();
            collecting.collect_unreferenced(&keep)
        });
        // The pass is cancelled while the collection is blocked.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), slot.run(commit))
                .await
                .is_err()
        );
        release.send(()).unwrap();
        // The next pass settles the operation first...
        let settled = slot.settle().await.unwrap().unwrap();
        assert!(matches!(
            settled,
            CommitOutcome::Installed {
                collected: Ok(()),
                ..
            }
        ));
        assert!(cache.open_verified(&stale).unwrap().is_none());
        // ...and only then fetches, so what it promotes survives.
        let fetched = pir_publication::ObjectReference::for_bytes(b"fetched");
        let mut partial = cache.begin_partial(&fetched).unwrap();
        let mut file = partial.take_file().unwrap();
        std::io::Write::write_all(&mut file, b"fetched").unwrap();
        partial.promote_verified(file).unwrap();
        assert!(cache.open_verified(&fetched).unwrap().is_some());
        assert!(cache.open_verified(&kept).unwrap().is_some());
    }
}
