//! Durable work the runtime owns: floor writes and commits.
//!
//! Both run as spawned tasks held in one [`OwnedOperationSlot`], so cancelling
//! a pass never abandons or duplicates a write: the next pass settles the
//! outstanding operation before it observes anything. Commits additionally
//! check the [`CommitFence`] generation they started under, atomically with
//! installing, so a fence raised while a commit was recording wins. A commit
//! that installed also collects unreferenced objects inside the same
//! operation, so a collection with an older keep set always finishes before a
//! later pass fetches anything.
use super::Error;
use std::{fmt, future::Future, io, pin::Pin, sync::Arc};
use tokio::task::JoinHandle;

type BoxedFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A product's commit of one loaded view, executed by the engine as
/// validate -> record -> fence check -> install.
pub struct PreparedCommit<R> {
    /// Durably record the view (for Status, the active-inventory record). Runs
    /// off the executor. A record never confers permission to serve.
    pub record: Box<dyn FnOnce() -> io::Result<()> + Send>,
    /// Install the recorded view. Runs while holding the commit fence; an error
    /// must leave serving unchanged.
    pub install: BoxedFuture<Result<R, Error>>,
    /// Stop serving and close admission after a failed record, which may have
    /// replaced the previous record. Runs under the commit fence.
    pub revoke_after_failed_record: BoxedFuture<()>,
}

/// How an owned commit ended.
pub enum CommitOutcome<R> {
    /// The view serves; `collected` reports the collection that followed.
    Installed {
        report: R,
        collected: io::Result<()>,
    },
    /// The install refused; serving is unchanged.
    InstallFailed(Error),
    /// Serving was fenced after the commit started; nothing was installed.
    Fenced,
    RecordFailed(io::Error),
}

/// Serializes revocation against installation. Every fence raises the
/// generation while revoking; a commit installs only if the generation is
/// still the one it captured before recording.
#[derive(Clone, Default)]
pub struct CommitFence(Arc<tokio::sync::Mutex<u64>>);
impl CommitFence {
    pub async fn generation(&self) -> u64 {
        *self.0.lock().await
    }
    /// Raise the generation and run `revoke` while holding the fence.
    pub async fn fence<F: Future>(&self, revoke: F) -> F::Output {
        let mut generation = self.0.lock().await;
        *generation += 1;
        revoke.await
    }
}

/// Record, then install only if no fence was raised since `started_under`,
/// then run `collect` (off the executor) once the view serves.
pub async fn run_commit<R>(
    fence: CommitFence,
    started_under: u64,
    prepared: PreparedCommit<R>,
    collect: impl FnOnce() -> io::Result<()> + Send + 'static,
) -> CommitOutcome<R> {
    let PreparedCommit {
        record,
        install,
        revoke_after_failed_record,
    } = prepared;
    let recorded = tokio::task::spawn_blocking(record)
        .await
        .unwrap_or_else(|error| Err(io::Error::other(error)));
    if let Err(error) = recorded {
        fence.fence(revoke_after_failed_record).await;
        return CommitOutcome::RecordFailed(error);
    }
    let generation = fence.0.lock().await;
    if *generation != started_under {
        return CommitOutcome::Fenced;
    }
    let installed = install.await;
    drop(generation);
    match installed {
        Ok(report) => CommitOutcome::Installed {
            report,
            collected: tokio::task::spawn_blocking(collect)
                .await
                .unwrap_or_else(|error| Err(io::Error::other(error))),
        },
        Err(error) => CommitOutcome::InstallFailed(error),
    }
}

/// Another durable operation is still outstanding.
#[derive(Debug)]
pub struct OperationOutstanding;
impl fmt::Display for OperationOutstanding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a durable publication operation is still outstanding")
    }
}
impl std::error::Error for OperationOutstanding {}

/// At most one runtime-owned durable operation. A cancelled waiter leaves the
/// task here; nothing new starts until [`Self::settle`] has awaited it.
pub struct OwnedOperationSlot<T> {
    outstanding: Option<JoinHandle<T>>,
}
impl<T> Default for OwnedOperationSlot<T> {
    fn default() -> Self {
        Self { outstanding: None }
    }
}
impl<T: Send + 'static> OwnedOperationSlot<T> {
    pub fn new() -> Self {
        Self { outstanding: None }
    }
    /// Await the outstanding operation, if any, and return how it ended.
    pub async fn settle(&mut self) -> Option<Result<T, tokio::task::JoinError>> {
        let handle = self.outstanding.as_mut()?;
        let outcome = handle.await;
        self.outstanding = None;
        Some(outcome)
    }
    /// Spawn `operation` and wait for it. Cancelling this future leaves the
    /// operation running and outstanding.
    pub async fn run(
        &mut self,
        operation: impl Future<Output = T> + Send + 'static,
    ) -> Result<Result<T, tokio::task::JoinError>, OperationOutstanding> {
        if self.outstanding.is_some() {
            return Err(OperationOutstanding);
        }
        let handle = self.outstanding.insert(tokio::spawn(operation));
        let outcome = handle.await;
        self.outstanding = None;
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc, Arc,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_withdrawal_during_a_blocked_record_prevents_installing_the_old_view() {
        let fence = CommitFence::default();
        let installed = Arc::new(AtomicBool::new(false));
        let (release, blocked) = mpsc::channel();
        let mut slot = OwnedOperationSlot::new();
        let started_under = fence.generation().await;
        let commit = run_commit(
            fence.clone(),
            started_under,
            blocked_commit(blocked, installed.clone()),
            || Ok(()),
        );
        // The pass waiting for the commit is cancelled while the record blocks.
        assert!(
            tokio::time::timeout(Duration::from_millis(50), slot.run(commit))
                .await
                .is_err()
        );
        // A later pass observes a withdrawal and fences serving.
        let revoked = Arc::new(AtomicBool::new(false));
        let observed = revoked.clone();
        fence
            .fence(async move { observed.store(true, Ordering::SeqCst) })
            .await;
        release.send(()).unwrap();
        let outcome = slot.settle().await.unwrap().unwrap();
        assert!(matches!(outcome, CommitOutcome::Fenced));
        assert!(revoked.load(Ordering::SeqCst));
        assert!(
            !installed.load(Ordering::SeqCst),
            "withdrawn view resurrected"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_record_revokes_under_the_fence_and_installs_nothing() {
        let fence = CommitFence::default();
        let installed = Arc::new(AtomicBool::new(false));
        let revoked = Arc::new(AtomicBool::new(false));
        let (install_flag, revoke_flag) = (installed.clone(), revoked.clone());
        let prepared = PreparedCommit::<()> {
            record: Box::new(|| Err(io::Error::other("disk full"))),
            install: Box::pin(async move {
                install_flag.store(true, Ordering::SeqCst);
                Ok(())
            }),
            revoke_after_failed_record: Box::pin(async move {
                revoke_flag.store(true, Ordering::SeqCst)
            }),
        };
        let outcome = run_commit(fence.clone(), 0, prepared, || Ok(())).await;
        assert!(matches!(outcome, CommitOutcome::RecordFailed(_)));
        assert!(revoked.load(Ordering::SeqCst));
        assert!(!installed.load(Ordering::SeqCst));
        assert_eq!(fence.generation().await, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_floor_write_is_settled_before_another_write_starts() {
        let active_writers = Arc::new(AtomicUsize::new(0));
        let overlapped = Arc::new(AtomicBool::new(false));
        let write = |release: Option<mpsc::Receiver<()>>| {
            let (active, overlapped) = (active_writers.clone(), overlapped.clone());
            async move {
                if active.fetch_add(1, Ordering::SeqCst) != 0 {
                    overlapped.store(true, Ordering::SeqCst);
                }
                if let Some(release) = release {
                    tokio::task::spawn_blocking(move || release.recv().unwrap())
                        .await
                        .unwrap();
                }
                active.fetch_sub(1, Ordering::SeqCst);
            }
        };
        let mut slot = OwnedOperationSlot::new();
        let (release, blocked) = mpsc::channel();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), slot.run(write(Some(blocked))))
                .await
                .is_err()
        );
        assert!(
            slot.run(write(None)).await.is_err(),
            "a second writer started"
        );
        release.send(()).unwrap();
        assert!(slot.settle().await.unwrap().is_ok());
        slot.run(write(None)).await.unwrap().unwrap();
        assert!(!overlapped.load(Ordering::SeqCst));
    }
}
