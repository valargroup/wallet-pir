//! Fencing, floor persistence, commit and retention: every step that changes
//! durable state or what may serve.
use super::{
    durable_operation::{run_commit, CommitOutcome, DurableOutcome, OwnedCommit},
    PeerHandover, PublicationAdapter, PublicationLifecycle, PublicationLifecycleError,
};
use crate::Error;
use pir_publication::ViewIdentity;
use std::{io, time::SystemTime};

/// A commit that installed and has not finished completing: the adapter
/// still needs the view, or the dependent peer still needs its release.
/// It stays pending across cancelled passes until both happened.
pub(super) struct PendingCompletion<R, V> {
    report: R,
    view: Option<V>,
    collection_failure: Option<io::Error>,
}

impl<A: PublicationAdapter> PublicationLifecycle<A> {
    /// Revoke serving under the commit fence, so no commit that started
    /// earlier can install afterwards. Returns the views that were serving.
    pub(super) async fn fence_serving(&mut self) -> Vec<ViewIdentity> {
        self.commit_fence.fence(self.adapter.fence_serving()).await
    }

    /// Await a durable operation a cancelled pass left behind and apply its
    /// outcome, then finish a pending completion, before this pass observes
    /// or writes anything. A commit that installed is completed here as its
    /// own pass would have: the adapter receives the view and the peer is
    /// released; its collection already ran inside the owned operation. A
    /// peer release still owed is retried once; a pending answer leaves it
    /// owed and does not hold the pass back.
    pub(super) async fn settle_durable_operation(&mut self) -> Result<(), Error> {
        match self.durable_operation.settle().await {
            None => {}
            Some(Ok(DurableOutcome::Commit(commit))) => match self.begin_completion(commit) {
                Ok(()) => {}
                // The pass that raised the fence already acted on it.
                Err(error)
                    if error.downcast_ref::<PublicationLifecycleError>()
                        == Some(&PublicationLifecycleError::CommitFenced) => {}
                Err(error) => return Err(error),
            },
            Some(Ok(DurableOutcome::FloorWritten(Ok(written)))) => {
                self.floor.confirm_durable(written);
            }
            // The floor stays dirty; nothing may serve until a later write.
            Some(Ok(DurableOutcome::FloorWritten(Err(_)))) | Some(Err(_)) => {
                self.close_after_failed_floor_write().await;
            }
        }
        if self.pending_completion.is_some() {
            self.finish_completion().await?;
        } else if let Some(report) = self.owed_peer_release.clone() {
            // Cleared only once released, so a cancelled retry stays owed.
            if self.adapter.release_peer(&report).await == PeerHandover::Complete {
                self.owed_peer_release = None;
            }
        }
        Ok(())
    }

    /// Make the latest floor durable as an owned operation. On failure the
    /// floor stays dirty, serving is fenced and admission is closed.
    pub(super) async fn persist_floor(&mut self) -> Result<(), Error> {
        let write = self.floor.pending_write()?;
        let outcome = self
            .durable_operation
            .run(async move {
                DurableOutcome::FloorWritten(
                    tokio::task::spawn_blocking(move || write.write())
                        .await
                        .unwrap_or_else(|error| Err(io::Error::other(error))),
                )
            })
            .await?;
        match outcome {
            Ok(DurableOutcome::FloorWritten(Ok(written))) => {
                self.floor.confirm_durable(written);
                Ok(())
            }
            Ok(DurableOutcome::FloorWritten(Err(error))) => {
                self.close_after_failed_floor_write().await;
                Err(error.into())
            }
            Ok(DurableOutcome::Commit(_)) => unreachable!("a floor write produced a commit"),
            Err(error) => {
                self.close_after_failed_floor_write().await;
                Err(error.into())
            }
        }
    }

    async fn close_after_failed_floor_write(&mut self) {
        self.fence_serving().await;
        self.adapter.close_admission().await;
    }

    /// Commit `loaded` as `identity`: refuse an invalidated view or a dirty
    /// floor, then record, install and collect around it as one owned
    /// operation, and complete it: the adapter receives the view and the
    /// dependent peer is released.
    pub(super) async fn commit(
        &mut self,
        identity: ViewIdentity,
        loaded: A::Loaded,
    ) -> Result<(A::Report, PeerHandover), Error> {
        if self.floor.is_invalidated(&identity) {
            return Err(Box::new(PublicationLifecycleError::CandidateInvalidated));
        }
        if !self.floor.is_durable() {
            return Err(Box::new(PublicationLifecycleError::FloorNotDurable));
        }
        let objects = self.adapter.view_objects(&loaded);
        let required = self.adapter.retained_views().await;
        let (prepared, view) = self.adapter.prepare_commit(loaded)?;
        let retained_views =
            self.retained_views
                .after_commit(identity, objects, &required, SystemTime::now());
        let keep = retained_views.collectable_beyond();
        let cache = self.cache.clone();
        let started_under = self.commit_fence.generation().await;
        let fence = self.commit_fence.clone();
        let outcome = self
            .durable_operation
            .run(async move {
                DurableOutcome::Commit(OwnedCommit {
                    outcome: run_commit(fence, started_under, prepared, move || match keep {
                        Some(keep) => cache.collect_unreferenced(&keep),
                        None => Ok(()),
                    })
                    .await,
                    view,
                    retained_views,
                })
            })
            .await??;
        match outcome {
            DurableOutcome::Commit(commit) => self.begin_completion(commit)?,
            DurableOutcome::FloorWritten(_) => unreachable!("a commit produced a floor write"),
        }
        self.finish_completion().await
    }

    /// Record a finished commit: an installed one becomes the pending
    /// completion and its retained views take effect; otherwise report why
    /// nothing was installed.
    fn begin_completion(
        &mut self,
        commit: OwnedCommit<A::Report, A::InstalledView>,
    ) -> Result<(), Error> {
        match commit.outcome {
            CommitOutcome::Installed { report, collected } => {
                self.retained_views = commit.retained_views;
                self.pending_completion = Some(PendingCompletion {
                    report,
                    view: Some(commit.view),
                    collection_failure: collected.err(),
                });
                Ok(())
            }
            CommitOutcome::InstallFailed(error) => Err(error),
            CommitOutcome::Fenced => Err(Box::new(PublicationLifecycleError::CommitFenced)),
            CommitOutcome::RecordFailed(error) => Err(error.into()),
        }
    }

    /// Hand the pending view to the adapter once and release the dependent
    /// peer. A pass cancelled while releasing leaves the completion pending,
    /// and the next pass releases again; the release is idempotent. A pending
    /// answer leaves this commit's release owed, superseding an older one.
    async fn finish_completion(&mut self) -> Result<(A::Report, PeerHandover), Error> {
        let pending = self
            .pending_completion
            .as_mut()
            .expect("a completion is pending");
        if let Some(view) = pending.view.take() {
            self.adapter.installed(view);
        }
        let handover = self.adapter.release_peer(&pending.report).await;
        let completed = self
            .pending_completion
            .take()
            .expect("a completion is pending");
        self.owed_peer_release =
            (handover == PeerHandover::Pending).then(|| completed.report.clone());
        if let Some(error) = completed.collection_failure {
            return Err(error.into());
        }
        Ok((completed.report, handover))
    }
}
