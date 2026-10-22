//! Fencing, floor persistence, commit and retention: every step that changes
//! durable state or what may serve.
use super::{
    durable_operation::{run_commit, CommitOutcome, DurableOutcome, OwnedCommit},
    PublicationAdapter, PublicationLifecycle, PublicationLifecycleError,
};
use crate::Error;
use pir_publication::MaterialIdentity;
use std::{collections::BTreeSet, io};

impl<A: PublicationAdapter> PublicationLifecycle<A> {
    /// Revoke serving under the commit fence, so no commit that started
    /// earlier can install afterwards. Returns the views that were serving.
    pub(super) async fn fence_serving(&mut self) -> Vec<MaterialIdentity> {
        self.commit_fence.fence(self.adapter.fence_serving()).await
    }

    /// Await a durable operation a cancelled pass left behind and apply its
    /// outcome, before this pass observes or writes anything. A commit that
    /// installed is completed here as its own pass would have: the adapter
    /// receives the view, its objects are retained and the peer is released.
    pub(super) async fn settle_durable_operation(&mut self) -> Result<(), Error> {
        match self.durable_operation.settle().await {
            None => {}
            Some(Ok(DurableOutcome::Commit(commit))) => match self.complete_commit(commit).await {
                Ok(report) => {
                    self.adapter.release_peer(&report).await;
                }
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
    /// floor, then record and install it as an owned operation, and retain its
    /// objects only after the install succeeded.
    pub(super) async fn commit(
        &mut self,
        identity: MaterialIdentity,
        loaded: A::Loaded,
    ) -> Result<A::Report, Error> {
        if self.floor.is_invalidated(&identity) {
            return Err(Box::new(PublicationLifecycleError::CandidateInvalidated));
        }
        if !self.floor.is_durable() {
            return Err(Box::new(PublicationLifecycleError::FloorNotDurable));
        }
        let retained = self.adapter.retained_objects(&loaded);
        let (prepared, view) = self.adapter.prepare_commit(loaded)?;
        let started_under = self.commit_fence.generation().await;
        let fence = self.commit_fence.clone();
        let outcome = self
            .durable_operation
            .run(async move {
                DurableOutcome::Commit(OwnedCommit {
                    outcome: run_commit(fence, started_under, prepared).await,
                    view,
                    retained,
                })
            })
            .await??;
        match outcome {
            DurableOutcome::Commit(commit) => self.complete_commit(commit).await,
            DurableOutcome::FloorWritten(_) => unreachable!("a commit produced a floor write"),
        }
    }

    /// Apply a finished commit once: hand an installed view to the adapter
    /// and retain its objects, or report why nothing was installed.
    async fn complete_commit(
        &mut self,
        commit: OwnedCommit<A::Report, A::InstalledView>,
    ) -> Result<A::Report, Error> {
        let report = match commit.outcome {
            CommitOutcome::Installed(report) => report?,
            CommitOutcome::Fenced => return Err(Box::new(PublicationLifecycleError::CommitFenced)),
            CommitOutcome::RecordFailed(error) => return Err(error.into()),
        };
        self.adapter.installed(commit.view);
        self.retain_commit(commit.retained).await?;
        Ok(report)
    }

    /// Keep the objects of the last two commits and collect everything else.
    async fn retain_commit(&mut self, objects: BTreeSet<String>) -> Result<(), Error> {
        self.retained_commits.retain(|old| *old != objects);
        self.retained_commits.push(objects);
        if self.retained_commits.len() > 2 {
            self.retained_commits.remove(0);
        }
        let keep: BTreeSet<String> = self.retained_commits.iter().flatten().cloned().collect();
        let cache = self.cache.clone();
        tokio::task::spawn_blocking(move || cache.collect_unreferenced(&keep)).await??;
        Ok(())
    }
}
