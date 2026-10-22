//! Fencing, floor persistence, commit and retention: every step that changes
//! durable state or what may serve.
use super::{
    durable_operation::{run_commit, CommitOutcome, DurableOutcome},
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
    /// outcome, before this pass observes or writes anything.
    pub(super) async fn settle_durable_operation(&mut self) {
        match self.durable_operation.settle().await {
            None | Some(Ok(DurableOutcome::Commit(_))) => {}
            Some(Ok(DurableOutcome::FloorWritten(Ok(written)))) => {
                self.floor.confirm_durable(written);
            }
            // The floor stays dirty; nothing may serve until a later write.
            Some(Ok(DurableOutcome::FloorWritten(Err(_)))) | Some(Err(_)) => {
                self.close_after_failed_floor_write().await;
            }
        }
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
        let (prepared, installed_view) = self.adapter.prepare_commit(loaded)?;
        let started_under = self.commit_fence.generation().await;
        let fence = self.commit_fence.clone();
        let outcome =
            self.durable_operation
                .run(async move {
                    DurableOutcome::Commit(run_commit(fence, started_under, prepared).await)
                })
                .await??;
        let report = match outcome {
            DurableOutcome::Commit(CommitOutcome::Installed(report)) => report?,
            DurableOutcome::Commit(CommitOutcome::Fenced) => {
                return Err(Box::new(PublicationLifecycleError::CommitFenced))
            }
            DurableOutcome::Commit(CommitOutcome::RecordFailed(error)) => return Err(error.into()),
            DurableOutcome::FloorWritten(_) => unreachable!("a commit produced a floor write"),
        };
        self.adapter.installed(installed_view);
        self.retain_commit(retained).await?;
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
