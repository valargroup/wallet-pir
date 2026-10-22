//! Task ownership and stop policy. Never restart an owner over unfinished work.
use super::status::{ProductPhase, PublisherStatus};
use crate::{
    publisher::{PublisherProduct, ReadinessProbe},
    Result,
};
use pir_pipeline::service::Shutdown;
use std::{collections::HashMap, future::Future};
use tokio::task::{Id, JoinHandle, JoinSet};

struct ProductOwner {
    product: PublisherProduct,
    shutdown: Shutdown,
}

pub(super) struct ProductSupervisor {
    tasks: JoinSet<Result<()>>,
    owners: HashMap<Id, ProductOwner>,
    status: PublisherStatus,
}
impl ProductSupervisor {
    pub(super) fn new(status: PublisherStatus) -> Self {
        Self {
            tasks: JoinSet::new(),
            owners: HashMap::new(),
            status,
        }
    }

    pub(super) fn start<F>(
        &mut self,
        product: PublisherProduct,
        probes: Vec<ReadinessProbe>,
        future: F,
        shutdown: Shutdown,
    ) where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        self.status.register(product, probes);
        let task_id = self.tasks.spawn(future).id();
        self.owners
            .insert(task_id, ProductOwner { product, shutdown });
    }

    /// A normal product exit affects only that owner. A panic may leave blocking
    /// work unfinished, so it stops the host rather than reopening product state.
    pub(super) async fn supervise(
        &mut self,
        management: &mut JoinHandle<std::io::Result<()>>,
        shutdown: &Shutdown,
    ) -> Result<()> {
        let mut fatal = None;
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = &mut *management => { fatal = Some("management HTTP service stopped"); break; },
                result = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                    match result.expect("nonempty publisher task group") {
                        Ok((task_id, result)) => {
                            let owner = self.owners.remove(&task_id).expect("registered publisher task");
                            owner.shutdown.request();
                            self.status.set_phase(owner.product, ProductPhase::Failed);
                            if let Err(error) = result {
                                tracing::error!(product=owner.product, %error, "publisher stopped; other products continue");
                            }
                        }
                        Err(_) => {
                            fatal = Some("publisher task panicked; stopping all owners safely");
                            break;
                        }
                    }
                }
            }
        }
        self.stop_and_join().await;
        shutdown.request();
        match fatal {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }

    /// Request every stop before waiting for any owner. Product futures finish
    /// their operations cooperatively; they are never aborted here.
    pub(super) async fn stop_and_join(&mut self) {
        for owner in self.owners.values() {
            owner.shutdown.request();
        }
        while let Some(result) = self.tasks.join_next_with_id().await {
            if let Ok((task_id, _)) = result {
                let owner = self
                    .owners
                    .remove(&task_id)
                    .expect("registered publisher task");
                self.status.set_phase(owner.product, ProductPhase::Stopped);
            }
        }
        self.owners.clear();
    }
}

#[cfg(test)]
mod tests;
