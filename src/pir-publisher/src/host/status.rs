//! Shared observations of product lifecycle, independent of HTTP and task IDs.
use crate::publisher::{PublisherProduct, ReadinessProbe};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, Copy)]
pub(super) enum ProductPhase {
    /// The task is scheduled; readiness still depends on its probes.
    Running,
    /// A long-running product returned, including a successful return.
    Failed,
    /// The owner completed during cooperative host shutdown.
    Stopped,
}
impl ProductPhase {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
        }
    }
    pub(super) fn running(self) -> bool {
        matches!(self, Self::Running)
    }
}

#[derive(Clone)]
pub(super) struct ProductStatus {
    pub(super) phase: ProductPhase,
    pub(super) probes: Vec<ReadinessProbe>,
}

#[derive(Clone, Default)]
pub(super) struct PublisherStatus(Arc<Mutex<BTreeMap<PublisherProduct, ProductStatus>>>);

impl PublisherStatus {
    pub(super) fn register(&self, product: PublisherProduct, probes: Vec<ReadinessProbe>) {
        self.0.lock().unwrap().insert(
            product,
            ProductStatus {
                phase: ProductPhase::Running,
                probes,
            },
        );
    }
    pub(super) fn set_phase(&self, product: PublisherProduct, phase: ProductPhase) {
        self.0
            .lock()
            .unwrap()
            .get_mut(&product)
            .expect("registered publisher owner")
            .phase = phase;
    }
    /// Clone observations so management never holds the registry lock across I/O.
    pub(super) fn snapshot(&self) -> BTreeMap<PublisherProduct, ProductStatus> {
        self.0.lock().unwrap().clone()
    }
}
