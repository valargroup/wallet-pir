//! Shared publication and consumption execution, independent of product formats and authority.
/// Products distinguish unchanged input from backpressure without errors or strings.
#[derive(Debug, PartialEq, Eq)]
pub enum Input<W> {
    Work(W),
    Unchanged,
    Deferred,
}

/// Product error policy, selected explicitly after a complete pass. Retrying a
/// pass must reconcile through the owner before any uncertain publication repeats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailurePolicy {
    NextPoll,
    Stop,
}

/// Managed application shutdown and injected HTTP serving.
pub mod service;

/// Generic publisher service, typed phase recipes and host registration.
pub mod publisher;

/// Root-scheduled journal ingestion contracts, the chain follower and schedulers.
pub mod ingestion;

/// The one publication and ingestion executor injected into every product.
pub mod engine;

/// Shared publication consumption and sequential polling.
pub mod consumer;
