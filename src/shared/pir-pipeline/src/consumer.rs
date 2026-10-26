//! A transport-neutral publication consumer. Products supply decisions and
//! typed material operations; this engine owns their execution order.
pub mod activation;
pub mod durable;
mod engine;
pub mod polling;
pub mod reservations;

use crate::Input;
pub use engine::ConsumerEngine;
use std::{future::Future, time::Duration};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Normal coordination, never a transport failure or renewed authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeferralReason {
    AuthorityAdvanced,
    PeerAdvanced,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerReservation {
    Reserved,
    PeerAdvanced,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerHandover {
    Complete,
    Pending,
}

/// One fully joined consumer pass, retaining the product's reporting type.
#[derive(Debug)]
pub enum ConsumerPass<R> {
    Committed { report: R, handover: PeerHandover },
    Deferred { report: R },
}

impl<R> ConsumerPass<R> {
    pub fn into_report(self) -> R {
        match self {
            Self::Committed { report, .. } | Self::Deferred { report } => report,
        }
    }
}

/// Services composed by one product consumer. Observations, material and
/// authority remain opaque to the engine; none can be constructed from a path
/// or inferred from a successful download. Hooks perform one phase only.
pub trait Consumer: Send {
    type Observation: Send + Sync;
    type Candidate: Send + Sync;
    type Loaded: Send + Sync;
    type Artifacts: Send;
    type Validation: Send + Sync;
    type Authority: Send;
    type Report: Send;

    /// Settle writes and acknowledgments left outstanding by a cancelled pass.
    fn settle(&mut self) -> impl Future<Output = Result<(), Error>> + Send;
    /// Obtain source evidence, applying revocation before persisting its floor.
    fn observe(&mut self) -> impl Future<Output = Result<Self::Observation, Error>> + Send;
    fn eligible(&self, observation: &Self::Observation) -> bool;
    fn keep_loaded(&self, loaded: &Self::Loaded) -> bool;
    /// Negative evidence may invalidate preparation even when a recovered
    /// publication is already eligible. Discard before rebinding that material.
    fn invalidates_preparation(&self, _observation: &Self::Observation) -> bool {
        false
    }
    fn check_serving(
        &mut self,
        observation: &Self::Observation,
    ) -> impl Future<Output = Result<(), Error>> + Send;
    fn select(
        &mut self,
        observation: &Self::Observation,
    ) -> impl Future<Output = Result<Input<Self::Candidate>, Error>> + Send;
    /// Reserve incoming material before any bulk transfer. No-peer roles
    /// return Reserved without acquiring a promise.
    fn reserve(
        &mut self,
        candidate: &mut Self::Candidate,
    ) -> impl Future<Output = Result<PeerReservation, Error>> + Send;
    fn obtain(
        &mut self,
        candidate: &Self::Candidate,
    ) -> impl Future<Output = Result<Self::Artifacts, Error>> + Send;
    fn load(
        &mut self,
        candidate: Self::Candidate,
        artifacts: Self::Artifacts,
    ) -> impl Future<Output = Result<Self::Loaded, Error>> + Send;
    fn rebind(
        &mut self,
        observation: &Self::Observation,
        loaded: Self::Loaded,
    ) -> impl Future<Output = Result<Self::Loaded, Error>> + Send;
    fn validate(
        &self,
        observation: &Self::Observation,
        loaded: &Self::Loaded,
    ) -> Result<Self::Validation, Error>;
    fn authorize(
        &mut self,
        observation: &Self::Observation,
        validation: &Self::Validation,
    ) -> impl Future<Output = Result<Self::Authority, Error>> + Send;
    fn confirm(
        &mut self,
        loaded: &mut Self::Loaded,
    ) -> impl Future<Output = Result<PeerReservation, Error>> + Send;
    /// Record, install under the fence, complete peer acknowledgment and collect.
    /// Use the shared owned-operation primitives for work that outlives a waiter.
    fn commit(
        &mut self,
        loaded: Self::Loaded,
        validation: Self::Validation,
        authority: Self::Authority,
    ) -> impl Future<Output = Result<(Self::Report, PeerHandover), Error>> + Send;
    fn serving_report(&mut self) -> impl Future<Output = Self::Report> + Send;
    fn deferral(&self, _error: &Error) -> Option<DeferralReason> {
        None
    }
    fn deferred(&mut self, _reason: DeferralReason) -> impl Future<Output = ()> + Send {
        async {}
    }
    /// Continue a product's bounded recovery handoff within this poll. Every
    /// continuation runs a complete observed and guarded pass.
    fn continue_after(&mut self, _result: &Result<ConsumerPass<Self::Report>, Error>) -> bool {
        false
    }
    fn terminal(&self, _error: &Error) -> bool {
        false
    }
    /// See [`polling::SyncTask::transient`].
    fn transient(&self, _error: &Error) -> bool {
        false
    }
    fn phase_finished(&mut self, _phase: &'static str, _elapsed: Duration) {}
    fn pass_started(&mut self) -> impl Future<Output = ()> + Send {
        async {}
    }
    fn pass_finished(
        &mut self,
        _result: &Result<ConsumerPass<Self::Report>, Error>,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }
}

#[cfg(test)]
mod tests;
