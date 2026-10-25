//! Domain operations supplied to the host's publication engine.
use super::Control;
use crate::FailurePolicy;
use std::{future::Future, pin::Pin};

/// One source-bound publication. Each phase consumes the preceding phase's
/// result; only the host sequences them. Product code supplies construction,
/// semantic checks and guarded state operations, never a complete service loop.
pub trait Publication: Send {
    type Error: Send;
    type Material: Send;
    type Stored: Send;
    type Verified: Send;
    type Candidate: Send;
    type Report: Send;

    /// Refuse stopped or locally invalidated work. The engine calls this after
    /// every awaited phase and immediately before entering the guarded commit.
    fn check(&self) -> Result<(), Self::Error>;

    /// Construct material from the captured source, using bounded work units.
    fn build(&mut self) -> impl Future<Output = Result<Self::Material, Self::Error>> + Send;
    /// Establish durable immutable storage without authorizing distribution.
    fn store(
        &mut self,
        material: Self::Material,
    ) -> impl Future<Output = Result<Self::Stored, Self::Error>> + Send;
    /// Establish byte integrity and product preparation consistency, reusing
    /// evidence only for unchanged files under its original semantic context.
    fn verify(
        &mut self,
        stored: Self::Stored,
    ) -> impl Future<Output = Result<Self::Verified, Self::Error>> + Send;
    /// Persist a complete candidate descriptor without advancing current.
    fn assemble(
        &mut self,
        verified: Self::Verified,
    ) -> impl Future<Output = Result<Self::Candidate, Self::Error>> + Send;
    /// Recheck the exact source after preparation. The final commit must also
    /// compare its local withdrawal fence under the state store's commit guard.
    fn recheck(
        &mut self,
        candidate: &mut Self::Candidate,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    /// Retain required objects before atomically advancing durable authority.
    /// Recheck the local fence and shutdown under the commit guard, and after
    /// any awaited operation inside a multi-record activation transaction.
    fn commit(
        self,
        candidate: Self::Candidate,
    ) -> impl Future<Output = Result<Self::Report, Self::Error>> + Send;
}

/// Product-selected input, independent of source polling and publication order.
/// A refresh retains material identity but carries newly checked metadata.
/// Products still choose equality, freshness deadlines and invalidation rules.
pub enum PublicationInput<P> {
    Unchanged,
    MetadataRefresh(P),
    NewMaterial(P),
    Deferred,
}

/// A publisher's input and policy. Work can borrow product state; constructing
/// it must not advance current or run preparation. Waiting is cancel-safe.
pub trait Publisher: Send {
    type Error: Send;
    type Report: Send;
    type Publication<'a>: Publication<Error = Self::Error, Report = Self::Report>
    where
        Self: 'a;
    /// Reconcile or start product background preparation through the root
    /// executor before acquiring foreground work. Background work must own its
    /// immutable inputs and be joined by `finish`; acquisition itself never
    /// starts preparation. Products without background work use this default.
    fn background<E: PublisherExecutor>(
        &mut self,
        _executor: E,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }
    fn acquire(
        &mut self,
    ) -> impl Future<Output = Result<PublicationInput<Self::Publication<'_>>, Self::Error>> + Send;
    fn completed(&mut self, _report: Self::Report) {}
    fn failed(&mut self, _error: &Self::Error) -> FailurePolicy {
        FailurePolicy::Stop
    }
    fn settled(&mut self) -> Result<Control, Self::Error> {
        Ok(Control::Continue)
    }
    fn wait(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn finish(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }
}

/// One service future; boxing this boundary keeps borrowed product operations
/// usable inside spawned services without erasing their work or candidate types.
pub type PublisherRun<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// The root injects execution into product construction without introducing a
/// dependency from a product library back to the application. Implementations
/// own phase ordering, service scheduling and cooperative completion.
pub trait PublisherExecutor: Clone + Send + Sync + 'static {
    fn publish<P: Publication>(
        &self,
        publication: P,
    ) -> impl Future<Output = Result<P::Report, P::Error>> + Send;
    /// Execute one product-selected input with the same order used by service polling.
    async fn publish_input<P: Publication>(
        &self,
        input: PublicationInput<P>,
    ) -> Result<Option<P::Report>, P::Error> {
        match input {
            PublicationInput::NewMaterial(publication)
            | PublicationInput::MetadataRefresh(publication) => {
                self.publish(publication).await.map(Some)
            }
            PublicationInput::Unchanged | PublicationInput::Deferred => Ok(None),
        }
    }
    fn run<'a, P: Publisher + 'a>(
        &'a self,
        publisher: P,
        shutdown: crate::service::Shutdown,
    ) -> PublisherRun<'a, P::Error>;
}
