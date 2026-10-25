//! Product-independent publisher lifecycle and registration contracts.
//! Products keep storage, canonicality and publication authority. The application
//! supplies HTTP serving; only registration erases concrete workflow types.
mod operations;
mod registration;
mod runtime;
mod workflow;
pub use operations::{Publication, PublicationInput, Publisher, PublisherExecutor, PublisherRun};
pub use registration::{
    PublisherDefinition, PublisherResources, ReadinessProbe, ReadinessRule, ServiceContext,
};
pub use runtime::dedicated_runtime;
pub use workflow::Control;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests;
