//! Product-independent publisher lifecycle and registration contracts.
//! Products keep storage, canonicality and publication authority. The application
//! supplies HTTP serving; only registration erases concrete workflow types.
mod pass;
mod registration;
mod runtime;
pub(crate) mod workflow;
pub use pass::PublicationPass;
pub use registration::{
    PublisherDefinition, PublisherResources, ReadinessProbe, ReadinessRule, ServiceContext,
};
pub use runtime::dedicated_runtime;
pub use workflow::{Control, PublisherService, Workflow};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests;
