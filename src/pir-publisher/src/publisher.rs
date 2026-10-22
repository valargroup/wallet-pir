//! Shared host contracts have no product vocabulary.
mod engine;
pub use engine::{PreparedPublication, PublisherEngine};
pub(crate) use pir_pipeline::publisher::{ReadinessProbe, ReadinessRule};
pub(crate) type PublisherProduct = &'static str;
