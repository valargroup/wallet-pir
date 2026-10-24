//! Shared host contracts have no product vocabulary.
pub use pir_pipeline::engine::{PreparedPublication, PublisherEngine};
pub(crate) use pir_pipeline::publisher::{ReadinessProbe, ReadinessRule};
pub(crate) type PublisherProduct = &'static str;
