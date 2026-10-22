//! Shared host contracts have no product vocabulary.
pub(crate) use pir_pipeline::publisher::{ReadinessProbe, ReadinessRule};
pub(crate) type PublisherProduct = &'static str;
