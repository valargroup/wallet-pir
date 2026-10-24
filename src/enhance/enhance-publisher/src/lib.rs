//! Enhance publication: canonical ingestion, durable recovery floors and local
//! preparation. Workers, packing routers and ingress synchronize independently.

// Shared Enhance modules the publication code builds on, named at the crate
// root so publisher modules refer to them alongside their own.
use enhance_shared::{artifact, prepared_packing, runtime, types, wire, PackingBudget};

mod durable_state;

pub mod application;
pub mod chain_source;
pub mod distribution;
pub mod handoff;
pub mod ingest;
pub mod journal;
pub mod publication;
pub mod publisher;
pub mod rebuild;

pub mod service;

pub mod commands;

// Exercise the host's actual executor without a product-to-application Cargo edge.
#[cfg(test)]
mod host_engine {
    #[allow(unused_imports)]
    pub(crate) use pir_pipeline::engine::{PreparedPublication, PublisherEngine};
}
