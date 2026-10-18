//! Verified Transparent PIR publications and prepared server runtimes.
//!
//! Publishers and query workers share artifact validation, preparation and
//! bounded caches here, as do the query workers' admission, request answering,
//! sizing and control socket. HTTP routes stay in each worker; chain ingestion
//! and publication lifecycle stay in the publisher.

pub mod admission;
pub mod assignment;
pub mod config;
pub mod control;
mod filecache;
pub mod memory;
pub mod metrics;
pub mod prebuild;
pub mod preparation;
pub mod prewarm;
pub mod procmem;
pub mod request;
pub mod runtime;
pub mod shardset;

pub mod http;

pub mod display;
pub mod distribution;
