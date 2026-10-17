//! Verified Transparent PIR publications and prepared server runtimes.
//!
//! Publishers and query workers share artifact validation, preparation and
//! bounded caches here. HTTP routes, admission and control stay in the worker;
//! chain ingestion and publication lifecycle stay in the publisher.

pub mod assignment;
pub mod display;
mod filecache;
pub mod memory;
pub mod metrics;
pub mod prebuild;
pub mod prewarm;
pub mod procmem;
pub mod runtime;
pub mod shardset;

pub mod http;

pub mod distribution;
