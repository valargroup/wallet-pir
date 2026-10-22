//! Status PIR serving core: generations, the authority that decides which one a
//! request may use, the private role protocol and aggregate telemetry.
//!
//! The [`service`] module owns Status listeners, admission and outbound HTTP.
//! `pir-query-server` reserves listeners and supervises the product task; chain
//! observation and publication remain in `status-publisher`, which this crate
//! must never depend on.
mod backend;
mod controller;
pub mod distributed;
pub mod fixture;
mod generation;
pub mod index;
pub mod protocol;
pub mod publication;
pub mod service;
pub mod telemetry;
use crate::protocol::*;
pub use controller::Controller;
pub use generation::{Generation, Preparation};
use ipir_sp::ProductionSimplePirParams;
use std::{
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
pub fn profile() -> &'static ProductionSimplePirParams {
    static P: OnceLock<ProductionSimplePirParams> = OnceLock::new();
    P.get_or_init(|| {
        let p = pir_runtime::params::p16q48(ROWS as u64, ITEM_BITS).expect("fixed status profile");
        assert_eq!(p.ypir().db_cols, 6144);
        assert_eq!(p.ypir().db_cols * 2, ROW_BYTES);
        assert_eq!(ROWS * ROW_BYTES, 96 * 1024 * 1024);
        p
    })
}
