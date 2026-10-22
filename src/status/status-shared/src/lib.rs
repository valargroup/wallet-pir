//! Status material and contracts shared by the Status publisher and query
//! server: the synchronous wire protocol and fixed profile, the source index,
//! generation preparation and loading, the typed publication contract with its
//! inventory validation, and aggregate preparation and serving telemetry.
//!
//! This package performs no I/O beyond reading prepared files it is handed.
//! HTTP, listeners, controllers, peer coordination and admission stay in
//! `status-query-server`; chain observation and publication stay in
//! `status-publisher`.
mod backend;
pub mod fixture;
mod generation;
pub mod index;
pub mod protocol;
pub mod publication;
pub mod telemetry;
use crate::protocol::*;
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
