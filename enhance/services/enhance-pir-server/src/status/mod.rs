//! Experimental isolated HTTP service. Production controller recovery is not wired here.
mod admission;
pub mod artifact;
pub mod authority;
pub mod cli;
mod controller;
pub mod distributed;
pub mod fixture;
mod fixture_serving;
mod generation;
mod http;
pub mod index;
pub mod publisher;
pub mod source;
pub mod telemetry;
pub use controller::Controller;
pub use generation::{Generation, Preparation};
pub(crate) use http::shared_bytes;
// Most of these imports serve `distributed` and `publisher`, which reach them
// through `use super::*`.
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use enhance_pir::status::*;
use http::{bounded, evaluate, forward, init, query, serving_http_client, session, HttpState};
use ipir_sp::{server::MatvecBackend, ProductionSimplePirParams, SimplePirProfile};
use sha2::{Digest, Sha256};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, OnceLock, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
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
        let p = ProductionSimplePirParams::new(ROWS as u64, ITEM_BITS, SimplePirProfile::P16Q48)
            .expect("fixed status profile");
        assert_eq!(p.ypir().db_cols, 6144);
        assert_eq!(p.ypir().db_cols * 2, ROW_BYTES);
        assert_eq!(ROWS * ROW_BYTES, 96 * 1024 * 1024);
        p
    })
}
