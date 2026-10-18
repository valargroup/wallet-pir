//! Status PIR serving: generations, the distributed controller and workers.
//!
//! This library serves prepared generations. Chain observation (`source`),
//! publication (`publisher`) and the CLI live in `enhance-pir-server`; this
//! library must not depend on chain readers or that application.
/// Status admission queues; the application's publisher uses the coordinator queue.
pub mod admission;
pub mod artifact;
pub mod authority;
mod controller;
pub mod distributed;
pub mod fixture;
mod fixture_serving;
mod generation;
/// HTTP handlers shared by the serving roles and the application's publisher.
pub mod http;
pub mod index;
pub mod telemetry;
pub use controller::Controller;
pub use generation::{Generation, Preparation};
pub use http::shared_bytes;
// Most of these imports serve `distributed` and `publisher`, which reach them
// through `use super::*`.
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use http::{bounded, evaluate, query, serving_http_client, HttpState};
use ipir_sp::{server::MatvecBackend, ProductionSimplePirParams};
use sha2::{Digest, Sha256};
use status_protocol::*;
use std::{
    sync::atomic::Ordering,
    sync::{Arc, OnceLock},
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
        let p = pir_runtime::params::p16q48(ROWS as u64, ITEM_BITS).expect("fixed status profile");
        assert_eq!(p.ypir().db_cols, 6144);
        assert_eq!(p.ypir().db_cols * 2, ROW_BYTES);
        assert_eq!(ROWS * ROW_BYTES, 96 * 1024 * 1024);
        p
    })
}
