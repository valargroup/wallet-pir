//! Single-process serving for the synthetic fixture and the experimental live
//! loop. Production roles build their own routes in `distributed` and
//! `publisher`.
use super::{
    http::{evaluate, forward, init, query, session, HttpState},
    now_ms, telemetry, Controller,
};
use crate::protocol::*;
use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use std::sync::Arc;

impl Controller {
    /// Synthetic fixture only: reaffirm an unchanged synthetic source, without rebuilding hints.
    /// Requires an explicit oracle digest so a failed source poll cannot renew freshness.
    pub fn reaffirm_fixture(&self, expected: Hash) -> Result<(), Error> {
        let mut views = self.views.write().unwrap();
        if views.active.manifest().rows_digest != expected {
            return Err(Error::Malformed);
        }
        if now_ms() / 1000 == views.active.manifest().observed_ms / 1000 {
            return Ok(());
        }
        views
            .retained
            .retain(|old| old.manifest().fresh(now_ms()).is_ok());
        // A one-second observation cadence needs enough retained identities to
        // cover the full freshness window while admitted requests finish.
        if views.retained.len() >= (MAX_AGE_MS / 1000 + 2) as usize {
            return Err(Error::Unavailable);
        }
        let next = views.active.reobserved(now_ms());
        let old = views.active.clone();
        views.retained.push(old);
        views.active = Arc::new(next);
        Ok(())
    }
    pub fn coordinator_routes(&self, router_origin: String) -> Router {
        let state = HttpState::new(self.clone(), router_origin);
        Router::new()
            .route("/v1/status/init", get(init))
            .route("/v1/status/session/:id", get(session))
            .route("/v1/status/query", post(forward))
            .layer(DefaultBodyLimit::max(512 * 1024))
            .with_state(state)
            .layer(axum::middleware::from_fn(|request, next| {
                telemetry::observe("coordinator", request, next)
            }))
    }
    pub fn router_routes(&self, worker_origin: String) -> Router {
        Router::new()
            .route("/v1/status/query", post(query))
            .layer(DefaultBodyLimit::max(512 * 1024))
            .with_state(HttpState::new(self.clone(), worker_origin))
            .layer(axum::middleware::from_fn(|request, next| {
                telemetry::observe("router", request, next)
            }))
    }
    pub fn worker_routes(&self) -> Router {
        Router::new()
            .route("/evaluate", post(evaluate))
            .layer(DefaultBodyLimit::max(HEADER_BYTES + ROWS * 8))
            .with_state(HttpState::new(self.clone(), String::new()))
            .layer(axum::middleware::from_fn(|request, next| {
                telemetry::observe("worker", request, next)
            }))
    }
}
