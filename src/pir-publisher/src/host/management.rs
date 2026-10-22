//! Operational observations only: readiness and discovery grant no publication authority.
use super::status::PublisherStatus;
use crate::{
    publisher::{ReadinessProbe, ReadinessRule},
    Result,
};
use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};

const MAX_PROBE_BYTES: usize = 65536;

#[derive(Clone)]
struct Management {
    status: PublisherStatus,
    client: reqwest::Client,
}
impl Management {
    async fn snapshot(&self) -> (bool, Value) {
        let statuses = self.status.snapshot();
        let mut products = BTreeMap::new();
        let mut all_ready = true;
        for (product, status) in statuses {
            let mut ready = status.phase.running();
            if ready {
                for probe in &status.probes {
                    ready &= self.probe(probe).await;
                }
            }
            all_ready &= ready;
            products.insert(product, json!({"phase":status.phase.name(),"ready":ready}));
        }
        (all_ready, json!({"ready":all_ready,"products":products}))
    }
    async fn probe(&self, probe: &ReadinessProbe) -> bool {
        let response = self
            .client
            .get(format!("http://{}{}", probe.address, probe.path))
            .send()
            .await;
        let Ok(mut response) = response else {
            return false;
        };
        if !response.status().is_success() {
            return false;
        }
        if matches!(probe.rule, ReadinessRule::HttpSuccess) {
            return true;
        }
        let mut bytes = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) if bytes.len() + chunk.len() <= MAX_PROBE_BYTES => {
                    bytes.extend_from_slice(&chunk)
                }
                Ok(None) => break,
                _ => return false,
            }
        }
        match probe.rule {
            ReadinessRule::Body(check) => check(&bytes),
            ReadinessRule::HttpSuccess => unreachable!("HTTP status checked before reading body"),
        }
    }
}

pub(super) fn router(status: PublisherStatus) -> Result<Router> {
    let state = Management {
        status,
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
    };
    Ok(Router::new()
        .route(
            "/health",
            get(|State(state): State<Management>| async move { Json(state.snapshot().await.1) }),
        )
        .route(
            "/ready",
            get(|State(state): State<Management>| async move {
                let (ready, report) = state.snapshot().await;
                (
                    if ready {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    },
                    Json(report),
                )
            }),
        )
        .with_state(state)
        .merge(discovery()))
}

/// Publisher service discovery: the contract version query servers compare
/// before they synchronize. Informational only; never health or authority.
fn discovery() -> Router {
    Router::new().route(
        pir_service_discovery::PATH,
        get(|| async {
            (
                [
                    (axum::http::header::CONTENT_TYPE, "application/json"),
                    (axum::http::header::CACHE_CONTROL, "no-store"),
                ],
                pir_service_discovery::Document::publisher().to_json(),
            )
        }),
    )
}

#[cfg(test)]
#[path = "discovery_tests.rs"]
mod discovery_tests;
#[cfg(test)]
mod tests;
