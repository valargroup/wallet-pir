//! Local integration service for one immutable publication. No production deployment policy.
use axum::{
    body::to_bytes,
    extract::{Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use receiver_pir::{query_bytes, server::Server, Error};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(Clone)]
struct Service {
    server: Arc<Server>,
    slots: Arc<Semaphore>,
}

/// Limit uploads and CPU evaluation together. Cancellation never frees a still-running CPU slot.
pub fn router(server: Server) -> Router {
    let service = Service {
        server: Arc::new(server),
        slots: Arc::new(Semaphore::new(2)),
    };
    Router::new()
        .route(
            "/v1/receiver/init",
            get(|State(s): State<Service>| async move { Json(s.server.manifest().clone()) }),
        )
        .route(
            "/v1/receiver/public",
            get(|State(s): State<Service>| async move { binary(s.server.public().to_vec()) }),
        )
        .route("/v1/receiver/query", post(query))
        .with_state(service)
}

fn binary(body: Vec<u8>) -> Response {
    (
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
        .into_response()
}

async fn query(State(s): State<Service>, request: Request) -> Response {
    let Ok(permit) = s.slots.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let body = match tokio::time::timeout(
        Duration::from_secs(15),
        to_bytes(request.into_body(), query_bytes()),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Err(_) => return StatusCode::REQUEST_TIMEOUT.into_response(),
    };
    let result = tokio::task::spawn_blocking(move || {
        // The blocking task outlives a disconnected request, so it owns admission until it finishes.
        let _permit = permit;
        s.server.respond(&body)
    })
    .await;
    match result {
        Ok(Ok(body)) => binary(body),
        Ok(Err(Error::Revision)) => StatusCode::CONFLICT.into_response(),
        Ok(Err(Error::Malformed)) => StatusCode::BAD_REQUEST.into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
