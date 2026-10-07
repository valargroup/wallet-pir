//! Receiver PIR serving with immutable publications and atomic session revocation.
pub mod publication;
use axum::{
    body::to_bytes,
    extract::{Path, Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
pub use publication::{Publication, Publications};
use receiver_pir::{query_bytes, Error, HEADER_BYTES, MAGIC, MAX_ROWS};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(Clone)]
struct Service {
    publications: Publications,
    slots: Arc<Semaphore>,
}

/// Serve the current publication while the owner prepares, validates and rotates revisions.
/// Limit uploads and CPU evaluation together. Cancellation never frees a still-running CPU slot.
pub fn router_with_publications(publications: Publications) -> Router {
    let service = Service {
        publications,
        slots: Arc::new(Semaphore::new(2)),
    };
    Router::new()
        .route(
            "/v1/receiver/init",
            get(|State(s): State<Service>| async move {
                match s.publications.select(None) {
                    Ok((p, _)) => (
                        [(header::CACHE_CONTROL, "no-store")],
                        Json(p.server.manifest().clone()),
                    )
                        .into_response(),
                    Err(status) => status.into_response(),
                }
            }),
        )
        .route(
            "/v1/receiver/public/:session",
            get(
                |State(s): State<Service>, Path(id): Path<String>| async move {
                    material(&s, &id, Material::Public)
                },
            ),
        )
        .route("/v1/receiver/query", post(query))
        .route(
            "/v1/receiver/rows/:session",
            get(
                |State(s): State<Service>, Path(id): Path<String>| async move {
                    material(&s, &id, Material::Rows)
                },
            ),
        )
        .route(
            "/v1/receiver/witness/:session",
            get(
                |State(s): State<Service>, Path(id): Path<String>| async move {
                    material(&s, &id, Material::Witness)
                },
            ),
        )
        .route(
            "/v1/receiver/filters/:session",
            get(
                |State(s): State<Service>, Path(id): Path<String>| async move {
                    material(&s, &id, Material::Filters)
                },
            ),
        )
        .with_state(service)
}

enum Material {
    Public,
    Witness,
    Rows,
    Filters,
}

fn material(s: &Service, id: &str, material: Material) -> Response {
    let Some(id) = hex::decode(id).ok().and_then(|v| v.try_into().ok()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let p = match s.publications.select(Some(id)) {
        Ok((p, _)) => p,
        Err(status) => return status.into_response(),
    };
    match material {
        Material::Public => binary(p.server.public().to_vec()),
        Material::Rows => binary(axum::body::Bytes::from_owner(p.server.rows())),
        Material::Filters => binary(axum::body::Bytes::from_owner(p.server.filters())),
        Material::Witness => match &p.witnesses {
            Some(bytes) => binary(bytes.clone()),
            None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        },
    }
}

fn binary(body: impl IntoResponse) -> Response {
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
        to_bytes(
            request.into_body(),
            query_bytes(MAX_ROWS).expect("supported maximum"),
        ),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        Err(_) => return StatusCode::REQUEST_TIMEOUT.into_response(),
    };
    if body.len() < HEADER_BYTES || &body[..4] != MAGIC {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let (publication, epoch) = match s.publications.select(Some(body[4..36].try_into().unwrap())) {
        Ok(p) => p,
        Err(status) => return status.into_response(),
    };
    let result = tokio::task::spawn_blocking(move || {
        // The blocking task outlives a disconnected request, so it owns admission until it finishes.
        let _permit = permit;
        publication.server.respond(&body)
    })
    .await;
    if s.publications.epoch() != epoch {
        return StatusCode::GONE.into_response();
    }
    match result {
        Ok(Ok(body)) => binary(body),
        Ok(Err(Error::Revision)) => StatusCode::CONFLICT.into_response(),
        Ok(Err(Error::Malformed)) => StatusCode::BAD_REQUEST.into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
