//! Bounded authority metadata and immutable retained-object streams.
use super::PublicationDistribution;
use axum::{
    body::Body,
    extract::{Path as HttpPath, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use enhance_query_server::{
    artifact,
    protocol::canonical_hash,
    publication::{AuthorityRequest, AuthorityResponse, Object},
};
use std::path::Path;

type ApiResult<T> = Result<T, (StatusCode, String)>;
fn refused(error: impl ToString) -> (StatusCode, String) {
    (StatusCode::SERVICE_UNAVAILABLE, error.to_string())
}

pub(super) async fn current(
    State(distribution): State<PublicationDistribution>,
) -> ApiResult<Response> {
    distribution
        .authority(None)
        .await
        .map(metadata)
        .map_err(refused)
}

pub(super) async fn authority(
    State(distribution): State<PublicationDistribution>,
    Json(request): Json<AuthorityRequest>,
) -> ApiResult<Response> {
    distribution
        .authority(Some(&request.manifest_sha256))
        .await
        .map(metadata)
        .map_err(refused)
}

fn metadata(value: AuthorityResponse) -> Response {
    ([(header::CACHE_CONTROL, "no-store")], Json(value)).into_response()
}

pub(super) async fn inventory(
    State(distribution): State<PublicationDistribution>,
    HttpPath(id): HttpPath<String>,
) -> ApiResult<Response> {
    if !canonical_hash(&id) {
        return Err((StatusCode::BAD_REQUEST, "invalid inventory digest".into()));
    }
    let descriptor = distribution
        .disk
        .lock()
        .unwrap()
        .retained
        .iter()
        .find(|r| r.inventory.checksum == id)
        .map(|r| r.inventory.clone())
        .ok_or((
            StatusCode::NOT_FOUND,
            "inventory is no longer retained".into(),
        ))?;
    stream(
        &distribution
            .root
            .join(pir_artifact::namespace("inventories"))
            .join(format!("{id}.json")),
        &descriptor,
    )
}

pub(super) async fn object(
    State(distribution): State<PublicationDistribution>,
    HttpPath(id): HttpPath<String>,
) -> ApiResult<Response> {
    if !canonical_hash(&id) {
        return Err((StatusCode::BAD_REQUEST, "invalid object digest".into()));
    }
    let distribution_for_lookup = distribution.clone();
    let objects = tokio::task::spawn_blocking(move || distribution_for_lookup.retained_objects())
        .await
        .map_err(refused)?
        .map_err(refused)?;
    let descriptor = objects.get(&id).ok_or((
        StatusCode::NOT_FOUND,
        "object is not referenced by a retained inventory".into(),
    ))?;
    stream(
        &descriptor.path(&distribution.root).map_err(refused)?,
        descriptor,
    )
}

fn stream(path: &Path, descriptor: &Object) -> ApiResult<Response> {
    let artifact =
        artifact::PublicationArtifact::open(path, descriptor.bytes, descriptor.checksum.clone())
            .map_err(refused)?;
    Ok((
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        Body::from_stream(artifact.stream()),
    )
        .into_response())
}
