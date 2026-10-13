//! Query handlers shared by the single-process fixture and the Status roles:
//! each one checks authority and freshness before and after admission.
use super::{admission, profile, telemetry, Controller};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use enhance_pir::status::*;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

/// Reuse TCP connections across lookups, including SSH forwarding channels.
/// Authority/freshness checks remain mandatory for every request on reused connections.
pub(super) fn serving_http_client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .timeout(admission::REQUEST_BUDGET)
        .connect_timeout(Duration::from_secs(1))
        .pool_max_idle_per_host(4)
        .pool_idle_timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

#[derive(Clone)]
pub(super) struct HttpState {
    pub(super) controller: Controller,
    pub(super) origin: String,
    pub(super) http: reqwest::Client,
    pub(super) permits: Arc<admission::Admission>,
}
impl HttpState {
    pub(super) fn new(controller: Controller, origin: String) -> Self {
        Self {
            controller,
            origin,
            http: serving_http_client().expect("Status HTTP client"),
            permits: Arc::new(admission::Admission::production()),
        }
    }
}
pub(super) async fn init(State(s): State<HttpState>) -> Result<Json<Manifest>, StatusCode> {
    let g = s.controller.current();
    s.controller.check(&g)?;
    Ok(Json(g.manifest.clone()))
}
/// Response body over shared immutable material, without copying it.
struct SharedBytes(Arc<Vec<u8>>);
impl AsRef<[u8]> for SharedBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}
pub(crate) fn shared_bytes(bytes: Arc<Vec<u8>>) -> Bytes {
    Bytes::from_owner(SharedBytes(bytes))
}
pub(super) async fn session(
    State(s): State<HttpState>,
    Path(id): Path<String>,
) -> Result<Bytes, StatusCode> {
    let id: Hash = hex::decode(id)
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .try_into()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let g = s.controller.resolve(id)?;
    s.controller.check(&g)?;
    Ok(shared_bytes(g.public.clone()))
}
pub(super) async fn bounded(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, StatusCode> {
    if !response.status().is_success() {
        return Err(
            StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY)
        );
    }
    let mut out = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?
    {
        if chunk.len() > limit.saturating_sub(out.len()) {
            return Err(StatusCode::BAD_GATEWAY);
        }
        out.extend(&chunk);
    }
    Ok(out)
}
pub(super) async fn forward(
    State(s): State<HttpState>,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    match tokio::time::timeout(admission::REQUEST_BUDGET, forward_admitted(s, body)).await {
        Ok(result) => result,
        Err(_) => {
            telemetry::admission_rejected("coordinator", "request_deadline");
            Err(StatusCode::GATEWAY_TIMEOUT)
        }
    }
}
async fn forward_admitted(s: HttpState, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    let g = s.controller.for_body(&body)?;
    s.controller.check(&g)?;
    let _permit = s.permits.acquire("coordinator").await?;
    s.controller.check(&g)?;
    let response = s
        .http
        .post(format!("{}/v1/status/query", s.origin))
        .body(body)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let bytes = bounded(response, 512 * 1024).await?;
    s.controller.check(&g)?;
    Ok(bytes)
}
pub(super) async fn evaluate(
    State(s): State<HttpState>,
    body: Bytes,
) -> Result<Vec<u8>, StatusCode> {
    match tokio::time::timeout(admission::REQUEST_BUDGET, evaluate_admitted(s, body)).await {
        Ok(result) => result,
        Err(_) => {
            telemetry::admission_rejected("worker", "request_deadline");
            Err(StatusCode::GATEWAY_TIMEOUT)
        }
    }
}
async fn evaluate_admitted(s: HttpState, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    let g = s.controller.for_body(&body)?;
    s.controller.check(&g)?;
    let permit = s.permits.acquire("worker").await?;
    s.controller.check(&g)?;
    if body.len() != HEADER_BYTES + ROWS * 8 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let controller = s.controller.clone();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let coefficients = body[HEADER_BYTES..]
            .chunks_exact(8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
            .collect::<Vec<_>>();
        let values = g
            .evaluate(&coefficients)
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        let mut response = body[..HEADER_BYTES].to_vec();
        for v in values {
            response.extend(v.to_le_bytes());
        }
        controller.check(&g)?;
        Ok(response)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}
pub(super) async fn query(State(s): State<HttpState>, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    match tokio::time::timeout(admission::REQUEST_BUDGET, query_admitted(s, body)).await {
        Ok(result) => result,
        Err(_) => {
            telemetry::admission_rejected("router", "request_deadline");
            Err(StatusCode::GATEWAY_TIMEOUT)
        }
    }
}
async fn query_admitted(s: HttpState, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    let g = s.controller.for_body(&body)?;
    s.controller.check(&g)?;
    let permit = s.permits.acquire("router").await?;
    s.controller.check(&g)?;
    let coefficients = g.coefficients(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut request = body[..HEADER_BYTES].to_vec();
    for value in coefficients {
        request.extend(value.to_le_bytes());
    }
    let response = s
        .http
        .post(format!("{}/evaluate", s.origin))
        .body(request)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let response = bounded(response, HEADER_BYTES + profile().ypir().db_cols * 8).await?;
    if response.len() != HEADER_BYTES + profile().ypir().db_cols * 8
        || response[..HEADER_BYTES] != body[..HEADER_BYTES]
    {
        return Err(StatusCode::BAD_GATEWAY);
    }
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let values = response[HEADER_BYTES..]
            .chunks_exact(8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
            .collect::<Vec<_>>();
        let pack_started = Instant::now();
        let packed = g.pack(&body, &values);
        telemetry::stage("router_pack", pack_started.elapsed(), packed.is_ok());
        let packed = packed.map_err(|_| StatusCode::BAD_GATEWAY)?;
        s.controller.check(&g)?;
        Ok(packed)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}
