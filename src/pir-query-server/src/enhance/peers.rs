//! Where an Enhance role reaches the role below it: a separate process over
//! private HTTP, or a component of this process through a direct call. Both
//! carry the same readiness, release, session and query messages, bind the
//! same incarnation and never replay work whose acceptance is unknown.
use crate::enhance::{
    ingress_sync::RouterPeer,
    router_sync::PackingRouter,
    synchronization::Error,
    worker_sync::{Readiness, ReadinessRequest, Release, Worker},
};
use axum::{http::HeaderMap, response::Response};
use enhance_shared::publication::METADATA_LIMIT;
use std::time::Duration;

/// The key a local component uses in peer selection, exclusion and counts.
pub(crate) const LOCAL: &str = "local";

/// One evaluation replica a packing router may select.
#[derive(Clone)]
pub(crate) enum WorkerEndpoint {
    Http(String),
    Local(Box<Worker>),
}

impl WorkerEndpoint {
    pub(crate) fn key(&self) -> &str {
        match self {
            Self::Http(url) => url,
            Self::Local(_) => LOCAL,
        }
    }

    pub(crate) async fn readiness(
        &self,
        http: &reqwest::Client,
        request: &ReadinessRequest,
        timeout: Duration,
    ) -> Result<Readiness, Error> {
        match self {
            Self::Http(url) => {
                let response = http
                    .post(format!("{url}/internal/readiness"))
                    .timeout(timeout)
                    .json(request)
                    .send()
                    .await?
                    .error_for_status()?;
                let bytes =
                    crate::enhance::query_serving::bounded(response, METADATA_LIMIT as usize)
                        .await?;
                Ok(serde_json::from_slice(&bytes)?)
            }
            Self::Local(worker) => worker
                .readiness(request.clone())
                .await
                .map_err(|(_, message)| message.into()),
        }
    }

    pub(crate) async fn release(&self, http: &reqwest::Client, request: Release) {
        match self {
            Self::Http(url) => {
                let _ = http
                    .post(format!("{url}/internal/release"))
                    .timeout(Duration::from_secs(1))
                    .json(&request)
                    .send()
                    .await;
            }
            Self::Local(worker) => {
                let _ = worker.release_promise(request);
            }
        }
    }
}

/// One packing router the ingress may select.
#[derive(Clone)]
pub(crate) enum RouterEndpoint {
    Http(RouterPeer),
    Local(Box<PackingRouter>),
}

/// A router's answer as the ingress forwards it: status, the headers the
/// ingress copies, and the complete bounded body.
pub(crate) struct Forwarded {
    pub status: axum::http::StatusCode,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

impl RouterEndpoint {
    pub(crate) fn key(&self) -> &str {
        match self {
            Self::Http(peer) => &peer.control_origin,
            Self::Local(_) => LOCAL,
        }
    }

    pub(crate) async fn readiness(
        &self,
        http: &reqwest::Client,
        request: &ReadinessRequest,
    ) -> Result<Readiness, Error> {
        match self {
            Self::Http(peer) => {
                let response = http
                    .post(format!("{}/internal/readiness", peer.control_origin))
                    .timeout(Duration::from_secs(2))
                    .json(request)
                    .send()
                    .await?
                    .error_for_status()?;
                let bytes =
                    crate::enhance::query_serving::bounded(response, METADATA_LIMIT as usize)
                        .await?;
                Ok(serde_json::from_slice(&bytes)?)
            }
            Self::Local(router) => router
                .readiness(request.clone())
                .await
                .map_err(|(_, message)| message.into()),
        }
    }

    pub(crate) async fn release(&self, http: &reqwest::Client, request: Release) {
        match self {
            Self::Http(peer) => {
                let _ = http
                    .post(format!("{}/internal/release", peer.control_origin))
                    .timeout(Duration::from_secs(2))
                    .json(&request)
                    .send()
                    .await;
            }
            Self::Local(router) => {
                let _ = router.release_promise(request);
            }
        }
    }

    /// Fetch one session's packing setup from the router that promised it.
    pub(crate) async fn session(
        &self,
        http: &reqwest::Client,
        incarnation: &str,
        id: &str,
        limit: usize,
    ) -> Result<Forwarded, String> {
        match self {
            Self::Http(peer) => {
                let response = http
                    .get(format!("{}/v1/enhance/session/{id}", peer.query_origin))
                    .header("x-enhance-router-incarnation", incarnation)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                forwarded(response, limit).await
            }
            Self::Local(router) => {
                collect(router.session_answer(Some(incarnation), id).await, limit).await
            }
        }
    }

    /// Hand one admitted wallet query to the router. Any failure after the
    /// router may have received it is final: the caller never replays it.
    pub(crate) async fn query(
        &self,
        http: &reqwest::Client,
        incarnation: &str,
        bytes: axum::body::Bytes,
        limit: usize,
    ) -> Result<Forwarded, String> {
        match self {
            Self::Http(peer) => {
                let response = http
                    .post(format!("{}/v1/enhance/query", peer.query_origin))
                    .header("x-enhance-router-incarnation", incarnation)
                    .body(bytes)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
                forwarded(response, limit).await
            }
            Self::Local(router) => {
                collect(
                    router
                        .query_answer(Some(incarnation), bytes, Duration::ZERO)
                        .await,
                    limit,
                )
                .await
            }
        }
    }
}

async fn forwarded(response: reqwest::Response, limit: usize) -> Result<Forwarded, String> {
    let status = response.status();
    let headers = response.headers().clone();
    let body = crate::enhance::query_serving::bounded(response, limit).await?;
    Ok(Forwarded {
        status,
        headers,
        body,
    })
}

/// A local answer's body ends with the same authority check its HTTP body
/// would run at the end of transport; a failed check fails the collection.
async fn collect(response: Response, limit: usize) -> Result<Forwarded, String> {
    let (parts, body) = response.into_parts();
    let body = axum::body::to_bytes(body, limit)
        .await
        .map_err(|e| e.to_string())?;
    Ok(Forwarded {
        status: parts.status,
        headers: parts.headers,
        body: body.to_vec(),
    })
}
