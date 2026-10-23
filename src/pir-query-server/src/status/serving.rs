//! Wallet-facing and worker query handlers of the Status roles. Each one checks
//! authority and freshness before and after admission.
use super::admission;
use crate::status::Controller;
use axum::{body::Bytes, extract::State, http::StatusCode};
use status_shared::{profile, protocol::*, telemetry};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

/// Reuse TCP connections across lookups, including SSH forwarding channels.
/// Authority/freshness checks remain mandatory for every request on reused connections.
pub fn serving_http_client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .timeout(admission::REQUEST_BUDGET)
        .connect_timeout(Duration::from_secs(1))
        .pool_max_idle_per_host(4)
        .pool_idle_timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

/// Where a wallet query's database evaluation runs.
#[derive(Clone)]
pub enum Evaluator {
    /// The router's worker, over its private loopback evaluation route.
    Peer {
        origin: String,
        http: reqwest::Client,
    },
    /// This process's own serving units: the worker's evaluation route, and
    /// a combined role answering wallets without a peer.
    Local,
}

#[derive(Clone)]
pub struct HttpState {
    pub controller: Controller,
    pub evaluator: Evaluator,
    pub permits: Arc<admission::Admission>,
}
#[cfg(test)]
impl HttpState {
    fn new(controller: Controller, origin: String) -> Self {
        Self {
            controller,
            evaluator: Evaluator::Peer {
                origin,
                http: serving_http_client().expect("Status HTTP client"),
            },
            permits: Arc::new(admission::Admission::production()),
        }
    }
}
/// Response body over shared immutable material, without copying it.
struct SharedBytes(Arc<Vec<u8>>);
impl AsRef<[u8]> for SharedBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}
pub fn shared_bytes(bytes: Arc<Vec<u8>>) -> Bytes {
    Bytes::from_owner(SharedBytes(bytes))
}
pub async fn bounded(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>, StatusCode> {
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
pub async fn evaluate(State(state): State<HttpState>, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    match tokio::time::timeout(admission::REQUEST_BUDGET, evaluate_admitted(state, body)).await {
        Ok(result) => result,
        Err(_) => {
            telemetry::admission_rejected("worker", "request_deadline");
            Err(StatusCode::GATEWAY_TIMEOUT)
        }
    }
}
async fn evaluate_admitted(state: HttpState, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    let generation = state.controller.for_body(&body)?;
    state.controller.check(&generation)?;
    let permit = state.permits.acquire("worker").await?;
    state.controller.check(&generation)?;
    if body.len() != HEADER_BYTES + ROWS * 8 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let controller = state.controller.clone();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let coefficients = body[HEADER_BYTES..]
            .chunks_exact(8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
            .collect::<Vec<_>>();
        let values = generation
            .evaluate(&coefficients)
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        let mut response = body[..HEADER_BYTES].to_vec();
        for v in values {
            response.extend(v.to_le_bytes());
        }
        controller.check(&generation)?;
        Ok(response)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}
pub async fn query(State(state): State<HttpState>, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    match tokio::time::timeout(admission::REQUEST_BUDGET, query_admitted(state, body)).await {
        Ok(result) => result,
        Err(_) => {
            telemetry::admission_rejected("router", "request_deadline");
            Err(StatusCode::GATEWAY_TIMEOUT)
        }
    }
}
async fn query_admitted(state: HttpState, body: Bytes) -> Result<Vec<u8>, StatusCode> {
    let generation = state.controller.for_body(&body)?;
    state.controller.check(&generation)?;
    let permit = state.permits.acquire("router").await?;
    state.controller.check(&generation)?;
    let coefficients = generation
        .coefficients(&body)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    // A peer answers with its evaluation; a local evaluation runs below under
    // this request's permit, beside packing.
    enum Evaluation {
        Answered(Vec<u8>),
        Pending(Vec<u64>),
    }
    let evaluation = match &state.evaluator {
        Evaluator::Peer { origin, http } => {
            let mut request = body[..HEADER_BYTES].to_vec();
            for value in coefficients {
                request.extend(value.to_le_bytes());
            }
            let response = http
                .post(format!("{origin}/evaluate"))
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
            Evaluation::Answered(response)
        }
        Evaluator::Local => Evaluation::Pending(coefficients),
    };
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let values = match evaluation {
            Evaluation::Answered(response) => response[HEADER_BYTES..]
                .chunks_exact(8)
                .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
                .collect::<Vec<_>>(),
            Evaluation::Pending(coefficients) => {
                let started = Instant::now();
                let values = generation.evaluate(&coefficients);
                telemetry::stage("worker_evaluate", started.elapsed(), values.is_ok());
                // A distributed router relays its worker's refusal status.
                let values = values.map_err(|_| StatusCode::BAD_REQUEST)?;
                state.controller.check(&generation)?;
                values
            }
        };
        let pack_started = Instant::now();
        let packed = generation.pack(&body, &values);
        telemetry::stage("router_pack", pack_started.elapsed(), packed.is_ok());
        let packed = packed.map_err(|_| StatusCode::BAD_GATEWAY)?;
        state.controller.check(&generation)?;
        Ok(packed)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use status_shared::{now_ms, Generation};

    fn generation() -> Generation {
        Generation::public_only(
            Manifest {
                protocol: PROTOCOL.into(),
                network: [1; 32],
                salt: [2; 32],
                generation: 1,
                recovery_epoch: 0,
                coverage_start: 1,
                anchor_height: 1,
                anchor_hash: [3; 32],
                observed_ms: now_ms(),
                entries: 0,
                rows_digest: [4; 32],
                public_digest: [5; 32],
            },
            Arc::new(Vec::new()),
        )
    }

    #[tokio::test]
    async fn serving_client_clones_reuse_forwarding_connection() {
        async fn peer(
            axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<std::net::SocketAddr>,
        ) -> String {
            peer.to_string()
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().route("/", get(peer));
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        let client = serving_http_client().unwrap();
        let first = client
            .get(&origin)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let second = client
            .clone()
            .get(&origin)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(
            first, second,
            "requests must reuse the same TCP/SSH channel"
        );
        server.abort();
    }

    #[tokio::test]
    async fn authority_is_rechecked_after_admission_wait() {
        let controller = Controller::new(generation());
        let state = HttpState::new(controller.clone(), "http://127.0.0.1:1".into());
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(state.permits.acquire("router").await.unwrap());
        }
        let mut body = vec![0; HEADER_BYTES];
        body[..4].copy_from_slice(QUERY_MAGIC);
        body[4..36].copy_from_slice(&controller.current().manifest().id());
        let request = query(State(state), Bytes::from(body));
        tokio::pin!(request);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut request)
                .await
                .is_err()
        );
        controller.revoke();
        drop(held);
        assert_eq!(request.await, Err(StatusCode::GONE));
    }

    #[tokio::test]
    async fn source_freshness_is_rechecked_after_admission_wait() {
        let generation = generation().reobserved(now_ms() - MAX_AGE_MS + 100);
        let controller = Controller::new(generation);
        let state = HttpState::new(controller.clone(), "http://127.0.0.1:1".into());
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(state.permits.acquire("router").await.unwrap());
        }
        let mut body = vec![0; HEADER_BYTES];
        body[..4].copy_from_slice(QUERY_MAGIC);
        body[4..36].copy_from_slice(&controller.current().manifest().id());
        let request = query(State(state), Bytes::from(body));
        tokio::pin!(request);
        assert!(
            tokio::time::timeout(Duration::from_millis(120), &mut request)
                .await
                .is_err()
        );
        drop(held);
        assert_eq!(request.await, Err(StatusCode::SERVICE_UNAVAILABLE));
    }
}
