//! Optional shared bearer token for private control APIs.
//!
//! When `ENHANCE_INTERNAL_TOKEN` is set, worker, packing-router and ingress
//! control routes reject requests without `Authorization: Bearer <token>`, and
//! the coordinator/router clients attach that header to every request. Unset
//! leaves the network-isolation-only behaviour unchanged. This is a shared
//! secret, not per-role identity; keep the private listeners firewalled.
use axum::{
    extract::{Request, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    middleware::{from_fn_with_state, FromFnLayer, Next},
    response::{IntoResponse, Response},
};
use std::{future::Future, pin::Pin, sync::Arc};

pub const ENV: &str = "ENHANCE_INTERNAL_TOKEN";

type BoxResponse = Pin<Box<dyn Future<Output = Response> + Send>>;
type RequireFn = fn(State<Token>, Request, Next) -> BoxResponse;
pub type Layer = FromFnLayer<RequireFn, Token, (State<Token>, Request)>;

#[derive(Clone, Default)]
pub struct Token(Option<Arc<str>>);

impl Token {
    /// The process-wide token from the environment; empty counts as unset.
    pub fn from_env() -> Self {
        Self(
            std::env::var(ENV)
                .ok()
                .filter(|t| !t.is_empty())
                .map(Arc::from),
        )
    }
    pub fn new(token: Option<&str>) -> Self {
        Self(token.filter(|t| !t.is_empty()).map(Arc::from))
    }
    pub fn is_set(&self) -> bool {
        self.0.is_some()
    }
    fn accepts(&self, headers: &HeaderMap) -> bool {
        let Some(expected) = &self.0 else {
            return true;
        };
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|presented| constant_time_eq(presented.as_bytes(), expected.as_bytes()))
    }
    /// Layer that rejects unauthenticated requests when a token is configured.
    pub fn layer(&self) -> Layer {
        from_fn_with_state(self.clone(), require as RequireFn)
    }
    /// A reqwest client builder that sends the token as a default header.
    pub fn client_builder(&self) -> reqwest::ClientBuilder {
        let builder = reqwest::Client::builder();
        match &self.0 {
            Some(token) => {
                let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                    .expect("internal token must be a printable header value");
                value.set_sensitive(true);
                let mut headers = HeaderMap::new();
                headers.insert(header::AUTHORIZATION, value);
                builder.default_headers(headers)
            }
            None => builder,
        }
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn require(State(token): State<Token>, request: Request, next: Next) -> BoxResponse {
    Box::pin(async move {
        if token.accepts(request.headers()) {
            next.run(request).await
        } else {
            (StatusCode::UNAUTHORIZED, "internal token required").into_response()
        }
    })
}

/// Convenience for roles that read the token from the environment.
pub fn client_builder() -> reqwest::ClientBuilder {
    Token::from_env().client_builder()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use tower::ServiceExt;

    fn app(token: Token) -> Router {
        Router::new()
            .route("/internal/health", get(|| async { "ok" }))
            .layer(token.layer())
    }
    async fn status(app: Router, auth: Option<&str>) -> StatusCode {
        let mut request = Request::builder().uri("/internal/health");
        if let Some(auth) = auth {
            request = request.header("authorization", auth);
        }
        app.oneshot(request.body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn configured_token_is_required_and_unset_token_is_open() {
        let token = Token::new(Some("s3cret"));
        assert_eq!(
            status(app(token.clone()), None).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(app(token.clone()), Some("Bearer wrong")).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(app(token.clone()), Some("Bearer s3cret")).await,
            StatusCode::OK
        );
        assert_eq!(status(app(Token::new(None)), None).await, StatusCode::OK);
        assert_eq!(
            status(app(Token::new(Some(""))), None).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn client_builder_sends_the_bearer_header() {
        let token = Token::new(Some("s3cret"));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/internal/health", listener.local_addr().unwrap());
        let server =
            tokio::spawn(async move { axum::serve(listener, app(token.clone())).await.unwrap() });
        let anonymous = reqwest::Client::new().get(&url).send().await.unwrap();
        assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
        let authenticated = Token::new(Some("s3cret"))
            .client_builder()
            .build()
            .unwrap()
            .get(&url)
            .send()
            .await
            .unwrap();
        assert_eq!(authenticated.status(), StatusCode::OK);
        server.abort();
    }
}
