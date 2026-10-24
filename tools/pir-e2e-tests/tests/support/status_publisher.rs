//! HTTPS Status publisher for one real prepared fixture generation: discovery,
//! the publication envelope, live authority answers and content-addressed
//! objects, behind the publisher distribution TLS terminator.
use axum::{
    extract::Path,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use pir_publication::{
    conditional_token, ObjectReference, SubjectAnswer, AUTHORITY_CHECK_PATH, CURRENT_PATH,
    OBJECTS_PATH,
};
use status_shared::{
    fixture::{self, NETWORK, SALT},
    now_ms,
    publication::{scope, AnchorCheckRequest, AnchorCheckResponse, Current, PREFIX},
    Generation,
};
use std::{collections::HashMap, sync::Arc};

use super::tls;

pub const ENTRIES: usize = 32;

pub struct StatusPublisher {
    pub origin: String,
    pub certificate: std::path::PathBuf,
    pub snapshot: status_shared::index::Snapshot,
    _tls: tls::Tls,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for StatusPublisher {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl StatusPublisher {
    /// Publish the fixture snapshot as generation 4 at recovery epoch 2.
    pub async fn start() -> Self {
        let snapshot = fixture::snapshot(ENTRIES, false).unwrap();
        let (generation, _) = Generation::prepare(&snapshot, 4, 2, 1, None).unwrap();
        let mut objects = HashMap::<String, Vec<u8>>::new();
        let mut inventory = generation
            .export_prepared(|bytes| {
                let object = ObjectReference::for_bytes(bytes);
                objects.insert(object.sha256.clone(), bytes.to_vec());
                Ok(object)
            })
            .unwrap();
        inventory.manifest.observed_ms = now_ms();
        let bytes = serde_json::to_vec(&inventory).unwrap();
        let object = ObjectReference::for_bytes(&bytes);
        objects.insert(object.sha256.clone(), bytes);
        let current = Current {
            scope: scope(NETWORK, SALT),
            sequence: 7,
            ..fixture::publishing(&inventory, &object)
        };
        let (current, objects) = (Arc::new(current), Arc::new(objects));
        let (envelope, authority) = (current.clone(), current);
        let app = Router::new()
            .route(
                pir_service_discovery::PATH,
                get(|| async { pir_service_discovery::Document::publisher().to_json() }),
            )
            .route(
                &format!("{PREFIX}{CURRENT_PATH}"),
                get(move |headers: HeaderMap| {
                    let current = envelope.clone();
                    async move {
                        let token = conditional_token(&current);
                        if headers.get("if-none-match").and_then(|v| v.to_str().ok())
                            == Some(&token)
                        {
                            StatusCode::NOT_MODIFIED.into_response()
                        } else {
                            Json((*current).clone()).into_response()
                        }
                    }
                }),
            )
            .route(
                &format!("{PREFIX}{AUTHORITY_CHECK_PATH}"),
                post(move |Json(request): Json<AnchorCheckRequest>| {
                    let current = authority.clone();
                    async move {
                        Json(AnchorCheckResponse {
                            authority: current.authority().unwrap(),
                            answers: request
                                .subjects
                                .into_iter()
                                .map(|subject| SubjectAnswer {
                                    subject,
                                    still_authoritative: true,
                                })
                                .collect(),
                        })
                    }
                }),
            )
            .route(
                &format!("{PREFIX}{OBJECTS_PATH}:id"),
                get(move |Path(id): Path<String>| {
                    let objects = objects.clone();
                    async move {
                        objects
                            .get(&id)
                            .cloned()
                            .map_or(StatusCode::NOT_FOUND.into_response(), |bytes| {
                                bytes.into_response()
                            })
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let tls = tls::Tls::start(&upstream).await;
        Self {
            origin: tls.origin.clone(),
            certificate: tls.certificate.clone(),
            snapshot,
            _tls: tls,
            server,
        }
    }
}
