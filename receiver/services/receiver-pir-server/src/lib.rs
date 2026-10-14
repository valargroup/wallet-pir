//! Production instrumentation around the independently released receiver handlers.
use axum::{http::header, routing::get, Router};
pub use receiver_server::{publication, HealthFields, Publication, Publications};
use std::sync::Arc;
const ENDPOINTS: [&str; 7] = [
    "receiver_init",
    "receiver_public",
    "receiver_query",
    "receiver_rows",
    "receiver_witness",
    "receiver_filters",
    "receiver_health",
];
/// Receiver routes with the service's process identity and aggregate HTTP metrics.
pub fn router_with_publications(publications: Publications) -> Router {
    router_with_health(publications, Arc::new(serde_json::Map::new))
}
/// Caller fields are augmented with process identity; reserved protocol fields win.
pub fn router_with_health(publications: Publications, fields: HealthFields) -> Router {
    let fields: HealthFields = Arc::new(move || {
        let mut fields = fields();
        fields.insert(
            "identity".into(),
            serde_json::json!(pir_control::Identity::process()),
        );
        fields
    });
    let metrics = pir_observability::HttpMetrics::default();
    metrics.initialize(&ENDPOINTS);
    let rendered = metrics.clone();
    receiver_server::router_with_health(publications, fields)
        .route(
            "/metrics",
            get(move || {
                let metrics = rendered.clone();
                async move { ([(header::CONTENT_TYPE, "text/plain")], metrics.render()) }
            }),
        )
        .layer(axum::middleware::from_fn_with_state(
            metrics,
            pir_observability::observe,
        ))
}
