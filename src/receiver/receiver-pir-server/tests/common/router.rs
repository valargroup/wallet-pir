//! The production router for tests that need no owner health fields.
#![allow(dead_code)]
use pir_query_server::receiver::router_with_health;
use receiver_pir_server::Publications;

/// [`router_with_health`] whose health reports only its own fields.
pub fn router_with_publications(publications: Publications) -> axum::Router {
    router_with_health(publications, std::sync::Arc::new(serde_json::Map::new))
}
