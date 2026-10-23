//! Observe publisher progress; readiness requires a fresh chain check.
use super::EnhancePublisher;
use enhance_shared::protocol;

pub(super) async fn health(
    axum::extract::State(p): axum::extract::State<EnhancePublisher>,
) -> axum::Json<serde_json::Value> {
    let current = p.distribution.durable_current();
    let state = p.state.lock().unwrap();
    axum::Json(
        serde_json::json!({"protocol": protocol::PROTOCOL_REVISION, "generation": current.manifest.as_ref().map(|m| m.generation),
        "manifest_sha256": current.manifest.as_ref().map(protocol::digest),
        "domains": current.manifest.as_ref().map(|m| m.coverage.shards.iter().map(|s| s.id).collect::<Vec<_>>()),
        "anchor_height": current.manifest.as_ref().map(|m| m.anchor_height), "records": current.manifest.as_ref().map(|m| m.coverage.records),
        "eligible": current.eligible && !state.poisoned, "preparing_height": state.preparing, "last_error": state.error }),
    )
}
pub(super) async fn ready(
    axum::extract::State(p): axum::extract::State<EnhancePublisher>,
) -> axum::http::StatusCode {
    match p.distribution.authority(None).await {
        Ok(observation) if observation.current.eligible => axum::http::StatusCode::OK,
        _ => axum::http::StatusCode::SERVICE_UNAVAILABLE,
    }
}
pub(super) async fn metrics(
    axum::extract::State(p): axum::extract::State<EnhancePublisher>,
) -> impl axum::response::IntoResponse {
    let current = p.distribution.durable_current();
    let state = p.state.lock().unwrap();
    let mut text = format!(
        "enhance_publication_eligible {}\nenhance_publication_blocked {}\nenhance_publication_consecutive_failures {}\n",
        u8::from(current.eligible && !state.poisoned),
        u8::from(!current.eligible || state.poisoned || state.error.is_some()),
        state.consecutive_failures,
    );
    if let Some(at) = state
        .last_advancement
        .and_then(|at| at.duration_since(std::time::UNIX_EPOCH).ok())
    {
        text.push_str(&format!(
            "enhance_publication_last_advancement_unix_seconds {}\n",
            at.as_secs()
        ));
    }
    if let Some(manifest) = current.manifest {
        text.push_str(&format!("enhance_published_anchor_height {}\nenhance_published_generation {}\nenhance_published_records {}\n", manifest.anchor_height, manifest.generation, manifest.coverage.records));
    }
    ([("content-type", "text/plain; version=0.0.4")], text)
}
