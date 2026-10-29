//! Wallet-facing filters, publication status, and telemetry.
use super::*;

pub(super) async fn public_request(
    State(authority): State<Authority>,
    request: Request,
) -> Response {
    let path = request.uri().path();
    if path.starts_with(transparent_shared::publication::PREFIX) {
        return distribution_request(authority, request).await;
    }
    let epoch = authority.0.epoch.load(Ordering::Acquire);
    if path == "/v1/status" {
        let observation = {
            let _gate = authority.0.publication_gate.lock().unwrap();
            let active = authority.0.active.read().unwrap();
            serde_json::json!({
                "height": active.publication.height,
                "hash": active.publication.hash,
                "map_sha256": active.publication.map_sha256,
                "revision": authority.0.distribution_revision.load(Ordering::Acquire),
                "withdrawn": authority.0.withdrawn.load(Ordering::Acquire),
            })
        };
        let mut status = authority.0.status.read().unwrap().clone();
        status["publication_observation"] = observation;
        return axum::Json(status).into_response();
    }
    if path == "/metrics" {
        let status = authority.0.status.read().unwrap().clone();
        let public = authority.0.active.read().unwrap().publication.height;
        let node = status["node_height"].as_u64().unwrap_or(public);
        let oldest = authority
            .0
            .observed
            .read()
            .unwrap()
            .range((public.saturating_add(1))..)
            .map(|(_, t)| t.elapsed().as_secs_f64())
            .fold(0f64, f64::max);
        let mut text = authority.0.telemetry.render();
        text.push_str(
            &authority
                .0
                .cycle_duration
                .render("transparent_publication_cycle_seconds"),
        );
        text.push_str(
            &authority
                .0
                .publication_freshness
                .render("transparent_publication_freshness_seconds"),
        );
        for (name, value) in [
            (
                "attempts_total",
                authority.0.publication_attempts.load(Ordering::Relaxed),
            ),
            (
                "failures_total",
                authority.0.publication_failures.load(Ordering::Relaxed),
            ),
            (
                "last_success_timestamp_seconds",
                authority.0.last_publication.load(Ordering::Relaxed),
            ),
            (
                "runtime_preparations_total",
                authority.0.runtime_preparations.load(Ordering::Relaxed),
            ),
            (
                "runtime_preparation_failures_total",
                authority
                    .0
                    .runtime_preparation_failures
                    .load(Ordering::Relaxed),
            ),
        ] {
            text.push_str(&format!("transparent_publication_{name} {value}\n"));
        }
        for (name, value) in [
            ("node_height", node as f64),
            (
                "journal_height",
                status["journal_height"].as_f64().unwrap_or(0.),
            ),
            ("public_height", public as f64),
            ("lag_blocks", node.saturating_sub(public) as f64),
            ("oldest_unpublished_seconds", oldest),
            (
                "withdrawn",
                if authority.0.withdrawn.load(Ordering::Acquire) {
                    1.
                } else {
                    0.
                },
            ),
            (
                "last_cycle_seconds",
                status["cycle_seconds"].as_f64().unwrap_or(0.),
            ),
            (
                "last_freshness_seconds",
                status["freshness_seconds"].as_f64().unwrap_or(0.),
            ),
            ("reorg_depth", status["reorg_depth"].as_f64().unwrap_or(0.)),
            (
                "integrity_validation_state",
                validation_state(&status["integrity_validation"]),
            ),
            (
                "integrity_validation_seconds",
                status["integrity_validation"]["seconds"]
                    .as_f64()
                    .unwrap_or(0.),
            ),
            (
                "correctness_validation_state",
                validation_state(&status["correctness_validation"]),
            ),
            (
                "correctness_validation_seconds",
                status["correctness_validation"]["seconds"]
                    .as_f64()
                    .unwrap_or(0.),
            ),
        ] {
            text.push_str(&format!("# TYPE transparent_publication_{name} gauge\ntransparent_publication_{name} {value}\n"));
        }
        return ([("content-type", "text/plain; version=0.0.4")], text).into_response();
    }
    if request.method() != axum::http::Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    if authority.0.withdrawn.load(Ordering::Acquire) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("retry-after", "1"), ("cache-control", "no-store")],
            "transparent publication is being reconciled",
        )
            .into_response();
    }
    let active = authority.0.active.read().unwrap().clone();
    // Wallet-facing operations answer under `/transparent/v1` and the deployed
    // `/v2` alias alike; status, metrics and distribution matched above.
    let operation = transparent_shared::http::shard_operation(path).unwrap_or_default();
    let body = if operation == "/shards" || operation == "/filters/shards" {
        Some(("application/json", active.filters.map_json().to_vec()))
    } else if operation == "/shards/init" {
        // Query servers answer setup themselves; this publisher has no
        // workers to assemble it from.
        return unavailable();
    } else {
        let parts: Vec<_> = operation.trim_start_matches('/').split('/').collect();
        match parts.as_slice() {
            ["filters", "shards", id, "filter"] => id
                .parse()
                .ok()
                .and_then(|id| active.filters.filter(id))
                .map(|b| ("application/octet-stream", b.to_vec())),
            ["shards", id, "revisions", digest, "manifest"]
                if digest.len() == 64 && digest.bytes().all(|c| c.is_ascii_hexdigit()) =>
            {
                if active
                    .filters
                    .map()
                    .shards
                    .iter()
                    .any(|e| e.shard_id.to_string() == *id && e.manifest_digest == *digest)
                {
                    std::fs::read(
                        active
                            .publication
                            .directory
                            .join(digest)
                            .join("manifest.json"),
                    )
                    .ok()
                    .map(|b| ("application/json", b))
                } else {
                    return StatusCode::CONFLICT.into_response();
                }
            }
            _ => None,
        }
    };
    if authority.0.withdrawn.load(Ordering::Acquire)
        || authority.0.epoch.load(Ordering::Acquire) != epoch
    {
        return unavailable();
    }
    match body {
        Some((content_type, body)) => (
            [
                ("content-type", content_type),
                ("cache-control", "no-cache"),
                ("x-shard-map-sha256", active.publication.map_sha256.as_str()),
            ],
            body,
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// 0 pending, 1 passed, -1 failed.
pub(super) fn validation_state(value: &serde_json::Value) -> f64 {
    match value["state"].as_str() {
        Some("passed") => 1.,
        Some("failed") => -1.,
        _ => 0.,
    }
}
