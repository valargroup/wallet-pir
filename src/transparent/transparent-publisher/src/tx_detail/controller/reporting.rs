//! Publication telemetry and retained-candidate collection.
use super::*;

impl Controller {
    /// Removes unused candidates and recent revisions; also after a failed
    /// cycle, whose candidate nothing serves.
    pub(super) fn collect(&self) {
        let keep = std::collections::BTreeSet::from([self.active.directory.clone()]);
        if let Err(error) = collect(&self.root, &keep, self.settings.retain_candidates) {
            self.timeline.record(
                "error",
                json!({"stage": "collect", "error": error.to_string()}),
            );
        }
    }

    /// Mirrors the rollback record into `invalidate.json`. A failed write
    /// leaves it in memory, and the next change retries.
    pub(super) fn save_invalidations(&mut self) {
        let next = Invalidations {
            rollback: self.rollback.clone(),
        };
        if next == self.saved_invalidations {
            return;
        }
        match next.store(&self.root) {
            Ok(()) => self.saved_invalidations = next,
            Err(error) => {
                tracing::error!(%error, "invalidate.json write failed");
                self.timeline.record(
                    "error",
                    json!({"stage": "invalidate_record", "error": error.to_string()}),
                );
            }
        }
    }

    pub(super) fn update_status(&self) {
        let status = json!({
            "phase": if self.halted.is_some() { "halted" } else { "running" },
            "halted": self.halted,
            "mode": self.source.mode(),
            "tip": self.cache.tip().map(|t| t.0),
            "journal_end": self.store.covered_through(),
            "published": {"tip": self.active.tip, "tip_hash": self.active.tip_hash,
                "map_sha256": self.active.map_sha256, "directory": self.active.directory,
                "archives": self.map.shards.len().saturating_sub(1),
                "first_shard_id": self.map.first_shard_id, "seals": self.active.seals.len()},
            "queue": self.queue.iter().map(SealJob::summary).collect::<Vec<_>>(),
            "last_cycle": self.last_cycle,
            "failures": self.failures,
            "stalled": self.failures >= STALL_FAILURES,
            "updated_ms": now_ms(),
        });
        *self.settings.status.write().unwrap() = status;
        let published_height = self.active.tip;
        self.settings.metrics.set_gauges(Gauges {
            node_height: self
                .source
                .node_tip()
                .max(self.cache.tip().map(|t| t.0))
                .unwrap_or(published_height),
            published_height,
            oldest_unpublished_ms: self
                .observed
                .values()
                .copied()
                .chain(self.source.oldest_unfetched_ms())
                .min(),
            stalled: self.failures >= STALL_FAILURES,
            halted: self.halted.is_some(),
            consecutive_failures: self.failures,
            seal_queue: self.queue.len() as u64,
            last_success_ms: self.last_success_ms,
            last_seal_ms: self.last_seal_ms,
        });
    }
}

pub fn status_router(status: Arc<RwLock<Value>>, metrics: Arc<Metrics>) -> axum::Router {
    use axum::{routing::get, Json, Router};
    let current = move || {
        let status = status.clone();
        async move { Json(status.read().unwrap().clone()) }
    };
    let rendered = move || {
        let metrics = metrics.clone();
        async move {
            (
                [("content-type", "text/plain; version=0.0.4")],
                metrics.render(),
            )
        }
    };
    Router::new()
        .route("/", get(current.clone()))
        .route("/v1/status", get(current))
        .route("/v1/health", get(|| async { "ok" }))
        .route("/metrics", get(rendered))
}
