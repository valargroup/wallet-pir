//! The coordinator's private operator surface: health, readiness and metrics
//! of its publication state. Wallets never reach it; query ingress serves the
//! manifest, session reads and queries from views the coordinator activates.
//!
//! Retained generations live behind one generation-view lock. The publisher
//! installs immutable prepared views under that lock and answers
//! [`PublisherStatus`] from its current durable state. Lock order is the view
//! lock, then any publisher state the status reads.
use crate::{runtime::PublishedPacking, PackingBudget};
use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use enhance_query_server::protocol::{Manifest, PROTOCOL_REVISION};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A retained generation's saved routing and packing metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SavedSnapshot {
    pub manifest: Manifest,
    pub routes: BTreeMap<u64, Vec<String>>,
    #[serde(default)]
    pub prepared: BTreeMap<u64, crate::prepared_packing::Artifact>,
    pub hints: BTreeMap<u64, String>,
    pub domain_keys: BTreeMap<u64, String>,
}

/// An immutable prepared view of one retained generation.
pub struct Snapshot {
    pub saved: SavedSnapshot,
    pub packing: BTreeMap<u64, Arc<PublishedPacking>>,
}

/// Retained generations, newest first, behind the one generation-view lock.
/// Only the publisher writes it.
pub type Views = Arc<RwLock<Vec<Arc<Snapshot>>>>;

/// Publisher fields of the health document, already projected for JSON.
#[derive(Default)]
pub struct PublisherHealth {
    pub registered_groups: usize,
    pub registered_workers: usize,
    pub pool: serde_json::Value,
    pub capacity: serde_json::Value,
    pub published_replica_counts: serde_json::Value,
    pub pending_commit_notifications: usize,
    pub pending_abort_notifications: usize,
    pub operation: serde_json::Value,
    pub blocked_reason: Option<String>,
}

/// Synchronous, read-only publisher state behind the operator routes. Every
/// call reads the publisher's current state. No method may prepare, commit,
/// recover or mutate publication.
pub trait PublisherStatus: Send + Sync + 'static {
    /// Whether exact-session packing runs on remote packing routers.
    fn remote_packing(&self) -> bool;
    /// Whether packing routers and ingress have activated this generation.
    fn public_ready(&self, generation: u64) -> bool;
    /// Publisher fields of `/v1/health` for the current view.
    fn health(&self, current: Option<&Snapshot>) -> PublisherHealth;
    /// Publisher Prometheus families for the current manifest and route counts.
    fn metrics(&self, manifest: Option<&Manifest>, routes: &BTreeMap<u64, usize>) -> String;
}

#[derive(Clone)]
pub struct OperatorStatus {
    packing_budget: PackingBudget,
    views: Views,
    status: Arc<dyn PublisherStatus>,
}

impl OperatorStatus {
    pub fn new(
        status: Arc<dyn PublisherStatus>,
        packing_budget: PackingBudget,
        views: Vec<Arc<Snapshot>>,
    ) -> Self {
        Self {
            packing_budget,
            views: Arc::new(RwLock::new(views)),
            status,
        }
    }

    /// The shared generation-view lock. Only the publisher writes it.
    pub fn views(&self) -> &Views {
        &self.views
    }

    pub fn packing_budget(&self) -> &PackingBudget {
        &self.packing_budget
    }

    pub async fn manifest(&self) -> Option<Manifest> {
        self.views
            .read()
            .await
            .first()
            .map(|s| s.saved.manifest.clone())
    }

    /// Loopback-only health, readiness and metrics.
    pub fn router(self) -> Router {
        Router::new()
            .route("/v1/health", get(health))
            .route("/ready", get(ready))
            .route("/metrics", get(metrics))
            .with_state(self)
    }
}

async fn health(State(c): State<OperatorStatus>) -> Json<serde_json::Value> {
    let snapshots = c.views.read().await;
    let manifest = snapshots.first().map(|s| &s.saved.manifest);
    let packing_ready = !c.status.remote_packing()
        || manifest
            .as_ref()
            .is_some_and(|m| c.status.public_ready(m.generation));
    let publisher = c.status.health(snapshots.first().map(|s| s.as_ref()));
    Json(
        serde_json::json!({"packing_charged_bytes":c.packing_budget.charged_bytes(),"protocol":PROTOCOL_REVISION,"generation":manifest.as_ref().map(|m|m.generation),
        "anchor_height":manifest.as_ref().map(|m|m.anchor_height),"placement_revision":manifest.as_ref().map(|m|m.placement_revision),
        "registered_groups":publisher.registered_groups,
        "registered_workers":publisher.registered_workers,
        "pool":publisher.pool,
        "remote_packing":c.status.remote_packing(),
        "packing_ready":packing_ready,
        "capacity":publisher.capacity,
        "published_replica_counts":publisher.published_replica_counts,
        "pending_commit_notifications":publisher.pending_commit_notifications,
        "pending_abort_notifications":publisher.pending_abort_notifications,
        "operation":publisher.operation,"blocked_reason":publisher.blocked_reason}),
    )
}
async fn ready(State(c): State<OperatorStatus>) -> StatusCode {
    if c.manifest().await.is_some() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn metrics(
    State(c): State<OperatorStatus>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    let snapshots = c.views.read().await;
    let manifest = snapshots.first().map(|s| &s.saved.manifest);
    let routes = snapshots
        .first()
        .map(|s| {
            s.saved
                .routes
                .iter()
                .map(|(id, replicas)| (*id, replicas.len()))
                .collect()
        })
        .unwrap_or_default();
    let mut body = c.status.metrics(manifest, &routes);
    body.push_str(&crate::prepared_packing::metrics());
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}
