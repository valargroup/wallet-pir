//! Coordinator with atomic manifests and generation-specific replica routes.
mod publication;
mod recovery;
mod snapshots;
use super::{
    control::{self, Group, Operation, PendingCommit, Phase, ReadyAck, Store},
    runtime::{self, DomainPlan, Packing, PublishedPacking},
    worker,
};
use axum::{
    body::to_bytes,
    extract::{Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use enhance_pir::protocol::*;
use serde::{Deserialize, Serialize};
use snapshots::{collect_artifacts, restore, SavedSnapshot, Snapshot};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path as FsPath, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use tokio::sync::{RwLock, Semaphore};

// Two replicas each admit two evaluations. Keep the coordinator aligned with
// that aggregate worker capacity so it does not queue behind idle replicas.
const QUERY_ACTIVE_LIMIT: usize = 4;
const QUERY_WAIT_LIMIT: usize = 16;
const QUERY_WAIT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(2);
const QUERY_BODY_LIMIT: usize = 512 * 1024;

#[derive(Default)]
struct QueryStats {
    route_cursor: AtomicU64,
    wait_count: AtomicU64,
    wait_micros: AtomicU64,
    rejected: AtomicU64,
    primary_success: AtomicU64,
    fallback_success: AtomicU64,
    worker_busy: AtomicU64,
    worker_failure: AtomicU64,
}

#[derive(Debug)]
struct QueryError(StatusCode, String);

impl From<(StatusCode, String)> for QueryError {
    fn from((status, message): (StatusCode, String)) -> Self {
        Self(status, message)
    }
}

impl IntoResponse for QueryError {
    fn into_response(self) -> Response {
        super::query_serving::public_error((self.0, self.1))
    }
}

// Native full-group cold preparation exceeded the shared 180-second HTTP
// deadline during sealed qualification. Queries/control probes keep their short
// deadlines; this bounded offline operation also covers recovery replay.
const PREPARATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

fn preparation_request(http: &reqwest::Client, worker_url: &str) -> reqwest::RequestBuilder {
    http.post(format!("{worker_url}/internal/prepare"))
        .timeout(PREPARATION_TIMEOUT)
}

#[derive(Clone)]
pub struct Coordinator {
    packing_budget: super::packing_budget::PackingBudget,
    store: Arc<Mutex<Store>>,
    snapshots: Arc<RwLock<Vec<Arc<Snapshot>>>>,
    publication: Arc<Semaphore>,
    queries: Arc<Semaphore>,
    query_waiters: Arc<Semaphore>,
    query_stats: Arc<QueryStats>,
    http_metrics: super::http_metrics::HttpMetrics,
    http: reqwest::Client,
    root: PathBuf,
    blocked: Arc<Mutex<Option<String>>>,
    telemetry: Arc<Mutex<super::telemetry::Publication>>,
    serving: Option<Arc<super::serving_control::ServingControl>>,
}

fn validate_inventory(groups: &[Group]) -> Result<(), String> {
    for group in groups {
        group.placement_policy.validate()?;
        if group.placement_policy != groups[0].placement_policy {
            return Err("inventory placement policies differ".into());
        }
    }
    let mut names = BTreeSet::new();
    let mut urls = BTreeSet::new();
    if groups.is_empty() || groups.len() > 16 {
        return Err("requires one to sixteen inventory containers".into());
    }
    for (sequence, group) in groups.iter().enumerate() {
        if group.id.is_empty()
            || group.sequence != sequence as u64
            || !names.insert(group.id.clone())
            || group.replicas.is_empty()
            || group.replicas.len() > 2
        {
            return Err("invalid group inventory".into());
        }
        for r in &group.replicas {
            let url = reqwest::Url::parse(&r.url).map_err(|e| e.to_string())?;
            if r.name.is_empty()
                || !names.insert(r.name.clone())
                || !urls.insert(url.to_string())
                || url.scheme() != "http"
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
                || url.path() != "/"
            {
                return Err("invalid or duplicate private worker origin".into());
            }
        }
    }
    Ok(())
}

fn inventory_extends(existing: &[Group], configured: &[Group]) -> bool {
    configured.len() >= existing.len()
        && existing.iter().zip(configured).all(|(a, b)| {
            a.id == b.id
                && a.placement_policy == b.placement_policy
                && a.sequence == b.sequence
                && a.replicas.len() == b.replicas.len()
                && a.replicas
                    .iter()
                    .zip(&b.replicas)
                    .all(|(a, b)| a.name == b.name && a.url == b.url)
        })
}

impl Coordinator {
    /// Records the outcome of the latest canonical ingestion pass. Ingestion
    /// failures never block serving, so they are reported separately from the
    /// publication `blocked_reason` in health and metrics.
    pub fn observe_ingestion(&self, result: Result<(), String>) {
        let mut telemetry = self.telemetry.lock().unwrap();
        match result {
            Ok(()) => {
                telemetry.ingestion_error = None;
                telemetry.last_ingest_success = Some(std::time::Instant::now());
            }
            Err(error) => telemetry.ingestion_error = Some(error),
        }
    }

    pub fn observe_capacity(
        &self,
        records: u64,
        now: u64,
        policy: super::capacity::Policy,
    ) -> Result<(), String> {
        self.store.lock().unwrap().update(|state| {
            if let Some(pool) = &state.pool {
                return state.capacity.observe_pool(
                    records,
                    now,
                    policy,
                    &state.groups,
                    &state.lifecycle,
                    pool,
                );
            }
            state.capacity.observe(
                records,
                now,
                policy,
                &state.groups,
                &state.lifecycle,
                &state.assignments,
            )
        })
    }
    // Run synchronously under the caller's publication permit: a detached cleanup
    // task must never outlive that permit and race creation of the next candidate.
    fn collect_retired_artifacts(&self) {
        let store = self.store.lock().unwrap();
        if store.state().operation.is_some() {
            return;
        }
        if let Err(error) = collect_artifacts(&self.root, &store.state().published) {
            tracing::warn!(%error, "coordinator artifact cleanup deferred");
        }
    }

    pub fn open(root: &FsPath, groups: Vec<Group>) -> Result<Self, String> {
        Self::open_with_routers(root, groups, Vec::new())
    }

    pub fn open_with_routers(
        root: &FsPath,
        groups: Vec<Group>,
        routers: Vec<super::packing_router::RouterRegistration>,
    ) -> Result<Self, String> {
        Self::open_with_serving(root, groups, routers, Vec::new())
    }
    pub fn open_with_serving(
        root: &FsPath,
        groups: Vec<Group>,
        routers: Vec<super::packing_router::RouterRegistration>,
        ingresses: Vec<String>,
    ) -> Result<Self, String> {
        let serving = if routers.is_empty() {
            None
        } else {
            Some(Arc::new(super::serving_control::ServingControl::open(
                root, routers, ingresses,
            )?))
        };
        fs::create_dir_all(root.join("public")).map_err(|e| e.to_string())?;
        validate_inventory(&groups)?;
        fs::create_dir_all(root.join("hints")).map_err(|e| e.to_string())?;
        fs::create_dir_all(root.join("snapshots")).map_err(|e| e.to_string())?;
        let mut store = Store::open(root)?;
        if store.state().pool.is_some() && serving.is_none() {
            return Err("pool state requires the remote serving configuration".into());
        }
        store.update(|s| {
            if !inventory_extends(&s.groups, &groups) {
                return Err("inventory must preserve all registered groups and endpoints".into());
            }
            if s.groups.is_empty() {
                s.groups = groups;
            }
            Ok(())
        })?;
        let packing_budget = super::packing_budget::PackingBudget::coordinator();
        let mut snapshots = Vec::new();
        let mut packing_cache = BTreeMap::new();
        for manifest in &store.state().published {
            snapshots.push(restore(
                root,
                manifest,
                &store.state().recovery.revoked,
                &mut packing_cache,
                serving.is_some(),
                &packing_budget,
            )?);
        }
        Ok(Self {
            packing_budget,
            serving,
            store: Arc::new(Mutex::new(store)),
            snapshots: Arc::new(RwLock::new(snapshots)),
            publication: Arc::new(Semaphore::new(1)),
            queries: Arc::new(Semaphore::new(QUERY_ACTIVE_LIMIT)),
            query_waiters: Arc::new(Semaphore::new(QUERY_WAIT_LIMIT)),
            query_stats: Arc::new(QueryStats::default()),
            http_metrics: super::http_metrics::HttpMetrics::default(),
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(180))
                .build()
                .map_err(|e| e.to_string())?,
            root: root.into(),
            blocked: Arc::new(Mutex::new(None)),
            telemetry: Arc::new(Mutex::new(super::telemetry::Publication::default())),
        })
    }

    /// Register only additional replica pairs. Existing placements and worker
    /// endpoints are immutable here; replacement/removal require a drain protocol.
    /// Readiness is a liveness check, not hardware qualification.
    pub async fn reconcile_inventory(&self, configured: Vec<Group>) -> Result<(), String> {
        validate_inventory(&configured)?;
        let _permit = self
            .publication
            .clone()
            .try_acquire_owned()
            .map_err(|_| "publication already in progress")?;
        let (existing, epoch, revision) = {
            let store = self.store.lock().unwrap();
            if !inventory_extends(&store.state().groups, &configured) {
                return Err("inventory must preserve all registered groups and endpoints".into());
            }
            if store.state().groups.len() == configured.len() {
                return Ok(());
            }
            if store.state().operation.is_some() {
                return Err("recover publication before registering capacity".into());
            }
            (
                store.state().groups.len(),
                store.state().epoch,
                store.state().revision,
            )
        };
        let mut additions = configured[existing..].to_vec();
        let mut incarnations = BTreeSet::new();
        for group in &mut additions {
            for replica in &mut group.replicas {
                let response = checked(
                    self.http
                        .get(format!("{}/internal/health", replica.url))
                        .timeout(std::time::Duration::from_secs(10))
                        .send()
                        .await
                        .map_err(|e| e.to_string())?,
                )
                .await?;
                let health: serde_json::Value =
                    serde_json::from_slice(&bounded(response, 65536).await?)
                        .map_err(|e| e.to_string())?;
                let incarnation = health["incarnation"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .ok_or("new replica has no process identity")?;
                if health["placement_policy"]
                    != serde_json::to_value(group.placement_policy).unwrap()
                    || health["protocol"].as_str() != Some(PROTOCOL_REVISION)
                    || health["published"].as_array().is_none_or(|s| !s.is_empty())
                    || health["candidate"] != serde_json::Value::Null
                    || health["epoch"].as_u64() != Some(0)
                    || health["revision"].as_u64() != Some(0)
                    || !incarnations.insert(incarnation.to_owned())
                {
                    return Err(
                        "new replica must be a distinct, idle process with fresh state".into(),
                    );
                }
                replica.incarnation = incarnation.to_owned();
                replica.ledger = control::Ledger::default();
            }
        }
        self.store.lock().unwrap().update(|state| {
            if state.epoch != epoch
                || state.revision != revision
                || state.operation.is_some()
                || state.groups.len() != existing
            {
                return Err("inventory changed during readiness checks".into());
            }
            state.revision = state
                .revision
                .checked_add(1)
                .ok_or("placement revision exhausted")?;
            state.groups.extend(additions);
            for request in state.capacity.requests.values_mut() {
                request.registered |= state.groups.len() >= request.target_groups;
            }
            state.capacity.requested = state
                .capacity
                .requests
                .values()
                .find(|r| !r.registered)
                .map(|r| r.id.clone());
            Ok(())
        })
    }

    /// One-way schema migration preserves current data and exact session identities.
    pub fn enable_pool(&self, frontier_replication: usize) -> Result<(), String> {
        if self.serving.is_none() {
            return Err("pool placement requires remote packing".into());
        }
        self.store.lock().unwrap().update(|state| {
            if state.operation.is_some()
                || !state.pending_commits.is_empty()
                || !state.pending_aborts.is_empty()
            {
                return Err("reconcile durable operations before enabling pool placement".into());
            }
            let placements = if let Some(pool) = &state.pool {
                pool.placements.clone()
            } else {
                state
                    .assignments
                    .iter()
                    .map(|(id, group)| {
                        let replicas = state
                            .groups
                            .iter()
                            .find(|g| &g.id == group)
                            .ok_or("unknown legacy group")?;
                        Ok((
                            *id,
                            replicas.replicas.iter().map(|r| r.name.clone()).collect(),
                        ))
                    })
                    .collect::<Result<_, String>>()?
            };
            let pool = crate::pool::Pool {
                replication: 2,
                frontier_replication,
                placements,
            };
            pool.validate(&state.groups)?;
            state.pool = Some(pool);
            state.version = 8;
            Ok(())
        })
    }

    async fn plan_pool(
        &self,
        coverage: &Coverage,
        groups: &[Group],
        plans: &[DomainPlan],
        pool: &crate::pool::Pool,
        consolidate: bool,
    ) -> Result<crate::pool::Placements, String> {
        let mut forbidden = BTreeSet::new();
        for name in self.pending_replicas() {
            for shard in &coverage.shards {
                forbidden.insert((shard.id, name.clone()));
            }
        }
        let workers: Vec<_> = groups.iter().flat_map(|g| &g.replicas).collect();
        for _ in 0..=workers.len() {
            let proposed = crate::pool::place(coverage, groups, pool, &forbidden)?;
            let mut rejected = false;
            for worker in &workers {
                let own: Vec<_> = plans
                    .iter()
                    .filter(|p| proposed[&p.shard.id].contains(&worker.name))
                    .collect();
                if own.is_empty() {
                    continue;
                }
                let response = self
                    .http
                    .post(format!("{}/internal/admit", worker.url))
                    .timeout(std::time::Duration::from_secs(3))
                    .json(&own)
                    .send()
                    .await;
                if !response.is_ok_and(|r| r.status() == StatusCode::NO_CONTENT) {
                    for shard in &coverage.shards {
                        forbidden.insert((shard.id, worker.name.clone()));
                    }
                    rejected = true;
                }
            }
            if rejected {
                continue;
            }
            if consolidate {
                let compact = crate::pool::consolidate(coverage, groups, pool, &proposed);
                if compact != proposed {
                    let mut admitted = true;
                    for worker in &workers {
                        let own: Vec<_> = plans
                            .iter()
                            .filter(|p| compact[&p.shard.id].contains(&worker.name))
                            .collect();
                        if own.is_empty() {
                            continue;
                        }
                        let response = self
                            .http
                            .post(format!("{}/internal/admit", worker.url))
                            .timeout(std::time::Duration::from_secs(3))
                            .json(&own)
                            .send()
                            .await;
                        if !response.is_ok_and(|r| r.status() == StatusCode::NO_CONTENT) {
                            admitted = false;
                            break;
                        }
                    }
                    if admitted {
                        return Ok(compact);
                    }
                }
            }
            return Ok(proposed);
        }
        Err("no admitted pool placement".into())
    }

    async fn assignment_admitted(
        &self,
        group: &Group,
        plans: &[DomainPlan],
        assignments: &BTreeMap<u64, String>,
    ) -> bool {
        let pending = self.pending_replicas();
        if group.replicas.len() != 2 || group.replicas.iter().any(|r| pending.contains(&r.name)) {
            return false;
        }
        let own: Vec<_> = plans
            .iter()
            .filter(|p| assignments.get(&p.shard.id) == Some(&group.id))
            .collect();
        for replica in &group.replicas {
            let admission = self
                .http
                .post(format!("{}/internal/admit", replica.url))
                .timeout(std::time::Duration::from_secs(3))
                .json(&own)
                .send()
                .await;
            if !admission.is_ok_and(|r| r.status() == StatusCode::NO_CONTENT) {
                return false;
            }
        }
        true
    }

    /// Try spare registered capacity for new domains without moving published
    /// domains. Preflight never reserves; actual reservation repeats admission.
    async fn place_new_if_admitted(
        &self,
        coverage: &Coverage,
        groups: &[Group],
        plans: &[DomainPlan],
        previous: &BTreeMap<u64, String>,
        mut assignments: BTreeMap<u64, String>,
    ) -> Result<(BTreeMap<u64, String>, BTreeSet<String>), String> {
        let mut strict = BTreeSet::new();
        let mut ordered: Vec<_> = groups.iter().collect();
        ordered.sort_by_key(|g| g.sequence);
        for shard in coverage
            .shards
            .iter()
            .filter(|s| !previous.contains_key(&s.id))
        {
            let source = assignments
                .get(&shard.id)
                .ok_or("new shard unassigned")?
                .clone();
            let preferred = groups
                .iter()
                .find(|g| g.id == source)
                .ok_or("unknown placement group")?;
            if self
                .assignment_admitted(preferred, plans, &assignments)
                .await
            {
                continue;
            }
            for destination in &ordered {
                if destination.id == source {
                    continue;
                }
                let mut proposed = assignments.clone();
                proposed.insert(shard.id, destination.id.clone());
                if destination.role(coverage, &proposed).is_err()
                    || !self
                        .assignment_admitted(destination, plans, &proposed)
                        .await
                {
                    continue;
                }
                strict.insert(destination.id.clone());
                assignments = proposed;
                break;
            }
        }
        Ok((assignments, strict))
    }

    /// Relocate a published shard only after explicit memory refusal and loss of
    /// required admission quorum. Busy or unreachable replicas alone are not a
    /// placement signal. Retained source snapshots remain charged until expiry.
    async fn relocate_if_memory_refused(
        &self,
        coverage: &Coverage,
        groups: &[Group],
        plans: &[DomainPlan],
        previous: &BTreeMap<u64, String>,
        mut assignments: BTreeMap<u64, String>,
    ) -> BTreeMap<u64, String> {
        let pending = self.pending_replicas();
        let mut ordered: Vec<_> = groups.iter().collect();
        ordered.sort_by_key(|g| g.sequence);
        for source in &ordered {
            let required_pairs = control::required_destination_pairs(&assignments, previous);
            let own: Vec<_> = plans
                .iter()
                .filter(|p| assignments.get(&p.shard.id) == Some(&source.id))
                .collect();
            if own.is_empty() {
                continue;
            }
            let mut refused = false;
            let mut admitted = 0;
            for replica in &source.replicas {
                if pending.contains(&replica.name) {
                    continue;
                }
                if let Ok(response) = self
                    .http
                    .post(format!("{}/internal/admit", replica.url))
                    .timeout(std::time::Duration::from_secs(3))
                    .json(&own)
                    .send()
                    .await
                {
                    refused |= response.status() == StatusCode::INSUFFICIENT_STORAGE;
                    admitted += usize::from(response.status() == StatusCode::NO_CONTENT);
                }
            }
            let quorum = if required_pairs.contains(&source.id) {
                2
            } else {
                1
            };
            if !refused || admitted >= quorum {
                continue;
            }
            // Try a whole-shard move. Both complete post-move assignments must
            // fit, including retained data. If no such move fits, reservation
            // preserves the existing failure path and requests more capacity.
            'search: for shard in &coverage.shards {
                if assignments.get(&shard.id) != Some(&source.id)
                    || !previous.contains_key(&shard.id)
                {
                    continue;
                }
                for destination in &ordered {
                    if destination.id == source.id {
                        continue;
                    }
                    let mut proposed = assignments.clone();
                    proposed.insert(shard.id, destination.id.clone());
                    if destination.role(coverage, &proposed).is_err()
                        || !self
                            .assignment_admitted(destination, plans, &proposed)
                            .await
                    {
                        continue;
                    }
                    if proposed.values().any(|id| id == &source.id)
                        && !self.assignment_admitted(source, plans, &proposed).await
                    {
                        continue;
                    }
                    assignments = proposed;
                    break 'search;
                }
            }
        }
        assignments
    }

    /// Elective moves must not hold the canonical update behind a known lack of
    /// destination capacity. Preflight has no side effects; reservation still
    /// repeats admission and both destination replicas remain mandatory.
    async fn consolidate_if_admitted(
        &self,
        coverage: &Coverage,
        groups: &[Group],
        plans: &[DomainPlan],
        original: BTreeMap<u64, String>,
    ) -> Result<(BTreeMap<u64, String>, BTreeSet<String>), String> {
        let mut proposed = original.clone();
        let destinations = control::consolidate(coverage, groups, &mut proposed)?;
        for group in groups.iter().filter(|g| destinations.contains(&g.id)) {
            if !self.assignment_admitted(group, plans, &proposed).await {
                tracing::info!(group = %group.id,
                    "elective consolidation deferred; preserving canonical update placement");
                return Ok((original, BTreeSet::new()));
            }
        }
        Ok((proposed, destinations))
    }

    pub fn artifact_router(&self) -> Router {
        super::serving_control::ServingControl::artifact_router(self.root.clone())
    }

    pub async fn refresh_routers(&self) {
        if let Some(serving) = &self.serving {
            if let Err(error) = serving.refresh().await {
                tracing::debug!(%error, "packing control refresh unavailable");
            }
        }
    }

    fn serving_view(
        &self,
        saved: &SavedSnapshot,
    ) -> Result<super::packing_router::ServingSnapshot, String> {
        let serving = self.serving.as_ref().ok_or("local packing")?;
        let revoked = self.store.lock().unwrap().state().recovery.revoked.clone();
        let mut artifacts = BTreeMap::new();
        let mut routes = saved.routes.clone();
        for (id, name) in &saved.hints {
            if revoked.contains(&hex::encode(saved.manifest.session_id(*id)?)) {
                routes.remove(id);
                continue;
            }
            artifacts.insert(*id, (name.clone(), serving.artifact_hash(name)?));
        }
        Ok(super::packing_router::ServingSnapshot {
            manifest: saved.manifest.clone(),
            routes,
            artifacts,
        })
    }

    async fn prepare_routers(
        &self,
        candidate: Option<&SavedSnapshot>,
    ) -> Result<
        Vec<(
            super::packing_router::RouterRegistration,
            super::packing_router::Ack,
        )>,
        String,
    > {
        let Some(serving) = &self.serving else {
            return Ok(Vec::new());
        };
        let mut views = Vec::new();
        if let Some(saved) = candidate {
            views.push(self.serving_view(saved)?);
        }
        for snapshot in self.snapshots.read().await.iter() {
            if candidate.is_none_or(|c| c.manifest.generation != snapshot.saved.manifest.generation)
            {
                views.push(self.serving_view(&snapshot.saved)?);
            }
        }
        views.truncate(RETAINED_GENERATIONS);
        if views.is_empty() {
            return Ok(Vec::new());
        }
        let (epoch, fence) = {
            let store = self.store.lock().unwrap();
            (
                store.state().epoch,
                worker::Revocation {
                    recovery_epoch: store.state().recovery.epoch,
                    sessions: store.state().recovery.revoked.clone(),
                },
            )
        };
        serving.prepare(epoch, views, fence).await
    }

    async fn synchronize_routers(&self) -> Result<(), String> {
        if let Some(serving) = &self.serving {
            if let Some(manifest) = self.manifest().await {
                if !serving.public_ready(manifest.generation) {
                    let prepared = self.prepare_routers(None).await?;
                    serving.activate(manifest.generation, prepared).await?;
                }
            }
        }
        Ok(())
    }

    pub fn router(self) -> Router {
        self.http_metrics.endpoint("init");
        self.http_metrics.endpoint("query");
        Router::new()
            .route("/v1/enhance/init", get(init))
            .route("/v1/enhance/sessions/:generation/:shard", get(session))
            .route("/v1/enhance/session/:session_id", get(session_by_id))
            .route("/v1/enhance/query", post(query))
            .route("/v1/health", get(health))
            .route("/ready", get(ready))
            .route("/metrics", get(metrics))
            .layer(axum::middleware::from_fn_with_state(
                self.http_metrics.clone(),
                super::http_metrics::measure,
            ))
            .with_state(self)
    }

    pub async fn manifest(&self) -> Option<Manifest> {
        self.snapshots
            .read()
            .await
            .first()
            .map(|s| s.saved.manifest.clone())
    }

    async fn synchronize_retention(&self) {
        let (epoch, groups, generations, pending) = {
            let store = self.store.lock().unwrap();
            (
                store.state().epoch,
                store.state().groups.clone(),
                store
                    .state()
                    .published
                    .iter()
                    .map(|m| m.generation)
                    .collect::<Vec<_>>(),
                store.state().pending_replicas(),
            )
        };
        if generations.is_empty() {
            return;
        }
        for group in groups {
            for replica in group.replicas {
                if pending.contains(&replica.name) {
                    continue;
                }
                let response = self
                    .http
                    .post(format!("{}/internal/retain", replica.url))
                    .timeout(std::time::Duration::from_secs(3))
                    .json(&worker::Retain {
                        epoch,
                        generations: generations.clone(),
                    })
                    .send()
                    .await;
                if !response.is_ok_and(|r| r.status().is_success()) {
                    tracing::warn!(replica = %replica.name, "retention synchronization deferred; worker continues charging old runtimes");
                }
            }
        }
    }
}

async fn checked(response: reqwest::Response) -> Result<reqwest::Response, String> {
    if response.status().is_success() {
        Ok(response)
    } else {
        let status = response.status();
        let reason = bounded(response, 4096).await.unwrap_or_default();
        Err(format!(
            "worker returned {status}: {}",
            String::from_utf8_lossy(&reason)
        ))
    }
}

async fn bounded(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>, String> {
    if response.content_length().is_some_and(|n| n > limit as u64) {
        return Err("oversized worker response".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
        if chunk.len() > limit - bytes.len() {
            return Err("oversized worker response".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

type ApiResult<T> = Result<T, (StatusCode, String)>;
type QueryResult<T> = Result<T, QueryError>;

async fn query_body(request: Request) -> QueryResult<axum::body::Bytes> {
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        to_bytes(request.into_body(), QUERY_BODY_LIMIT),
    )
    .await
    .map_err(|_| QueryError(StatusCode::REQUEST_TIMEOUT, "body deadline".into()))?
    .map_err(|e| QueryError(StatusCode::PAYLOAD_TOO_LARGE, e.to_string()))
}

async fn reject_query(c: &Coordinator, request: Request) -> QueryResult<Response> {
    // Finish reading the bounded body before responding; otherwise the public
    // reverse proxy can be left writing to a closed upstream connection.
    let _ = query_body(request).await?;
    c.query_stats.rejected.fetch_add(1, Ordering::Relaxed);
    Err(QueryError(
        StatusCode::TOO_MANY_REQUESTS,
        "query limit".into(),
    ))
}

async fn init(State(c): State<Coordinator>) -> ApiResult<Json<Manifest>> {
    let manifest = c.manifest().await;
    if c.serving.as_ref().is_some_and(|s| {
        manifest
            .as_ref()
            .is_none_or(|m| !s.public_ready(m.generation))
    }) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "packing routers not activated".into(),
        ));
    }
    manifest.map(Json).ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "no published generation".into(),
    ))
}
async fn session(
    State(c): State<Coordinator>,
    Path((generation, shard)): Path<(u64, u64)>,
) -> ApiResult<Json<ShardSession>> {
    let snapshots = c.snapshots.read().await;
    let s = snapshots
        .iter()
        .find(|s| s.saved.manifest.generation == generation)
        .ok_or((StatusCode::GONE, "expired session".into()))?;
    let id = s
        .saved
        .manifest
        .session_id(shard)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    if c.store
        .lock()
        .unwrap()
        .state()
        .recovery
        .revoked
        .contains(&hex::encode(id))
    {
        return Err((StatusCode::GONE, "noncanonical_session".into()));
    }
    let pack = s
        .packing
        .get(&shard)
        .ok_or((StatusCode::BAD_REQUEST, "wrong shard".into()))?;
    Ok(Json(pack.session(&s.saved.manifest, shard)))
}
async fn session_by_id(
    State(c): State<Coordinator>,
    Path(id): Path<String>,
) -> QueryResult<Json<ShardSession>> {
    if !enhance_pir::protocol::canonical_hash(&id) {
        return Err(QueryError(
            StatusCode::BAD_REQUEST,
            "invalid session id".into(),
        ));
    }
    let snapshots = c.snapshots.read().await;
    if c.store
        .lock()
        .unwrap()
        .state()
        .recovery
        .revoked
        .contains(&id)
    {
        return Err(QueryError(StatusCode::GONE, "noncanonical_session".into()));
    }
    for snapshot in snapshots.iter() {
        let manifest = &snapshot.saved.manifest;
        for shard in &manifest.coverage.shards {
            if manifest
                .session_id(shard.id)
                .is_ok_and(|hash| hex::encode(hash) == id)
            {
                return Ok(Json(
                    snapshot.packing[&shard.id].session(manifest, shard.id),
                ));
            }
        }
    }
    Err(QueryError(StatusCode::GONE, "session_unavailable".into()))
}

async fn query(State(c): State<Coordinator>, request: Request) -> QueryResult<Response> {
    if c.serving.is_some() {
        return Err(QueryError(
            StatusCode::SERVICE_UNAVAILABLE,
            "query serving moved to packing router".into(),
        ));
    }
    let waiting = match c.query_waiters.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return reject_query(&c, request).await,
    };
    let started_wait = std::time::Instant::now();
    let acquired =
        tokio::time::timeout(QUERY_WAIT_DEADLINE, c.queries.clone().acquire_owned()).await;
    c.query_stats.wait_count.fetch_add(1, Ordering::Relaxed);
    c.query_stats
        .wait_micros
        .fetch_add(started_wait.elapsed().as_micros() as u64, Ordering::Relaxed);
    drop(waiting);
    let permit = match acquired {
        Ok(Ok(permit)) => permit,
        _ => return reject_query(&c, request).await,
    };
    let body = query_body(request).await?;
    let binding = QueryBinding::decode(&body).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let generation_pin = c.snapshots.clone().read_owned().await;
    if c.store
        .lock()
        .unwrap()
        .state()
        .recovery
        .revoked
        .contains(&hex::encode(binding.session_id))
    {
        return Err(QueryError(StatusCode::GONE, "noncanonical_session".into()));
    }
    let snapshot = generation_pin
        .first()
        .cloned()
        .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no routing view".into()))?;
    let manifest = &snapshot.saved.manifest;
    super::query_serving::validate_binding(manifest, binding)?;
    let pack = snapshot
        .packing
        .get(&binding.shard_id)
        .cloned()
        .ok_or((StatusCode::BAD_REQUEST, "wrong shard".into()))?;
    // The admitted task owns capacity even if its HTTP caller disconnects.
    super::query_serving::admitted(async move {
        let coefficients = pack
            .query_coefficients(&body, binding)
            .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
        let request = worker::Evaluate {
            binding: Some(binding.encode()),
            generation: binding.generation,
            shard_id: binding.shard_id,
            epoch: hex::encode(binding.epoch),
            session_id: hex::encode(binding.session_id),
            coefficients,
        };
        let routes = snapshot
            .saved
            .routes
            .get(&binding.shard_id)
            .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no replica route".into()))?;
        let answer = evaluate_query(&c, &request, routes).await?;
        let (response, guards) = tokio::task::spawn_blocking(move || {
            let response = pack.pack(&body, &answer)?;
            Ok::<_, String>((response, (permit, snapshot, generation_pin, pack)))
        })
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
        guarded_query_response(c, hex::encode(binding.session_id), response, guards)
    })
    .await
    .map_err(QueryError::from)?
}

async fn evaluate_query(
    c: &Coordinator,
    request: &worker::Evaluate,
    routes: &[String],
) -> QueryResult<Vec<u64>> {
    if routes.is_empty() {
        return Err(QueryError(
            StatusCode::SERVICE_UNAVAILABLE,
            "no replica route".into(),
        ));
    }
    let first = c.query_stats.route_cursor.fetch_add(1, Ordering::Relaxed) as usize % routes.len();
    let mut last_busy = false;
    let answer = super::query_serving::evaluate_observed(
        &c.http,
        request,
        routes.len(),
        |excluded| {
            let url = (0..routes.len())
                .map(|offset| &routes[(first + offset) % routes.len()])
                .find(|url| Some(url.as_str()) != excluded)
                .ok_or((StatusCode::SERVICE_UNAVAILABLE, "no replica route".into()))?;
            Ok((url.clone(), ()))
        },
        |failure| {
            last_busy = matches!(failure, super::query_serving::AttemptFailure::Busy);
            if last_busy {
                c.query_stats.worker_busy.fetch_add(1, Ordering::Relaxed);
            } else {
                c.query_stats.worker_failure.fetch_add(1, Ordering::Relaxed);
            }
        },
    )
    .await
    .map_err(|error| {
        // Preserve the compatibility endpoint's worker-overload response, without
        // replaying evaluations whose acceptance is unknown.
        if last_busy {
            c.query_stats.rejected.fetch_add(1, Ordering::Relaxed);
            QueryError(StatusCode::TOO_MANY_REQUESTS, "workers not admitted".into())
        } else {
            QueryError::from(error)
        }
    })?;
    if answer.attempts == 1 {
        c.query_stats
            .primary_success
            .fetch_add(1, Ordering::Relaxed);
    } else {
        c.query_stats
            .fallback_success
            .fetch_add(1, Ordering::Relaxed);
    }
    Ok(answer.coefficients)
}

fn guarded_query_response<G: Send + Unpin + 'static>(
    c: Coordinator,
    session: String,
    response: Vec<u8>,
    guards: G,
) -> QueryResult<Response> {
    let fence = move || {
        if c.store
            .lock()
            .unwrap()
            .state()
            .recovery
            .revoked
            .contains(&session)
        {
            Err("noncanonical_session".to_string())
        } else {
            Ok(())
        }
    };
    fence().map_err(|e| (StatusCode::GONE, e))?;
    Ok(super::response_body::guarded(response, guards, fence).into_response())
}

async fn health(State(c): State<Coordinator>) -> Json<serde_json::Value> {
    let snapshots = c.snapshots.read().await;
    let manifest = snapshots.first().map(|s| &s.saved.manifest);
    let store = c.store.lock().unwrap();
    let mut published_replica_counts = BTreeMap::new();
    if let Some(snapshot) = snapshots.first() {
        for (shard, routes) in &snapshot.saved.routes {
            if let Some(group) = store.state().assignments.get(shard) {
                published_replica_counts
                    .entry(group.clone())
                    .and_modify(|n: &mut usize| *n = (*n).min(routes.len()))
                    .or_insert(routes.len());
            }
        }
    }
    Json(
        serde_json::json!({"packing_charged_bytes":c.packing_budget.charged_bytes(),"protocol":PROTOCOL_REVISION,"generation":manifest.as_ref().map(|m|m.generation),
        "anchor_height":manifest.as_ref().map(|m|m.anchor_height),"placement_revision":manifest.as_ref().map(|m|m.placement_revision),
        "registered_groups":store.state().groups.len(),
        "registered_workers":store.state().groups.iter().map(|g|g.replicas.len()).sum::<usize>(),
        "pool":store.state().pool,
        "remote_packing":c.serving.is_some(),
        "resident_packing_objects":snapshots.iter().flat_map(|s|s.packing.values()).filter(|p|p.is_serving()).map(|p|Arc::as_ptr(p) as usize).collect::<BTreeSet<_>>().len(),
        "packing_ready":c.serving.as_ref().is_none_or(|s|manifest.as_ref().is_some_and(|m|s.public_ready(m.generation))),
        "capacity":store.state().capacity,
        "published_replica_counts":published_replica_counts,
        "pending_commit_notifications":store.state().pending_commits.len(),
        "pending_abort_notifications":store.state().pending_aborts.len(),
        "operation":store.state().operation,"blocked_reason":*c.blocked.lock().unwrap(),
        "ingestion_error":c.telemetry.lock().unwrap().ingestion_error}),
    )
}
async fn ready(State(c): State<Coordinator>) -> StatusCode {
    if c.manifest().await.is_some() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn metrics(
    State(c): State<Coordinator>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    let snapshots = c.snapshots.read().await;
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
    let store = c.store.lock().unwrap();
    let blocked = c.blocked.lock().unwrap().is_some();
    let telemetry = c.telemetry.lock().unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut body =
        super::telemetry::coordinator(store.state(), manifest, &routes, blocked, &telemetry, now);
    body.push_str(&c.http_metrics.render());
    for (name, value) in [
        (
            "enhance_query_active",
            (QUERY_ACTIVE_LIMIT - c.queries.available_permits()) as u64,
        ),
        (
            "enhance_query_waiting",
            (QUERY_WAIT_LIMIT - c.query_waiters.available_permits()) as u64,
        ),
        (
            "enhance_query_wait_count_total",
            c.query_stats.wait_count.load(Ordering::Relaxed),
        ),
        (
            "enhance_query_wait_microseconds_total",
            c.query_stats.wait_micros.load(Ordering::Relaxed),
        ),
        (
            "enhance_query_rejected_total",
            c.query_stats.rejected.load(Ordering::Relaxed),
        ),
        (
            "enhance_query_primary_success_total",
            c.query_stats.primary_success.load(Ordering::Relaxed),
        ),
        (
            "enhance_query_fallback_success_total",
            c.query_stats.fallback_success.load(Ordering::Relaxed),
        ),
        (
            "enhance_query_worker_busy_total",
            c.query_stats.worker_busy.load(Ordering::Relaxed),
        ),
        (
            "enhance_query_worker_failure_total",
            c.query_stats.worker_failure.load(Ordering::Relaxed),
        ),
    ] {
        let kind = if name.ends_with("_total") {
            "counter"
        } else {
            "gauge"
        };
        body.push_str(&format!("# TYPE {name} {kind}\n{name} {value}\n"));
    }
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

#[cfg(test)]
mod admission_tests {
    fn test_coordinator() -> super::Coordinator {
        let root = tempfile::tempdir().unwrap();
        // The coordinator only needs a valid inventory for admission tests.
        let group = Group {
            placement_policy: Default::default(),
            id: "g0".into(),
            sequence: 0,

            replicas: (0..2)
                .map(|i| control::Replica {
                    name: format!("r{i}"),
                    url: format!("http://127.0.0.1:{}", 9100 + i),
                    incarnation: String::new(),
                    ledger: Default::default(),
                })
                .collect(),
        };
        Coordinator::open(root.path(), vec![group]).unwrap()
    }

    #[tokio::test]
    async fn poisoned_journal_cannot_publish_even_empty_coverage() {
        let coordinator = test_coordinator();
        let dir = tempfile::tempdir().unwrap();
        let mut journal = crate::store::RecordJournal::open(
            dir.path(),
            crate::types::DatabaseId::Enhance,
            crate::types::ENHANCE_LAYOUT,
        )
        .unwrap();
        std::fs::remove_file(dir.path().join("records.bin")).unwrap();
        assert!(journal
            .append_block(10, "aa".into(), &[] as &[Vec<u8>])
            .is_err());
        let before = coordinator.store.lock().unwrap().state().revision;
        let error = coordinator
            .publish_checked(&journal, 10, "aa".into(), || async {
                panic!("validation must not run for a poisoned journal");
            })
            .await
            .unwrap_err();
        assert!(error.contains("drop and reopen"));
        assert_eq!(coordinator.store.lock().unwrap().state().revision, before);
        assert!(coordinator
            .telemetry
            .lock()
            .unwrap()
            .last_attempt_seconds
            .is_none());
        assert!(coordinator.manifest().await.is_none());
    }

    fn empty_query() -> axum::extract::Request {
        axum::http::Request::builder()
            .uri("/v1/enhance/query")
            .body(axum::body::Body::empty())
            .unwrap()
    }

    async fn occupy_query_slots(
        coordinator: &Coordinator,
    ) -> Vec<tokio::sync::OwnedSemaphorePermit> {
        let mut permits = Vec::new();
        for _ in 0..QUERY_ACTIVE_LIMIT {
            permits.push(coordinator.queries.clone().acquire_owned().await.unwrap());
        }
        permits
    }

    #[tokio::test]
    async fn queue_waits_for_a_slot_and_releases_it() {
        let coordinator = test_coordinator();
        let mut active = occupy_query_slots(&coordinator).await;
        let waiting = tokio::spawn(query(State(coordinator.clone()), empty_query()));
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(
            coordinator.query_waiters.available_permits(),
            QUERY_WAIT_LIMIT - 1
        );
        drop(active.pop());
        let error = waiting.await.unwrap().unwrap_err();
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert_eq!(
            coordinator.query_waiters.available_permits(),
            QUERY_WAIT_LIMIT
        );
        drop(active);
    }

    #[tokio::test]
    async fn full_queue_and_expired_wait_return_retryable_429() {
        let coordinator = test_coordinator();
        let _active = occupy_query_slots(&coordinator).await;
        let mut waiters = Vec::new();
        for _ in 0..QUERY_WAIT_LIMIT {
            waiters.push(tokio::spawn(query(
                State(coordinator.clone()),
                empty_query(),
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(coordinator.query_waiters.available_permits(), 0);
        let response = query(State(coordinator.clone()), empty_query())
            .await
            .unwrap_err()
            .into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
        for waiter in waiters {
            let response = waiter.await.unwrap().unwrap_err().into_response();
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        }
        assert_eq!(
            coordinator.query_waiters.available_permits(),
            QUERY_WAIT_LIMIT
        );
    }

    #[tokio::test]
    async fn cancelled_waiter_releases_capacity() {
        let coordinator = test_coordinator();
        let _active = occupy_query_slots(&coordinator).await;
        let waiting = tokio::spawn(query(State(coordinator.clone()), empty_query()));
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(
            coordinator.query_waiters.available_permits(),
            QUERY_WAIT_LIMIT - 1
        );
        waiting.abort();
        let _ = waiting.await;
        assert_eq!(
            coordinator.query_waiters.available_permits(),
            QUERY_WAIT_LIMIT
        );
    }

    #[tokio::test]
    async fn compatibility_evaluation_preserves_attempt_metrics_without_ambiguous_replay() {
        let coordinator = test_coordinator();
        let request = worker::Evaluate {
            binding: Some(vec![7]),
            generation: 1,
            shard_id: 0,
            epoch: "epoch".into(),
            session_id: "session".into(),
            coefficients: vec![1],
        };
        for explicit in [true, false] {
            let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let second = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let routes = vec![
                format!("http://{}", first.local_addr().unwrap()),
                format!("http://{}", second.local_addr().unwrap()),
            ];
            let second_calls = Arc::new(AtomicU64::new(0));
            let calls = second_calls.clone();
            let busy = Router::new().route(
                "/internal/evaluate",
                post(move || async move {
                    let mut response = StatusCode::TOO_MANY_REQUESTS.into_response();
                    if explicit {
                        response
                            .headers_mut()
                            .insert("x-enhance-evaluation", "not-accepted".parse().unwrap());
                    }
                    response
                }),
            );
            let ready = Router::new().route(
                "/internal/evaluate",
                post(move || {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::Relaxed);
                        Json(worker::Intermediate {
                            binding: Some(vec![7]),
                            generation: 1,
                            shard_id: 0,
                            epoch: "epoch".into(),
                            coefficients: vec![9],
                        })
                    }
                }),
            );
            let a = tokio::spawn(async move { axum::serve(first, busy).await.unwrap() });
            let b = tokio::spawn(async move { axum::serve(second, ready).await.unwrap() });
            coordinator
                .query_stats
                .route_cursor
                .store(0, Ordering::Relaxed);
            let before_busy = coordinator.query_stats.worker_busy.load(Ordering::Relaxed);
            let before_fallback = coordinator
                .query_stats
                .fallback_success
                .load(Ordering::Relaxed);
            let before_rejected = coordinator.query_stats.rejected.load(Ordering::Relaxed);
            let result = evaluate_query(&coordinator, &request, &routes).await;
            assert_eq!(
                coordinator.query_stats.worker_busy.load(Ordering::Relaxed),
                before_busy + 1
            );
            if explicit {
                assert_eq!(result.unwrap(), vec![9]);
                assert_eq!(
                    coordinator
                        .query_stats
                        .fallback_success
                        .load(Ordering::Relaxed),
                    before_fallback + 1
                );
                assert_eq!(second_calls.load(Ordering::Relaxed), 1);
            } else {
                assert_eq!(result.unwrap_err().0, StatusCode::TOO_MANY_REQUESTS);
                assert_eq!(
                    coordinator.query_stats.rejected.load(Ordering::Relaxed),
                    before_rejected + 1
                );
                assert_eq!(second_calls.load(Ordering::Relaxed), 0);
            }
            a.abort();
            b.abort();
        }
    }

    #[tokio::test]
    async fn compatibility_response_holds_admission_and_rechecks_durable_revocation() {
        let inventory = test_coordinator()
            .store
            .lock()
            .unwrap()
            .state()
            .groups
            .clone();
        let root = tempfile::tempdir().unwrap();
        let coordinator = Coordinator::open(root.path(), inventory).unwrap();
        let permit = coordinator.queries.clone().acquire_owned().await.unwrap();
        let response =
            guarded_query_response(coordinator.clone(), "session".into(), vec![7], permit).unwrap();
        assert_eq!(
            coordinator.queries.available_permits(),
            QUERY_ACTIVE_LIMIT - 1
        );
        coordinator
            .store
            .lock()
            .unwrap()
            .update(|state| {
                state.recovery.revoked.insert("session".into());
                Ok(())
            })
            .unwrap();
        assert!(to_bytes(response.into_body(), 10).await.is_err());
        assert_eq!(coordinator.queries.available_permits(), QUERY_ACTIVE_LIMIT);
        let rejected =
            guarded_query_response(coordinator, "session".into(), vec![7], ()).unwrap_err();
        assert_eq!(rejected.0, StatusCode::GONE);
    }

    #[test]
    fn restart_rejects_changed_placement_policy() {
        let root = tempfile::tempdir().unwrap();
        let group = Group {
            placement_policy: control::PlacementPolicy { sealed_shards: 7 },
            id: "g0".into(),
            sequence: 0,

            replicas: (0..2)
                .map(|i| control::Replica {
                    name: format!("r{i}"),
                    url: format!("http://127.0.0.1:{}", 9100 + i),
                    incarnation: String::new(),
                    ledger: Default::default(),
                })
                .collect(),
        };
        drop(Coordinator::open(root.path(), vec![group.clone()]).unwrap());
        let mut changed = group.clone();
        changed.placement_policy.sealed_shards = 6;
        assert!(Coordinator::open(root.path(), vec![changed]).is_err());
        assert!(Coordinator::open(root.path(), vec![group]).is_ok());
    }

    #[tokio::test]
    async fn preparation_outlives_default_http_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = axum::Router::new().route(
            "/internal/prepare",
            axum::routing::post(|| async {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                axum::http::StatusCode::OK
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(10))
            .build()
            .unwrap();
        let ordinary = http
            .post(format!("http://{address}/internal/prepare"))
            .send()
            .await
            .unwrap_err();
        assert!(ordinary.is_timeout());
        let result = super::preparation_request(&http, &format!("http://{address}"))
            .send()
            .await;
        server.abort();
        assert_eq!(result.unwrap().status(), axum::http::StatusCode::OK);
    }

    use super::*;
    use axum::response::IntoResponse;
    use sha2::{Digest, Sha256};
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn reorg_destination_memory_refusal_replans_to_complete_spare_pair() {
        let root = tempfile::tempdir().unwrap();
        let memory_refusal = Arc::new(AtomicBool::new(true));
        let mut groups = Vec::new();
        let mut tasks = Vec::new();
        for sequence in 0..3 {
            let mut replicas = Vec::new();
            for index in 0..2 {
                let refuse = memory_refusal.clone();
                let router = Router::new().route(
                    "/internal/admit",
                    post(move || {
                        let refuse = refuse.clone();
                        async move {
                            if sequence == 1 && index == 1 {
                                if refuse.load(Ordering::SeqCst) {
                                    StatusCode::INSUFFICIENT_STORAGE
                                } else {
                                    StatusCode::SERVICE_UNAVAILABLE
                                }
                            } else {
                                StatusCode::NO_CONTENT
                            }
                        }
                    }),
                );
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let url = format!("http://{}", listener.local_addr().unwrap());
                tasks.push(tokio::spawn(async move {
                    axum::serve(listener, router).await.unwrap();
                }));
                replicas.push(control::Replica {
                    name: format!("g{sequence}-r{index}"),
                    url,
                    incarnation: String::new(),
                    ledger: control::Ledger::default(),
                });
            }
            groups.push(Group {
                placement_policy: Default::default(),
                id: format!("g{sequence}"),
                sequence,
                replicas,
            });
        }
        let coordinator = Coordinator::open(&root.path().join("control"), groups.clone()).unwrap();
        let mut lifecycle = enhance_pir::protocol::Lifecycle::default();
        let old = lifecycle
            .coverage((6 * 32768 + 4096) * 33, Geometry::default())
            .unwrap();
        let previous: BTreeMap<_, _> = old
            .shards
            .iter()
            .enumerate()
            .map(|(i, s)| (s.id, if i < 6 { "g0" } else { "g1" }.to_owned()))
            .collect();
        let coverage = lifecycle
            .coverage((5 * 32768 + 4096) * 33, Geometry::default())
            .unwrap();
        let planned = control::assign(&coverage, &groups, &previous).unwrap();
        let frontier = coverage.shards.last().unwrap().id;
        assert_eq!(previous[&frontier], "g0");
        assert_eq!(planned[&frontier], "g1");
        // Valid plan geometry and identities, but no materialized data: these
        // endpoints model admission outcomes, not measured memory qualification.
        let plans: Vec<_> = coverage
            .shards
            .iter()
            .map(|shard| DomainPlan {
                shard: shard.clone(),
                units: shard
                    .units
                    .iter()
                    .map(|u| UnitIdentity {
                        recovery_epoch: 0,
                        table: "enhance".into(),
                        shard_id: shard.id,
                        local_row_start: u.local_row_start,
                        allocated_rows: u.allocated_rows,
                        setup_sha256: hex::encode(Sha256::digest(setup_seed(shard.id))),
                        parameter_id: unit_parameter_id(u.allocated_rows).unwrap(),
                        content_sha256: "00".repeat(32),
                    })
                    .collect(),
            })
            .collect();
        let relocated = coordinator
            .relocate_if_memory_refused(&coverage, &groups, &plans, &previous, planned.clone())
            .await;
        assert_eq!(
            relocated[&frontier], "g2",
            "one admitting replica cannot satisfy a moved shard's pair quorum"
        );
        assert_eq!(
            control::required_destination_pairs(&relocated, &previous),
            ["g2".to_owned()].into()
        );
        for (shard, group) in &planned {
            if *shard != frontier {
                assert_eq!(&relocated[shard], group);
            }
        }
        memory_refusal.store(false, Ordering::SeqCst);
        assert_eq!(
            coordinator
                .relocate_if_memory_refused(&coverage, &groups, &plans, &previous, planned.clone())
                .await,
            planned,
            "unavailable peer alone must not be interpreted as memory pressure"
        );
        memory_refusal.store(true, Ordering::SeqCst);
        let mut unavailable = groups.clone();
        unavailable[2].replicas.pop();
        assert_eq!(
            coordinator
                .relocate_if_memory_refused(
                    &coverage,
                    &unavailable,
                    &plans,
                    &previous,
                    planned.clone()
                )
                .await,
            planned,
            "an incomplete spare pair cannot receive the reorg shard"
        );
        for task in tasks {
            task.abort();
        }
    }

    #[tokio::test]
    async fn elective_consolidation_requires_both_admissions_and_does_not_reserve() {
        let root = tempfile::tempdir().unwrap();
        let reject_peer = Arc::new(AtomicBool::new(true));
        let mut tasks = Vec::new();
        let mut groups = Vec::new();
        let mut original_files = Vec::new();
        for sequence in 0..2 {
            let mut replicas = Vec::new();
            for index in 0..2 {
                let name = format!("g{sequence}-r{index}");
                let path = root.path().join(&name);
                let worker = worker::Worker::open(&path).unwrap();
                let state_file = path.join("worker.json");
                original_files.push((state_file.clone(), fs::read(state_file).ok()));
                let reject = reject_peer.clone();
                let router = worker.router().layer(axum::middleware::from_fn(
                    move |request: Request, next: axum::middleware::Next| {
                        let reject = reject.clone();
                        async move {
                            if sequence == 0
                                && index == 1
                                && request.uri().path() == "/internal/admit"
                                && reject.load(Ordering::SeqCst)
                            {
                                return StatusCode::SERVICE_UNAVAILABLE.into_response();
                            }
                            next.run(request).await
                        }
                    },
                ));
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let url = format!("http://{}", listener.local_addr().unwrap());
                tasks.push(tokio::spawn(async move {
                    axum::serve(listener, router).await.unwrap();
                }));
                replicas.push(control::Replica {
                    name,
                    url,
                    incarnation: String::new(),
                    ledger: control::Ledger::default(),
                });
            }
            groups.push(Group {
                placement_policy: Default::default(),
                id: format!("g{sequence}"),
                sequence,
                replicas,
            });
        }
        let coordinator = Coordinator::open(&root.path().join("control"), groups.clone()).unwrap();
        let mut coverage = Lifecycle::default()
            .coverage((6 * 32768 + 4096) * 33, Geometry::default())
            .unwrap();
        let original: BTreeMap<_, _> = coverage
            .shards
            .iter()
            .enumerate()
            .map(|(i, s)| (s.id, if i < 5 { "g0" } else { "g1" }.to_owned()))
            .collect();
        // Full-sized plan metadata is enough to exercise the real admission model;
        // these hashes do not stand for materialized or qualified databases.
        for shard in &mut coverage.shards {
            if shard.state == enhance_pir::protocol::ShardState::Provisional {
                shard.state = enhance_pir::protocol::ShardState::Sealed;
            }
        }
        let plans: Vec<_> = coverage
            .shards
            .iter()
            .map(|shard| DomainPlan {
                shard: shard.clone(),
                units: shard
                    .units
                    .iter()
                    .map(|u| UnitIdentity {
                        recovery_epoch: 0,
                        table: "enhance".into(),
                        shard_id: shard.id,
                        local_row_start: u.local_row_start,
                        allocated_rows: u.allocated_rows,
                        setup_sha256: hex::encode(Sha256::digest(setup_seed(shard.id))),
                        parameter_id: unit_parameter_id(u.allocated_rows).unwrap(),
                        content_sha256: "00".repeat(32),
                    })
                    .collect(),
            })
            .collect();
        let mut previous = original.clone();
        let new_shard = coverage.shards[0].id;
        previous.remove(&new_shard);
        let (alternative, strict) = coordinator
            .place_new_if_admitted(&coverage, &groups, &plans, &previous, original.clone())
            .await
            .unwrap();
        assert_eq!(alternative[&new_shard], "g1");
        assert_eq!(strict, ["g1".to_owned()].into());
        for (shard, group) in &previous {
            assert_eq!(alternative[shard], *group);
        }
        coordinator
            .store
            .lock()
            .unwrap()
            .update(|state| {
                state.pending_aborts.push(control::PendingAbort {
                    replica: groups[1].replicas[1].name.clone(),
                    operation: "pending".into(),
                    attempt: 1,
                });
                Ok(())
            })
            .unwrap();
        let (unchanged, strict) = coordinator
            .place_new_if_admitted(&coverage, &groups, &plans, &previous, original.clone())
            .await
            .unwrap();
        assert_eq!(
            unchanged, original,
            "pending decision excludes an alternative despite free memory"
        );
        assert!(strict.is_empty());
        coordinator
            .store
            .lock()
            .unwrap()
            .update(|state| {
                state.pending_aborts.clear();
                Ok(())
            })
            .unwrap();
        let mut unavailable = groups.clone();
        unavailable[1].replicas.pop();
        let (unchanged, strict) = coordinator
            .place_new_if_admitted(&coverage, &unavailable, &plans, &previous, original.clone())
            .await
            .unwrap();
        assert_eq!(
            unchanged, original,
            "one alternative replica cannot authorize relocation"
        );
        assert!(strict.is_empty());
        // No newly introduced shards means the fallback cannot relocate published data.
        let (unchanged, strict) = coordinator
            .place_new_if_admitted(&coverage, &groups, &plans, &original, original.clone())
            .await
            .unwrap();
        assert_eq!(unchanged, original);
        assert!(strict.is_empty());
        let (deferred, strict) = coordinator
            .consolidate_if_admitted(&coverage, &groups, &plans, original.clone())
            .await
            .unwrap();
        assert_eq!(
            deferred, original,
            "one available destination cannot force an elective move"
        );
        assert!(strict.is_empty());
        reject_peer.store(false, Ordering::SeqCst);
        coordinator
            .store
            .lock()
            .unwrap()
            .update(|state| {
                state.pending_aborts.push(control::PendingAbort {
                    replica: groups[0].replicas[0].name.clone(),
                    operation: "pending".into(),
                    attempt: 2,
                });
                Ok(())
            })
            .unwrap();
        let (unchanged, strict) = coordinator
            .consolidate_if_admitted(&coverage, &groups, &plans, original.clone())
            .await
            .unwrap();
        assert_eq!(
            unchanged, original,
            "pending decision defers elective consolidation too"
        );
        assert!(strict.is_empty());
        coordinator
            .store
            .lock()
            .unwrap()
            .update(|state| {
                state.pending_aborts.clear();
                Ok(())
            })
            .unwrap();

        let (admitted, strict) = coordinator
            .consolidate_if_admitted(&coverage, &groups, &plans, original.clone())
            .await
            .unwrap();
        assert_eq!(strict, ["g0".to_owned()].into());
        assert_eq!(admitted[&coverage.shards[5].id], "g0");
        assert_eq!(admitted[&coverage.shards[6].id], "g1");
        for (path, before) in original_files {
            assert_eq!(fs::read(path).ok(), before);
        }
        assert!(coordinator
            .store
            .lock()
            .unwrap()
            .state()
            .operation
            .is_none());
        let origin = &groups[0].replicas[0].url;
        let mut invalid = plans[..6].to_vec();
        invalid[0].shard.state = ShardState::Growing;
        assert_eq!(
            coordinator
                .http
                .post(format!("{origin}/internal/admit"))
                .json(&invalid)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        // An unresolved candidate also defers consolidation; admission must not
        // implicitly cancel it or consume another operation attempt.
        let candidate = worker::Candidate {
            placement_policy: Default::default(),
            operation: "held".into(),
            attempt: 0,
            epoch: 1,
            expected_revision: 0,
            generation: 1,
            plans: vec![plans[0].clone()],
        };
        assert!(coordinator
            .http
            .post(format!("{origin}/internal/reserve"))
            .json(&candidate)
            .send()
            .await
            .unwrap()
            .status()
            .is_success());
        let (deferred, strict) = coordinator
            .consolidate_if_admitted(&coverage, &groups, &plans, original.clone())
            .await
            .unwrap();
        assert_eq!(deferred, original);
        assert!(strict.is_empty());
        let health: serde_json::Value = coordinator
            .http
            .get(format!("{origin}/internal/health"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(health["candidate"]["operation"], "held");
        for task in tasks {
            task.abort();
        }
    }
}
