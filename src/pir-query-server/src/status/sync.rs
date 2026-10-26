//! Status material hooks for the shared publication lifecycle.
//!
//! [`pir_sync_manager::publication`] owns envelope succession, the
//! durable floor, object transfer, authority checks and commit ordering. This
//! adapter chooses the Status inventory (from the envelope, or from the worker
//! for a router), loads generations, pins the worker during router handover
//! and installs committed views in the role's controller.
mod worker_handover;

use pir_pipeline::consumer::{
    ConsumerEngine, ConsumerPass, DeferralReason, PeerHandover, PeerReservation,
};

use super::Role;
use super::{config::RoleConfig, Controller, Service};
use pir_pipeline::consumer::polling::SyncTask;
use pir_publication::{AuthorityFloor, ObjectCache, ObjectReference, SourceBinding, ViewIdentity};
use pir_sync_manager::publication::{
    EnvelopeConsumer, ObjectFetcher, PreparedCommit, PublicationAdapter, PublicationPlan,
    ViewRequirement,
};
use pir_sync_manager::Error;
use status_shared::{
    now_ms,
    protocol::{Manifest, MAX_AGE_MS},
    publication::*,
    Generation,
};
use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

/// Durable floor file. Floors of the earlier pointer format live under another
/// name; their pointers can never be served again, so they are left unread.
const FLOOR_FILE: &str = "publication-floor.json";
const ACTIVE_RECORD: &str = "active.json";

pub(super) type StatusSynchronization = ConsumerEngine<EnvelopeConsumer<StatusPublication>>;

const FAST_POLL: Duration = Duration::from_millis(100);
const NORMAL_POLL: Duration = Duration::from_secs(1);

/// Keep most of the observation's lifetime available for the serialized
/// worker/router handover. Young steady state keeps the ordinary poll rate.
pub(super) struct StatusPolling {
    inner: StatusSynchronization,
    last_pass_succeeded: bool,
}

pub(super) fn manager(
    task: StatusSynchronization,
) -> pir_pipeline::consumer::polling::SyncManager<StatusPolling> {
    pir_pipeline::consumer::polling::SyncManager::new(StatusPolling {
        inner: task,
        last_pass_succeeded: false,
    })
    .with_success_interval(FAST_POLL)
}

impl StatusPolling {
    async fn poll_deadline(&self, minimum: tokio::time::Instant) -> tokio::time::Instant {
        // The manager selects the wait after a failure. Preserve it.
        if !self.last_pass_succeeded || self.inner.context().adapter().poll_soon {
            return minimum;
        }
        let state = self.inner.context().adapter().service.state.lock().await;
        let now = now_ms();
        let young = !state.poisoned
            && state.controller.as_ref().is_some_and(|controller| {
                let active = controller.current();
                controller.check(&active).is_ok()
                    && now.saturating_sub(active.manifest().observed_ms) < MAX_AGE_MS / 4
            });
        if young {
            minimum.max(tokio::time::Instant::now() + NORMAL_POLL)
        } else {
            minimum
        }
    }
}

impl SyncTask for StatusPolling {
    type Error = Error;
    type Report = ConsumerPass<u64>;
    async fn synchronize(&mut self) -> Result<Self::Report, Error> {
        let result = self.inner.synchronize().await;
        self.last_pass_succeeded = result.is_ok();
        result
    }
    fn terminal(&self, error: &Error) -> bool {
        self.inner.terminal(error)
    }
    fn transient(&self, error: &Error) -> bool {
        self.inner.transient(error)
    }
    async fn wait_until(&mut self, deadline: tokio::time::Instant) {
        tokio::time::sleep_until(self.poll_deadline(deadline).await).await;
    }
}

/// Open the role's floor and cache and bind them to the configured publisher.
pub(super) fn synchronization(
    service: Service,
    config: &RoleConfig,
) -> Result<StatusSynchronization, Failure> {
    let source = super::source(config)?;
    let floor = AuthorityFloor::open(
        service.state_dir.join(FLOOR_FILE),
        SourceBinding {
            publisher_origin: source.origin().to_string(),
            product_namespace: PREFIX.into(),
            chain: service.network,
        },
    )?;
    let cache = ObjectCache::open(service.state_dir.join("objects"))?;
    let adapter = StatusPublication {
        service,
        loaded_inventory: None,
        poll_soon: false,
    };
    EnvelopeConsumer::open(adapter, source, floor, cache)
}

pub(super) struct StatusPublication {
    service: Service,
    /// The inventory of the last view this process loaded and committed. Only
    /// its already-verified material may be re-identified without reloading.
    loaded_inventory: Option<Inventory>,
    /// Product coordination needs another check even when serving is young.
    poll_soon: bool,
}

/// A planned Status inventory and how much of it must be loaded.
pub(super) struct StatusCandidate {
    inventory: Inventory,
    inventory_object: ObjectReference,
    authorization: InventoryAuthorization,
    previous: Option<Arc<Generation>>,
    material: CandidateMaterial,
    observed: Current,
}
enum CandidateMaterial {
    /// The serving generation already is this inventory.
    AlreadyServing,
    /// The serving material under a new publication identity.
    Reidentified(Box<Generation>),
    /// Load the material from verified cached objects.
    LoadFromCache,
}
pub(super) struct StatusLoaded {
    inventory: Inventory,
    inventory_object: ObjectReference,
    authorization: InventoryAuthorization,
    generation: Generation,
    /// The live, floor-checked envelope that authorized this exact inventory.
    /// Never restored from an active record or inferred from cached bytes.
    observed: Current,
}

impl StatusPublication {
    async fn serving_controller(&self) -> Option<Controller> {
        self.service.state.lock().await.controller.clone()
    }
    async fn waiting(&self, reason: &'static str) {
        self.service.state.lock().await.synchronization = reason.into();
        tracing::debug!(role = ?self.service.role, reason, "Status synchronization deferred");
    }
}

impl PublicationAdapter for StatusPublication {
    type Descriptor = StatusDescriptor;
    type AuthoritySubject = AnchorSubject;
    type Candidate = StatusCandidate;
    type Loaded = StatusLoaded;
    type Report = u64;
    type InstalledView = Inventory;

    fn validate_envelope(&self, envelope: &Current) -> Result<(), Error> {
        validate_current(envelope, self.service.network)
    }

    fn fence_serving(
        &mut self,
    ) -> impl std::future::Future<Output = Vec<ViewIdentity>> + Send + 'static {
        let service = self.service.clone();
        async move {
            let mut state = service.state.lock().await;
            let fenced = state
                .controller
                .as_ref()
                .map(Controller::serving_manifests)
                .unwrap_or_default()
                .iter()
                .map(view)
                .collect();
            if let Some(controller) = &state.controller {
                controller.revoke();
            }
            state.router_holds.clear();
            fenced
        }
    }

    fn invalidate_views(
        &mut self,
        views: Vec<ViewIdentity>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let service = self.service.clone();
        async move {
            let mut state = service.state.lock().await;
            let views: BTreeSet<_> = views.into_iter().map(|view| view.0).collect();
            if let Some(controller) = &state.controller {
                controller.invalidate(views.iter().copied());
            }
            for identity in views {
                state.router_holds.revoke(&identity);
            }
        }
    }

    fn close_admission(&mut self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let service = self.service.clone();
        async move { service.state.lock().await.poisoned = true }
    }

    async fn serving_subjects(&mut self, envelope: &Current) -> Vec<(ViewIdentity, AnchorSubject)> {
        let Some(controller) = self.serving_controller().await.filter(|c| !c.revoked()) else {
            return Vec::new();
        };
        controller
            .serving_manifests()
            .into_iter()
            .filter(|m| {
                m.recovery_epoch == envelope.fence.recovery_epoch && m.fresh(now_ms()).is_ok()
            })
            .map(|m| (view(&m), (m.anchor_height, m.anchor_hash)))
            .collect()
    }

    async fn plan(
        &mut self,
        envelope: &Current,
        objects: &mut ObjectFetcher<'_>,
    ) -> Result<PublicationPlan<StatusCandidate>, Error> {
        self.service
            .state
            .lock()
            .await
            .router_holds
            .retain(|g| g.manifest().fresh(now_ms()).is_ok());
        // A router follows its worker, which may still serve a retained
        // predecessor of the latest publication.
        let (inventory_object, authorization) = if self.service.role == Role::Router {
            match worker_handover::serving_inventory(&self.service).await? {
                Some(inventory) => (inventory, InventoryAuthorization::RetainedHandover),
                None => {
                    self.poll_soon = true;
                    self.waiting("waiting_for_worker").await;
                    return Ok(PublicationPlan::Deferred);
                }
            }
        } else {
            let latest = envelope
                .inventory()
                .cloned()
                .ok_or("Status current has no inventory")?;
            (latest, InventoryAuthorization::Latest)
        };
        let inventory: Inventory = serde_json::from_slice(
            &objects
                .fetch_metadata(&inventory_object, METADATA_LIMIT as u64)
                .await?,
        )?;
        // A worker still serving a view from before the latest recovery is
        // loading, like a worker with no fresh view.
        if authorization == InventoryAuthorization::RetainedHandover
            && inventory.manifest.recovery_epoch < envelope.fence.recovery_epoch
        {
            self.poll_soon = true;
            self.waiting("waiting_for_worker_recovery").await;
            return Ok(PublicationPlan::Deferred);
        }
        // The worker is sampled after the publisher envelope. It may already
        // serve a later publication; observe that publication next pass rather
        // than applying transport-failure backoff to an ordinary peer advance.
        if authorization == InventoryAuthorization::RetainedHandover
            && inventory.manifest.network == envelope.scope.chain
            && inventory.manifest.salt == envelope.scope.source
            && inventory.manifest.recovery_epoch == envelope.fence.recovery_epoch
            && envelope
                .publication
                .as_ref()
                .is_some_and(|latest| inventory.manifest.generation > latest.descriptor.generation)
        {
            inventory.validate_material()?;
            self.poll_soon = true;
            self.waiting("waiting_for_publisher").await;
            return Ok(PublicationPlan::Deferred);
        }
        inventory.authorize(&inventory_object, envelope, authorization)?;
        match inventory.manifest.fresh(now_ms()) {
            Ok(()) => {}
            Err(status_shared::protocol::Error::Stale) => {
                // A fresh publication can arrive on the next normal pass.
                // Expiry grants no authority and is not a transport failure.
                self.waiting("waiting_for_fresh_publication").await;
                return Ok(PublicationPlan::Deferred);
            }
            Err(error) => return Err(error.into()),
        }
        let controller = self.serving_controller().await;
        let previous = controller.as_ref().map(Controller::current);
        let already_serving = previous
            .as_ref()
            .is_some_and(|g| g.manifest().id() == inventory.manifest.id());
        // Reuse only material this process already verified and loaded. The
        // new publication identity still passes live authority and freshness
        // checks.
        let reidentified = previous
            .as_ref()
            .zip(self.loaded_inventory.as_ref())
            .and_then(|(serving, loaded)| reidentify(serving, loaded, &inventory));
        // Pins bound the number of distinct loaded materials. Re-identifying
        // the current material shares those same allocations and must still
        // accept fresh publisher observations while the router holds an old view.
        if !already_serving
            && reidentified.is_none()
            && controller.as_ref().is_some_and(|c| !c.can_prepare())
        {
            self.poll_soon = true;
            self.waiting("waiting_for_retention").await;
            return Ok(PublicationPlan::Deferred);
        }
        let (material, required_objects) = if already_serving {
            (CandidateMaterial::AlreadyServing, Vec::new())
        } else if let Some(generation) = reidentified {
            (
                CandidateMaterial::Reidentified(Box::new(generation)),
                Vec::new(),
            )
        } else {
            let material = self.service.material();
            let mut required = vec![inventory.public.clone()];
            if material.evaluates() {
                required.extend(inventory.units.iter().cloned());
            }
            if material.packs() {
                required.extend(inventory.packing.iter().cloned());
            }
            for object in &required {
                object.validate(attempt_limit())?;
            }
            (CandidateMaterial::LoadFromCache, required)
        };
        Ok(PublicationPlan::Candidate {
            identity: view(&inventory.manifest),
            candidate: StatusCandidate {
                inventory,
                inventory_object,
                authorization,
                previous,
                material,
                observed: envelope.clone(),
            },
            required_objects,
        })
    }

    async fn load(
        &mut self,
        candidate: StatusCandidate,
        objects: pir_publication::VerifiedObjects,
    ) -> Result<StatusLoaded, Error> {
        let StatusCandidate {
            inventory,
            inventory_object,
            authorization,
            previous,
            material,
            observed,
        } = candidate;
        let generation = match material {
            CandidateMaterial::AlreadyServing => previous
                .as_deref()
                .cloned()
                .ok_or("Status serving generation disappeared")?,
            CandidateMaterial::Reidentified(generation) => *generation,
            CandidateMaterial::LoadFromCache => {
                let loading = inventory.clone();
                let material = self.service.material();
                tokio::task::spawn_blocking(move || {
                    Generation::load_prepared_objects(
                        &loading,
                        material,
                        &objects,
                        previous.as_deref(),
                    )
                })
                .await??
            }
        };
        Ok(StatusLoaded {
            inventory,
            inventory_object,
            authorization,
            generation,
            observed,
        })
    }

    /// A worker's candidate whose latest publication re-identifies the same
    /// material, typically refreshed while it loaded, takes the latest
    /// inventory's metadata. A router similarly follows refreshed metadata
    /// already served by its worker, then pins that exact view before commit.
    async fn rebind(
        &mut self,
        envelope: &Current,
        loaded: StatusLoaded,
        objects: &mut ObjectFetcher<'_>,
    ) -> Result<StatusLoaded, Error> {
        let Some(latest) = envelope.publication.as_ref().filter(|_| envelope.eligible) else {
            return Ok(loaded);
        };
        let inventory_object = match loaded.authorization {
            InventoryAuthorization::Latest => {
                if latest.material != loaded.inventory.material_identity() {
                    return Ok(loaded);
                }
                latest.inventory.clone()
            }
            InventoryAuthorization::RetainedHandover => {
                let Some(inventory) = worker_handover::serving_inventory(&self.service).await?
                else {
                    return Ok(loaded);
                };
                inventory
            }
        };
        if inventory_object == loaded.inventory_object {
            return Ok(loaded);
        }
        let inventory: Inventory = serde_json::from_slice(
            &objects
                .fetch_metadata(&inventory_object, METADATA_LIMIT as u64)
                .await?,
        )?;
        if inventory.manifest.generation <= loaded.inventory.manifest.generation
            || inventory.manifest.generation > latest.descriptor.generation
        {
            return Ok(loaded);
        }
        let Some(generation) = reidentify(&loaded.generation, &loaded.inventory, &inventory) else {
            return Ok(loaded);
        };
        inventory.authorize(&inventory_object, envelope, loaded.authorization)?;
        if loaded.authorization == InventoryAuthorization::RetainedHandover
            && worker_handover::pin(&self.service, &inventory.manifest).await?
                == PeerReservation::PeerAdvanced
        {
            // The original view remains pinned. If the worker advanced again,
            // commit that still-fresh view rather than discarding its loaded
            // material to chase metadata.
            return Ok(loaded);
        }
        Ok(StatusLoaded {
            inventory,
            inventory_object,
            authorization: loaded.authorization,
            generation,
            observed: envelope.clone(),
        })
    }

    fn revalidate(
        &self,
        envelope: &Current,
        loaded: &StatusLoaded,
    ) -> Result<(ViewIdentity, AnchorSubject), Error> {
        let manifest = &loaded.inventory.manifest;
        loaded.inventory.authorize(
            &loaded.inventory_object,
            &loaded.observed,
            loaded.authorization,
        )?;
        // A normal newer publication does not withdraw a snapshot that was
        // observed live. Keep its original timestamp and material, but require
        // the same source and complete fence under the latest live check.
        if !envelope.eligible
            || envelope.scope != loaded.observed.scope
            || envelope.fence != loaded.observed.fence
            || envelope.sequence < loaded.observed.sequence
            || envelope
                .publication
                .as_ref()
                .is_none_or(|latest| latest.descriptor.generation < manifest.generation)
        {
            return Err("Status candidate source or authority fence changed".into());
        }
        manifest.fresh(now_ms())?;
        Ok((
            view(manifest),
            (manifest.anchor_height, manifest.anchor_hash),
        ))
    }

    fn resumable_view(&self, loaded: &StatusLoaded, envelope: &Current) -> Option<ViewIdentity> {
        let valid = self
            .revalidate(envelope, loaded)
            .ok()
            .map(|(identity, _)| identity);
        if valid.is_none() && loaded.inventory.manifest.fresh(now_ms()).is_err() {
            tracing::debug!(role = ?self.service.role, reason = "candidate_expired", "Status prepared candidate discarded");
        }
        valid
    }

    async fn deferred(&mut self, reason: DeferralReason) {
        self.poll_soon = matches!(reason, DeferralReason::PeerAdvanced);
        self.waiting(match reason {
            DeferralReason::AuthorityAdvanced => "waiting_for_authority",
            DeferralReason::PeerAdvanced => "waiting_for_worker_pin",
        })
        .await;
    }

    fn phase_finished(&mut self, phase: &'static str, elapsed: Duration) {
        tracing::debug!(role = ?self.service.role, phase, elapsed_ms = elapsed.as_secs_f64() * 1000., "Status synchronization phase completed");
    }

    async fn reserve_candidate(
        &mut self,
        candidate: &StatusCandidate,
    ) -> Result<PeerReservation, Error> {
        if self.service.role != Role::Router {
            return Ok(PeerReservation::Reserved);
        }
        worker_handover::pin(&self.service, &candidate.inventory.manifest).await
    }

    async fn reserve_peer(&mut self, loaded: &StatusLoaded) -> Result<PeerReservation, Error> {
        if self.service.role != Role::Router {
            return Ok(PeerReservation::Reserved);
        }
        worker_handover::pin(&self.service, &loaded.inventory.manifest).await
    }

    fn view_objects(&self, loaded: &StatusLoaded) -> BTreeSet<String> {
        std::iter::once(&loaded.inventory_object)
            .chain(loaded.inventory.referenced_objects())
            .map(|object| object.sha256.clone())
            .collect()
    }

    /// Every view the controller can still answer from, and each view a
    /// router holds until the view is too old to answer.
    async fn retained_views(&mut self) -> Vec<(ViewIdentity, ViewRequirement)> {
        let state = self.service.state.lock().await;
        let serving = state
            .controller
            .as_ref()
            .filter(|controller| !controller.revoked())
            .map(Controller::retained_manifests)
            .unwrap_or_default()
            .into_iter()
            .map(|manifest| (view(&manifest), ViewRequirement::Serving));
        let held = state.router_holds.iter().map(|generation| {
            let manifest = generation.manifest();
            let answerable_until =
                UNIX_EPOCH + Duration::from_millis(manifest.observed_ms.saturating_add(MAX_AGE_MS));
            (view(manifest), ViewRequirement::HeldUntil(answerable_until))
        });
        serving.chain(held).collect()
    }

    fn prepare_commit(
        &mut self,
        loaded: StatusLoaded,
    ) -> Result<(PreparedCommit<u64>, Inventory), Error> {
        let StatusLoaded {
            inventory,
            inventory_object,
            generation,
            ..
        } = loaded;
        let root = self.service.state_dir.clone();
        let record_bytes = serde_json::to_vec(&inventory_object)?;
        let record = Box::new(move || {
            pir_control::durable::write_atomic(&root, ACTIVE_RECORD, |f| {
                f.write_all(&record_bytes)
            })?;
            pir_control::durable::sync_dir(&root)
        });
        let reported_generation = inventory.manifest.generation;
        let source_observed_ms = inventory.manifest.observed_ms;
        let install_service = self.service.clone();
        let install = Box::pin(async move {
            let mut state = install_service.state.lock().await;
            let changed = state.inventory.as_ref() != Some(&inventory_object);
            let mut retired_owner = None;
            // Activation refuses before changing any view, so an error leaves
            // serving unchanged.
            let retired = match state.controller.as_ref() {
                Some(controller) if !controller.revoked() => {
                    if controller.current().manifest().id() != generation.manifest().id() {
                        controller.activate_deferring_release(generation)?
                    } else {
                        Vec::new()
                    }
                }
                _ => {
                    retired_owner = state.controller.replace(Controller::new(generation));
                    Vec::new()
                }
            };
            state.inventory = Some(inventory_object);
            state.poisoned = false;
            if changed {
                tracing::debug!(role = ?install_service.role, generation = reported_generation,
                    source_observed_ms, observation_age_ms = now_ms().saturating_sub(source_observed_ms),
                    "Status serving view installed");
            }
            drop(state);
            if !retired.is_empty() || retired_owner.is_some() {
                tokio::task::spawn_blocking(move || drop((retired, retired_owner)));
            }
            Ok(reported_generation)
        });
        let revoke_service = self.service.clone();
        let revoke_after_failed_record = Box::pin(async move {
            let mut state = revoke_service.state.lock().await;
            if let Some(controller) = &state.controller {
                controller.revoke();
            }
            state.router_holds.clear();
            state.poisoned = true;
        });
        Ok((
            PreparedCommit {
                record,
                install,
                revoke_after_failed_record,
            },
            inventory,
        ))
    }

    fn installed(&mut self, inventory: Inventory) {
        self.loaded_inventory = Some(inventory);
    }

    async fn release_peer(&mut self, _generation: &u64) -> PeerHandover {
        match (&self.service.role, &self.loaded_inventory) {
            (Role::Router, Some(inventory)) => {
                worker_handover::release(&self.service, &inventory.manifest).await
            }
            _ => PeerHandover::Complete,
        }
    }

    async fn serving_report(&mut self) -> u64 {
        self.serving_controller()
            .await
            .map_or(0, |c| c.current().manifest().generation)
    }

    async fn pass_started(&mut self) {
        self.poll_soon = false;
        self.service.state.lock().await.synchronization = "synchronizing".into();
    }

    async fn pass_finished(&mut self, result: &Result<ConsumerPass<u64>, Error>) {
        let synchronization = match result {
            Ok(ConsumerPass::Committed {
                handover: PeerHandover::Complete,
                ..
            }) => "synchronized",
            Ok(ConsumerPass::Committed { .. }) => "waiting_for_worker_commit",
            Ok(_) => {
                let mut state = self.service.state.lock().await;
                if state.synchronization == "synchronizing" {
                    state.synchronization = "waiting_for_publication".into();
                }
                return;
            }
            Err(error) => {
                tracing::warn!(%error, "Status synchronization refused");
                "refused"
            }
        };
        self.service.state.lock().await.synchronization = synchronization.into();
    }
}

/// `generation`, loaded from `loaded`, under `inventory` when that names the
/// same verified material with new metadata. Only the manifest's generation,
/// recovery epoch and observation time may differ.
fn reidentify(
    generation: &Generation,
    loaded: &Inventory,
    inventory: &Inventory,
) -> Option<Generation> {
    if loaded.manifest != *generation.manifest()
        || loaded.material_identity() != inventory.material_identity()
    {
        return None;
    }
    let next = generation.reidentified(
        inventory.manifest.generation,
        inventory.manifest.recovery_epoch,
        inventory.manifest.observed_ms,
    );
    (next.manifest() == &inventory.manifest).then_some(next)
}

/// A Status serving view is one exact manifest.
fn view(manifest: &Manifest) -> ViewIdentity {
    ViewIdentity(manifest.id())
}

#[cfg(test)]
mod tests;
