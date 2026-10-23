//! Status material hooks for the shared publication lifecycle.
//!
//! [`crate::synchronization::publication_lifecycle`] owns envelope succession, the
//! durable floor, object transfer, authority checks and commit ordering. This
//! adapter chooses the Status inventory (from the envelope, or from the worker
//! for a router), loads generations, pins the worker during router handover
//! and installs committed views in the role's controller.
mod worker_handover;

use super::{config::RoleConfig, Controller, Service};
use crate::synchronization::publication_lifecycle::{
    ObjectFetcher, PeerHandover, PeerReservation, PreparedCommit, PublicationAdapter,
    PublicationLifecycle, PublicationPass, PublicationPlan, ViewRequirement,
};
use pir_publication::{AuthorityFloor, ObjectCache, ObjectReference, SourceBinding, ViewIdentity};
use pir_sync_manager::Error;
use status_query_server::distributed::Role;
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

pub(super) type StatusSynchronization = PublicationLifecycle<StatusPublication>;

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
    };
    PublicationLifecycle::new(adapter, source, floor, cache)
}

pub(super) struct StatusPublication {
    service: Service,
    /// The inventory of the last view this process loaded and committed. Only
    /// its already-verified material may be re-identified without reloading.
    loaded_inventory: Option<Inventory>,
}

/// A planned Status inventory and how much of it must be loaded.
pub(super) struct StatusCandidate {
    inventory: Inventory,
    inventory_object: ObjectReference,
    authorization: InventoryAuthorization,
    previous: Option<Arc<Generation>>,
    material: CandidateMaterial,
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
}

impl StatusPublication {
    async fn serving_controller(&self) -> Option<Controller> {
        self.service.state.lock().await.controller.clone()
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
            state
                .router_holds
                .retain(|generation| !views.contains(&generation.manifest().id()));
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
                None => return Ok(PublicationPlan::Deferred),
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
            return Ok(PublicationPlan::Deferred);
        }
        inventory.authorize(&inventory_object, envelope, authorization)?;
        inventory.manifest.fresh(now_ms())?;
        let controller = self.serving_controller().await;
        let previous = controller.as_ref().map(Controller::current);
        let already_serving = previous
            .as_ref()
            .is_some_and(|g| g.manifest().id() == inventory.manifest.id());
        if !already_serving && controller.as_ref().is_some_and(|c| !c.can_prepare()) {
            return Ok(PublicationPlan::Deferred);
        }
        // Reuse only material this process already verified and loaded. The
        // new publication identity still passes live authority and freshness
        // checks.
        let reidentified = previous
            .as_ref()
            .zip(self.loaded_inventory.as_ref())
            .and_then(|(serving, loaded)| reidentify(serving, loaded, &inventory));
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
            },
            required_objects,
        })
    }

    async fn load(
        &mut self,
        candidate: StatusCandidate,
        cache: &ObjectCache,
    ) -> Result<StatusLoaded, Error> {
        let StatusCandidate {
            inventory,
            inventory_object,
            authorization,
            previous,
            material,
        } = candidate;
        let generation = match material {
            CandidateMaterial::AlreadyServing => previous
                .as_deref()
                .cloned()
                .ok_or("Status serving generation disappeared")?,
            CandidateMaterial::Reidentified(generation) => *generation,
            CandidateMaterial::LoadFromCache => {
                let cache = cache.clone();
                let loading = inventory.clone();
                let material = self.service.material();
                tokio::task::spawn_blocking(move || {
                    Generation::load_prepared(
                        &loading,
                        material,
                        |object| cache.object_path(object),
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
        })
    }

    /// A worker's candidate whose latest publication re-identifies the same
    /// material, typically refreshed while it loaded, takes the latest
    /// inventory's metadata. A router follows its worker's own inventory.
    async fn rebind(
        &mut self,
        envelope: &Current,
        loaded: StatusLoaded,
        objects: &mut ObjectFetcher<'_>,
    ) -> Result<StatusLoaded, Error> {
        let Some(latest) = envelope.publication.as_ref().filter(|_| envelope.eligible) else {
            return Ok(loaded);
        };
        if loaded.authorization != InventoryAuthorization::Latest
            || latest.material != loaded.inventory.material_identity()
            || *latest
                == loaded
                    .inventory
                    .publication_reference(&loaded.inventory_object)
        {
            return Ok(loaded);
        }
        let inventory: Inventory = serde_json::from_slice(
            &objects
                .fetch_metadata(&latest.inventory, METADATA_LIMIT as u64)
                .await?,
        )?;
        let Some(generation) = reidentify(&loaded.generation, &loaded.inventory, &inventory) else {
            return Ok(loaded);
        };
        Ok(StatusLoaded {
            inventory,
            inventory_object: latest.inventory.clone(),
            authorization: InventoryAuthorization::Latest,
            generation,
        })
    }

    fn revalidate(
        &self,
        envelope: &Current,
        loaded: &StatusLoaded,
    ) -> Result<(ViewIdentity, AnchorSubject), Error> {
        let manifest = &loaded.inventory.manifest;
        loaded
            .inventory
            .authorize(&loaded.inventory_object, envelope, loaded.authorization)?;
        if manifest.recovery_epoch != envelope.fence.recovery_epoch {
            return Err("Status candidate belongs to an earlier recovery epoch".into());
        }
        manifest.fresh(now_ms())?;
        Ok((
            view(manifest),
            (manifest.anchor_height, manifest.anchor_hash),
        ))
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
        let install_service = self.service.clone();
        let install = Box::pin(async move {
            let mut state = install_service.state.lock().await;
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
        self.service.state.lock().await.synchronization = "synchronizing".into();
    }

    async fn pass_finished(&mut self, result: &Result<PublicationPass<u64>, Error>) {
        let synchronization = match result {
            Ok(PublicationPass::Committed {
                handover: PeerHandover::Complete,
                ..
            }) => "synchronized",
            Ok(_) => "waiting_for_peer",
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
