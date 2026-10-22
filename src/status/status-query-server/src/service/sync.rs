//! Status material hooks for the shared publication lifecycle.
//!
//! [`pir_sync_manager::publication_lifecycle`] owns pointer succession, the
//! durable floor, object transfer, authority checks and commit ordering. This
//! adapter chooses the Status inventory (from the pointer, or from the worker
//! for a router), loads generations, pins the worker during router handover
//! and installs committed views in the role's controller.
mod worker_handover;

use super::Service;
use crate::{
    distributed::{Role, RoleConfig},
    now_ms,
    protocol::Manifest,
    publication::*,
    Controller, Generation,
};
use ipir_sp::server::MatvecBackend;
use pir_publication::{
    AuthorityFloor, MaterialIdentity, ObjectCache, ObjectReference, SourceBinding,
};
use pir_sync_manager::{
    publication_lifecycle::{
        ObjectFetcher, PeerHandover, PeerReservation, PublicationAdapter, PublicationLifecycle,
        PublicationPass, PublicationPlan,
    },
    Error,
};
use std::{collections::BTreeSet, sync::Arc};

/// Durable floor file. Its name and pointer encoding are unchanged so an
/// existing floor keeps its revision and invalidations across this upgrade.
const FLOOR_FILE: &str = "publisher-high-water.json";
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
    let backend = if config.cuda {
        MatvecBackend::Cuda { device: 0 }
    } else {
        MatvecBackend::Cpu
    };
    let adapter = StatusPublication {
        service,
        backend,
        loaded_inventory: None,
    };
    PublicationLifecycle::new(adapter, source, floor, cache)
}

pub(super) struct StatusPublication {
    service: Service,
    backend: MatvecBackend,
    /// The inventory of the last view this process loaded and committed. Only
    /// its already-verified material may be re-identified without reloading.
    loaded_inventory: Option<Inventory>,
}

/// A planned Status inventory and how much of it must be loaded.
pub(super) struct StatusCandidate {
    inventory: Inventory,
    inventory_object: ObjectReference,
    previous: Option<Arc<Generation>>,
    material: CandidateMaterial,
}
enum CandidateMaterial {
    /// The serving generation already is this inventory.
    AlreadyServing,
    /// The serving material under a new publication identity.
    Reidentified(Generation),
    /// Load the material from verified cached objects.
    LoadFromCache,
}
pub(super) struct StatusLoaded {
    inventory: Inventory,
    inventory_object: ObjectReference,
    generation: Generation,
}

impl StatusPublication {
    async fn serving_controller(&self) -> Option<Controller> {
        self.service.state.lock().await.controller.clone()
    }
}

impl PublicationAdapter for StatusPublication {
    type Pointer = Current;
    type AuthoritySubject = AnchorSubject;
    type Candidate = StatusCandidate;
    type Loaded = StatusLoaded;
    type Report = u64;

    fn validate_pointer(&self, pointer: &Current) -> Result<(), Error> {
        pointer.validate(self.service.network)
    }

    async fn fence_serving(&mut self) -> Vec<MaterialIdentity> {
        let mut state = self.service.state.lock().await;
        let fenced = state
            .controller
            .as_ref()
            .map(Controller::serving_manifests)
            .unwrap_or_default()
            .iter()
            .map(Manifest::id)
            .collect();
        if let Some(controller) = &state.controller {
            controller.revoke();
        }
        state.router_holds.clear();
        fenced
    }

    async fn close_admission(&mut self) {
        self.service.state.lock().await.poisoned = true;
    }

    async fn serving_subjects(
        &mut self,
        pointer: &Current,
    ) -> Vec<(MaterialIdentity, AnchorSubject)> {
        let Some(controller) = self.serving_controller().await.filter(|c| !c.revoked()) else {
            return Vec::new();
        };
        controller
            .serving_manifests()
            .into_iter()
            .filter(|m| m.recovery_epoch == pointer.recovery_epoch && m.fresh(now_ms()).is_ok())
            .map(|m| (m.id(), (m.anchor_height, m.anchor_hash)))
            .collect()
    }

    async fn plan(
        &mut self,
        pointer: &Current,
        objects: &mut ObjectFetcher<'_>,
    ) -> Result<PublicationPlan<StatusCandidate>, Error> {
        self.service
            .state
            .lock()
            .await
            .router_holds
            .retain(|g| g.manifest().fresh(now_ms()).is_ok());
        let inventory_object = if self.service.role == Role::Router {
            match worker_handover::serving_inventory(&self.service).await? {
                Some(inventory) => inventory,
                None => return Ok(PublicationPlan::Deferred),
            }
        } else {
            pointer
                .inventory
                .clone()
                .ok_or("Status current has no inventory")?
        };
        let inventory: Inventory = serde_json::from_slice(
            &objects
                .fetch_metadata(&inventory_object, METADATA_LIMIT as u64)
                .await?,
        )?;
        inventory.validate(pointer)?;
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
        // new identity still passes live authority and freshness checks.
        let reidentified = previous
            .as_ref()
            .zip(self.loaded_inventory.as_ref())
            .and_then(|(serving, loaded)| {
                if loaded.manifest != *serving.manifest()
                    || !loaded.same_prepared_material(&inventory)
                {
                    return None;
                }
                let next = serving.reidentified(
                    inventory.manifest.generation,
                    inventory.manifest.recovery_epoch,
                    inventory.manifest.observed_ms,
                );
                (next.manifest() == &inventory.manifest).then_some(next)
            });
        let (material, required_objects) = if already_serving {
            (CandidateMaterial::AlreadyServing, Vec::new())
        } else if let Some(generation) = reidentified {
            (CandidateMaterial::Reidentified(generation), Vec::new())
        } else {
            let role_objects = if self.service.role == Role::Worker {
                &inventory.units
            } else {
                &inventory.packing
            };
            let mut required = vec![inventory.public.clone()];
            required.extend(role_objects.iter().cloned());
            for object in &required {
                object.validate(attempt_limit())?;
            }
            (CandidateMaterial::LoadFromCache, required)
        };
        Ok(PublicationPlan::Candidate {
            identity: inventory.manifest.id(),
            candidate: StatusCandidate {
                inventory,
                inventory_object,
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
            previous,
            material,
        } = candidate;
        let generation = match material {
            CandidateMaterial::AlreadyServing => previous
                .as_deref()
                .cloned()
                .ok_or("Status serving generation disappeared")?,
            CandidateMaterial::Reidentified(generation) => generation,
            CandidateMaterial::LoadFromCache => {
                let cache = cache.clone();
                let loading = inventory.clone();
                let worker = self.service.role == Role::Worker;
                let backend = self.backend;
                tokio::task::spawn_blocking(move || {
                    Generation::load_prepared(
                        &loading,
                        worker,
                        |object| cache.object_path(object),
                        backend,
                        previous.as_deref(),
                    )
                })
                .await??
            }
        };
        Ok(StatusLoaded {
            inventory,
            inventory_object,
            generation,
        })
    }

    fn revalidate(
        &self,
        pointer: &Current,
        loaded: &StatusLoaded,
    ) -> Result<(MaterialIdentity, AnchorSubject), Error> {
        let manifest = &loaded.inventory.manifest;
        loaded.inventory.validate(pointer)?;
        if manifest.recovery_epoch != pointer.recovery_epoch {
            return Err("Status candidate belongs to an earlier recovery epoch".into());
        }
        manifest.fresh(now_ms())?;
        Ok((
            manifest.id(),
            (manifest.anchor_height, manifest.anchor_hash),
        ))
    }

    async fn reserve_peer(&mut self, loaded: &StatusLoaded) -> Result<PeerReservation, Error> {
        if self.service.role != Role::Router {
            return Ok(PeerReservation::Reserved);
        }
        worker_handover::pin(&self.service, &loaded.inventory.manifest).await
    }

    fn retained_objects(&self, loaded: &StatusLoaded) -> BTreeSet<String> {
        std::iter::once(&loaded.inventory_object)
            .chain(loaded.inventory.referenced_objects())
            .map(|object| object.sha256.clone())
            .collect()
    }

    fn commit(
        &mut self,
        loaded: StatusLoaded,
    ) -> impl std::future::Future<Output = Result<u64, Error>> + Send + 'static {
        self.loaded_inventory = Some(loaded.inventory.clone());
        let service = self.service.clone();
        async move {
            let StatusLoaded {
                inventory,
                inventory_object,
                generation,
            } = loaded;
            let root = service.state_dir.clone();
            let bytes = serde_json::to_vec(&inventory_object)?;
            let written = tokio::task::spawn_blocking(move || {
                pir_control::durable::write_atomic(&root, ACTIVE_RECORD, |f| f.write_all(&bytes))?;
                pir_control::durable::sync_dir(&root)
            })
            .await;
            let mut state = service.state.lock().await;
            if let Err(error) = written.map_err(Error::from).and_then(|w| Ok(w?)) {
                // The record may have been replaced; nothing may serve until a
                // later commit succeeds.
                if let Some(controller) = &state.controller {
                    controller.revoke();
                }
                state.router_holds.clear();
                state.poisoned = true;
                return Err(error);
            }
            let mut retired_owner = None;
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
            Ok(inventory.manifest.generation)
        }
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

#[cfg(test)]
mod tests;
