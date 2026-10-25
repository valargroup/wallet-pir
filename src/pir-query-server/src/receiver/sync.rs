//! Product hooks; envelope succession, transfers and authority belong to the
//! shared lifecycle. Cached material alone never opens serving admission.
use super::Config;
use crate::synchronization::publication_lifecycle::{
    Envelope, ObjectFetcher, PeerHandover, PeerReservation, PreparedCommit, PublicationAdapter,
    PublicationLifecycle, PublicationPlan, ViewRequirement,
};
use pir_publication::{AuthorityFloor, ObjectCache, ObjectReference, SourceBinding, ViewIdentity};
use pir_sync_manager::Error;
use receiver_pir_server::{
    blocks::BlockWindow,
    distribution::{self, Descriptor, Inventory, Subject},
    Publication, Publications,
};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, Weak},
};

type Lifecycle = PublicationLifecycle<Receiver>;
pub(super) fn synchronization(
    config: &Config,
    publications: Publications,
) -> Result<Lifecycle, Error> {
    let owner = Arc::new(pir_pipeline::service::lock(&config.state_dir)?);
    let source = config.source(true)?;
    std::fs::create_dir_all(&config.state_dir)?;
    let floor = AuthorityFloor::open(
        config.state_dir.join("authority-floor.json"),
        SourceBinding {
            publisher_origin: source.origin().to_string(),
            product_namespace: distribution::PREFIX.into(),
            chain: config.genesis,
        },
    )?;
    let cache = ObjectCache::open(config.state_dir.join("objects"))?;
    PublicationLifecycle::new(
        Receiver {
            config: config.clone(),
            owner,
            publications,
            views: Vec::new(),
            current: None,
        },
        source,
        floor,
        cache,
    )
}
pub(super) struct View {
    id: ViewIdentity,
    inventory: Inventory,
    directory: Arc<Mutex<Option<Weak<Publication>>>>,
}
pub(super) struct Receiver {
    config: Config,
    owner: Arc<std::fs::File>,
    publications: Publications,
    views: Vec<View>,
    current: Option<ViewIdentity>,
}
pub(super) struct Candidate {
    inventory: Inventory,
    object: ObjectReference,
    id: ViewIdentity,
}
pub(super) struct Loaded {
    candidate: Candidate,
    epoch: u64,
    directory: Option<Publication>,
    filters: Option<BlockWindow>,
}
impl Receiver {
    fn revoke(&mut self) -> (Publications, Vec<ViewIdentity>) {
        self.current = None;
        let ids = self.views.drain(..).map(|view| view.id).collect();
        (self.publications.clone(), ids)
    }
    fn expire(&mut self) {
        // Expires ordinary retention before checking weak references. An
        // in-flight response still holds its directory and its lazy files.
        drop(self.publications.live());
        self.views.retain(|view| {
            Some(&view.id) == self.current.as_ref()
                || view
                    .directory
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|p| p.strong_count() > 0)
        });
    }
}
impl PublicationAdapter for Receiver {
    type Descriptor = Descriptor;
    type AuthoritySubject = Subject;
    type InstalledView = View;
    type Candidate = Candidate;
    type Loaded = Loaded;
    type Report = Option<ViewIdentity>;
    fn validate_envelope(&self, envelope: &Envelope<Self>) -> Result<(), Error> {
        if envelope.scope != distribution::scope(self.config.genesis, self.config.activation_height)
        {
            return Err(
                "Receiver publisher scope differs from configured network or activation".into(),
            );
        }
        Ok(())
    }
    fn fence_serving(
        &mut self,
    ) -> impl std::future::Future<Output = Vec<ViewIdentity>> + Send + 'static {
        let (publications, ids) = self.revoke();
        async move {
            publications.revoke();
            ids
        }
    }
    fn invalidate_views(
        &mut self,
        _views: Vec<ViewIdentity>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let (publications, _) = self.revoke();
        async move {
            publications.revoke();
        }
    }
    fn close_admission(&mut self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let (publications, _) = self.revoke();
        async move {
            publications.revoke();
        }
    }
    async fn serving_subjects(&mut self, _: &Envelope<Self>) -> Vec<(ViewIdentity, Subject)> {
        self.expire();
        self.views
            .iter()
            .map(|view| (view.id.clone(), view.inventory.anchors()))
            .collect()
    }
    async fn plan(
        &mut self,
        envelope: &Envelope<Self>,
        objects: &mut ObjectFetcher<'_>,
    ) -> Result<PublicationPlan<Candidate>, Error> {
        let reference = envelope
            .publication
            .as_ref()
            .ok_or("Receiver envelope has no publication")?;
        let id = ViewIdentity(reference.publication);
        if self.current.as_ref() == Some(&id) {
            return Ok(PublicationPlan::Deferred);
        }
        let object = reference.inventory.clone();
        let bytes = objects
            .fetch_metadata(&object, distribution::METADATA_LIMIT)
            .await?;
        let inventory: Inventory = serde_json::from_slice(&bytes)?;
        inventory.validate(self.config.activation_height)?;
        if inventory.genesis != self.config.genesis
            || &inventory.reference(object.clone()) != reference
        {
            return Err("Receiver metadata does not describe the authorized publication".into());
        }
        Ok(PublicationPlan::Candidate {
            required_objects: inventory.objects(),
            identity: id.clone(),
            candidate: Candidate {
                inventory,
                object,
                id,
            },
        })
    }
    async fn load(&mut self, candidate: Candidate, cache: &ObjectCache) -> Result<Loaded, Error> {
        // Metadata/public setup is small relative to rows; lazy row loaders
        // rebuild evaluators only under the existing shared table budget.
        let inventory = candidate.inventory.clone();
        let cache = cache.clone();
        let owner = self.owner.clone();
        let (directory, filters) =
            tokio::task::spawn_blocking(move || super::material::load(&inventory, &cache, owner))
                .await??;
        Ok(Loaded {
            candidate,
            epoch: self.publications.epoch(),
            directory,
            filters,
        })
    }
    async fn rebind(
        &mut self,
        _: &Envelope<Self>,
        loaded: Loaded,
        _: &mut ObjectFetcher<'_>,
    ) -> Result<Loaded, Error> {
        Ok(loaded)
    }
    fn revalidate(
        &self,
        envelope: &Envelope<Self>,
        loaded: &Loaded,
    ) -> Result<(ViewIdentity, Subject), Error> {
        let candidate = &loaded.candidate;
        if envelope.publication.as_ref()
            != Some(&candidate.inventory.reference(candidate.object.clone()))
        {
            return Err("Receiver publication advanced during loading".into());
        }
        Ok((candidate.id.clone(), candidate.inventory.anchors()))
    }
    async fn reserve_peer(&mut self, _: &Loaded) -> Result<PeerReservation, Error> {
        Ok(PeerReservation::Reserved)
    }
    fn view_objects(&self, loaded: &Loaded) -> BTreeSet<String> {
        std::iter::once(&loaded.candidate.object)
            .chain(loaded.candidate.inventory.objects().iter())
            .map(|object| object.sha256.clone())
            .collect()
    }
    async fn retained_views(&mut self) -> Vec<(ViewIdentity, ViewRequirement)> {
        self.expire();
        self.views
            .iter()
            .map(|view| (view.id.clone(), ViewRequirement::Serving))
            .collect()
    }
    fn prepare_commit(
        &mut self,
        loaded: Loaded,
    ) -> Result<(PreparedCommit<Self::Report>, View), Error> {
        let Loaded {
            candidate,
            epoch,
            directory,
            filters,
        } = loaded;
        let root = self.config.state_dir.clone();
        let bytes = serde_json::to_vec(&candidate.object)?;
        let record = Box::new(move || {
            pir_control::durable::write_atomic(&root, "active.json", |f| f.write_all(&bytes))?;
            pir_control::durable::sync_dir(&root)
        });
        let held = Arc::new(Mutex::new(None));
        let installed = held.clone();
        let publications = self.publications.clone();
        let revision = candidate.inventory.history_revision;
        let id = candidate.id.clone();
        let install = Box::pin(async move {
            let directory = publications.install_view(directory, filters, revision, epoch)?;
            *installed.lock().unwrap() = directory.as_ref().map(Arc::downgrade);
            Ok(Some(id))
        });
        let publications = self.publications.clone();
        let revoke_after_failed_record = Box::pin(async move {
            publications.revoke();
        });
        Ok((
            PreparedCommit {
                record,
                install,
                revoke_after_failed_record,
            },
            View {
                id: candidate.id,
                inventory: candidate.inventory,
                directory: held,
            },
        ))
    }
    fn installed(&mut self, view: View) {
        self.current = Some(view.id.clone());
        self.views.push(view);
        self.expire();
    }
    async fn release_peer(&mut self, _: &Self::Report) -> PeerHandover {
        PeerHandover::Complete
    }
    async fn serving_report(&mut self) -> Self::Report {
        if self.publications.current_anchor().is_none()
            && self.publications.filter_window().is_none()
        {
            None
        } else {
            self.current.clone()
        }
    }
}
