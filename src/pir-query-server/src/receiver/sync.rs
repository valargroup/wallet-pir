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
            views: Arc::default(),
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
    views: Arc<Mutex<Views>>,
}
#[derive(Default)]
struct Views {
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
    #[cfg(test)]
    pub(super) fn check_unused_revocations(&self) {
        let mut copy = Self {
            config: self.config.clone(),
            owner: self.owner.clone(),
            publications: self.publications.clone(),
            views: self.views.clone(),
        };
        let before = self.views.lock().unwrap().views.len();
        drop(copy.fence_serving());
        drop(copy.close_admission());
        assert_eq!(self.views.lock().unwrap().views.len(), before);
        assert!(self.publications.current_anchor().is_some());
    }

    fn revoke(&self) -> impl std::future::Future<Output = Vec<ViewIdentity>> + Send + 'static {
        let views = self.views.clone();
        let publications = self.publications.clone();
        async move {
            let mut views = views.lock().unwrap();
            views.current = None;
            let ids = views.views.drain(..).map(|view| view.id).collect();
            publications.revoke();
            ids
        }
    }
    fn expire(&mut self) {
        drop(self.publications.live());
        let mut views = self.views.lock().unwrap();
        let current = views.current;
        views.views.retain(|view| {
            Some(&view.id) == current.as_ref()
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
        self.revoke()
    }
    fn invalidate_views(
        &mut self,
        rejected: Vec<ViewIdentity>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let views = self.views.clone();
        let publications = self.publications.clone();
        async move {
            let mut views = views.lock().unwrap();
            let mut directories = std::collections::HashSet::new();
            let mut filters = Vec::new();
            for view in views
                .views
                .iter()
                .filter(|view| rejected.contains(&view.id))
            {
                if let Some(directory) = &view.inventory.directory {
                    directories.insert(directory.head.inventory);
                }
                if let Some(window) = &view.inventory.filters {
                    filters.push(window.head.clone());
                }
            }
            views.views.retain(|view| !rejected.contains(&view.id));
            for view in &views.views {
                if let Some(directory) = &view.inventory.directory {
                    directories.remove(&directory.head.inventory);
                }
                if let Some(window) = &view.inventory.filters {
                    filters.retain(|head| head != &window.head);
                }
            }
            if views
                .current
                .as_ref()
                .is_some_and(|id| rejected.contains(id))
            {
                views.current = None;
            }
            publications.invalidate(&directories, None);
            for head in filters {
                publications.invalidate(&std::collections::HashSet::new(), Some(&head));
            }
        }
    }
    fn close_admission(&mut self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let revoke = self.revoke();
        async move {
            revoke.await;
        }
    }
    async fn serving_subjects(&mut self, _: &Envelope<Self>) -> Vec<(ViewIdentity, Subject)> {
        self.expire();
        self.views
            .lock()
            .unwrap()
            .views
            .iter()
            .map(|view| (view.id, view.inventory.anchors()))
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
        if self.views.lock().unwrap().current.as_ref() == Some(&id) {
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
            identity: id,
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
        Ok((candidate.id, candidate.inventory.anchors()))
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
            .lock()
            .unwrap()
            .views
            .iter()
            .map(|view| (view.id, ViewRequirement::Serving))
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
        let id = candidate.id;
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
        {
            let mut views = self.views.lock().unwrap();
            views.current = Some(view.id);
            views.views.push(view);
        }
        self.expire();
    }
    async fn release_peer(&mut self, _: &Self::Report) -> PeerHandover {
        PeerHandover::Complete
    }
    async fn pass_finished(
        &mut self,
        result: &Result<
            crate::synchronization::publication_lifecycle::PublicationPass<Self::Report>,
            Error,
        >,
    ) {
        if let Err(error) = result {
            tracing::warn!(%error, "Receiver publication synchronization failed");
        }
    }
    async fn serving_report(&mut self) -> Self::Report {
        if self.publications.current_anchor().is_none()
            && self.publications.filter_window().is_none()
        {
            None
        } else {
            self.views.lock().unwrap().current
        }
    }
}
