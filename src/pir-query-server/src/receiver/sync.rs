//! Product hooks; envelope succession, transfers and authority belong to the
//! shared lifecycle. Cached material alone never opens serving admission.
use super::Config;
use pir_pipeline::consumer::{ConsumerEngine, ConsumerPass, PeerHandover, PeerReservation};
use pir_publication::{AuthorityFloor, ObjectCache, ObjectReference, SourceBinding, ViewIdentity};
use pir_sync_manager::publication::{
    Envelope, EnvelopeConsumer, ObjectFetcher, PreparedCommit, PublicationAdapter, PublicationPlan,
    ViewRequirement,
};
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

pub(super) type Lifecycle = ConsumerEngine<EnvelopeConsumer<Receiver>>;
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
    EnvelopeConsumer::open(
        Receiver {
            config: config.clone(),
            owner,
            publications,
            views: Arc::default(),
            tables: None,
            #[cfg(test)]
            loads: 0,
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
    /// The tables last loaded, reused while publications name the same files.
    tables: Option<super::material::Tables>,
    #[cfg(test)]
    loads: usize,
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
    reusable: bool,
    directory: Option<Publication>,
    filters: Option<BlockWindow>,
}
impl Receiver {
    #[cfg(test)]
    pub(super) fn directory_loads(&self) -> usize {
        self.loads
    }
    #[cfg(test)]
    pub(super) fn check_unused_revocations(&self) {
        let mut copy = Self {
            config: self.config.clone(),
            owner: self.owner.clone(),
            publications: self.publications.clone(),
            views: self.views.clone(),
            tables: self.tables.clone(),
            loads: self.loads,
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
    async fn load(
        &mut self,
        candidate: Candidate,
        artifacts: pir_publication::VerifiedObjects,
    ) -> Result<Loaded, Error> {
        // Metadata/public setup is small relative to rows; lazy row loaders
        // rebuild evaluators only under the existing shared table budget, and
        // tables new since the last publication are prepared here, before serving.
        let inventory = candidate.inventory.clone();
        let owner = self.owner.clone();
        let previous = self.tables.clone();
        let cache = self.publications.tables();
        let (directory, filters, tables) = tokio::task::spawn_blocking(move || {
            super::material::load(&inventory, &artifacts, owner, previous, &cache)
        })
        .await??;
        #[cfg(test)]
        if tables.is_some()
            && tables.as_ref().map(|t| &t.key) != self.tables.as_ref().map(|t| &t.key)
        {
            self.loads += 1;
        }
        // A publication without tables lets their objects be collected; fetched
        // again they are new files, which the cached sources' readers refuse.
        self.tables = tables;
        Ok(Loaded {
            candidate,
            epoch: self.publications.epoch(),
            reusable: true,
            directory,
            filters,
        })
    }
    async fn rebind(
        &mut self,
        envelope: &Envelope<Self>,
        mut loaded: Loaded,
        objects: &mut ObjectFetcher<'_>,
    ) -> Result<Loaded, Error> {
        let Some(latest) = envelope.publication.as_ref().filter(|_| envelope.eligible) else {
            return Ok(loaded);
        };
        if latest
            == &loaded
                .candidate
                .inventory
                .reference(loaded.candidate.object.clone())
        {
            return Ok(loaded);
        }
        let inventory: Inventory = serde_json::from_slice(
            &objects
                .fetch_metadata(&latest.inventory, distribution::METADATA_LIMIT)
                .await?,
        )?;
        inventory.validate(self.config.activation_height)?;
        if inventory.genesis != self.config.genesis
            || inventory.reference(latest.inventory.clone()) != *latest
        {
            return Err("Receiver replacement metadata does not describe its publication".into());
        }
        if inventory.history_revision != loaded.candidate.inventory.history_revision
            || serde_json::to_vec(&inventory.directory)?
                != serde_json::to_vec(&loaded.candidate.inventory.directory)?
        {
            loaded.reusable = false;
            return Ok(loaded);
        }
        let mut checked = pir_publication::VerifiedObjects::default();
        if let Some(filters) = &inventory.filters {
            for batch in &filters.batches {
                checked.insert(objects.fetch(batch).await?.file);
            }
        }
        let filter_inventory = inventory.clone();
        loaded.filters = tokio::task::spawn_blocking(move || {
            super::material::load_filters(&filter_inventory, &checked)
        })
        .await??;
        loaded.candidate = Candidate {
            inventory,
            object: latest.inventory.clone(),
            id: ViewIdentity(latest.publication),
        };
        Ok(loaded)
    }
    fn resumable_view(&self, loaded: &Loaded, envelope: &Envelope<Self>) -> Option<ViewIdentity> {
        let latest = envelope.publication.as_ref()?;
        (loaded.reusable
            && envelope.eligible
            && loaded.epoch == self.publications.epoch()
            && latest.descriptor.history_revision == loaded.candidate.inventory.history_revision
            && latest.descriptor.directory == loaded.candidate.inventory.descriptor().directory)
            .then_some(loaded.candidate.id)
    }
    fn authority_batches(
        &self,
        subjects: Vec<(ViewIdentity, Subject)>,
    ) -> Vec<Vec<(ViewIdentity, Subject)>> {
        proof_batches(subjects)
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
    async fn reserve_candidate(&mut self, _: &Candidate) -> Result<PeerReservation, Error> {
        Ok(pir_pipeline::consumer::reservations::NoPeers.reserve())
    }
    async fn reserve_peer(&mut self, _: &Loaded) -> Result<PeerReservation, Error> {
        Ok(pir_pipeline::consumer::reservations::NoPeers.reserve())
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
            ..
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
        pir_pipeline::consumer::reservations::NoPeers.complete()
    }
    async fn pass_finished(&mut self, result: &Result<ConsumerPass<Self::Report>, Error>) {
        match result {
            Err(error) => tracing::warn!(%error, "Receiver publication synchronization failed"),
            Ok(ConsumerPass::Deferred { report }) => {
                tracing::debug!(serving = ?report, "Receiver publication pass deferred")
            }
            Ok(ConsumerPass::Committed { report, .. }) => {
                tracing::debug!(serving = ?report, "Receiver publication committed")
            }
        }
    }
    fn phase_finished(&mut self, phase: &'static str, elapsed: std::time::Duration) {
        tracing::debug!(
            phase,
            elapsed_ms = elapsed.as_millis() as u64,
            "Receiver publication phase"
        );
    }
    async fn deferred(&mut self, reason: pir_pipeline::consumer::DeferralReason) {
        tracing::debug!(?reason, "Receiver publication deferred");
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

/// At most 128 anchors and 128 subjects leave room for the echoed proof and
/// authority within the shared 64 KiB response bound, and stay below the
/// publisher's request limits. Several chunks may refer to the same view.
fn proof_batches(subjects: Vec<(ViewIdentity, Subject)>) -> Vec<Vec<(ViewIdentity, Subject)>> {
    const ANCHORS: usize = 128;
    let mut batches = Vec::new();
    let mut batch = Vec::new();
    let mut count = 0;
    for (identity, subject) in subjects {
        let chunks: Vec<_> = if subject.is_empty() {
            vec![Vec::new()]
        } else {
            subject.chunks(ANCHORS).map(<[_]>::to_vec).collect()
        };
        for chunk in chunks {
            let weight = chunk.len().max(1);
            if count + weight > ANCHORS {
                batches.push(std::mem::take(&mut batch));
                count = 0;
            }
            batch.push((identity, chunk));
            count += weight;
        }
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    batches
}
