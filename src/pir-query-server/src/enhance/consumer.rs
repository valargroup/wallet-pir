//! Enhance's existing source protocol composed with the shared consumer engine.
//! Roles supply assignment, material preparation and installation policy only.
use super::synchronization::{Client, Deferral, Error, Lease, Safety};
use enhance_shared::{
    protocol::Manifest,
    publication::{AuthorityResponse, Object},
};
use pir_pipeline::consumer::durable::OwnedOperationSlot;
use pir_pipeline::{
    consumer::{Consumer, PeerHandover, PeerReservation},
    Input,
};
use pir_publication::VerifiedObjects;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
pub(super) type Cleanup = Pin<Box<dyn Future<Output = ()> + Send>>;
pub(super) struct Incoming<P> {
    pub manifest: Manifest,
    pub peers: Arc<Mutex<P>>,
}

pub(super) trait Role: Send {
    type Selection: Send + Sync;
    type Material: Send + Sync;
    type Promise: Send + Sync;
    fn safety(&self) -> &Safety;
    fn observed(&mut self) {}
    fn abandon(&mut self) -> Option<Cleanup> {
        None
    }
    fn select(
        &mut self,
        response: &AuthorityResponse,
        client: &mut Client,
    ) -> impl Future<Output = Result<Input<Self::Selection>, Error>> + Send;
    fn objects(&self, selection: &Self::Selection) -> Vec<Object>;
    fn reserve(
        &mut self,
        manifest: &Manifest,
        selection: &Self::Selection,
    ) -> impl Future<Output = Result<Option<Self::Promise>, Error>> + Send;
    fn load(
        &mut self,
        selection: Self::Selection,
        objects: VerifiedObjects,
    ) -> impl Future<Output = Result<Self::Material, Error>> + Send;
    fn confirm(
        &mut self,
        manifest: &Manifest,
        promise: &mut Self::Promise,
    ) -> impl Future<Output = Result<bool, Error>> + Send;
    fn install(
        &mut self,
        manifest: Manifest,
        material: Self::Material,
        promise: Self::Promise,
        lease: Lease,
    ) -> Result<(), Error>;
}

/// A role without downstream peer promises still participates in reservation
/// ordering, explicitly carrying this token through loading and activation.
pub(super) struct NoPeers;

pub(super) struct EnhanceConsumer<R: Role> {
    pub role: R,
    client: Client,
    cleanup: OwnedOperationSlot<()>,
}
impl<R: Role> EnhanceConsumer<R> {
    pub fn new(role: R, client: Client) -> Self {
        Self {
            role,
            client,
            cleanup: OwnedOperationSlot::new(),
        }
    }
}
pub(super) struct Selected<R: Role> {
    manifest: Manifest,
    observed: Lease,
    selection: R::Selection,
    promise: Option<R::Promise>,
}
pub(super) struct Loaded<R: Role> {
    manifest: Manifest,
    observed: Lease,
    material: R::Material,
    promise: R::Promise,
}
impl<R: Role> Consumer for EnhanceConsumer<R> {
    type Observation = AuthorityResponse;
    type Candidate = Selected<R>;
    type Artifacts = VerifiedObjects;
    type Loaded = Loaded<R>;
    type Validation = (Manifest, Lease);
    type Authority = Lease;
    type Report = ();
    async fn settle(&mut self) -> Result<(), Error> {
        if let Some(outcome) = self.cleanup.settle().await {
            outcome?;
        }
        if let Some(cleanup) = self.role.abandon() {
            self.cleanup.run(cleanup).await??;
        }
        self.client.begin().await
    }
    async fn observe(&mut self) -> Result<AuthorityResponse, Error> {
        let response = self.client.current().await?;
        self.role.observed();
        if response.current.manifest.is_some() && !response.current.eligible {
            return Err(Deferral::Withdrawn.into());
        }
        Ok(response)
    }
    fn eligible(&self, response: &AuthorityResponse) -> bool {
        response.current.manifest.is_some()
    }
    fn keep_loaded(&self, _: &Self::Loaded) -> bool {
        false
    }
    async fn check_serving(&mut self, _: &AuthorityResponse) -> Result<(), Error> {
        Ok(())
    }
    async fn select(
        &mut self,
        response: &AuthorityResponse,
    ) -> Result<Input<Self::Candidate>, Error> {
        let manifest = response
            .current
            .manifest
            .as_ref()
            .ok_or("missing Enhance manifest")?;
        let observed = Lease::from_response(response.clone(), manifest, self.role.safety())?;
        Ok(match self.role.select(response, &mut self.client).await? {
            Input::Work(selection) => Input::Work(Selected {
                manifest: manifest.clone(),
                observed,
                selection,
                promise: None,
            }),
            Input::Unchanged => Input::Unchanged,
            Input::Deferred => Input::Deferred,
        })
    }
    async fn reserve(&mut self, candidate: &mut Self::Candidate) -> Result<PeerReservation, Error> {
        candidate.promise = self
            .role
            .reserve(&candidate.manifest, &candidate.selection)
            .await?;
        Ok(if candidate.promise.is_some() {
            PeerReservation::Reserved
        } else {
            PeerReservation::PeerAdvanced
        })
    }
    async fn obtain(&mut self, candidate: &Self::Candidate) -> Result<VerifiedObjects, Error> {
        let mut objects = VerifiedObjects::default();
        for object in self.role.objects(&candidate.selection) {
            objects.insert(self.client.object(&object).await?);
        }
        Ok(objects)
    }
    async fn load(
        &mut self,
        candidate: Self::Candidate,
        objects: VerifiedObjects,
    ) -> Result<Self::Loaded, Error> {
        let material = self.role.load(candidate.selection, objects).await?;
        Ok(Loaded {
            manifest: candidate.manifest,
            observed: candidate.observed,
            material,
            promise: candidate
                .promise
                .ok_or("Enhance candidate was not reserved")?,
        })
    }
    async fn rebind(
        &mut self,
        _: &AuthorityResponse,
        loaded: Self::Loaded,
    ) -> Result<Self::Loaded, Error> {
        Ok(loaded)
    }
    fn validate(
        &self,
        _: &AuthorityResponse,
        loaded: &Self::Loaded,
    ) -> Result<Self::Validation, Error> {
        self.role.safety().permits(&loaded.manifest)?;
        Ok((loaded.manifest.clone(), loaded.observed.clone()))
    }
    async fn authorize(
        &mut self,
        _: &AuthorityResponse,
        validation: &Self::Validation,
    ) -> Result<Lease, Error> {
        self.client.renew(&validation.0, &validation.1).await
    }
    async fn confirm(&mut self, loaded: &mut Self::Loaded) -> Result<PeerReservation, Error> {
        Ok(
            if self
                .role
                .confirm(&loaded.manifest, &mut loaded.promise)
                .await?
            {
                PeerReservation::Reserved
            } else {
                PeerReservation::PeerAdvanced
            },
        )
    }
    async fn commit(
        &mut self,
        loaded: Self::Loaded,
        _: Self::Validation,
        lease: Lease,
    ) -> Result<((), PeerHandover), Error> {
        lease.check(&loaded.manifest, self.role.safety())?;
        self.role
            .install(loaded.manifest, loaded.material, loaded.promise, lease)?;
        Ok(((), PeerHandover::Complete))
    }
    async fn serving_report(&mut self) {}
}
