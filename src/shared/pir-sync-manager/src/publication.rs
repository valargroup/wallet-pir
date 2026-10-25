//! The existing HTTPS publication-envelope protocol as services for
//! `pir_pipeline::consumer::ConsumerEngine`. This module supplies discovery,
//! source-bound floors, object transfer, authority queries and durable owner
//! actions. The transport-neutral engine owns phase ordering and loaded work.
//! Product adapters retain material formats, authority subjects and serving policy.
//!
//! Ordering guarantees:
//! - Negative evidence (withdrawal, a new recovery epoch, a non-authoritative
//!   answer) fences serving through the adapter before the floor is persisted,
//!   so a failing write can never leave a fenced view serving.
//! - The floor is dirty until its latest version is durably synced; a dirty
//!   floor blocks every positive step, even under an unchanged envelope.
//! - A pass commits only after re-observing the envelope and a live authority
//!   check, then rechecks the floor. Floor writes and commits run as owned
//!   tasks; a cancelled pass leaves its task outstanding and the next pass
//!   settles it before observing anything. A commit installs only if no fence
//!   was raised while it was recording, and collects unreferenced objects
//!   inside its owned operation. Handing the view to the adapter stays pending
//!   across cancelled passes until done; releasing the dependent peer stays
//!   owed until a release completes or a newer commit supersedes it.
//! - Objects stay cached while a view the adapter still needs references them
//!   ([`ViewRequirement`]); every other object is collected at the next commit.
//!   While the adapter still needs a view this lifecycle never committed (for
//!   example one installed from local state), its objects are unknown and no
//!   commit collects anything.
//! - A 304 or a successful response refreshes neither source-observation age
//!   nor authority-check time; the product's freshness checks still apply.
mod authority_round;
mod commit_phase;
#[cfg(test)]
mod completion_tests;
mod durable_operation;
mod envelope_observation;

pub use crate::objects::ObjectFetcher;
use crate::{Error, HttpsSource};
use commit_phase::PendingCompletion;
use durable_operation::DurableOutcome;
pub use pir_pipeline::consumer::durable::PreparedCommit;
use pir_pipeline::consumer::durable::{CommitFence, OwnedOperationSlot};
use pir_pipeline::{
    consumer::{
        Consumer, ConsumerEngine, ConsumerPass, DeferralReason, PeerHandover, PeerReservation,
    },
    Input,
};
use pir_publication::RetainedViews;
pub use pir_publication::ViewRequirement;
use pir_publication::{
    AuthorityFloor, ObjectCache, ObjectReference, PublicationEnvelope, VerifiedObjects,
    ViewIdentity,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::BTreeSet, fmt, future::Future, time::Duration};

/// Product hooks for one query-server role.
pub trait PublicationAdapter: Send {
    /// The product's typed part of a [`PublicationEnvelope`]; every other
    /// envelope field and its succession rule are shared.
    type Descriptor: Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static;
    /// What the publisher checks per serving view (for Status, an anchor).
    /// A positive answer confirms the subject only at the publisher's check;
    /// how long a view may then serve stays the adapter's own freshness rule
    /// (for Status, the manifest's observation age at admission).
    type AuthoritySubject: Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync;
    /// What the adapter keeps about a view once it is installed (for Status,
    /// the inventory whose material may later be re-identified).
    type InstalledView: Send + 'static;
    /// Planned material whose objects the engine fetches before loading.
    type Candidate: Send + Sync;
    /// Material loaded from verified objects, ready to install.
    type Loaded: Send + Sync + 'static;
    /// What a committed pass reports, for example the serving generation.
    type Report: Clone + Send + Sync + 'static;

    /// Product checks of an envelope's scope and descriptor. The engine has
    /// already checked the shared fields; succession against the floor is the
    /// engine's job.
    fn validate_envelope(&self, envelope: &Envelope<Self>) -> Result<(), Error>;
    /// Stop serving every installed view and drop held handover views, and
    /// return the identities of views that were serving so the floor can
    /// invalidate them permanently.
    /// The returned action owns its state so the engine can finish it after
    /// the pass that requested revocation is cancelled.
    fn fence_serving(&mut self) -> impl Future<Output = Vec<ViewIdentity>> + Send + 'static;
    /// Stop answering from exactly `views`, including requests already in
    /// flight, and release their peer holds. Other views remain available.
    /// The returned action owns its state and changes nothing until polled.
    fn invalidate_views(
        &mut self,
        views: Vec<ViewIdentity>,
    ) -> impl Future<Output = ()> + Send + 'static;
    /// Close admission after a durable floor write failed.
    fn close_admission(&mut self) -> impl Future<Output = ()> + Send + 'static;
    /// Installed views that must stay authoritative under `envelope`.
    fn serving_subjects(
        &mut self,
        envelope: &Envelope<Self>,
    ) -> impl Future<Output = Vec<(ViewIdentity, Self::AuthoritySubject)>> + Send;
    /// Split authority proofs into independently bounded requests. Each returned
    /// proof keeps its view identity; a negative chunk invalidates that view.
    fn authority_batches(
        &self,
        subjects: Vec<(ViewIdentity, Self::AuthoritySubject)>,
    ) -> Vec<Vec<(ViewIdentity, Self::AuthoritySubject)>> {
        vec![subjects]
    }
    /// Choose what to load under `envelope`, fetching metadata through
    /// `objects`. The engine refuses a plan whose identity the floor invalidated.
    fn plan(
        &mut self,
        envelope: &Envelope<Self>,
        objects: &mut ObjectFetcher<'_>,
    ) -> impl Future<Output = Result<PublicationPlan<Self::Candidate>, Error>> + Send;
    /// Load from the exact checked files obtained for this candidate.
    fn load(
        &mut self,
        candidate: Self::Candidate,
        objects: VerifiedObjects,
    ) -> impl Future<Output = Result<Self::Loaded, Error>> + Send;
    /// Re-identify `loaded` under the re-observed `envelope` when that
    /// publishes the same reusable material under new metadata, as a
    /// publisher refreshing its observation during a long load does, fetching
    /// only metadata through `objects`. Otherwise return `loaded` unchanged. [`Self::revalidate`]
    /// and the live authority check still decide whether it may commit.
    fn rebind(
        &mut self,
        envelope: &Envelope<Self>,
        loaded: Self::Loaded,
        objects: &mut ObjectFetcher<'_>,
    ) -> impl Future<Output = Result<Self::Loaded, Error>> + Send;
    /// Product checks that `loaded` still belongs to the re-observed `envelope`
    /// (identity, recovery epoch, freshness), and the subject to check live.
    fn revalidate(
        &self,
        envelope: &Envelope<Self>,
        loaded: &Self::Loaded,
    ) -> Result<(ViewIdentity, Self::AuthoritySubject), Error>;
    /// Whether verified, loaded work may survive an authority conflict. This
    /// grants no serving authority: every retry observes the floor and repeats
    /// revalidation, the live authority check and peer reservation. Products
    /// that require exactly the latest material keep the default.
    /// The identity of reusable prepared work, before it has been rebound to
    /// the latest metadata. This never authorizes installation or serving.
    fn resumable_view(
        &self,
        _loaded: &Self::Loaded,
        _envelope: &Envelope<Self>,
    ) -> Option<ViewIdentity> {
        None
    }
    /// Report expected coordination without turning it into failure backoff.
    fn deferred(&mut self, _reason: DeferralReason) -> impl Future<Output = ()> + Send {
        async {}
    }
    /// Timing of a completed phase; phase names are fixed by the engine.
    fn phase_finished(&mut self, _phase: &'static str, _elapsed: Duration) {}
    /// Reserve the selected view before transfer. Products without peers do
    /// not acquire a promise. Reservation grants no serving authority.
    fn reserve_candidate(
        &mut self,
        _candidate: &Self::Candidate,
    ) -> impl Future<Output = Result<PeerReservation, Error>> + Send {
        async { Ok(PeerReservation::Reserved) }
    }
    /// Reserve `loaded` with a dependent peer before committing (for a Status
    /// router, the worker's matching generation).
    fn reserve_peer(
        &mut self,
        loaded: &Self::Loaded,
    ) -> impl Future<Output = Result<PeerReservation, Error>> + Send;
    /// Objects the view of `loaded` references while it is retained.
    fn view_objects(&self, loaded: &Self::Loaded) -> BTreeSet<String>;
    /// Every committed view the adapter still needs, and why: each view that
    /// can answer a request and each view a peer holds. The engine keeps the
    /// objects of these views and of the view it is committing, and collects
    /// every other object inside that commit, unless one of these views was
    /// never committed by this lifecycle: then nothing is collected.
    fn retained_views(
        &mut self,
    ) -> impl Future<Output = Vec<(ViewIdentity, ViewRequirement)>> + Send;
    /// Split `loaded` into the engine-run commit (record, install, revoke on a
    /// failed record) and what the adapter keeps once the install succeeded.
    /// Nothing here may change serving state.
    fn prepare_commit(
        &mut self,
        loaded: Self::Loaded,
    ) -> Result<(PreparedCommit<Self::Report>, Self::InstalledView), Error>;
    /// The engine installed the view prepared with `view`. Called exactly
    /// once per installed commit, even when the pass that installed it was
    /// cancelled; it is local and must not fail.
    fn installed(&mut self, view: Self::InstalledView);
    /// Tell a dependent peer the commit happened. The outcome of a network
    /// release can be uncertain, so it must be idempotent: a release cut off
    /// by cancellation or answered [`PeerHandover::Pending`] stays owed, and
    /// each later pass makes one release attempt, bounded by the adapter, and
    /// awaits its answer before it observes fresh authority, until a release
    /// completes or a newer commit's release supersedes it.
    fn release_peer(&mut self, report: &Self::Report) -> impl Future<Output = PeerHandover> + Send;
    /// The report of the view still serving, for deferred passes.
    fn serving_report(&mut self) -> impl Future<Output = Self::Report> + Send;
    /// A pass is starting, for status reporting.
    fn pass_started(&mut self) -> impl Future<Output = ()> + Send {
        async {}
    }
    /// Observe the outcome of a pass, for status and logging.
    fn pass_finished(
        &mut self,
        _result: &Result<ConsumerPass<Self::Report>, Error>,
    ) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// The envelope a [`PublicationAdapter`] publishes under.
pub type Envelope<A> = PublicationEnvelope<<A as PublicationAdapter>::Descriptor>;

/// What a pass should do under the observed envelope.
pub enum PublicationPlan<C> {
    /// Nothing can be loaded yet (for example a peer is still loading or a
    /// preparation slot is busy). The current view keeps its own checks.
    Deferred,
    /// Load `candidate` once `required_objects` are fetched and verified.
    Candidate {
        candidate: C,
        identity: ViewIdentity,
        required_objects: Vec<ObjectReference>,
    },
}

/// Distinct lifecycle refusals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationLifecycleError {
    /// A non-success, non-304 answer to an envelope request.
    EnvelopeUnavailable(u16),
    /// A 304 without a previously observed envelope.
    UnexpectedNotModified,
    /// The publisher refused an authority check for a reason other than an
    /// advanced authority.
    AuthorityCheckRefused(u16),
    /// The authority response binds another authority, or answers subjects
    /// other than exactly the requested ones in their order.
    AuthorityResponseMismatch,
    /// The candidate was invalidated by the floor.
    CandidateInvalidated,
    /// The envelope was withdrawn between planning and commit.
    WithdrawnBeforeCommit,
    /// The envelope's shared fields are inconsistent.
    MalformedEnvelope,
    /// The latest floor is not durable yet, so nothing may progress.
    FloorNotDurable,
    /// Serving was fenced while the commit was recording; nothing installed.
    CommitFenced,
}
impl fmt::Display for PublicationLifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EnvelopeUnavailable(status) => {
                write!(f, "publication envelope unavailable (HTTP {status})")
            }
            Self::UnexpectedNotModified => f.write_str("unexpected unchanged envelope response"),
            Self::AuthorityCheckRefused(status) => {
                write!(f, "publisher refused the authority check (HTTP {status})")
            }
            Self::AuthorityResponseMismatch => f.write_str("authority check response differs"),
            Self::CandidateInvalidated => f.write_str("publication candidate was invalidated"),
            Self::WithdrawnBeforeCommit => f.write_str("publication withdrawn before commit"),
            Self::MalformedEnvelope => f.write_str("publication envelope is malformed"),
            Self::FloorNotDurable => f.write_str("publication floor is not durable yet"),
            Self::CommitFenced => f.write_str("serving was fenced while the commit was recording"),
        }
    }
}
impl std::error::Error for PublicationLifecycleError {}

/// A newer envelope won the authority check; nothing was committed.
#[derive(Debug)]
struct AuthorityAdvanced;
impl fmt::Display for AuthorityAdvanced {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("publication authority advanced during the authority check")
    }
}
impl std::error::Error for AuthorityAdvanced {}

/// Owns one role's publication source, floor, cache and adapter.
pub struct EnvelopeConsumer<A: PublicationAdapter> {
    adapter: A,
    source: HttpsSource,
    floor: AuthorityFloor<A::Descriptor>,
    cache: ObjectCache,
    /// The last envelope body received, used only to answer a 304.
    last_received_envelope: Option<Envelope<A>>,
    /// Objects of the views the adapter still needs; everything else is
    /// collected at the next commit.
    retained_views: RetainedViews,
    commit_fence: CommitFence,
    durable_operation: OwnedOperationSlot<DurableOutcome<A::Report, A::InstalledView>>,
    /// An installed commit whose view or peer release is still outstanding.
    pending_completion: Option<PendingCompletion<A::Report, A::InstalledView>>,
    /// The latest commit whose peer release answered pending.
    owed_peer_release: Option<A::Report>,
    /// The first observation distinguishes a changing authority from refusal.
    first_observation: Option<Envelope<A>>,
    observation_fenced: bool,
}

impl<A: PublicationAdapter> EnvelopeConsumer<A> {
    /// `floor` was opened for `source`'s origin, namespace and chain; `cache`
    /// is exclusively owned by this role.
    pub fn new(
        adapter: A,
        source: HttpsSource,
        floor: AuthorityFloor<A::Descriptor>,
        cache: ObjectCache,
    ) -> Result<Self, Error> {
        if let Some(envelope) = floor.envelope() {
            adapter.validate_envelope(envelope)?;
        }
        if floor.source().publisher_origin != source.origin().as_str() {
            return Err("publication floor and source name different publishers".into());
        }
        Ok(Self {
            adapter,
            source,
            floor,
            cache,
            last_received_envelope: None,
            retained_views: RetainedViews::default(),
            commit_fence: CommitFence::default(),
            durable_operation: OwnedOperationSlot::new(),
            pending_completion: None,
            owed_peer_release: None,
            first_observation: None,
            observation_fenced: false,
        })
    }
    pub fn adapter(&self) -> &A {
        &self.adapter
    }
    pub fn floor(&self) -> &AuthorityFloor<A::Descriptor> {
        &self.floor
    }

    /// Construct the shared transport-neutral engine for this envelope source.
    pub fn open(
        adapter: A,
        source: HttpsSource,
        floor: AuthorityFloor<A::Descriptor>,
        cache: ObjectCache,
    ) -> Result<ConsumerEngine<Self>, Error> {
        Ok(ConsumerEngine::new(Self::new(
            adapter, source, floor, cache,
        )?))
    }
}

/// Selected inventory and its verified transfer requirements. Kept together so
/// products cannot substitute a different identity after planning.
pub struct SelectedPublication<C> {
    candidate: C,
    required_objects: Vec<ObjectReference>,
}

impl<A: PublicationAdapter> Consumer for EnvelopeConsumer<A> {
    type Observation = Envelope<A>;
    type Candidate = SelectedPublication<A::Candidate>;
    type Loaded = A::Loaded;
    type Artifacts = VerifiedObjects;
    type Validation = (ViewIdentity, A::AuthoritySubject);
    type Report = A::Report;

    async fn settle(&mut self) -> Result<(), Error> {
        self.settle_durable_operation().await?;
        self.source.discover().await?;
        self.source.reset_budget();
        self.first_observation = None;
        Ok(())
    }
    async fn observe(&mut self) -> Result<Self::Observation, Error> {
        let observed = self.observe_envelope().await?;
        if self.first_observation.is_some() && !observed.eligible {
            return Err(Box::new(PublicationLifecycleError::WithdrawnBeforeCommit));
        }
        self.first_observation
            .get_or_insert_with(|| observed.clone());
        Ok(observed)
    }
    fn eligible(&self, observed: &Self::Observation) -> bool {
        observed.eligible
    }
    fn invalidates_preparation(&self, _: &Self::Observation) -> bool {
        self.observation_fenced
    }
    fn keep_loaded(&self, loaded: &A::Loaded) -> bool {
        self.floor.envelope().is_some_and(|envelope| {
            self.adapter
                .resumable_view(loaded, envelope)
                .is_some_and(|identity| !self.floor.is_invalidated(&identity))
        })
    }
    async fn check_serving(&mut self, observed: &Self::Observation) -> Result<(), Error> {
        let subjects = self.adapter.serving_subjects(observed).await;
        if !subjects.is_empty() {
            self.check_authority(observed, subjects).await?;
        }
        Ok(())
    }
    async fn select(
        &mut self,
        observed: &Self::Observation,
    ) -> Result<Input<Self::Candidate>, Error> {
        let mut objects = ObjectFetcher::new(&mut self.source, &self.cache);
        match self.adapter.plan(observed, &mut objects).await? {
            PublicationPlan::Deferred => Ok(Input::Deferred),
            PublicationPlan::Candidate {
                candidate,
                identity,
                required_objects,
            } => {
                if self.floor.is_invalidated(&identity) {
                    return Err(Box::new(PublicationLifecycleError::CandidateInvalidated));
                }
                Ok(Input::Work(SelectedPublication {
                    candidate,
                    required_objects,
                }))
            }
        }
    }
    async fn reserve(&mut self, selected: &Self::Candidate) -> Result<PeerReservation, Error> {
        self.adapter.reserve_candidate(&selected.candidate).await
    }
    async fn obtain(&mut self, selected: &Self::Candidate) -> Result<VerifiedObjects, Error> {
        let mut fetcher = ObjectFetcher::new(&mut self.source, &self.cache);
        let mut artifacts = VerifiedObjects::default();
        for object in &selected.required_objects {
            artifacts.insert(fetcher.fetch(object).await?);
        }
        Ok(artifacts)
    }
    async fn load(
        &mut self,
        selected: Self::Candidate,
        artifacts: VerifiedObjects,
    ) -> Result<A::Loaded, Error> {
        self.adapter.load(selected.candidate, artifacts).await
    }
    async fn rebind(
        &mut self,
        observed: &Self::Observation,
        loaded: A::Loaded,
    ) -> Result<A::Loaded, Error> {
        let mut objects = ObjectFetcher::new(&mut self.source, &self.cache);
        self.adapter.rebind(observed, loaded, &mut objects).await
    }
    fn validate(
        &self,
        observed: &Self::Observation,
        loaded: &A::Loaded,
    ) -> Result<Self::Validation, Error> {
        match self.adapter.revalidate(observed, loaded) {
            Ok(checked) => Ok(checked),
            Err(_) if Some(observed) != self.first_observation.as_ref() => {
                Err(Box::new(AuthorityAdvanced))
            }
            Err(error) => Err(error),
        }
    }
    async fn authorize(
        &mut self,
        observed: &Self::Observation,
        checked: &Self::Validation,
    ) -> Result<(), Error> {
        let mut subjects = vec![(checked.0, checked.1.clone())];
        subjects.extend(
            self.adapter
                .serving_subjects(observed)
                .await
                .into_iter()
                .filter(|(identity, _)| *identity != checked.0),
        );
        self.check_authority(observed, subjects).await
    }
    async fn confirm(&mut self, loaded: &A::Loaded) -> Result<PeerReservation, Error> {
        self.adapter.reserve_peer(loaded).await
    }
    async fn commit(
        &mut self,
        loaded: A::Loaded,
        checked: Self::Validation,
    ) -> Result<(A::Report, PeerHandover), Error> {
        EnvelopeConsumer::commit(self, checked.0, loaded).await
    }
    async fn serving_report(&mut self) -> A::Report {
        self.adapter.serving_report().await
    }
    fn deferral(&self, error: &Error) -> Option<DeferralReason> {
        error
            .is::<AuthorityAdvanced>()
            .then_some(DeferralReason::AuthorityAdvanced)
    }
    async fn deferred(&mut self, reason: DeferralReason) {
        self.adapter.deferred(reason).await;
    }
    fn terminal(&self, error: &Error) -> bool {
        crate::is_incompatible(error.as_ref())
    }
    fn phase_finished(&mut self, phase: &'static str, elapsed: Duration) {
        self.adapter.phase_finished(phase, elapsed);
    }
    async fn pass_started(&mut self) {
        self.adapter.pass_started().await;
    }
    async fn pass_finished(&mut self, result: &Result<ConsumerPass<A::Report>, Error>) {
        self.adapter.pass_finished(result).await;
    }
}
