//! The one publication lifecycle every query-server product runs against its
//! publisher: observe -> fence -> plan -> fetch -> load -> revalidate -> commit
//! -> retain.
//!
//! The engine owns transport, the durable [`AuthorityFloor`], the object cache,
//! the authority-check protocol and every ordering rule. A product supplies a
//! [`PublicationAdapter`] whose hooks return typed decisions and perform only
//! product material operations (parse an inventory, load a generation, install
//! it); no hook runs its own synchronize, download or persistence loop.
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
mod object_fetch;
mod retention;

use crate::synchronization::SyncTask;
use commit_phase::PendingCompletion;
pub use durable_operation::PreparedCommit;
use durable_operation::{CommitFence, DurableOutcome, OwnedOperationSlot};
pub use object_fetch::ObjectFetcher;
use pir_publication::{
    AuthorityFloor, ObjectCache, ObjectReference, PublicationEnvelope, ViewIdentity,
};
use pir_sync_manager::{Error, HttpsSource};
use retention::RetainedViews;
pub use retention::ViewRequirement;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::BTreeSet,
    fmt,
    future::Future,
    time::{Duration, Instant},
};

/// Expected coordination outcomes, distinct from transport or validation errors.
#[derive(Clone, Copy, Debug)]
pub enum DeferralReason {
    AuthorityAdvanced,
    PeerAdvanced,
}

/// Product hooks for one query-server role.
pub trait PublicationAdapter: Send {
    /// The product's typed part of a [`PublicationEnvelope`]; every other
    /// envelope field and its succession rule are shared.
    type Descriptor: Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static;
    /// What the publisher checks per serving view (for Status, an anchor).
    /// A positive answer confirms the subject only at the publisher's check;
    /// how long a view may then serve stays the adapter's own freshness rule
    /// (for Status, the manifest's observation age at admission).
    type AuthoritySubject: PartialEq + Serialize + DeserializeOwned + Send + Sync;
    /// What the adapter keeps about a view once it is installed (for Status,
    /// the inventory whose material may later be re-identified).
    type InstalledView: Send + 'static;
    /// Planned material whose objects the engine fetches before loading.
    type Candidate: Send;
    /// Material loaded from verified objects, ready to install.
    type Loaded: Send + 'static;
    /// What a committed pass reports, for example the serving generation.
    type Report: Clone + Send + 'static;

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
    /// Load a candidate whose required objects are verified in `cache`.
    fn load(
        &mut self,
        candidate: Self::Candidate,
        cache: &ObjectCache,
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
        _result: &Result<PublicationPass<Self::Report>, Error>,
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

/// Whether a dependent peer accepted a reservation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerReservation {
    Reserved,
    /// The peer already moved to a newer publication; retry next pass.
    PeerAdvanced,
}

/// Whether a dependent peer acknowledged a commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerHandover {
    Complete,
    Pending,
}

/// The outcome of one successful pass.
#[derive(Debug)]
pub enum PublicationPass<R> {
    /// A candidate committed; `handover` reports the dependent peer.
    Committed { report: R, handover: PeerHandover },
    /// Nothing committed this pass: withdrawn, a newer envelope appeared during
    /// the authority check, or the plan deferred.
    Deferred { report: R },
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
pub struct PublicationLifecycle<A: PublicationAdapter> {
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
    /// At most one loaded candidate, never serialized or authorized by cache
    /// presence. Only an opted-in adapter can reuse it after a conflict.
    pending_loaded: Option<A::Loaded>,
}

impl<A: PublicationAdapter> PublicationLifecycle<A> {
    #[cfg(test)]
    pub(crate) async fn check_serving_proofs(
        &mut self,
        subjects: Vec<(ViewIdentity, A::AuthoritySubject)>,
    ) -> Result<(), Error> {
        let envelope = self.observe_envelope().await?;
        self.check_authority(&envelope, subjects).await
    }
    #[cfg(test)]
    pub(crate) fn view_was_invalidated(&self, identity: ViewIdentity) -> bool {
        self.floor.is_invalidated(&identity)
    }

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
            pending_loaded: None,
        })
    }
    pub fn adapter(&self) -> &A {
        &self.adapter
    }
    pub fn floor(&self) -> &AuthorityFloor<A::Descriptor> {
        &self.floor
    }

    async fn run_pass(&mut self) -> Result<PublicationPass<A::Report>, Error> {
        self.settle_durable_operation().await?;
        self.source.discover().await?;
        self.source.reset_budget();
        // Observe and fence.
        let envelope = self.observe_envelope().await?;
        self.retain_resumable();
        if !envelope.eligible {
            return Ok(self.deferred().await);
        }
        let serving = self.adapter.serving_subjects(&envelope).await;
        if !serving.is_empty() {
            self.check_authority(&envelope, serving).await?;
        }
        // Finish existing prepared work before selecting a newer candidate.
        if self.pending_loaded.is_none() {
            let started = Instant::now();
            let mut objects = ObjectFetcher::new(&mut self.source, &self.cache);
            let (candidate, identity, required_objects) =
                match self.adapter.plan(&envelope, &mut objects).await? {
                    PublicationPlan::Deferred => {
                        return Ok(PublicationPass::Deferred {
                            report: self.adapter.serving_report().await,
                        })
                    }
                    PublicationPlan::Candidate {
                        candidate,
                        identity,
                        required_objects,
                    } => (candidate, identity, required_objects),
                };
            if self.floor.is_invalidated(&identity) {
                return Err(Box::new(PublicationLifecycleError::CandidateInvalidated));
            }
            for object in &required_objects {
                objects.fetch(object).await?;
            }
            self.adapter.phase_finished("fetch", started.elapsed());
            let started = Instant::now();
            self.pending_loaded = Some(self.adapter.load(candidate, &self.cache).await?);
            self.adapter.phase_finished("load", started.elapsed());
        }
        // Reusing work never reuses its authority answer.
        let started = Instant::now();
        let latest = self.observe_envelope().await?;
        if !latest.eligible {
            return Err(Box::new(PublicationLifecycleError::WithdrawnBeforeCommit));
        }
        let mut objects = ObjectFetcher::new(&mut self.source, &self.cache);
        let Some(loaded) = self.pending_loaded.take() else {
            // Re-observation fenced the candidate while it was loading.
            self.adapter
                .deferred(DeferralReason::AuthorityAdvanced)
                .await;
            return Ok(self.deferred().await);
        };
        let loaded = self.adapter.rebind(&latest, loaded, &mut objects).await?;
        self.pending_loaded = Some(loaded);
        let validation = {
            let loaded = self
                .pending_loaded
                .as_ref()
                .expect("a candidate was rebound");
            self.adapter.revalidate(&latest, loaded)
        };
        let (identity, subject) = match validation {
            Ok(checked) => checked,
            // A newer envelope superseded the candidate; nothing was
            // activated, so retry at the normal cadence.
            Err(_) if latest != envelope => return Err(Box::new(AuthorityAdvanced)),
            Err(error) => return Err(error),
        };
        self.adapter.phase_finished("revalidate", started.elapsed());
        let mut subjects = vec![(identity, subject)];
        subjects.extend(
            self.adapter
                .serving_subjects(&latest)
                .await
                .into_iter()
                .filter(|(serving, _)| *serving != identity),
        );
        let started = Instant::now();
        self.check_authority(&latest, subjects).await?;
        self.adapter.phase_finished("authority", started.elapsed());
        let loaded = self
            .pending_loaded
            .as_ref()
            .expect("the candidate is pending");
        if self.adapter.reserve_peer(loaded).await? == PeerReservation::PeerAdvanced {
            // The peer no longer promises these bytes. Follow its new view
            // rather than repeatedly retrying an unavailable reservation.
            self.pending_loaded = None;
            self.adapter.deferred(DeferralReason::PeerAdvanced).await;
            return Ok(self.deferred().await);
        }
        let loaded = self
            .pending_loaded
            .take()
            .expect("the candidate is reserved");
        let started = Instant::now();
        let (report, handover) = self.commit(identity, loaded).await?;
        self.adapter.phase_finished("commit", started.elapsed());
        Ok(PublicationPass::Committed { report, handover })
    }

    fn retain_resumable(&mut self) {
        let keep = self
            .pending_loaded
            .as_ref()
            .zip(self.floor.envelope())
            .is_some_and(|(loaded, envelope)| {
                self.adapter
                    .resumable_view(loaded, envelope)
                    .is_some_and(|identity| !self.floor.is_invalidated(&identity))
            });
        if !keep {
            self.pending_loaded = None;
        }
    }

    async fn deferred(&mut self) -> PublicationPass<A::Report> {
        PublicationPass::Deferred {
            report: self.adapter.serving_report().await,
        }
    }
}

impl<A: PublicationAdapter> SyncTask for PublicationLifecycle<A> {
    type Report = PublicationPass<A::Report>;
    type Error = Error;

    async fn synchronize(&mut self) -> Result<Self::Report, Error> {
        self.adapter.pass_started().await;
        let result = match self.run_pass().await {
            // A newer envelope won the authority check. Nothing was activated;
            // retry at the normal cadence instead of transport backoff.
            Err(error) if error.is::<AuthorityAdvanced>() => {
                self.retain_resumable();
                self.adapter
                    .deferred(DeferralReason::AuthorityAdvanced)
                    .await;
                Ok(self.deferred().await)
            }
            Err(error) => {
                self.pending_loaded = None;
                Err(error)
            }
            result => result,
        };
        self.adapter.pass_finished(&result).await;
        result
    }
    fn terminal(&self, error: &Error) -> bool {
        pir_sync_manager::is_incompatible(error.as_ref())
    }
}
