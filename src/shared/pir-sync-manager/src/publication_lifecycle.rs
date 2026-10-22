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
//! - A pass commits only after re-observing the pointer and a live authority
//!   check, then rechecks the floor. The commit runs as an owned task: if the
//!   waiter is cancelled, the durable write and install still finish together.
//! - A 304 or a successful response refreshes neither source-observation age
//!   nor authority-check time; the product's freshness checks still apply.
mod authority_round;
mod object_fetch;
mod pointer_observation;

use crate::{Error, HttpsSource, SyncTask};
pub use object_fetch::ObjectFetcher;
use pir_publication::{
    AuthorityFloor, MaterialIdentity, ObjectCache, ObjectReference, PublicationPointer,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::BTreeSet, fmt, future::Future};

/// Product hooks for one query-server role.
pub trait PublicationAdapter: Send {
    type Pointer: PublicationPointer + Serialize + DeserializeOwned + Send + Sync + 'static;
    /// What the publisher checks per serving view (for Status, an anchor).
    type AuthoritySubject: Serialize + Send + Sync;
    /// Planned material whose objects the engine fetches before loading.
    type Candidate: Send;
    /// Material loaded from verified objects, ready to install.
    type Loaded: Send + 'static;
    /// What a committed pass reports, for example the serving generation.
    type Report: Send + 'static;

    /// Product checks of a pointer's own fields (schema, chain, required
    /// references). Succession against the floor is the engine's job.
    fn validate_pointer(&self, pointer: &Self::Pointer) -> Result<(), Error>;
    /// Stop serving every installed view and drop held handover views, and
    /// return the identities of views that were serving so the floor can
    /// invalidate them permanently.
    fn fence_serving(&mut self) -> impl Future<Output = Vec<MaterialIdentity>> + Send;
    /// Close admission after a durable floor or commit write failed.
    fn close_admission(&mut self) -> impl Future<Output = ()> + Send;
    /// Installed views that must stay authoritative under `pointer`.
    fn serving_subjects(
        &mut self,
        pointer: &Self::Pointer,
    ) -> impl Future<Output = Vec<(MaterialIdentity, Self::AuthoritySubject)>> + Send;
    /// Choose what to load under `pointer`, fetching metadata through
    /// `objects`. The engine refuses a plan whose identity the floor invalidated.
    fn plan(
        &mut self,
        pointer: &Self::Pointer,
        objects: &mut ObjectFetcher<'_>,
    ) -> impl Future<Output = Result<PublicationPlan<Self::Candidate>, Error>> + Send;
    /// Load a candidate whose required objects are verified in `cache`.
    fn load(
        &mut self,
        candidate: Self::Candidate,
        cache: &ObjectCache,
    ) -> impl Future<Output = Result<Self::Loaded, Error>> + Send;
    /// Product checks that `loaded` still belongs to the re-observed `pointer`
    /// (identity, recovery epoch, freshness), and the subject to check live.
    fn revalidate(
        &self,
        pointer: &Self::Pointer,
        loaded: &Self::Loaded,
    ) -> Result<(MaterialIdentity, Self::AuthoritySubject), Error>;
    /// Reserve `loaded` with a dependent peer before committing (for a Status
    /// router, the worker's matching generation).
    fn reserve_peer(
        &mut self,
        loaded: &Self::Loaded,
    ) -> impl Future<Output = Result<PeerReservation, Error>> + Send;
    /// Objects `loaded` keeps referenced after commit.
    fn retained_objects(&self, loaded: &Self::Loaded) -> BTreeSet<String>;
    /// Durably record and install `loaded`. The returned future is spawned and
    /// owned by the runtime, so it must not borrow the adapter.
    fn commit(
        &mut self,
        loaded: Self::Loaded,
    ) -> impl Future<Output = Result<Self::Report, Error>> + Send + 'static;
    /// Tell a dependent peer the commit happened.
    fn release_peer(
        &mut self,
        report: &Self::Report,
    ) -> impl Future<Output = PeerHandover> + Send;
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

/// What a pass should do under the observed pointer.
pub enum PublicationPlan<C> {
    /// Nothing can be loaded yet (for example a peer is still loading or a
    /// preparation slot is busy). The current view keeps its own checks.
    Deferred,
    /// Load `candidate` once `required_objects` are fetched and verified.
    Candidate {
        candidate: C,
        identity: MaterialIdentity,
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
    /// Nothing committed this pass: withdrawn, a newer pointer appeared during
    /// the authority check, or the plan deferred.
    Deferred { report: R },
}

/// Distinct lifecycle refusals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationLifecycleError {
    /// A non-success, non-304 answer to a pointer request.
    PointerUnavailable(u16),
    /// A 304 without a previously observed pointer.
    UnexpectedNotModified,
    /// The publisher refused an authority check for a reason other than an
    /// advanced pointer.
    AuthorityCheckRefused(u16),
    /// The authority response names another pointer or another subject count.
    AuthorityResponseMismatch,
    /// The candidate was invalidated by the floor.
    CandidateInvalidated,
    /// The pointer was withdrawn between planning and commit.
    WithdrawnBeforeCommit,
}
impl fmt::Display for PublicationLifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PointerUnavailable(status) => {
                write!(f, "publication pointer unavailable (HTTP {status})")
            }
            Self::UnexpectedNotModified => f.write_str("unexpected unchanged pointer response"),
            Self::AuthorityCheckRefused(status) => {
                write!(f, "publisher refused the authority check (HTTP {status})")
            }
            Self::AuthorityResponseMismatch => f.write_str("authority check response differs"),
            Self::CandidateInvalidated => f.write_str("publication candidate was invalidated"),
            Self::WithdrawnBeforeCommit => f.write_str("publication withdrawn before commit"),
        }
    }
}
impl std::error::Error for PublicationLifecycleError {}

/// A newer pointer won the authority check; nothing was committed.
#[derive(Debug)]
struct PointerAdvanced;
impl fmt::Display for PointerAdvanced {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("publication pointer advanced during the authority check")
    }
}
impl std::error::Error for PointerAdvanced {}

/// Owns one role's publication source, floor, cache and adapter.
pub struct PublicationLifecycle<A: PublicationAdapter> {
    adapter: A,
    source: HttpsSource,
    floor: AuthorityFloor<A::Pointer>,
    cache: ObjectCache,
    /// The last pointer body received, used only to answer a 304.
    last_received_pointer: Option<A::Pointer>,
    /// Object sets of the last two commits; everything else is collected.
    retained_commits: Vec<BTreeSet<String>>,
}

impl<A: PublicationAdapter> PublicationLifecycle<A> {
    /// `floor` was opened for `source`'s origin, namespace and chain; `cache`
    /// is exclusively owned by this role.
    pub fn new(
        adapter: A,
        source: HttpsSource,
        floor: AuthorityFloor<A::Pointer>,
        cache: ObjectCache,
    ) -> Result<Self, Error> {
        if let Some(pointer) = floor.pointer() {
            adapter.validate_pointer(pointer)?;
        }
        if floor.source().publisher_origin != source.origin().as_str() {
            return Err("publication floor and source name different publishers".into());
        }
        Ok(Self {
            adapter,
            source,
            floor,
            cache,
            last_received_pointer: None,
            retained_commits: Vec::new(),
        })
    }
    pub fn adapter(&self) -> &A {
        &self.adapter
    }
    pub fn floor(&self) -> &AuthorityFloor<A::Pointer> {
        &self.floor
    }

    async fn run_pass(&mut self) -> Result<PublicationPass<A::Report>, Error> {
        self.source.discover().await?;
        self.source.reset_budget();
        // Observe and fence.
        let pointer = self.observe_pointer().await?;
        if !pointer.eligible() {
            return Ok(self.deferred().await);
        }
        let serving = self.adapter.serving_subjects(&pointer).await;
        if !serving.is_empty() {
            self.check_authority(&pointer, serving).await?;
        }
        // Plan and fetch.
        let mut objects = ObjectFetcher::new(&mut self.source, &self.cache);
        let (candidate, identity, required_objects) =
            match self.adapter.plan(&pointer, &mut objects).await? {
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
        // Load, then revalidate against a freshly observed pointer.
        let loaded = self.adapter.load(candidate, &self.cache).await?;
        let latest = self.observe_pointer().await?;
        if !latest.eligible() {
            return Err(Box::new(PublicationLifecycleError::WithdrawnBeforeCommit));
        }
        let (identity, subject) = self.adapter.revalidate(&latest, &loaded)?;
        let mut subjects = vec![(identity, subject)];
        subjects.extend(
            self.adapter
                .serving_subjects(&latest)
                .await
                .into_iter()
                .filter(|(serving, _)| *serving != identity),
        );
        self.check_authority(&latest, subjects).await?;
        if self.adapter.reserve_peer(&loaded).await? == PeerReservation::PeerAdvanced {
            return Err(Box::new(PointerAdvanced));
        }
        // Commit: the floor is final here, no await separates this check from
        // spawning the owned commit.
        if self.floor.is_invalidated(&identity) {
            return Err(Box::new(PublicationLifecycleError::CandidateInvalidated));
        }
        let retained = self.adapter.retained_objects(&loaded);
        let commit = tokio::spawn(self.adapter.commit(loaded));
        let report = match commit.await? {
            Ok(report) => report,
            Err(error) => {
                self.adapter.fence_serving().await;
                self.adapter.close_admission().await;
                return Err(error);
            }
        };
        // Retain.
        self.retain_commit(retained).await?;
        let handover = self.adapter.release_peer(&report).await;
        Ok(PublicationPass::Committed { report, handover })
    }

    async fn deferred(&mut self) -> PublicationPass<A::Report> {
        PublicationPass::Deferred {
            report: self.adapter.serving_report().await,
        }
    }

    async fn retain_commit(&mut self, objects: BTreeSet<String>) -> Result<(), Error> {
        self.retained_commits.retain(|old| *old != objects);
        self.retained_commits.push(objects);
        if self.retained_commits.len() > 2 {
            self.retained_commits.remove(0);
        }
        let keep: BTreeSet<String> = self.retained_commits.iter().flatten().cloned().collect();
        let cache = self.cache.clone();
        tokio::task::spawn_blocking(move || cache.collect_unreferenced(&keep)).await??;
        Ok(())
    }
}

impl<A: PublicationAdapter> SyncTask for PublicationLifecycle<A> {
    type Report = PublicationPass<A::Report>;
    type Error = Error;

    async fn synchronize(&mut self) -> Result<Self::Report, Error> {
        self.adapter.pass_started().await;
        let result = match self.run_pass().await {
            // A newer pointer won the authority check. Nothing was activated;
            // retry at the normal cadence instead of transport backoff.
            Err(error) if error.is::<PointerAdvanced>() => Ok(self.deferred().await),
            result => result,
        };
        self.adapter.pass_finished(&result).await;
        result
    }
    fn terminal(&self, error: &Error) -> bool {
        crate::is_incompatible(error.as_ref())
    }
}
