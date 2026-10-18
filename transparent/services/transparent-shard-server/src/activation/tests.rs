//! The shared activation transaction against a lightweight fake owner, so
//! every ordering and failure case is driven by explicit barriers rather than
//! by timing or real disk behavior.

use super::{activate, Adapter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;

const OLD: &str = "old";
const NEW: &str = "new";

/// How the fake's durable write ends.
enum Write {
    Succeeds,
    /// Fails before the record is replaced.
    FailsBeforeRename,
    /// Replaces the record, then fails as a directory sync would.
    FailsAfterRename,
    /// Reports entry, then waits for release before succeeding.
    Blocks(Sender<()>, Receiver<()>),
}

struct Candidate {
    digest: String,
    epoch: u64,
    ready: bool,
    /// Stands in for the runtimes a request may still hold.
    handle: Arc<String>,
}

struct Owner {
    active: Mutex<(String, bool, Arc<String>)>,
    candidate: AsyncMutex<Option<Candidate>>,
    gate: Mutex<()>,
    epoch: AtomicU64,
    reject: Mutex<Option<String>>,
    write: Mutex<Write>,
    durable: Mutex<String>,
    events: Mutex<Vec<&'static str>>,
    /// Bumps the epoch when the predecessor is read, as a revocation landing
    /// before the gate is taken would.
    revoke_on_read: Mutex<bool>,
}

#[derive(Clone)]
struct Fake(Arc<Owner>);

impl Fake {
    fn new() -> Self {
        Self(Arc::new(Owner {
            active: Mutex::new((OLD.into(), true, Arc::new(OLD.into()))),
            candidate: AsyncMutex::new(Some(Candidate {
                digest: NEW.into(),
                epoch: 0,
                ready: true,
                handle: Arc::new(NEW.into()),
            })),
            gate: Mutex::new(()),
            epoch: AtomicU64::new(0),
            reject: Mutex::new(None),
            write: Mutex::new(Write::Succeeds),
            durable: Mutex::new(OLD.into()),
            events: Mutex::new(Vec::new()),
            revoke_on_read: Mutex::new(false),
        }))
    }

    fn serving(&self) -> String {
        self.0.active.lock().unwrap().0.clone()
    }

    fn durable(&self) -> String {
        self.0.durable.lock().unwrap().clone()
    }

    fn events(&self) -> Vec<&'static str> {
        self.0.events.lock().unwrap().clone()
    }

    fn candidate_digest(&self) -> Option<String> {
        self.0
            .candidate
            .try_lock()
            .unwrap()
            .as_ref()
            .map(|c| c.digest.clone())
    }

    fn event(&self, event: &'static str) {
        self.0.events.lock().unwrap().push(event);
    }
}

impl Adapter for Fake {
    type Candidate = Candidate;
    type Validated = &'static str;

    fn active(&self, requested: &str) -> (String, bool) {
        self.event("active");
        if *self.0.revoke_on_read.lock().unwrap() {
            self.0.epoch.fetch_add(1, Ordering::AcqRel);
        }
        let active = self.0.active.lock().unwrap();
        (active.0.clone(), active.0 == requested && active.1)
    }
    fn candidate(&self) -> &AsyncMutex<Option<Candidate>> {
        &self.0.candidate
    }
    fn publication_gate(&self) -> &Mutex<()> {
        &self.0.gate
    }
    fn epoch(&self) -> u64 {
        self.0.epoch.load(Ordering::Acquire)
    }
    fn candidate_epoch(candidate: &Candidate) -> u64 {
        candidate.epoch
    }
    fn digest(candidate: &Candidate) -> &str {
        &candidate.digest
    }
    fn ready(candidate: &Candidate) -> bool {
        candidate.ready
    }
    fn validate(&self, _: &Candidate) -> Result<&'static str, String> {
        self.event("validate");
        match self.0.reject.lock().unwrap().clone() {
            Some(error) => Err(error),
            None => Ok("validated"),
        }
    }
    fn persist(&self, candidate: &Candidate) -> Result<(), String> {
        self.event("persist");
        let mut write = self.0.write.lock().unwrap();
        match &mut *write {
            Write::Succeeds => {}
            Write::FailsBeforeRename => return Err("cannot create record".into()),
            Write::FailsAfterRename => {
                *self.0.durable.lock().unwrap() = candidate.digest.clone();
                return Err("cannot sync record directory".into());
            }
            Write::Blocks(entered, release) => {
                entered.send(()).unwrap();
                release.recv().unwrap();
            }
        }
        *self.0.durable.lock().unwrap() = candidate.digest.clone();
        Ok(())
    }
    fn install(&self, candidate: Candidate, validated: &'static str) {
        assert_eq!(validated, "validated");
        assert_eq!(*self.0.durable.lock().unwrap(), candidate.digest);
        self.event("install");
        *self.0.active.lock().unwrap() = (candidate.digest, candidate.ready, candidate.handle);
    }
}

async fn run(fake: &Fake, expected: &str, requested: &str) -> Result<(), String> {
    let operations = Arc::new(AsyncMutex::new(()));
    let operation = operations.lock_owned().await;
    activate(expected.into(), requested.into(), fake.clone(), operation).await
}

/// Refused before anything durable happens: the active view, the durable
/// record and the candidate are all as they were.
fn untouched(fake: &Fake) {
    assert_eq!(fake.serving(), OLD);
    assert_eq!(fake.durable(), OLD);
    assert_eq!(fake.candidate_digest().as_deref(), Some(NEW));
    assert!(!fake.events().contains(&"persist"));
}

#[tokio::test]
async fn validation_then_persistence_then_installation() {
    let fake = Fake::new();
    let request = fake.0.active.lock().unwrap().2.clone();
    run(&fake, OLD, NEW).await.unwrap();
    assert_eq!(fake.events(), ["active", "validate", "persist", "install"]);
    assert_eq!(fake.serving(), NEW);
    assert_eq!(fake.durable(), NEW);
    assert_eq!(fake.candidate_digest(), None);
    // A request that took the old view before activation still holds it.
    assert_eq!(*request, OLD);
}

#[tokio::test]
async fn an_already_active_publication_succeeds_without_a_candidate() {
    let fake = Fake::new();
    *fake.0.active.lock().unwrap() = (NEW.into(), true, Arc::new(NEW.into()));
    *fake.0.candidate.try_lock().unwrap() = None;
    run(&fake, "anything", NEW).await.unwrap();
    assert_eq!(fake.events(), ["active"]);
    assert_eq!(fake.durable(), OLD);
}

#[tokio::test]
async fn an_unwarm_active_publication_is_not_already_active() {
    let fake = Fake::new();
    *fake.0.active.lock().unwrap() = (NEW.into(), false, Arc::new(NEW.into()));
    let error = run(&fake, OLD, NEW).await.unwrap_err();
    assert_eq!(error, "active predecessor changed");
}

#[tokio::test]
async fn refusals_before_persistence_keep_everything() {
    let changed = Fake::new();
    let error = run(&changed, "someone else", NEW).await.unwrap_err();
    assert_eq!(error, "active predecessor changed");
    untouched(&changed);

    let missing = Fake::new();
    *missing.0.candidate.try_lock().unwrap() = None;
    let error = run(&missing, OLD, NEW).await.unwrap_err();
    assert_eq!(error, "no prepared candidate");
    assert_eq!(missing.serving(), OLD);
    assert_eq!(missing.durable(), OLD);

    let fenced = Fake::new();
    fenced.0.epoch.store(1, Ordering::Release);
    let error = run(&fenced, OLD, NEW).await.unwrap_err();
    assert_eq!(error, "reorg invalidated prepared candidate");
    untouched(&fenced);

    let other = Fake::new();
    let error = run(&other, OLD, "another").await.unwrap_err();
    assert_eq!(error, "candidate identity/readiness mismatch");
    untouched(&other);

    let cold = Fake::new();
    cold.0.candidate.try_lock().unwrap().as_mut().unwrap().ready = false;
    let error = run(&cold, OLD, NEW).await.unwrap_err();
    assert_eq!(error, "candidate identity/readiness mismatch");
    untouched(&cold);

    let revoked = Fake::new();
    *revoked.0.reject.lock().unwrap() = Some("candidate contains invalidated history".into());
    let error = run(&revoked, OLD, NEW).await.unwrap_err();
    assert_eq!(error, "candidate contains invalidated history");
    untouched(&revoked);
}

/// The reorg fence is checked under the gate, after the predecessor check,
/// so a revocation that lands in between still refuses the candidate.
#[tokio::test]
async fn a_revocation_after_the_predecessor_check_fences_the_candidate() {
    let fake = Fake::new();
    *fake.0.revoke_on_read.lock().unwrap() = true;
    let error = run(&fake, OLD, NEW).await.unwrap_err();
    assert_eq!(error, "reorg invalidated prepared candidate");
    untouched(&fake);
}

#[tokio::test]
async fn a_write_failing_before_its_rename_installs_nothing() {
    let fake = Fake::new();
    *fake.0.write.lock().unwrap() = Write::FailsBeforeRename;
    assert_eq!(
        run(&fake, OLD, NEW).await.unwrap_err(),
        "cannot create record"
    );
    assert_eq!(fake.events(), ["active", "validate", "persist"]);
    assert_eq!(fake.serving(), OLD);
    assert_eq!(fake.durable(), OLD);
    assert_eq!(fake.candidate_digest().as_deref(), Some(NEW));
}

/// The record already names the candidate, which a restart recovers, but
/// this process neither installs it nor reports success, and does not retry.
#[tokio::test]
async fn a_write_failing_after_its_rename_installs_nothing() {
    let fake = Fake::new();
    *fake.0.write.lock().unwrap() = Write::FailsAfterRename;
    assert_eq!(
        run(&fake, OLD, NEW).await.unwrap_err(),
        "cannot sync record directory"
    );
    assert_eq!(fake.events(), ["active", "validate", "persist"]);
    assert_eq!(fake.serving(), OLD);
    assert_eq!(fake.durable(), NEW);
    assert_eq!(fake.candidate_digest().as_deref(), Some(NEW));
    // A later activation by the controller completes the same candidate.
    *fake.0.write.lock().unwrap() = Write::Succeeds;
    run(&fake, OLD, NEW).await.unwrap();
    assert_eq!(fake.serving(), NEW);
}

/// While the write is blocked, serialization, the candidate and the gate stay
/// held even after the caller is cancelled, the serving view is not locked,
/// and the write still completes and installs once released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_caller_does_not_release_a_blocked_write() {
    let fake = Fake::new();
    let (entered, entry) = channel();
    let (release, released) = channel();
    *fake.0.write.lock().unwrap() = Write::Blocks(entered, released);
    let operations = Arc::new(AsyncMutex::new(()));
    let operation = operations.clone().lock_owned().await;
    let caller = tokio::spawn(activate(OLD.into(), NEW.into(), fake.clone(), operation));
    tokio::task::spawn_blocking(move || entry.recv().unwrap())
        .await
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());

    assert!(operations.try_lock().is_err(), "serialization was released");
    assert!(fake.0.candidate.try_lock().is_err());
    assert!(
        fake.0.gate.try_lock().is_err(),
        "revocation could interleave"
    );
    assert_eq!(fake.serving(), OLD, "serving view was locked or swapped");

    release.send(()).unwrap();
    let _next = operations.lock().await;
    assert_eq!(fake.serving(), NEW);
    assert_eq!(fake.durable(), NEW);
    assert_eq!(fake.events(), ["active", "validate", "persist", "install"]);
}
