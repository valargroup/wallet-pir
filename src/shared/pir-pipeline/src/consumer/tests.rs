use super::polling::SyncTask;
use super::*;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Recipe {
    events: Arc<Mutex<Vec<&'static str>>>,
    advance_once: bool,
    refuse_once: bool,
    generation: u64,
    owner: Option<Arc<tokio::sync::Mutex<()>>>,
    block_authority: bool,
    authority_entered: Option<Arc<tokio::sync::Notify>>,
    continuations: usize,
}
struct Prepared {
    value: u64,
    _operation: Option<tokio::sync::OwnedMutexGuard<()>>,
}
#[derive(Debug)]
struct Advanced;
impl std::fmt::Display for Advanced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("source advanced")
    }
}
impl std::error::Error for Advanced {}
impl Recipe {
    fn record(&self, phase: &'static str) {
        self.events.lock().unwrap().push(phase);
    }
}
impl Consumer for Recipe {
    // These are deliberately not publication envelopes or file paths: the
    // orchestration can also drive a product reading a local record.
    type Observation = u64;
    type Candidate = u64;
    type Loaded = Prepared;
    type Artifacts = ();
    type Validation = u64;
    type Authority = ();
    type Report = u64;
    async fn settle(&mut self) -> Result<(), Error> {
        self.record("settle");
        if let Some(owner) = &self.owner {
            drop(owner.lock().await);
        }
        Ok(())
    }
    async fn observe(&mut self) -> Result<u64, Error> {
        self.record("observe");
        Ok(self.generation)
    }
    fn eligible(&self, _: &u64) -> bool {
        true
    }
    fn keep_loaded(&self, _: &Prepared) -> bool {
        self.owner.is_none()
    }
    async fn check_serving(&mut self, _: &u64) -> Result<(), Error> {
        self.record("check-serving");
        Ok(())
    }
    async fn select(&mut self, observed: &u64) -> Result<Input<u64>, Error> {
        self.record("select");
        Ok(Input::Work(*observed))
    }
    async fn reserve(&mut self, _: &mut u64) -> Result<PeerReservation, Error> {
        self.record("reserve");
        Ok(PeerReservation::Reserved)
    }
    async fn obtain(&mut self, _: &u64) -> Result<(), Error> {
        self.record("obtain");
        Ok(())
    }
    async fn load(&mut self, candidate: u64, _: ()) -> Result<Prepared, Error> {
        self.record("load");
        let operation = match &self.owner {
            Some(owner) => Some(owner.clone().lock_owned().await),
            None => None,
        };
        Ok(Prepared {
            value: candidate,
            _operation: operation,
        })
    }
    async fn rebind(&mut self, _: &u64, loaded: Prepared) -> Result<Prepared, Error> {
        self.record("rebind");
        Ok(loaded)
    }
    fn validate(&self, _: &u64, loaded: &Prepared) -> Result<u64, Error> {
        self.record("validate");
        Ok(loaded.value)
    }
    async fn authorize(&mut self, _: &u64, _: &u64) -> Result<(), Error> {
        self.record("authorize");
        if std::mem::take(&mut self.block_authority) {
            if let Some(entered) = &self.authority_entered {
                entered.notify_one();
            }
            std::future::pending::<()>().await;
        }
        if std::mem::take(&mut self.advance_once) {
            return Err(Advanced.into());
        }
        if std::mem::take(&mut self.refuse_once) {
            return Err(std::io::Error::other("source unavailable").into());
        }
        Ok(())
    }
    async fn confirm(&mut self, _: &mut Prepared) -> Result<PeerReservation, Error> {
        self.record("confirm");
        Ok(PeerReservation::Reserved)
    }
    async fn commit(
        &mut self,
        loaded: Prepared,
        _: u64,
        _: (),
    ) -> Result<(u64, PeerHandover), Error> {
        self.record("commit");
        Ok((loaded.value, PeerHandover::Complete))
    }
    async fn serving_report(&mut self) -> u64 {
        self.generation
    }
    fn continue_after(&mut self, _: &Result<ConsumerPass<u64>, Error>) -> bool {
        if self.continuations == 0 {
            return false;
        }
        self.continuations -= 1;
        true
    }
    fn deferral(&self, error: &Error) -> Option<DeferralReason> {
        error
            .is::<Advanced>()
            .then_some(DeferralReason::AuthorityAdvanced)
    }
}
#[tokio::test]
async fn reserves_before_transfer_and_rechecks_authority_before_commit() {
    let recipe = Recipe::default();
    let events = recipe.events.clone();
    let mut engine = ConsumerEngine::new(recipe);
    assert!(matches!(
        engine.synchronize().await.unwrap(),
        ConsumerPass::Committed { .. }
    ));
    assert_eq!(
        *events.lock().unwrap(),
        [
            "settle",
            "observe",
            "check-serving",
            "select",
            "reserve",
            "obtain",
            "load",
            "observe",
            "rebind",
            "validate",
            "authorize",
            "confirm",
            "commit"
        ]
    );
}
#[tokio::test]
async fn source_advance_reuses_loaded_bytes_but_never_an_authority_answer() {
    let recipe = Recipe {
        advance_once: true,
        ..Recipe::default()
    };
    let events = recipe.events.clone();
    let mut engine = ConsumerEngine::new(recipe);
    assert!(matches!(
        engine.synchronize().await.unwrap(),
        ConsumerPass::Deferred { .. }
    ));
    engine.synchronize().await.unwrap();
    let events = events.lock().unwrap();
    assert_eq!(events.iter().filter(|phase| **phase == "load").count(), 1);
    assert_eq!(
        events.iter().filter(|phase| **phase == "authorize").count(),
        2
    );
    assert_eq!(events.iter().filter(|phase| **phase == "commit").count(), 1);
}
#[tokio::test]
async fn a_genuine_failure_discards_the_candidate_instead_of_granting_authority() {
    let recipe = Recipe {
        refuse_once: true,
        ..Recipe::default()
    };
    let events = recipe.events.clone();
    let mut engine = ConsumerEngine::new(recipe);
    assert!(engine.synchronize().await.is_err());
    assert!(!events.lock().unwrap().contains(&"commit"));
    engine.synchronize().await.unwrap();
    assert_eq!(
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|phase| **phase == "load")
            .count(),
        2
    );
}

#[tokio::test]
async fn a_cancelled_candidate_releases_its_owner_guard_before_settlement() {
    let owner = Arc::new(tokio::sync::Mutex::new(()));
    let entered = Arc::new(tokio::sync::Notify::new());
    let recipe = Recipe {
        owner: Some(owner.clone()),
        block_authority: true,
        authority_entered: Some(entered.clone()),
        ..Recipe::default()
    };
    let events = recipe.events.clone();
    let mut engine = ConsumerEngine::new(recipe);
    let mut pass = Box::pin(engine.synchronize());
    tokio::select! {
        _ = &mut pass => panic!("authority deliberately waits for cancellation"),
        _ = entered.notified() => {},
    }
    drop(pass);
    assert!(events.lock().unwrap().contains(&"authorize"));
    assert!(
        owner.try_lock().is_err(),
        "the cancelled candidate still owns serialization"
    );
    let completed = tokio::time::timeout(Duration::from_secs(1), engine.synchronize())
        .await
        .expect("settlement must not wait on the discarded candidate")
        .unwrap();
    assert!(matches!(completed, ConsumerPass::Committed { .. }));
    assert!(owner.try_lock().is_ok());
}
#[tokio::test]
async fn recovery_continuations_repeat_the_full_observed_sequence() {
    let recipe = Recipe {
        continuations: 1,
        ..Recipe::default()
    };
    let events = recipe.events.clone();
    let mut engine = ConsumerEngine::new(recipe);
    engine.synchronize().await.unwrap();
    let events = events.lock().unwrap();
    assert_eq!(events.iter().filter(|phase| **phase == "commit").count(), 2);
    assert_eq!(
        events.iter().filter(|phase| **phase == "observe").count(),
        4
    );
    assert_eq!(
        events.iter().filter(|phase| **phase == "authorize").count(),
        2
    );
}
