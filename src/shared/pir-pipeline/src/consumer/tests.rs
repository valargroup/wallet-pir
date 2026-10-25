use super::polling::SyncTask;
use super::*;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Recipe {
    events: Arc<Mutex<Vec<&'static str>>>,
    advance_once: bool,
    refuse_once: bool,
    generation: u64,
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
    type Loaded = u64;
    type Artifacts = ();
    type Validation = u64;
    type Report = u64;
    async fn settle(&mut self) -> Result<(), Error> {
        self.record("settle");
        Ok(())
    }
    async fn observe(&mut self) -> Result<u64, Error> {
        self.record("observe");
        Ok(self.generation)
    }
    fn eligible(&self, _: &u64) -> bool {
        true
    }
    fn keep_loaded(&self, _: &u64) -> bool {
        true
    }
    async fn check_serving(&mut self, _: &u64) -> Result<(), Error> {
        self.record("check-serving");
        Ok(())
    }
    async fn select(&mut self, observed: &u64) -> Result<Input<u64>, Error> {
        self.record("select");
        Ok(Input::Work(*observed))
    }
    async fn reserve(&mut self, _: &u64) -> Result<PeerReservation, Error> {
        self.record("reserve");
        Ok(PeerReservation::Reserved)
    }
    async fn obtain(&mut self, _: &u64) -> Result<(), Error> {
        self.record("obtain");
        Ok(())
    }
    async fn load(&mut self, candidate: u64, _: ()) -> Result<u64, Error> {
        self.record("load");
        Ok(candidate)
    }
    async fn rebind(&mut self, _: &u64, loaded: u64) -> Result<u64, Error> {
        self.record("rebind");
        Ok(loaded)
    }
    fn validate(&self, _: &u64, loaded: &u64) -> Result<u64, Error> {
        self.record("validate");
        Ok(*loaded)
    }
    async fn authorize(&mut self, _: &u64, _: &u64) -> Result<(), Error> {
        self.record("authorize");
        if std::mem::take(&mut self.advance_once) {
            return Err(Advanced.into());
        }
        if std::mem::take(&mut self.refuse_once) {
            return Err(std::io::Error::other("source unavailable").into());
        }
        Ok(())
    }
    async fn confirm(&mut self, _: &u64) -> Result<PeerReservation, Error> {
        self.record("confirm");
        Ok(PeerReservation::Reserved)
    }
    async fn commit(&mut self, loaded: u64, _: u64) -> Result<(u64, PeerHandover), Error> {
        self.record("commit");
        Ok((loaded, PeerHandover::Complete))
    }
    async fn serving_report(&mut self) -> u64 {
        self.generation
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
