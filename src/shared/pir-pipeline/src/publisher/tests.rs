use super::*;
use crate::{engine::PublisherEngine, FailurePolicy};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

#[tokio::test]
async fn dedicated_runtime_constructs_thread_affine_state_on_its_owner_thread() {
    dedicated_runtime("test-publisher", || async {
        let state = std::rc::Rc::new(7);
        tokio::task::yield_now().await;
        assert_eq!(*state, 7);
        Ok(())
    })
    .await
    .unwrap();
}

struct RecordedPublication(Arc<std::sync::Mutex<Vec<&'static str>>>);
impl Publication for RecordedPublication {
    type Error = &'static str;
    type Material = ();
    type Stored = ();
    type Verified = ();
    type Candidate = ();
    type Report = ();
    fn check(&self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
    async fn build(&mut self) -> std::result::Result<(), Self::Error> {
        self.0.lock().unwrap().push("build");
        Ok(())
    }
    async fn store(&mut self, _: ()) -> std::result::Result<(), Self::Error> {
        self.0.lock().unwrap().push("store");
        Ok(())
    }
    async fn verify(&mut self, _: ()) -> std::result::Result<(), Self::Error> {
        self.0.lock().unwrap().push("verify");
        Ok(())
    }
    async fn assemble(&mut self, _: ()) -> std::result::Result<(), Self::Error> {
        self.0.lock().unwrap().push("assemble");
        Ok(())
    }
    async fn recheck(&mut self, _: &mut ()) -> std::result::Result<(), Self::Error> {
        self.0.lock().unwrap().push("recheck");
        Ok(())
    }
    async fn commit(self, _: ()) -> std::result::Result<(), Self::Error> {
        self.0.lock().unwrap().push("commit");
        Ok(())
    }
}
struct SelectedInputs {
    next: usize,
    events: Arc<std::sync::Mutex<Vec<&'static str>>>,
}
impl Publisher for SelectedInputs {
    type Error = &'static str;
    type Report = ();
    type Publication<'a> = RecordedPublication;
    async fn acquire(
        &mut self,
    ) -> std::result::Result<PublicationInput<Self::Publication<'_>>, Self::Error> {
        self.next += 1;
        Ok(match self.next {
            1 => PublicationInput::Unchanged,
            2 => PublicationInput::Deferred,
            3 => PublicationInput::MetadataRefresh(RecordedPublication(self.events.clone())),
            4 => PublicationInput::NewMaterial(RecordedPublication(self.events.clone())),
            _ => panic!("the bounded source is complete"),
        })
    }
    fn settled(&mut self) -> std::result::Result<Control, Self::Error> {
        Ok(if self.next == 4 {
            Control::Complete
        } else {
            Control::Continue
        })
    }
    async fn wait(&mut self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}
#[tokio::test]
async fn selected_refresh_and_material_follow_the_same_checked_phases() {
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    crate::engine::PublisherEngine
        .run(
            SelectedInputs {
                next: 0,
                events: events.clone(),
            },
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        [
            "build", "store", "verify", "assemble", "recheck", "commit", "build", "store",
            "verify", "assemble", "recheck", "commit"
        ]
    );
}

struct HeldPublication {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    release: Option<tokio::sync::oneshot::Receiver<()>>,
    drained: Arc<AtomicBool>,
}
impl Publisher for HeldPublication {
    type Error = &'static str;
    type Report = ();
    type Publication<'a> = RecordedPublication;
    async fn acquire(
        &mut self,
    ) -> std::result::Result<PublicationInput<Self::Publication<'_>>, Self::Error> {
        self.entered.take().unwrap().send(()).unwrap();
        self.release.take().unwrap().await.unwrap();
        Ok(PublicationInput::Unchanged)
    }
    async fn wait(&mut self) -> std::result::Result<(), Self::Error> {
        std::future::pending().await
    }
    async fn finish(&mut self) -> std::result::Result<(), Self::Error> {
        self.drained.store(true, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn shutdown_never_drops_an_owner_operation_and_always_drains() {
    let (entered, observed) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel();
    let drained = Arc::new(AtomicBool::new(false));
    let shutdown = crate::service::Shutdown::default();
    let stop = shutdown.clone();
    let finished = drained.clone();
    let task = tokio::spawn(async move {
        PublisherEngine
            .run(
                HeldPublication {
                    entered: Some(entered),
                    release: Some(held),
                    drained: finished,
                },
                stop,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), observed)
        .await
        .unwrap()
        .unwrap();
    shutdown.request();
    assert!(!task.is_finished());
    assert!(!drained.load(Ordering::SeqCst));
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(drained.load(Ordering::SeqCst));
}

#[tokio::test]
async fn failed_steps_and_failed_wakes_both_finish_before_return() {
    struct Failing {
        fail_step: bool,
        drained: bool,
    }
    impl Publisher for &mut Failing {
        type Error = &'static str;
        type Report = ();
        type Publication<'a>
            = RecordedPublication
        where
            Self: 'a;
        async fn acquire(
            &mut self,
        ) -> std::result::Result<PublicationInput<Self::Publication<'_>>, Self::Error> {
            if self.fail_step {
                Err("step failed")
            } else {
                Ok(PublicationInput::Unchanged)
            }
        }
        async fn wait(&mut self) -> std::result::Result<(), Self::Error> {
            Err("wake failed")
        }
        async fn finish(&mut self) -> std::result::Result<(), Self::Error> {
            self.drained = true;
            Ok(())
        }
    }
    for fail_step in [true, false] {
        let mut state = Failing {
            fail_step,
            drained: false,
        };
        let result = PublisherEngine.run(&mut state, Default::default()).await;
        assert_eq!(
            result,
            Err(if fail_step {
                "step failed"
            } else {
                "wake failed"
            })
        );
        assert!(state.drained);
    }
}

#[tokio::test(start_paused = true)]
async fn retries_wait_after_complete_passes_and_notifications_can_wake_early() {
    struct Retry {
        passes: usize,
        wakes: usize,
    }
    impl Publisher for &mut Retry {
        type Error = &'static str;
        type Report = ();
        type Publication<'a>
            = RecordedPublication
        where
            Self: 'a;
        async fn acquire(
            &mut self,
        ) -> std::result::Result<PublicationInput<Self::Publication<'_>>, Self::Error> {
            self.passes += 1;
            if self.passes == 1 {
                Err("source unavailable")
            } else {
                Ok(PublicationInput::Unchanged)
            }
        }
        fn failed(&mut self, _: &Self::Error) -> FailurePolicy {
            FailurePolicy::NextPoll
        }
        fn settled(&mut self) -> std::result::Result<Control, Self::Error> {
            Ok(if self.passes == 3 {
                Control::Complete
            } else {
                Control::Continue
            })
        }
        async fn wait(&mut self) -> std::result::Result<(), Self::Error> {
            self.wakes += 1;
            tokio::select! { _ = tokio::time::sleep(Duration::from_secs(10)) => {}, _ = tokio::time::sleep(Duration::from_secs(2)) => {} }
            Ok(())
        }
    }
    let started = tokio::time::Instant::now();
    let mut state = Retry {
        passes: 0,
        wakes: 0,
    };
    PublisherEngine
        .run(&mut state, Default::default())
        .await
        .unwrap();
    assert_eq!((state.passes, state.wakes), (3, 2));
    assert_eq!(started.elapsed(), Duration::from_secs(4));
}

#[tokio::test]
async fn shutdown_already_requested_starts_no_new_pass() {
    let shutdown = crate::service::Shutdown::default();
    shutdown.request();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    PublisherEngine
        .run(
            SelectedInputs {
                next: 0,
                events: events.clone(),
            },
            shutdown,
        )
        .await
        .unwrap();
    assert!(events.lock().unwrap().is_empty());
}

struct BorrowedRecords<'a> {
    journal: &'a [usize],
    activated: &'a mut Vec<usize>,
    fail_preparation: bool,
    fail_commit: bool,
}
impl<'a> Publication for BorrowedRecords<'a> {
    type Error = &'static str;
    type Material = &'a [usize];
    type Stored = &'a [usize];
    type Verified = &'a [usize];
    type Candidate = &'a [usize];
    type Report = ();
    fn check(&self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
    async fn build(&mut self) -> std::result::Result<Self::Material, Self::Error> {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if self.fail_preparation {
            Err("preparation refused")
        } else {
            Ok(self.journal)
        }
    }
    async fn store(
        &mut self,
        material: Self::Material,
    ) -> std::result::Result<Self::Stored, Self::Error> {
        Ok(material)
    }
    async fn verify(
        &mut self,
        stored: Self::Stored,
    ) -> std::result::Result<Self::Verified, Self::Error> {
        Ok(stored)
    }
    async fn assemble(
        &mut self,
        verified: Self::Verified,
    ) -> std::result::Result<Self::Candidate, Self::Error> {
        Ok(verified)
    }
    async fn recheck(&mut self, _: &mut Self::Candidate) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
    async fn commit(self, candidate: Self::Candidate) -> std::result::Result<(), Self::Error> {
        self.activated.extend_from_slice(candidate);
        if self.fail_commit {
            Err("owner must reconcile")
        } else {
            Ok(())
        }
    }
}
#[tokio::test(start_paused = true)]
async fn borrowed_commits_survive_failed_preparation_and_uncertain_activation_is_not_retried() {
    for (fail_preparation, fail_commit) in [(false, false), (true, false), (false, true)] {
        let mut journal = vec![1];
        journal.push(2);
        let mut activated = Vec::new();
        let result = PublisherEngine
            .publish(BorrowedRecords {
                journal: &journal,
                activated: &mut activated,
                fail_preparation,
                fail_commit,
            })
            .await;
        assert_eq!(journal, [1, 2]);
        assert_eq!(
            activated,
            if fail_preparation { vec![] } else { vec![1, 2] }
        );
        assert_eq!(
            result,
            if fail_preparation {
                Err("preparation refused")
            } else if fail_commit {
                Err("owner must reconcile")
            } else {
                Ok(())
            }
        );
    }
}
#[tokio::test(start_paused = true)]
async fn independent_observation_continues_during_ordered_preparation() {
    let mut heartbeats = 0;
    let mut activated = Vec::new();
    let publisher = async {
        for seal in [1, 2, 3] {
            PublisherEngine
                .publish(BorrowedRecords {
                    journal: &[seal],
                    activated: &mut activated,
                    fail_preparation: false,
                    fail_commit: false,
                })
                .await
                .unwrap();
        }
    };
    let observer = async {
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            heartbeats += 1;
        }
    };
    tokio::join!(publisher, observer);
    assert_eq!(activated, [1, 2, 3]);
    assert_eq!(heartbeats, 5);
}
