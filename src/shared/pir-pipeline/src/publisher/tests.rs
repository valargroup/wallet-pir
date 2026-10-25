use super::*;
use crate::{FailurePolicy, Input};
use std::{
    cell::Cell,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

#[tokio::test]
async fn typed_pass_preserves_borrowed_commits_and_owner_permits() {
    let mut journal = vec![1];
    let acquired = async {
        journal.push(2);
        Ok::<_, ()>(Input::Work(journal.as_slice()))
    };
    let result = PublicationPass::new(
        acquired,
        |records| async move { Ok(Input::Work(records)) },
        |candidate| async move { Ok(candidate.len()) },
    )
    .run(&mut ())
    .await
    .unwrap();
    assert_eq!(result, Input::Work(2));
    assert_eq!(journal, [1, 2]);
}

#[tokio::test]
async fn failed_preparation_keeps_acquisition_and_skips_publication() {
    let committed = Cell::new(false);
    let result = PublicationPass::new(
        async {
            committed.set(true);
            Ok::<_, &str>(Input::Work(()))
        },
        |_| async { Err::<Input<()>, _>("preparation refused") },
        |_| async {
            panic!("invalid candidate must never publish");
            #[allow(unreachable_code)]
            Ok(())
        },
    )
    .run(&mut ())
    .await;
    assert_eq!(result, Err("preparation refused"));
    assert!(committed.get());
    for input in [Input::<()>::Unchanged, Input::Deferred] {
        let result = PublicationPass::new(
            async { Ok::<_, ()>(input) },
            |_| async {
                panic!("no work to prepare");
                #[allow(unreachable_code)]
                Ok(Input::Work(()))
            },
            |_| async {
                panic!("no candidate to publish");
                #[allow(unreachable_code)]
                Ok(())
            },
        )
        .run(&mut ())
        .await
        .unwrap();
        assert!(matches!(result, Input::Unchanged | Input::Deferred));
    }
}

struct HeldPublication {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    release: Option<tokio::sync::oneshot::Receiver<()>>,
    drained: Arc<AtomicBool>,
}
impl Workflow for HeldPublication {
    type Error = &'static str;
    async fn step(&mut self) -> std::result::Result<Control, Self::Error> {
        self.entered.take().unwrap().send(()).unwrap();
        self.release.take().unwrap().await.unwrap();
        Ok(Control::Continue)
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
    let task = tokio::spawn(
        PublisherService::new(HeldPublication {
            entered: Some(entered),
            release: Some(held),
            drained: drained.clone(),
        })
        .run(async move { stop.cancelled().await }),
    );
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
    impl Workflow for &mut Failing {
        type Error = &'static str;
        async fn step(&mut self) -> std::result::Result<Control, Self::Error> {
            if self.fail_step {
                Err("step failed")
            } else {
                Ok(Control::Continue)
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
        let result = PublisherService::new(&mut state)
            .run(std::future::pending())
            .await;
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
    impl Workflow for &mut Retry {
        type Error = ();
        async fn step(&mut self) -> std::result::Result<Control, ()> {
            self.passes += 1;
            match self.passes {
                1 => Err(()),
                2 => Ok(Control::Continue),
                _ => Ok(Control::Complete),
            }
        }
        fn failed(&mut self, _: &()) -> FailurePolicy {
            FailurePolicy::NextPoll
        }
        async fn wait(&mut self) -> std::result::Result<(), ()> {
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
    PublisherService::new(&mut state)
        .run(std::future::pending())
        .await
        .unwrap();
    assert_eq!((state.passes, state.wakes), (3, 2));
    assert_eq!(started.elapsed(), Duration::from_secs(4));
}

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
