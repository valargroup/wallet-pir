//! Composable ingestion phases. Products own work, storage, candidates and authority.
//!
//! Acquisition commits before preparation; failure never rolls it back. A work
//! token may borrow a store, while an independently running input task may hand
//! owned work to its product's queue. This crate neither queues nor coalesces work.
//! Publication is an owner operation: validation, fencing, atomic activation and
//! any preparation that must share their permit stay inside that operation.
//! Observations confer no freshness or authority and carry no persisted state.
use std::{future::Future, time::Duration};
use tokio::time::Instant;

/// Products distinguish unchanged input from backpressure without errors or strings.
#[derive(Debug, PartialEq, Eq)]
pub enum Input<W> {
    Work(W),
    Unchanged,
    Deferred,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Reconcile,
    Acquire,
    Prepare,
    Publish,
}

/// Internal progress only; adapters translate into their existing status formats.
pub trait Observer {
    fn started(&mut self, _phase: Phase) {}
    fn finished(&mut self, _phase: Phase, _elapsed: Duration, _succeeded: bool) {}
}
impl Observer for () {}

/// Observe an owner operation without imposing retries or cancellation on it.
pub async fn phase<T, E>(
    phase: Phase,
    operation: impl Future<Output = Result<T, E>>,
    observer: &mut impl Observer,
) -> Result<T, E> {
    observer.started(phase);
    let started = Instant::now();
    let result = operation.await;
    observer.finished(phase, started.elapsed(), result.is_ok());
    result
}

/// Acquire and commit typed product work, or observe unchanged/deferred input.
pub async fn acquire<W, E>(
    operation: impl Future<Output = Result<Input<W>, E>>,
    observer: &mut impl Observer,
) -> Result<Input<W>, E> {
    phase(Phase::Acquire, operation, observer).await
}

/// Prepare one committed work item, then delegate publication to its existing owner.
/// The callbacks consume product tokens, including borrowed tokens, without copies.
/// Neither a failed preparation nor an uncertain activation is retried here.
/// Independent tasks can call this on their own ordered work queue while input
/// acquisition and heartbeats continue elsewhere.
pub async fn advance<W, C, T, E, P, PF, A, AF>(
    input: Input<W>,
    prepare: P,
    publish: A,
    observer: &mut impl Observer,
) -> Result<Input<T>, E>
where
    P: FnOnce(W) -> PF,
    PF: Future<Output = Result<Input<C>, E>>,
    A: FnOnce(C) -> AF,
    AF: Future<Output = Result<T, E>>,
{
    let work = match input {
        Input::Work(work) => work,
        Input::Unchanged => return Ok(Input::Unchanged),
        Input::Deferred => return Ok(Input::Deferred),
    };
    match phase(Phase::Prepare, prepare(work), observer).await? {
        Input::Work(candidate) => phase(Phase::Publish, publish(candidate), observer)
            .await
            .map(Input::Work),
        Input::Unchanged => Ok(Input::Unchanged),
        Input::Deferred => Ok(Input::Deferred),
    }
}

/// Product error policy, selected explicitly after a complete pass. Retrying a
/// pass must reconcile through the owner before any uncertain publication repeats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailurePolicy {
    NextPoll,
    Stop,
}

/// A poll task may compose acquisition and publication, or implement just one of
/// them. No shared task or queue couples independent product tasks.
#[allow(async_fn_in_trait)]
pub trait Task {
    type Error;
    async fn run(&mut self) -> Result<(), Self::Error>;
    fn failed(&mut self, error: &Self::Error) -> FailurePolicy;
    fn finished(&mut self, _elapsed: Duration) {}
}

/// Run complete passes and wait *after* each pass, preserving delay scheduling.
/// Shutdown is observed only between passes: it cannot drop an uncertain owner
/// operation. Callers retain their existing outer cancellation/recovery contract.
pub async fn supervise<T: Task>(
    task: &mut T,
    poll: Duration,
    shutdown: impl Future<Output = ()>,
) -> Result<(), T::Error> {
    publisher::PublisherService::new(publisher::workflow::Polling { task, poll })
        .run(shutdown)
        .await
}

/// A boundary wait for independently supervised tasks. Products supply their
/// existing deadline (including jitter), wake source and shutdown signal. It
/// never cancels acquisition, preparation or activation.
pub async fn wait(
    deadline: Instant,
    wake: impl Future<Output = ()>,
    shutdown: impl Future<Output = ()>,
) -> Wake {
    tokio::select! {
        _ = shutdown => Wake::Shutdown,
        _ = wake => Wake::Notified,
        _ = tokio::time::sleep_until(deadline) => Wake::Deadline,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wake {
    Deadline,
    Notified,
    Shutdown,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, future::pending};

    #[derive(Default)]
    struct Progress(Vec<(Phase, Duration, bool)>);
    impl Observer for Progress {
        fn finished(&mut self, phase: Phase, elapsed: Duration, succeeded: bool) {
            self.0.push((phase, elapsed, succeeded));
        }
    }

    #[tokio::test]
    async fn shutdown_already_requested_starts_no_new_pass() {
        struct Count(usize);
        impl Task for Count {
            type Error = ();
            async fn run(&mut self) -> Result<(), ()> {
                self.0 += 1;
                Ok(())
            }
            fn failed(&mut self, _: &()) -> FailurePolicy {
                FailurePolicy::Stop
            }
        }
        let mut task = Count(0);
        supervise(&mut task, Duration::from_secs(1), std::future::ready(()))
            .await
            .unwrap();
        assert_eq!(task.0, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn borrowed_commit_prepares_then_publishes_without_cloning() {
        let mut journal = vec![1];
        let mut progress = Progress::default();
        let input = acquire(
            async {
                journal.push(2);
                Ok::<_, ()>(Input::Work(journal.as_slice()))
            },
            &mut progress,
        )
        .await
        .unwrap();
        let published: Result<Input<usize>, ()> = advance(
            input,
            |records| async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                Ok(Input::Work(records))
            },
            |candidate| async move { Ok(candidate.len()) },
            &mut progress,
        )
        .await;
        assert_eq!(published, Ok(Input::Work(2)));
        assert_eq!(
            progress.0,
            vec![
                (Phase::Acquire, Duration::ZERO, true),
                (Phase::Prepare, Duration::from_secs(2), true),
                (Phase::Publish, Duration::ZERO, true),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn preparation_failure_keeps_commits_and_never_calls_owner() {
        let mut journal = Vec::new();
        let input = acquire(
            async {
                journal.push(7);
                Ok::<_, &str>(Input::Work(journal.as_slice()))
            },
            &mut (),
        )
        .await
        .unwrap();
        let mut progress = Progress::default();
        let result = advance(
            input,
            |_| async { Err::<Input<()>, _>("prepare") },
            |_| async {
                panic!("must not publish");
                #[allow(unreachable_code)]
                Ok(())
            },
            &mut progress,
        )
        .await;
        assert_eq!(result, Err("prepare"));
        assert_eq!(journal, [7]);
        assert_eq!(progress.0, [(Phase::Prepare, Duration::ZERO, false)]);
    }

    #[tokio::test(start_paused = true)]
    async fn no_work_and_backpressure_skip_both_phases() {
        for input in [Input::<()>::Unchanged, Input::Deferred] {
            let expected = if input == Input::Unchanged {
                Input::Unchanged
            } else {
                Input::Deferred
            };
            let mut progress = Progress::default();
            let result = advance(
                input,
                |_| async {
                    panic!("must not prepare");
                    #[allow(unreachable_code)]
                    Ok::<Input<()>, ()>(Input::Unchanged)
                },
                |_| async {
                    panic!("must not publish");
                    #[allow(unreachable_code)]
                    Ok(())
                },
                &mut progress,
            )
            .await
            .unwrap();
            assert_eq!(result, expected);
            assert!(progress.0.is_empty());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn prepare_can_report_unchanged_and_uncertain_publication_is_not_retried() {
        let publications = Cell::new(0);
        let mut progress = Progress::default();
        for unchanged in [true, false] {
            let result = advance(
                Input::Work(()),
                |_| async {
                    Ok(if unchanged {
                        Input::Unchanged
                    } else {
                        Input::Work(())
                    })
                },
                |_| async {
                    publications.set(publications.get() + 1);
                    Err::<(), _>("owner must reconcile")
                },
                &mut progress,
            )
            .await;
            assert_eq!(
                result,
                if unchanged {
                    Ok(Input::Unchanged)
                } else {
                    Err("owner must reconcile")
                }
            );
        }
        assert_eq!(publications.get(), 1);
        assert_eq!(
            progress.0.last(),
            Some(&(Phase::Publish, Duration::ZERO, false))
        );
    }

    struct Poller {
        passes: usize,
        failures: usize,
        elapsed: Vec<Duration>,
        policy: FailurePolicy,
    }
    impl Task for Poller {
        type Error = ();
        async fn run(&mut self) -> Result<(), ()> {
            tokio::time::sleep(Duration::from_secs(2)).await;
            self.passes += 1;
            if self.passes == 1 {
                Err(())
            } else {
                Ok(())
            }
        }
        fn failed(&mut self, _: &()) -> FailurePolicy {
            self.failures += 1;
            self.policy
        }
        fn finished(&mut self, elapsed: Duration) {
            self.elapsed.push(elapsed);
        }
    }
    fn poller(policy: FailurePolicy) -> Poller {
        Poller {
            passes: 0,
            failures: 0,
            elapsed: Vec::new(),
            policy,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retry_waits_after_pass_and_shutdown_does_not_cancel_work() {
        let mut task = poller(FailurePolicy::NextPoll);
        supervise(
            &mut task,
            Duration::from_secs(3),
            tokio::time::sleep(Duration::from_secs(6)),
        )
        .await
        .unwrap();
        // Passes run at 0..2 and 5..7; shutdown at 6 waits for the second pass.
        assert_eq!((task.passes, task.failures), (2, 1));
        assert_eq!(task.elapsed, [Duration::from_secs(2); 2]);
    }

    #[tokio::test(start_paused = true)]
    async fn stop_policy_does_not_retry() {
        let mut task = poller(FailurePolicy::Stop);
        assert_eq!(
            supervise(&mut task, Duration::from_secs(3), pending()).await,
            Err(())
        );
        assert_eq!((task.passes, task.failures), (1, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn boundary_wait_preserves_deadlines_notifications_and_shutdown() {
        let now = Instant::now();
        assert_eq!(
            wait(now + Duration::from_secs(3), pending(), pending()).await,
            Wake::Deadline
        );
        assert_eq!(Instant::now() - now, Duration::from_secs(3));
        assert_eq!(
            wait(now + Duration::from_secs(10), async {}, pending()).await,
            Wake::Notified
        );
        assert_eq!(
            wait(now + Duration::from_secs(10), pending(), async {}).await,
            Wake::Shutdown
        );
    }

    #[tokio::test(start_paused = true)]
    async fn independent_observation_continues_during_ordered_preparation() {
        let heartbeats = Cell::new(0);
        let mut activated = Vec::new();
        let publisher = async {
            // This ordered queue belongs to the product, not the engine.
            for seal in [1, 2, 3] {
                advance(
                    Input::Work(seal),
                    |seal| async move {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        Ok::<_, ()>(Input::Work(seal))
                    },
                    |seal| {
                        activated.push(seal);
                        async { Ok(()) }
                    },
                    &mut (),
                )
                .await
                .unwrap();
            }
        };
        let observer = async {
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                heartbeats.set(heartbeats.get() + 1);
            }
        };
        tokio::join!(publisher, observer);
        assert_eq!(activated, [1, 2, 3]);
        assert_eq!(heartbeats.get(), 5);
    }
}

/// Managed application shutdown and injected HTTP serving.
pub mod service;

/// Generic publisher service, typed phase recipes and host registration.
pub mod publisher;

/// Root-scheduled journal ingestion contracts, the chain follower and schedulers.
pub mod ingestion;

/// The one publication and ingestion executor injected into every product.
pub mod engine;

/// Shared publication consumption and sequential polling.
pub mod consumer;
