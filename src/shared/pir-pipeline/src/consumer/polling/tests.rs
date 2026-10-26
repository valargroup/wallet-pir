use super::*;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Probe {
    calls: usize,
    events: Vec<&'static str>,
    completed: Vec<(tokio::time::Instant, bool)>,
}
struct Task(Arc<Mutex<Probe>>);

#[tokio::test(start_paused = true)]
async fn peer_maintenance_runs_between_passes_without_shortening_error_backoff() {
    struct Maintained {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        waiting: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }
    impl SyncTask for Maintained {
        type Report = ();
        type Error = ();
        async fn synchronize(&mut self) -> Result<(), ()> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(())
        }
        async fn wait_until(&mut self, deadline: tokio::time::Instant) {
            self.waiting.notify_one();
            self.release.notified().await;
            tokio::time::sleep_until(deadline).await;
        }
    }
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let waiting = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let task = Maintained {
        calls: calls.clone(),
        waiting: waiting.clone(),
        release: release.clone(),
    };
    let running = tokio::spawn(SyncManager::new(task).run());
    waiting.notified().await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_secs(9)).await;
    release.notify_one();
    tokio::task::yield_now().await;
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "maintenance must not start another publication pass"
    );
    tokio::time::advance(Duration::from_secs(2)).await;
    waiting.notified().await;
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    running.abort();
    let _ = running.await;
}
impl SyncTask for Task {
    type Report = ();
    type Error = ();
    fn started(&mut self) {
        self.0.lock().unwrap().events.push("started");
    }
    async fn synchronize(&mut self) -> Result<(), ()> {
        let n = {
            let mut p = self.0.lock().unwrap();
            p.calls += 1;
            p.events.push("work");
            p.calls
        };
        tokio::time::sleep(Duration::from_secs(2)).await;
        if (2..=7).contains(&n) {
            Err(())
        } else {
            Ok(())
        }
    }
    fn finished(&mut self, result: &Result<(), ()>, _: SystemTime, elapsed: Duration) {
        assert_eq!(elapsed, Duration::from_secs(2));
        let mut p = self.0.lock().unwrap();
        p.events.push("finished");
        p.completed
            .push((tokio::time::Instant::now(), result.is_ok()));
    }
}
#[tokio::test(start_paused = true)]
async fn polling_is_sequential_and_backs_off_after_complete_passes() {
    let probe = Arc::new(Mutex::new(Probe::default()));
    let manager = SyncManager::new(Task(probe.clone()));
    let running = tokio::spawn(manager.run());
    // Explicit bounded advances keep the continuously running task from making
    // the test's paused clock auto-advance without an observation limit.
    for _ in 0..850 {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    running.abort();
    let _ = running.await;
    let p = probe.lock().unwrap();
    assert!(p.completed.len() >= 9);
    for (pair, failures) in p.completed.windows(2).zip([0, 1, 2, 3, 4, 5, 6, 0]) {
        let gap = pair[1].0 - pair[0].0;
        let minimum = delay(failures, 0) + Duration::from_secs(2);
        assert!(
            gap >= minimum && gap <= minimum + Duration::from_secs(2),
            "unexpected gap {gap:?}"
        );
    }
    assert!(
        !p.completed[1].1 && p.completed[7].1,
        "a success must reset backoff"
    );
    for events in p.events.chunks_exact(3) {
        assert_eq!(events, ["started", "work", "finished"]);
    }
    assert_eq!(delay(u32::MAX, 999), Duration::from_millis(120999));
    assert_eq!(delay(0, 1000), Duration::from_secs(5));
}

#[tokio::test(start_paused = true)]
async fn shorter_success_interval_preserves_sequential_work_and_failure_backoff() {
    let probe = Arc::new(Mutex::new(Probe::default()));
    let manager =
        SyncManager::new(Task(probe.clone())).with_success_interval(Duration::from_secs(1));
    let running = tokio::spawn(manager.run());
    for _ in 0..30 {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    running.abort();
    let _ = running.await;
    let p = probe.lock().unwrap();
    assert!(p.completed.len() >= 3);
    assert_eq!(p.completed[1].0 - p.completed[0].0, Duration::from_secs(3));
    let retry_gap = p.completed[2].0 - p.completed[1].0;
    assert!(retry_gap >= Duration::from_secs(12));
    assert!(retry_gap <= Duration::from_secs(14));
    for events in p.events.chunks_exact(3) {
        assert_eq!(events, ["started", "work", "finished"]);
    }
}

struct Terminal(usize);
impl SyncTask for Terminal {
    type Report = ();
    type Error = &'static str;
    async fn synchronize(&mut self) -> Result<(), &'static str> {
        self.0 += 1;
        match self.0 {
            1 => Ok(()),
            2 | 3 => Err("temporary"),
            _ => Err("fatal"),
        }
    }
    fn terminal(&self, error: &&'static str) -> bool {
        *error == "fatal"
    }
}
#[tokio::test(start_paused = true)]
async fn continuous_sync_retries_ordinary_failures_and_returns_a_terminal_error() {
    let manager = SyncManager::new(Terminal(0));
    assert_eq!(manager.run().await, "fatal");
}

/// Fails as a restarting publisher would, `restarts` times, then fatally.
struct Restarting(u32);
impl SyncTask for Restarting {
    type Report = ();
    type Error = &'static str;
    async fn synchronize(&mut self) -> Result<(), &'static str> {
        self.0 = self.0.saturating_sub(1);
        Err(if self.0 == 0 { "fatal" } else { "restarting" })
    }
    fn terminal(&self, error: &&'static str) -> bool {
        *error == "fatal"
    }
    fn transient(&self, error: &&'static str) -> bool {
        *error == "restarting"
    }
}
#[tokio::test(start_paused = true)]
async fn a_transient_failure_retries_after_a_second_until_it_lasts_too_long() {
    let started = tokio::time::Instant::now();
    assert_eq!(SyncManager::new(Restarting(6)).run().await, "fatal");
    assert_eq!(started.elapsed(), TRANSIENT_RETRY * 5);
    // Past the transient budget the ordinary backoff applies again.
    let started = tokio::time::Instant::now();
    assert_eq!(
        SyncManager::new(Restarting(TRANSIENT_ATTEMPTS + 2))
            .run()
            .await,
        "fatal"
    );
    assert!(started.elapsed() > TRANSIENT_RETRY * (TRANSIENT_ATTEMPTS + 5));
    // A long restart followed by an ordinary failure waits the first backoff
    // step, not one lengthened by the restart's retries.
    let started = tokio::time::Instant::now();
    assert_eq!(
        SyncManager::new(Restarting(TRANSIENT_ATTEMPTS + 1))
            .run()
            .await,
        "fatal"
    );
    assert!(started.elapsed() < TRANSIENT_RETRY * (TRANSIENT_ATTEMPTS + 20));
}

/// A stop requested during a pass lets that pass, with its durable writes,
/// finish; the manager then returns without starting another pass. A stop
/// during the wait between passes returns at once.
#[tokio::test(start_paused = true)]
async fn a_stop_ends_only_the_wait_between_complete_passes() {
    let probe = Arc::new(Mutex::new(Probe::default()));
    let stop = Arc::new(tokio::sync::Notify::new());
    let requested = stop.clone();
    let running = tokio::spawn(
        SyncManager::new(Task(probe.clone())).run_until(async move { requested.notified().await }),
    );
    // Mid-pass: the first pass sleeps two seconds inside `synchronize`.
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(probe.lock().unwrap().events, ["started", "work"]);
    stop.notify_one();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(running.await.unwrap().is_none());
    {
        let p = probe.lock().unwrap();
        assert_eq!(p.events, ["started", "work", "finished"]);
        assert_eq!(p.calls, 1, "no pass starts after the stop");
    }

    let probe = Arc::new(Mutex::new(Probe::default()));
    let stop = Arc::new(tokio::sync::Notify::new());
    let requested = stop.clone();
    let running = tokio::spawn(
        SyncManager::new(Task(probe.clone())).run_until(async move { requested.notified().await }),
    );
    for _ in 0..3 {
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    tokio::task::yield_now().await;
    assert_eq!(
        probe.lock().unwrap().events.len(),
        3,
        "waiting after one pass"
    );
    stop.notify_one();
    assert!(running.await.unwrap().is_none());
    assert_eq!(probe.lock().unwrap().calls, 1);
}

#[tokio::test(start_paused = true)]
async fn long_preparation_keeps_peer_promises_alive_without_restarting_it() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let renewals = Arc::new(AtomicUsize::new(0));
    let observed = renewals.clone();
    let result = super::maintain_during(
        async {
            tokio::time::sleep(Duration::from_secs(16)).await;
            42
        },
        Duration::from_secs(5),
        || {
            observed.fetch_add(1, Ordering::SeqCst);
            async {}
        },
    )
    .await;
    assert_eq!(result, 42);
    assert_eq!(renewals.load(Ordering::SeqCst), 3);
}
