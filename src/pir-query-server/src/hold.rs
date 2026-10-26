//! Wallet reads that wait out a short refusal instead of failing at once.
//!
//! A publisher restart withdraws its authority until it publishes again, and a
//! product refuses wallets meanwhile; a deploy makes that a gap of seconds. An
//! idempotent read retries its own handler, re-checking authority on every
//! attempt, so it never answers from a view it could not answer from directly.
//! Waiting is bounded in time and in the number of waiting requests, so a real
//! outage still refuses, only later.
use std::{
    future::Future,
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::sync::Semaphore;

/// Longer than a typical publisher restart and shorter than wallet clients'
/// 20 s request timeouts.
pub const WINDOW: Duration = Duration::from_secs(15);
const RETRY: Duration = Duration::from_millis(250);
/// Requests waiting at once on this host; others refuse immediately.
const WAITERS: usize = 512;
static WAITING: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(WAITERS)));

/// Run `attempt` until it succeeds, `retry` rejects its error, or `window` ends.
pub async fn until<T, E, F, A>(
    window: Duration,
    retry: impl Fn(&E) -> bool,
    mut attempt: A,
) -> Result<T, E>
where
    A: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
{
    let first = attempt().await;
    let error = match first {
        Err(error) if retry(&error) => error,
        done => return done,
    };
    let Ok(_waiting) = WAITING.clone().try_acquire_owned() else {
        return Err(error);
    };
    let deadline = tokio::time::Instant::now() + window;
    let mut last = error;
    while tokio::time::Instant::now() + RETRY < deadline {
        tokio::time::sleep(RETRY).await;
        match attempt().await {
            Err(error) if retry(&error) => last = error,
            done => return done,
        }
    }
    Err(last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test(start_paused = true)]
    async fn a_refusal_that_clears_within_the_window_is_answered() {
        let calls = AtomicU32::new(0);
        let answer = until(
            WINDOW,
            |_: &&str| true,
            || async {
                if calls.fetch_add(1, Ordering::SeqCst) < 8 {
                    Err("refused")
                } else {
                    Ok("answered")
                }
            },
        )
        .await;
        assert_eq!(answer, Ok("answered"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_refusal_that_lasts_still_refuses_after_the_window() {
        let started = tokio::time::Instant::now();
        let answer: Result<(), _> =
            until(WINDOW, |_: &&str| true, || async { Err("refused") }).await;
        assert_eq!(answer, Err("refused"));
        assert!(started.elapsed() <= WINDOW);
        assert!(started.elapsed() >= WINDOW - Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn an_error_it_must_not_retry_returns_at_once() {
        let calls = AtomicU32::new(0);
        let answer: Result<(), _> = until(
            WINDOW,
            |error: &&str| *error == "refused",
            || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err("gone")
            },
        )
        .await;
        assert_eq!(answer, Err("gone"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
