//! Bounded retries for transient startup/revision warming admission pressure.
use crate::runtime::CacheError;
use std::{
    future::Future,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

pub(crate) async fn retry<T, F, Fut>(
    cancelled: &AtomicBool,
    budget: Duration,
    mut get: F,
) -> Result<T, CacheError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, CacheError>>,
{
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Err(CacheError::Overloaded);
        }
        match get().await {
            Err(CacheError::Overloaded) if tokio::time::Instant::now() < deadline => {
                // Other restores may have materialized their reservations in
                // memory.current. Let those operations release the extra charge
                // before retrying; never discard an assigned warm target.
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn transient_admission_does_not_permanently_lose_a_warm_target() {
        let attempts = AtomicUsize::new(0);
        let value = retry(&AtomicBool::new(false), Duration::from_secs(1), || async {
            if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                Err(CacheError::Overloaded)
            } else {
                Ok(28)
            }
        })
        .await
        .unwrap();
        assert_eq!(value, 28);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn corruption_is_not_retried_and_permanent_pressure_is_bounded() {
        let attempts = AtomicUsize::new(0);
        let result = retry(&AtomicBool::new(false), Duration::from_secs(1), || async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(CacheError::Failed("corrupt source".into()))
        })
        .await;
        assert!(matches!(result, Err(CacheError::Failed(_))));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        let result = retry(&AtomicBool::new(false), Duration::ZERO, || async {
            Err::<(), _>(CacheError::Overloaded)
        })
        .await;
        assert!(matches!(result, Err(CacheError::Overloaded)));
    }

    #[tokio::test]
    async fn cancellation_stops_retrying_an_obsolete_candidate() {
        let cancelled = AtomicBool::new(false);
        let attempts = AtomicUsize::new(0);
        let result = retry(&cancelled, Duration::from_secs(1), || async {
            attempts.fetch_add(1, Ordering::SeqCst);
            cancelled.store(true, Ordering::Release);
            Err::<(), _>(CacheError::Overloaded)
        })
        .await;
        assert!(matches!(result, Err(CacheError::Overloaded)));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
