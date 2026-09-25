//! Bounded, cancellation-safe admission. CPU jobs own their execution permit.
use super::telemetry;
use axum::http::StatusCode;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) const REQUEST_BUDGET: Duration = Duration::from_secs(5);

pub(super) struct Admission {
    executing: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
    wait: Duration,
}
impl Admission {
    pub(super) fn production() -> Self {
        Self::new(4, 8, Duration::from_millis(250))
    }
    pub(super) fn coordinator() -> Self {
        Self::new(16, 8, Duration::from_millis(250))
    }
    fn new(executing: usize, waiting: usize, wait: Duration) -> Self {
        Self {
            executing: Arc::new(Semaphore::new(executing)),
            waiting: Arc::new(Semaphore::new(waiting)),
            wait,
        }
    }
    pub(super) async fn acquire(&self, role: &'static str) -> Result<Execution, StatusCode> {
        let start = Instant::now();
        let permit = match self.executing.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                let _slot = self.waiting.clone().try_acquire_owned().map_err(|_| {
                    telemetry::admission_rejected(role, "queue_full");
                    StatusCode::TOO_MANY_REQUESTS
                })?;
                let _waiting = Gauge::new(role, true);
                match tokio::time::timeout(self.wait, self.executing.clone().acquire_owned()).await
                {
                    Ok(Ok(permit)) => permit,
                    _ => {
                        telemetry::admission_timing(role, true, start.elapsed());
                        telemetry::admission_rejected(role, "queue_timeout");
                        return Err(StatusCode::TOO_MANY_REQUESTS);
                    }
                }
            }
        };
        telemetry::admission_timing(role, true, start.elapsed());
        Ok(Execution {
            _permit: permit,
            _gauge: Gauge::new(role, false),
            started: Instant::now(),
            role,
        })
    }
}
struct Gauge {
    role: &'static str,
    waiting: bool,
}
impl Gauge {
    fn new(role: &'static str, waiting: bool) -> Self {
        telemetry::admission_gauge(role, waiting, true);
        Self { role, waiting }
    }
}
impl Drop for Gauge {
    fn drop(&mut self) {
        telemetry::admission_gauge(self.role, self.waiting, false);
    }
}
pub(super) struct Execution {
    _permit: OwnedSemaphorePermit,
    _gauge: Gauge,
    started: Instant,
    role: &'static str,
}
impl Drop for Execution {
    fn drop(&mut self) {
        telemetry::admission_timing(self.role, false, self.started.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn bounded_queue_cancellation_timeout_and_recovery() {
        let a = Arc::new(Admission::new(1, 1, Duration::from_millis(40)));
        let active = a.acquire("router").await.unwrap();
        let copy = a.clone();
        let queued = tokio::spawn(async move { copy.acquire("router").await });
        while a.waiting.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            a.acquire("router").await,
            Err(StatusCode::TOO_MANY_REQUESTS)
        ));
        queued.abort();
        let _ = queued.await;
        assert_eq!(a.waiting.available_permits(), 1);
        assert!(matches!(
            a.acquire("router").await,
            Err(StatusCode::TOO_MANY_REQUESTS)
        ));
        assert_eq!(a.waiting.available_permits(), 1);
        drop(active);
        assert!(a.acquire("router").await.is_ok());
    }
    #[tokio::test]
    async fn cancelled_blocking_work_keeps_execution_permit() {
        let a = Admission::new(1, 1, Duration::from_millis(10));
        let permit = a.acquire("worker").await.unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let job = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            started_tx.send(()).unwrap();
            rx.recv().unwrap();
        });
        started_rx.await.unwrap();
        job.abort(); // A running blocking job cannot be cancelled.
        assert_eq!(a.executing.available_permits(), 0);
        let _ = tx.send(());
        let _ = job.await;
        assert!(a.acquire("worker").await.is_ok());
    }
}
