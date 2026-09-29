//! Bounded, cancellation-safe admission. CPU jobs own their execution permit.
use super::telemetry;
use crate::admission::{Queue, Refusal};
use axum::http::StatusCode;
use std::time::{Duration, Instant};
use tokio::sync::OwnedSemaphorePermit;

pub(super) const REQUEST_BUDGET: Duration = Duration::from_secs(5);

pub(super) struct Admission(Queue);
impl Admission {
    /// Serving roles absorb pauses of up to a second: on the CPU host a role
    /// occasionally stops for 0.2-0.9 s, and at 20 QPS the former eight-slot,
    /// 250 ms queue turned each pause into a burst of 429s (2026-09-27 soak).
    pub(super) fn production() -> Self {
        Self::new(4, 32, Duration::from_secs(1))
    }
    pub(super) fn coordinator() -> Self {
        Self::new(16, 8, Duration::from_millis(250))
    }
    fn new(executing: usize, waiting: usize, wait: Duration) -> Self {
        // A free permit is taken without queueing; a waiting slot is held only
        // while waiting.
        Self(Queue::new(executing, waiting, wait, true))
    }
    pub(super) async fn acquire(&self, role: &'static str) -> Result<Execution, StatusCode> {
        let start = Instant::now();
        let permit = self
            .0
            .acquire(|| Gauge::new(role, true))
            .await
            .map_err(|refusal| {
                if refusal == Refusal::Full {
                    telemetry::admission_rejected(role, "queue_full");
                } else {
                    telemetry::admission_timing(role, true, start.elapsed());
                    telemetry::admission_rejected(role, "queue_timeout");
                }
                StatusCode::TOO_MANY_REQUESTS
            })?;
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
        let a = std::sync::Arc::new(Admission::new(1, 1, Duration::from_millis(40)));
        let active = a.acquire("router").await.unwrap();
        let copy = a.clone();
        let queued = tokio::spawn(async move { copy.acquire("router").await });
        while a.0.waiting_available() != 0 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            a.acquire("router").await,
            Err(StatusCode::TOO_MANY_REQUESTS)
        ));
        queued.abort();
        let _ = queued.await;
        assert_eq!(a.0.waiting_available(), 1);
        assert!(matches!(
            a.acquire("router").await,
            Err(StatusCode::TOO_MANY_REQUESTS)
        ));
        assert_eq!(a.0.waiting_available(), 1);
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
        assert_eq!(a.0.executing_available(), 0);
        let _ = tx.send(());
        let _ = job.await;
        assert!(a.acquire("worker").await.is_ok());
    }
}
