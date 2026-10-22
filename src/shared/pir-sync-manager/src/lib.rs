//! HTTPS transfer, sequential polling and the shared publication lifecycle
//! used by every PIR query server.
//!
//! [`publication_lifecycle`] owns envelope succession, durable floors, object
//! transfer and commit ordering; products supply material hooks. A successful
//! transfer or poll confers no permission to serve.
mod discovery;
pub mod publication_lifecycle;
mod transfer;
mod transport;
pub use discovery::{is_incompatible, DiscoveryError, DISCOVERY_DEADLINE};
pub use pir_service_discovery as service_discovery;
pub use transfer::{Transfer, Verification};
pub use transport::{HttpsSource, Response};

use std::{
    future::Future,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// One complete product operation, including its final authority checks.
/// Observation callbacks describe progress only and must not change authority.
pub trait SyncTask: Send {
    type Report: Send;
    type Error: Send;

    fn synchronize(&mut self) -> impl Future<Output = Result<Self::Report, Self::Error>> + Send;
    fn started(&mut self) {}
    fn finished(
        &mut self,
        _result: &Result<Self::Report, Self::Error>,
        _completed: SystemTime,
        _elapsed: Duration,
    ) {
    }
    /// Continuous-loop logging; one-shot callers retain their existing logging.
    fn report(&self, _result: &Result<Self::Report, Self::Error>) {}
    /// Whether `error` must stop continuous synchronization instead of being
    /// retried. By default every failure is retried.
    fn terminal(&self, _error: &Self::Error) -> bool {
        false
    }
    /// Wait between publication passes. Products may maintain peer promises
    /// during this wait; the next publication pass still starts only at the deadline.
    fn wait_until(&mut self, deadline: tokio::time::Instant) -> impl Future<Output = ()> + Send {
        tokio::time::sleep_until(deadline)
    }
}

/// Owns one adapter and runs complete, non-overlapping synchronization passes.
/// Dropping the outer future retains the product's existing cancellation rules;
/// this manager never races a pass against a timer or retries part of a pass.
pub struct SyncManager<T> {
    task: T,
    success_interval: Option<Duration>,
}
impl<T> SyncManager<T> {
    pub fn new(task: T) -> Self {
        Self {
            task,
            success_interval: None,
        }
    }
    /// Use a product's freshness budget for successful passes. Failure backoff
    /// and the default cadence remain unchanged.
    pub fn with_success_interval(mut self, interval: Duration) -> Self {
        assert!(
            !interval.is_zero(),
            "successful polling interval must be positive"
        );
        self.success_interval = Some(interval);
        self
    }
    pub fn task(&self) -> &T {
        &self.task
    }
    pub fn task_mut(&mut self) -> &mut T {
        &mut self.task
    }
}
impl<T: SyncTask> SyncManager<T> {
    pub async fn poll_once(&mut self) -> Result<T::Report, T::Error> {
        let started = tokio::time::Instant::now();
        self.task.started();
        let result = self.task.synchronize().await;
        self.task
            .finished(&result, SystemTime::now(), started.elapsed());
        result
    }
    /// Poll until a pass fails with an error the task marks terminal, and
    /// return that error unchanged. Every other failure is retried with backoff.
    pub async fn run(mut self) -> T::Error {
        let mut failures = 0u32;
        loop {
            let result = self.poll_once().await;
            self.task.report(&result);
            failures = match result {
                Ok(_) => 0,
                Err(error) if self.task.terminal(&error) => return error,
                Err(_) => failures.saturating_add(1),
            };
            let jitter = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| u64::from(d.subsec_nanos()) % 1000);
            let wait = self
                .success_interval
                .filter(|_| failures == 0)
                .unwrap_or_else(|| delay(failures, jitter));
            self.task
                .wait_until(tokio::time::Instant::now() + wait)
                .await;
        }
    }
}
fn delay(failures: u32, jitter_ms: u64) -> Duration {
    let seconds = 5u64.saturating_mul(1u64 << failures.min(5)).min(120);
    Duration::from_millis(seconds * 1000 + jitter_ms % 1000)
}

#[cfg(test)]
mod tests;
