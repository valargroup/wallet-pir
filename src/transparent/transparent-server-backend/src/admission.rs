//! Bounding what a worker holds for requests it has not answered yet.
//!
//! The evaluation semaphore bounds how many queries *run*. It does not bound
//! how many wait, or how many request bodies sit in memory while they do, and
//! a semaphore with unlimited waiters is exactly the shape that lets a burst —
//! or a slow client holding a connection open — grow a worker's memory past
//! its cgroup without a single query being answered. This module puts the
//! bounds in front of the semaphore: a fixed number of waiting requests, a
//! fixed budget of buffered body bytes, a deadline on the upload and another
//! on the whole wait, and explicit, retryable refusals when any of them is
//! reached.
//!
//! Every bound is a [`Pending`] guard. Dropping it — because the handler
//! returned, or because the connection went away and the future was dropped —
//! gives everything back and counts the cancellation, so a client that
//! disconnects mid-queue costs the worker nothing after the fact.

use crate::metrics::Metrics;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Bytes each body permit stands for. Permits are counted in kibibytes so a
/// budget of tens of megabytes fits a semaphore comfortably.
const BODY_UNIT: u64 = 1024;

/// How admission is sized. Everything here is per process.
#[derive(Clone, Copy, Debug)]
pub struct AdmissionConfig {
    /// Query evaluations that may run at once.
    pub query_slots: usize,
    /// Requests that may wait for a slot or a runtime, beyond those running.
    ///
    /// Sized so the queue is a buffer against jitter, not a backlog: a wallet
    /// told to retry in a second is better served than one whose request sits
    /// behind a minute of others.
    pub max_waiters: usize,
    /// Request body bytes that may be buffered at once across every waiting
    /// and running query.
    pub max_body_bytes: u64,
    /// How long a client has to deliver its body once admitted.
    pub upload_deadline: Duration,
    /// How long a request may wait, in total, before it is refused retryably.
    pub query_deadline: Duration,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            query_slots: 2,
            max_waiters: 64,
            max_body_bytes: 64 << 20,
            upload_deadline: Duration::from_secs(10),
            query_deadline: Duration::from_secs(30),
        }
    }
}

/// Why a request was not admitted.
#[derive(Debug, PartialEq, Eq)]
pub enum AdmissionError {
    /// Every waiting place is taken. Retryable.
    QueueFull,
    /// The body budget cannot hold this request's bytes right now. Retryable.
    BodyBudget,
    /// The body did not arrive within the upload deadline. Not retryable as
    /// it stands: the client, not the worker, is the slow party.
    UploadTimeout,
    /// The request waited its whole deadline without a slot. Retryable.
    DeadlineExceeded,
    ShuttingDown,
}

/// The bounds, shared by every request.
pub struct Admission {
    waiters: Arc<Semaphore>,
    body: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    config: AdmissionConfig,
    metrics: Arc<Metrics>,
}

/// A request that has been counted but is not yet evaluating.
///
/// Holds a waiting place and, for a query, its share of the body budget.
/// Dropped without [`Pending::complete`] having been called, it counts as a
/// cancellation.
pub struct Pending {
    _waiter: OwnedSemaphorePermit,
    _body: Option<OwnedSemaphorePermit>,
    body_bytes: u64,
    deadline: Instant,
    metrics: Arc<Metrics>,
    completed: bool,
}

impl Pending {
    /// Time left before the request's deadline.
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// Marks the request answered, so its release is not a cancellation.
    pub fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if !self.completed {
            Metrics::incr(&self.metrics.queries_cancelled);
        }
        Metrics::sub(&self.metrics.query_queue_depth, 1);
        Metrics::sub(&self.metrics.body_bytes_in_flight, self.body_bytes);
    }
}

/// An evaluation slot held by a test, counted as a running request.
pub struct HeldSlot {
    _waiter: OwnedSemaphorePermit,
    _slot: OwnedSemaphorePermit,
}

/// An evaluation slot, timed from acquisition to release.
///
/// Busy time over wall time and slots is the worker's utilization, the
/// signal replica scaling reads. It includes everything done while holding
/// the slot, runtime acquisition as well as evaluation.
struct Slot {
    _permit: OwnedSemaphorePermit,
    acquired: Instant,
    metrics: Arc<Metrics>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        Metrics::add(
            &self.metrics.slot_busy_micros,
            self.acquired.elapsed().as_micros() as u64,
        );
    }
}

/// A request holding an evaluation slot.
pub struct Admitted {
    pending: Pending,
    _slot: Slot,
}

impl Admitted {
    pub fn remaining(&self) -> Duration {
        self.pending.remaining()
    }

    pub fn complete(self) {
        self.pending.complete();
    }
}

impl Admission {
    pub fn new(config: AdmissionConfig, metrics: Arc<Metrics>) -> Self {
        Metrics::set(&metrics.query_slots, config.query_slots.max(1) as u64);
        Self {
            waiters: Arc::new(Semaphore::new(
                config.query_slots.max(1) + config.max_waiters,
            )),
            body: Arc::new(Semaphore::new(
                usize::try_from(config.max_body_bytes / BODY_UNIT)
                    .unwrap_or(usize::MAX)
                    .min(Semaphore::MAX_PERMITS),
            )),
            slots: Arc::new(Semaphore::new(config.query_slots.max(1))),
            config,
            metrics,
        }
    }

    /// A single forwarding operation, including upload and reply-body lifetime.
    /// It never borrows local waiting/evaluation places. Both roles still charge
    /// the SAME body semaphore. Cap the forward below the headroom needed by all
    /// local execution slots; unused headroom remains available to local waiters.
    /// `local_body_bytes` covers the compiled geometry registry, so replacing a
    /// publication cannot silently shrink the local reserve.
    pub fn forwarding(&self, local_body_bytes: u64) -> Self {
        let total = self.config.max_body_bytes / BODY_UNIT;
        let reserved = local_body_bytes.div_ceil(BODY_UNIT);
        Self {
            waiters: Arc::new(Semaphore::new(1)),
            body: self.body.clone(),
            slots: Arc::new(Semaphore::new(1)),
            config: AdmissionConfig {
                query_slots: 1,
                max_waiters: 0,
                max_body_bytes: total.saturating_sub(reserved) * BODY_UNIT,
                ..self.config
            },
            // Upload/queue/cancellation counters include both roles. Do not
            // overwrite the local execution-slot gauge or time remote waits as
            // local evaluation utilization: forwarding only uses Pending.
            metrics: self.metrics.clone(),
        }
    }

    pub fn config(&self) -> &AdmissionConfig {
        &self.config
    }

    /// Takes a waiting place and, if `body_bytes` is given, that much of the
    /// body budget. Never waits: a request that cannot be counted now is
    /// refused now, which is what keeps the counted set bounded.
    pub fn try_enter(&self, body_bytes: Option<usize>) -> Result<Pending, AdmissionError> {
        let waiter = self.waiters.clone().try_acquire_owned().map_err(|_| {
            Metrics::incr(&self.metrics.queue_rejections);
            AdmissionError::QueueFull
        })?;
        let body = match body_bytes {
            None => None,
            Some(bytes) => {
                if bytes as u64 > self.config.max_body_bytes {
                    Metrics::incr(&self.metrics.body_budget_rejections);
                    return Err(AdmissionError::BodyBudget);
                }
                let permits = (bytes as u64).div_ceil(BODY_UNIT);
                let permits = u32::try_from(permits).map_err(|_| AdmissionError::BodyBudget)?;
                Some(
                    self.body
                        .clone()
                        .try_acquire_many_owned(permits)
                        .map_err(|_| {
                            Metrics::incr(&self.metrics.body_budget_rejections);
                            AdmissionError::BodyBudget
                        })?,
                )
            }
        };
        let body_bytes = body_bytes.unwrap_or(0) as u64;
        Metrics::add(&self.metrics.query_queue_depth, 1);
        Metrics::add(&self.metrics.body_bytes_in_flight, body_bytes);
        Ok(Pending {
            _waiter: waiter,
            _body: body,
            body_bytes,
            deadline: Instant::now() + self.config.query_deadline,
            metrics: self.metrics.clone(),
            completed: false,
        })
    }

    /// Waits for an evaluation slot, no longer than the request's deadline.
    pub async fn wait_slot(&self, pending: Pending) -> Result<Admitted, AdmissionError> {
        let _timer = self.metrics.queue_wait_seconds.timer();
        let waited = Instant::now();
        let slot =
            match tokio::time::timeout(pending.remaining(), self.slots.clone().acquire_owned())
                .await
            {
                Ok(Ok(slot)) => slot,
                Ok(Err(_)) => return Err(AdmissionError::ShuttingDown),
                Err(_) => {
                    Metrics::incr(&self.metrics.deadline_exceeded);
                    return Err(AdmissionError::DeadlineExceeded);
                }
            };
        Metrics::add(
            &self.metrics.query_queue_micros,
            waited.elapsed().as_micros() as u64,
        );
        Ok(Admitted {
            _slot: Slot {
                _permit: slot,
                acquired: Instant::now(),
                metrics: pending.metrics.clone(),
            },
            pending,
        })
    }

    /// Holds one evaluation slot for as long as the guard lives.
    ///
    /// For tests that need the worker to look busy without answering anything.
    /// The guard counts as a running request, so the waiting places available
    /// to others are exactly what they would be behind a real evaluation.
    pub async fn hold_slot(&self) -> HeldSlot {
        let waiter = self
            .waiters
            .clone()
            .acquire_owned()
            .await
            .expect("the semaphore is never closed");
        let slot = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .expect("the semaphore is never closed");
        HeldSlot {
            _waiter: waiter,
            _slot: slot,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admission(slots: usize, waiters: usize, body: u64) -> Admission {
        Admission::new(
            AdmissionConfig {
                query_slots: slots,
                max_waiters: waiters,
                max_body_bytes: body,
                upload_deadline: Duration::from_millis(50),
                query_deadline: Duration::from_millis(50),
            },
            Arc::new(Metrics::default()),
        )
    }

    #[tokio::test]
    async fn forwarding_reserves_local_capacity_with_one_aggregate_body_budget() {
        let local = admission(2, 2, 8192);
        // Two original local evaluation slots, each requiring at most 2 KiB.
        let remote = local.forwarding(4096);
        assert_eq!(Metrics::get(&local.metrics.query_slots), 2);
        let forwarding = remote.try_enter(Some(4096)).unwrap();
        assert_eq!(
            remote.try_enter(None).err(),
            Some(AdmissionError::QueueFull)
        );
        let a = local
            .wait_slot(local.try_enter(Some(2048)).unwrap())
            .await
            .unwrap();
        let b = local
            .wait_slot(local.try_enter(Some(2048)).unwrap())
            .await
            .unwrap();
        assert_eq!(
            local.try_enter(Some(1)).err(),
            Some(AdmissionError::BodyBudget)
        );
        assert_eq!(Metrics::get(&local.metrics.body_bytes_in_flight), 8192);
        drop(forwarding);
        let queued = local.try_enter(Some(4096)).unwrap();
        assert_eq!(
            remote.try_enter(Some(1)).err(),
            Some(AdmissionError::BodyBudget)
        );
        drop(queued);
        drop(a);
        drop(b);
        // Local-only operation retains its original whole body capacity.
        let all = local.try_enter(Some(8192)).unwrap();
        drop(all);
        assert_eq!(
            remote.try_enter(Some(4097)).err(),
            Some(AdmissionError::BodyBudget)
        );
        let recovered = remote.try_enter(Some(4096)).unwrap();
        drop(recovered);
        assert_eq!(Metrics::get(&local.metrics.body_bytes_in_flight), 0);
        // A small configured total cannot be enlarged to fit a forward.
        let no_surplus = local.forwarding(8192);
        assert_eq!(
            no_surplus.try_enter(Some(1)).err(),
            Some(AdmissionError::BodyBudget)
        );
        assert!(no_surplus.try_enter(None).is_ok());
    }

    #[tokio::test]
    async fn a_dropped_wait_is_in_the_latency_histogram() {
        let admission = admission(1, 1, 1 << 20);
        let _held = admission.hold_slot().await;
        let pending = admission.try_enter(None).unwrap();
        let waiting = admission.wait_slot(pending);
        assert!(tokio::time::timeout(Duration::from_millis(1), waiting)
            .await
            .is_err());
        assert_eq!(admission.metrics.queue_wait_seconds.count(), 1);
        assert_eq!(Metrics::get(&admission.metrics.queries_cancelled), 1);
    }

    #[tokio::test]
    async fn slots_and_their_busy_time_are_reported() {
        let admission = admission(3, 1, 1 << 20);
        assert_eq!(Metrics::get(&admission.metrics.query_slots), 3);
        let admitted = admission
            .wait_slot(admission.try_enter(None).unwrap())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            Metrics::get(&admission.metrics.slot_busy_micros),
            0,
            "busy time is counted when the slot is released"
        );
        admitted.complete();
        assert!(Metrics::get(&admission.metrics.slot_busy_micros) >= 20_000);
    }

    #[tokio::test]
    async fn waiting_places_are_bounded_and_returned_on_drop() {
        let admission = admission(1, 1, 1 << 20);
        let first = admission.try_enter(None).unwrap();
        let second = admission.try_enter(None).unwrap();
        assert_eq!(
            admission.try_enter(None).err(),
            Some(AdmissionError::QueueFull)
        );
        drop(second);
        let third = admission.try_enter(None).unwrap();
        assert_eq!(
            Metrics::get(&admission.metrics.queries_cancelled),
            1,
            "a pending request dropped unanswered is a cancellation"
        );
        first.complete();
        drop(third);
        assert_eq!(Metrics::get(&admission.metrics.queries_cancelled), 2);
        assert_eq!(Metrics::get(&admission.metrics.query_queue_depth), 0);
    }

    #[tokio::test]
    async fn body_bytes_are_bounded_across_waiting_requests() {
        let admission = admission(4, 4, 4096);
        let a = admission.try_enter(Some(3000)).unwrap();
        assert_eq!(
            admission.try_enter(Some(2000)).err(),
            Some(AdmissionError::BodyBudget)
        );
        assert_eq!(Metrics::get(&admission.metrics.body_bytes_in_flight), 3000);
        drop(a);
        assert_eq!(Metrics::get(&admission.metrics.body_bytes_in_flight), 0);
        admission.try_enter(Some(4096)).unwrap();
    }

    #[tokio::test]
    async fn a_request_that_outwaits_its_deadline_is_refused_retryably() {
        let admission = admission(1, 4, 1 << 20);
        let held = admission.hold_slot().await;
        let pending = admission.try_enter(None).unwrap();
        assert_eq!(
            admission.wait_slot(pending).await.err(),
            Some(AdmissionError::DeadlineExceeded)
        );
        drop(held);
        let pending = admission.try_enter(None).unwrap();
        let admitted = admission.wait_slot(pending).await.unwrap();
        admitted.complete();
        assert_eq!(Metrics::get(&admission.metrics.deadline_exceeded), 1);
        assert_eq!(admission.metrics.queue_wait_seconds.count(), 2);
    }
}
