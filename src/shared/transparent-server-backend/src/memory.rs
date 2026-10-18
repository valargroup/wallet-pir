//! Admission for allocations outside the runtime cache. Reservations remain
//! owned by blocking work after its HTTP/control caller is cancelled.
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct WorkMemory {
    reserved: Mutex<u64>,
    build_waiters: tokio::sync::Mutex<()>,
}

pub struct Reservation {
    owner: Arc<WorkMemory>,
    bytes: u64,
}

impl WorkMemory {
    /// Leave ten percent of the cgroup limit for unmodelled process overhead.
    /// This complements (and does not replace) the runtime cache budget.
    /// Charge outstanding reservations again even if partly materialized in
    /// the cgroup's memory in use: conservative admission is preferable to an
    /// OOM kill. Page cache the kernel can drop is not in use.
    pub fn reserve(self: &Arc<Self>, bytes: u64) -> Option<Reservation> {
        self.reserve_at(bytes, crate::procmem::cgroup_memory_in_use_bytes())
    }

    /// An already-admitted query may briefly wait for construction scratch to
    /// retire. It still owns one of the bounded query slots; cancellation drops
    /// the wait. Its budget is the smaller of the caller deadline and 250 ms.
    pub async fn reserve_query(
        self: &Arc<Self>,
        bytes: u64,
        remaining: std::time::Duration,
    ) -> Option<Reservation> {
        self.reserve_query_with(bytes, remaining, crate::procmem::cgroup_memory_in_use_bytes)
            .await
    }

    async fn reserve_query_with(
        self: &Arc<Self>,
        bytes: u64,
        remaining: std::time::Duration,
        sample: impl Fn() -> Option<(u64, Option<u64>)>,
    ) -> Option<Reservation> {
        let budget = remaining.min(std::time::Duration::from_millis(250));
        if budget.is_zero() {
            return None;
        }
        tokio::time::timeout(budget, async {
            loop {
                if let Some(reservation) = self.reserve_at(bytes, sample()) {
                    return reservation;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .ok()
    }

    /// Cold builders queue fairly while scratch is busy. Keep the build permit
    /// while waiting so newly scheduled work cannot bypass an older builder.
    /// Restores remain nonblocking; queries have a short bounded wait. All work
    /// uses the same guard.
    pub async fn reserve_build(self: &Arc<Self>, bytes: u64) -> Option<Reservation> {
        self.reserve_build_with(bytes, crate::procmem::cgroup_memory_in_use_bytes)
            .await
    }

    async fn reserve_build_with(
        self: &Arc<Self>,
        bytes: u64,
        sample: impl Fn() -> Option<(u64, Option<u64>)>,
    ) -> Option<Reservation> {
        let started = std::time::Instant::now();
        let _turn = self.build_waiters.lock().await;
        let result = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Some(reservation) = self.reserve_at(bytes, sample()) {
                    return reservation;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        })
        .await
        .ok();
        tracing::debug!(
            seconds = started.elapsed().as_secs_f64(),
            admitted = result.is_some(),
            "cold build admission wait"
        );
        result
    }

    pub(crate) fn reserve_at(
        self: &Arc<Self>,
        bytes: u64,
        sample: Option<(u64, Option<u64>)>,
    ) -> Option<Reservation> {
        let mut held = self.reserved.lock().expect("work memory");
        let next = held.checked_add(bytes)?;
        if let Some((current, Some(limit))) = sample {
            if current.checked_add(next)? > limit.saturating_sub(limit / 10) {
                tracing::debug!(
                    current,
                    limit,
                    held = *held,
                    requested = bytes,
                    "work memory admission denied"
                );
                return None;
            }
        }
        *held = next;
        Some(Reservation {
            owner: self.clone(),
            bytes,
        })
    }

    pub fn reserved_bytes(&self) -> u64 {
        *self.reserved.lock().expect("work memory")
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        *self.owner.reserved.lock().expect("work memory") -= self.bytes;
    }
}

impl Reservation {
    /// Release a completed phase's allocations without an unreserved gap for
    /// the remaining work. The caller must drop that phase's buffers first.
    /// Growth requires a fresh admission check; this operation only shrinks.
    pub(crate) fn shrink_to(&mut self, bytes: u64) {
        assert!(
            bytes <= self.bytes,
            "a reservation cannot grow without admission"
        );
        let mut held = self.owner.reserved.lock().expect("work memory");
        *held -= self.bytes - bytes;
        self.bytes = bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn phase_handoff_retains_remaining_work_and_other_owners() {
        let memory = Arc::new(WorkMemory::default());
        let mut build = memory.reserve_at(400, Some((400, Some(1000)))).unwrap();
        let other = memory.reserve_at(100, Some((400, Some(1000)))).unwrap();
        assert!(memory.reserve_at(1, Some((400, Some(1000)))).is_none());
        build.shrink_to(150);
        let query = memory.reserve_at(250, Some((400, Some(1000)))).unwrap();
        assert!(memory.reserve_at(1, Some((400, Some(1000)))).is_none());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| build.shrink_to(151)))
                .is_err()
        );
        assert_eq!(memory.reserved_bytes(), 500);
        drop(build);
        assert_eq!(memory.reserved_bytes(), 350);
        drop(other);
        drop(query);
        assert_eq!(memory.reserved_bytes(), 0);
    }
    #[test]
    fn simultaneous_work_and_resident_memory_share_headroom() {
        let memory = Arc::new(WorkMemory::default());
        let first = memory.reserve_at(200, Some((600, Some(1000)))).unwrap();
        assert!(memory.reserve_at(101, Some((600, Some(1000)))).is_none());
        drop(first);
        assert!(memory.reserve_at(300, Some((600, Some(1000)))).is_some());
        assert_eq!(memory.reserved_bytes(), 0);
    }

    #[tokio::test]
    async fn cold_builders_keep_fifo_order_until_memory_is_released() {
        let memory = Arc::new(WorkMemory::default());
        let active = memory.reserve_at(100, Some((500, Some(1000)))).unwrap();
        let first = {
            let memory = memory.clone();
            tokio::spawn(async move {
                memory
                    .reserve_build_with(400, || Some((500, Some(1000))))
                    .await
                    .unwrap()
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while memory.build_waiters.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let second = {
            let memory = memory.clone();
            // This smaller request fits now, but must not bypass the first.
            tokio::spawn(async move {
                memory
                    .reserve_build_with(200, || Some((500, Some(1000))))
                    .await
                    .unwrap()
            })
        };
        assert_eq!(memory.reserved_bytes(), 100);
        drop(active);
        let first = tokio::time::timeout(std::time::Duration::from_secs(2), first)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(memory.reserved_bytes(), 400);
        assert!(!second.is_finished());
        drop(first);
        let second = tokio::time::timeout(std::time::Duration::from_secs(2), second)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(memory.reserved_bytes(), 200);
        drop(second);
        assert_eq!(memory.reserved_bytes(), 0);
    }

    #[tokio::test]
    async fn cancelling_a_queued_builder_releases_its_turn() {
        let memory = Arc::new(WorkMemory::default());
        let first = {
            let memory = memory.clone();
            tokio::spawn(async move {
                memory
                    .reserve_build_with(500, || Some((500, Some(1000))))
                    .await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while memory.build_waiters.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        first.abort();
        assert!(matches!(first.await, Err(error) if error.is_cancelled()));
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            memory.reserve_build_with(200, || Some((500, Some(1000)))),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(memory.reserved_bytes(), 200);
        drop(next);
    }

    #[tokio::test]
    async fn cancelled_waiter_cannot_release_blocking_work_reservation() {
        let memory = Arc::new(WorkMemory::default());
        let guard = memory.reserve_at(200, Some((600, Some(1000)))).unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                let _ = started_tx.send(());
                finish_rx.recv().unwrap();
                drop(guard);
                let _ = done_tx.send(());
            })
            .await
            .unwrap();
        });
        started_rx.await.unwrap();
        waiter.abort();
        let _ = waiter.await;
        assert_eq!(memory.reserved_bytes(), 200);
        finish_tx.send(()).unwrap();
        done_rx.await.unwrap();
        assert_eq!(memory.reserved_bytes(), 0);
    }
    #[tokio::test]
    async fn admitted_query_waits_for_memory_without_exceeding_the_budget() {
        let memory = Arc::new(WorkMemory::default());
        let held = memory.reserve_at(400, Some((400, Some(1000)))).unwrap();
        let (query, ()) = tokio::join!(
            memory.reserve_query_with(200, std::time::Duration::from_secs(1), || Some((
                400,
                Some(1000)
            ))),
            async {
                tokio::task::yield_now().await;
                drop(held);
            }
        );
        let query = query.expect("released memory should admit the waiting query");
        assert_eq!(memory.reserved_bytes(), 200);
        drop(query);
        assert_eq!(memory.reserved_bytes(), 0);
    }

    #[tokio::test]
    async fn expired_or_cancelled_query_wait_does_not_leave_a_reservation() {
        let memory = Arc::new(WorkMemory::default());
        assert!(memory
            .reserve_query_with(200, std::time::Duration::ZERO, || Some((400, Some(1000))))
            .await
            .is_none());
        assert_eq!(memory.reserved_bytes(), 0);
        let held = memory.reserve_at(400, Some((400, Some(1000)))).unwrap();
        assert!(memory
            .reserve_query_with(200, std::time::Duration::ZERO, || Some((400, Some(1000))))
            .await
            .is_none());
        assert_eq!(memory.reserved_bytes(), 400);
        let waiting = {
            let memory = memory.clone();
            tokio::spawn(async move {
                memory
                    .reserve_query_with(200, std::time::Duration::from_secs(1), || {
                        Some((400, Some(1000)))
                    })
                    .await
            })
        };
        tokio::task::yield_now().await;
        waiting.abort();
        assert!(matches!(waiting.await, Err(error) if error.is_cancelled()));
        drop(held);
        assert_eq!(memory.reserved_bytes(), 0);
    }
}
