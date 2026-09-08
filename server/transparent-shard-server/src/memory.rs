//! Admission for allocations outside the runtime cache. Reservations remain
//! owned by blocking work after its HTTP/control caller is cancelled.
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct WorkMemory {
    reserved: Mutex<u64>,
}

pub struct Reservation {
    owner: Arc<WorkMemory>,
    bytes: u64,
}

impl WorkMemory {
    /// Leave ten percent of the cgroup limit for unmodelled process overhead.
    /// This complements (and does not replace) the runtime cache budget.
    /// Charge outstanding reservations again even if partly materialized in
    /// memory.current: conservative admission is preferable to an OOM kill.
    pub fn reserve(self: &Arc<Self>, bytes: u64) -> Option<Reservation> {
        self.reserve_at(bytes, crate::procmem::cgroup_memory_bytes())
    }

    fn reserve_at(
        self: &Arc<Self>,
        bytes: u64,
        sample: Option<(u64, Option<u64>)>,
    ) -> Option<Reservation> {
        let mut held = self.reserved.lock().expect("work memory");
        let next = held.checked_add(bytes)?;
        if let Some((current, Some(limit))) = sample {
            if current.checked_add(next)? > limit.saturating_sub(limit / 10) {
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

#[cfg(test)]
mod tests {
    use super::*;
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
}
