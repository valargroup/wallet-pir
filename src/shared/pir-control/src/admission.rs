//! Admission primitives shared by the Enhance roles, Status and Receiver: a
//! bounded wait queue in front of execution permits, bounded body reception,
//! and a per-client concurrency cap.
//!
//! Callers keep their own refusal responses. Each answers overload with 429
//! and differs in messages, headers and telemetry; those mappings are wire
//! behavior and stay at each call site (see `docs/serving-contract.md`).

use axum::body::{to_bytes, Body, Bytes};
use axum::http::HeaderMap;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Why a [`Queue`] refused.
#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    /// Every waiting slot is taken.
    Full,
    /// The wait deadline passed before a permit became free.
    Deadline,
    /// The execution permits were closed.
    Closed,
}

/// A fixed number of execution permits behind a bounded waiting queue.
///
/// With `fast_path`, a free execution permit is taken without occupying a
/// waiting slot, and a waiting slot is held only while waiting. Without it,
/// every request takes a waiting slot first, so a full queue refuses even if
/// an execution permit happens to be free.
pub struct Queue {
    executing: Arc<Semaphore>,
    capacity: u32,
    waiting: Arc<Semaphore>,
    wait: Duration,
    fast_path: bool,
}

impl Queue {
    /// A queue with its own `executing` permits and `waiting` slots.
    pub fn new(executing: usize, waiting: usize, wait: Duration, fast_path: bool) -> Self {
        Self::with_permits(
            Arc::new(Semaphore::new(executing)),
            Arc::new(Semaphore::new(waiting)),
            wait,
            fast_path,
        )
    }

    /// A queue over existing semaphores, for callers that also report their
    /// available permits. Permits free at construction are its capacity.
    pub fn with_permits(
        executing: Arc<Semaphore>,
        waiting: Arc<Semaphore>,
        wait: Duration,
        fast_path: bool,
    ) -> Self {
        let capacity = u32::try_from(executing.available_permits()).unwrap_or(u32::MAX);
        Self {
            executing,
            capacity,
            waiting,
            wait,
            fast_path,
        }
    }

    /// Waits for an execution permit. `on_wait` runs only when the request
    /// actually queues, and its guard is dropped when waiting ends.
    pub async fn acquire<G>(
        &self,
        on_wait: impl FnOnce() -> G,
    ) -> Result<OwnedSemaphorePermit, Refusal> {
        if self.fast_path {
            if let Ok(permit) = self.executing.clone().try_acquire_owned() {
                return Ok(permit);
            }
        }
        let _slot = self
            .waiting
            .clone()
            .try_acquire_owned()
            .map_err(|_| Refusal::Full)?;
        let _waiting = on_wait();
        match tokio::time::timeout(self.wait, self.executing.clone().acquire_owned()).await {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(Refusal::Closed),
            Err(_) => Err(Refusal::Deadline),
        }
    }

    /// Execution permits free now.
    pub fn executing_available(&self) -> usize {
        self.executing.available_permits()
    }

    /// Waiting slots free now.
    pub fn waiting_available(&self) -> usize {
        self.waiting.available_permits()
    }

    /// Waits until every execution permit is free again: admitted work has
    /// ended, including blocking work whose request was cancelled or dropped.
    /// Owners call this at shutdown before releasing what that work uses.
    pub async fn drained(&self) {
        let _all = self.executing.acquire_many(self.capacity).await;
    }
}

/// Why a body could not be read.
#[derive(Debug)]
pub enum BodyError {
    /// The deadline passed before the body completed.
    Timeout,
    /// The body exceeded the limit or the stream failed.
    Read(axum::Error),
}

impl BodyError {
    /// Whether the read stopped at the length limit rather than on a stream
    /// failure.
    pub fn over_limit(&self) -> bool {
        matches!(self, BodyError::Read(e) if e.to_string().contains("length limit"))
    }
}

impl std::fmt::Display for BodyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BodyError::Timeout => f.write_str("body deadline"),
            BodyError::Read(e) => e.fmt(f),
        }
    }
}

/// Reads at most `limit` bytes of `body` within `deadline`.
pub async fn read_body(body: Body, limit: usize, deadline: Duration) -> Result<Bytes, BodyError> {
    tokio::time::timeout(deadline, to_bytes(body, limit))
        .await
        .map_err(|_| BodyError::Timeout)?
        .map_err(BodyError::Read)
}

/// Move admission and prepared handles into blocking execution. Cancelling the
/// awaiting request cannot release them while work is still running. On success
/// the caller chooses whether to retain them in its response or release them.
pub fn spawn_admitted<G, R, F>(guard: G, work: F) -> tokio::task::JoinHandle<(R, G)>
where
    G: Send + 'static,
    R: Send + 'static,
    F: FnOnce(&G) -> R + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let result = work(&guard);
        (result, guard)
    })
}

/// The client a proxied request came from: the first `X-Forwarded-For`
/// address, else `X-Real-IP`, else the socket peer if known, else `unknown`.
///
/// The proxy must overwrite these headers; they are trusted as sent.
pub fn client_key(headers: &HeaderMap, peer: Option<IpAddr>) -> String {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
    };
    header("x-forwarded-for")
        .or_else(|| header("x-real-ip"))
        .or_else(|| peer.map(|ip| ip.to_string()))
        .unwrap_or_else(|| "unknown".into())
}

/// At most `cap` concurrent requests per client key. Entries are removed when
/// their last slot drops, so the map is bounded by clients in flight.
#[derive(Clone)]
pub struct ClientSlots {
    cap: usize,
    active: Arc<Mutex<HashMap<String, usize>>>,
}

/// One admitted request for a client; releases on drop.
pub struct ClientSlot {
    active: Arc<Mutex<HashMap<String, usize>>>,
    key: String,
}

impl ClientSlots {
    /// At most `cap` requests in flight per client.
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            active: Arc::default(),
        }
    }

    /// A slot for the client `key`, or `None` if it is at its cap.
    pub fn try_acquire(&self, key: &str) -> Option<ClientSlot> {
        let mut active = self.active.lock().unwrap();
        let count = active.entry(key.to_owned()).or_insert(0);
        if *count >= self.cap {
            if *count == 0 {
                active.remove(key);
            }
            return None;
        }
        *count += 1;
        Some(ClientSlot {
            active: self.active.clone(),
            key: key.to_owned(),
        })
    }

    /// Clients with a request in flight.
    pub fn tracked(&self) -> usize {
        self.active.lock().unwrap().len()
    }
}

impl Drop for ClientSlot {
    fn drop(&mut self) {
        let mut active = self.active.lock().unwrap();
        if let Some(count) = active.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                active.remove(&self.key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_client_slots_cap_concurrency_and_evict_idle_keys() {
        let slots = ClientSlots::new(2);
        let a1 = slots.try_acquire("1.2.3.4").expect("first slot");
        let a2 = slots.try_acquire("1.2.3.4").expect("second slot");
        assert!(slots.try_acquire("1.2.3.4").is_none(), "third is refused");
        // Another client is independent.
        let b1 = slots.try_acquire("5.6.7.8").expect("other client");
        assert_eq!(slots.tracked(), 2);
        drop(a1);
        let a3 = slots.try_acquire("1.2.3.4").expect("slot freed by drop");
        assert!(slots.try_acquire("1.2.3.4").is_none());
        drop(a2);
        drop(a3);
        assert_eq!(slots.tracked(), 1, "zero entries are evicted");
        drop(b1);
        assert_eq!(slots.tracked(), 0);
        // A refused acquisition never leaves a zero entry behind.
        let c1 = slots.try_acquire("unknown").unwrap();
        let c2 = slots.try_acquire("unknown").unwrap();
        assert!(slots.try_acquire("unknown").is_none());
        drop((c1, c2));
        assert_eq!(slots.tracked(), 0);
    }

    #[test]
    fn client_key_prefers_first_forwarded_address() {
        let mut h = HeaderMap::new();
        assert_eq!(client_key(&h, None), "unknown");
        h.insert("x-real-ip", "9.9.9.9".parse().unwrap());
        assert_eq!(client_key(&h, None), "9.9.9.9");
        h.insert(
            "x-forwarded-for",
            " 1.2.3.4 , 10.0.0.1, 127.0.0.1".parse().unwrap(),
        );
        assert_eq!(client_key(&h, None), "1.2.3.4");
        h.insert("x-forwarded-for", " ".parse().unwrap());
        assert_eq!(client_key(&h, None), "9.9.9.9");
    }

    #[tokio::test]
    async fn cancelled_blocking_wait_retains_admission_and_prepared_handle() {
        let permits = Arc::new(Semaphore::new(1));
        let clients = ClientSlots::new(1);
        let prepared = Arc::new(());
        let weak = Arc::downgrade(&prepared);
        struct Released(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Released {
            fn drop(&mut self) {
                let _ = self.0.take().unwrap().send(());
            }
        }
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let guard = (
            permits.clone().acquire_owned().await.unwrap(),
            clients.try_acquire("client").unwrap(),
            prepared,
            Released(Some(done_tx)),
        );
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocking = spawn_admitted(guard, move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        let waiter = tokio::spawn(async move {
            let result = blocking.await;
            drop(result);
        });
        started_rx.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(permits.available_permits(), 0);
        assert!(clients.try_acquire("client").is_none());
        assert!(weak.upgrade().is_some());
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(weak.upgrade().is_none());
        assert_eq!(permits.available_permits(), 1);
        assert!(clients.try_acquire("client").is_some());
    }

    #[tokio::test]
    async fn drained_waits_for_admitted_blocking_work_whose_request_was_dropped() {
        let q = Arc::new(Queue::new(2, 1, Duration::from_millis(10), true));
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let permit = q.acquire(|| ()).await.unwrap();
        let request = tokio::spawn(spawn_admitted(permit, move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        }));
        started_rx.await.unwrap();
        // The request is abandoned, as a listener past its drain deadline does.
        request.abort();
        let _ = request.await;
        let drained = tokio::spawn({
            let q = q.clone();
            async move { q.drained().await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!drained.is_finished(), "drained before admitted work ended");
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), drained)
            .await
            .unwrap()
            .unwrap();
        // An idle queue is drained at once.
        tokio::time::timeout(Duration::from_secs(1), q.drained())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn without_a_fast_path_a_full_queue_refuses_even_with_a_free_permit() {
        let q = Queue::new(1, 0, Duration::from_millis(10), false);
        assert_eq!(q.acquire(|| ()).await.unwrap_err(), Refusal::Full);
        let q = Queue::new(1, 0, Duration::from_millis(10), true);
        assert!(q.acquire(|| ()).await.is_ok());
    }

    #[tokio::test]
    async fn a_waiter_times_out_and_frees_its_slot() {
        let q = Queue::new(1, 1, Duration::from_millis(20), true);
        let held = q.acquire(|| ()).await.unwrap();
        let mut waited = false;
        assert_eq!(
            q.acquire(|| waited = true).await.unwrap_err(),
            Refusal::Deadline
        );
        assert!(waited);
        drop(held);
        assert!(q.acquire(|| ()).await.is_ok());
    }

    #[tokio::test]
    async fn a_waiter_gets_a_released_permit() {
        let q = Arc::new(Queue::new(1, 1, Duration::from_secs(2), false));
        let held = q.acquire(|| ()).await.unwrap();
        let waiter = tokio::spawn({
            let q = q.clone();
            async move { q.acquire(|| ()).await.is_ok() }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(held);
        assert!(waiter.await.unwrap());
    }

    #[tokio::test]
    async fn bodies_over_the_limit_or_deadline_are_refused() {
        let body = read_body(Body::from(vec![0u8; 8]), 8, Duration::from_secs(1)).await;
        assert_eq!(body.unwrap().len(), 8);
        let over = read_body(Body::from(vec![0u8; 9]), 8, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(over.over_limit());
        let (_tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
        let stalled = Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx));
        let late = read_body(stalled, 8, Duration::from_millis(20))
            .await
            .unwrap_err();
        assert!(matches!(late, BodyError::Timeout));
        assert_eq!(late.to_string(), "body deadline");
    }

    #[test]
    fn client_keys_prefer_forwarded_headers_then_the_peer() {
        let mut h = HeaderMap::new();
        let peer: Option<IpAddr> = Some("10.0.0.9".parse().unwrap());
        assert_eq!(client_key(&h, peer), "10.0.0.9");
        assert_eq!(client_key(&h, None), "unknown");
        h.insert("x-real-ip", "198.51.100.2".parse().unwrap());
        assert_eq!(client_key(&h, peer), "198.51.100.2");
        h.insert(
            "x-forwarded-for",
            " 203.0.113.7 , 10.0.0.1".parse().unwrap(),
        );
        assert_eq!(client_key(&h, peer), "203.0.113.7");
    }

    #[test]
    fn client_slots_cap_each_client_and_forget_idle_ones() {
        let slots = ClientSlots::new(2);
        let a = slots.try_acquire("a").unwrap();
        let b = slots.try_acquire("a").unwrap();
        assert!(slots.try_acquire("a").is_none());
        assert!(slots.try_acquire("b").is_some());
        drop(a);
        assert!(slots.try_acquire("a").is_some());
        drop(b);
        assert_eq!(slots.tracked(), 0);
        assert!(ClientSlots::new(0).try_acquire("z").is_none());
        assert_eq!(ClientSlots::new(0).tracked(), 0);
    }
}
