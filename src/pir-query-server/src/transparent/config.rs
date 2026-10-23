//! How a query worker is sized and what its readiness attests.

use crate::transparent::admission::AdmissionConfig;

/// How the worker is sized. Everything here is per process, not per shard.
#[derive(Clone, Copy, Debug)]
pub struct ServiceConfig {
    /// Bytes of prepared runtime the cache may reserve.
    ///
    /// Set below the host's real headroom: the reservation counts the database
    /// and pack matrices and nothing else, so allocator overhead, the transient
    /// plaintext a build reads, and in-flight requests all come out of the
    /// remainder.
    pub cache_bytes: u64,
    /// Runtime builds that may run at once.
    ///
    /// One by default. A build is a second of CPU and a few hundred megabytes
    /// of transient allocation; running several concurrently is how a worker
    /// inside its steady-state budget still dies during a burst of cold
    /// requests.
    pub build_slots: usize,
    /// Query evaluations that may run at once.
    ///
    /// Two by default, because evaluation is memory-bandwidth-bound and
    /// `shard-scaling` measured it saturating at two threads: one core takes 66
    /// of the ~87 GiB/s the kernel reaches and sixteen return 1.39x one core.
    /// Admitting more would add queueing without adding throughput, and would
    /// hold more runtimes pinned against eviction while it did.
    pub query_slots: usize,
    /// Requests that may wait beyond those running; see [`AdmissionConfig`].
    pub max_waiters: usize,
    /// Query body bytes that may be buffered at once.
    pub max_body_bytes: u64,
    /// How long a client has to deliver a query body once admitted.
    pub upload_deadline: std::time::Duration,
    /// How long a request may wait in total before a retryable refusal.
    pub query_deadline: std::time::Duration,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        let admission = AdmissionConfig::default();
        Self {
            cache_bytes: 4 << 30,
            build_slots: 1,
            query_slots: admission.query_slots,
            max_waiters: admission.max_waiters,
            max_body_bytes: admission.max_body_bytes,
            upload_deadline: admission.upload_deadline,
            query_deadline: admission.query_deadline,
        }
    }
}

impl ServiceConfig {
    /// The admission limits these settings imply.
    pub fn admission(&self) -> AdmissionConfig {
        AdmissionConfig {
            query_slots: self.query_slots,
            max_waiters: self.max_waiters,
            max_body_bytes: self.max_body_bytes,
            upload_deadline: self.upload_deadline,
            query_deadline: self.query_deadline,
        }
    }
}
