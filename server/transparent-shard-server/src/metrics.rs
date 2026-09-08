//! What the worker reports about itself.
//!
//! The predecessor of this module was `/v1/health` returning a shard count and
//! how many runtimes had been built. That was enough when runtimes were built
//! once and kept forever, because nothing could go wrong quietly: the process
//! either had room for its whole set or it was killed.
//!
//! A bounded cache changes that. It can thrash, it can spend its time
//! rebuilding what it just evicted, and it can refuse work — all while looking
//! healthy from outside. So the numbers that decide whether a worker is sized
//! correctly are counters here rather than inferences from latency.
//!
//! These are operator bytes. The edge does not route them; the deploy asserts
//! that, because the way it goes wrong is a route wider than intended and the
//! failure is otherwise silent.

use std::sync::atomic::{AtomicU64, Ordering};

/// Counters and gauges for one process.
///
/// Relaxed ordering throughout: these are observations, and a reader that saw
/// two of them from either side of one request would draw the same conclusion.
/// Paying for stronger ordering on the query path to make a scrape
/// self-consistent would be spending latency on a report.
#[derive(Debug, Default)]
pub struct Metrics {
    pub queries: AtomicU64,
    pub query_errors: AtomicU64,
    /// Queries refused because the cache had no room it could free.
    pub overloads: AtomicU64,
    /// Queries and setups refused because the revision they named is gone.
    pub stale_revisions: AtomicU64,
    pub setups: AtomicU64,
    pub builds: AtomicU64,
    pub build_failures: AtomicU64,
    pub build_micros: AtomicU64,
    /// Requests that waited on a build another request had already started,
    /// rather than starting a second one.
    ///
    /// The number the previous implementation could not have reported, because
    /// it did duplicate the work instead.
    pub build_coalesced: AtomicU64,
    pub evictions: AtomicU64,
    /// Bytes the cache currently has reserved.
    pub resident_bytes: AtomicU64,
    pub cache_entries: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    /// Microseconds spent waiting for a query slot.
    pub query_queue_micros: AtomicU64,
    /// Manifests served.
    pub manifests: AtomicU64,
    /// Queries refused before any work because their declared length was not
    /// the one their table demands.
    pub query_length_rejections: AtomicU64,
    /// Requests refused because every waiting place was taken.
    pub queue_rejections: AtomicU64,
    /// Queries refused because the body budget could not hold them.
    pub body_budget_rejections: AtomicU64,
    /// Queries whose body did not arrive within the upload deadline.
    pub upload_timeouts: AtomicU64,
    /// Requests that waited their whole deadline without a slot.
    pub deadline_exceeded: AtomicU64,
    /// Requests dropped by their client before they were answered.
    pub queries_cancelled: AtomicU64,
    /// Requests currently counted as waiting or running.
    pub query_queue_depth: AtomicU64,
    /// Query body bytes currently buffered.
    pub body_bytes_in_flight: AtomicU64,
}

impl Metrics {
    pub fn incr(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(counter: &AtomicU64, by: u64) {
        counter.fetch_add(by, Ordering::Relaxed);
    }

    pub fn set(gauge: &AtomicU64, to: u64) {
        gauge.store(to, Ordering::Relaxed);
    }

    pub fn sub(gauge: &AtomicU64, by: u64) {
        gauge.fetch_sub(by, Ordering::Relaxed);
    }

    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    /// Renders the Prometheus text exposition format.
    pub fn render(&self, shards: usize, revisions: usize, budget_bytes: u64) -> String {
        let mut out = String::new();
        let mut line = |name: &str, kind: &str, help: &str, value: u64| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}\n"
            ));
        };
        line(
            "transparent_shard_shards",
            "gauge",
            "Shards the served map names.",
            shards as u64,
        );
        line(
            "transparent_shard_revisions_held",
            "gauge",
            "Shard revisions loaded, current and superseded.",
            revisions as u64,
        );
        line(
            "transparent_shard_cache_budget_bytes",
            "gauge",
            "Byte budget for prepared runtimes.",
            budget_bytes,
        );
        line(
            "transparent_shard_cache_resident_bytes",
            "gauge",
            "Bytes the runtime cache has reserved.",
            Self::get(&self.resident_bytes),
        );
        line(
            "transparent_shard_cache_entries",
            "gauge",
            "Prepared runtimes held.",
            Self::get(&self.cache_entries),
        );
        line(
            "transparent_shard_cache_hits_total",
            "counter",
            "Runtime lookups served from the cache.",
            Self::get(&self.cache_hits),
        );
        line(
            "transparent_shard_cache_misses_total",
            "counter",
            "Runtime lookups that had to build.",
            Self::get(&self.cache_misses),
        );
        line(
            "transparent_shard_evictions_total",
            "counter",
            "Runtimes evicted to stay inside the budget.",
            Self::get(&self.evictions),
        );
        line(
            "transparent_shard_builds_total",
            "counter",
            "Runtimes built.",
            Self::get(&self.builds),
        );
        line(
            "transparent_shard_build_failures_total",
            "counter",
            "Runtime builds that failed.",
            Self::get(&self.build_failures),
        );
        line(
            "transparent_shard_build_microseconds_total",
            "counter",
            "Time spent building runtimes.",
            Self::get(&self.build_micros),
        );
        line(
            "transparent_shard_build_coalesced_total",
            "counter",
            "Requests that waited on an in-flight build instead of starting one.",
            Self::get(&self.build_coalesced),
        );
        line(
            "transparent_shard_queries_total",
            "counter",
            "Private queries answered.",
            Self::get(&self.queries),
        );
        line(
            "transparent_shard_query_errors_total",
            "counter",
            "Private queries rejected.",
            Self::get(&self.query_errors),
        );
        line(
            "transparent_shard_overloads_total",
            "counter",
            "Requests refused because the cache could free no room.",
            Self::get(&self.overloads),
        );
        line(
            "transparent_shard_stale_revisions_total",
            "counter",
            "Requests naming a revision this worker no longer holds.",
            Self::get(&self.stale_revisions),
        );
        line(
            "transparent_shard_setups_total",
            "counter",
            "Published setups served.",
            Self::get(&self.setups),
        );
        line(
            "transparent_shard_query_queue_microseconds_total",
            "counter",
            "Time queries spent waiting for an evaluation slot.",
            Self::get(&self.query_queue_micros),
        );
        line(
            "transparent_shard_manifests_total",
            "counter",
            "Manifests served.",
            Self::get(&self.manifests),
        );
        line(
            "transparent_shard_query_length_rejections_total",
            "counter",
            "Queries refused before any work for declaring the wrong length.",
            Self::get(&self.query_length_rejections),
        );
        line(
            "transparent_shard_queue_rejections_total",
            "counter",
            "Requests refused because every waiting place was taken.",
            Self::get(&self.queue_rejections),
        );
        line(
            "transparent_shard_body_budget_rejections_total",
            "counter",
            "Queries refused because the body budget could not hold them.",
            Self::get(&self.body_budget_rejections),
        );
        line(
            "transparent_shard_upload_timeouts_total",
            "counter",
            "Queries whose body did not arrive within the upload deadline.",
            Self::get(&self.upload_timeouts),
        );
        line(
            "transparent_shard_deadline_exceeded_total",
            "counter",
            "Requests that waited their whole deadline without a slot.",
            Self::get(&self.deadline_exceeded),
        );
        line(
            "transparent_shard_queries_cancelled_total",
            "counter",
            "Requests dropped by their client before they were answered.",
            Self::get(&self.queries_cancelled),
        );
        line(
            "transparent_shard_query_queue_depth",
            "gauge",
            "Requests currently waiting or running.",
            Self::get(&self.query_queue_depth),
        );
        line(
            "transparent_shard_body_bytes_in_flight",
            "gauge",
            "Query body bytes currently buffered.",
            Self::get(&self.body_bytes_in_flight),
        );
        out
    }
}
