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
//! A fleet changes it again. Every series carries the worker's id, role and the
//! digests of the map and assignment it runs under, so a dashboard over many
//! workers can tell a replica from an owner and a worker on the old assignment
//! from one on the new. Cold-build latency is a histogram rather than a total,
//! because the tail is what a wallet meets on a cold shard. And the process's
//! real memory is sampled beside the cache's reservation, because the gap
//! between them is what the memory budget has to cover.
//!
//! These are operator bytes. The edge does not route them; the deploy asserts
//! that, because the way it goes wrong is a route wider than intended and the
//! failure is otherwise silent.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Cold-build latency buckets, in seconds. Builds take a second or so at the
/// recent geometry and around fourteen at the archive geometry on the
/// measured host; the top bucket is for a host that is worse than measured.
const BUILD_BUCKETS: [f64; 9] = [0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0];

const REQUEST_BUCKETS: [f64; 16] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 600.0,
];

/// Records the whole scope, including early returns and dropped futures.
pub struct Timer<'a> {
    histogram: &'a Histogram,
    started: std::time::Instant,
}

impl Drop for Timer<'_> {
    fn drop(&mut self) {
        self.histogram.observe(self.started.elapsed());
    }
}

/// Query handler duration is classified at response construction. A dropped
/// handler records cancellation even when its detached evaluation finishes later.
pub struct QueryTimer {
    metrics: std::sync::Arc<Metrics>,
    started: std::time::Instant,
    outcome: usize,
}

impl QueryTimer {
    pub fn new(metrics: std::sync::Arc<Metrics>) -> Self {
        Self {
            metrics,
            started: std::time::Instant::now(),
            outcome: 2,
        }
    }
    pub fn finish(&mut self, success: bool) {
        self.outcome = usize::from(!success);
    }
}

impl Drop for QueryTimer {
    fn drop(&mut self) {
        self.metrics.query_seconds[self.outcome].observe(self.started.elapsed());
    }
}

/// A fixed-bucket histogram in Prometheus's cumulative form.
#[derive(Debug)]
pub struct Histogram {
    buckets: &'static [f64],
    counts: Vec<AtomicU64>,
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    fn new(buckets: &'static [f64]) -> Self {
        Self {
            buckets,
            counts: buckets.iter().map(|_| AtomicU64::new(0)).collect(),
            sum_micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    pub fn timer(&self) -> Timer<'_> {
        Timer {
            histogram: self,
            started: std::time::Instant::now(),
        }
    }

    pub fn observe(&self, elapsed: Duration) {
        let seconds = elapsed.as_secs_f64();
        for (bucket, count) in self.buckets.iter().zip(&self.counts) {
            if seconds <= *bucket {
                count.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.sum_micros
            .fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    fn render(&self, name: &str, help: &str, labels: &str, out: &mut String) {
        out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} histogram\n"));
        let comma = if labels.is_empty() { "" } else { "," };
        for (bucket, count) in self.buckets.iter().zip(&self.counts) {
            out.push_str(&format!(
                "{name}_bucket{{{labels}{comma}le=\"{bucket}\"}} {}\n",
                count.load(Ordering::Relaxed)
            ));
        }
        out.push_str(&format!(
            "{name}_bucket{{{labels}{comma}le=\"+Inf\"}} {}\n",
            self.count()
        ));
        let braces = if labels.is_empty() {
            String::new()
        } else {
            format!("{{{labels}}}")
        };
        out.push_str(&format!(
            "{name}_sum{braces} {}\n{name}_count{braces} {}\n",
            self.sum_micros.load(Ordering::Relaxed) as f64 / 1e6,
            self.count()
        ));
    }
}

/// Counters and gauges for one process.
///
/// Relaxed ordering throughout: these are observations, and a reader that saw
/// two of them from either side of one request would draw the same conclusion.
/// Paying for stronger ordering on the query path to make a scrape
/// self-consistent would be spending latency on a report.
#[derive(Debug)]
pub struct Metrics {
    pub http: pir_observability::HttpMetrics,
    pub query_seconds: [Histogram; 3],
    pub queue_wait_seconds: Histogram,
    pub evaluation_seconds: Histogram,
    pub queries: AtomicU64,
    pub query_errors: AtomicU64,
    /// Queries refused because the cache had no room it could free.
    pub overloads: AtomicU64,
    /// Queries and setups refused because the revision they named is gone.
    pub stale_revisions: AtomicU64,
    /// Requests for a shard this worker is not assigned.
    pub unassigned_refusals: AtomicU64,
    pub setups: AtomicU64,
    pub builds: AtomicU64,
    pub build_failures: AtomicU64,
    pub build_micros: AtomicU64,
    /// Cold-build latency, by bucket.
    pub build_seconds: Histogram,
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
    pub disk_hits: AtomicU64,
    pub disk_misses: AtomicU64,
    pub disk_load_micros: AtomicU64,
    pub disk_write_failures: AtomicU64,
    pub disk_save_pending: AtomicU64,
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
    /// Refusals sent before the body was read whose body could not be
    /// discarded in time; the connection closed under the upload.
    pub refusals_unread: AtomicU64,
    /// Requests that waited their whole deadline without a slot.
    pub deadline_exceeded: AtomicU64,
    /// Requests dropped by their client before they were answered.
    pub queries_cancelled: AtomicU64,
    /// Requests currently counted as waiting or running.
    pub query_queue_depth: AtomicU64,
    /// Query body bytes currently buffered.
    pub body_bytes_in_flight: AtomicU64,
    /// Runtimes the prewarm has built or found, and the number it aims for.
    pub warm_runtimes: AtomicU64,
    pub target_runtimes: AtomicU64,
    pub prewarm_failed: AtomicU64,
    pub prewarm_micros: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            http: pir_observability::HttpMetrics::default(),
            query_seconds: std::array::from_fn(|_| Histogram::new(&REQUEST_BUCKETS)),
            queue_wait_seconds: Histogram::new(&REQUEST_BUCKETS),
            evaluation_seconds: Histogram::new(&REQUEST_BUCKETS),
            queries: AtomicU64::new(0),
            query_errors: AtomicU64::new(0),
            overloads: AtomicU64::new(0),
            stale_revisions: AtomicU64::new(0),
            unassigned_refusals: AtomicU64::new(0),
            setups: AtomicU64::new(0),
            builds: AtomicU64::new(0),
            build_failures: AtomicU64::new(0),
            build_micros: AtomicU64::new(0),
            build_seconds: Histogram::new(&BUILD_BUCKETS),
            build_coalesced: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            resident_bytes: AtomicU64::new(0),
            cache_entries: AtomicU64::new(0),
            disk_hits: AtomicU64::new(0),
            disk_misses: AtomicU64::new(0),
            disk_load_micros: AtomicU64::new(0),
            disk_write_failures: AtomicU64::new(0),
            disk_save_pending: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            query_queue_micros: AtomicU64::new(0),
            manifests: AtomicU64::new(0),
            query_length_rejections: AtomicU64::new(0),
            queue_rejections: AtomicU64::new(0),
            body_budget_rejections: AtomicU64::new(0),
            upload_timeouts: AtomicU64::new(0),
            refusals_unread: AtomicU64::new(0),
            deadline_exceeded: AtomicU64::new(0),
            queries_cancelled: AtomicU64::new(0),
            query_queue_depth: AtomicU64::new(0),
            body_bytes_in_flight: AtomicU64::new(0),
            warm_runtimes: AtomicU64::new(0),
            target_runtimes: AtomicU64::new(0),
            prewarm_failed: AtomicU64::new(0),
            prewarm_micros: AtomicU64::new(0),
        }
    }
}

/// What a scrape says beyond the counters: the identity every series carries
/// and the gauges the set and the process supply.
#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    /// `(name, value)` pairs printed on every series. Empty in whole-set mode.
    pub labels: Vec<(String, String)>,
    pub shards: u64,
    pub assigned_shards: u64,
    pub revisions: u64,
    pub prunable_revisions: u64,
    pub cache_budget_bytes: u64,
    pub warm_runtimes: u64,
    pub work_memory_reserved_bytes: u64,
    pub process_rss_bytes: Option<u64>,
    pub process_cpu: Option<(u64, u64)>,
    pub cgroup_memory_bytes: Option<(u64, Option<u64>)>,
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
    pub fn render(&self, snapshot: &Snapshot) -> String {
        let labels = snapshot
            .labels
            .iter()
            .map(|(name, value)| format!("{name}=\"{}\"", value.replace('"', "\\\"")))
            .collect::<Vec<_>>()
            .join(",");
        let braces = if labels.is_empty() {
            String::new()
        } else {
            format!("{{{labels}}}")
        };
        let mut out = self.http.render();
        let mut line = |name: &str, kind: &str, help: &str, value: u64| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} {kind}\n{name}{braces} {value}\n"
            ));
        };
        line(
            "transparent_shard_shards",
            "gauge",
            "Shards the served map names.",
            snapshot.shards,
        );
        line(
            "transparent_shard_assigned_shards",
            "gauge",
            "Shards this worker holds tables for.",
            snapshot.assigned_shards,
        );
        line(
            "transparent_shard_revisions_held",
            "gauge",
            "Shard revisions loaded, current and superseded.",
            snapshot.revisions,
        );
        line(
            "transparent_shard_prunable_revisions",
            "gauge",
            "Superseded revisions on disk past the retention bound.",
            snapshot.prunable_revisions,
        );
        line(
            "transparent_shard_cache_budget_bytes",
            "gauge",
            "Byte budget for prepared runtimes.",
            snapshot.cache_budget_bytes,
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
            "transparent_shard_disk_hits_total",
            "counter",
            "Runtimes restored from disk.",
            Self::get(&self.disk_hits),
        );
        line(
            "transparent_shard_disk_misses_total",
            "counter",
            "Absent or rejected disk runtime entries.",
            Self::get(&self.disk_misses),
        );
        line(
            "transparent_shard_disk_load_micros_total",
            "counter",
            "Microseconds spent restoring runtimes.",
            Self::get(&self.disk_load_micros),
        );
        line(
            "transparent_shard_disk_save_pending",
            "gauge",
            "Owned runtime snapshots queued or being persisted.",
            Self::get(&self.disk_save_pending),
        );
        line(
            "transparent_shard_disk_write_failures_total",
            "counter",
            "Runtime cache writes that failed.",
            Self::get(&self.disk_write_failures),
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
            "transparent_shard_unassigned_refusals_total",
            "counter",
            "Requests for a shard this worker is not assigned.",
            Self::get(&self.unassigned_refusals),
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
            "transparent_shard_refusals_unread_total",
            "counter",
            "Early refusals whose request body could not be discarded before closing.",
            Self::get(&self.refusals_unread),
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
        line(
            "transparent_shard_prewarm_operations_total",
            "counter",
            "Runtimes the prewarm has built or found resident.",
            Self::get(&self.warm_runtimes),
        );
        line(
            "transparent_shard_target_runtimes",
            "gauge",
            "Runtimes the prewarm aims to hold.",
            Self::get(&self.target_runtimes),
        );
        line(
            "transparent_shard_prewarm_failed_total",
            "counter",
            "Runtimes the prewarm could not build.",
            Self::get(&self.prewarm_failed),
        );
        line(
            "transparent_shard_prewarm_microseconds_total",
            "counter",
            "Time the prewarm spent building.",
            Self::get(&self.prewarm_micros),
        );
        if let Some(rss) = snapshot.process_rss_bytes {
            line(
                "transparent_shard_process_rss_bytes",
                "gauge",
                "Resident set size of the process.",
                rss,
            );
        }
        line(
            "transparent_shard_warm_runtimes",
            "gauge",
            "Current publication runtimes warmed.",
            snapshot.warm_runtimes,
        );
        line(
            "transparent_shard_work_memory_reserved_bytes",
            "gauge",
            "Reservations held by build, restore and query work.",
            snapshot.work_memory_reserved_bytes,
        );
        if let Some((current, max)) = snapshot.cgroup_memory_bytes {
            line(
                "transparent_shard_cgroup_memory_current_bytes",
                "gauge",
                "Memory charged to the process's cgroup.",
                current,
            );
            if let Some(max) = max {
                line(
                    "transparent_shard_cgroup_memory_max_bytes",
                    "gauge",
                    "The cgroup's memory limit.",
                    max,
                );
            }
        }
        if let Some((millis, start)) = snapshot.process_cpu {
            out.push_str(&format!("# TYPE transparent_shard_process_cpu_seconds_total counter\ntransparent_shard_process_cpu_seconds_total{braces} {}\n# TYPE transparent_shard_process_start_time_seconds gauge\ntransparent_shard_process_start_time_seconds{braces} {start}\n", millis as f64 / 1000.0));
        }
        for (histogram, outcome) in self
            .query_seconds
            .iter()
            .zip(["success", "error", "cancelled"])
        {
            let scoped = if labels.is_empty() {
                format!("outcome=\"{outcome}\"")
            } else {
                format!("{labels},outcome=\"{outcome}\"")
            };
            let mut rendered = String::new();
            histogram.render(
                "transparent_shard_query_seconds",
                "Handler entry through response construction; excludes response transmission.",
                &scoped,
                &mut rendered,
            );
            if outcome == "success" {
                out.push_str(&rendered);
            } else {
                for line in rendered.lines().filter(|line| !line.starts_with('#')) {
                    out.push_str(line);
                    out.push('\n');
                }
            }
        }
        self.queue_wait_seconds.render(
            "transparent_shard_queue_wait_seconds",
            "Evaluation semaphore wait, including failed and cancelled waits.",
            &labels,
            &mut out,
        );
        self.evaluation_seconds.render(
            "transparent_shard_evaluation_seconds",
            "All-segment evaluation inside the blocking task; excludes runtime acquisition.",
            &labels,
            &mut out,
        );
        self.build_seconds.render(
            "transparent_shard_build_seconds",
            "Cold runtime build latency.",
            &labels,
            &mut out,
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timers_classify_success_error_and_dropped_handlers_once() {
        let metrics = std::sync::Arc::new(Metrics::default());
        {
            let mut timer = QueryTimer::new(metrics.clone());
            timer.finish(true);
        }
        {
            let mut timer = QueryTimer::new(metrics.clone());
            timer.finish(false);
        }
        {
            let _timer = QueryTimer::new(metrics.clone());
        }
        {
            let _timer = metrics.queue_wait_seconds.timer();
        }
        assert_eq!(
            metrics
                .query_seconds
                .iter()
                .map(Histogram::count)
                .collect::<Vec<_>>(),
            [1, 1, 1]
        );
        assert_eq!(metrics.queue_wait_seconds.count(), 1);
        let rendered = metrics.render(&Snapshot::default());
        assert_eq!(
            rendered
                .matches("# TYPE transparent_shard_query_seconds histogram")
                .count(),
            1
        );
    }

    #[test]
    fn labels_reach_every_series_and_the_histogram_is_cumulative() {
        let metrics = Metrics::default();
        metrics.build_seconds.observe(Duration::from_millis(300));
        metrics.build_seconds.observe(Duration::from_secs(3));
        let snapshot = Snapshot {
            labels: vec![
                ("worker_id".into(), "r1".into()),
                ("role".into(), "recent-replica".into()),
            ],
            shards: 4,
            assigned_shards: 2,
            revisions: 2,
            prunable_revisions: 0,
            cache_budget_bytes: 1,
            warm_runtimes: 1,
            work_memory_reserved_bytes: 0,
            process_rss_bytes: Some(2),
            process_cpu: Some((1500, 123)),
            cgroup_memory_bytes: Some((3, None)),
        };
        let text = metrics.render(&snapshot);
        assert!(
            text.contains("transparent_shard_shards{worker_id=\"r1\",role=\"recent-replica\"} 4")
        );
        assert!(text.contains(
            "transparent_shard_assigned_shards{worker_id=\"r1\",role=\"recent-replica\"} 2"
        ));
        assert!(text.contains(
            "transparent_shard_process_rss_bytes{worker_id=\"r1\",role=\"recent-replica\"} 2"
        ));
        assert!(text.contains("transparent_shard_build_seconds_bucket{worker_id=\"r1\",role=\"recent-replica\",le=\"0.5\"} 1"));
        assert!(text.contains("transparent_shard_build_seconds_bucket{worker_id=\"r1\",role=\"recent-replica\",le=\"4\"} 2"));
        assert!(text.contains("transparent_shard_build_seconds_bucket{worker_id=\"r1\",role=\"recent-replica\",le=\"+Inf\"} 2"));
        assert!(text.contains(
            "transparent_shard_build_seconds_count{worker_id=\"r1\",role=\"recent-replica\"} 2"
        ));
        assert!(!text.contains("cgroup_memory_max"));

        let bare = metrics.render(&Snapshot::default());
        assert!(bare.contains("transparent_shard_shards 0\n"));
        assert!(bare.contains("transparent_shard_build_seconds_bucket{le=\"+Inf\"} 2"));
    }
}
