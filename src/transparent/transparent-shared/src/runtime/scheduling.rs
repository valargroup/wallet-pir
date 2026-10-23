//! Separate construction and validation thread pools.

/// Scheduling priority of runtime construction threads (a nice value).
#[cfg(target_os = "linux")]
pub(super) const BUILD_NICE: i32 = 10;

/// Runtime construction runs here, never in the global rayon pool that query
/// evaluation uses.
///
/// Every recent replica rebuilds its tail runtimes at every publication. In
/// the global pool that build took all of a 4-vCPU host's cores for seconds,
/// queries queued behind its parallel work items, and p99 reached 2.5 s at
/// 20 QPS on two replicas. Half the cores at lower priority keep builds
/// progressing while query evaluation wins under contention.
/// `TRANSPARENT_BUILD_THREADS` overrides the thread count.
pub fn build_pool() -> &'static rayon::ThreadPool {
    static POOL: std::sync::OnceLock<rayon::ThreadPool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let threads = std::env::var("TRANSPARENT_BUILD_THREADS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|threads| *threads > 0)
            .unwrap_or_else(|| {
                std::thread::available_parallelism().map_or(1, |cores| (cores.get() / 2).max(1))
            });
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|index| format!("runtime-build-{index}"))
            .start_handler(|_| lower_priority())
            .build()
            .expect("runtime build pool")
    })
}

/// One low-priority thread for background publication validation, so any
/// parallel work inside a runtime load or check stays on a single CPU.
pub fn validation_pool() -> &'static rayon::ThreadPool {
    static POOL: std::sync::OnceLock<rayon::ThreadPool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .thread_name(|_| "history-validation".into())
            .start_handler(|_| lower_priority())
            .build()
            .expect("validation pool")
    })
}

#[cfg(target_os = "linux")]
pub(super) fn lower_priority() {
    // On Linux, PRIO_PROCESS with a thread id changes only that thread.
    if let Err(error) =
        rustix::process::setpriority_process(Some(rustix::thread::gettid()), BUILD_NICE)
    {
        tracing::warn!(%error, "could not lower runtime build thread priority");
    }
}

#[cfg(not(target_os = "linux"))]
pub(super) fn lower_priority() {}
