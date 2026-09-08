//! What the process and its cgroup actually occupy.
//!
//! The runtime cache reserves bytes for what it knows about; the kernel
//! charges the process for everything. The gap — allocator overhead, the
//! transient plaintext a build reads, buffered request bodies, shared
//! parameters — is what a memory budget has to cover, and it can only be set
//! from a measurement of the whole process. These readings are sampled at
//! scrape time so a fleet's real headroom is on the dashboard beside the
//! reservation, not inferred from it.
//!
//! RSS and cgroup readings are Linux-only. CPU and process start time use
//! sysinfo where available; unavailable readings are omitted.

/// Resident set size of this process, in bytes.
pub fn process_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        let page = unsafe_page_size();
        Some(resident_pages * page)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(target_os = "linux")]
fn unsafe_page_size() -> u64 {
    // `sysconf(_SC_PAGESIZE)` without a libc dependency: the kernel reports it
    // in the auxiliary vector, and 4096 is right on every host this runs on.
    std::fs::read("/proc/self/auxv")
        .ok()
        .and_then(|auxv| {
            const AT_PAGESZ: u64 = 6;
            auxv.chunks_exact(16).find_map(|entry| {
                let key = u64::from_ne_bytes(entry[..8].try_into().ok()?);
                let value = u64::from_ne_bytes(entry[8..].try_into().ok()?);
                (key == AT_PAGESZ).then_some(value)
            })
        })
        .unwrap_or(4096)
}

/// The cgroup v2 memory charge and limit for this process, in bytes.
///
/// `memory.max` reads `max` when unlimited; that is reported as `None` for
/// the limit while the current charge is still returned.
pub fn cgroup_memory_bytes() -> Option<(u64, Option<u64>)> {
    #[cfg(target_os = "linux")]
    {
        let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
        // cgroup v2: a single line `0::/path`.
        let path = cgroup
            .lines()
            .find_map(|line| line.strip_prefix("0::"))?
            .trim();
        let base = std::path::Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/'));
        let current: u64 = std::fs::read_to_string(base.join("memory.current"))
            .ok()?
            .trim()
            .parse()
            .ok()?;
        let max = std::fs::read_to_string(base.join("memory.max"))
            .ok()
            .and_then(|text| text.trim().parse::<u64>().ok());
        Some((current, max))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Cumulative process CPU milliseconds and Unix process start time. Sampling a
/// single process avoids treating a first-refresh CPU percentage as a real zero.
pub fn process_cpu() -> Option<(u64, u64)> {
    let pid = sysinfo::get_current_pid().ok()?;
    let mut system = sysinfo::System::new();
    system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    let process = system.process(pid)?;
    Some((process.accumulated_cpu_time(), process.start_time()))
}
