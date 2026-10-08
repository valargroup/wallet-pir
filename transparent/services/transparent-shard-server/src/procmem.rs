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

#[cfg(target_os = "linux")]
fn cgroup_dir() -> Option<std::path::PathBuf> {
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    // cgroup v2: a single line `0::/path`.
    let path = cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))?
        .trim();
    Some(std::path::Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/')))
}

/// The cgroup v2 memory charge and limit for this process, in bytes.
///
/// `memory.max` reads `max` when unlimited; that is reported as `None` for
/// the limit while the current charge is still returned.
pub fn cgroup_memory_bytes() -> Option<(u64, Option<u64>)> {
    #[cfg(target_os = "linux")]
    {
        let base = cgroup_dir()?;
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

/// The cgroup charge less the page cache reclaim can drop, and the limit.
///
/// `memory.current` also counts the cache of every file the cgroup has read or
/// written: consumed sources, snapshots, a CI runner's build outputs. The
/// kernel drops clean cache that nothing maps before it OOM-kills, so a cgroup
/// can sit at its limit with little memory in use, and admission against the
/// raw charge then refuses work that fits. Mapped file pages, such as a
/// restored runtime's preprocessing, stay counted as serving state; dirty and
/// writeback pages stay counted until written. Without a readable
/// `memory.stat` this is the raw charge.
pub fn cgroup_memory_in_use_bytes() -> Option<(u64, Option<u64>)> {
    #[cfg(target_os = "linux")]
    {
        // Read the cache first: cache that grows before the charge is read is
        // counted rather than subtracted.
        let stat =
            cgroup_dir().and_then(|base| std::fs::read_to_string(base.join("memory.stat")).ok());
        let (current, max) = cgroup_memory_bytes()?;
        Some((in_use(current, stat.as_deref().unwrap_or_default()), max))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// `current` less the clean, unmapped file pages in a `memory.stat` listing.
/// A listing missing any field it needs subtracts nothing.
#[cfg(any(target_os = "linux", test))]
fn in_use(current: u64, stat: &str) -> u64 {
    let field = |name: &str| {
        stat.lines().find_map(|line| {
            line.strip_prefix(name)?
                .strip_prefix(' ')?
                .trim()
                .parse::<u64>()
                .ok()
        })
    };
    let reclaimable = || {
        let cached = field("active_file")?.checked_add(field("inactive_file")?)?;
        let kept = field("file_mapped")?
            .checked_add(field("file_dirty")?)?
            .checked_add(field("file_writeback")?)?;
        Some(cached.saturating_sub(kept))
    };
    current.saturating_sub(reclaimable().unwrap_or(0))
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

/// Return free allocator pages after retiring large revision allocations.
/// glibc may retain freed arenas indefinitely across publication churn.
/// This does not free live objects and is only an optimization; admission
/// still uses the actual kernel charge on the next allocation.
pub fn release_allocator_pages() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        unsafe extern "C" {
            fn malloc_trim(pad: usize) -> std::ffi::c_int;
        }
        // SAFETY: glibc documents malloc_trim as thread-safe; zero retains no
        // extra top-of-heap padding. No Rust allocation pointers are exposed.
        unsafe {
            malloc_trim(0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::in_use;

    /// The idle fast CI runner's cgroup: 5.14 GB charged against its 6 GiB
    /// limit, which it had reached 6,909 times without an OOM kill, and 72 MB
    /// of it process memory. What stays is process, kernel and mapped memory.
    const CI_RUNNER: &str = "anon 72699904\nfile 4808884224\nkernel 263008256\n\
        shmem 29519872\nfile_mapped 34123776\nfile_dirty 0\nfile_writeback 0\n\
        inactive_file 1839828992\nactive_file 2939535360\n";

    #[test]
    fn reclaimable_cache_is_not_memory_in_use() {
        let current = 5_144_592_384;
        let in_use = in_use(current, CI_RUNNER);
        assert_eq!(
            in_use,
            current - (2_939_535_360 + 1_839_828_992 - 34_123_776)
        );
        assert!(in_use < 400 << 20);
    }

    #[test]
    fn mapped_dirty_and_writeback_pages_stay_counted() {
        let stat = "active_file 300\ninactive_file 700\nfile_mapped 100\n\
            file_dirty 50\nfile_writeback 25\n";
        assert_eq!(in_use(2_000, stat), 2_000 - (1_000 - 175));
        // Mapped shared memory is not on the file lists; never go negative.
        let stat =
            "active_file 0\ninactive_file 10\nfile_mapped 40\nfile_dirty 0\nfile_writeback 0\n";
        assert_eq!(in_use(2_000, stat), 2_000);
        assert_eq!(
            in_use(
                10,
                "active_file 0\ninactive_file 90\nfile_mapped 0\nfile_dirty 0\nfile_writeback 0\n"
            ),
            0
        );
    }

    #[test]
    fn an_incomplete_listing_leaves_the_charge_counted() {
        assert_eq!(in_use(2_000, ""), 2_000);
        assert_eq!(
            in_use(2_000, "active_file 300\ninactive_file 700\nfile 1000\n"),
            2_000
        );
        assert_eq!(
            in_use(2_000, "active_file 300\ninactive_file 700\nfile_mapped x\nfile_dirty 0\nfile_writeback 0\n"),
            2_000
        );
    }
}
