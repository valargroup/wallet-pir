//! Coordinator-owned packing charges follow the allocation through cancellation.
//! Conservative p16/q48 charges include allocator slack; qualification records
//! both this ledger and physical/cgroup peaks, not just Rust allocation sizes.
use std::sync::{
    atomic::{AtomicU64, Ordering},
    OnceLock,
};
const MIB: u64 = 1024 * 1024;
const RESIDENT: u64 = 768 * MIB;
const PREPARATION: u64 = 1024 * MIB;
const HOST_AND_REQUEST_RESERVE: u64 = 1536 * MIB;
static LIVE: AtomicU64 = AtomicU64::new(0);
static LIMIT: OnceLock<u64> = OnceLock::new();

/// Set before constructing any packing state in the standalone router process.
/// Six 768-MiB resident charges plus one 1792-MiB construction charge leave
/// 768 MiB within the 7-GiB process ceiling for admitted requests and runtime.
pub(crate) fn configure_router() -> Result<(), String> {
    LIMIT
        .set(6400 * MIB)
        .map_err(|_| "packing budget already initialized".into())
}

fn limit() -> u64 {
    *LIMIT.get_or_init(|| {
        #[allow(unused_mut)] // Linux additionally caps against the host and cgroup.
        let mut limit = std::env::var("ENHANCE_COORDINATOR_MEMORY_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(24 * 1024 * MIB);
        #[cfg(target_os = "linux")]
        {
            if let Ok(info) = std::fs::read_to_string("/proc/meminfo") {
                if let Some(kib) = info.lines().find_map(|line| {
                    line.strip_prefix("MemTotal:")
                        .and_then(|s| s.split_whitespace().next())
                        .and_then(|s| s.parse::<u64>().ok())
                }) {
                    limit = limit.min(kib * 1024);
                }
            }
            if let Ok(groups) = std::fs::read_to_string("/proc/self/cgroup") {
                if let Some(path) = groups.lines().find_map(|s| s.strip_prefix("0::")) {
                    let root =
                        std::path::Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/'));
                    if let Ok(max) = std::fs::read_to_string(root.join("memory.max")) {
                        if let Ok(bytes) = max.trim().parse::<u64>() {
                            limit = limit.min(bytes);
                        }
                    }
                }
            }
        }
        limit.saturating_sub(HOST_AND_REQUEST_RESERVE)
    })
}

pub(crate) struct Charge(u64);
impl Charge {
    pub(crate) fn prepare() -> Result<Self, String> {
        let bytes = RESIDENT + PREPARATION;
        LIVE.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |live| {
            live.checked_add(bytes).filter(|sum| *sum <= limit())
        })
        .map_err(|_| "coordinator packing memory admission refused".to_string())?;
        Ok(Self(bytes))
    }
    pub(crate) fn resident(&mut self) {
        LIVE.fetch_sub(self.0 - RESIDENT, Ordering::SeqCst);
        self.0 = RESIDENT;
    }
}
impl Drop for Charge {
    fn drop(&mut self) {
        LIVE.fetch_sub(self.0, Ordering::SeqCst);
    }
}
pub(crate) fn charged_bytes() -> u64 {
    LIVE.load(Ordering::SeqCst)
}
