//! Coordinator-owned packing charges follow the allocation through cancellation.
//! Conservative p16/q48 charges include allocator slack; qualification records
//! both this ledger and physical/cgroup peaks, not just Rust allocation sizes.
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
const MIB: u64 = 1024 * 1024;
const RESIDENT: u64 = 768 * MIB;
const PREPARATION: u64 = 1024 * MIB;
const MAPPED_LOAD: u64 = 128 * MIB;
const HOST_AND_REQUEST_RESERVE: u64 = 1536 * MIB;
/// Shared admission ledger for one service. Construct once at startup and clone
/// into background tasks; constructing a ledger per request bypasses admission.
/// Separate services in one process may share a ledger when they share a limit.
#[derive(Clone)]
pub struct PackingBudget(Arc<Ledger>);
struct Ledger {
    live: AtomicU64,
    limit: u64,
    cgroup: Option<std::path::PathBuf>,
}

impl PackingBudget {
    fn new(limit: u64) -> Self {
        Self(Arc::new(Ledger {
            live: AtomicU64::new(0),
            limit,
            cgroup: None,
        }))
    }
    pub(crate) fn router() -> Self {
        let cgroup = current_cgroup();
        let limit = cgroup
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p.join("memory.max")).ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(7 * 1024 * MIB);
        Self(Arc::new(Ledger {
            live: AtomicU64::new(0),
            limit: limit.saturating_sub(HOST_AND_REQUEST_RESERVE),
            cgroup,
        }))
    }
    /// Coordinator limit derived from environment, physical memory, and cgroup.
    pub fn coordinator() -> Self {
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
        Self::new(limit.saturating_sub(HOST_AND_REQUEST_RESERVE))
    }
    pub fn charged_bytes(&self) -> u64 {
        self.0.live.load(Ordering::SeqCst)
    }
}

fn current_cgroup() -> Option<std::path::PathBuf> {
    #[cfg(target_os = "linux")]
    {
        let groups = std::fs::read_to_string("/proc/self/cgroup").ok()?;
        let path = groups.lines().find_map(|s| s.strip_prefix("0::"))?;
        let root = std::path::Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/'));
        root.join("memory.max").is_file().then_some(root)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

fn load_fits(current: u64, inactive_file: u64, limit_after_request_reserve: u64) -> bool {
    current
        .saturating_sub(inactive_file)
        .checked_add(RESIDENT + MAPPED_LOAD)
        .is_some_and(|n| n <= limit_after_request_reserve)
}

pub(crate) struct Charge {
    bytes: u64,
    budget: PackingBudget,
}
impl Charge {
    pub(crate) fn prepare(budget: &PackingBudget) -> Result<Self, String> {
        Self::reserve(budget, RESIDENT + PREPARATION)
    }
    pub(crate) fn mapping(budget: &PackingBudget) -> Result<Self, String> {
        Self::reserve(budget, RESIDENT + MAPPED_LOAD)
    }
    fn reserve(budget: &PackingBudget, bytes: u64) -> Result<Self, String> {
        if let Some(root) = &budget.0.cgroup {
            // Publication backpressure only: requests keep their existing slots.
            // Clean inactive file pages are reclaimable; anonymous allocator
            // retention must count even after logical Rust charges are released.
            let current = std::fs::read_to_string(root.join("memory.current"))
                .map_err(|e| e.to_string())?
                .trim()
                .parse::<u64>()
                .map_err(|e| e.to_string())?;
            let stat =
                std::fs::read_to_string(root.join("memory.stat")).map_err(|e| e.to_string())?;
            let counter = |name: &str| {
                stat.lines()
                    .filter_map(|line| line.split_once(' '))
                    .find_map(|(key, value)| {
                        (key == name).then(|| value.parse::<u64>().ok()).flatten()
                    })
                    .unwrap_or(0)
            };
            // Dirty/writeback pages cannot be assumed immediately reclaimable.
            // Subtract all of them conservatively, even if some are active.
            let inactive = counter("inactive_file")
                .saturating_sub(counter("file_dirty"))
                .saturating_sub(counter("file_writeback"));
            if !load_fits(current, inactive, budget.0.limit) {
                return Err(
                    "packing artifact load deferred: insufficient physical memory headroom".into(),
                );
            }
        }
        budget
            .0
            .live
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |live| {
                live.checked_add(bytes).filter(|sum| *sum <= budget.0.limit)
            })
            .map_err(|_| "coordinator packing memory admission refused".to_string())?;
        Ok(Self {
            bytes,
            budget: budget.clone(),
        })
    }
    pub(crate) fn resident(&mut self) {
        self.budget
            .0
            .live
            .fetch_sub(self.bytes - RESIDENT, Ordering::SeqCst);
        self.bytes = RESIDENT;
    }
}
impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.0.live.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapped_replacement_fits_six_live_objects_without_reducing_request_reserve() {
        let budget = PackingBudget::new(7 * 1024 * MIB - HOST_AND_REQUEST_RESERVE);
        let mut resident = Vec::new();
        for _ in 0..6 {
            let mut c = Charge::mapping(&budget).unwrap();
            c.resident();
            resident.push(c);
        }
        let mut incoming = Charge::mapping(&budget).expect("six plus replacement must progress");
        incoming.resident();
        assert!(Charge::mapping(&budget).is_err());
        drop(resident.pop());
        drop(incoming);
        assert!(Charge::mapping(&budget).is_ok());
    }

    #[test]
    fn physical_headroom_counts_retained_heap_and_allows_reclaimable_file_pages() {
        let limit = 7 * 1024 * MIB - HOST_AND_REQUEST_RESERVE;
        assert!(!load_fits(6800 * MIB, 0, limit));
        assert!(load_fits(6800 * MIB, 3000 * MIB, limit));
        assert!(load_fits(limit - RESIDENT - MAPPED_LOAD, 0, limit));
        assert!(!load_fits(limit - RESIDENT - MAPPED_LOAD + 1, 0, limit));
    }

    #[test]
    fn charges_follow_shared_ownership_and_release_preparation() {
        let budget = PackingBudget::new(RESIDENT + PREPARATION);
        let independent = PackingBudget::new(RESIDENT + PREPARATION);
        let clone = budget.clone();
        let mut charge = Charge::prepare(&budget).unwrap();
        assert_eq!(clone.charged_bytes(), RESIDENT + PREPARATION);
        assert_eq!(independent.charged_bytes(), 0);
        assert!(Charge::prepare(&clone).is_err());
        charge.resident();
        charge.resident();
        assert_eq!(budget.charged_bytes(), RESIDENT);
        let owned = Arc::new(charge);
        let retained = owned.clone();
        drop(owned);
        assert_eq!(budget.charged_bytes(), RESIDENT);
        drop(retained);
        assert_eq!(budget.charged_bytes(), 0);
        drop(Charge::prepare(&budget).unwrap());
        assert_eq!(budget.charged_bytes(), 0);
    }

    #[test]
    fn concurrent_admission_cannot_exceed_limit() {
        let budget = PackingBudget::new(2 * (RESIDENT + PREPARATION));
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let admitted = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let budget = budget.clone();
                    let barrier = barrier.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        let charge = Charge::prepare(&budget).ok();
                        barrier.wait();
                        assert!(budget.charged_bytes() <= budget.0.limit);
                        charge
                    })
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(admitted.len(), 2);
        assert_eq!(budget.charged_bytes(), budget.0.limit);
        drop(admitted);
        assert_eq!(budget.charged_bytes(), 0);
    }

    #[tokio::test]
    async fn cancellation_keeps_detached_construction_charged_until_it_finishes() {
        let budget = PackingBudget::new(RESIDENT + PREPARATION);
        let background_budget = budget.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                let charge = Charge::prepare(&background_budget).unwrap();
                started_tx.send(()).unwrap();
                finish_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .unwrap();
                drop(charge);
                let _ = done_tx.send(());
            })
            .await
            .unwrap();
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), started_rx)
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(budget.charged_bytes(), RESIDENT + PREPARATION);
        assert!(Charge::prepare(&budget).is_err());
        finish_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), done_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(budget.charged_bytes(), 0);
    }

    #[test]
    fn failed_preparation_releases_its_charge() {
        let budget = PackingBudget::new(RESIDENT + PREPARATION);
        assert!(crate::runtime::Packing::new(0, &[], &budget).is_err());
        assert_eq!(budget.charged_bytes(), 0);
    }
}
