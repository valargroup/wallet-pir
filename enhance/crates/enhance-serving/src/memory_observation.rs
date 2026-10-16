//! Host and own-cgroup memory readings for worker and packing-router metrics.
//!
//! The memory gate fails on reclaim pressure and swap growth, which process RSS
//! does not show. Each role reports its own systemd cgroup so the APM can alert
//! without SSH access to the host. A value that cannot be read is omitted, never
//! reported as zero.
use std::path::Path;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemoryObservation {
    pub host_memory_available_bytes: Option<u64>,
    pub host_memory_total_bytes: Option<u64>,
    pub cgroup_memory_bytes: Option<u64>,
    pub cgroup_swap_bytes: Option<u64>,
    /// Times the cgroup crossed `memory.high` and was throttled into reclaim.
    pub cgroup_memory_high_events_total: Option<u64>,
    /// Times the cgroup hit `memory.max`.
    pub cgroup_memory_max_events_total: Option<u64>,
    pub cgroup_oom_kills_total: Option<u64>,
}

impl MemoryObservation {
    /// Prometheus text lines named `<prefix><field>`, skipping unavailable fields.
    pub fn render(&self, prefix: &str) -> String {
        [
            (
                "host_memory_available_bytes",
                self.host_memory_available_bytes,
            ),
            ("host_memory_total_bytes", self.host_memory_total_bytes),
            ("cgroup_memory_bytes", self.cgroup_memory_bytes),
            ("cgroup_swap_bytes", self.cgroup_swap_bytes),
            (
                "cgroup_memory_high_events_total",
                self.cgroup_memory_high_events_total,
            ),
            (
                "cgroup_memory_max_events_total",
                self.cgroup_memory_max_events_total,
            ),
            ("cgroup_oom_kills_total", self.cgroup_oom_kills_total),
        ]
        .into_iter()
        .filter_map(|(name, value)| value.map(|v| format!("{prefix}{name} {v}\n")))
        .collect()
    }
}

/// Read this process's host and cgroup v2 memory state from the live system.
pub fn observe() -> MemoryObservation {
    let read = |path: &Path| std::fs::read_to_string(path).ok();
    let mut observation = read(Path::new("/proc/meminfo"))
        .map(|text| parse_meminfo(&text))
        .unwrap_or_default();
    let Some(group) = read(Path::new("/proc/self/cgroup")).and_then(|t| cgroup_path(&t)) else {
        return observation;
    };
    let dir = Path::new("/sys/fs/cgroup").join(group.trim_start_matches('/'));
    let number = |file: &str| read(&dir.join(file)).and_then(|t| t.trim().parse::<u64>().ok());
    observation.cgroup_memory_bytes = number("memory.current");
    observation.cgroup_swap_bytes = number("memory.swap.current");
    if let Some(events) = read(&dir.join("memory.events")) {
        let event = |key: &str| {
            events.lines().find_map(|line| {
                let (name, value) = line.split_once(' ')?;
                (name == key).then(|| value.trim().parse().ok()).flatten()
            })
        };
        observation.cgroup_memory_high_events_total = event("high");
        observation.cgroup_memory_max_events_total = event("max");
        observation.cgroup_oom_kills_total = event("oom_kill");
    }
    observation
}

fn parse_meminfo(text: &str) -> MemoryObservation {
    let field = |key: &str| {
        text.lines().find_map(|line| {
            let rest = line.strip_prefix(key)?.strip_prefix(':')?;
            rest.split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()?
                .checked_mul(1024)
        })
    };
    MemoryObservation {
        host_memory_available_bytes: field("MemAvailable"),
        host_memory_total_bytes: field("MemTotal"),
        ..Default::default()
    }
}

/// The unified (v2) hierarchy entry, `0::<path>`. Hybrid v1 hosts have none.
fn cgroup_path(text: &str) -> Option<String> {
    let path = text
        .lines()
        .find_map(|line| line.strip_prefix("0::"))?
        .trim();
    (path.starts_with('/') && !path.split('/').any(|part| part == "..")).then(|| path.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meminfo_values_are_kibibytes() {
        let o = parse_meminfo(
            "MemTotal:        8131360 kB\nMemFree: 1 kB\nMemAvailable:    6022012 kB\n",
        );
        assert_eq!(o.host_memory_total_bytes, Some(8_131_360 * 1024));
        assert_eq!(o.host_memory_available_bytes, Some(6_022_012 * 1024));
    }

    #[test]
    fn only_the_unified_hierarchy_is_used_and_parent_paths_are_refused() {
        assert_eq!(
            cgroup_path("12:memory:/old\n0::/system.slice/enhance-pir-worker.service\n").as_deref(),
            Some("/system.slice/enhance-pir-worker.service")
        );
        assert_eq!(cgroup_path("12:memory:/old\n"), None);
        assert_eq!(cgroup_path("0::/system.slice/../x\n"), None);
    }

    #[test]
    fn unavailable_fields_are_omitted_not_zero() {
        let o = MemoryObservation {
            cgroup_swap_bytes: Some(0),
            cgroup_oom_kills_total: Some(2),
            ..Default::default()
        };
        assert_eq!(
            o.render("enhance_worker_"),
            "enhance_worker_cgroup_swap_bytes 0\nenhance_worker_cgroup_oom_kills_total 2\n"
        );
    }
}
