//! What the display controller reports on `/metrics`.
//!
//! Workers keep serving the last activation whatever the controller does, so
//! a stalled or halted controller looks healthy from the wallet's side. These
//! series are how an operator sees it: the node and published tips, how long
//! the oldest unpublished block has waited, and how long each cycle and each
//! of its stages took. The controller loop writes them; the status listener
//! renders them in the Prometheus text format.

use super::now_ms;
use pir_observability::Distribution;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Cycle stages, each observed once per successful cycle.
pub const STAGES: [&str; 8] = [
    "build", "verify", "digest", "write", "prebuild", "ship", "prepare", "activate",
];

/// The gauges, replaced together whenever the controller updates its status.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Gauges {
    pub node_height: u64,
    pub published_height: u64,
    /// When the oldest observed but unpublished block was first seen, in
    /// Unix milliseconds.
    pub oldest_unpublished_ms: Option<u64>,
    pub stalled: bool,
    pub halted: bool,
    pub consecutive_failures: u32,
    pub seal_queue: u64,
    /// Unix milliseconds of the last activation by a cycle, or 0.
    pub last_success_ms: u64,
    /// Unix milliseconds of the last seal staged on its owners, or 0.
    pub last_seal_ms: u64,
}

#[derive(Default)]
pub struct Metrics {
    gauges: Mutex<Gauges>,
    cycles: AtomicU64,
    cycle_failures: AtomicU64,
    cycle_seconds: Distribution,
    freshness_seconds: Distribution,
    stage_seconds: [Distribution; STAGES.len()],
}

impl Metrics {
    pub fn set_gauges(&self, gauges: Gauges) {
        *self.gauges.lock().unwrap() = gauges;
    }

    /// Counts one publication cycle, failed or not.
    pub fn cycle_finished(&self, succeeded: bool) {
        self.cycles.fetch_add(1, Ordering::Relaxed);
        if !succeeded {
            self.cycle_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records a successful cycle: its duration, each newly published block's
    /// age when it went active, and each stage that ran.
    pub fn observe_cycle(
        &self,
        seconds: f64,
        freshness: impl IntoIterator<Item = f64>,
        stages: impl IntoIterator<Item = (&'static str, f64)>,
    ) {
        self.cycle_seconds.observe(seconds);
        for age in freshness {
            self.freshness_seconds.observe(age);
        }
        for (stage, seconds) in stages {
            if let Some(index) = STAGES.iter().position(|s| *s == stage) {
                self.stage_seconds[index].observe(seconds);
            }
        }
    }

    /// Renders the Prometheus text exposition format.
    pub fn render(&self) -> String {
        let gauges = self.gauges.lock().unwrap().clone();
        let seconds = |ms: u64| ms as f64 / 1e3;
        let oldest = gauges
            .oldest_unpublished_ms
            .map_or(0., |observed| seconds(now_ms().saturating_sub(observed)));
        let mut out = String::new();
        for (name, value) in [
            ("node_height", gauges.node_height as f64),
            ("published_height", gauges.published_height as f64),
            (
                "lag_blocks",
                gauges.node_height.saturating_sub(gauges.published_height) as f64,
            ),
            ("oldest_unpublished_seconds", oldest),
            ("stalled", u8::from(gauges.stalled).into()),
            ("halted", u8::from(gauges.halted).into()),
            ("consecutive_failures", gauges.consecutive_failures.into()),
            ("seal_queue", gauges.seal_queue as f64),
            (
                "last_success_timestamp_seconds",
                seconds(gauges.last_success_ms),
            ),
            ("last_seal_timestamp_seconds", seconds(gauges.last_seal_ms)),
        ] {
            out.push_str(&format!(
                "# TYPE txid_display_{name} gauge\ntxid_display_{name} {value}\n"
            ));
        }
        for (name, counter) in [
            ("cycles_total", &self.cycles),
            ("cycle_failures_total", &self.cycle_failures),
        ] {
            out.push_str(&format!(
                "# TYPE txid_display_{name} counter\ntxid_display_{name} {}\n",
                counter.load(Ordering::Relaxed)
            ));
        }
        for (name, distribution) in [
            ("txid_display_cycle_seconds", &self.cycle_seconds),
            ("txid_display_freshness_seconds", &self.freshness_seconds),
        ] {
            out.push_str(&format!("# TYPE {name} histogram\n"));
            out.push_str(&distribution.render(name));
        }
        let name = "txid_display_cycle_stage_seconds";
        out.push_str(&format!("# TYPE {name} histogram\n"));
        for (stage, distribution) in STAGES.iter().zip(&self.stage_seconds) {
            out.push_str(&distribution.render_labeled(name, &format!("stage=\"{stage}\"")));
        }
        out
    }
}
