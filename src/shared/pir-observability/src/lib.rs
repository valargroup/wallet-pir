//! Bounded aggregate HTTP observations. Paths and client data never become labels.
//!
//! The counters and their rendering are runtime-independent. The Axum middleware
//! and streamed-body observer that feed them are the host-only `http` feature.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "http")]
mod http;
#[cfg(feature = "http")]
pub use http::{observe, BodyObserver, ObservedBody};

pub const BOUNDS: [f64; 16] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1., 2., 5., 10., 30., 60., 120., 600.,
];

#[derive(Default)]
pub struct Distribution(Mutex<([u64; 17], f64)>);
impl Distribution {
    pub fn observe(&self, seconds: f64) {
        if !seconds.is_finite() || seconds < 0. {
            return;
        }
        let mut value = self.0.lock().unwrap();
        value.0[BOUNDS.iter().position(|b| seconds <= *b).unwrap_or(16)] += 1;
        value.1 += seconds;
    }
    pub fn render(&self, name: &str) -> String {
        self.render_labeled(name, "")
    }
    /// Renders with `labels` (`key="value"` pairs, comma separated, or empty)
    /// on every series.
    pub fn render_labeled(&self, name: &str, labels: &str) -> String {
        let value = self.0.lock().unwrap();
        let (comma, braces) = if labels.is_empty() {
            ("", String::new())
        } else {
            (",", format!("{{{labels}}}"))
        };
        let mut total = 0;
        let mut out = String::new();
        for (i, count) in value.0.iter().enumerate() {
            total += count;
            let bound = BOUNDS
                .get(i)
                .map(|n| n.to_string())
                .unwrap_or_else(|| "+Inf".into());
            out.push_str(&format!(
                "{name}_bucket{{{labels}{comma}le=\"{bound}\"}} {total}\n"
            ));
        }
        out.push_str(&format!(
            "{name}_count{braces} {total}\n{name}_sum{braces} {}\n",
            value.1
        ));
        out
    }
}

#[derive(Default, Debug)]
pub(crate) struct Operation {
    pub(crate) arrivals: u64,
    pub(crate) inflight: u64,
    pub(crate) responses: [u64; 5],
    pub(crate) cancelled: u64,
    pub(crate) upload: u64,
    pub(crate) download: u64,
    pub(crate) body_errors: u64,
    pub(crate) incomplete_responses: u64,
    pub(crate) durations: [u64; 17],
    pub(crate) duration_sum: f64,
}

#[derive(Clone, Debug)]
pub struct HttpMetrics {
    pub(crate) inner: Arc<Mutex<BTreeMap<&'static str, Operation>>>,
    started: f64,
}
impl Default for HttpMetrics {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            started: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
        }
    }
}

impl HttpMetrics {
    pub fn initialize(&self, endpoints: &[&'static str]) {
        let mut ops = self.inner.lock().unwrap();
        for endpoint in endpoints {
            ops.entry(endpoint).or_default();
        }
    }
    pub fn render(&self) -> String {
        let ops = self.inner.lock().unwrap();
        let mut out = format!(
            "pir_http_observation_version 1\npir_http_process_start_time_seconds {}\n",
            self.started
        );
        for (endpoint, op) in ops.iter() {
            for (name, value) in [
                ("arrivals_total", op.arrivals),
                ("inflight", op.inflight),
                ("cancelled_total", op.cancelled),
                ("request_body_bytes_total", op.upload),
                ("response_body_bytes_total", op.download),
                ("body_errors_total", op.body_errors),
                ("incomplete_responses_total", op.incomplete_responses),
            ] {
                out.push_str(&format!(
                    "pir_http_{name}{{endpoint=\"{endpoint}\"}} {value}\n"
                ));
            }
            for (i, value) in op.responses.iter().enumerate() {
                out.push_str(&format!(
                    "pir_http_responses_total{{endpoint=\"{endpoint}\",code=\"{}xx\"}} {value}\n",
                    i + 1
                ));
            }
            let mut cumulative = 0;
            for (i, count) in op.durations.iter().enumerate() {
                cumulative += count;
                let bound = BOUNDS
                    .get(i)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "+Inf".into());
                out.push_str(&format!("pir_http_duration_seconds_bucket{{endpoint=\"{endpoint}\",le=\"{bound}\"}} {cumulative}\n"));
            }
            out.push_str(&format!("pir_http_duration_seconds_count{{endpoint=\"{endpoint}\"}} {cumulative}\npir_http_duration_seconds_sum{{endpoint=\"{endpoint}\"}} {}\n",op.duration_sum));
        }
        out
    }
}

pub(crate) struct RequestGuard {
    pub(crate) metrics: HttpMetrics,
    pub(crate) endpoint: &'static str,
    pub(crate) began: Instant,
    pub(crate) code: Option<u16>,
}
impl Drop for RequestGuard {
    fn drop(&mut self) {
        let elapsed = self.began.elapsed().as_secs_f64();
        let mut ops = self.metrics.inner.lock().unwrap();
        let op = ops.entry(self.endpoint).or_default();
        op.inflight = op.inflight.saturating_sub(1);
        if let Some(code) = self.code {
            op.responses[(usize::from(code / 100).clamp(1, 5)) - 1] += 1;
        } else {
            op.cancelled += 1;
        }
        op.durations[BOUNDS.iter().position(|b| elapsed <= *b).unwrap_or(16)] += 1;
        op.duration_sum += elapsed;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn labels_reach_every_distribution_series() {
        let distribution = Distribution::default();
        distribution.observe(0.5);
        distribution.observe(3.);
        let text = distribution.render_labeled("d", "stage=\"build\"");
        assert!(text.contains("d_bucket{stage=\"build\",le=\"0.5\"} 1\n"));
        assert!(text.contains("d_bucket{stage=\"build\",le=\"+Inf\"} 2\n"));
        assert!(text.contains("d_count{stage=\"build\"} 2\nd_sum{stage=\"build\"} 3.5\n"));
        let bare = distribution.render("d");
        assert!(bare.contains("d_bucket{le=\"0.5\"} 1\n"));
        assert!(bare.contains("d_count 2\nd_sum 3.5\n"));
    }
    #[test]
    fn dropped_handler_is_a_cancellation_and_labels_are_bounded() {
        let metrics = HttpMetrics::default();
        metrics.initialize(&["init"]);
        drop(RequestGuard {
            metrics: metrics.clone(),
            endpoint: "init",
            began: Instant::now(),
            code: None,
        });
        assert!(metrics
            .render()
            .contains("pir_http_cancelled_total{endpoint=\"init\"} 1"));
        assert!(!metrics.render().contains("private"));
    }
}
