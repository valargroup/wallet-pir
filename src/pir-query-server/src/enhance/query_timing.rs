//! Matched successful-query timing samples, with no identifiers or query data.
use prometheus::{Encoder, HistogramOpts, HistogramVec, Registry, TextEncoder};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub(crate) struct QueryTiming {
    registry: Registry,
    stages: HistogramVec,
    started: f64,
}
impl Default for QueryTiming {
    fn default() -> Self {
        let registry = Registry::new();
        let stages = HistogramVec::new(
            HistogramOpts::new(
                "enhance_query_stage_duration_seconds",
                "Successful query stages, total excludes body read and response transmission",
            )
            .buckets(vec![
                0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1., 2.5,
                5., 10., 30., 60., 120.,
            ]),
            &["stage"],
        )
        .unwrap();
        registry.register(Box::new(stages.clone())).unwrap();
        for stage in ["worker", "packing", "total"] {
            stages.with_label_values(&[stage]);
        }
        Self {
            registry,
            stages,
            started: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
        }
    }
}
impl QueryTiming {
    pub(crate) fn observe(&self, stages: CompletedStages, elapsed: Duration) {
        let total = elapsed.saturating_sub(stages.body_read);
        let Some(worker) = stages.worker.filter(|w| *w <= total) else {
            return;
        };
        if stages.packing > total {
            return;
        }
        for (stage, time) in [
            ("worker", worker),
            ("packing", stages.packing),
            ("total", total),
        ] {
            self.stages
                .with_label_values(&[stage])
                .observe(time.as_secs_f64());
        }
    }
    pub(crate) fn render(&self) -> String {
        let mut bytes = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut bytes)
            .unwrap();
        format!(
            "{}process_start_time_seconds {}\n",
            String::from_utf8(bytes).unwrap(),
            self.started
        )
    }
}
/// Response extension only: it is never serialized onto the public response.
#[derive(Clone)]
pub(crate) struct CompletedStages {
    pub body_read: Duration,
    pub worker: Option<Duration>,
    pub packing: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn matched_samples_exclude_upload_and_reject_missing_timing() {
        let metrics = QueryTiming::default();
        let stages = CompletedStages {
            body_read: Duration::from_secs(2),
            worker: Some(Duration::from_secs(3)),
            packing: Duration::from_secs(1),
        };
        metrics.observe(stages.clone(), Duration::from_secs(7));
        metrics.observe(
            CompletedStages {
                worker: None,
                ..stages.clone()
            },
            Duration::from_secs(7),
        );
        metrics.observe(stages, Duration::from_secs(3));
        for stage in ["worker", "packing", "total"] {
            assert_eq!(
                metrics
                    .stages
                    .with_label_values(&[stage])
                    .get_sample_count(),
                1
            );
        }
        assert_eq!(
            metrics
                .stages
                .with_label_values(&["total"])
                .get_sample_sum(),
            5.0
        );
        assert_eq!(
            metrics
                .stages
                .with_label_values(&["worker"])
                .get_sample_sum(),
            3.0
        );
        assert_eq!(
            metrics
                .stages
                .with_label_values(&["packing"])
                .get_sample_sum(),
            1.0
        );
    }
}
